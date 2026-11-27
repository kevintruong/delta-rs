use std::collections::HashMap;

use aws_sdk_glue::config::{Builder, retry::RetryConfig};
use aws_sdk_glue::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_glue::operation::update_table::UpdateTableError;
use aws_sdk_glue::types::{Column, Table, TableInput};
use deltalake_core::data_catalog::{
    CatalogLifecycleError, CatalogProjectionStamp, CatalogResult, CatalogRevision,
    CatalogSyncProjection, CatalogSyncState, CatalogSyncUpdate, CatalogSyncWriteOutcome,
    CatalogSyncWriter, CatalogTable, TableIdentifier,
};
use deltalake_core::kernel::StructType;
use sha2::{Digest, Sha256};

use super::GlueDataCatalog;
use super::registration::{
    DELTA_ID, DELTA_PROVIDER, glue_database, project_columns, provider_error,
};

const FORMAT: &str = "delta-rs.sync-format";
const VERSION: &str = "delta-rs.sync-version";
const DIGEST: &str = "delta-rs.sync-digest";

/// Delta-owned metadata fields in Glue's representation.
#[doc(hidden)]
pub struct GlueMetadataProjection {
    pub(super) description: Option<String>,
    pub(super) columns: Vec<Column>,
    pub(super) partitions: Vec<Column>,
}

impl GlueMetadataProjection {
    pub(super) fn from_delta(
        schema: &StructType,
        partition_names: &[String],
        description: Option<&str>,
    ) -> CatalogResult<Self> {
        let (columns, partitions) = project_columns(schema, partition_names)?;
        Ok(Self {
            description: description.map(str::to_owned),
            columns,
            partitions,
        })
    }

    pub(super) fn digest(&self) -> CatalogResult<String> {
        digest(self.description.as_deref(), &self.columns, &self.partitions)
    }
}

fn digest(
    description: Option<&str>,
    columns: &[Column],
    partitions: &[Column],
) -> CatalogResult<String> {
    fn pairs(columns: &[Column]) -> CatalogResult<Vec<(&str, &str)>> {
        columns
            .iter()
            .map(|column| {
                let data_type = column
                    .r#type()
                    .ok_or(CatalogLifecycleError::InvalidSyncState)?;
                Ok((column.name(), data_type))
            })
            .collect()
    }
    let fields = (description, pairs(columns)?, pairs(partitions)?);
    let bytes = serde_json::to_vec(&fields).map_err(provider_error)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

pub(super) fn stamp_parameters(
    parameters: &mut HashMap<String, String>,
    stamp: &CatalogProjectionStamp,
) {
    parameters.insert(DELTA_ID.into(), stamp.delta_id().into());
    parameters.insert(FORMAT.into(), stamp.format_version().to_string());
    parameters.insert(VERSION.into(), stamp.delta_version().to_string());
    parameters.insert(DIGEST.into(), stamp.digest().into());
}

#[async_trait::async_trait]
impl CatalogSyncWriter for GlueDataCatalog {
    type Record = Table;
    type Projection = GlueMetadataProjection;

    async fn get_sync_state(
        &self,
        id: &TableIdentifier,
    ) -> CatalogResult<Option<CatalogSyncState<Table>>> {
        let database = glue_database(id)?;
        let unsupported = || CatalogLifecycleError::Unsupported { table: id.clone() };
        let qualified = self.qualified_for(database).ok_or_else(unsupported)?;
        let Some((table, entry)) = self.read_external_table(id).await? else {
            return Ok(None);
        };
        if table.catalog_id() != self.catalog_id.as_deref()
            || table.database_name() != Some(database)
        {
            return Err(CatalogLifecycleError::InvalidSyncState);
        }
        let params = table
            .parameters()
            .ok_or(CatalogLifecycleError::InvalidSyncState)?;
        if table.is_registered_with_lake_formation() != qualified.lake_formation_registered
            || params.get(DELTA_PROVIDER).map(String::as_str) != Some("delta")
            || table.view_definition.is_some()
            || table.is_multi_dialect_view == Some(true)
            || table.is_materialized_view == Some(true)
            || table.iceberg_table_metadata.is_some()
            || table.rls_properties.is_some()
        {
            return Err(unsupported());
        }
        let required = |key| {
            params
                .get(key)
                .map(String::as_str)
                .ok_or(CatalogLifecycleError::InvalidSyncState)
        };
        let format = required(FORMAT)?
            .parse()
            .map_err(|_| CatalogLifecycleError::InvalidSyncState)?;
        let version = required(VERSION)?
            .parse()
            .map_err(|_| CatalogLifecycleError::InvalidSyncState)?;
        let stamp =
            CatalogProjectionStamp::new(format, required(DELTA_ID)?, version, required(DIGEST)?)?;
        let revision = CatalogRevision::new(
            table
                .version_id()
                .ok_or(CatalogLifecycleError::InvalidSyncState)?,
        )?;
        let storage = table
            .storage_descriptor()
            .ok_or(CatalogLifecycleError::InvalidSyncState)?;
        let observed_digest = digest(
            table.description(),
            storage.columns(),
            table.partition_keys(),
        )?;
        let create_time = table
            .create_time()
            .ok_or(CatalogLifecycleError::InvalidSyncState)?;
        let entry = entry.with_provider_id(format!(
            "{}:{:09}",
            create_time.secs(),
            create_time.subsec_nanos()
        ));
        CatalogSyncState::new(entry, revision, stamp, observed_digest, table).map(Some)
    }

    fn project_table(
        &self,
        table: &CatalogTable,
    ) -> CatalogResult<CatalogSyncProjection<GlueMetadataProjection>> {
        let projection = GlueMetadataProjection::from_delta(
            &table.schema(),
            table.metadata().partition_columns(),
            table.metadata().description(),
        )?;
        CatalogSyncProjection::new(projection.digest()?, projection)
    }

    async fn update_table_metadata(
        &self,
        update: CatalogSyncUpdate<'_, Table, GlueMetadataProjection>,
    ) -> CatalogResult<CatalogSyncWriteOutcome> {
        let id = update.state().entry().identifier();
        let database = glue_database(id)?;
        let qualified = self
            .qualified_for(database)
            .ok_or_else(|| CatalogLifecycleError::Unsupported { table: id.clone() })?;
        let input = merge(update.state().record(), update.projection(), update.stamp())?;
        let result = qualified
            .client
            .update_table()
            .set_catalog_id(self.catalog_id.clone())
            .database_name(database)
            .name(id.name())
            .version_id(update.state().revision().as_str())
            .table_input(input)
            .customize()
            .config_override(Builder::new().retry_config(RetryConfig::disabled()))
            .send()
            .await;
        match result {
            Ok(_) => Ok(CatalogSyncWriteOutcome::Applied),
            Err(SdkError::ServiceError(service))
                if service.raw().status().as_u16() == 400
                    && service.err().is_concurrent_modification_exception() =>
            {
                Ok(CatalogSyncWriteOutcome::Rejected)
            }
            Err(error) if definitive_failure(&error) => Err(provider_error(error)),
            Err(error) => Err(CatalogLifecycleError::OutcomeUnknown {
                table: id.clone(),
                attempted_stamp: Some(Box::new(update.stamp().clone())),
                source: Box::new(error),
            }),
        }
    }
}

fn definitive_failure(error: &SdkError<UpdateTableError>) -> bool {
    match error {
        SdkError::ConstructionFailure(_) => true,
        SdkError::ServiceError(service) if service.raw().status().is_client_error() => {
            let error = service.err();
            error.is_invalid_input_exception()
                || error.is_entity_not_found_exception()
                || matches!(error.code(), Some("AccessDeniedException" | "AccessDenied"))
        }
        _ => false,
    }
}

fn merge(
    observed: &Table,
    projection: &GlueMetadataProjection,
    stamp: &CatalogProjectionStamp,
) -> CatalogResult<TableInput> {
    let mut storage = observed
        .storage_descriptor
        .clone()
        .ok_or(CatalogLifecycleError::InvalidSyncState)?;
    storage.columns = Some(annotated(&projection.columns, storage.columns()));
    let mut parameters = observed
        .parameters
        .clone()
        .ok_or(CatalogLifecycleError::InvalidSyncState)?;
    stamp_parameters(&mut parameters, stamp);
    TableInput::builder()
        .name(observed.name())
        .set_description(projection.description.clone())
        .set_owner(observed.owner.clone())
        .set_last_access_time(observed.last_access_time)
        .set_last_analyzed_time(observed.last_analyzed_time)
        .retention(observed.retention())
        .storage_descriptor(storage)
        .set_partition_keys(Some(annotated(
            &projection.partitions,
            observed.partition_keys(),
        )))
        .set_view_original_text(observed.view_original_text.clone())
        .set_view_expanded_text(observed.view_expanded_text.clone())
        .set_table_type(observed.table_type.clone())
        .set_parameters(Some(parameters))
        .build()
        .map_err(provider_error)
}

fn annotated(projected: &[Column], previous: &[Column]) -> Vec<Column> {
    let previous: HashMap<_, _> = previous
        .iter()
        .map(|column| (column.name(), column))
        .collect();
    projected
        .iter()
        .map(|column| {
            let mut column = column.clone();
            if let Some(existing) = previous.get(column.name()) {
                column.comment = existing.comment.clone();
                column.parameters = existing.parameters.clone();
            }
            column
        })
        .collect()
}
