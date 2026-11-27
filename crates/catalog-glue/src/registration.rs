use std::collections::HashSet;
use std::error::Error;

use aws_sdk_glue::config::{Builder, retry::RetryConfig};
use aws_sdk_glue::error::SdkError;
use aws_sdk_glue::operation::create_table::CreateTableError;
use aws_sdk_glue::types::{Column, SerDeInfo, StorageDescriptor, Table, TableInput};
use deltalake_core::data_catalog::{
    CatalogEntry, CatalogLifecycleError, CatalogReader, CatalogResult, CatalogWriter,
    CreateTableOutcome, TableIdentifier, TableRegistration,
};
use deltalake_core::kernel::{DataType, PrimitiveType, StructField, StructType};
use url::Url;

use crate::sync::{GlueMetadataProjection, stamp_parameters};
use crate::{GlueDataCatalog, GlueError, PLACEHOLDER_SUFFIX};

const EXTERNAL_TABLE: &str = "EXTERNAL_TABLE";
pub(super) const DELTA_PROVIDER: &str = "spark.sql.sources.provider";
pub(super) const DELTA_ID: &str = "delta-rs.table-id";

#[derive(Debug, thiserror::Error)]
enum ProjectionError {
    #[error("Glue cannot represent this Delta column: {0}")]
    UnsupportedColumn(String),
    #[error("Glue cannot represent this Delta type: {0:?}")]
    UnsupportedType(DataType),
}

#[async_trait::async_trait]
impl CatalogReader for GlueDataCatalog {
    async fn get_table(&self, id: &TableIdentifier) -> CatalogResult<Option<CatalogEntry>> {
        self.read_external_table(id)
            .await
            .map(|record| record.map(|(_, entry)| entry))
    }
}

impl GlueDataCatalog {
    pub(super) async fn read_external_table(
        &self,
        id: &TableIdentifier,
    ) -> CatalogResult<Option<(Table, CatalogEntry)>> {
        let database = glue_database(id)?;
        let response = self
            .client_for_database(database)
            .get_table()
            .set_catalog_id(self.catalog_id.clone())
            .database_name(database)
            .name(id.name())
            .send()
            .await;

        let response = match response {
            Ok(response) => response,
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|e| e.is_entity_not_found_exception()) =>
            {
                return Ok(None);
            }
            Err(error) => return Err(provider_error(error)),
        };
        let table = response.table.ok_or_else(|| missing_metadata("Table"))?;
        let conflict = || CatalogLifecycleError::Conflict { table: id.clone() };
        if table.name() != id.name()
            || table.database_name().is_some_and(|name| name != database)
            || self
                .catalog_id
                .as_deref()
                .zip(table.catalog_id())
                .is_some_and(|(expected, actual)| expected != actual)
            || table.table_type() != Some(EXTERNAL_TABLE)
            || table.target_table().is_some()
            || table.federated_table().is_some()
        {
            return Err(conflict());
        }
        let parameters = table.parameters().ok_or_else(conflict)?;
        let is_delta = |value: &String| value.eq_ignore_ascii_case("delta");
        let native = match parameters.get(DELTA_PROVIDER) {
            Some(provider) => is_delta(provider),
            None => {
                parameters.get("classification").is_some_and(is_delta)
                    || parameters.get("table_type").is_some_and(is_delta)
            }
        };
        if !native {
            return Err(conflict());
        }

        let storage = table
            .storage_descriptor()
            .ok_or_else(|| missing_metadata("Storage Descriptor"))?;
        if storage
            .input_format()
            .is_some_and(|format| format.ends_with(".SymlinkTextInputFormat"))
        {
            return Err(conflict());
        }
        let location = storage
            .location()
            .ok_or_else(|| missing_metadata("Location"))?;
        let mut location =
            Url::parse(location).map_err(|_| CatalogLifecycleError::InvalidLocation)?;
        if location.scheme() == "s3a" {
            location
                .set_scheme("s3")
                .map_err(|_| CatalogLifecycleError::InvalidLocation)?;
        }
        if let Some(path) = location.path().strip_suffix(PLACEHOLDER_SUFFIX) {
            location.set_path(&format!("{path}/"));
        }
        validate_s3(&location)?;

        let mut entry = CatalogEntry::new(id.clone(), location)?;
        if let Some(delta_id) = parameters.get(DELTA_ID) {
            entry = entry.with_delta_id(delta_id);
        }
        Ok(Some((table, entry)))
    }
}

#[async_trait::async_trait]
impl CatalogWriter for GlueDataCatalog {
    async fn create_table(
        &self,
        registration: &TableRegistration,
    ) -> CatalogResult<CreateTableOutcome> {
        let id = registration.identifier();
        let database = glue_database(id)?;
        validate_s3(registration.location())?;
        let input = table_input(registration, self.qualified_for(database).is_some())?;

        // The lifecycle reconciles ambiguous creates by reading the binding. Limit
        // this operation to one attempt independently of the client's read policy.
        let response = self
            .client_for_database(database)
            .create_table()
            .set_catalog_id(self.catalog_id.clone())
            .database_name(database)
            .table_input(input)
            .customize()
            .config_override(Builder::new().retry_config(RetryConfig::disabled()))
            .send()
            .await;

        match response {
            Ok(_) => Ok(CreateTableOutcome::Created),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(CreateTableError::is_already_exists_exception) =>
            {
                Ok(CreateTableOutcome::AlreadyExists)
            }
            Err(error) if ambiguous_create(&error) => Err(CatalogLifecycleError::OutcomeUnknown {
                table: id.clone(),
                source: Box::new(error),
            }),
            Err(error) => Err(provider_error(error)),
        }
    }
}

fn ambiguous_create(error: &SdkError<CreateTableError>) -> bool {
    match error {
        SdkError::ConstructionFailure(_) => false,
        SdkError::ServiceError(service) => {
            let cause = service.err();
            service.raw().status().is_server_error()
                || cause.is_operation_timeout_exception()
                || cause.is_internal_service_exception()
                || cause.is_concurrent_modification_exception()
        }
        _ => true,
    }
}

pub(super) fn glue_database(id: &TableIdentifier) -> CatalogResult<&str> {
    let [database] = id.namespace() else {
        return Err(CatalogLifecycleError::InvalidIdentifier);
    };
    // Glue folds table names to lowercase and expects lowercase database names.
    // Keep the caller's identifier unambiguous through request and read-back.
    if database.chars().any(char::is_uppercase) || id.name().chars().any(char::is_uppercase) {
        return Err(CatalogLifecycleError::InvalidIdentifier);
    }
    Ok(database)
}

fn validate_s3(location: &Url) -> CatalogResult<()> {
    // The table loader normalizes repeated path separators. They are significant
    // in an S3 key, so preserve registration identity by rejecting those roots.
    if location.scheme() != "s3"
        || location.host_str().is_none_or(str::is_empty)
        || location.port().is_some()
        || location.path().contains("//")
    {
        return Err(CatalogLifecycleError::InvalidLocation);
    }
    Ok(())
}

fn table_input(registration: &TableRegistration, seed_sync: bool) -> CatalogResult<TableInput> {
    let metadata = registration.metadata();
    let projection = GlueMetadataProjection::from_delta(
        registration.schema(),
        metadata.partition_columns(),
        metadata.description(),
    )?;
    let location = registration.location().as_str();
    let mut parameters = std::collections::HashMap::from([
        ("EXTERNAL".to_owned(), "TRUE".to_owned()),
        (DELTA_PROVIDER.to_owned(), "delta".to_owned()),
        (DELTA_ID.to_owned(), metadata.id().to_owned()),
    ]);
    if seed_sync {
        let stamp = deltalake_core::data_catalog::CatalogProjectionStamp::new(
            deltalake_core::data_catalog::CATALOG_PROJECTION_FORMAT_VERSION,
            metadata.id(),
            registration.version(),
            projection.digest()?,
        )?;
        stamp_parameters(&mut parameters, &stamp);
    }
    let descriptor = StorageDescriptor::builder()
        .location(location)
        .set_columns(Some(projection.columns))
        .serde_info(
            SerDeInfo::builder()
                .parameters("path", location)
                .parameters("serialization.format", "1")
                .build(),
        )
        .build();
    TableInput::builder()
        .name(registration.identifier().name())
        .set_description(projection.description)
        .table_type(EXTERNAL_TABLE)
        .set_parameters(Some(parameters))
        .storage_descriptor(descriptor)
        .set_partition_keys(Some(projection.partitions))
        .build()
        .map_err(provider_error)
}

pub(super) fn project_columns(
    schema: &StructType,
    partition_names: &[String],
) -> CatalogResult<(Vec<Column>, Vec<Column>)> {
    validate_column_names(schema).map_err(provider_error)?;
    let columns = schema
        .fields()
        .filter(|field| !partition_names.contains(field.name()))
        .map(glue_column)
        .collect::<CatalogResult<Vec<_>>>()?;
    let partitions = partition_names
        .iter()
        .map(|name| {
            let field = schema.field(name).ok_or_else(|| unsupported_column(name))?;
            if !matches!(field.data_type(), DataType::Primitive(primitive) if *primitive != PrimitiveType::Binary) {
                return Err(unsupported_column(name));
            }
            glue_column(field)
        })
        .collect::<CatalogResult<Vec<_>>>()?;
    Ok((columns, partitions))
}

fn validate_column_names(schema: &StructType) -> Result<(), ProjectionError> {
    let mut names = HashSet::new();
    for name in schema.field_names() {
        if !names.insert(name.to_lowercase()) {
            return Err(ProjectionError::UnsupportedColumn(name.clone()));
        }
    }
    Ok(())
}

fn glue_column(field: &StructField) -> CatalogResult<Column> {
    Column::builder()
        .name(field.name())
        .r#type(hive_type(field.data_type()).map_err(provider_error)?)
        .build()
        .map_err(provider_error)
}

fn hive_type(data_type: &DataType) -> Result<String, ProjectionError> {
    let unsupported = || ProjectionError::UnsupportedType(data_type.clone());
    match data_type {
        DataType::Primitive(primitive) => {
            let name = match primitive {
                PrimitiveType::Boolean => "boolean",
                PrimitiveType::Byte => "tinyint",
                PrimitiveType::Short => "smallint",
                PrimitiveType::Integer => "int",
                PrimitiveType::Long => "bigint",
                PrimitiveType::Float => "float",
                PrimitiveType::Double => "double",
                PrimitiveType::String => "string",
                PrimitiveType::Binary => "binary",
                PrimitiveType::Date => "date",
                PrimitiveType::Timestamp => "timestamp",
                PrimitiveType::Decimal(decimal) => {
                    return Ok(format!(
                        "decimal({},{})",
                        decimal.precision(),
                        decimal.scale()
                    ));
                }
                _ => return Err(unsupported()),
            };
            Ok(name.to_owned())
        }
        DataType::Array(array) => Ok(format!("array<{}>", hive_type(array.element_type())?)),
        DataType::Map(map) if matches!(map.key_type(), DataType::Primitive(_)) => Ok(format!(
            "map<{},{}>",
            hive_type(map.key_type())?,
            hive_type(map.value_type())?
        )),
        DataType::Struct(structure) => {
            validate_column_names(structure)?;
            let fields = structure
                .fields()
                .map(|field| {
                    let name = field.name();
                    if name.is_empty()
                        || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                    {
                        return Err(ProjectionError::UnsupportedColumn(name.clone()));
                    }
                    Ok(format!("{name}:{}", hive_type(field.data_type())?))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("struct<{}>", fields.join(",")))
        }
        _ => Err(unsupported()),
    }
}

fn missing_metadata(metadata: &str) -> CatalogLifecycleError {
    provider_error(GlueError::MissingMetadata {
        metadata: metadata.to_owned(),
    })
}

fn unsupported_column(name: &str) -> CatalogLifecycleError {
    provider_error(ProjectionError::UnsupportedColumn(name.to_owned()))
}

pub(super) fn provider_error(error: impl Error + Send + Sync + 'static) -> CatalogLifecycleError {
    CatalogLifecycleError::Provider {
        source: Box::new(error),
    }
}
