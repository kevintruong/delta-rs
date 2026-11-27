//! Tables API adapter for external Delta table lifecycle operations.

use std::collections::HashMap;

use delta_kernel::schema::{DataType, MetadataValue, PrimitiveType, StructField};
use deltalake_core::DeltaTable;
use deltalake_core::data_catalog::{
    CatalogEntry, CatalogLifecycleError, CatalogReader, CatalogResult, CatalogWriter,
    CreateTableOutcome, TableIdentifier, TableRegistration,
};
use reqwest::header::AUTHORIZATION;
use reqwest::{Response, StatusCode};
use serde::Serialize;
use url::{Url, form_urlencoded::byte_serialize};

use crate::models::{
    ColumnInfo, ColumnTypeName, DataSourceFormat, ErrorResponse, Table,
    TableTempCredentialsResponse, TableType,
};
use crate::{StorageConfig, UnityCatalog, UnityCatalogError, build_object_store, default_logstore};

pub(super) const DELTA_ID_PROPERTY: &str = "delta-rs.table-id";

#[async_trait::async_trait]
impl CatalogReader for UnityCatalog {
    async fn get_table(&self, id: &TableIdentifier) -> CatalogResult<Option<CatalogEntry>> {
        let [catalog, schema, name] = names(id)?;
        let full_name = format!("{catalog}.{schema}.{name}");
        let mut url =
            Url::parse(&format!("{}/tables", self.catalog_url())).map_err(provider_error)?;
        url.path_segments_mut()
            .map_err(|_| CatalogLifecycleError::InvalidLocation)?
            .push(&full_name);

        let token = self.get_credential().await.map_err(provider_error)?;
        let response = self
            .client
            .get(url)
            .header(AUTHORIZATION, token)
            .send()
            .await
            .map_err(provider_error)?;
        Ok(parse_external_table(id, response)
            .await?
            .map(|(_, entry)| entry))
    }

    async fn open_table(
        &self,
        entry: &CatalogEntry,
        storage_options: &HashMap<String, String>,
    ) -> CatalogResult<DeltaTable> {
        let table_id = entry.provider_id().ok_or_else(|| {
            provider_error("Unity storage credentials require an observed table ID")
        })?;

        // The name could resolve to a different provider record after lookup.
        let id = entry.identifier();
        let fresh = CatalogReader::get_table(self, id).await?;
        if !fresh.is_some_and(|item| {
            item.provider_id() == Some(table_id)
                && item.location() == entry.location()
                && item.delta_id() == entry.delta_id()
        }) {
            return Err(CatalogLifecycleError::Conflict { table: id.clone() });
        }

        let response = self
            .get_table_credentials_by_id(table_id, "READ")
            .await
            .map_err(provider_error)?;
        let credentials = match response {
            TableTempCredentialsResponse::Success(response) => response
                .get_credentials()
                .ok_or_else(|| provider_error(UnityCatalogError::MissingCredential))?,
            TableTempCredentialsResponse::Error(response) => {
                return Err(provider_error(
                    UnityCatalogError::TemporaryCredentialsFetchFailure {
                        error_code: response.error_code,
                        message: response.message,
                    },
                ));
            }
        };

        // Use the observed root and ID with this client's identity. Resolving the UC URI
        // again in the factory would allow the name to be rebound before the load.
        let config = StorageConfig::parse_options(storage_options.clone())?;
        let store = build_object_store(entry.location().as_str(), credentials, &config)?;
        let mut table = DeltaTable::new(default_logstore(
            store.clone(),
            store,
            entry.read_url(),
            &config,
        ));
        table.load().await?;
        Ok(table)
    }
}

#[async_trait::async_trait]
impl CatalogWriter for UnityCatalog {
    async fn create_table(
        &self,
        registration: &TableRegistration,
    ) -> CatalogResult<CreateTableOutcome> {
        let id = registration.identifier();
        let [catalog, schema, name] = names(id)?;
        let mut properties = HashMap::from([(
            DELTA_ID_PROPERTY.to_owned(),
            registration.metadata().id().to_owned(),
        )]);
        self.seed_sync_stamp(registration, &mut properties).await?;
        let request = CreateExternalDelta {
            catalog_name: catalog,
            schema_name: schema,
            name,
            table_type: "EXTERNAL",
            data_source_format: "DELTA",
            storage_location: registration.location().as_str(),
            comment: registration.metadata().description(),
            columns: columns(registration)?,
            properties,
        };
        let token = self.get_credential().await.map_err(provider_error)?;
        let result = self
            .create_client
            .post(format!("{}/tables", self.catalog_url()))
            .header(AUTHORIZATION, token)
            .json(&request)
            .send()
            .await;

        self.invalidate_table(&format!("{catalog}.{schema}.{name}"));
        let unknown = |source| CatalogLifecycleError::OutcomeUnknown {
            table: id.clone(),
            source,
        };
        let response = result.map_err(|error| unknown(error.into()))?;
        let status = response.status();
        if status.is_success() {
            Ok(CreateTableOutcome::Created)
        } else if status == StatusCode::CONFLICT {
            Ok(CreateTableOutcome::AlreadyExists)
        } else if status.is_server_error()
            || status.is_redirection()
            || matches!(
                status,
                StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS
            )
        {
            Err(unknown(
                format!("Unity Tables API returned {status}").into(),
            ))
        } else {
            Err(provider_error(reply_error(response).await))
        }
    }
}

pub(super) async fn parse_external_table(
    id: &TableIdentifier,
    response: Response,
) -> CatalogResult<Option<(Table, CatalogEntry)>> {
    let [catalog, schema, name] = names(id)?;
    let full_name = format!("{catalog}.{schema}.{name}");
    if !response.status().is_success() {
        let status = response.status();
        let error = reply_error(response).await;
        if status == StatusCode::NOT_FOUND
            && matches!(
                &error,
                UnityCatalogError::InvalidTable { error_code, .. }
                    if matches!(error_code.as_str(), "TABLE_NOT_FOUND" | "RESOURCE_DOES_NOT_EXIST")
            )
        {
            return Ok(None);
        }
        return Err(provider_error(error));
    }

    let table = response.json::<Table>().await.map_err(provider_error)?;
    if table.table_type != TableType::External
        || table.data_source_format != Some(DataSourceFormat::Delta)
        || ![&table.catalog_name, &table.schema_name, &table.name]
            .into_iter()
            .zip([catalog, schema, name])
            .all(|(actual, expected)| actual == expected || *actual == expected.to_lowercase())
    {
        return Err(CatalogLifecycleError::Conflict { table: id.clone() });
    }

    if table.table_id.is_empty() {
        return Err(provider_error(
            "Unity external Delta table is missing its table ID",
        ));
    }
    let location = table
        .storage_location
        .as_deref()
        .and_then(|location| Url::parse(location).ok())
        .ok_or(CatalogLifecycleError::InvalidLocation)?;
    let authority: String = byte_serialize(full_name.as_bytes()).collect();
    let read_url = Url::parse(&format!("uc://{authority}")).map_err(provider_error)?;

    let mut entry = CatalogEntry::new(id.clone(), location)?
        .with_read_url(read_url)?
        .with_provider_id(&table.table_id);
    if let Some(delta_id) = table.properties.get(DELTA_ID_PROPERTY) {
        entry = entry.with_delta_id(delta_id);
    }
    Ok(Some((table, entry)))
}

pub(super) fn names(id: &TableIdentifier) -> CatalogResult<[&str; 3]> {
    let [catalog, schema] = id.namespace() else {
        return Err(CatalogLifecycleError::InvalidIdentifier);
    };
    let result = [catalog.as_str(), schema.as_str(), id.name()];
    if result.iter().any(|name| {
        name.chars().count() > 255
            || name
                .chars()
                .any(|c| c.is_ascii_control() || matches!(c, '.' | '/' | ' '))
    }) {
        return Err(CatalogLifecycleError::InvalidIdentifier);
    }
    Ok(result)
}

pub(super) fn provider_error(
    source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> CatalogLifecycleError {
    CatalogLifecycleError::Provider {
        source: source.into(),
    }
}

async fn reply_error(response: Response) -> UnityCatalogError {
    let status = response.status();
    match response.json::<ErrorResponse>().await {
        Ok(error) => error.into(),
        Err(error) => UnityCatalogError::InvalidTable {
            error_code: status.to_string(),
            message: error.to_string(),
        },
    }
}

#[derive(Serialize)]
struct CreateExternalDelta<'a> {
    catalog_name: &'a str,
    schema_name: &'a str,
    name: &'a str,
    table_type: &'static str,
    data_source_format: &'static str,
    storage_location: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    comment: Option<&'a str>,
    columns: Vec<ColumnInfo>,
    properties: HashMap<String, String>,
}

fn columns(registration: &TableRegistration) -> CatalogResult<Vec<ColumnInfo>> {
    let partition_columns = registration.metadata().partition_columns();
    registration
        .schema()
        .fields()
        .enumerate()
        .map(|(position, field)| {
            let (type_name, type_text) = unity_type(field.data_type())?;
            let (type_precision, type_scale) = match field.data_type() {
                DataType::Primitive(PrimitiveType::Decimal(decimal)) => (
                    Some(i32::from(decimal.precision())),
                    Some(i32::from(decimal.scale())),
                ),
                _ => (None, None),
            };
            let partition_index = partition_columns
                .iter()
                .position(|name| name == field.name())
                .map(i32::try_from)
                .transpose()
                .map_err(provider_error)?;
            Ok(ColumnInfo {
                name: field.name().clone(),
                type_name: Some(type_name),
                type_text: Some(type_text),
                type_json: Some(serde_json::to_string(field).map_err(provider_error)?),
                type_precision,
                type_scale,
                position: u32::try_from(position).map_err(provider_error)?,
                nullable: field.is_nullable(),
                partition_index,
                comment: match field.metadata().get("comment") {
                    Some(MetadataValue::String(comment)) => Some(comment.clone()),
                    _ => None,
                },
                ..Default::default()
            })
        })
        .collect()
}

fn nested_field(field: &StructField) -> CatalogResult<String> {
    let name = field.name().replace('`', "``");
    let (_, data_type) = unity_type(field.data_type())?;
    Ok(format!("`{name}`:{data_type}"))
}

#[allow(unreachable_patterns)] // Kernel feature unification can add user-defined types.
fn unity_type(data_type: &DataType) -> CatalogResult<(ColumnTypeName, String)> {
    use ColumnTypeName as Unity;
    use PrimitiveType as Delta;

    let unsupported =
        || provider_error(format!("unsupported Unity registration type: {data_type}"));
    Ok(match data_type {
        DataType::Array(array) => (
            Unity::Array,
            format!("array<{}>", unity_type(array.element_type())?.1),
        ),
        DataType::Map(map) => (
            Unity::Map,
            format!(
                "map<{},{}>",
                unity_type(map.key_type())?.1,
                unity_type(map.value_type())?.1
            ),
        ),
        DataType::Struct(schema) => (
            Unity::Struct,
            format!(
                "struct<{}>",
                schema
                    .fields()
                    .map(nested_field)
                    .collect::<CatalogResult<Vec<_>>>()?
                    .join(",")
            ),
        ),
        DataType::Variant(_) => (Unity::Variant, "variant".into()),
        DataType::Primitive(primitive) => {
            let (kind, alias) = match primitive {
                Delta::Boolean => (Unity::Boolean, None),
                Delta::Byte => (Unity::Byte, Some("tinyint")),
                Delta::Short => (Unity::Short, Some("smallint")),
                Delta::Integer => (Unity::Int, Some("int")),
                Delta::Long => (Unity::Long, Some("bigint")),
                Delta::Float => (Unity::Float, None),
                Delta::Double => (Unity::Double, None),
                Delta::Date => (Unity::Date, None),
                Delta::Timestamp => (Unity::Timestamp, None),
                Delta::TimestampNtz => (Unity::TimestampNtz, None),
                Delta::String => (Unity::String, None),
                Delta::Binary => (Unity::Binary, None),
                Delta::Decimal(_) => (Unity::Decimal, None),
                Delta::Void => (Unity::Null, None),
                _ => return Err(unsupported()),
            };
            (
                kind,
                alias.map_or_else(|| primitive.to_string(), str::to_owned),
            )
        }
        _ => return Err(unsupported()),
    })
}
