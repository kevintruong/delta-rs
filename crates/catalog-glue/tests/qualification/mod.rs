use std::error::Error;

use aws_sdk_glue::Client;
use aws_sdk_glue::config::{Builder, retry::RetryConfig};
use aws_sdk_glue::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_glue::operation::update_table::UpdateTableError;
use aws_sdk_glue::types::{Table, TableInput, builders::TableInputBuilder};

mod live;
mod tests;

type ProbeResult<T> = Result<T, Box<dyn Error>>;

const UNRELATED: &str = "delta-rs.qualification-unrelated";
const FORMAT: &str = "delta-rs.sync-format";
const VERSION: &str = "delta-rs.sync-version";
const DIGEST: &str = "delta-rs.sync-digest";
const PROJECTION_DESCRIPTION: &str = "delta-rs Glue qualification projection";
const PROJECTION_VERSION: &str = "9";
const PROJECTION_DIGEST: &str = "1111111111111111111111111111111111111111111111111111111111111111";

struct Scope<'a> {
    catalog: &'a str,
    database: &'a str,
    table: &'a str,
    location: &'a str,
    delta_id: &'a str,
}

#[derive(Debug)]
struct Evidence {
    versions: [String; 3],
    stale_error_code: String,
}

async fn qualify_version_id(client: &Client, scope: &Scope<'_>) -> ProbeResult<Evidence> {
    let initial = get_native_delta_table(client, scope).await?;
    let version_before = version(&initial)?;
    let unrelated = copy_native_input(&initial)
        .parameters(UNRELATED, "advanced")
        .build()?;
    update(client, scope, version_before, unrelated.clone()).await?;
    let advanced = get_native_delta_table(client, scope).await?;
    let version_advanced = version(&advanced)?;
    if version_before == version_advanced || copy_native_input(&advanced).build()? != unrelated {
        return Err("fresh VersionId did not advance the unrelated field cleanly".into());
    }

    let attempt = update(client, scope, version_before, compound_input(&initial)?).await;
    let preserved = get_native_delta_table(client, scope).await?;
    let stale_error_code = require_concurrent_modification(attempt)?;
    if version(&preserved)? != version_advanced
        || copy_native_input(&preserved).build()? != unrelated
    {
        return Err("stale attempt changed the record".into());
    }

    let compound = compound_input(&preserved)?;
    update(client, scope, version(&preserved)?, compound.clone()).await?;
    let round_trip = get_native_delta_table(client, scope).await?;
    let version_round_trip = version(&round_trip)?;
    if version_round_trip == version_before
        || version_round_trip == version_advanced
        || copy_native_input(&round_trip).build()? != compound
    {
        return Err("compound projection did not round-trip with its watermark".into());
    }

    Ok(Evidence {
        versions: [
            version_before.into(),
            version_advanced.into(),
            version_round_trip.into(),
        ],
        stale_error_code,
    })
}

async fn qualify_replacement(
    client: &Client,
    scope: &Scope<'_>,
    original: &Table,
    replacement: &Table,
) -> ProbeResult<String> {
    let old_version = version(original)?;
    let replacement_version = version(replacement)?;
    if old_version == replacement_version {
        return Err(format!("replacement reused VersionId {replacement_version}").into());
    }
    let attempt = update(client, scope, old_version, compound_input(original)?).await;
    let preserved = get_native_delta_table(client, scope).await?;
    let code = require_concurrent_modification(attempt)?;
    if version(&preserved)? != replacement_version
        || copy_native_input(&preserved).build()? != copy_native_input(replacement).build()?
    {
        return Err("old VersionId changed the replacement record".into());
    }
    Ok(code)
}

async fn get_native_delta_table(client: &Client, scope: &Scope<'_>) -> ProbeResult<Table> {
    let table = client
        .get_table()
        .catalog_id(scope.catalog)
        .database_name(scope.database)
        .name(scope.table)
        .send()
        .await?
        .table
        .ok_or("GetTable did not return the disposable entry")?;
    if table.name() != scope.table
        || table.database_name() != Some(scope.database)
        || table.catalog_id() != Some(scope.catalog)
        || table.table_type() != Some("EXTERNAL_TABLE")
        || table.target_table().is_some()
        || table.federated_table().is_some()
        || table.view_definition().is_some()
        || table.storage_descriptor().and_then(|sd| sd.location()) != Some(scope.location)
        || !table.parameters().is_some_and(|parameters| {
            parameters
                .get("spark.sql.sources.provider")
                .is_some_and(|value| value == "delta")
                && parameters
                    .get("delta-rs.table-id")
                    .is_some_and(|value| value == scope.delta_id)
        })
    {
        return Err("GetTable did not return a scoped native Delta registration".into());
    }
    Ok(table)
}

fn version(table: &Table) -> ProbeResult<&str> {
    table
        .version_id()
        .filter(|id| !id.trim().is_empty())
        .ok_or_else(|| "GetTable omitted an observed VersionId".into())
}

fn copy_native_input(table: &Table) -> TableInputBuilder {
    TableInput::builder()
        .name(table.name())
        .set_description(table.description.clone())
        .set_owner(table.owner.clone())
        .set_last_access_time(table.last_access_time)
        .set_last_analyzed_time(table.last_analyzed_time)
        .retention(table.retention)
        .set_storage_descriptor(table.storage_descriptor.clone())
        .set_partition_keys(table.partition_keys.clone())
        .set_view_original_text(table.view_original_text.clone())
        .set_view_expanded_text(table.view_expanded_text.clone())
        .set_table_type(table.table_type.clone())
        .set_parameters(table.parameters.clone())
}

fn compound_input(table: &Table) -> ProbeResult<TableInput> {
    Ok(copy_native_input(table)
        .description(PROJECTION_DESCRIPTION)
        .parameters(FORMAT, "1")
        .parameters(VERSION, PROJECTION_VERSION)
        .parameters(DIGEST, PROJECTION_DIGEST)
        .build()?)
}

async fn update(
    client: &Client,
    scope: &Scope<'_>,
    version: &str,
    input: TableInput,
) -> Result<(), SdkError<UpdateTableError>> {
    client
        .update_table()
        .catalog_id(scope.catalog)
        .database_name(scope.database)
        .name(scope.table)
        .version_id(version)
        .table_input(input)
        .customize()
        .config_override(Builder::new().retry_config(RetryConfig::disabled()))
        .send()
        .await?;
    Ok(())
}

fn require_concurrent_modification(
    attempt: Result<(), SdkError<UpdateTableError>>,
) -> ProbeResult<String> {
    match attempt {
        Ok(()) => Err("Glue accepted stale VersionId".into()),
        Err(error) => {
            let service = error.as_service_error();
            if let Some(cause) =
                service.filter(|cause| cause.is_concurrent_modification_exception())
            {
                return Ok(cause
                    .code()
                    .unwrap_or("ConcurrentModificationException")
                    .into());
            }
            let code = service
                .and_then(ProvideErrorMetadata::code)
                .unwrap_or("non-service error");
            Err(format!(
                "stale attempt returned {code}, expected ConcurrentModificationException: {error}"
            )
            .into())
        }
    }
}
