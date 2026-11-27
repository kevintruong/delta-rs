use aws_sdk_glue::Client;
use aws_sdk_glue::config::{Builder, retry::RetryConfig};
use aws_sdk_glue::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_glue::operation::update_table::UpdateTableError;
use aws_sdk_glue::types::{Table, TableInput};

mod live;
mod tests;

#[path = "../support/live_glue.rs"]
mod live_glue;
use live_glue::{ProbeResult, Scope, copy_native_input, get_native_delta_table, version};

const UNRELATED: &str = "delta-rs.qualification-unrelated";
const FORMAT: &str = "delta-rs.sync-format";
const VERSION: &str = "delta-rs.sync-version";
const DIGEST: &str = "delta-rs.sync-digest";
const PROJECTION_DESCRIPTION: &str = "delta-rs Glue qualification projection";
const PROJECTION_VERSION: &str = "9";
const PROJECTION_DIGEST: &str = "1111111111111111111111111111111111111111111111111111111111111111";

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
