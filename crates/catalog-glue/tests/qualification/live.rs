use std::collections::HashMap;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use aws_config::{BehaviorVersion, profile::ProfileFileCredentialsProvider};
use aws_sdk_glue::Client;
use aws_sdk_glue::config::{Region, retry::RetryConfig};
use aws_sdk_glue::error::SdkError;
use aws_sdk_glue::operation::get_table::GetTableError;
use deltalake_catalog_glue::GlueDataCatalog;
use deltalake_core::data_catalog::{
    CatalogLifecycle, CatalogTable, RegistrationOutcome, TableIdentifier,
};
use serde_json::{Value, json};
use url::Url;

use super::{
    ProbeResult, Scope, UNRELATED, copy_native_input, get_native_delta_table, qualify_replacement,
    qualify_version_id, version,
};

#[tokio::test]
#[ignore = "requires explicit authorization for a disposable AWS Glue catalog entry"]
async fn live_glue_version_id_qualification() -> ProbeResult<()> {
    run_live(LiveCheck::VersionId).await
}

#[tokio::test]
#[ignore = "requires explicit authorization for a disposable AWS Glue catalog entry"]
async fn live_glue_registration_and_load() -> ProbeResult<()> {
    run_live(LiveCheck::Registration).await
}

enum LiveCheck {
    Registration,
    VersionId,
}

async fn run_live(check: LiveCheck) -> ProbeResult<()> {
    let profile = required("DELTA_RS_GLUE_CAS_PROFILE")?;
    let region = required("DELTA_RS_GLUE_CAS_REGION")?;
    if !region
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Err("invalid AWS region name".into());
    }
    let catalog = required("DELTA_RS_GLUE_CAS_CATALOG")?;
    let database = required("DELTA_RS_GLUE_CAS_DATABASE")?;
    let location = Url::parse(&required("DELTA_RS_GLUE_CAS_DELTA_URI")?)?;
    let name = format!(
        "delta_rs_cas_{}_{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let identifier = TableIdentifier::new(vec![database.clone()], name.clone())?;
    let sdk_revision = sdk_revision()?;
    let dns_suffix = if region.starts_with("cn-") {
        "amazonaws.com.cn"
    } else {
        "amazonaws.com"
    };
    let endpoint = format!("https://glue.{region}.{dns_suffix}");
    let sts_endpoint = format!("https://sts.{region}.{dns_suffix}");
    let config = aws_config::defaults(BehaviorVersion::latest())
        .profile_name(&profile)
        .credentials_provider(
            ProfileFileCredentialsProvider::builder()
                .profile_name(&profile)
                .build(),
        )
        .region(Region::new(region.clone()))
        .retry_config(RetryConfig::disabled())
        .load()
        .await;
    let sts_config = config.to_builder().endpoint_url(sts_endpoint).build();
    let identity = aws_sdk_sts::Client::new(&sts_config)
        .get_caller_identity()
        .send()
        .await?;
    let account = identity
        .account()
        .ok_or("STS did not identify the caller account")?;
    let config = config.to_builder().endpoint_url(&endpoint).build();

    let client = Client::new(&config);
    deltalake_aws::register_handlers(None);
    let lifecycle =
        CatalogLifecycle::new(GlueDataCatalog::with_config(&config).with_catalog_id(&catalog))
            .with_storage_options(HashMap::from([
                ("AWS_PROFILE".to_owned(), profile.clone()),
                ("AWS_REGION".to_owned(), region.clone()),
            ]));
    eprintln!(
        "Glue live test target {catalog}/{database}/{name}, region={region}, profile={profile}, aws-sdk-glue={sdk_revision}"
    );
    let RegistrationOutcome::Created(registered) =
        lifecycle.register_table(&identifier, location).await?
    else {
        return Err("registration could not establish ownership of the disposable record".into());
    };
    let scope = Scope {
        catalog: &catalog,
        database: &database,
        table: &name,
        location: registered.entry().location().as_str(),
        delta_id: registered.metadata().id(),
    };

    let (evidence_source, result) = match check {
        LiveCheck::Registration => (
            "aws-live-registration",
            verify_registration(&client, &scope, &lifecycle, &registered).await,
        ),
        LiveCheck::VersionId => ("aws-live", run_live_probe(&client, &scope).await),
    };
    let cleanup = delete_owned_table(&client, &scope).await;
    match (result, cleanup) {
        (Ok(observations), Ok(())) => {
            println!(
                "{}",
                json!({
                    "format_version": 1,
                    "evidence_source": evidence_source,
                    "profile": profile,
                    "account_id": account,
                    "catalog_id": catalog,
                    "database": database,
                    "region": region,
                    "endpoint": endpoint,
                    "aws_sdk_glue_version": sdk_revision,
                    "observations": observations,
                })
            );
            Ok(())
        }
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(probe), Err(cleanup)) => {
            Err(format!("{probe}. Cleanup also failed ({cleanup})").into())
        }
    }
}

async fn verify_registration(
    client: &Client,
    scope: &Scope<'_>,
    lifecycle: &CatalogLifecycle<GlueDataCatalog>,
    registered: &CatalogTable,
) -> ProbeResult<Value> {
    let before = get_native_delta_table(client, scope).await?;
    let entry = registered.entry();
    let RegistrationOutcome::AlreadyRegistered(repeated) = lifecycle
        .register_table(entry.identifier(), entry.location().clone())
        .await?
    else {
        return Err("repeated registration created a second binding".into());
    };
    let loaded = lifecycle.load_table(entry.identifier()).await?;
    for observed in [repeated, loaded] {
        if observed.entry() != entry
            || observed.metadata().id() != registered.metadata().id()
            || observed.version() != registered.version()
            || observed.schema() != registered.schema()
        {
            return Err("catalog lookup did not preserve the registered Delta snapshot".into());
        }
    }
    let after = get_native_delta_table(client, scope).await?;
    if version(&before)? != version(&after)? {
        return Err("registration retry or lookup changed the Glue table version".into());
    }
    Ok(json!({
        "delta_version": registered.version(),
        "glue_version_id": version(&after)?,
        "repeated_registration": "already_registered",
    }))
}

async fn run_live_probe(client: &Client, scope: &Scope<'_>) -> ProbeResult<Value> {
    let original = get_native_delta_table(client, scope).await?;
    let evidence = qualify_version_id(client, scope).await?;
    eprintln!(
        "Same-incarnation VersionIds {:?}, stale service code {}",
        evidence.versions, evidence.stale_error_code
    );

    let latest = get_native_delta_table(client, scope).await?;
    if version(&latest)? != evidence.versions[2] {
        return Err("disposable entry changed before the replacement probe".into());
    }

    delete_owned_table(client, scope).await?;
    client
        .create_table()
        .catalog_id(scope.catalog)
        .database_name(scope.database)
        .table_input(
            copy_native_input(&original)
                .parameters(UNRELATED, "replacement")
                .build()?,
        )
        .send()
        .await?;
    let replacement = get_native_delta_table(client, scope).await?;
    if replacement
        .parameters()
        .and_then(|parameters| parameters.get(UNRELATED))
        .map(String::as_str)
        != Some("replacement")
    {
        return Err("same-name fixture did not return the replacement marker".into());
    }
    let replacement_version = version(&replacement)?;
    if evidence
        .versions
        .iter()
        .any(|old| old == replacement_version)
    {
        return Err(format!("replacement reused VersionId {replacement_version}").into());
    }
    let code = qualify_replacement(client, scope, &latest, &replacement).await?;
    Ok(json!({
        "lake_formation_registered": original.is_registered_with_lake_formation(),
        "initial_version": evidence.versions[0],
        "advanced_version": evidence.versions[1],
        "compound_version": evidence.versions[2],
        "stale_error_code": evidence.stale_error_code,
        "replacement_version": replacement_version,
        "replacement_error_code": code,
    }))
}

async fn delete_owned_table(client: &Client, scope: &Scope<'_>) -> ProbeResult<()> {
    match get_native_delta_table(client, scope).await {
        Ok(_) => (),
        Err(error)
            if error
                .downcast_ref::<SdkError<GetTableError>>()
                .and_then(SdkError::as_service_error)
                .is_some_and(|cause| cause.is_entity_not_found_exception()) =>
        {
            return Ok(());
        }
        Err(error) => {
            return Err(
                format!("could not verify disposable entry before deleting: {error}").into(),
            );
        }
    }
    client
        .delete_table()
        .catalog_id(scope.catalog)
        .database_name(scope.database)
        .name(scope.table)
        .send()
        .await?;
    Ok(())
}

fn required(key: &str) -> ProbeResult<String> {
    let value = std::env::var(key).map_err(|_| format!("missing {key}"))?;
    if value.trim().is_empty() {
        return Err(format!("empty {key}").into());
    }
    Ok(value)
}

fn sdk_revision() -> ProbeResult<String> {
    let lock =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../Cargo.lock"))?;
    let mut versions = lock.split("[[package]]").filter_map(|entry| {
        let mut lines = entry.trim_start().lines();
        if lines.next()? != "name = \"aws-sdk-glue\"" {
            return None;
        }
        lines
            .next()?
            .strip_prefix("version = \"")?
            .strip_suffix('"')
    });
    let version = versions
        .next()
        .ok_or("Cargo.lock did not contain aws-sdk-glue")?;
    if versions.next().is_some() {
        return Err("Cargo.lock had multiple aws-sdk-glue resolutions".into());
    }
    Ok(version.to_owned())
}
