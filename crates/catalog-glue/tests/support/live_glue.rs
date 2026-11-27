use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use aws_config::{BehaviorVersion, SdkConfig, profile::ProfileFileCredentialsProvider};
use aws_sdk_glue::Client;
use aws_sdk_glue::config::{Region, retry::RetryConfig};
use aws_sdk_glue::error::SdkError;
use aws_sdk_glue::operation::get_table::GetTableError;
use aws_sdk_glue::types::{Table, TableInput, builders::TableInputBuilder};
use url::Url;

pub(crate) type ProbeResult<T> = Result<T, Box<dyn std::error::Error>>;

pub(crate) struct LiveTarget {
    pub(crate) config: SdkConfig,
    pub(crate) catalog: String,
    pub(crate) database: String,
    pub(crate) name: String,
    pub(crate) location: Url,
    pub(crate) profile: String,
    pub(crate) region: String,
    pub(crate) endpoint: String,
    pub(crate) account: String,
}

impl LiveTarget {
    pub(crate) async fn load() -> ProbeResult<Self> {
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
        if location.scheme() != "s3" || location.host_str().is_none() {
            return Err("DELTA_RS_GLUE_CAS_DELTA_URI must identify an S3 Delta table".into());
        }
        let name = format!(
            "delta_rs_cas_{}_{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        );
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
            .ok_or("STS did not identify the caller account")?
            .to_owned();
        let config = config.to_builder().endpoint_url(&endpoint).build();

        deltalake_aws::register_handlers(None);
        Ok(Self {
            config,
            catalog,
            database,
            name,
            location,
            profile,
            region,
            endpoint,
            account,
        })
    }

    pub(crate) fn storage_options(&self) -> HashMap<String, String> {
        HashMap::from([
            ("AWS_PROFILE".into(), self.profile.clone()),
            ("AWS_REGION".into(), self.region.clone()),
        ])
    }
}

pub(crate) struct Scope<'a> {
    pub(crate) catalog: &'a str,
    pub(crate) database: &'a str,
    pub(crate) table: &'a str,
    pub(crate) location: &'a str,
    pub(crate) delta_id: &'a str,
}

pub(crate) async fn get_native_delta_table(
    client: &Client,
    scope: &Scope<'_>,
) -> ProbeResult<Table> {
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

pub(crate) fn version(table: &Table) -> ProbeResult<&str> {
    table
        .version_id()
        .filter(|id| !id.trim().is_empty())
        .ok_or_else(|| "GetTable omitted an observed VersionId".into())
}

pub(crate) async fn delete_owned_table(
    client: &Client,
    scope: &Scope<'_>,
    marker: Option<(&str, &str)>,
) -> ProbeResult<()> {
    match get_native_delta_table(client, scope).await {
        Ok(table) => {
            if let Some((key, value)) = marker
                && table
                    .parameters()
                    .and_then(|p| p.get(key))
                    .map(String::as_str)
                    != Some(value)
            {
                return Err(
                    "refusing to delete an entry without the fixture's ownership marker".into(),
                );
            }
        }
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

pub(crate) fn copy_native_input(table: &Table) -> TableInputBuilder {
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
