use aws_sdk_glue::Client;
use deltalake_core::data_catalog::CatalogResult;
use serde::Deserialize;

use super::GlueDataCatalog;
use super::registration::provider_error;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Report {
    format_version: u32,
    evidence_source: String,
    profile: String,
    account_id: String,
    catalog_id: String,
    database: String,
    region: String,
    endpoint: String,
    aws_sdk_glue_version: String,
    observations: Observations,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Observations {
    lake_formation_registered: bool,
    initial_version: String,
    advanced_version: String,
    compound_version: String,
    stale_error_code: String,
    replacement_version: String,
    replacement_error_code: String,
}

impl Observations {
    fn proves_guard(&self) -> bool {
        let versions = [
            &self.initial_version,
            &self.advanced_version,
            &self.compound_version,
            &self.replacement_version,
        ];
        versions
            .iter()
            .enumerate()
            .all(|(i, token)| !token.trim().is_empty() && !versions[..i].contains(token))
            && self.stale_error_code == "ConcurrentModificationException"
            && self.replacement_error_code == "ConcurrentModificationException"
    }
}

#[derive(Debug, thiserror::Error)]
enum QualificationError {
    #[error("Glue synchronization requires a successful live qualification report")]
    Report,
    #[error("Glue client scope differs from the qualified scope")]
    Scope,
    #[error("AWS caller account differs from the qualified account")]
    Account,
}

pub(super) struct QualifiedGlue {
    pub(super) client: Client,
    pub(super) lake_formation_registered: bool,
    catalog_id: String,
    database: String,
}

impl GlueDataCatalog {
    /// Enable guarded refresh using a trusted report emitted by the live Glue qualification test.
    ///
    /// Use the AWS principal from the reported profile. The report attests to
    /// stale and replaced-record rejection for the same catalog, database,
    /// regional endpoint, SDK release and Lake Formation context. This method
    /// checks the configured scope and verifies the caller account using STS.
    /// It does not authenticate the report or establish the principal's grants.
    /// Registration seeds a projection stamp within the qualified scope.
    /// Existing unstamped records need explicit adoption before refresh.
    ///
    /// ```no_run
    /// use deltalake_catalog_glue::GlueDataCatalog;
    /// use deltalake_core::data_catalog::{CatalogLifecycle, TableIdentifier};
    ///
    /// async fn refresh_after_delta_commit() -> Result<(), Box<dyn std::error::Error>> {
    ///     let report = std::fs::read_to_string("glue-qualification.json")?;
    ///     let client = GlueDataCatalog::from_env()
    ///         .await?
    ///         .with_catalog_id("123456789012")
    ///         .with_sync_qualification(&report)
    ///         .await?;
    ///     let catalog = CatalogLifecycle::new(client);
    ///     let id = TableIdentifier::new(vec!["analytics".into()], "orders")?;
    ///     // Publish the catalog projection after committing the Delta change.
    ///     catalog.sync_table_metadata(&id).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn with_sync_qualification(mut self, report_json: &str) -> CatalogResult<Self> {
        let report: Report = serde_json::from_str(report_json).map_err(provider_error)?;
        if report.format_version != 1
            || report.evidence_source != "aws-live"
            || report.profile.trim().is_empty()
            || report.database.trim().is_empty()
            || !valid_account_id(&report.account_id)
            || !valid_account_id(&report.catalog_id)
            || report.aws_sdk_glue_version != aws_sdk_glue::meta::PKG_VERSION
            || !report.observations.proves_guard()
        {
            return Err(provider_error(QualificationError::Report));
        }

        if !valid_region(&report.region)
            || self.sdk_config.region().map(|region| region.as_ref())
                != Some(report.region.as_str())
            || self.catalog_id.as_deref() != Some(report.catalog_id.as_str())
            || self.sdk_config.use_fips() == Some(true)
            || self.sdk_config.use_dual_stack() == Some(true)
        {
            return Err(provider_error(QualificationError::Scope));
        }
        let suffix = if report.region.starts_with("cn-") {
            "amazonaws.com.cn"
        } else {
            "amazonaws.com"
        };
        let endpoint = format!("https://glue.{}.{suffix}", report.region);
        if report.endpoint != endpoint
            || self
                .sdk_config
                .endpoint_url()
                .is_some_and(|configured| configured != endpoint)
        {
            return Err(provider_error(QualificationError::Scope));
        }

        let sts_config = aws_sdk_sts::config::Builder::from(&self.sdk_config)
            .endpoint_url(format!("https://sts.{}.{suffix}", report.region))
            .build();
        let identity = aws_sdk_sts::Client::from_conf(sts_config)
            .get_caller_identity()
            .send()
            .await
            .map_err(provider_error)?;
        if identity.account() != Some(report.account_id.as_str()) {
            return Err(provider_error(QualificationError::Account));
        }

        let glue_config = aws_sdk_glue::config::Builder::from(&self.sdk_config)
            .endpoint_url(endpoint)
            .build();
        self.qualified_sync = Some(QualifiedGlue {
            client: Client::from_conf(glue_config),
            lake_formation_registered: report.observations.lake_formation_registered,
            catalog_id: report.catalog_id,
            database: report.database,
        });
        Ok(self)
    }

    pub(super) fn qualified_for(&self, database: &str) -> Option<&QualifiedGlue> {
        self.qualified_sync.as_ref().filter(|qualified| {
            qualified.database == database
                && self.catalog_id.as_deref() == Some(qualified.catalog_id.as_str())
        })
    }

    pub(super) fn client_for_database(&self, database: &str) -> &Client {
        self.qualified_for(database)
            .map_or(&self.client, |qualified| &qualified.client)
    }
}

fn valid_account_id(value: &str) -> bool {
    value.len() == 12 && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn valid_region(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}
