use std::collections::HashMap;

use delta_kernel::schema::{DataType, StructType};
use deltalake_core::data_catalog::{
    CATALOG_PROJECTION_FORMAT_VERSION, CatalogLifecycleError, CatalogProjectionStamp,
    CatalogResult, CatalogRevision, CatalogSyncProjection, CatalogSyncState, CatalogSyncUpdate,
    CatalogSyncWriteOutcome, CatalogSyncWriter, CatalogTable, TableIdentifier, TableRegistration,
};
use reqwest::header::{ETAG, HeaderMap};
use reqwest::{StatusCode, Url};
use sha2::{Digest, Sha256};
use url::Host;
use uuid::Uuid;

use crate::UnityCatalog;
use crate::external_metadata::{ExternalMetadataClient, MetadataSession, Observation, Patch};
use crate::external_metadata::{QualificationError, TableName};
use crate::models::{ColumnInfo, Table};
use crate::registration::{DELTA_ID_PROPERTY, names, parse_external_table, provider_error};

const FORMAT: &str = "delta-rs.sync-format";
const VERSION: &str = "delta-rs.sync-version";
const DIGEST: &str = "delta-rs.sync-digest";

/// Unity external Delta metadata protocols selectable for negotiation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum UnitySyncProfile {
    /// The OSS Rust Unity Catalog server 0.0.2 implementing Delta REST protocol 1.0.
    ///
    /// Guarded writes require the deployment to expose a shared strong validator
    /// on General Tables and enforce it on all writes. Stock 0.0.2 omits this
    /// validator. The projection covers table descriptions and unpartitioned
    /// `bigint` and `string` columns without custom field metadata.
    OssRustServer002,
}

impl UnityCatalog {
    /// Enable guarded synchronization for an operator-confirmed Unity deployment.
    ///
    /// The deployment must match the selected server profile. The adapter negotiates
    /// the Delta API protocol and verifies the table binding before using it.
    /// Use an HTTPS origin in production. Plain HTTP is allowed for numeric loopback
    /// origins used with disposable servers.
    pub fn with_sync_profile(mut self, profile: UnitySyncProfile) -> CatalogResult<Self> {
        let origin = Url::parse(&self.workspace_url).map_err(provider_error)?;
        let loopback = match origin.host() {
            Some(Host::Ipv4(addr)) => addr.is_loopback(),
            Some(Host::Ipv6(addr)) => addr.is_loopback(),
            _ => false,
        };
        let trusted = origin.host().is_some()
            && (origin.scheme() == "https" || (origin.scheme() == "http" && loopback))
            && origin.username().is_empty()
            && origin.password().is_none()
            && origin.path() == "/"
            && origin.query().is_none()
            && origin.fragment().is_none();
        if !trusted {
            return Err(provider_error(
                "Unity guarded sync requires an HTTPS or HTTP loopback origin",
            ));
        }

        match profile {
            UnitySyncProfile::OssRustServer002 => self.sync_origin = Some(origin),
        }
        Ok(self)
    }

    /// Whether this client was configured to negotiate guarded sync.
    ///
    /// Individual tables must also support the negotiated protocol and projection.
    pub fn supports_guarded_sync(&self) -> bool {
        self.sync_origin.is_some()
    }

    async fn metadata_session(&self, id: &TableIdentifier) -> CatalogResult<MetadataSession> {
        let origin = self.sync_origin.clone().ok_or_else(|| unsupported(id))?;
        let [catalog, schema, name] = names(id)?;
        let table = TableName::new(catalog, schema, name).map_err(provider_error)?;
        let token = self.get_credential().await.map_err(provider_error)?;
        ExternalMetadataClient::from_catalog(origin, self.create_client.clone(), token)
            .negotiate(table)
            .await
            .map_err(|error| read_error(id, error))
    }

    pub(super) async fn seed_sync_stamp(
        &self,
        registration: &TableRegistration,
        properties: &mut HashMap<String, String>,
    ) -> CatalogResult<()> {
        if !self.supports_guarded_sync() {
            return Ok(());
        }
        let Some(projection) = UnityMetadataProjection::from_delta(
            registration.schema(),
            registration.metadata().partition_columns(),
            registration.metadata().description(),
        ) else {
            return Ok(());
        };
        match self.metadata_session(registration.identifier()).await {
            Ok(_) => {}
            Err(CatalogLifecycleError::Unsupported { .. }) => return Ok(()),
            Err(error) => return Err(error),
        }
        let stamp = CatalogProjectionStamp::new(
            CATALOG_PROJECTION_FORMAT_VERSION,
            registration.metadata().id(),
            registration.version(),
            projection.digest()?,
        )?;
        properties.extend(stamp_properties(&stamp));
        Ok(())
    }
}

/// Fresh, scoped REST observation used for one guarded Unity mutation.
#[doc(hidden)]
pub struct UnitySyncRecord {
    session: MetadataSession,
    observation: Observation,
    fields: ProjectedFields,
}

/// Metadata fields owned by the Unity guarded sync profile.
#[doc(hidden)]
pub struct UnityMetadataProjection {
    schema: StructType,
    fields: ProjectedFields,
}

#[derive(PartialEq, Eq, serde::Serialize)]
struct ProjectedFields {
    comment: Option<String>,
    columns: Vec<(String, &'static str, bool)>,
}

impl UnityMetadataProjection {
    fn from_delta(
        schema: &StructType,
        partitions: &[String],
        comment: Option<&str>,
    ) -> Option<Self> {
        if !partitions.is_empty() {
            return None;
        }
        let columns = schema
            .fields()
            .map(|field| {
                if !field.metadata().is_empty() {
                    return None;
                }
                let data_type = match field.data_type() {
                    &DataType::LONG => "bigint",
                    &DataType::STRING => "string",
                    _ => return None,
                };
                Some((field.name().to_owned(), data_type, field.is_nullable()))
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self {
            schema: schema.clone(),
            fields: ProjectedFields {
                comment: comment
                    .filter(|comment| !comment.is_empty())
                    .map(str::to_owned),
                columns,
            },
        })
    }

    fn digest(&self) -> CatalogResult<String> {
        self.fields.digest()
    }
}

impl ProjectedFields {
    fn from_table(table: &Table) -> Option<Self> {
        fn column(position: usize, column: &ColumnInfo) -> Option<(String, &'static str, bool)> {
            if column.position as usize != position
                || column.partition_index.is_some()
                || column
                    .comment
                    .as_ref()
                    .is_some_and(|comment| !comment.is_empty())
            {
                return None;
            }
            let data_type = match column.type_text.as_deref() {
                Some("bigint") => "bigint",
                Some("string") => "string",
                _ => return None,
            };
            Some((column.name.clone(), data_type, column.nullable))
        }

        let columns = table
            .columns
            .iter()
            .enumerate()
            .map(|(position, field)| column(position, field))
            .collect::<Option<Vec<_>>>()?;
        let comment = table.comment.clone().filter(|value| !value.is_empty());
        Some(Self { comment, columns })
    }

    fn digest(&self) -> CatalogResult<String> {
        let bytes = serde_json::to_vec(self).map_err(provider_error)?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

fn stamp_properties(stamp: &CatalogProjectionStamp) -> HashMap<String, String> {
    HashMap::from([
        (DELTA_ID_PROPERTY.into(), stamp.delta_id().into()),
        (FORMAT.into(), stamp.format_version().to_string()),
        (VERSION.into(), stamp.delta_version().to_string()),
        (DIGEST.into(), stamp.digest().into()),
    ])
}

fn read_stamp(id: &TableIdentifier, table: &Table) -> CatalogResult<CatalogProjectionStamp> {
    let properties = &table.properties;
    if [FORMAT, VERSION, DIGEST]
        .iter()
        .all(|key| !properties.contains_key(*key))
    {
        return Err(unsupported(id));
    }
    let required = |key| {
        properties
            .get(key)
            .ok_or(CatalogLifecycleError::InvalidSyncState)
    };
    CatalogProjectionStamp::new(
        required(FORMAT)?
            .parse()
            .map_err(|_| CatalogLifecycleError::InvalidSyncState)?,
        required(DELTA_ID_PROPERTY)?,
        required(VERSION)?
            .parse()
            .map_err(|_| CatalogLifecycleError::InvalidSyncState)?,
        required(DIGEST)?,
    )
}

/// Only a single strong validator identifies the exact General Tables representation
/// used to prepare or confirm a mutation (RFC 9110, section 8.8.3).
fn strong_table_etag(headers: &HeaderMap) -> Option<&[u8]> {
    let mut tags = headers.get_all(ETAG).iter();
    let raw = tags.next()?.as_bytes();
    let opaque = raw.strip_prefix(b"\"")?.strip_suffix(b"\"")?;
    (tags.next().is_none()
        && opaque
            .iter()
            .all(|&c| c == b'!' || (0x23..=0x7e).contains(&c) || c >= 0x80))
    .then_some(opaque)
}

#[async_trait::async_trait]
impl CatalogSyncWriter for UnityCatalog {
    type Record = UnitySyncRecord;
    type Projection = UnityMetadataProjection;

    async fn get_sync_state(
        &self,
        id: &TableIdentifier,
    ) -> CatalogResult<Option<CatalogSyncState<Self::Record>>> {
        let session = self.metadata_session(id).await?;
        let observation = session.load().await;
        let response = session.tables_response().await.map_err(provider_error)?;
        let tables_etag = strong_table_etag(response.headers()).map(<[u8]>::to_vec);
        let Some((table, entry)) = parse_external_table(id, response).await? else {
            return Ok(None);
        };
        let observation = observation.map_err(|error| read_error(id, error))?;
        let tables_etag = tables_etag.ok_or_else(|| unsupported(id))?;
        if tables_etag != observation.etag.as_bytes()
            || Uuid::parse_str(&table.table_id) != Ok(observation.id)
        {
            return Err(CatalogLifecycleError::Conflict { table: id.clone() });
        }
        let fields = ProjectedFields::from_table(&table).ok_or_else(|| unsupported(id))?;
        let stamp = read_stamp(id, &table)?;
        let revision = CatalogRevision::new(&observation.etag)?;
        CatalogSyncState::new(
            entry,
            revision,
            stamp,
            fields.digest()?,
            UnitySyncRecord {
                session,
                observation,
                fields,
            },
        )
        .map(Some)
    }

    fn project_table(
        &self,
        table: &CatalogTable,
    ) -> CatalogResult<CatalogSyncProjection<Self::Projection>> {
        let projection = UnityMetadataProjection::from_delta(
            &table.schema(),
            table.metadata().partition_columns(),
            table.metadata().description(),
        )
        .ok_or_else(|| unsupported(table.entry().identifier()))?;
        CatalogSyncProjection::new(projection.digest()?, projection)
    }

    async fn update_table_metadata(
        &self,
        update: CatalogSyncUpdate<'_, Self::Record, Self::Projection>,
    ) -> CatalogResult<CatalogSyncWriteOutcome> {
        let id = update.state().entry().identifier();
        let record = update.state().record();
        let next = update.projection();
        let columns = (record.fields.columns != next.fields.columns).then(|| next.schema.clone());
        // The qualified API represents a removed description with an empty string.
        let comment = (record.fields.comment != next.fields.comment)
            .then(|| next.fields.comment.clone().unwrap_or_default());
        let patch = Patch::from_changes(columns, comment, stamp_properties(update.stamp()));
        let result = record.session.update(&record.observation, patch).await;
        let [catalog, schema, name] = names(id)?;
        self.invalidate_table(&format!("{catalog}.{schema}.{name}"));

        match result {
            Ok(_) => Ok(CatalogSyncWriteOutcome::Applied),
            Err(QualificationError::RequirementConflict) => Ok(CatalogSyncWriteOutcome::Rejected),
            Err(error @ QualificationError::Http(status))
                if matches!(
                    status,
                    StatusCode::BAD_REQUEST
                        | StatusCode::UNAUTHORIZED
                        | StatusCode::FORBIDDEN
                        | StatusCode::NOT_FOUND
                        | StatusCode::METHOD_NOT_ALLOWED
                        | StatusCode::UNSUPPORTED_MEDIA_TYPE
                        | StatusCode::UNPROCESSABLE_ENTITY
                ) =>
            {
                Err(provider_error(error))
            }
            Err(error) => Err(CatalogLifecycleError::OutcomeUnknown {
                table: id.clone(),
                attempted_stamp: Some(Box::new(update.stamp().clone())),
                source: Box::new(error),
            }),
        }
    }
}

fn read_error(id: &TableIdentifier, error: QualificationError) -> CatalogLifecycleError {
    match error {
        QualificationError::Unsupported(_) => unsupported(id),
        QualificationError::MissingIdentity => CatalogLifecycleError::InvalidSyncState,
        error => provider_error(error),
    }
}

fn unsupported(id: &TableIdentifier) -> CatalogLifecycleError {
    CatalogLifecycleError::Unsupported { table: id.clone() }
}
