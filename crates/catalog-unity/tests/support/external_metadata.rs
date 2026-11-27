use std::collections::HashMap;

use delta_kernel::schema::StructType;
use reqwest::header::{AUTHORIZATION, HeaderValue};
use reqwest::{Client, Method, RequestBuilder, Response, StatusCode, Url, redirect, retry};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const VERSION: &str = "1.0";
const BASE: &[&str] = &["api", "2.1", "unity-catalog"];

#[derive(Debug, thiserror::Error)]
pub(super) enum QualificationError {
    #[error("invalid Unity qualification origin, authorization header, or table name")]
    InvalidTarget,
    #[error("Unity Delta metadata endpoint is unsupported: {0}")]
    Unsupported(&'static str),
    #[error("expected an external Delta table with a UUID and ETag")]
    MissingIdentity,
    #[error("Unity Delta rejected a table UUID or ETag requirement")]
    RequirementConflict,
    #[error("Unity returned HTTP {0}")]
    Http(StatusCode),
    #[error("Unity request or response failed")]
    Request(#[from] reqwest::Error),
}

#[derive(Clone)]
pub(super) struct ExternalMetadataClient {
    origin: Url,
    client: Client,
    authorization: Option<HeaderValue>,
}

impl ExternalMetadataClient {
    pub(super) fn new(
        origin: Url,
        authorization: Option<&str>,
    ) -> Result<Self, QualificationError> {
        if !matches!(origin.scheme(), "http" | "https")
            || origin.host().is_none()
            || !origin.username().is_empty()
            || origin.password().is_some()
            || origin.path() != "/"
            || origin.query().is_some()
            || origin.fragment().is_some()
        {
            return Err(QualificationError::InvalidTarget);
        }
        let authorization = authorization
            .map(|text| {
                let mut value =
                    HeaderValue::from_str(text).map_err(|_| QualificationError::InvalidTarget)?;
                value.set_sensitive(true);
                Ok::<_, QualificationError>(value)
            })
            .transpose()?;
        let client = Client::builder()
            .no_proxy()
            .redirect(redirect::Policy::none())
            .retry(retry::never())
            .build()?;
        Ok(Self {
            origin,
            client,
            authorization,
        })
    }

    pub(super) async fn negotiate(
        &self,
        table: TableName,
    ) -> Result<MetadataSession, QualificationError> {
        let mut url = self.endpoint(&["delta", "v1", "config"]);
        url.query_pairs_mut()
            .append_pair("catalog", &table.catalog)
            .append_pair("protocol-versions", VERSION);
        let response = self.authorize(self.client.get(url)).send().await?;
        let status = response.status();
        if matches!(
            status,
            StatusCode::BAD_REQUEST | StatusCode::NOT_FOUND | StatusCode::NOT_IMPLEMENTED
        ) {
            return Err(QualificationError::Unsupported("config unavailable"));
        }
        let config: Config = success(response)?.json().await?;
        if config.protocol_version != VERSION {
            return Err(QualificationError::Unsupported("protocol version"));
        }

        // Accept the known v1 lookup and update templates. Other advertised routes
        // cannot redirect these requests or receive the authorization header.
        for method in ["GET", "POST"] {
            if !config
                .endpoints
                .iter()
                .any(|endpoint| table.matches_template(endpoint, method))
            {
                return Err(QualificationError::Unsupported("table endpoint template"));
            }
        }
        let delta = self.endpoint(&[
            "delta",
            "v1",
            "catalogs",
            &table.catalog,
            "schemas",
            &table.schema,
            "tables",
            &table.name,
        ]);
        let tables = self.endpoint(&["tables", &table.full_name()]);
        Ok(MetadataSession {
            client: self.clone(),
            delta,
            tables,
        })
    }

    pub(super) fn tables_request(
        &self,
        method: Method,
        table: Option<&TableName>,
    ) -> RequestBuilder {
        let url = match table {
            Some(table) => self.endpoint(&["tables", &table.full_name()]),
            None => self.endpoint(&["tables"]),
        };
        self.authorize(self.client.request(method, url))
    }

    fn authorize(&self, request: RequestBuilder) -> RequestBuilder {
        match self.authorization.clone() {
            Some(header) => request.header(AUTHORIZATION, header),
            None => request,
        }
    }

    fn endpoint(&self, suffix: &[&str]) -> Url {
        let mut url = self.origin.clone();
        url.path_segments_mut()
            .expect("validated hierarchical HTTP origin")
            .pop_if_empty()
            .extend(BASE)
            .extend(suffix);
        url
    }
}

#[derive(Clone)]
pub(super) struct TableName {
    pub(super) catalog: String,
    pub(super) schema: String,
    pub(super) name: String,
}

impl TableName {
    pub(super) fn new(catalog: &str, schema: &str, name: &str) -> Result<Self, QualificationError> {
        if [catalog, schema, name].iter().any(|s| {
            s.is_empty() || s.contains(['.', '/', '\\']) || s.chars().any(char::is_control)
        }) {
            return Err(QualificationError::InvalidTarget);
        }
        Ok(Self {
            catalog: catalog.to_owned(),
            schema: schema.to_owned(),
            name: name.to_owned(),
        })
    }

    fn matches_template(&self, endpoint: &str, method: &str) -> bool {
        let Some((verb, path)) = endpoint.split_once(' ') else {
            return false;
        };
        if verb != method {
            return false;
        }
        let path = path
            .strip_prefix("/api/2.1/unity-catalog/delta")
            .or_else(|| path.strip_prefix("/delta"))
            .unwrap_or(path);
        let Some(path) = path
            .strip_prefix("/v1/catalogs/")
            .and_then(|path| path.strip_suffix("/schemas/{schema}/tables/{table}"))
        else {
            return false;
        };
        path == "{catalog}" || path == self.catalog
    }

    pub(super) fn full_name(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.schema, self.name)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Config {
    protocol_version: String,
    endpoints: Vec<String>,
}

pub(super) struct MetadataSession {
    client: ExternalMetadataClient,
    delta: Url,
    tables: Url,
}

impl MetadataSession {
    pub(super) async fn load(&self) -> Result<Observation, QualificationError> {
        let response = self
            .client
            .authorize(self.client.client.get(self.delta.clone()))
            .send()
            .await?;
        Observation::read(response, &self.delta).await
    }

    pub(super) async fn update(
        &self,
        observed: &Observation,
        patch: Patch,
    ) -> Result<Observation, QualificationError> {
        if observed.table != self.delta {
            return Err(QualificationError::MissingIdentity);
        }
        let response = self
            .client
            .authorize(self.client.client.post(self.delta.clone()))
            .json(&UpdateRequest {
                requirements: [
                    Requirement::AssertTableUuid { uuid: observed.id },
                    Requirement::AssertEtag {
                        etag: &observed.etag,
                    },
                ],
                updates: patch.updates,
            })
            .send()
            .await?;

        if response.status() == StatusCode::CONFLICT {
            let error = response.json::<ErrorResponse>().await?;
            return if error.error.kind == "UpdateRequirementConflictException" {
                Err(QualificationError::RequirementConflict)
            } else {
                Err(QualificationError::Http(StatusCode::CONFLICT))
            };
        }

        let updated = Observation::read(response, &self.delta).await?;
        if updated.id != observed.id {
            return Err(QualificationError::MissingIdentity);
        }
        Ok(updated)
    }

    pub(super) async fn tables_readback(&self) -> Result<TablesRecord, QualificationError> {
        let response = self
            .client
            .authorize(self.client.client.get(self.tables.clone()))
            .send()
            .await?;
        let table: TablesRecord = success(response)?.json().await?;
        if table.table_type != "EXTERNAL" || table.data_source_format != "DELTA" {
            return Err(QualificationError::MissingIdentity);
        }
        Ok(table)
    }
}

fn success(response: Response) -> Result<Response, QualificationError> {
    let status = response.status();
    if status.is_success() {
        Ok(response)
    } else {
        Err(QualificationError::Http(status))
    }
}

pub(super) struct Observation {
    table: Url,
    pub(super) id: Uuid,
    pub(super) etag: String,
}

impl Observation {
    async fn read(response: Response, table: &Url) -> Result<Self, QualificationError> {
        let response: MetadataResponse = success(response)?.json().await?;
        let metadata = response.metadata;
        let id = metadata
            .table_uuid
            .filter(|id| !id.is_nil())
            .ok_or(QualificationError::MissingIdentity)?;
        let etag = metadata
            .etag
            .filter(|etag| !etag.trim().is_empty())
            .ok_or(QualificationError::MissingIdentity)?;
        if metadata.table_type != "EXTERNAL" {
            return Err(QualificationError::MissingIdentity);
        }
        Ok(Self {
            table: table.clone(),
            id,
            etag,
        })
    }
}

#[derive(Deserialize)]
struct MetadataResponse {
    metadata: MetadataIdentity,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct MetadataIdentity {
    table_type: String,
    table_uuid: Option<Uuid>,
    etag: Option<String>,
}

#[derive(Deserialize)]
struct ErrorResponse {
    error: ApiError,
}

#[derive(Deserialize)]
struct ApiError {
    #[serde(rename = "type")]
    kind: String,
}

#[derive(Deserialize)]
pub(super) struct TablesRecord {
    #[serde(rename = "table_id")]
    pub(super) id: Uuid,
    table_type: String,
    data_source_format: String,
    pub(super) comment: Option<String>,
    pub(super) columns: Vec<deltalake_catalog_unity::models::ColumnInfo>,
    pub(super) properties: HashMap<String, String>,
}

pub(super) struct Patch {
    updates: [Update; 3],
}

impl Patch {
    pub(super) fn new(
        columns: StructType,
        comment: Option<String>,
        properties: HashMap<String, String>,
    ) -> Self {
        Self {
            updates: [
                Update::Columns { columns },
                Update::TableComment { comment },
                Update::Properties {
                    updates: properties,
                },
            ],
        }
    }
}

#[derive(Serialize)]
struct UpdateRequest<'a> {
    requirements: [Requirement<'a>; 2],
    updates: [Update; 3],
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum Requirement<'a> {
    AssertTableUuid { uuid: Uuid },
    AssertEtag { etag: &'a str },
}

#[derive(Serialize)]
#[serde(tag = "action")]
enum Update {
    #[serde(rename = "set-columns")]
    Columns { columns: StructType },
    #[serde(rename = "set-table-comment")]
    TableComment { comment: Option<String> },
    #[serde(rename = "set-properties")]
    Properties { updates: HashMap<String, String> },
}
