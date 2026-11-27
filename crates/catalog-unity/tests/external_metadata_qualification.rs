//! Wire qualification of Unity's external Delta metadata API.
//!
//! Run the default tests for HTTP request and response coverage. For a real local
//! server, set `UC_QUALIFICATION_URL` to its loopback origin and
//! `UC_QUALIFICATION_DATA_DIR` to the absolute mounted file root, then pass
//! `-- --ignored qualify_external_metadata_on_disposable_server`.
//! The explicit live test expects an existing catalog/schema (`unity/default`
//! by default), creates a disposable external table, then removes the entry.
//! For authorization testing, set `UC_QUALIFICATION_BEARER` to a test token and
//! bind the fixture root to an external location the token's principal can use.
//!
//! Observed profile: `unitycatalog-rs-server==0.0.2` with Delta protocol 1.0,
//! both with authorization disabled and with a signed local admin token. The
//! authenticated profile rejected anonymous reads and required an external
//! location for table creation. Java Unity and Databricks are separate profiles.

#[path = "support/external_metadata.rs"]
mod external_metadata;

use std::collections::HashMap;

use delta_kernel::schema::{DataType, StructField, StructType};
use external_metadata::{ExternalMetadataClient, Patch, QualificationError, TableName};
use httpmock::prelude::*;
use serde_json::{Value, json};
use uuid::Uuid;

const CONFIG: &str = "/api/2.1/unity-catalog/delta/v1/config";
const DELTA: &str = "/api/2.1/unity-catalog/delta/v1/catalogs/unity/schemas/default/tables/events";
const TABLES: &str = "/api/2.1/unity-catalog/tables/unity.default.events";
const ID: &str = "118eb852-9950-4c80-9be1-2ed294a632e9";
const ANOTHER_ID: &str = "118eb852-9950-4c80-9be1-2ed294a632e8";
const TOKEN: &str = "Bearer qualification-fixture";
const MARKER: &str = "delta-rs.projection-version";

fn table() -> TableName {
    TableName::new("unity", "default", "events").unwrap()
}

fn client(server: &MockServer) -> ExternalMetadataClient {
    ExternalMetadataClient::new(server.base_url().parse().unwrap(), Some(TOKEN)).unwrap()
}

fn config(version: &str, load: &str, update: &str) -> Value {
    json!({
        "protocol-version": version,
        "endpoints": [format!("GET {load}"), format!("POST {update}")],
    })
}

fn metadata(uuid: &str, etag: &str) -> Value {
    json!({"metadata": {
        "table-type": "EXTERNAL",
        "table-uuid": uuid,
        "etag": etag,
    }})
}

fn patch() -> Patch {
    let columns =
        StructType::try_new([StructField::new("order_id", DataType::LONG, false)]).unwrap();
    Patch::new(
        columns,
        Some("projected from the Delta log".to_owned()),
        HashMap::from([(MARKER.to_owned(), "7".to_owned())]),
    )
}

#[tokio::test]
async fn negotiates_and_verifies_one_guarded_projection_with_matching_tables_readback() {
    let server = MockServer::start_async().await;
    let endpoint = "/v1/catalogs/unity/schemas/{schema}/tables/{table}";
    let negotiation = server
        .mock_async(|when, then| {
            when.method(GET)
                .path(CONFIG)
                .query_param("catalog", "unity")
                .query_param("protocol-versions", "1.0")
                .header("authorization", TOKEN);
            then.status(200)
                .json_body(config("1.0", endpoint, endpoint));
        })
        .await;
    let load = server
        .mock_async(|when, then| {
            when.method(GET).path(DELTA).header("authorization", TOKEN);
            then.status(200).json_body(metadata(ID, "version-6"));
        })
        .await;
    let apply =
        server
            .mock_async(|when, then| {
                when.method(POST).path(DELTA).header("authorization", TOKEN)
                .json_body(json!({
                    "requirements": [
                        {"type": "assert-table-uuid", "uuid": ID},
                        {"type": "assert-etag", "etag": "version-6"}
                    ],
                    "updates": [
                        {"action": "set-columns", "columns": {"type": "struct", "fields": [
                            {"name": "order_id", "type": "long", "nullable": false, "metadata": {}}
                        ]}},
                        {"action": "set-table-comment", "comment": "projected from the Delta log"},
                        {"action": "set-properties", "updates": {MARKER: "7"}}
                    ]
                }));
                then.status(200).json_body(metadata(ID, "version-7"));
            })
            .await;
    let readback = server
        .mock_async(|when, then| {
            when.method(GET).path(TABLES).header("authorization", TOKEN);
            then.status(200).json_body(json!({
                "table_id": ID,
                "table_type": "EXTERNAL",
                "data_source_format": "DELTA",
                "comment": "projected from the Delta log",
                "properties": {MARKER: "7", "fixture-owned": "separate"},
                "columns": [{"name": "order_id", "type_text": "bigint", "nullable": false, "position": 0}],
            }));
        })
        .await;

    let session = client(&server).negotiate(table()).await.unwrap();
    let observed = session.load().await.unwrap();
    let reply = session.update(&observed, patch()).await.unwrap();
    assert_eq!(reply.id, Uuid::parse_str(ID).unwrap());
    assert_ne!(reply.etag, observed.etag);
    let verified = session.tables_readback().await.unwrap();
    assert_eq!(verified.id, reply.id);
    assert_eq!(
        verified.properties.get(MARKER).map(String::as_str),
        Some("7")
    );
    assert_eq!(verified.properties["fixture-owned"], "separate");
    assert_eq!(
        verified.comment.as_deref(),
        Some("projected from the Delta log")
    );
    assert_eq!(verified.columns[0].name, "order_id");
    assert_eq!(verified.columns[0].type_text.as_deref(), Some("bigint"));
    assert!(!verified.columns[0].nullable);
    negotiation.assert_calls_async(1).await;
    load.assert_calls_async(1).await;
    apply.assert_calls_async(1).await;
    readback.assert_calls_async(1).await;
}

#[tokio::test]
async fn rejects_unsupported_version_missing_update_and_off_origin_routes() {
    let server = MockServer::start_async().await;
    let negotiation = server
        .mock_async(|when, then| {
            when.method(GET).path(CONFIG).header("authorization", TOKEN);
            then.status(200).json_body(config("9.0", DELTA, DELTA));
        })
        .await;
    assert!(matches!(
        client(&server).negotiate(table()).await,
        Err(QualificationError::Unsupported(_))
    ));
    negotiation.delete_async().await;

    let negotiation = server
        .mock_async(|when, then| {
            when.method(GET).path(CONFIG);
            then.status(200).json_body(json!({
                "protocol-version": "1.0",
                "endpoints": ["GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}"],
            }));
        })
        .await;
    assert!(matches!(
        client(&server).negotiate(table()).await,
        Err(QualificationError::Unsupported(_))
    ));
    negotiation.delete_async().await;

    let outside = MockServer::start_async().await;
    let redirected = outside
        .mock_async(|when, then| {
            when.method(POST);
            then.status(200).json_body(metadata(ID, "outside"));
        })
        .await;
    let foreign = format!(
        "{}/delta/v1/catalogs/{{catalog}}/schemas/{{schema}}/tables/{{table}}",
        outside.base_url()
    );
    server
        .mock_async(|when, then| {
            when.method(GET).path(CONFIG);
            then.status(200).json_body(config(
                "1.0",
                "/delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}",
                &foreign,
            ));
        })
        .await;
    assert!(matches!(
        client(&server).negotiate(table()).await,
        Err(QualificationError::Unsupported(_))
    ));
    redirected.assert_calls_async(0).await;
}

#[test]
fn invalid_scoped_identifiers_cannot_change_endpoint_structure() {
    for invalid in [
        "",
        ".",
        "..",
        "other/path",
        "other\\path",
        "other.schema",
        "bad\nname",
    ] {
        assert!(
            TableName::new("unity", invalid, "events").is_err(),
            "{invalid:?}"
        );
    }
    assert!(TableName::new("unity", "文字", "events#1").is_ok());
}

#[tokio::test]
async fn rejects_stale_etag_and_wrong_uuid_without_replaying_the_mutation() {
    let server = MockServer::start_async().await;
    let endpoint = "/delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}";
    server
        .mock_async(|when, then| {
            when.method(GET).path(CONFIG);
            then.status(200)
                .json_body(config("1.0", endpoint, endpoint));
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(GET).path(DELTA);
            then.status(200).json_body(metadata(ID, "version-7"));
        })
        .await;
    let mut rejection_mocks = Vec::new();
    for (uuid, etag) in [(ID, "version-6"), (ANOTHER_ID, "version-7")] {
        rejection_mocks.push(server.mock_async(|when, then| {
            when.method(POST).path(DELTA).header("authorization", TOKEN)
                .json_body_includes(json!({"requirements": [
                    {"type": "assert-table-uuid", "uuid": uuid},
                    {"type": "assert-etag", "etag": etag},
                ]}).to_string());
            then.status(409).json_body(json!({
                "error": {"type": "UpdateRequirementConflictException", "message": "requirement failed", "code": 409},
            }));
        }).await);
    }
    let original_projection = server.mock_async(|when, then| {
        when.method(GET).path(TABLES).header("authorization", TOKEN);
        then.status(200).json_body(json!({
            "table_id": ID,
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "comment": "original",
            "properties": {MARKER: "6"},
            "columns": [{"name": "original", "type_text": "bigint", "nullable": false, "position": 0}],
        }));
    }).await;
    let session = client(&server).negotiate(table()).await.unwrap();
    let mut observed = session.load().await.unwrap();
    observed.etag = "version-6".to_owned();
    assert!(matches!(
        session.update(&observed, patch()).await,
        Err(QualificationError::RequirementConflict)
    ));
    let mut observed = session.load().await.unwrap();
    observed.id = Uuid::parse_str(ANOTHER_ID).unwrap();
    assert!(matches!(
        session.update(&observed, patch()).await,
        Err(QualificationError::RequirementConflict)
    ));
    let record = session.tables_readback().await.unwrap();
    assert_eq!(record.comment.as_deref(), Some("original"));
    assert_eq!(record.columns[0].name, "original");
    assert_eq!(record.properties[MARKER], "6");
    for rejected in rejection_mocks {
        rejected.assert_calls_async(1).await;
    }
    original_projection.assert_calls_async(1).await;
}

#[tokio::test]
async fn rejects_missing_etag_and_receives_auth_errors_without_a_write() {
    let server = MockServer::start_async().await;
    let endpoint = "/delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}";
    server
        .mock_async(|when, then| {
            when.method(GET).path(CONFIG).header("authorization", TOKEN);
            then.status(200)
                .json_body(config("1.0", endpoint, endpoint));
        })
        .await;
    let load = server
        .mock_async(|when, then| {
            when.method(GET).path(DELTA);
            then.status(200).json_body(metadata(ID, ""));
        })
        .await;
    let update = server
        .mock_async(|when, then| {
            when.method(POST).path(DELTA);
            then.status(200).json_body(metadata(ID, "version-7"));
        })
        .await;
    let session = client(&server).negotiate(table()).await.unwrap();
    assert!(matches!(
        session.load().await,
        Err(QualificationError::MissingIdentity)
    ));
    load.delete_async().await;
    server
        .mock_async(|when, then| {
            when.method(GET).path(DELTA);
            then.status(403);
        })
        .await;
    assert!(matches!(
        session.load().await,
        Err(QualificationError::Http(status)) if status.as_u16() == 403
    ));
    update.assert_calls_async(0).await;
}

#[tokio::test]
async fn scopes_the_delta_and_tables_routes_to_encoded_identifiers() {
    let server = MockServer::start_async().await;
    let endpoint = "/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}";
    server
        .mock_async(|when, then| {
            when.method(GET).path(CONFIG).query_param("catalog", "文字");
            then.status(200)
                .json_body(config("1.0", endpoint, endpoint));
        })
        .await;
    let delta = server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/api/2.1/unity-catalog/delta/v1/catalogs/%E6%96%87%E5%AD%97/schemas/default/tables/events%231");
            then.status(200).json_body(metadata(ID, "version-6"));
        })
        .await;
    let tables = server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/api/2.1/unity-catalog/tables/%E6%96%87%E5%AD%97.default.events%231");
            then.status(200).json_body(json!({
                "table_id": ID, "table_type": "EXTERNAL", "data_source_format": "DELTA",
                "columns": [], "properties": {},
            }));
        })
        .await;
    let session = client(&server)
        .negotiate(TableName::new("文字", "default", "events#1").unwrap())
        .await
        .unwrap();
    assert_eq!(
        session.load().await.unwrap().id,
        Uuid::parse_str(ID).unwrap()
    );
    assert_eq!(
        session.tables_readback().await.unwrap().id,
        Uuid::parse_str(ID).unwrap()
    );
    delta.assert_calls_async(1).await;
    tables.assert_calls_async(1).await;
}

#[tokio::test]
async fn does_not_forward_authorization_or_replay_a_redirected_post() {
    let server = MockServer::start_async().await;
    let outside = MockServer::start_async().await;
    let target = format!("{}/redirect", outside.base_url());
    let endpoint = "/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}";
    server
        .mock_async(|when, then| {
            when.method(GET).path(CONFIG);
            then.status(200)
                .json_body(config("1.0", endpoint, endpoint));
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(GET).path(DELTA);
            then.status(200).json_body(metadata(ID, "version-6"));
        })
        .await;
    let redirected = server
        .mock_async(|when, then| {
            when.method(POST).path(DELTA).header("authorization", TOKEN);
            then.status(307).header("location", target);
        })
        .await;
    let untrusted = outside
        .mock_async(|when, then| {
            when.path("/redirect");
            then.status(200);
        })
        .await;
    let session = client(&server).negotiate(table()).await.unwrap();
    let observed = session.load().await.unwrap();
    assert!(matches!(
        session.update(&observed, patch()).await,
        Err(QualificationError::Http(status)) if status.as_u16() == 307
    ));
    redirected.assert_calls_async(1).await;
    untrusted.assert_calls_async(0).await;
}

#[path = "support/external_metadata_live.rs"]
mod live;
