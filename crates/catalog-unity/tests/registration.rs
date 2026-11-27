//! Registration through the public catalog lifecycle API.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use deltalake_catalog_unity::UnityCatalogBuilder;
use deltalake_catalog_unity::models::GetTableResponse;
use deltalake_core::data_catalog::{
    CatalogLifecycle, CatalogLifecycleError, RegistrationOutcome, TableIdentifier,
};
use deltalake_test::utils::TestTables;
use httpmock::prelude::*;
use reqwest::Url;
use serde_json::{Value, json};

const TABLES: &str = "/api/2.1/unity-catalog/tables";
const TABLE: &str = "/api/2.1/unity-catalog/tables/unity.default.partitioned";
const CREDENTIALS: &str = "/api/2.1/unity-catalog/temporary-table-credentials";
const TABLE_ID: &str = "118eb852-9950-4c80-9be1-2ed294a632e9";

fn identifier() -> TableIdentifier {
    TableIdentifier::new(
        vec!["unity".to_owned(), "default".to_owned()],
        "partitioned",
    )
    .unwrap()
}

fn table_body(location: &Url, table_id: &str) -> Value {
    json!({
        "name": "partitioned",
        "catalog_name": "unity",
        "schema_name": "default",
        "table_type": "EXTERNAL",
        "data_source_format": "DELTA",
        "storage_location": location.as_str(),
        "columns": [],
        "properties": {},
        "created_at": 0,
        "updated_at": 0,
        "table_id": table_id,
    })
}

#[tokio::test]
async fn registration_checks_fresh_binding_after_ambiguous_create() {
    let server = MockServer::start_async().await;
    let path = TestTables::Delta0_8_0Partitioned.as_path();
    let location = Url::from_directory_path(&path).unwrap();
    let log = path.join("_delta_log/00000000000000000000.json");
    let original = std::fs::read(&log).unwrap();
    let dispatched = Arc::new(AtomicBool::new(false));

    let missing_state = dispatched.clone();
    let missing = server
        .mock_async(|when, then| {
            when.method(GET)
                .path(TABLE)
                .header("authorization", "Bearer fixture-token")
                .is_true(move |_| !missing_state.load(Ordering::SeqCst));
            then.status(404).json_body(json!({
                "error_code": "TABLE_NOT_FOUND",
                "message": "Table does not exist",
            }));
        })
        .await;

    let create_state = dispatched.clone();
    let expected_location = location.clone();
    let create = server
        .mock_async(|when, then| {
            when.method(POST)
                .path(TABLES)
                .header("authorization", "Bearer fixture-token")
                .json_body_includes(
                    json!({
                        "name": "partitioned",
                        "catalog_name": "unity",
                        "schema_name": "default",
                        "table_type": "EXTERNAL",
                        "data_source_format": "DELTA",
                    })
                    .to_string(),
                )
                .is_true(move |request| {
                    let Ok(body) = serde_json::from_slice::<Value>(request.body_ref()) else {
                        return false;
                    };
                    let c = &body["columns"][1];
                    if body["storage_location"]
                        .as_str()
                        .map(|url| url.trim_end_matches('/'))
                        != Some(expected_location.as_str().trim_end_matches('/'))
                        || c["name"] != "year"
                        || c["position"] != 1
                        || c["partition_index"] != 0
                        || c["type_name"] != "STRING"
                        || c["type_text"] != "string"
                        || c["nullable"] != true
                    {
                        return false;
                    }
                    let field = c["type_json"]
                        .as_str()
                        .and_then(|value| serde_json::from_str::<Value>(value).ok());
                    if field
                        != Some(
                            json!({"name":"year","type":"string","nullable":true,"metadata":{}}),
                        )
                    {
                        return false;
                    }
                    create_state.store(true, Ordering::SeqCst);
                    true
                });
            then.status(503);
        })
        .await;

    let present_state = dispatched.clone();
    let present = server
        .mock_async(|when, then| {
            when.method(GET)
                .path(TABLE)
                .header("authorization", "Bearer fixture-token")
                .is_true(move |_| present_state.load(Ordering::SeqCst));
            then.status(200).json_body(table_body(&location, TABLE_ID));
        })
        .await;
    let credentials = server
        .mock_async(|when, then| {
            when.method(POST)
                .path(CREDENTIALS)
                .header("authorization", "Bearer fixture-token")
                .json_body_includes(json!({"table_id": TABLE_ID}).to_string());
            then.status(200)
                .json_body(json!({"url": location.as_str()}));
        })
        .await;

    let client = UnityCatalogBuilder::builder()
        .workspace_url(server.base_url())
        .bearer_token("fixture-token")
        .allow_http_url(true)
        .build()
        .build()
        .unwrap();

    assert!(matches!(
        client
            .get_table("unity", "default", "partitioned")
            .await
            .unwrap(),
        GetTableResponse::Error(_)
    ));
    let lifecycle = CatalogLifecycle::new(client);
    let outcome = lifecycle
        .register_table(&identifier(), location)
        .await
        .unwrap();
    assert!(matches!(outcome, RegistrationOutcome::AlreadyRegistered(_)));
    missing.assert_calls_async(2).await;
    create.assert_calls_async(1).await;
    assert!(present.calls_async().await >= 1);
    assert!(credentials.calls_async().await >= 1);
    assert_eq!(original, std::fs::read(log).unwrap());
}

#[tokio::test]
async fn rejects_rebound_table_before_vending_credentials() {
    let server = MockServer::start_async().await;
    let location = Url::from_directory_path(TestTables::Delta0_8_0Partitioned.as_path()).unwrap();
    let lookup_count = Arc::new(AtomicUsize::new(0));
    let first_lookup = lookup_count.clone();
    let first = server
        .mock_async(|when, then| {
            when.method(GET)
                .path(TABLE)
                .is_true(move |_| first_lookup.fetch_add(1, Ordering::SeqCst) == 0);
            then.status(200).json_body(table_body(&location, TABLE_ID));
        })
        .await;
    let rebound = server
        .mock_async(|when, then| {
            when.method(GET).path(TABLE);
            then.status(200).json_body(table_body(
                &location,
                "a99c5e89-41b3-4deb-9f75-3fb5103b4fc1",
            ));
        })
        .await;
    let credentials = server
        .mock_async(|when, then| {
            when.method(POST).path(CREDENTIALS);
            then.status(200)
                .json_body(json!({"url": location.as_str()}));
        })
        .await;

    let client = UnityCatalogBuilder::builder()
        .workspace_url(server.base_url())
        .bearer_token("fixture-token")
        .allow_http_url(true)
        .build()
        .build()
        .unwrap();
    let result = CatalogLifecycle::new(client)
        .load_table(&identifier())
        .await;
    assert!(matches!(
        result,
        Err(CatalogLifecycleError::Conflict { .. })
    ));
    first.assert_calls_async(1).await;
    rebound.assert_calls_async(1).await;
    credentials.assert_calls_async(0).await;
}
