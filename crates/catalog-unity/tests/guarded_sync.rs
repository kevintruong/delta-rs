//! Exercise guarded synchronization through the public lifecycle API.

use std::error::Error;
use std::path::Path;

use delta_kernel::schema::{DataType, StructField};
use deltalake_catalog_unity::{UnityCatalogBuilder, UnitySyncProfile};
use deltalake_core::data_catalog::{
    CatalogLifecycle, CatalogLifecycleError, CatalogSyncOutcome, TableIdentifier,
};
use deltalake_core::operations::create::CreateBuilder;
use deltalake_core::operations::update_table_metadata::TableMetadataUpdate;
use reqwest::Url;
use serde_json::Value;
use uuid::Uuid;

type TestResult = Result<(), Box<dyn Error>>;
const VERSION: &str = "delta-rs.sync-version";

#[tokio::test]
async fn guarded_sync_requires_profile_and_trusted_origin() -> TestResult {
    let catalog = UnityCatalogBuilder::builder()
        .workspace_url("http://192.0.2.1")
        .allow_http_url(true)
        .bearer_token("fixture")
        .build()
        .build()?;
    assert!(!catalog.supports_guarded_sync());
    let id = TableIdentifier::new(vec!["unity".to_owned(), "default".to_owned()], "orders")?;
    let lifecycle = CatalogLifecycle::new(catalog);
    assert!(matches!(
        lifecycle.sync_table_metadata(&id).await,
        Err(CatalogLifecycleError::Unsupported { .. })
    ));

    let catalog = UnityCatalogBuilder::builder()
        .workspace_url("http://192.0.2.1")
        .allow_http_url(true)
        .bearer_token("fixture")
        .build()
        .build()?;
    assert!(
        catalog
            .with_sync_profile(UnitySyncProfile::OssRustServer002)
            .is_err()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable local Unity Delta API server"]
async fn registration_and_sync_respect_the_live_validator_contract() -> TestResult {
    let origin = Url::parse(&std::env::var("UC_QUALIFICATION_URL")?)?;
    let permitted = matches!(origin.host(),
        Some(url::Host::Ipv4(address)) if address.is_loopback())
        || matches!(origin.host(),
        Some(url::Host::Ipv6(address)) if address.is_loopback());
    assert!(
        permitted,
        "qualification requires a numeric loopback origin"
    );
    let root = std::env::var("UC_QUALIFICATION_DATA_DIR")?;
    let fixture = tempfile::tempdir_in(root)?;
    let location = Url::from_directory_path(fixture.path())
        .map_err(|()| "fixture directory must be absolute")?;
    CreateBuilder::new()
        .with_location(location.as_str())
        .with_columns([StructField::new("order_id", DataType::LONG, false)])
        .await?;
    let catalog_name = std::env::var("UC_QUALIFICATION_CATALOG").unwrap_or_else(|_| "unity".into());
    let schema_name = std::env::var("UC_QUALIFICATION_SCHEMA").unwrap_or_else(|_| "default".into());
    let table_name = format!("guarded_sync_{}", Uuid::new_v4().simple());
    let id = TableIdentifier::new(vec![catalog_name.clone(), schema_name.clone()], &table_name)?;
    let url = origin.join(&format!(
        "/api/2.1/unity-catalog/tables/{catalog_name}.{schema_name}.{table_name}"
    ))?;
    let delta_url = origin.join(&format!(
        "/api/2.1/unity-catalog/delta/v1/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}"
    ))?;
    let token = std::env::var("UC_QUALIFICATION_BEARER").unwrap_or_else(|_| "unused".into());
    let raw = reqwest::Client::builder().no_proxy().build()?;
    let catalog = UnityCatalogBuilder::builder()
        .workspace_url(origin.to_string())
        .bearer_token(token.clone())
        .allow_http_url(true)
        .build()
        .build()?
        .with_sync_profile(UnitySyncProfile::OssRustServer002)?;
    assert!(catalog.supports_guarded_sync());
    let lifecycle = CatalogLifecycle::new(catalog);
    let result: TestResult = async {
        lifecycle.register_table(&id, location.clone()).await?;
        let before = read_table(&raw, &url, &token).await?;
        assert_eq!(before["properties"][VERSION], "0");
        assert_eq!(before["columns"].as_array().unwrap().len(), 1);
        // Stock 0.0.2 has no validator on the General Tables representation.
        let response = raw.get(url.clone()).bearer_auth(&token).send().await?.error_for_status()?;
        if !response.headers().contains_key(reqwest::header::ETAG) {
            let table = deltalake_core::open_table(location.clone()).await?;
            table.add_columns().with_fields([StructField::new("carrier", DataType::STRING, true)]).await?;
            assert!(matches!(lifecycle.sync_table_metadata(&id).await,
                Err(CatalogLifecycleError::Unsupported { .. })));
            assert_eq!(read_table(&raw, &url, &token).await?, before);
            return Ok(());
        }

        assert!(matches!(lifecycle.sync_table_metadata(&id).await?,
            CatalogSyncOutcome::Unchanged(stamp) if stamp.delta_version() == 0));
        let guarded = read_table(&raw, &delta_url, &token).await?;
        assert!(guarded["metadata"]["etag"].is_string());
        raw.post(delta_url)
            .bearer_auth(&token)
            .json(&serde_json::json!({
                "requirements": [
                    {"type": "assert-table-uuid", "uuid": before["table_id"]},
                    {"type": "assert-etag", "etag": guarded["metadata"]["etag"]},
                ],
                "updates": [{"action": "set-properties", "updates": {"catalog-only": "keep"}}],
            }))
            .send()
            .await?
            .error_for_status()?;

        let table = deltalake_core::open_table(location.clone()).await?;
        let table = table
            .add_columns()
            .with_fields([StructField::new("carrier", DataType::STRING, true)])
            .await?;
        let table = table
            .update_table_metadata()
            .with_update(TableMetadataUpdate {
                name: None,
                description: Some("Committed Delta description".to_owned()),
            })
            .await?;
        let version = table.snapshot()?.version();
        let before_sync = log_bytes(fixture.path())?;
        assert!(matches!(lifecycle.sync_table_metadata(&id).await.map_err(|error| format!("publish schema and comment failed: {error}"))?,
            CatalogSyncOutcome::Updated(stamp) if stamp.delta_version() == version));
        let after = read_table(&raw, &url, &token).await?;
        assert_eq!(after["table_id"], before["table_id"]);
        assert_eq!(after["properties"][VERSION], version.to_string());
        assert_eq!(after["properties"]["catalog-only"], "keep");
        assert_eq!(after["comment"], "Committed Delta description");
        assert_eq!(after["columns"][1]["name"], "carrier");
        assert_eq!(after["columns"][1]["position"], 1);
        assert_eq!(after["columns"][1]["type_text"], "string");
        assert_eq!(after["columns"][1]["nullable"], true);
        assert_eq!(log_bytes(fixture.path())?, before_sync);
        assert!(matches!(lifecycle.sync_table_metadata(&id).await?,
            CatalogSyncOutcome::Unchanged(stamp) if stamp.delta_version() == version));

        let table = table
            .update_table_metadata()
            .with_update(TableMetadataUpdate {
                name: None,
                description: Some(String::new()),
            })
            .await?;
        let version = table.snapshot()?.version();
        let before_sync = log_bytes(fixture.path())?;
        assert!(matches!(lifecycle.sync_table_metadata(&id).await.map_err(|error| format!("clear the published comment failed: {error}"))?,
            CatalogSyncOutcome::Updated(stamp) if stamp.delta_version() == version));
        let after_clear = read_table(&raw, &url, &token).await?;
        assert!(after_clear["comment"].is_null() || after_clear["comment"] == "");
        assert_eq!(after_clear["properties"]["catalog-only"], "keep");
        assert_eq!(log_bytes(fixture.path())?, before_sync);
        Ok(())
    }
    .await;
    let cleanup = raw.delete(url).bearer_auth(token).send().await?;
    if let Err(error) = result {
        return Err(error);
    }
    cleanup.error_for_status()?;
    Ok(())
}

async fn read_table(
    client: &reqwest::Client,
    url: &Url,
    token: &str,
) -> Result<Value, reqwest::Error> {
    client
        .get(url.clone())
        .bearer_auth(token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
}

fn log_bytes(root: &Path) -> Result<Vec<(String, Vec<u8>)>, std::io::Error> {
    let mut files = std::fs::read_dir(root.join("_delta_log"))?
        .map(|entry| {
            let entry = entry?;
            Ok((
                entry.file_name().to_string_lossy().into_owned(),
                std::fs::read(entry.path())?,
            ))
        })
        .collect::<Result<Vec<_>, std::io::Error>>()?;
    files.sort();
    Ok(files)
}

mod mocked {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use httpmock::prelude::*;
    use serde_json::json;
    use sha2::{Digest, Sha256};

    use super::*;

    const CONFIG: &str = "/api/2.1/unity-catalog/delta/v1/config";
    const DELTA: &str =
        "/api/2.1/unity-catalog/delta/v1/catalogs/unity/schemas/default/tables/orders";
    const TABLES: &str = "/api/2.1/unity-catalog/tables/unity.default.orders";
    const CREDENTIALS: &str = "/api/2.1/unity-catalog/temporary-table-credentials";
    const ID: &str = "118eb852-9950-4c80-9be1-2ed294a632e9";
    const REPLACEMENT: &str = "118eb852-9950-4c80-9be1-2ed294a632e8";
    const TOKEN: &str = "Bearer fixture";
    const OWNED_ID: &str = "delta-rs.table-id";

    fn table_identifier() -> TableIdentifier {
        TableIdentifier::new(vec!["unity".into(), "default".into()], "orders").unwrap()
    }

    fn catalog(server: &MockServer) -> deltalake_catalog_unity::UnityCatalog {
        UnityCatalogBuilder::builder()
            .workspace_url(server.base_url())
            .allow_http_url(true)
            .bearer_token("fixture")
            .build()
            .build()
            .unwrap()
            .with_sync_profile(UnitySyncProfile::OssRustServer002)
            .unwrap()
    }

    fn projection_hash(columns: &[(&str, &str, bool)]) -> String {
        let columns = serde_json::to_string(columns).unwrap();
        let document = format!(r#"{{"comment":null,"columns":{columns}}}"#);
        format!("{:x}", Sha256::digest(document))
    }

    fn table_body(location: &Url, delta_id: &str, id: &str, version: u64, note: &str) -> Value {
        let mut fields = vec![("order_id", "bigint", false)];
        if version != 0 {
            fields.push(("carrier", "string", true));
        }
        let columns = fields.iter().enumerate().map(|(position, (name, data_type, nullable))|
            json!({"name": name, "type_text": data_type, "nullable": nullable, "position": position})
        ).collect::<Vec<_>>();
        json!({
            "name": "orders", "schema_name": "default", "catalog_name": "unity",
            "table_type": "EXTERNAL", "data_source_format": "DELTA",
            "storage_location": location, "table_id": id,
            "columns": columns, "created_at": 0, "updated_at": 0,
            "properties": {
                OWNED_ID: delta_id, "delta-rs.sync-format": "1",
                VERSION: version.to_string(), "delta-rs.sync-digest": projection_hash(&fields),
                "catalog-only": note,
            },
        })
    }

    #[derive(Clone, Copy)]
    enum Scenario {
        LostAcknowledgement,
        StaleReadBack,
        StaleEtag,
        StaleGeneral,
        MissingValidator,
        WeakValidator,
        MalformedValidator,
        MultipleValidators,
        ReboundUuid,
    }

    #[tokio::test]
    async fn lost_acknowledgement_uses_fresh_tables_without_replay() -> TestResult {
        exercise(Scenario::LostAcknowledgement).await
    }

    #[tokio::test]
    async fn stale_general_fields_do_not_borrow_the_current_delta_etag() -> TestResult {
        exercise(Scenario::StaleGeneral).await
    }

    #[tokio::test]
    async fn general_tables_requires_one_strong_validator() -> TestResult {
        for scenario in [
            Scenario::MissingValidator,
            Scenario::WeakValidator,
            Scenario::MalformedValidator,
            Scenario::MultipleValidators,
        ] {
            exercise(scenario).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn stale_readback_cannot_confirm_an_ambiguous_write() -> TestResult {
        exercise(Scenario::StaleReadBack).await
    }

    #[tokio::test]
    async fn stale_etag_rebuilds_from_fresh_binding_and_preserves_unrelated_fields() -> TestResult {
        exercise(Scenario::StaleEtag).await
    }

    #[tokio::test]
    async fn stale_uuid_cannot_target_a_rebound_name() -> TestResult {
        exercise(Scenario::ReboundUuid).await
    }

    #[tokio::test]
    async fn sql_repair_only_endpoint_reports_unsupported() -> TestResult {
        let server = MockServer::start_async().await;
        let config = server
            .mock_async(|when, then| {
                when.method(GET).path(CONFIG);
                then.status(404);
            })
            .await;
        let writes = server
            .mock_async(|when, then| {
                when.method(POST).path(DELTA);
                then.status(500);
            })
            .await;
        let client = CatalogLifecycle::new(catalog(&server));
        assert!(matches!(
            client.sync_table_metadata(&table_identifier()).await,
            Err(CatalogLifecycleError::Unsupported { .. })
        ));
        config.assert_calls_async(1).await;
        writes.assert_calls_async(0).await;
        Ok(())
    }

    async fn exercise(scenario: Scenario) -> TestResult {
        let fixture = tempfile::tempdir()?;
        let location = Url::from_directory_path(fixture.path()).unwrap();
        let delta = CreateBuilder::new()
            .with_location(location.as_str())
            .with_columns([StructField::new("order_id", DataType::LONG, false)])
            .await?;
        let delta_id = delta.snapshot()?.metadata().id().to_owned();
        delta
            .add_columns()
            .with_fields([StructField::new("carrier", DataType::STRING, true)])
            .await?;

        let server = MockServer::start_async().await;
        let state = Arc::new(AtomicUsize::new(0));
        let endpoint = "/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}";
        server
            .mock_async(|when, then| {
                when.method(GET)
                    .path(CONFIG)
                    .header("authorization", TOKEN)
                    .query_param("catalog", "unity")
                    .query_param("protocol-versions", "1.0");
                then.status(200).json_body(json!({
                    "protocol-version": "1.0",
                    "endpoints": [format!("GET {endpoint}"), format!("POST {endpoint}")],
                }));
            })
            .await;
        server
            .mock_async(|when, then| {
                when.method(POST)
                    .path(CREDENTIALS)
                    .header("authorization", TOKEN)
                    .json_body_includes(json!({"table_id": ID}).to_string());
                then.status(200).json_body(json!({"url": location}));
            })
            .await;

        let mut tables = Vec::new();
        for phase in 0..=2 {
            let id = if phase == 1 && matches!(scenario, Scenario::ReboundUuid) {
                REPLACEMENT
            } else {
                ID
            };
            let etag = format!("rev-{phase}");
            let phase_state = state.clone();
            server.mock_async(|when, then| {
                when.method(GET).path(DELTA).header("authorization", TOKEN)
                    .header("cache-control", "no-cache")
                    .is_true(move |_| phase_state.load(Ordering::SeqCst) == phase);
                then.status(200).json_body(json!({"metadata": {"table-type": "EXTERNAL", "table-uuid": id, "etag": etag}}));
            }).await;

            let phase_state = state.clone();
            let version = u64::from(phase == 2 && !matches!(scenario, Scenario::StaleReadBack));
            let record = table_body(
                &location,
                &delta_id,
                id,
                version,
                if phase == 0 { "first" } else { "concurrent" },
            );
            tables.push(
                server
                    .mock_async(|when, then| {
                        when.method(GET)
                            .path(TABLES)
                            .header("authorization", TOKEN)
                            .is_true(move |_| phase_state.load(Ordering::SeqCst) == phase);
                        let then = then.status(200).json_body(record);
                        match scenario {
                            Scenario::MissingValidator => {}
                            Scenario::WeakValidator => {
                                then.header("etag", format!(r#"W/"{etag}""#));
                            }
                            Scenario::MalformedValidator => {
                                then.header("etag", &etag);
                            }
                            Scenario::MultipleValidators => {
                                then.header("etag", format!(r#""{etag}""#))
                                    .header("etag", r#""other""#);
                            }
                            Scenario::StaleGeneral if phase == 0 => {
                                then.header("etag", r#""etag-older""#);
                            }
                            _ => {
                                then.header("etag", format!(r#""{etag}""#));
                            }
                        }
                    })
                    .await,
            );
        }

        let requirement_conflict = matches!(scenario, Scenario::StaleEtag | Scenario::ReboundUuid);
        let first_state = state.clone();
        let first = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path(DELTA)
                    .header("authorization", TOKEN)
                    .is_true(move |request| {
                        if first_state.load(Ordering::SeqCst) != 0
                            || !valid_patch(request.body_ref(), "rev-0")
                        {
                            return false;
                        }
                        first_state
                            .store(if requirement_conflict { 1 } else { 2 }, Ordering::SeqCst);
                        true
                    });
                if requirement_conflict {
                    then.status(409).json_body(
                        json!({"error": {"type": "UpdateRequirementConflictException"}}),
                    );
                } else {
                    then.status(503);
                }
            })
            .await;
        let second_state = state.clone();
        let second = server.mock_async(|when, then| {
            when.method(POST).path(DELTA).header("authorization", TOKEN)
                .is_true(move |request| {
                    if second_state.load(Ordering::SeqCst) != 1 || !valid_patch(request.body_ref(), "rev-1") {
                        return false;
                    }
                    second_state.store(2, Ordering::SeqCst);
                    true
                });
            then.status(200).json_body(json!({"metadata": {"table-type": "EXTERNAL", "table-uuid": ID, "etag": "rev-2"}}));
        }).await;
        let fallback = server
            .mock_async(|when, then| {
                when.method(POST).path(DELTA);
                then.status(500);
            })
            .await;

        let client = catalog(&server);
        // Warm the ordinary cached read. Confirmation must use the independent GET.
        client.get_table("unity", "default", "orders").await?;
        let result = CatalogLifecycle::new(client)
            .sync_table_metadata(&table_identifier())
            .await;
        match scenario {
            Scenario::LostAcknowledgement | Scenario::StaleEtag => assert!(matches!(
                result?, CatalogSyncOutcome::Updated(stamp) if stamp.delta_version() == 1
            )),
            Scenario::StaleReadBack => assert!(matches!(
                result,
                Err(CatalogLifecycleError::OutcomeUnknown { .. })
            )),
            Scenario::WeakValidator
            | Scenario::MissingValidator
            | Scenario::MalformedValidator
            | Scenario::MultipleValidators => assert!(matches!(
                result,
                Err(CatalogLifecycleError::Unsupported { .. })
            )),
            Scenario::ReboundUuid | Scenario::StaleGeneral => assert!(matches!(
                result,
                Err(CatalogLifecycleError::Conflict { .. })
            )),
        }
        if matches!(
            scenario,
            Scenario::StaleGeneral
                | Scenario::MissingValidator
                | Scenario::WeakValidator
                | Scenario::MalformedValidator
                | Scenario::MultipleValidators
        ) {
            first.assert_calls_async(0).await;
            second.assert_calls_async(0).await;
            fallback.assert_calls_async(0).await;
            return Ok(());
        }
        first.assert_calls_async(1).await;
        second
            .assert_calls_async(usize::from(matches!(scenario, Scenario::StaleEtag)))
            .await;
        fallback.assert_calls_async(0).await;
        tables[if matches!(scenario, Scenario::ReboundUuid) {
            1
        } else {
            2
        }]
        .assert_calls_async(1)
        .await;
        Ok(())
    }

    fn valid_patch(body: &[u8], etag: &str) -> bool {
        let Ok(value) = serde_json::from_slice::<Value>(body) else {
            return false;
        };
        value["requirements"]
            == json!([
                {"type": "assert-table-uuid", "uuid": ID},
                {"type": "assert-etag", "etag": etag},
            ])
            && value["updates"][0]["action"] == "set-columns"
            && value["updates"][0]["columns"]["fields"][1]["name"] == "carrier"
            && value["updates"][1]["action"] == "set-properties"
            && value["updates"][1]["updates"][VERSION] == "1"
            && value["updates"][1]["updates"].get("catalog-only").is_none()
            && value["updates"]
                .as_array()
                .is_some_and(|updates| updates.len() == 2)
    }
}
