use super::{ACCOUNT, FIXTURE_LOCK, FixtureS3, GlueConnector, LOCATION, LOG, identifier, table};
use aws_sdk_glue::meta::PKG_VERSION;
use deltalake_catalog_glue::GlueDataCatalog;
use deltalake_core::data_catalog::{
    CatalogLifecycle, CatalogLifecycleError, CatalogSyncOutcome, CatalogSyncWriter,
    RegistrationOutcome, TableIdentifier,
};
use deltalake_core::logstore::object_store::{ObjectStoreExt, memory::InMemory, path::Path};
use deltalake_core::logstore::{logstore_factories, object_store_factories};
use serde_json::{Value, json};
use std::sync::Arc;
use url::Url;

const ENDPOINT: &str = "https://glue.us-east-1.amazonaws.com";
const ORIGINAL_DIGEST: &str = "f28d24599387ea9682ab8adc33aec1f9127059a9e7eeeba9c33d7615e0f76af2";
const UPDATED_DIGEST: &str = "9edf62fe3bb840abcacb41f1383782297acc77de0695c5916b3475f3ef81a355";

fn qualified_report() -> Value {
    json!({
        "format_version":1,
        "evidence_source":"aws-live",
        "profile":"disposable-aws-profile",
        "account_id":ACCOUNT,
        "catalog_id":ACCOUNT,
        "database":"default",
        "region":"us-east-1",
        "endpoint":ENDPOINT,
        "aws_sdk_glue_version":PKG_VERSION,
        "observations":{
            "lake_formation_registered":false,
            "initial_version":"probe-1",
            "advanced_version":"probe-2",
            "compound_version":"probe-3",
            "stale_error_code":"ConcurrentModificationException",
            "replacement_version":"probe-4",
            "replacement_error_code":"ConcurrentModificationException"
        }
    })
}

fn identity() -> (u16, Value) {
    (200, json!({"Account":ACCOUNT}))
}

fn observed(revision: &str, version: u64, unrelated: &str) -> Value {
    let mut value = table(LOCATION, "delta");
    let row = &mut value["Table"];
    row["CreateTime"] = json!(100);
    row["Owner"] = json!("governance");
    row["Retention"] = json!(42);
    row["VersionId"] = json!(revision);
    row["Parameters"]["unrelated"] = json!(unrelated);
    row["Parameters"]["delta-rs.sync-format"] = json!("1");
    row["Parameters"]["delta-rs.sync-version"] = json!(version.to_string());
    row["Parameters"]["delta-rs.sync-digest"] = json!(if version == 0 {
        ORIGINAL_DIGEST
    } else {
        UPDATED_DIGEST
    });
    row["StorageDescriptor"] = json!({
        "Location":LOCATION,
        "Parameters":{"provider-setting":"keep"},
        "NumberOfBuckets":3,
        "SerdeInfo":{"Parameters":{"path":LOCATION,"serialization.format":"1","owner":"preserve"}},
        "SortColumns":[{"Column":"id","SortOrder":1}],
        "Columns":[
            {"Name":"id","Type":"bigint","Comment":"verify catalog annotation",
             "Parameters":{"classifier":"preserve"}},
            {"Name":"tags","Type":"array<string>"}
        ],
    });
    row["PartitionKeys"] =
        json!([{"Name":"region","Type":"string","Comment":"keep partition annotation"}]);
    if version != 0 {
        row["Description"] = json!("new description");
        row["StorageDescriptor"]["Columns"]
            .as_array_mut()
            .unwrap()
            .push(json!({"Name":"active","Type":"boolean"}));
    }
    value
}

fn store_with_snapshot() -> Arc<InMemory> {
    let store = Arc::new(InMemory::new());
    let fixture = Arc::new(FixtureS3(store.clone()));
    let scheme = Url::parse("s3://").unwrap();
    object_store_factories().insert(scheme.clone(), fixture.clone());
    logstore_factories().insert(scheme, fixture);
    store
}

fn log_path(version: u64) -> Path {
    Path::from(format!("warehouse/orders/_delta_log/{version:020}.json"))
}

fn new_snapshot_metadata() -> String {
    let mut action: Value = serde_json::from_str(LOG.lines().nth(1).unwrap()).unwrap();
    let metadata = &mut action["metaData"];
    let mut schema: Value =
        serde_json::from_str(metadata["schemaString"].as_str().unwrap()).unwrap();
    schema["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"active","type":"boolean","nullable":true,"metadata":{}}));
    metadata["schemaString"] = json!(schema.to_string());
    metadata["description"] = json!("new description");
    format!("{action}\n")
}

#[tokio::test]
async fn ordinary_clients_and_unqualified_scopes_fail_closed() {
    let plain = GlueConnector::new([]);
    assert!(matches!(
        CatalogLifecycle::new(plain.catalog())
            .sync_table_metadata(&identifier())
            .await,
        Err(CatalogLifecycleError::Unsupported { .. })
    ));
    assert!(plain.calls().is_empty());

    let script = GlueConnector::new([identity()]);
    let qualified = script
        .catalog()
        .with_sync_qualification(&qualified_report().to_string())
        .await
        .unwrap();
    let other_database = TableIdentifier::new(vec!["other".to_owned()], "orders").unwrap();
    assert!(matches!(
        qualified.get_sync_state(&other_database).await,
        Err(CatalogLifecycleError::Unsupported { .. })
    ));
    let other_catalog = qualified.with_catalog_id("000000000000");
    let lifecycle = CatalogLifecycle::new(other_catalog);
    assert!(matches!(
        lifecycle.sync_table_metadata(&other_database).await,
        Err(CatalogLifecycleError::Unsupported { .. })
    ));
    assert!(matches!(
        lifecycle.sync_table_metadata(&identifier()).await,
        Err(CatalogLifecycleError::Unsupported { .. })
    ));
    assert_eq!(script.calls().len(), 1);
}

#[tokio::test]
async fn qualification_rejects_non_live_evidence_reused_revision_scope_and_account_mismatch() {
    let script = GlueConnector::new([]);
    for (key, value) in [
        ("evidence_source", json!("mock")),
        ("region", json!("us-west-2")),
        ("catalog_id", json!("000000000000")),
        ("aws_sdk_glue_version", json!("1.2.3")),
        ("endpoint", json!("https://unverified.example")),
    ] {
        let mut report = qualified_report();
        report[key] = value;
        assert!(
            script
                .catalog()
                .with_sync_qualification(&report.to_string())
                .await
                .is_err(),
            "{key}"
        );
    }
    for (key, value) in [
        ("replacement_version", json!("probe-3")),
        ("stale_error_code", json!("OperationTimeoutException")),
        ("replacement_error_code", json!("InternalServiceException")),
    ] {
        let mut report = qualified_report();
        report["observations"][key] = value;
        assert!(
            script
                .catalog()
                .with_sync_qualification(&report.to_string())
                .await
                .is_err(),
            "{key}"
        );
    }
    let other_endpoint = script
        .sdk_config()
        .to_builder()
        .endpoint_url("https://unverified.example")
        .build();
    let other_client = GlueDataCatalog::with_config(&other_endpoint).with_catalog_id(ACCOUNT);
    assert!(
        other_client
            .with_sync_qualification(&qualified_report().to_string())
            .await
            .is_err()
    );
    assert!(script.calls().is_empty());

    let wrong_account = GlueConnector::new([(200, json!({"Account":"000000000000"}))]);
    assert!(
        wrong_account
            .catalog()
            .with_sync_qualification(&qualified_report().to_string())
            .await
            .is_err()
    );
    assert_eq!(wrong_account.calls().len(), 1);
}

#[tokio::test]
async fn sync_seeds_a_stamp_and_confirms_an_ambiguous_guarded_merge() {
    let _fixture_lock = FIXTURE_LOCK.lock().await;
    let store = store_with_snapshot();
    store.put(&log_path(0), LOG.into()).await.unwrap();
    let script = GlueConnector::new([
        identity(),
        (400, json!({"__type":"EntityNotFoundException"})),
        (200, json!({})),
        (200, observed("revision-1", 0, "first")),
        (200, observed("revision-1", 0, "first")),
        (400, json!({"__type":"ConcurrentModificationException"})),
        (200, observed("revision-2", 0, "concurrent")),
        (500, json!({"__type":"InternalServiceException"})),
        (200, observed("revision-3", 1, "concurrent")),
    ]);
    let lifecycle = CatalogLifecycle::new(
        script
            .catalog()
            .with_sync_qualification(&qualified_report().to_string())
            .await
            .unwrap(),
    );
    let id = identifier();
    let registered = lifecycle
        .register_table(&id, Url::parse(LOCATION).unwrap())
        .await
        .unwrap();
    assert!(matches!(registered, RegistrationOutcome::Created(_)));
    let metadata = new_snapshot_metadata();
    store
        .put(&log_path(1), metadata.clone().into())
        .await
        .unwrap();
    let outcome = lifecycle.sync_table_metadata(&id).await.unwrap();
    assert!(
        matches!(outcome, CatalogSyncOutcome::Updated(stamp) if stamp.delta_version() == 1 && stamp.digest() == UPDATED_DIGEST)
    );

    let calls = script.calls();
    assert_eq!(
        calls.iter().map(|call| call.0.as_str()).collect::<Vec<_>>(),
        [
            "STS.GetCallerIdentity",
            "AWSGlue.GetTable",
            "AWSGlue.CreateTable",
            "AWSGlue.GetTable",
            "AWSGlue.GetTable",
            "AWSGlue.UpdateTable",
            "AWSGlue.GetTable",
            "AWSGlue.UpdateTable",
            "AWSGlue.GetTable",
        ]
    );
    let create = &calls[2].1["TableInput"]["Parameters"];
    assert_eq!(create["delta-rs.sync-version"], "0");
    assert_eq!(create["delta-rs.sync-digest"], ORIGINAL_DIGEST);
    assert_eq!(calls[5].1["VersionId"], "revision-1");
    let update = &calls[7].1;
    assert_eq!(update["VersionId"], "revision-2");
    assert!(update.get("SkipArchive").is_none());
    assert!(update.get("Force").is_none());
    let input = &update["TableInput"];
    assert_eq!(input["Owner"], "governance");
    assert_eq!(input["Retention"], 42);
    assert_eq!(input["Description"], "new description");
    assert_eq!(input["Parameters"]["unrelated"], "concurrent");
    assert_eq!(input["Parameters"]["delta-rs.sync-digest"], UPDATED_DIGEST);
    let storage = &input["StorageDescriptor"];
    assert_eq!(storage["SerdeInfo"]["Parameters"]["owner"], "preserve");
    assert_eq!(storage["Parameters"]["provider-setting"], "keep");
    assert_eq!(storage["SortColumns"][0]["Column"], "id");
    assert_eq!(
        storage["Columns"][0]["Comment"],
        "verify catalog annotation"
    );
    assert_eq!(
        storage["Columns"][0]["Parameters"]["classifier"],
        "preserve"
    );
    assert_eq!(
        storage["Columns"][2],
        json!({"Name":"active","Type":"boolean"})
    );
    assert_eq!(
        input["PartitionKeys"][0]["Comment"],
        "keep partition annotation"
    );
    assert_eq!(
        store
            .get(&log_path(0))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        LOG.as_bytes()
    );
    assert_eq!(
        store
            .get(&log_path(1))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        metadata.as_bytes()
    );
}

#[tokio::test]
async fn sync_rejects_drift_rebound_record_missing_stamp_and_profile_mismatch() {
    let _fixture_lock = FIXTURE_LOCK.lock().await;
    let store = store_with_snapshot();
    store.put(&log_path(0), LOG.into()).await.unwrap();
    store
        .put(&log_path(1), new_snapshot_metadata().into())
        .await
        .unwrap();

    let mut drift = observed("revision-1", 0, "first");
    drift["Table"]["StorageDescriptor"]["Columns"][0]["Type"] = json!("string");
    let mut unstamped = observed("revision-1", 0, "first");
    unstamped["Table"]["Parameters"]
        .as_object_mut()
        .unwrap()
        .remove("delta-rs.sync-digest");
    let mut changed_profile = observed("revision-1", 0, "first");
    changed_profile["Table"]["IsRegisteredWithLakeFormation"] = json!(true);
    let mut rebound = observed("revision-9", 1, "first");
    rebound["Table"]["CreateTime"] = json!(200);
    let script = GlueConnector::new([
        identity(),
        (200, drift),
        (200, unstamped),
        (200, changed_profile),
        (200, observed("revision-1", 0, "first")),
        (500, json!({"__type":"InternalServiceException"})),
        (200, rebound),
    ]);
    let lifecycle = CatalogLifecycle::new(
        script
            .catalog()
            .with_sync_qualification(&qualified_report().to_string())
            .await
            .unwrap(),
    );
    assert!(matches!(
        lifecycle.sync_table_metadata(&identifier()).await,
        Err(CatalogLifecycleError::Conflict { .. })
    ));
    assert!(matches!(
        lifecycle.sync_table_metadata(&identifier()).await,
        Err(CatalogLifecycleError::InvalidSyncState)
    ));
    assert!(matches!(
        lifecycle.sync_table_metadata(&identifier()).await,
        Err(CatalogLifecycleError::Unsupported { .. })
    ));
    assert!(matches!(
        lifecycle.sync_table_metadata(&identifier()).await,
        Err(CatalogLifecycleError::OutcomeUnknown { .. })
    ));
    let calls = script.calls();
    assert_eq!(
        calls
            .iter()
            .filter(|call| call.0 == "AWSGlue.UpdateTable")
            .count(),
        1
    );
    assert_eq!(calls.last().unwrap().0, "AWSGlue.GetTable");
}

#[tokio::test]
async fn guarded_observation_requires_explicit_catalog_scope_from_glue() {
    let mut record = observed("revision-1", 0, "first");
    record["Table"].as_object_mut().unwrap().remove("CatalogId");
    let script = GlueConnector::new([identity(), (200, record)]);
    let catalog = script
        .catalog()
        .with_sync_qualification(&qualified_report().to_string())
        .await
        .unwrap();
    assert!(matches!(
        catalog.get_sync_state(&identifier()).await,
        Err(CatalogLifecycleError::InvalidSyncState)
    ));
    assert_eq!(script.calls().len(), 2);
}
