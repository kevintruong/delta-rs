use aws_sdk_glue::Client;
use serde_json::{Value, json};

use crate::{ACCOUNT, DELTA_ID, GlueConnector, LOCATION, table};

use super::{
    DIGEST, FORMAT, PROJECTION_DESCRIPTION, PROJECTION_DIGEST, PROJECTION_VERSION, Scope,
    UNRELATED, VERSION, get_native_delta_table, qualify_replacement, qualify_version_id,
};

fn snapshot(version: &str, unrelated: bool, projected: bool) -> Value {
    let mut value = table(LOCATION, "delta");
    let table = &mut value["Table"];
    table["VersionId"] = json!(version);
    table["Owner"] = json!("unrelated_owner");
    table["Description"] = json!(if projected {
        PROJECTION_DESCRIPTION
    } else {
        "original projection"
    });
    table["StorageDescriptor"]["Columns"] = json!([{"Name":"id","Type":"bigint"}]);
    if unrelated {
        table["Parameters"][UNRELATED] = json!("advanced");
    }
    if projected {
        table["Parameters"][FORMAT] = json!("1");
        table["Parameters"][VERSION] = json!(PROJECTION_VERSION);
        table["Parameters"][DIGEST] = json!(PROJECTION_DIGEST);
    }
    value
}

fn scope() -> Scope<'static> {
    Scope {
        catalog: ACCOUNT,
        database: "default",
        table: "orders",
        location: LOCATION,
        delta_id: DELTA_ID,
    }
}

#[tokio::test]
async fn qualification_requires_stale_rejection_and_compound_round_trip() {
    let initial = snapshot("1", false, false);
    let advanced = snapshot("2", true, false);
    let round_trip = snapshot("3", true, true);
    let script = GlueConnector::new([
        (200, initial),
        (200, json!({})),
        (200, advanced.clone()),
        (400, json!({"__type": "ConcurrentModificationException"})),
        (200, advanced),
        (200, json!({})),
        (200, round_trip),
    ]);
    let client = Client::new(&script.sdk_config());

    let evidence = qualify_version_id(&client, &scope()).await.unwrap();
    assert_eq!(evidence.stale_error_code, "ConcurrentModificationException");
    assert_eq!(evidence.versions, ["1", "2", "3"]);

    let calls = script.calls();
    assert_eq!(
        calls
            .iter()
            .map(|(target, _)| target.as_str())
            .collect::<Vec<_>>(),
        [
            "AWSGlue.GetTable",
            "AWSGlue.UpdateTable",
            "AWSGlue.GetTable",
            "AWSGlue.UpdateTable",
            "AWSGlue.GetTable",
            "AWSGlue.UpdateTable",
            "AWSGlue.GetTable",
        ]
    );
    for (_, request) in &calls {
        assert_eq!(request["CatalogId"], ACCOUNT);
        assert_eq!(request["DatabaseName"], "default");
        assert_eq!(request["Name"], "orders");
        assert!(request.get("Force").is_none());
        assert!(request.get("SkipArchive").is_none());
    }
    assert_eq!(calls[1].1["VersionId"], "1");
    assert_eq!(calls[3].1["VersionId"], "1");
    assert_eq!(calls[5].1["VersionId"], "2");
    assert_eq!(
        calls[1].1["TableInput"]["Parameters"][UNRELATED],
        "advanced"
    );
    for index in [3, 5] {
        let input = &calls[index].1["TableInput"];
        assert_eq!(input["Description"], PROJECTION_DESCRIPTION);
        assert_eq!(input["Parameters"][VERSION], PROJECTION_VERSION);
        assert_eq!(input["Parameters"][DIGEST], PROJECTION_DIGEST);
        assert_eq!(input["Owner"], "unrelated_owner");
        assert_eq!(input["StorageDescriptor"]["Location"], LOCATION);
    }
    assert!(
        calls[3].1["TableInput"]["Parameters"]
            .get(UNRELATED)
            .is_none()
    );
    assert_eq!(
        calls[5].1["TableInput"]["Parameters"][UNRELATED],
        "advanced"
    );
}

#[tokio::test]
async fn qualification_fails_if_stale_update_was_accepted() {
    let script = GlueConnector::new([
        (200, snapshot("1", false, false)),
        (200, json!({})),
        (200, snapshot("2", true, false)),
        (200, json!({})),
        (200, snapshot("3", false, true)),
    ]);
    let client = Client::new(&script.sdk_config());
    let error = qualify_version_id(&client, &scope()).await.unwrap_err();
    assert!(error.to_string().contains("accepted stale VersionId"));
    assert_eq!(script.calls().len(), 5);
}

#[tokio::test]
async fn qualification_fails_if_stale_rejection_changes_record() {
    let script = GlueConnector::new([
        (200, snapshot("1", false, false)),
        (200, json!({})),
        (200, snapshot("2", true, false)),
        (400, json!({"__type": "ConcurrentModificationException"})),
        (200, snapshot("3", false, true)),
    ]);
    let client = Client::new(&script.sdk_config());
    let error = qualify_version_id(&client, &scope()).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("stale attempt changed the record")
    );
    assert_eq!(script.calls().len(), 5);
}

#[tokio::test]
async fn qualification_fails_if_stale_attempt_has_an_indeterminate_outcome() {
    let script = GlueConnector::new([
        (200, snapshot("1", false, false)),
        (200, json!({})),
        (200, snapshot("2", true, false)),
        (500, json!({"__type": "InternalServiceException"})),
        (200, snapshot("2", true, false)),
    ]);
    let client = Client::new(&script.sdk_config());
    let error = qualify_version_id(&client, &scope()).await.unwrap_err();
    assert!(error.to_string().contains("InternalServiceException"));
    assert_eq!(script.calls().len(), 5);
}

#[tokio::test]
async fn qualification_fails_if_projection_changes_without_its_watermark() {
    let mut partial = snapshot("3", true, true);
    partial["Table"]["Parameters"]
        .as_object_mut()
        .unwrap()
        .remove(DIGEST);
    let script = GlueConnector::new([
        (200, snapshot("1", false, false)),
        (200, json!({})),
        (200, snapshot("2", true, false)),
        (400, json!({"__type": "ConcurrentModificationException"})),
        (200, snapshot("2", true, false)),
        (200, json!({})),
        (200, partial),
    ]);
    let client = Client::new(&script.sdk_config());
    let error = qualify_version_id(&client, &scope()).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("compound projection did not round-trip")
    );
    assert_eq!(script.calls().len(), 7);
}

#[tokio::test]
async fn qualification_rejects_version_reuse_on_replacement_without_a_write() {
    let script = GlueConnector::new([
        (200, snapshot("1", false, false)),
        (200, snapshot("1", true, false)),
    ]);
    let client = Client::new(&script.sdk_config());
    let old = get_native_delta_table(&client, &scope()).await.unwrap();
    let replacement = get_native_delta_table(&client, &scope()).await.unwrap();
    let error = qualify_replacement(&client, &scope(), &old, &replacement)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("replacement reused VersionId"));
    assert_eq!(script.calls().len(), 2);
}

#[tokio::test]
async fn qualification_requires_the_old_token_to_leave_the_replacement_intact() {
    let replacement = snapshot("4", true, false);
    let script = GlueConnector::new([
        (200, snapshot("1", false, false)),
        (200, replacement.clone()),
        (400, json!({"__type": "ConcurrentModificationException"})),
        (200, replacement),
    ]);
    let client = Client::new(&script.sdk_config());
    let old = get_native_delta_table(&client, &scope()).await.unwrap();
    let replacement = get_native_delta_table(&client, &scope()).await.unwrap();
    let error_code = qualify_replacement(&client, &scope(), &old, &replacement)
        .await
        .unwrap();
    assert_eq!(error_code, "ConcurrentModificationException");
    let calls = script.calls();
    assert_eq!(calls[2].1["VersionId"], "1");
    assert_eq!(calls.len(), 4);
}
