use std::{collections::HashMap, env, error::Error, path::PathBuf};

use delta_kernel::schema::{DataType, StructField, StructType};
use deltalake_core::operations::create::CreateBuilder;
use reqwest::{Method, StatusCode, Url};
use serde_json::json;
use url::Host;
use uuid::Uuid;

use super::MARKER;
use super::external_metadata::{
    ExternalMetadataClient, MetadataSession, Patch, QualificationError, TableName,
};

type TestResult = Result<(), Box<dyn Error>>;

const COMMENT: &str = "qualification projection";
const CATALOG_ONLY: &str = "fixture.catalog-only";

/// Creates a random external catalog entry on an explicitly supplied local server.
/// The mounted Delta fixture belongs to this test and lives in a temporary directory.
#[tokio::test]
#[ignore = "requires a disposable local Unity Delta API server"]
async fn qualify_external_metadata_on_disposable_server() -> TestResult {
    let origin = Url::parse(&env::var("UC_QUALIFICATION_URL")?)?;
    let is_loopback = match origin.host() {
        Some(Host::Ipv4(address)) => address.is_loopback(),
        Some(Host::Ipv6(address)) => address.is_loopback(),
        _ => false,
    };
    if !is_loopback {
        return Err("UC_QUALIFICATION_URL must have a numeric loopback address".into());
    }
    let fixture = tempfile::tempdir_in(PathBuf::from(
        env::var_os("UC_QUALIFICATION_DATA_DIR").ok_or("UC_QUALIFICATION_DATA_DIR is required")?,
    ))?;
    let location = Url::from_directory_path(fixture.path())
        .map_err(|()| "expected an absolute fixture directory")?;
    CreateBuilder::new()
        .with_location(location.as_str())
        .with_columns(columns(true))
        .await?;
    let commit = fixture.path().join("_delta_log/00000000000000000000.json");
    let original = std::fs::read(&commit)?;

    let scope = TableName::new(
        &env::var("UC_QUALIFICATION_CATALOG").unwrap_or_else(|_| "unity".to_owned()),
        &env::var("UC_QUALIFICATION_SCHEMA").unwrap_or_else(|_| "default".to_owned()),
        &format!("qualification_{}", Uuid::new_v4().simple()),
    )?;
    let bearer = env::var("UC_QUALIFICATION_BEARER")
        .ok()
        .map(|token| format!("Bearer {token}"));
    let client = ExternalMetadataClient::new(origin.clone(), bearer.as_deref())?;
    // Establish protocol support before creating any entry in the catalog.
    let session = client.negotiate(scope.clone()).await?;
    create(&client, &scope, &location).await?;
    let result = async {
        if bearer.is_some() {
            let anonymous = ExternalMetadataClient::new(origin, None)?
                .tables_request(Method::GET, Some(&scope))
                .send()
                .await?;
            if !matches!(
                anonymous.status(),
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
            ) {
                return Err("authorization did not deny an anonymous General Tables read".into());
            }
        }
        verify_contract(&client, &session, &scope, &location).await?;
        if std::fs::read(&commit)? != original {
            return Err("the external Delta log changed during metadata qualification".into());
        }
        Ok(())
    }
    .await;
    // Cleanup still runs when the server fails qualification.
    let cleanup = delete(&client, &scope).await;
    result.and(cleanup)
}

async fn verify_contract(
    client: &ExternalMetadataClient,
    session: &MetadataSession,
    scope: &TableName,
    location: &Url,
) -> TestResult {
    let original = session.load().await?;
    verify_tables(session, original.id, "pending", None, &["order_id"]).await?;
    let updated = session.update(&original, patch("0", COMMENT)?).await?;
    let fresh = session.load().await?;
    if original.etag == updated.etag || updated.etag != fresh.etag || original.id != fresh.id {
        return Err("update did not issue an observable ETag for the same provider UUID".into());
    }
    verify_tables(
        session,
        fresh.id,
        "0",
        Some(COMMENT),
        &["order_id", "carrier"],
    )
    .await?;

    let stale = session
        .update(&original, patch("stale", "stale precondition")?)
        .await;
    if !matches!(stale, Err(QualificationError::RequirementConflict)) {
        return Err("stale ETag did not return the documented requirement conflict".into());
    }
    verify_tables(
        session,
        fresh.id,
        "0",
        Some(COMMENT),
        &["order_id", "carrier"],
    )
    .await?;

    delete(client, scope).await?;
    create(client, scope, location).await?;
    let mut replacement = session.load().await?;
    let replacement_id = replacement.id;
    if original.id == replacement_id {
        return Err("recreating the external entry reused its provider UUID".into());
    }
    replacement.id = original.id;
    let stale_identity = session
        .update(&replacement, patch("wrong-uuid", "wrong identity")?)
        .await;
    if !matches!(stale_identity, Err(QualificationError::RequirementConflict)) {
        return Err(
            "old UUID with a fresh replacement ETag did not return a requirement conflict".into(),
        );
    }
    verify_tables(session, replacement_id, "pending", None, &["order_id"]).await
}

async fn verify_tables(
    session: &MetadataSession,
    id: Uuid,
    marker: &str,
    comment: Option<&str>,
    expected_columns: &[&str],
) -> TestResult {
    let record = session.tables_readback().await?;
    let names: Vec<_> = record
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    if record.id != id
        || record.properties.get(MARKER).map(String::as_str) != Some(marker)
        || record.properties.get(CATALOG_ONLY).map(String::as_str) != Some("keep")
        || record.comment.as_deref().filter(|value| !value.is_empty()) != comment
        || names != expected_columns
        || record.columns[0].type_text.as_deref() != Some("bigint")
        || (names.len() > 1 && record.columns[1].type_text.as_deref() != Some("string"))
    {
        return Err(
            "General Tables did not preserve the expected binding, projection and marker".into(),
        );
    }
    Ok(())
}

async fn create(client: &ExternalMetadataClient, scope: &TableName, location: &Url) -> TestResult {
    let response = client
        .tables_request(Method::POST, None)
        .json(&json!({
            "name": scope.name,
            "catalog_name": scope.catalog,
            "schema_name": scope.schema,
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "storage_location": location.as_str(),
            "columns": [{
                "name": "order_id",
                "type_name": "LONG",
                "type_text": "bigint",
                "type_json": serde_json::to_string(&columns(false)[0])?,
                "nullable": false,
                "position": 0,
            }],
            "properties": {MARKER: "pending", CATALOG_ONLY: "keep"},
        }))
        .send()
        .await?;
    response.error_for_status()?;
    Ok(())
}

async fn delete(client: &ExternalMetadataClient, scope: &TableName) -> TestResult {
    let response = client
        .tables_request(Method::DELETE, Some(scope))
        .send()
        .await?;
    if response.status() != StatusCode::NOT_FOUND {
        response.error_for_status()?;
    }
    Ok(())
}

fn patch(marker: &str, comment: &str) -> Result<Patch, Box<dyn Error>> {
    Ok(Patch::new(
        StructType::try_new(columns(true))?,
        Some(comment.to_owned()),
        HashMap::from([(MARKER.to_owned(), marker.to_owned())]),
    ))
}

fn columns(include_carrier: bool) -> Vec<StructField> {
    let mut columns = vec![StructField::new("order_id", DataType::LONG, false)];
    if include_carrier {
        columns.push(StructField::new("carrier", DataType::STRING, true));
    }
    columns
}
