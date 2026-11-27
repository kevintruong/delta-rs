use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use aws_config::{BehaviorVersion, SdkConfig};
use aws_sdk_glue::config::{Credentials, Region, SharedCredentialsProvider, retry::RetryConfig};
use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::body::SdkBody;
use deltalake_catalog_glue::GlueDataCatalog;
use deltalake_core::DeltaResult;
use deltalake_core::data_catalog::{
    CatalogLifecycle, CatalogLifecycleError, CatalogReader, DataCatalog, RegistrationOutcome,
    TableIdentifier,
};
use deltalake_core::logstore::object_store::{ObjectStoreExt, memory::InMemory, path::Path};
use deltalake_core::logstore::{
    LogStore, LogStoreFactory, ObjectStoreFactory, ObjectStoreRef, StorageConfig, default_logstore,
    logstore_factories, object_store_factories,
};
use serde_json::{Value, json};
use url::Url;

mod qualification;

const LOCATION: &str = "s3://catalog-test-bucket/warehouse/orders/";
const ACCOUNT: &str = "123456789012";
const DELTA_ID: &str = "3dd380f6-4f85-466a-b337-81a1b13b2d2e";
const LOG: &str = concat!(
    r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
    "\n",
    r#"{"metaData":{"id":"3dd380f6-4f85-466a-b337-81a1b13b2d2e","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}},{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":false},\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["region"],"configuration":{}}}"#,
    "\n"
);

#[derive(Clone, Debug)]
struct GlueConnector(Arc<Mutex<Script>>);

#[derive(Debug)]
struct Script {
    responses: VecDeque<(u16, Value)>,
    calls: Vec<(String, Value)>,
}

impl GlueConnector {
    fn new(responses: impl IntoIterator<Item = (u16, Value)>) -> Self {
        Self(Arc::new(Mutex::new(Script {
            responses: responses.into_iter().collect(),
            calls: Vec::new(),
        })))
    }

    fn sdk_config(&self) -> SdkConfig {
        SdkConfig::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "test-key",
                "test-secret",
                None,
                None,
                "test",
            )))
            .http_client(aws_smithy_runtime_api::client::http::SharedHttpClient::new(
                self.clone(),
            ))
            .retry_config(RetryConfig::standard().with_max_attempts(3))
            .build()
    }

    fn catalog(&self) -> GlueDataCatalog {
        GlueDataCatalog::with_config(&self.sdk_config()).with_catalog_id(ACCOUNT)
    }

    fn calls(&self) -> Vec<(String, Value)> {
        let script = self.0.lock().unwrap();
        assert!(script.responses.is_empty(), "unconsumed Glue responses");
        script.calls.clone()
    }
}

impl HttpClient for GlueConnector {
    fn http_connector(
        &self,
        _: &HttpConnectorSettings,
        _: &RuntimeComponents,
    ) -> SharedHttpConnector {
        SharedHttpConnector::new(self.clone())
    }
}

impl HttpConnector for GlueConnector {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let target = request.headers().get("x-amz-target").unwrap().to_owned();
        let input = serde_json::from_slice(request.body().bytes().unwrap()).unwrap();
        let mut script = self.0.lock().unwrap();
        script.calls.push((target, input));
        let (status, body) = script
            .responses
            .pop_front()
            .expect("unexpected Glue request");
        let mut response =
            HttpResponse::new(status.try_into().unwrap(), SdkBody::from(body.to_string()));
        response
            .headers_mut()
            .insert("content-type", "application/x-amz-json-1.1");
        HttpConnectorFuture::new(async move { Ok(response) })
    }
}

struct FixtureS3(ObjectStoreRef);

impl ObjectStoreFactory for FixtureS3 {
    fn parse_url_opts(&self, url: &Url, _: &StorageConfig) -> DeltaResult<(ObjectStoreRef, Path)> {
        assert_eq!(url.host_str(), Some("catalog-test-bucket"));
        Ok((self.0.clone(), Path::from_url_path(url.path())?))
    }
}

impl LogStoreFactory for FixtureS3 {
    fn with_options(
        &self,
        prefixed_store: ObjectStoreRef,
        root_store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        Ok(default_logstore(
            prefixed_store,
            root_store,
            location,
            options,
        ))
    }
}

fn identifier() -> TableIdentifier {
    TableIdentifier::new(vec!["default".to_owned()], "orders").unwrap()
}

fn table(location: &str, provider: &str) -> Value {
    json!({
        "Table": {
            "Name": "orders",
            "DatabaseName": "default",
            "CatalogId": ACCOUNT,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "spark.sql.sources.provider": provider,
                "classification": "delta",
                "delta-rs.table-id": DELTA_ID
            },
            "StorageDescriptor": {"Location": location}
        }
    })
}

#[tokio::test]
async fn registration_reconciles_one_ambiguous_create() {
    let store = Arc::new(InMemory::new());
    let commit = Path::from("warehouse/orders/_delta_log/00000000000000000000.json");
    store.put(&commit, LOG.into()).await.unwrap();
    let fixture = Arc::new(FixtureS3(store.clone()));
    let scheme = Url::parse("s3://").unwrap();
    object_store_factories().insert(scheme.clone(), fixture.clone());
    logstore_factories().insert(scheme, fixture);

    let script = GlueConnector::new([
        (400, json!({"__type":"EntityNotFoundException"})),
        (500, json!({"__type":"InternalServiceException"})),
        (200, table(LOCATION, "delta")),
        (200, table(LOCATION, "delta")),
        (200, table(LOCATION, "delta")),
    ]);
    let lifecycle = CatalogLifecycle::new(script.catalog());
    let location = Url::parse(LOCATION).unwrap();
    let id = identifier();

    let outcome = lifecycle
        .register_table(&id, location.clone())
        .await
        .unwrap();
    assert!(matches!(outcome, RegistrationOutcome::AlreadyRegistered(_)));
    let repeated = lifecycle.register_table(&id, location).await.unwrap();
    assert!(lifecycle.load_table(&id).await.is_ok());
    assert!(matches!(
        repeated,
        RegistrationOutcome::AlreadyRegistered(_)
    ));
    assert_eq!(
        store.get(&commit).await.unwrap().bytes().await.unwrap(),
        LOG.as_bytes()
    );

    let calls = script.calls();
    let targets: Vec<_> = calls.iter().map(|(target, _)| target.as_str()).collect();
    assert_eq!(
        targets,
        [
            "AWSGlue.GetTable",
            "AWSGlue.CreateTable",
            "AWSGlue.GetTable",
            "AWSGlue.GetTable",
            "AWSGlue.GetTable"
        ]
    );
    for (_, request) in &calls {
        assert_eq!(request["CatalogId"], ACCOUNT);
        assert_eq!(request["DatabaseName"], "default");
    }
    let input = &calls[1].1["TableInput"];
    assert_eq!(input["Name"], "orders");
    assert_eq!(input["TableType"], "EXTERNAL_TABLE");
    assert_eq!(input["Parameters"]["spark.sql.sources.provider"], "delta");
    assert_eq!(input["Parameters"]["EXTERNAL"], "TRUE");
    assert_eq!(input["Parameters"]["delta-rs.table-id"], DELTA_ID);
    let descriptor = &input["StorageDescriptor"];
    assert_eq!(descriptor["Location"], LOCATION);
    assert_eq!(descriptor["SerdeInfo"]["Parameters"]["path"], LOCATION);
    assert_eq!(
        descriptor["SerdeInfo"]["Parameters"]["serialization.format"],
        "1"
    );
    assert_eq!(
        descriptor["Columns"],
        json!([{"Name":"id","Type":"bigint"},{"Name":"tags","Type":"array<string>"}])
    );
    assert_eq!(
        input["PartitionKeys"],
        json!([{"Name":"region","Type":"string"}])
    );
    let raced = GlueConnector::new([
        (400, json!({"__type": "EntityNotFoundException"})),
        (400, json!({"__type": "AlreadyExistsException"})),
        (
            200,
            table("s3://catalog-test-bucket/another-table/", "delta"),
        ),
    ]);
    let lifecycle = CatalogLifecycle::new(raced.catalog());
    assert!(matches!(
        lifecycle
            .register_table(&id, Url::parse(LOCATION).unwrap())
            .await,
        Err(CatalogLifecycleError::Conflict { .. })
    ));
    let raced_calls = raced.calls();
    assert_eq!(
        raced_calls
            .iter()
            .map(|(target, _)| target.as_str())
            .collect::<Vec<_>>(),
        [
            "AWSGlue.GetTable",
            "AWSGlue.CreateTable",
            "AWSGlue.GetTable"
        ]
    );

    // A timestamp-NTZ value inside an array must not be projected as a UTC timestamp.
    let log = LOG
        .replace(
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["timestampNtz"],"writerFeatures":["timestampNtz"]}}"#,
        )
        .replace(r#"\"elementType\":\"string\""#, r#"\"elementType\":\"timestamp_ntz\""#);
    store
        .put(
            &Path::from("warehouse/local_time/_delta_log/00000000000000000000.json"),
            log.into(),
        )
        .await
        .unwrap();
    let rejected = GlueConnector::new([(400, json!({"__type": "EntityNotFoundException"}))]);
    let lifecycle = CatalogLifecycle::new(rejected.catalog());
    let error = lifecycle
        .register_table(
            &id,
            Url::parse("s3://catalog-test-bucket/warehouse/local_time/").unwrap(),
        )
        .await
        .expect_err("a lossy Glue projection must be rejected");
    assert!(
        matches!(error, CatalogLifecycleError::Provider { source } if source.to_string().contains("TimestampNtz"))
    );
    let rejected_calls = rejected.calls();
    assert_eq!(rejected_calls.len(), 1);
    assert_eq!(rejected_calls[0].0, "AWSGlue.GetTable");
}

#[tokio::test]
async fn fresh_lookup_rejects_other_formats_and_preserves_s3_key() {
    let script = GlueConnector::new([
        (200, table(LOCATION, "parquet")),
        (
            200,
            table(
                "s3a://catalog-test-bucket/s3a-keep-__PLACEHOLDER__",
                "delta",
            ),
        ),
        (
            200,
            table("s3a://catalog-test-bucket/legacy-__PLACEHOLDER__", "delta"),
        ),
    ]);
    let catalog = script.catalog();
    let id = identifier();

    assert!(matches!(
        CatalogReader::get_table(&catalog, &id).await,
        Err(CatalogLifecycleError::Conflict { .. })
    ));
    let got = CatalogReader::get_table(&catalog, &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        got.location().as_str(),
        "s3://catalog-test-bucket/s3a-keep/"
    );
    assert_eq!(got.delta_id(), Some(DELTA_ID));
    let legacy = catalog
        .get_table_storage_location(Some(ACCOUNT.to_owned()), "default", "orders")
        .await
        .unwrap();
    assert_eq!(legacy, "s3://catalog-test-bucket/legacy");

    let scope =
        TableIdentifier::new(vec!["other".to_owned(), "default".to_owned()], "orders").unwrap();
    assert!(CatalogReader::get_table(&catalog, &scope).await.is_err());
    assert_eq!(script.calls().len(), 3);
}
