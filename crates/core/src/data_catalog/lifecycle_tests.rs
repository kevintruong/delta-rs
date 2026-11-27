use url::Url;

use super::{
    CatalogEntry, CatalogLifecycle, CatalogLifecycleError, CatalogReader, CatalogResult,
    TableIdentifier,
};
use crate::kernel::{DataType, StructField};
use crate::operations::create::CreateBuilder;

struct FixedCatalog(Option<CatalogEntry>);

#[async_trait::async_trait]
impl CatalogReader for FixedCatalog {
    async fn get_table(&self, _id: &TableIdentifier) -> CatalogResult<Option<CatalogEntry>> {
        Ok(self.0.clone())
    }
}

async fn create_table(location: &Url) -> crate::DeltaTable {
    CreateBuilder::new()
        .with_location(location.as_str())
        .with_columns([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("region", DataType::STRING, false),
        ])
        .with_partition_columns(["region"])
        .await
        .unwrap()
}

#[tokio::test]
async fn loads_one_committed_snapshot_from_a_resolved_catalog_entry() {
    let dir = tempfile::tempdir().unwrap();
    let root = Url::from_directory_path(dir.path()).unwrap();
    let original = create_table(&root).await;
    let id = TableIdentifier::new(vec!["分析".into()], "Sales").unwrap();
    let entry = CatalogEntry::new(id.clone(), root)
        .unwrap()
        .with_provider_id("provider-id")
        .with_delta_id(original.snapshot().unwrap().metadata().id());
    let loaded = CatalogLifecycle::new(FixedCatalog(Some(entry.clone())))
        .load_table(&id)
        .await
        .unwrap();

    assert_eq!(loaded.entry(), &entry);
    assert_eq!(loaded.entry().provider_id(), Some("provider-id"));
    assert_eq!(loaded.entry().delta_id(), Some(loaded.metadata().id()));
    assert_eq!(loaded.version(), 0);
    assert_eq!(
        loaded.metadata().id(),
        original.snapshot().unwrap().metadata().id()
    );
    assert_eq!(loaded.metadata().partition_columns(), &["region"]);
    assert!(loaded.schema().field("region").is_some());
}

#[tokio::test]
async fn rejects_missing_or_mismatched_catalog_bindings() {
    let id = TableIdentifier::new(vec!["db".into()], "table").unwrap();
    assert!(matches!(
        CatalogLifecycle::new(FixedCatalog(None))
            .load_table(&id)
            .await,
        Err(CatalogLifecycleError::NotFound { .. })
    ));

    let other_id = TableIdentifier::new(vec!["db".into()], "wrong").unwrap();
    let unreadable = Url::parse("file:///missing-delta-table").unwrap();
    let other = CatalogEntry::new(other_id, unreadable).unwrap();
    assert!(matches!(
        CatalogLifecycle::new(FixedCatalog(Some(other)))
            .load_table(&id)
            .await,
        Err(CatalogLifecycleError::Conflict { .. })
    ));
}

#[test]
fn validates_identifier_components_and_preserves_case_sensitive_storage_paths() {
    for (namespace, name) in [
        (vec![], "table"),
        (vec!["".into()], "table"),
        (vec!["db".into()], ""),
        (vec!["db".into()], "bad/name"),
        (vec!["db".into()], "bad\nname"),
    ] {
        assert!(matches!(
            TableIdentifier::new(namespace, name),
            Err(CatalogLifecycleError::InvalidIdentifier)
        ));
    }

    let id = TableIdentifier::new(vec!["États".into()], "Ventes").unwrap();
    assert_eq!(id.namespace(), &["États"]);
    assert_eq!(id.name(), "Ventes");
    let location = Url::parse("s3://bucket/Upper/%2FLower").unwrap();
    let read = Url::parse("uc://main.schema.table").unwrap();
    let entry = CatalogEntry::new(id.clone(), location.clone())
        .unwrap()
        .with_read_url(read.clone())
        .unwrap();
    assert_eq!(entry.location(), &location);
    assert_eq!(entry.read_url(), &read);

    assert!(matches!(
        CatalogEntry::new(
            id,
            Url::parse("https://example.com/path?token=secret").unwrap()
        ),
        Err(CatalogLifecycleError::InvalidLocation)
    ));
}

#[tokio::test]
async fn rejects_a_replaced_delta_log_at_the_same_location() {
    let dir = tempfile::tempdir().unwrap();
    let root = Url::from_directory_path(dir.path()).unwrap();
    create_table(&root).await;
    let id = TableIdentifier::new(vec!["db".into()], "table").unwrap();
    let entry = CatalogEntry::new(id.clone(), root)
        .unwrap()
        .with_delta_id("old-delta-id");
    assert!(matches!(
        CatalogLifecycle::new(FixedCatalog(Some(entry)))
            .load_table(&id)
            .await,
        Err(CatalogLifecycleError::Conflict { .. })
    ));
}

#[test]
fn accepts_abfs_container_addressing_without_accepting_credentials() {
    let id = TableIdentifier::new(vec!["db".into()], "table").unwrap();
    let root = Url::parse("abfss://container@account.dfs.core.windows.net/Table").unwrap();
    assert_eq!(
        CatalogEntry::new(id.clone(), root.clone())
            .unwrap()
            .location(),
        &root,
    );

    for uri in [
        "abfss://container:secret@account.dfs.core.windows.net/Table",
        "s3://credential@bucket/Table",
    ] {
        assert!(matches!(
            CatalogEntry::new(id.clone(), Url::parse(uri).unwrap()),
            Err(CatalogLifecycleError::InvalidLocation)
        ));
    }
}
