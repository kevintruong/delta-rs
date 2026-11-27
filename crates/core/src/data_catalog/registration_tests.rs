use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

use tokio::sync::Barrier;
use url::Url;

use super::lifecycle_tests::create_table;
use super::{
    CatalogEntry, CatalogLifecycle, CatalogLifecycleError, CatalogReader, CatalogResult,
    CatalogWriter, CreateTableOutcome, RegistrationOutcome, TableIdentifier, TableRegistration,
};

enum CreateBehavior {
    Normal,
    LoseResponseBeforeApply,
    TransportErrorBeforeApply,
    LoseResponseAfterApply,
    ConcurrentWinner(CatalogEntry),
}

struct TestCatalog {
    entry: Mutex<Option<CatalogEntry>>,
    calls: Mutex<Vec<&'static str>>,
    behavior: CreateBehavior,
    preflight_gate: Option<Barrier>,
    reads: AtomicUsize,
}

impl TestCatalog {
    fn new(behavior: CreateBehavior) -> Self {
        Self {
            entry: Mutex::new(None),
            calls: Mutex::new(Vec::new()),
            behavior,
            preflight_gate: None,
            reads: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> Vec<&'static str> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl CatalogReader for &TestCatalog {
    async fn get_table(&self, _id: &TableIdentifier) -> CatalogResult<Option<CatalogEntry>> {
        self.calls.lock().unwrap().push("get");
        let entry = self.entry.lock().unwrap().clone();
        if let Some(gate) = &self.preflight_gate
            && self.reads.fetch_add(1, Ordering::Relaxed) < 2
        {
            gate.wait().await;
        }
        Ok(entry)
    }
}

#[async_trait::async_trait]
impl CatalogWriter for &TestCatalog {
    async fn create_table(
        &self,
        registration: &TableRegistration,
    ) -> CatalogResult<CreateTableOutcome> {
        self.calls.lock().unwrap().push("create");
        assert_eq!(registration.version(), 0);
        assert_eq!(registration.metadata().partition_columns(), &["region"]);
        assert!(registration.schema().field("id").is_some());

        let mut entry = self.entry.lock().unwrap();
        if entry.is_some() {
            return Ok(CreateTableOutcome::AlreadyExists);
        }

        match &self.behavior {
            CreateBehavior::LoseResponseBeforeApply => Ok(CreateTableOutcome::Unknown),
            CreateBehavior::TransportErrorBeforeApply => {
                Err(CatalogLifecycleError::OutcomeUnknown {
                    table: registration.identifier().clone(),
                    source: Box::new(std::io::Error::other("connection lost")),
                })
            }
            CreateBehavior::ConcurrentWinner(winner) => {
                *entry = Some(winner.clone());
                Ok(CreateTableOutcome::AlreadyExists)
            }
            CreateBehavior::Normal | CreateBehavior::LoseResponseAfterApply => {
                *entry = Some(
                    CatalogEntry::new(
                        registration.identifier().clone(),
                        registration.location().clone(),
                    )?
                    .with_delta_id(registration.metadata().id()),
                );
                Ok(match self.behavior {
                    CreateBehavior::Normal => CreateTableOutcome::Created,
                    _ => CreateTableOutcome::Unknown,
                })
            }
        }
    }
}

fn table_id() -> TableIdentifier {
    TableIdentifier::new(vec!["db".into()], "test_table").unwrap()
}

#[tokio::test]
async fn creates_from_one_snapshot_and_verifies_the_binding_without_modifying_the_delta_log() {
    let dir = tempfile::tempdir().unwrap();
    let root = Url::from_directory_path(dir.path()).unwrap();
    let original = create_table(&root).await;
    let log_dir = dir.path().join("_delta_log");
    let first_commit = log_dir.join("00000000000000000000.json");
    let original_bytes = std::fs::read(&first_commit).unwrap();
    let original_entries = std::fs::read_dir(&log_dir).unwrap().count();

    let backend = TestCatalog::new(CreateBehavior::Normal);
    let catalog = CatalogLifecycle::new(&backend);
    let RegistrationOutcome::Created(table) = catalog
        .register_table(&table_id(), root.clone())
        .await
        .unwrap()
    else {
        panic!("expected a verified creation");
    };
    assert_eq!(
        table.metadata().id(),
        original.snapshot().unwrap().metadata().id()
    );
    assert_eq!(backend.calls(), ["get", "create", "get"]);

    assert!(matches!(
        catalog.register_table(&table_id(), root).await.unwrap(),
        RegistrationOutcome::AlreadyRegistered(_)
    ));
    assert_eq!(backend.calls(), ["get", "create", "get", "get"]);
    assert_eq!(std::fs::read(&first_commit).unwrap(), original_bytes);
    assert_eq!(
        std::fs::read_dir(&log_dir).unwrap().count(),
        original_entries
    );
}

#[tokio::test]
async fn lost_acknowledgement_does_not_trigger_a_second_create() {
    let dir = tempfile::tempdir().unwrap();
    let root = Url::from_directory_path(dir.path()).unwrap();
    create_table(&root).await;

    let before = TestCatalog::new(CreateBehavior::LoseResponseBeforeApply);
    let before_outcome = CatalogLifecycle::new(&before)
        .register_table(&table_id(), root.clone())
        .await;
    assert!(matches!(
        before_outcome,
        Err(CatalogLifecycleError::OutcomeUnknown { .. })
    ));
    assert_eq!(before.calls(), ["get", "create", "get"]);

    let after = TestCatalog::new(CreateBehavior::LoseResponseAfterApply);
    let after_outcome = CatalogLifecycle::new(&after)
        .register_table(&table_id(), root)
        .await;
    assert!(matches!(
        after_outcome,
        Ok(RegistrationOutcome::AlreadyRegistered(_))
    ));
    assert_eq!(after.calls(), ["get", "create", "get"]);
}

#[tokio::test]
async fn competing_binding_is_a_conflict_after_create() {
    let dir = tempfile::tempdir().unwrap();
    let root = Url::from_directory_path(dir.path()).unwrap();
    create_table(&root).await;

    let wrong = Url::parse("file:///different-delta-location").unwrap();
    let conflicting = CatalogEntry::new(table_id(), wrong).unwrap();
    let backend = TestCatalog::new(CreateBehavior::ConcurrentWinner(conflicting));
    assert!(matches!(
        CatalogLifecycle::new(&backend)
            .register_table(&table_id(), root)
            .await,
        Err(CatalogLifecycleError::Conflict { .. })
    ));
    assert_eq!(backend.calls(), ["get", "create", "get"]);
}

#[tokio::test]
async fn rejects_existing_binding_with_a_different_delta_id_without_create() {
    let dir = tempfile::tempdir().unwrap();
    let root = Url::from_directory_path(dir.path()).unwrap();
    create_table(&root).await;

    let mut backend = TestCatalog::new(CreateBehavior::Normal);
    *backend.entry.get_mut().unwrap() = Some(
        CatalogEntry::new(table_id(), root.clone())
            .unwrap()
            .with_delta_id("another Delta table"),
    );
    assert!(matches!(
        CatalogLifecycle::new(&backend)
            .register_table(&table_id(), root)
            .await,
        Err(CatalogLifecycleError::Conflict { .. })
    ));
    assert_eq!(backend.calls(), ["get"]);
}

#[tokio::test]
async fn concurrent_registrations_converge_on_one_binding() {
    let dir = tempfile::tempdir().unwrap();
    let root = Url::from_directory_path(dir.path()).unwrap();
    create_table(&root).await;

    let mut backend = TestCatalog::new(CreateBehavior::Normal);
    backend.preflight_gate = Some(Barrier::new(2));
    let catalog = CatalogLifecycle::new(&backend);
    let id = table_id();
    let (left, right) = tokio::join!(
        catalog.register_table(&id, root.clone()),
        catalog.register_table(&id, root),
    );
    assert!(matches!(
        (left.unwrap(), right.unwrap()),
        (
            RegistrationOutcome::Created(_),
            RegistrationOutcome::AlreadyRegistered(_)
        ) | (
            RegistrationOutcome::AlreadyRegistered(_),
            RegistrationOutcome::Created(_)
        )
    ));
    let calls = backend.calls();
    assert_eq!(calls.iter().filter(|&&call| call == "create").count(), 2);
    assert_eq!(calls.iter().filter(|&&call| call == "get").count(), 4);
}

#[tokio::test]
async fn missing_delta_log_is_rejected_before_catalog_calls() {
    let dir = tempfile::tempdir().unwrap();
    let root = Url::from_directory_path(dir.path()).unwrap();
    let backend = TestCatalog::new(CreateBehavior::Normal);
    assert!(matches!(
        CatalogLifecycle::new(&backend)
            .register_table(&table_id(), root)
            .await,
        Err(CatalogLifecycleError::Delta(_))
    ));
    assert!(backend.calls().is_empty());
}

#[tokio::test]
async fn an_uncertain_create_preserves_its_cause_if_confirmation_fails() {
    let dir = tempfile::tempdir().unwrap();
    let root = Url::from_directory_path(dir.path()).unwrap();
    create_table(&root).await;
    let backend = TestCatalog::new(CreateBehavior::TransportErrorBeforeApply);
    let error = CatalogLifecycle::new(&backend)
        .register_table(&table_id(), root)
        .await
        .unwrap_err();
    let CatalogLifecycleError::OutcomeUnknown { source, .. } = error else {
        panic!("expected an uncertain create outcome");
    };
    assert_eq!(source.to_string(), "connection lost");
    assert_eq!(backend.calls(), ["get", "create", "get"]);
}
