use std::collections::VecDeque;
use std::sync::Mutex;

use tokio::sync::Notify;
use url::Url;

use super::lifecycle_tests::create_table;
use super::{
    CATALOG_PROJECTION_FORMAT_VERSION, CatalogEntry, CatalogLifecycle, CatalogLifecycleError,
    CatalogProjectionStamp, CatalogReader, CatalogResult, CatalogRevision, CatalogSyncOutcome,
    CatalogSyncProjection, CatalogSyncState, CatalogSyncUpdate, CatalogSyncWriteOutcome,
    CatalogSyncWriter, CatalogTable, TableIdentifier,
};
use crate::kernel::{DataType, StructField, StructType, Version};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Projection {
    fields: Vec<String>,
    partitions: Vec<String>,
}

impl Projection {
    fn from_snapshot(schema: &StructType, partitions: &[String]) -> Self {
        Self {
            fields: schema.fields().map(|field| field.name().clone()).collect(),
            partitions: partitions.to_vec(),
        }
    }

    fn digest(&self) -> String {
        serde_json::to_string(&(&self.fields, &self.partitions)).unwrap()
    }
}

#[derive(Clone)]
struct Record {
    owned: Projection,
    provider_note: String,
}

#[derive(Debug, PartialEq, Eq)]
enum Call {
    Read(String),
    Write(String, Version, String),
}

enum WriteStep {
    RejectAfterUnrelatedChange,
    RejectAfterReplacement,
    Reject,
    ApplyAndLoseAck,
    AcknowledgeWithoutApplying,
    ApplyAfterReplacement,
    ApplyWithoutChangingRevision,
    LoseAckWithoutApplying,
}

#[derive(Default)]
struct FirstWriteGate {
    reached: Notify,
    release: Notify,
}

struct TestCatalog {
    current: Mutex<CatalogSyncState<Record>>,
    steps: Mutex<VecDeque<WriteStep>>,
    calls: Mutex<Vec<Call>>,
    first_write_gate: Option<FirstWriteGate>,
    supported: bool,
}

#[async_trait::async_trait]
impl CatalogReader for TestCatalog {
    async fn get_table(&self, _: &TableIdentifier) -> CatalogResult<Option<CatalogEntry>> {
        panic!("metadata synchronization requires an atomic observation and its exact revision")
    }
}

#[async_trait::async_trait]
impl CatalogSyncWriter for TestCatalog {
    type Record = Record;
    type Projection = Projection;

    async fn get_sync_state(
        &self,
        id: &TableIdentifier,
    ) -> CatalogResult<Option<CatalogSyncState<Self::Record>>> {
        if !self.supported {
            return Err(CatalogLifecycleError::Unsupported { table: id.clone() });
        }
        let state = self.current.lock().unwrap().clone();
        self.calls
            .lock()
            .unwrap()
            .push(Call::Read(state.revision().as_str().into()));
        Ok(Some(state))
    }

    fn project_table(
        &self,
        table: &CatalogTable,
    ) -> CatalogResult<CatalogSyncProjection<Projection>> {
        let owned =
            Projection::from_snapshot(&table.schema(), table.metadata().partition_columns());
        CatalogSyncProjection::new(owned.digest(), owned)
    }

    async fn update_table_metadata(
        &self,
        update: CatalogSyncUpdate<'_, Record, Projection>,
    ) -> CatalogResult<CatalogSyncWriteOutcome> {
        self.calls.lock().unwrap().push(Call::Write(
            update.state().revision().as_str().into(),
            update.stamp().delta_version(),
            update.state().record().provider_note.clone(),
        ));

        if update.stamp().delta_version() == 1
            && let Some(gate) = &self.first_write_gate
        {
            gate.reached.notify_one();
            gate.release.notified().await;
        }

        let mut current = self.current.lock().unwrap();
        if current.revision() != update.state().revision() {
            return Ok(CatalogSyncWriteOutcome::Rejected);
        }
        let step = self.steps.lock().unwrap().pop_front();
        let next_revision = format!("{}+", current.revision().as_str());
        let state = |entry: CatalogEntry, stamp: CatalogProjectionStamp, record: Record| {
            let digest = record.owned.digest();
            CatalogSyncState::new(
                entry,
                CatalogRevision::new(next_revision.clone()).unwrap(),
                stamp,
                digest,
                record,
            )
            .unwrap()
        };

        match step {
            Some(WriteStep::RejectAfterUnrelatedChange) => {
                let mut record = current.record().clone();
                record.provider_note = "concurrent owner note".into();
                *current = state(current.entry().clone(), current.stamp().clone(), record);
                Ok(CatalogSyncWriteOutcome::Rejected)
            }
            Some(WriteStep::RejectAfterReplacement) => {
                *current = state(
                    current.entry().clone().with_provider_id("replacement"),
                    current.stamp().clone(),
                    current.record().clone(),
                );
                Ok(CatalogSyncWriteOutcome::Rejected)
            }
            Some(WriteStep::Reject) => Ok(CatalogSyncWriteOutcome::Rejected),
            Some(WriteStep::AcknowledgeWithoutApplying) => Ok(CatalogSyncWriteOutcome::Applied),
            Some(WriteStep::ApplyAfterReplacement) => {
                *current = state(
                    current.entry().clone().with_provider_id("replacement"),
                    update.stamp().clone(),
                    Record {
                        owned: update.projection().clone(),
                        provider_note: current.record().provider_note.clone(),
                    },
                );
                Ok(CatalogSyncWriteOutcome::Applied)
            }
            Some(WriteStep::ApplyWithoutChangingRevision) => {
                let owned = update.projection().clone();
                *current = CatalogSyncState::new(
                    current.entry().clone(),
                    current.revision().clone(),
                    update.stamp().clone(),
                    owned.digest(),
                    Record {
                        owned,
                        provider_note: current.record().provider_note.clone(),
                    },
                )
                .unwrap();
                Ok(CatalogSyncWriteOutcome::Applied)
            }
            Some(WriteStep::LoseAckWithoutApplying) => Err(CatalogLifecycleError::OutcomeUnknown {
                table: current.entry().identifier().clone(),
                attempted_stamp: None,
                source: Box::new(std::io::Error::other("response lost")),
            }),
            Some(WriteStep::ApplyAndLoseAck) | None => {
                *current = state(
                    current.entry().clone(),
                    update.stamp().clone(),
                    Record {
                        owned: update.projection().clone(),
                        provider_note: update.state().record().provider_note.clone(),
                    },
                );
                Ok(match step {
                    Some(WriteStep::ApplyAndLoseAck) => CatalogSyncWriteOutcome::Unknown,
                    _ => CatalogSyncWriteOutcome::Applied,
                })
            }
        }
    }
}

struct Fixture {
    directory: tempfile::TempDir,
    location: Url,
    id: TableIdentifier,
    delta_id: String,
    original: Projection,
}

impl Fixture {
    async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let location = Url::from_directory_path(directory.path()).unwrap();
        let table = create_table(&location).await;
        let snapshot = table.snapshot().unwrap();
        Self {
            directory,
            location,
            id: TableIdentifier::new(vec!["schema".into()], "table").unwrap(),
            delta_id: snapshot.metadata().id().into(),
            original: Projection::from_snapshot(
                &snapshot.schema(),
                snapshot.metadata().partition_columns(),
            ),
        }
    }

    async fn add_column(&self, name: &str) -> Version {
        let table = crate::open_table(self.location.clone()).await.unwrap();
        let table = table
            .add_columns()
            .with_fields([StructField::new(name, DataType::STRING, true)])
            .await
            .unwrap();
        table.snapshot().unwrap().version()
    }

    fn catalog(&self, version: Version, steps: impl IntoIterator<Item = WriteStep>) -> TestCatalog {
        let digest = self.original.digest();
        let entry = CatalogEntry::new(self.id.clone(), self.location.clone())
            .unwrap()
            .with_provider_id("original")
            .with_delta_id(&self.delta_id);
        TestCatalog {
            current: Mutex::new(
                CatalogSyncState::new(
                    entry,
                    CatalogRevision::new("first-revision").unwrap(),
                    CatalogProjectionStamp::new(
                        CATALOG_PROJECTION_FORMAT_VERSION,
                        &self.delta_id,
                        version,
                        &digest,
                    )
                    .unwrap(),
                    digest,
                    Record {
                        owned: self.original.clone(),
                        provider_note: "owner note".into(),
                    },
                )
                .unwrap(),
            ),
            steps: Mutex::new(steps.into_iter().collect()),
            calls: Mutex::new(Vec::new()),
            first_write_gate: None,
            supported: true,
        }
    }

    fn delta_log(&self) -> Vec<(std::ffi::OsString, Vec<u8>)> {
        let mut files: Vec<_> = std::fs::read_dir(self.directory.path().join("_delta_log"))
            .unwrap()
            .map(|entry| entry.unwrap())
            .filter(|entry| entry.file_type().unwrap().is_file())
            .map(|entry| (entry.file_name(), std::fs::read(entry.path()).unwrap()))
            .collect();
        files.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
        files
    }
}

#[tokio::test]
async fn advances_the_catalog_from_one_delta_snapshot_after_read_back() {
    let fixture = Fixture::new().await;
    let version = fixture.add_column("writer").await;
    let before = fixture.delta_log();
    let lifecycle = CatalogLifecycle::new(fixture.catalog(0, []));
    let result = lifecycle.sync_table_metadata(&fixture.id).await.unwrap();
    let CatalogSyncOutcome::Updated(stamp) = result else {
        panic!("expected a confirmed update, got {result:?}");
    };
    let state = lifecycle.backend.current.lock().unwrap();
    assert_eq!(stamp.delta_id(), fixture.delta_id);
    assert_eq!(stamp.delta_version(), version);
    assert_eq!(stamp.digest(), state.record().owned.digest());
    assert_eq!(state.record().owned.fields, ["id", "region", "writer"]);
    assert_eq!(state.record().owned.partitions, ["region"]);
    assert_eq!(state.record().provider_note, "owner note");
    assert_eq!(
        *lifecycle.backend.calls.lock().unwrap(),
        [
            Call::Read("first-revision".into()),
            Call::Write("first-revision".into(), version, "owner note".into()),
            Call::Read("first-revision+".into()),
        ]
    );
    assert_eq!(fixture.delta_log(), before);
}

#[tokio::test]
async fn treats_matching_current_projection_as_unchanged() {
    let fixture = Fixture::new().await;
    let lifecycle = CatalogLifecycle::new(fixture.catalog(0, []));
    assert!(matches!(
        lifecycle.sync_table_metadata(&fixture.id).await.unwrap(),
        CatalogSyncOutcome::Unchanged(_)
    ));
    assert_eq!(
        *lifecycle.backend.calls.lock().unwrap(),
        [Call::Read("first-revision".into())]
    );
}

#[tokio::test]
async fn rejects_drift_from_the_equal_version_stamp() {
    let fixture = Fixture::new().await;
    let lifecycle = CatalogLifecycle::new(fixture.catalog(0, []));
    {
        let mut state = lifecycle.backend.current.lock().unwrap();
        let mut drifted = state.record().clone();
        drifted.owned.fields.push("out_of_band".into());
        *state = CatalogSyncState::new(
            state.entry().clone(),
            state.revision().clone(),
            state.stamp().clone(),
            drifted.owned.digest(),
            drifted,
        )
        .unwrap();
    }
    assert!(matches!(
        lifecycle.sync_table_metadata(&fixture.id).await,
        Err(CatalogLifecycleError::Conflict { .. })
    ));
    assert_eq!(lifecycle.backend.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn never_regresses_a_higher_verified_watermark() {
    let fixture = Fixture::new().await;
    let lifecycle = CatalogLifecycle::new(fixture.catalog(1, []));
    assert!(matches!(
        lifecycle.sync_table_metadata(&fixture.id).await.unwrap(),
        CatalogSyncOutcome::Superseded(stamp) if stamp.delta_version() == 1
    ));
    assert_eq!(lifecycle.backend.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn retries_definite_rejection_with_the_fresh_raw_record_and_revision() {
    let fixture = Fixture::new().await;
    let version = fixture.add_column("writer").await;
    let lifecycle =
        CatalogLifecycle::new(fixture.catalog(0, [WriteStep::RejectAfterUnrelatedChange]));
    assert!(matches!(
        lifecycle.sync_table_metadata(&fixture.id).await.unwrap(),
        CatalogSyncOutcome::Updated(stamp) if stamp.delta_version() == version
    ));
    assert_eq!(
        *lifecycle.backend.calls.lock().unwrap(),
        [
            Call::Read("first-revision".into()),
            Call::Write("first-revision".into(), version, "owner note".into()),
            Call::Read("first-revision+".into()),
            Call::Write(
                "first-revision+".into(),
                version,
                "concurrent owner note".into()
            ),
            Call::Read("first-revision++".into()),
        ]
    );
    assert_eq!(
        lifecycle
            .backend
            .current
            .lock()
            .unwrap()
            .record()
            .provider_note,
        "concurrent owner note"
    );
}

#[tokio::test]
async fn does_not_retry_the_same_rejected_revision_or_a_replacement_binding() {
    let fixture = Fixture::new().await;
    let version = fixture.add_column("writer").await;
    for (step, observed_revision) in [
        (WriteStep::Reject, "first-revision"),
        (WriteStep::RejectAfterReplacement, "first-revision+"),
    ] {
        let lifecycle = CatalogLifecycle::new(fixture.catalog(0, [step]));
        assert!(matches!(
            lifecycle.sync_table_metadata(&fixture.id).await,
            Err(CatalogLifecycleError::Conflict { .. })
        ));
        assert_eq!(
            *lifecycle.backend.calls.lock().unwrap(),
            [
                Call::Read("first-revision".into()),
                Call::Write("first-revision".into(), version, "owner note".into()),
                Call::Read(observed_revision.into()),
            ]
        );
    }
}

#[tokio::test]
async fn limits_definitive_conflicts_to_one_retry() {
    let fixture = Fixture::new().await;
    let version = fixture.add_column("writer").await;
    let lifecycle = CatalogLifecycle::new(fixture.catalog(
        0,
        [
            WriteStep::RejectAfterUnrelatedChange,
            WriteStep::RejectAfterUnrelatedChange,
        ],
    ));
    assert!(matches!(
        lifecycle.sync_table_metadata(&fixture.id).await,
        Err(CatalogLifecycleError::Conflict { .. })
    ));
    assert_eq!(
        *lifecycle.backend.calls.lock().unwrap(),
        [
            Call::Read("first-revision".into()),
            Call::Write("first-revision".into(), version, "owner note".into()),
            Call::Read("first-revision+".into()),
            Call::Write(
                "first-revision+".into(),
                version,
                "concurrent owner note".into()
            ),
        ]
    );
}

#[tokio::test]
async fn confirms_applied_unknown_updates_and_does_not_replay_ambiguous_results() {
    let fixture = Fixture::new().await;
    let version = fixture.add_column("writer").await;
    for (step, applied, reread) in [
        (WriteStep::ApplyAndLoseAck, true, "first-revision+"),
        (
            WriteStep::AcknowledgeWithoutApplying,
            false,
            "first-revision",
        ),
        (WriteStep::ApplyAfterReplacement, false, "first-revision+"),
        (
            WriteStep::ApplyWithoutChangingRevision,
            false,
            "first-revision",
        ),
        (WriteStep::LoseAckWithoutApplying, false, "first-revision"),
    ] {
        let lost_reply = matches!(step, WriteStep::LoseAckWithoutApplying);
        let lifecycle = CatalogLifecycle::new(fixture.catalog(0, [step]));
        let result = lifecycle.sync_table_metadata(&fixture.id).await;
        if applied {
            assert!(matches!(result, Ok(CatalogSyncOutcome::Updated(_))));
        } else {
            let Err(CatalogLifecycleError::OutcomeUnknown {
                table,
                attempted_stamp: Some(stamp),
                source,
            }) = result
            else {
                panic!("unconfirmed sync must identify the dispatched projection");
            };
            let mut projection = fixture.original.clone();
            projection.fields.push("writer".into());
            assert_eq!(table, fixture.id);
            assert_eq!(stamp.delta_id(), fixture.delta_id);
            assert_eq!(stamp.delta_version(), version);
            assert_eq!(stamp.digest(), projection.digest());
            if lost_reply {
                assert_eq!(source.to_string(), "response lost");
            }
        }
        assert_eq!(
            *lifecycle.backend.calls.lock().unwrap(),
            [
                Call::Read("first-revision".into()),
                Call::Write("first-revision".into(), version, "owner note".into()),
                Call::Read(reread.into()),
            ]
        );
    }
}

#[tokio::test]
async fn refuses_an_unsupported_provider_before_dispatch() {
    let fixture = Fixture::new().await;
    let mut catalog = fixture.catalog(0, []);
    catalog.supported = false;
    let lifecycle = CatalogLifecycle::new(catalog);
    assert!(matches!(
        lifecycle.sync_table_metadata(&fixture.id).await,
        Err(CatalogLifecycleError::Unsupported { .. })
    ));
    assert!(lifecycle.backend.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn older_concurrent_writer_cannot_regress_the_catalog() {
    let fixture = Fixture::new().await;
    assert_eq!(fixture.add_column("first").await, 1);
    let mut catalog = fixture.catalog(0, []);
    catalog.first_write_gate = Some(FirstWriteGate::default());
    let lifecycle = CatalogLifecycle::new(catalog);
    let gate = lifecycle.backend.first_write_gate.as_ref().unwrap();

    let older = lifecycle.sync_table_metadata(&fixture.id);
    let newer = async {
        gate.reached.notified().await;
        assert_eq!(fixture.add_column("second").await, 2);
        let result = lifecycle.sync_table_metadata(&fixture.id).await;
        gate.release.notify_one();
        result
    };
    let (older, newer) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(older, newer)
    })
    .await
    .expect("older writer must reach its conditional update");
    assert!(matches!(
        newer.unwrap(),
        CatalogSyncOutcome::Updated(stamp) if stamp.delta_version() == 2
    ));
    assert!(matches!(
        older.unwrap(),
        CatalogSyncOutcome::Unchanged(stamp) if stamp.delta_version() == 2
    ));
    let state = lifecycle.backend.current.lock().unwrap();
    assert_eq!(
        state.record().owned.fields,
        ["id", "region", "first", "second"]
    );
    assert_eq!(
        lifecycle
            .backend
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|call| matches!(call, Call::Write(..)))
            .count(),
        2
    );
}

#[test]
fn rejects_empty_revisions_and_unrecognized_or_incomplete_projection_stamps() {
    assert!(CatalogRevision::new("").is_err());
    assert!(CatalogProjectionStamp::new(2, "delta-id", 1, "digest").is_err());
    assert!(CatalogProjectionStamp::new(1, "", 1, "digest").is_err());
    assert!(CatalogProjectionStamp::new(1, "delta-id", 1, "").is_err());
}

#[tokio::test]
async fn rejects_a_stamp_for_a_different_delta_id() {
    let fixture = Fixture::new().await;
    let lifecycle = CatalogLifecycle::new(fixture.catalog(0, []));
    {
        let mut state = lifecycle.backend.current.lock().unwrap();
        *state = CatalogSyncState::new(
            state.entry().clone(),
            state.revision().clone(),
            CatalogProjectionStamp::new(
                CATALOG_PROJECTION_FORMAT_VERSION,
                "catalog-stamp-for-another-log",
                state.stamp().delta_version(),
                state.stamp().digest(),
            )
            .unwrap(),
            state.observed_digest(),
            state.record().clone(),
        )
        .unwrap();
    }
    assert!(matches!(
        lifecycle.sync_table_metadata(&fixture.id).await,
        Err(CatalogLifecycleError::Conflict { .. })
    ));
    assert_eq!(lifecycle.backend.calls.lock().unwrap().len(), 1);
}
