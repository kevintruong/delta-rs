use std::cmp::Ordering;
use std::io;

use crate::kernel::Version;
use crate::table::normalize_table_url;

use super::{
    CatalogEntry, CatalogLifecycle, CatalogLifecycleError, CatalogReader, CatalogResult,
    CatalogTable, TableIdentifier,
};

/// Format of projection stamps written by guarded catalog synchronization.
pub const CATALOG_PROJECTION_FORMAT_VERSION: u32 = 1;

/// Opaque provider revision that guards a metadata write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogRevision(String);

impl CatalogRevision {
    /// Wrap a nonempty token supplied by a fresh provider read.
    pub fn new(value: impl Into<String>) -> CatalogResult<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(CatalogLifecycleError::InvalidSyncState);
        }
        Ok(Self(value))
    }

    /// Borrow the token without normalizing it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Identifies the committed Delta snapshot projected into a catalog record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogProjectionStamp {
    format_version: u32,
    delta_id: String,
    delta_version: Version,
    digest: String,
}

impl CatalogProjectionStamp {
    /// Validate a stamp from a versioned catalog record or a new projection.
    pub fn new(
        format_version: u32,
        delta_id: impl Into<String>,
        delta_version: Version,
        digest: impl Into<String>,
    ) -> CatalogResult<Self> {
        let delta_id = delta_id.into();
        let digest = digest.into();
        if format_version != CATALOG_PROJECTION_FORMAT_VERSION
            || delta_id.trim().is_empty()
            || digest.trim().is_empty()
        {
            return Err(CatalogLifecycleError::InvalidSyncState);
        }
        Ok(Self {
            format_version,
            delta_id,
            delta_version,
            digest,
        })
    }

    /// Encoding version for this stamp.
    pub fn format_version(&self) -> u32 {
        self.format_version
    }

    /// Persistent identity of the source Delta table.
    pub fn delta_id(&self) -> &str {
        &self.delta_id
    }

    /// Source Delta log version represented by this projection.
    pub fn delta_version(&self) -> Version {
        self.delta_version
    }

    /// Provider-comparable digest of the owned projection.
    pub fn digest(&self) -> &str {
        &self.digest
    }
}

/// A fresh catalog record with its atomic revision and projection marker.
#[derive(Clone)]
pub struct CatalogSyncState<R> {
    entry: CatalogEntry,
    revision: CatalogRevision,
    stamp: CatalogProjectionStamp,
    observed_digest: String,
    record: R,
}

impl<R> CatalogSyncState<R> {
    /// Construct an observation from one provider read.
    ///
    /// The digest is computed from the actual owned fields in that record. An
    /// observation can expose drift between those fields and the stored stamp.
    pub fn new(
        entry: CatalogEntry,
        revision: CatalogRevision,
        stamp: CatalogProjectionStamp,
        observed_digest: impl Into<String>,
        record: R,
    ) -> CatalogResult<Self> {
        let observed_digest = observed_digest.into();
        if observed_digest.trim().is_empty() {
            return Err(CatalogLifecycleError::InvalidSyncState);
        }
        Ok(Self {
            entry,
            revision,
            stamp,
            observed_digest,
            record,
        })
    }

    /// Binding observed in the catalog.
    pub fn entry(&self) -> &CatalogEntry {
        &self.entry
    }

    /// Revision supplied by this observation.
    pub fn revision(&self) -> &CatalogRevision {
        &self.revision
    }

    /// Marker recorded in the catalog.
    pub fn stamp(&self) -> &CatalogProjectionStamp {
        &self.stamp
    }

    /// Digest computed from the owned catalog fields.
    pub fn observed_digest(&self) -> &str {
        &self.observed_digest
    }

    /// Exact provider record used to merge unowned fields into the update.
    pub fn record(&self) -> &R {
        &self.record
    }
}

/// Provider-specific owned metadata derived from one committed Delta snapshot.
pub struct CatalogSyncProjection<P> {
    digest: String,
    projection: P,
}

impl<P> CatalogSyncProjection<P> {
    /// Pair the fields with a digest computed using the same encoding as read-back.
    pub fn new(digest: impl Into<String>, projection: P) -> CatalogResult<Self> {
        let digest = digest.into();
        if digest.trim().is_empty() {
            return Err(CatalogLifecycleError::InvalidSyncState);
        }
        Ok(Self { digest, projection })
    }
}

/// One guarded update built from a fresh provider observation and a Delta snapshot.
pub struct CatalogSyncUpdate<'a, R, P> {
    state: &'a CatalogSyncState<R>,
    stamp: &'a CatalogProjectionStamp,
    projection: &'a P,
}

impl<R, P> CatalogSyncUpdate<'_, R, P> {
    /// Provider observation supplying the exact precondition and the merge base.
    pub fn state(&self) -> &CatalogSyncState<R> {
        self.state
    }

    /// Marker to publish atomically with the owned projection.
    pub fn stamp(&self) -> &CatalogProjectionStamp {
        self.stamp
    }

    /// Provider-specific fields owned by this update.
    pub fn projection(&self) -> &P {
        self.projection
    }
}

/// Acknowledgement from one guarded catalog write attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CatalogSyncWriteOutcome {
    /// The provider acknowledged receipt and application.
    Applied,
    /// The provider definitively rejected the precondition.
    Rejected,
    /// The provider did not establish whether the write applied.
    Unknown,
}

/// A catalog adapter with an atomic revision precondition for metadata updates.
#[async_trait::async_trait]
pub trait CatalogSyncWriter: CatalogReader {
    /// Provider representation of the complete observed record.
    type Record: Send + Sync;

    /// Provider representation of the metadata fields owned by synchronization.
    type Projection: Send + Sync;

    /// Read the binding, revision, marker and owned-field digest in one observation.
    ///
    /// Return `Unsupported` for configurations without a qualified atomic write
    /// contract. Return `None` only for an absent record. Unstamped records
    /// require explicit adoption by the provider.
    async fn get_sync_state(
        &self,
        id: &TableIdentifier,
    ) -> CatalogResult<Option<CatalogSyncState<Self::Record>>>;

    /// Derive the fields and comparable digest from the loaded Delta snapshot.
    fn project_table(
        &self,
        table: &CatalogTable,
    ) -> CatalogResult<CatalogSyncProjection<Self::Projection>>;

    /// Attempt exactly one atomic update against the supplied observation.
    ///
    /// Preserve provider-owned fields from the observation's record and publish
    /// its new owned fields with the new stamp. Only return `Rejected` when the
    /// provider guarantees that the precondition failed and no write occurred.
    /// Return `Unknown` or an `OutcomeUnknown` error if the write may have
    /// applied. Other errors are treated as definitive failures.
    async fn update_table_metadata(
        &self,
        update: CatalogSyncUpdate<'_, Self::Record, Self::Projection>,
    ) -> CatalogResult<CatalogSyncWriteOutcome>;
}

/// Result of guarded metadata synchronization for a catalog binding.
#[derive(Debug, PartialEq, Eq)]
pub enum CatalogSyncOutcome {
    /// Read-back established that the requested projection is now published.
    Updated(CatalogProjectionStamp),
    /// The provider already had the requested projection.
    Unchanged(CatalogProjectionStamp),
    /// A corroborated provider projection represents a newer Delta version.
    Superseded(CatalogProjectionStamp),
}

impl<B: CatalogSyncWriter> CatalogLifecycle<B> {
    /// Synchronize only the provider fields owned by the adapter.
    ///
    /// Load a single committed Delta snapshot for each attempt. Allow one
    /// retry after a definitive rejection, using a fresh observation of the
    /// same binding and a different revision. After an ambiguous response,
    /// perform read-back and preserve an unknown result unless confirmed.
    pub async fn sync_table_metadata(
        &self,
        id: &TableIdentifier,
    ) -> CatalogResult<CatalogSyncOutcome> {
        let mut state = self.read_sync_state(id).await?;
        let mut can_retry = true;
        loop {
            let table = self.load_entry(id, state.entry.clone()).await?;
            if state.stamp.delta_id() != table.metadata().id()
                || state.observed_digest != state.stamp.digest()
            {
                return Err(conflict(id));
            }

            let ordering = state.stamp.delta_version().cmp(&table.version());
            if ordering == Ordering::Greater {
                return Ok(CatalogSyncOutcome::Superseded(state.stamp));
            }
            let projection = self.backend.project_table(&table)?;
            if ordering == Ordering::Equal {
                return if state.stamp.digest() == projection.digest {
                    Ok(CatalogSyncOutcome::Unchanged(state.stamp))
                } else {
                    Err(conflict(id))
                };
            }

            let stamp = CatalogProjectionStamp::new(
                CATALOG_PROJECTION_FORMAT_VERSION,
                table.metadata().id(),
                table.version(),
                projection.digest,
            )?;
            let result = self
                .backend
                .update_table_metadata(CatalogSyncUpdate {
                    state: &state,
                    stamp: &stamp,
                    projection: &projection.projection,
                })
                .await;
            match result {
                Ok(CatalogSyncWriteOutcome::Rejected) if can_retry => {
                    let fresh = self.read_sync_state(id).await?;
                    if !same_binding(&state, &fresh)
                        || state.revision == fresh.revision
                        || fresh.stamp.delta_version() < state.stamp.delta_version()
                    {
                        return Err(conflict(id));
                    }
                    can_retry = false;
                    state = fresh;
                }
                Ok(CatalogSyncWriteOutcome::Rejected) => return Err(conflict(id)),
                Ok(CatalogSyncWriteOutcome::Applied | CatalogSyncWriteOutcome::Unknown) => {
                    return self.confirm_sync(id, &state, &stamp, None).await;
                }
                Err(error @ CatalogLifecycleError::OutcomeUnknown { .. }) => {
                    return self.confirm_sync(id, &state, &stamp, Some(error)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn read_sync_state(
        &self,
        id: &TableIdentifier,
    ) -> CatalogResult<CatalogSyncState<B::Record>> {
        let state = self
            .backend
            .get_sync_state(id)
            .await?
            .ok_or_else(|| CatalogLifecycleError::NotFound { table: id.clone() })?;
        if state.entry.identifier() != id {
            return Err(conflict(id));
        }
        Ok(state)
    }

    async fn confirm_sync(
        &self,
        id: &TableIdentifier,
        previous: &CatalogSyncState<B::Record>,
        desired: &CatalogProjectionStamp,
        ambiguous_write: Option<CatalogLifecycleError>,
    ) -> CatalogResult<CatalogSyncOutcome> {
        let failure = match self.read_sync_state(id).await {
            Ok(after)
                if same_binding(previous, &after)
                    && previous.revision != after.revision
                    && after.observed_digest == after.stamp.digest() =>
            {
                if after.stamp == *desired {
                    return Ok(CatalogSyncOutcome::Updated(after.stamp));
                }
                if after.stamp.delta_version() > desired.delta_version() {
                    return Ok(CatalogSyncOutcome::Superseded(after.stamp));
                }
                Box::new(io::Error::other(
                    "catalog read-back did not confirm the projection",
                )) as Box<dyn std::error::Error + Send + Sync>
            }
            Ok(_) => Box::new(io::Error::other(
                "catalog read-back did not confirm the guarded binding and revision",
            )),
            Err(error) => Box::new(error),
        };
        Err(
            ambiguous_write.unwrap_or_else(|| CatalogLifecycleError::OutcomeUnknown {
                table: id.clone(),
                source: failure,
            }),
        )
    }
}

fn conflict(id: &TableIdentifier) -> CatalogLifecycleError {
    CatalogLifecycleError::Conflict { table: id.clone() }
}

fn same_binding<R>(before: &CatalogSyncState<R>, after: &CatalogSyncState<R>) -> bool {
    let before_entry = before.entry();
    let after_entry = after.entry();
    before_entry.identifier() == after_entry.identifier()
        && normalize_table_url(before_entry.location())
            == normalize_table_url(after_entry.location())
        && before_entry.provider_id() == after_entry.provider_id()
        && before_entry.delta_id() == after_entry.delta_id()
        && before.stamp.delta_id() == after.stamp.delta_id()
}
