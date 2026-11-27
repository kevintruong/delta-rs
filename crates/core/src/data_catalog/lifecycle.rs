use std::collections::HashMap;

use url::Url;

use crate::kernel::{Metadata, SchemaRef, StructType, Version};
use crate::table::{normalize_table_url, state::DeltaTableState};
use crate::{DeltaTable, DeltaTableError, open_table_with_storage_options};

/// A result from the catalog table lifecycle API.
pub type CatalogResult<T> = Result<T, CatalogLifecycleError>;

/// Errors from resolving, loading, or operating on catalog tables.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CatalogLifecycleError {
    /// The identifier has an empty namespace or an invalid component.
    #[error("invalid catalog table identifier")]
    InvalidIdentifier,

    /// The URL does not identify a catalog table root or read route.
    #[error(
        "catalog table locations must be hierarchical URLs without credentials, query, or fragment"
    )]
    InvalidLocation,

    /// The catalog does not contain the selected table.
    #[error("catalog table not found: {table:?}")]
    NotFound {
        /// Requested table.
        table: TableIdentifier,
    },

    /// The catalog binding or its Delta log belongs to a different table.
    #[error("catalog table conflicts with the requested binding: {table:?}")]
    Conflict {
        /// Requested table.
        table: TableIdentifier,
    },

    /// The provider could not perform the operation.
    #[error("catalog provider request failed")]
    Provider {
        /// Underlying provider error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// The selected catalog does not support guarded metadata synchronization.
    #[error("guarded catalog synchronization is unsupported for: {table:?}")]
    Unsupported {
        /// Requested table.
        table: TableIdentifier,
    },

    /// A revision or projection marker could not be interpreted safely.
    #[error("invalid guarded catalog synchronization state")]
    InvalidSyncState,

    /// A catalog mutation may have been applied, but its result could not be verified.
    #[error("unable to confirm the catalog operation outcome: {table:?}")]
    OutcomeUnknown {
        /// Requested table.
        table: TableIdentifier,
        /// Error from the mutation attempt or the subsequent confirmation.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// The selected storage does not provide a readable Delta snapshot.
    #[error("could not load the catalog table's Delta snapshot")]
    Delta(#[from] DeltaTableError),
}

/// A table name qualified by the provider's namespace components.
///
/// Components retain their original spelling. The provider interprets their number,
/// order, and case requirements.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TableIdentifier {
    namespace: Vec<String>,
    name: String,
}

impl TableIdentifier {
    /// Create a table identifier from a non-empty namespace and a table name.
    pub fn new(namespace: Vec<String>, name: impl Into<String>) -> CatalogResult<Self> {
        let name = name.into();
        if namespace.is_empty()
            || namespace.iter().any(|part| !valid_identifier_part(part))
            || !valid_identifier_part(&name)
        {
            return Err(CatalogLifecycleError::InvalidIdentifier);
        }
        Ok(Self { namespace, name })
    }

    /// Namespace components in provider order.
    pub fn namespace(&self) -> &[String] {
        &self.namespace
    }

    /// Name within the namespace.
    pub fn name(&self) -> &str {
        &self.name
    }
}

fn valid_identifier_part(part: &str) -> bool {
    !part.trim().is_empty()
        && !matches!(part, "." | "..")
        && !part.contains(['/', '\\'])
        && !part.chars().any(char::is_control)
}

/// A fresh observation of a Delta table's catalog binding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogEntry {
    identifier: TableIdentifier,
    location: Url,
    read_url: Option<Url>,
    provider_id: Option<String>,
    delta_id: Option<String>,
}

impl CatalogEntry {
    /// Bind a scoped catalog identifier to the physical table root.
    pub fn new(identifier: TableIdentifier, location: Url) -> CatalogResult<Self> {
        validate_location(&location)?;
        Ok(Self {
            identifier,
            location,
            read_url: None,
            provider_id: None,
            delta_id: None,
        })
    }

    /// Set a separate read route, such as a catalog URI that vends credentials.
    pub fn with_read_url(mut self, read_url: Url) -> CatalogResult<Self> {
        validate_location(&read_url)?;
        self.read_url = Some(read_url);
        Ok(self)
    }

    /// Set the provider record's opaque identity when the provider supplies one.
    pub fn with_provider_id(mut self, provider_id: impl Into<String>) -> Self {
        self.provider_id = Some(provider_id.into());
        self
    }

    /// Set a persisted Delta metadata ID recorded by the catalog when available.
    pub fn with_delta_id(mut self, delta_id: impl Into<String>) -> Self {
        self.delta_id = Some(delta_id.into());
        self
    }

    /// The catalog's scoped identifier.
    pub fn identifier(&self) -> &TableIdentifier {
        &self.identifier
    }

    /// The physical location recorded in the catalog.
    pub fn location(&self) -> &Url {
        &self.location
    }

    /// The table's read route. Defaults to its physical location.
    pub fn read_url(&self) -> &Url {
        self.read_url.as_ref().unwrap_or(&self.location)
    }

    /// The provider record identity, when present.
    pub fn provider_id(&self) -> Option<&str> {
        self.provider_id.as_deref()
    }

    /// The persisted identity of the Delta log, when present.
    pub fn delta_id(&self) -> Option<&str> {
        self.delta_id.as_deref()
    }
}

fn validate_location(location: &Url) -> CatalogResult<()> {
    // In ABFS URLs the username component names the container, not a credential.
    let has_username_credential =
        !location.username().is_empty() && !matches!(location.scheme(), "abfs" | "abfss");
    if location.cannot_be_a_base()
        || has_username_credential
        || location.password().is_some()
        || location.query().is_some()
        || location.fragment().is_some()
    {
        return Err(CatalogLifecycleError::InvalidLocation);
    }
    Ok(())
}

/// Provides fresh catalog entries and opens the specific Delta table they identify.
#[async_trait::async_trait]
pub trait CatalogReader: Send + Sync {
    /// Look up a Delta table by scoped name without using a cached catalog entry.
    ///
    /// Return `None` only when the table is absent. A binding to a non-Delta or
    /// otherwise incompatible record should be reported as a conflict.
    async fn get_table(&self, id: &TableIdentifier) -> CatalogResult<Option<CatalogEntry>>;

    /// Open the Delta table identified by this exact catalog observation.
    ///
    /// A provider may override this to vend credentials for the observed table.
    async fn open_table(
        &self,
        entry: &CatalogEntry,
        storage_options: &HashMap<String, String>,
    ) -> CatalogResult<DeltaTable> {
        Ok(
            open_table_with_storage_options(entry.read_url().clone(), storage_options.clone())
                .await?,
        )
    }
}

/// A resolved catalog entry with a readable, committed Delta snapshot.
#[derive(Debug)]
pub struct CatalogTable {
    entry: CatalogEntry,
    table: DeltaTable,
}

impl CatalogTable {
    /// The catalog binding used to load the table.
    pub fn entry(&self) -> &CatalogEntry {
        &self.entry
    }

    /// The committed Delta log version that was loaded.
    pub fn version(&self) -> Version {
        self.state().version()
    }

    /// Metadata, including the Delta table ID, from the loaded snapshot.
    pub fn metadata(&self) -> &Metadata {
        self.state().metadata()
    }

    /// Schema from the same loaded snapshot.
    pub fn schema(&self) -> SchemaRef {
        self.state().schema()
    }

    /// The loaded Delta table.
    pub fn table(&self) -> &DeltaTable {
        &self.table
    }

    /// Take ownership of the loaded Delta table.
    pub fn into_table(self) -> DeltaTable {
        self.table
    }

    fn state(&self) -> &DeltaTableState {
        self.table
            .snapshot()
            .expect("a catalog table is constructed from a loaded Delta snapshot")
    }
}

/// A provider-neutral interface to catalog tables.
pub struct CatalogLifecycle<B> {
    pub(super) backend: B,
    storage_options: HashMap<String, String>,
}

impl<B: CatalogReader> CatalogLifecycle<B> {
    /// Create a lifecycle client with a configured catalog backend.
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            storage_options: HashMap::new(),
        }
    }

    /// Set storage options for opening Delta snapshots.
    pub fn with_storage_options(mut self, storage_options: HashMap<String, String>) -> Self {
        self.storage_options = storage_options;
        self
    }

    /// Resolve a named table and load its most recent committed Delta snapshot.
    pub async fn load_table(&self, id: &TableIdentifier) -> CatalogResult<CatalogTable> {
        let entry = self
            .backend
            .get_table(id)
            .await?
            .ok_or_else(|| CatalogLifecycleError::NotFound { table: id.clone() })?;
        self.load_entry(id, entry).await
    }

    pub(super) async fn load_entry(
        &self,
        id: &TableIdentifier,
        entry: CatalogEntry,
    ) -> CatalogResult<CatalogTable> {
        if entry.identifier() != id {
            return Err(CatalogLifecycleError::Conflict { table: id.clone() });
        }
        let table = self
            .backend
            .open_table(&entry, &self.storage_options)
            .await?;
        let snapshot = table.snapshot()?;
        if entry
            .delta_id()
            .is_some_and(|delta_id| delta_id != snapshot.metadata().id())
        {
            return Err(CatalogLifecycleError::Conflict { table: id.clone() });
        }
        Ok(CatalogTable { entry, table })
    }
}

/// Registration inputs derived from one committed Delta snapshot.
#[derive(Debug)]
pub struct TableRegistration {
    identifier: TableIdentifier,
    location: Url,
    metadata: Metadata,
    schema: SchemaRef,
    version: Version,
}

impl TableRegistration {
    /// Scoped table name to register.
    pub fn identifier(&self) -> &TableIdentifier {
        &self.identifier
    }

    /// Physical root of the existing Delta table.
    pub fn location(&self) -> &Url {
        &self.location
    }

    /// Metadata action from the selected committed Delta snapshot.
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Schema from the selected committed Delta snapshot.
    pub fn schema(&self) -> &StructType {
        &self.schema
    }

    /// Version of the selected Delta snapshot.
    pub fn version(&self) -> Version {
        self.version
    }
}

/// Provider acknowledgement of an attempted create.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateTableOutcome {
    /// The provider acknowledged the create.
    Created,
    /// An entry with the same scoped name already exists.
    AlreadyExists,
    /// The provider did not establish whether the create was applied.
    Unknown,
}

/// Creates a catalog binding without writing to the Delta log.
#[async_trait::async_trait]
pub trait CatalogWriter: CatalogReader {
    /// Attempt one conditional create for the scoped name.
    ///
    /// A provider must preserve an existing binding and report its existence.
    /// Report a lost acknowledgement as `Unknown` or an `OutcomeUnknown`
    /// error with the original cause. Other errors are treated as definitive.
    async fn create_table(
        &self,
        registration: &TableRegistration,
    ) -> CatalogResult<CreateTableOutcome>;
}

/// Successfully reconciled outcome of an external table registration.
#[derive(Debug)]
pub enum RegistrationOutcome {
    /// The provider acknowledged creation and a fresh lookup verified the binding.
    Created(CatalogTable),
    /// A matching binding exists, without an acknowledgement that this call created it.
    AlreadyRegistered(CatalogTable),
}

impl<B: CatalogWriter> CatalogLifecycle<B> {
    /// Register an existing Delta table and verify the resulting catalog binding.
    ///
    /// A repeated registration of the same binding is idempotent. After a create
    /// attempt, the service reads the binding back without replaying the mutation.
    pub async fn register_table(
        &self,
        id: &TableIdentifier,
        location: Url,
    ) -> CatalogResult<RegistrationOutcome> {
        validate_location(&location)?;
        let source =
            open_table_with_storage_options(location.clone(), self.storage_options.clone()).await?;
        let snapshot = source.snapshot()?;
        let registration = TableRegistration {
            identifier: id.clone(),
            location,
            metadata: snapshot.metadata().clone(),
            schema: snapshot.schema(),
            version: snapshot.version(),
        };

        if let Some(entry) = self.backend.get_table(id).await? {
            return self
                .verify_registration(&registration, entry)
                .await
                .map(RegistrationOutcome::AlreadyRegistered);
        }

        let (created, create_error) = match self.backend.create_table(&registration).await {
            Ok(outcome) => (outcome == CreateTableOutcome::Created, None),
            Err(error @ CatalogLifecycleError::OutcomeUnknown { .. }) => (false, Some(error)),
            Err(error) => return Err(error),
        };
        let verified = match self.backend.get_table(id).await {
            Ok(Some(entry)) => self.verify_registration(&registration, entry).await,
            Ok(None) => Err(CatalogLifecycleError::NotFound { table: id.clone() }),
            Err(error) => Err(error),
        };
        let table = match verified {
            Ok(table) => table,
            Err(error @ CatalogLifecycleError::Conflict { .. }) => return Err(error),
            Err(error) => {
                return Err(create_error.unwrap_or_else(|| {
                    CatalogLifecycleError::OutcomeUnknown {
                        table: id.clone(),
                        source: Box::new(error),
                    }
                }));
            }
        };

        Ok(if created {
            RegistrationOutcome::Created(table)
        } else {
            RegistrationOutcome::AlreadyRegistered(table)
        })
    }

    async fn verify_registration(
        &self,
        registration: &TableRegistration,
        entry: CatalogEntry,
    ) -> CatalogResult<CatalogTable> {
        let expected_id = registration.metadata().id();
        let id = registration.identifier();
        if normalize_table_url(entry.location()) != normalize_table_url(registration.location())
            || entry
                .delta_id()
                .is_some_and(|delta_id| delta_id != expected_id)
        {
            return Err(CatalogLifecycleError::Conflict { table: id.clone() });
        }

        let table = self.load_entry(id, entry).await?;
        if table.metadata().id() != expected_id {
            return Err(CatalogLifecycleError::Conflict { table: id.clone() });
        }
        Ok(table)
    }
}
