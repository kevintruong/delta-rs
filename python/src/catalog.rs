use std::collections::HashMap;

use deltalake::data_catalog::{
    CatalogLifecycle, CatalogLifecycleError, CatalogSyncOutcome, RegistrationOutcome,
    TableIdentifier,
};
#[cfg(any(feature = "rustls", feature = "native-tls"))]
use deltalake::glue_catalog::GlueDataCatalog;
use deltalake::unity_catalog::{
    UnityCatalog, UnityCatalogBuilder, UnityCatalogConfigKey, UnitySyncProfile,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use url::Url;

use crate::error::{
    CatalogConflictError, CatalogError, CatalogOutcomeUnknownError, CatalogUnsupportedError,
    PythonError, TableNotFoundError,
};
use crate::{RawDeltaTable, rt};

enum CatalogBackend {
    Unity(Box<CatalogLifecycle<UnityCatalog>>),
    #[cfg(any(feature = "rustls", feature = "native-tls"))]
    Glue(Box<CatalogLifecycle<GlueDataCatalog>>),
}

#[pyclass(module = "deltalake._internal", frozen)]
pub(crate) struct RawCatalog {
    backend: CatalogBackend,
    storage_options: HashMap<String, String>,
    supports_guarded_sync: bool,
}

#[pymethods]
impl RawCatalog {
    #[new]
    #[pyo3(signature = (provider, options, storage_options, *, sync_profile=None))]
    fn new(
        py: Python<'_>,
        provider: &str,
        options: HashMap<String, String>,
        storage_options: HashMap<String, String>,
        sync_profile: Option<&str>,
    ) -> PyResult<Self> {
        let sync_profile = match (provider, sync_profile) {
            (_, None) => None,
            ("unity", Some("oss_rust_server_0_0_2")) => Some(UnitySyncProfile::OssRustServer002),
            _ => return Err(PyValueError::new_err("unsupported catalog sync profile")),
        };
        let (backend, supports_guarded_sync) = py.detach(|| match provider {
            "unity" => {
                let mut unity = build_unity(options)?;
                if let Some(profile) = sync_profile {
                    unity = unity.with_sync_profile(profile).map_err(catalog_error)?;
                }
                let supports_guarded_sync = unity.supports_guarded_sync();
                let lifecycle =
                    CatalogLifecycle::new(unity).with_storage_options(storage_options.clone());
                Ok((
                    CatalogBackend::Unity(Box::new(lifecycle)),
                    supports_guarded_sync,
                ))
            }
            "glue" => build_glue(options, &storage_options).map(|backend| (backend, false)),
            _ => Err(PyValueError::new_err(
                "unknown catalog provider, expected 'glue' or 'unity'",
            )),
        })?;
        Ok(Self {
            backend,
            storage_options,
            supports_guarded_sync,
        })
    }

    #[getter]
    fn supports_guarded_sync(&self) -> bool {
        self.supports_guarded_sync
    }

    fn load_table(
        &self,
        py: Python<'_>,
        identifier: &Bound<'_, PyTuple>,
    ) -> PyResult<RawDeltaTable> {
        let identifier = self.identifier(identifier.extract()?)?;
        let table = py
            .detach(|| {
                rt().block_on(async {
                    match &self.backend {
                        CatalogBackend::Unity(catalog) => catalog.load_table(&identifier).await,
                        #[cfg(any(feature = "rustls", feature = "native-tls"))]
                        CatalogBackend::Glue(catalog) => catalog.load_table(&identifier).await,
                    }
                })
            })
            .map_err(catalog_error)?;
        Ok(RawDeltaTable::from_table(
            table.into_table(),
            self.storage_options.clone(),
        ))
    }

    fn register_table(
        &self,
        py: Python<'_>,
        identifier: &Bound<'_, PyTuple>,
        location: &str,
    ) -> PyResult<(&'static str, RawDeltaTable, Option<String>, String)> {
        let identifier = self.identifier(identifier.extract()?)?;
        let outcome = py.detach(|| {
            let location =
                deltalake::table::builder::parse_table_uri(location).map_err(PythonError::from)?;
            rt().block_on(async {
                match &self.backend {
                    CatalogBackend::Unity(catalog) => {
                        catalog.register_table(&identifier, location).await
                    }
                    #[cfg(any(feature = "rustls", feature = "native-tls"))]
                    CatalogBackend::Glue(catalog) => {
                        catalog.register_table(&identifier, location).await
                    }
                }
            })
            .map_err(catalog_error)
        })?;
        let (status, table) = match outcome {
            RegistrationOutcome::Created(table) => ("created", table),
            RegistrationOutcome::AlreadyRegistered(table) => ("already_registered", table),
        };
        let provider_id = table.entry().provider_id().map(str::to_owned);
        let delta_id = table.metadata().id().to_owned();
        Ok((
            status,
            RawDeltaTable::from_table(table.into_table(), self.storage_options.clone()),
            provider_id,
            delta_id,
        ))
    }

    fn sync_table_metadata(
        &self,
        py: Python<'_>,
        identifier: &Bound<'_, PyTuple>,
    ) -> PyResult<(&'static str, u32, String, u64, String)> {
        let identifier = self.identifier(identifier.extract()?)?;
        let catalog = match &self.backend {
            CatalogBackend::Unity(catalog) if self.supports_guarded_sync => catalog,
            _ => {
                return Err(catalog_error(CatalogLifecycleError::Unsupported {
                    table: identifier,
                }));
            }
        };
        let outcome = py
            .detach(|| rt().block_on(catalog.sync_table_metadata(&identifier)))
            .map_err(catalog_error)?;
        let (status, stamp) = match outcome {
            CatalogSyncOutcome::Updated(stamp) => ("updated", stamp),
            CatalogSyncOutcome::Unchanged(stamp) => ("unchanged", stamp),
            CatalogSyncOutcome::Superseded(stamp) => ("superseded", stamp),
        };
        Ok((
            status,
            stamp.format_version(),
            stamp.delta_id().to_owned(),
            stamp.delta_version(),
            stamp.digest().to_owned(),
        ))
    }
}

impl RawCatalog {
    fn identifier(&self, mut identifier: Vec<String>) -> PyResult<TableIdentifier> {
        let expected = match &self.backend {
            CatalogBackend::Unity(_) => 3,
            #[cfg(any(feature = "rustls", feature = "native-tls"))]
            CatalogBackend::Glue(_) => 2,
        };
        if identifier.len() != expected {
            return Err(PyValueError::new_err(format!(
                "expected a catalog table identifier with {expected} components"
            )));
        }
        let name = identifier
            .pop()
            .expect("the identifier includes a table name");
        TableIdentifier::new(identifier, name).map_err(catalog_error)
    }
}

#[allow(deprecated)]
fn build_unity(options: HashMap<String, String>) -> PyResult<UnityCatalog> {
    let config_error =
        |err| PyValueError::new_err(format!("invalid Unity catalog configuration: {err}"));
    let mut builder = UnityCatalogBuilder::from_env();
    for (name, value) in options {
        let key = name
            .parse::<UnityCatalogConfigKey>()
            .map_err(config_error)?;
        if matches!(
            key,
            UnityCatalogConfigKey::Host | UnityCatalogConfigKey::WorkspaceUrl
        ) && !Url::parse(&value).is_ok_and(|url| {
            matches!(url.scheme(), "http" | "https")
                && url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
        }) {
            return Err(PyValueError::new_err(
                "invalid Unity catalog workspace_url, expected an HTTP(S) workspace URL",
            ));
        }
        if value.trim().is_empty() {
            return Err(PyValueError::new_err(format!(
                "invalid Unity catalog option {name:?}: expected a nonempty value"
            )));
        }
        builder = builder.try_with_option(name, value).map_err(config_error)?;
    }
    builder.build().map_err(config_error)
}

#[cfg(any(feature = "rustls", feature = "native-tls"))]
fn build_glue(
    mut options: HashMap<String, String>,
    storage_options: &HashMap<String, String>,
) -> PyResult<CatalogBackend> {
    let catalog_id = options.remove("catalog_id");
    if let Some(name) = options.keys().next() {
        return Err(PyValueError::new_err(format!(
            "unknown Glue catalog option {name:?}"
        )));
    }
    if catalog_id.as_deref().is_some_and(|id| id.trim().is_empty()) {
        return Err(PyValueError::new_err("Glue catalog_id cannot be empty"));
    }

    let mut catalog = rt()
        .block_on(GlueDataCatalog::from_env())
        .map_err(|error| CatalogError::new_err(error.to_string()))?;
    if let Some(catalog_id) = catalog_id {
        catalog = catalog.with_catalog_id(catalog_id);
    }
    Ok(CatalogBackend::Glue(Box::new(
        CatalogLifecycle::new(catalog).with_storage_options(storage_options.clone()),
    )))
}

#[cfg(not(any(feature = "rustls", feature = "native-tls")))]
fn build_glue(
    _options: HashMap<String, String>,
    _storage_options: &HashMap<String, String>,
) -> PyResult<CatalogBackend> {
    Err(CatalogUnsupportedError::new_err(
        "this build has no Glue catalog backend",
    ))
}

fn catalog_error(error: CatalogLifecycleError) -> PyErr {
    match error {
        CatalogLifecycleError::InvalidIdentifier | CatalogLifecycleError::InvalidLocation => {
            PyValueError::new_err(error.to_string())
        }
        CatalogLifecycleError::NotFound { .. } => TableNotFoundError::new_err(error.to_string()),
        CatalogLifecycleError::Conflict { .. } => CatalogConflictError::new_err(error.to_string()),
        CatalogLifecycleError::Unsupported { .. } => {
            CatalogUnsupportedError::new_err(error.to_string())
        }
        CatalogLifecycleError::OutcomeUnknown {
            ref table,
            ref attempted_stamp,
            ..
        } => {
            let unknown = CatalogOutcomeUnknownError::new_err(error.to_string());
            Python::attach(|py| -> PyResult<PyErr> {
                let mut identifier = table.namespace().to_vec();
                identifier.push(table.name().to_owned());
                let value = unknown.value(py);
                value.setattr("identifier", PyTuple::new(py, identifier)?)?;
                let stamp = match attempted_stamp {
                    Some(stamp) => Some(py.import("deltalake.catalog")?.call_method1(
                        "ProjectionStamp",
                        (
                            stamp.format_version(),
                            stamp.delta_id(),
                            stamp.delta_version(),
                            stamp.digest(),
                        ),
                    )?),
                    None => None,
                };
                value.setattr("attempted_stamp", stamp)?;
                Ok(unknown)
            })
            .unwrap_or_else(|error| error)
        }
        CatalogLifecycleError::Delta(error) => PythonError::from(error).into(),
        _ => CatalogError::new_err(error.to_string()),
    }
}
