"""Register, load and synchronize existing Delta tables through a catalog."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from os import PathLike, fspath
from typing import Literal

from deltalake._internal import RawCatalog
from deltalake.table import DeltaTable

__all__ = [
    "Catalog",
    "ProjectionStamp",
    "RegistrationResult",
    "SyncResult",
    "load_catalog",
]


@dataclass(frozen=True)
class RegistrationResult:
    """The verified outcome of registering an existing Delta table."""

    status: Literal["created", "already_registered"]
    table: DeltaTable
    provider_id: str | None
    delta_id: str


@dataclass(frozen=True)
class ProjectionStamp:
    """Identity and version of the committed Delta metadata in the catalog."""

    format_version: int
    delta_id: str
    delta_version: int
    digest: str


@dataclass(frozen=True)
class SyncResult:
    """The confirmed outcome of reconciling a catalog entry with its Delta log."""

    status: Literal["updated", "unchanged", "superseded"]
    stamp: ProjectionStamp


class Catalog:
    """A catalog client with provider configuration and Delta storage options."""

    def __init__(
        self,
        provider: Literal["glue", "unity"],
        *,
        options: Mapping[str, str] | None = None,
        storage_options: Mapping[str, str] | None = None,
        sync_profile: Literal["oss_rust_server_0_0_2"] | None = None,
    ) -> None:
        self._storage_options = dict(storage_options or {})
        self._catalog = RawCatalog(
            provider,
            dict(options or {}),
            self._storage_options,
            sync_profile=sync_profile,
        )

    @property
    def supports_guarded_sync(self) -> bool:
        """Whether this instance is configured for a qualified guarded sync profile."""
        return self._catalog.supports_guarded_sync

    def load_table(self, identifier: tuple[str, ...]) -> DeltaTable:
        """Load the committed Delta snapshot for a catalog table name.

        Glue identifiers are tuples of database and table name. Unity identifiers
        are tuples of catalog, schema and table name.
        """
        return DeltaTable._from_raw(
            self._catalog.load_table(identifier), self._storage_options
        )

    def register_table(
        self, identifier: tuple[str, ...], location: str | PathLike[str]
    ) -> RegistrationResult:
        """Register a readable Delta table at an existing storage location.

        Registering the same binding again returns `already_registered`.
        A different binding raises `CatalogConflictError`. An ambiguous
        response with no confirmed read-back raises `CatalogOutcomeUnknownError`.
        """
        status, table, provider_id, delta_id = self._catalog.register_table(
            identifier, fspath(location)
        )
        return RegistrationResult(
            status,
            DeltaTable._from_raw(table, self._storage_options),
            provider_id,
            delta_id,
        )

    def sync_table_metadata(self, identifier: tuple[str, ...]) -> SyncResult:
        """Publish the committed Delta metadata projection to a qualified catalog.

        The catalog write is guarded by a fresh provider revision and confirmed
        by a separate read. An unsupported table or instance raises
        `CatalogUnsupportedError`. A competing binding or revision raises
        `CatalogConflictError`. A write with an unconfirmed outcome raises
        `CatalogOutcomeUnknownError`. Reconciliation only writes to the catalog.
        """
        status, format_version, delta_id, delta_version, digest = (
            self._catalog.sync_table_metadata(identifier)
        )
        return SyncResult(
            status, ProjectionStamp(format_version, delta_id, delta_version, digest)
        )


def load_catalog(
    provider: Literal["glue", "unity"],
    *,
    options: Mapping[str, str] | None = None,
    storage_options: Mapping[str, str] | None = None,
    sync_profile: Literal["oss_rust_server_0_0_2"] | None = None,
) -> Catalog:
    """Open a Glue or Unity catalog.

    Glue uses the standard AWS SDK configuration and accepts an optional
    `catalog_id` in `options`. Unity accepts provider options such as
    `workspace_url` and `access_token` or their environment equivalents.
    `storage_options` configures access to Delta storage.

    Set `sync_profile` only for a matching Unity OSS Rust server 0.0.2 deployment.
    Registration and load are available with the default configuration.
    """
    return Catalog(
        provider,
        options=options,
        storage_options=storage_options,
        sync_profile=sync_profile,
    )
