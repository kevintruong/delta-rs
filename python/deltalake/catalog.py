"""Register existing Delta tables and load them through a catalog."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from os import PathLike, fspath
from typing import Literal

from deltalake._internal import RawCatalog
from deltalake.table import DeltaTable

__all__ = ["Catalog", "RegistrationResult", "load_catalog"]


@dataclass(frozen=True)
class RegistrationResult:
    """The verified outcome of registering an existing Delta table."""

    status: Literal["created", "already_registered"]
    table: DeltaTable
    provider_id: str | None
    delta_id: str


class Catalog:
    """A catalog client with provider configuration and Delta storage options."""

    def __init__(
        self,
        provider: Literal["glue", "unity"],
        *,
        options: Mapping[str, str] | None = None,
        storage_options: Mapping[str, str] | None = None,
    ) -> None:
        self._storage_options = dict(storage_options or {})
        self._catalog = RawCatalog(provider, dict(options or {}), self._storage_options)

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


def load_catalog(
    provider: Literal["glue", "unity"],
    *,
    options: Mapping[str, str] | None = None,
    storage_options: Mapping[str, str] | None = None,
) -> Catalog:
    """Open a Glue or Unity catalog.

    Glue uses the standard AWS SDK configuration and accepts an optional
    `catalog_id` in `options`. Unity accepts provider options such as
    `workspace_url` and `access_token` or their environment equivalents.
    `storage_options` configures access to Delta storage.
    """
    return Catalog(provider, options=options, storage_options=storage_options)
