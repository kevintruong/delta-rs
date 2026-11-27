"""Publish a Delta schema to Unity with a qualified shared strong validator."""

import argparse

from deltalake.catalog import load_catalog
from deltalake.exceptions import (
    CatalogConflictError,
    CatalogOutcomeUnknownError,
    CatalogUnsupportedError,
)
from deltalake.schema import Field, PrimitiveType


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("location", help="Path or URI of an existing Delta table")
    parser.add_argument("catalog")
    parser.add_argument("schema")
    parser.add_argument("table")
    parser.add_argument("--column", default="carrier", help="String column to add")
    parser.add_argument(
        "--sync-only",
        action="store_true",
        help="Reconcile a previously committed version",
    )
    args = parser.parse_args()

    identifier = (args.catalog, args.schema, args.table)
    catalog = load_catalog("unity", sync_profile="oss_rust_server_0_0_2")
    if not args.sync_only:
        registration = catalog.register_table(identifier, args.location)
        table = registration.table
        table.alter.add_columns(Field(args.column, PrimitiveType("string")))
        print(f"Committed Delta version {table.version()}")

    try:
        result = catalog.sync_table_metadata(identifier)
    except CatalogUnsupportedError as error:
        raise SystemExit(
            "The target lacks the required guarded sync capability. Stock Unity 0.0.2 omits the General Tables validator."
        ) from error
    except CatalogConflictError as error:
        raise SystemExit(
            "Inspect the catalog binding and Delta identity before reconciling."
        ) from error
    except CatalogOutcomeUnknownError as error:
        raise SystemExit(
            "Inspect the catalog metadata, then rerun with --sync-only."
        ) from error

    print(f"Catalog {result.status} at Delta version {result.stamp.delta_version}")


if __name__ == "__main__":
    main()
