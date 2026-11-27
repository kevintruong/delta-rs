# Syncing catalog metadata

Commit metadata changes to the Delta table first. Call
`Catalog.sync_table_metadata(identifier)` to publish the committed projection to
the catalog. Each guarded attempt loads a committed Delta snapshot and
reconciles the metastore entry under provider version and identity checks. The
Delta log is the source of truth throughout reconciliation.

| Provider | Registration and load | Guarded metadata sync |
| --- | --- | --- |
| Stock Unity OSS Rust server 0.0.2 | Available | Unavailable. General Tables omits a strong HTTP ETag. |
| Other Unity deployments | Available on compatible Unity Tables APIs | Requires separate qualification |
| AWS Glue | Available on builds with the Glue catalog backend | Requires live AWS qualification |

To exercise the Glue registration and lookup APIs against AWS, configure a
disposable Glue database and an S3 Delta fixture, then run `make test-glue-live`.
The separate VersionId test checks whether a deployment can safely synchronize
metadata. [Live AWS Glue test setup](../../crates/catalog-glue/tests/qualification/README.md)

The `oss_rust_server_0_0_2` option enables protocol negotiation and allows
registration to seed a projection stamp. `catalog.supports_guarded_sync` reports
whether this option was selected. Each sync checks the table at runtime.

Guarded writes require the General Tables response to carry one strong HTTP
ETag covering the full representation. Its opaque value must equal the Delta
metadata ETag for the same table UUID. The deployment must enforce that shared
revision atomically across every table writer. A missing, weak, or malformed
validator produces `CatalogUnsupportedError`. A different valid revision
produces `CatalogConflictError`. Both outcomes stop the write.

Stock 0.0.2 derives the Delta ETag from a millisecond timestamp. Distinct
General Tables edits can share that timestamp. Adding an HTTP ETag from that
timestamp in a proxy cannot establish the required guarantee. The positive
guarded-sync fixtures exercise the shared-validator protocol. Production
deployments with this guarantee require separate qualification. Existing
records without a projection stamp require explicit adoption.
[Unity 0.0.2 ETag computation](https://github.com/open-lakehouse/unitycatalog-rs/blob/7ab5d624fc1ce2b6ba5bbba21ab286c196980082/crates/uc-delta/src/table.rs#L35)
and [General Tables update](https://github.com/open-lakehouse/unitycatalog-rs/blob/7ab5d624fc1ce2b6ba5bbba21ab286c196980082/crates/uc-metastore/src/table/sql.rs#L474).

The Unity profile supports unpartitioned tables with `long` and `string`
columns and empty field metadata. It publishes the schema, table comment, and
sync stamp. `SyncResult` includes a status (`updated`, `unchanged`, or
`superseded`) and the confirmed stamp with its Delta table ID, version, format
version, and digest.

## Run the Unity cookbook

Run the publishing example only on a deployment qualified for the shared strong
validator contract. The example commits the schema change to Delta before it
synchronizes the catalog. On stock 0.0.2, only registration and load are available.

With a supported Unity catalog and schema, an existing Delta table, and your
Unity connection configured through the environment, run the example from the
repository root. Configure `UNITY_ACCESS_TOKEN` or another supported Unity
credential before running it. A disposable loopback server can use an HTTP
origin with `UNITY_ALLOW_HTTP_URL=true`.

```sh
export UNITY_WORKSPACE_URL=https://unity.example.com
python docs/src/python/catalog_metadata_sync.py /mnt/orders unity default orders
```

The example registers the table, commits a supported `carrier` column to its
Delta log, and explicitly publishes that committed schema. Locations accessible
to both delta-rs and the catalog deployment are required.

- `CatalogUnsupportedError` indicates an unavailable profile, endpoint, or
  table shape. Check the configured deployment and its supported fields.
- `CatalogConflictError` indicates a conflicting catalog binding or revision.
  Inspect the entry and Delta identity before reconciling again.
- `CatalogOutcomeUnknownError` indicates that fresh read-back could not confirm
  the write. Inspect the catalog record and rerun with `--sync-only` to reconcile
  the existing Delta commit.

```sh
python docs/src/python/catalog_metadata_sync.py /mnt/orders unity default orders --sync-only
```
