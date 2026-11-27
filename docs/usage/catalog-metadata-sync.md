# Syncing catalog metadata

Commit metadata changes to the Delta table first. Call
`Catalog.sync_table_metadata(identifier)` to publish the committed projection to
the catalog. Each guarded attempt loads a committed Delta snapshot and
reconciles the metastore entry under provider version and identity checks. The
Delta log is the source of truth throughout reconciliation.

| Provider | Registration and load | Guarded metadata sync |
| --- | --- | --- |
| Unity OSS Rust server 0.0.2 | Available | External Delta tables on the explicit `oss_rust_server_0_0_2` profile using the Delta REST API 1.0 |
| Other Unity deployments | Available on compatible Unity Tables APIs | Requires separate qualification |
| AWS Glue | Available on builds with the Glue catalog backend | Requires live AWS qualification |

Select `sync_profile="oss_rust_server_0_0_2"` only for a deployment matching the
qualified server. `catalog.supports_guarded_sync` reports whether the instance
has a configured profile. The provider still checks the endpoint and table at
runtime. Use the qualified instance for new registration so it can write a
projection stamp. Existing records without a stamp require explicit adoption.

The Unity profile supports unpartitioned tables with `long` and `string`
columns and empty field metadata. It publishes the schema, table comment, and
sync stamp. `SyncResult` includes a status (`updated`, `unchanged`, or
`superseded`) and the confirmed stamp with its Delta table ID, version, format
version, and digest.

## Run the Unity cookbook

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
