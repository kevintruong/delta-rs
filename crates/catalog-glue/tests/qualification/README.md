# Glue VersionId qualification

The scripted tests verify the SDK request sequence and the evidence checker. Live qualification is specific to an AWS principal, endpoint, region, catalog, database, and Lake Formation context. The Glue API documents `VersionId` and `ConcurrentModificationException`. This fixture measures their behavior on a native external Delta registration. [AWS Glue UpdateTable](https://docs.aws.amazon.com/glue/latest/webapi/API_UpdateTable.html)

Use a disposable Glue database and an existing, readable S3 Delta table in the target region. The selected profile needs permission to read the Delta log and to create, inspect, update, and delete the test's generated `delta_rs_cas_*` catalog entry. The entry points to the existing Delta location. The probe changes Glue metadata, reads the Delta log, and deletes only its disposable Glue entry. Keep data files and Delta log changes out of this qualification run.

```sh
DELTA_RS_GLUE_CAS_PROFILE=qualification \
DELTA_RS_GLUE_CAS_REGION=us-east-1 \
DELTA_RS_GLUE_CAS_CATALOG=123456789012 \
DELTA_RS_GLUE_CAS_DATABASE=disposable_glue_database \
DELTA_RS_GLUE_CAS_DELTA_URI=s3://disposable-bucket/fixtures/delta-table \
  cargo test -p deltalake-catalog-glue --test glue_registration -- \
  --ignored --exact qualification::live::live_glue_version_id_qualification --nocapture
```

The probe creates the entry through the catalog registration API. It reads the current `VersionId`, changes an unrelated parameter under that token, then submits a compound projection update using the stale token. A definitive conflict and an unchanged fresh record are required. The same compound update under the current token must round-trip the description and the format, version, and digest markers while preserving the unrelated parameter. The projection values are fixture data confined to the disposable record.

The probe recreates its entry with a distinct fixture marker and checks the original token against the replacement. A reused token fails qualification before any stale update is sent. Otherwise the original token must be rejected and the replacement must remain intact. If ownership cannot be verified during cleanup, the failure includes the generated target name printed at the start of the test.

A passing live run prints a JSON record after cleanup. It includes the resolved SDK version from `Cargo.lock`, the STS account, the Glue scope, the Lake Formation registration flag, observed versions and both AWS service error codes. The JSON record applies to that exact configuration. Only a passing live run qualifies a scope for guarded writes.

## Observed same-name replacement behavior

An authenticated AWS CLI 2.27.14 probe used disposable external Delta catalog metadata in `ap-southeast-1` outside Lake Formation. The same probe ran against [AWS Emulator 1.0.634](https://github.com/aws/aws-emulator). The AWS probe made zero S3 calls. Both probes verified cleanup.

| Probe step | AWS Glue and AWS Emulator |
| --- | --- |
| Update the initial entry with `VersionId` `0` | Accepted. A fresh read returned version `1`. |
| Repeat a write with `0` against version `1` | Rejected with `ConcurrentModificationException`. The newer record was intact. |
| Publish a column, description, and watermark with `1` | Accepted. A fresh read returned version `2` and preserved the unrelated parameter. |
| Delete and recreate the table at the same name | The replacement reused version `0`. |
| Write to the replacement with the original entry's `0` | Accepted. The write overwrote the replacement's metadata. |

The tested AWS scope fails the same-name replacement requirement. The official emulator reproduces the failure for local regression tests. Guarded writes remain disabled for this scope.
