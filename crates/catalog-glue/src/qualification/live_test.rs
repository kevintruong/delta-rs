#[path = "../../tests/support/live_glue.rs"]
mod live_glue;

use std::collections::HashMap;

use aws_sdk_glue::Client;
use aws_sdk_glue::types::{StorageDescriptor, TableInput};
use deltalake_core::DeltaTableBuilder;
use deltalake_core::data_catalog::{
    CATALOG_PROJECTION_FORMAT_VERSION, CatalogLifecycle, CatalogLifecycleError,
    CatalogProjectionStamp, CatalogSyncOutcome, CatalogSyncWriter, TableIdentifier,
};

use crate::GlueDataCatalog;
use crate::registration::DELTA_PROVIDER;
use crate::sync::{GlueMetadataProjection, stamp_parameters};
use live_glue::{
    LiveTarget, ProbeResult, Scope, copy_native_input, delete_owned_table, get_native_delta_table,
    version,
};

const MARKER: &str = "delta-rs.live-fixture";

#[tokio::test]
#[ignore = "requires a disposable AWS Glue database and readable S3 Delta history"]
async fn live_guarded_updates() -> ProbeResult<()> {
    let target = LiveTarget::load().await?;
    let id = TableIdentifier::new(vec![target.database.clone()], target.name.clone())?;
    let client = Client::new(&target.config);
    let source = || {
        DeltaTableBuilder::from_url(target.location.clone())
            .map(|builder| builder.with_storage_options(target.storage_options()))
    };
    let current = source()?.load().await?;
    let snapshot = current.snapshot()?;
    if snapshot.version() == 0 {
        return Err("fixture needs at least two committed Delta versions".into());
    }
    let previous = source()?.with_version(0).load().await?;
    let previous = previous.snapshot()?;
    let delta_id = snapshot.metadata().id();
    if previous.metadata().id() != delta_id {
        return Err("the two Delta snapshots must have the same table ID".into());
    }
    let old = GlueMetadataProjection::from_delta(
        &previous.schema(),
        previous.metadata().partition_columns(),
        previous.metadata().description(),
    )?;
    let next = GlueMetadataProjection::from_delta(
        &snapshot.schema(),
        snapshot.metadata().partition_columns(),
        snapshot.metadata().description(),
    )?;
    let previous_stamp = CatalogProjectionStamp::new(
        CATALOG_PROJECTION_FORMAT_VERSION,
        delta_id,
        previous.version(),
        old.digest()?,
    )?;
    let desired = CatalogProjectionStamp::new(
        CATALOG_PROJECTION_FORMAT_VERSION,
        delta_id,
        snapshot.version(),
        next.digest()?,
    )?;
    if previous_stamp.digest() == desired.digest() {
        return Err(
            "fixture needs a change to a Glue-projected field since Delta version zero".into(),
        );
    }
    let scope = Scope {
        catalog: &target.catalog,
        database: &target.database,
        table: &target.name,
        location: current.table_url().as_str(),
        delta_id,
    };
    let mut params = HashMap::from([
        (DELTA_PROVIDER.to_owned(), "delta".to_owned()),
        (MARKER.to_owned(), target.name.clone()),
    ]);
    stamp_parameters(&mut params, &previous_stamp);
    let input = TableInput::builder()
        .name(&target.name)
        .table_type("EXTERNAL_TABLE")
        .set_description(old.description)
        .set_parameters(Some(params))
        .storage_descriptor(
            StorageDescriptor::builder()
                .location(scope.location)
                .set_columns(Some(old.columns))
                .build(),
        )
        .set_partition_keys(Some(old.partitions))
        .build()?;
    eprintln!(
        "Glue guarded-update fixture {}/{}/{} on {} (caller account {})",
        target.catalog, target.database, target.name, target.endpoint, target.account
    );
    let created = client
        .create_table()
        .catalog_id(&target.catalog)
        .database_name(&target.database)
        .table_input(input)
        .send()
        .await;
    let result = match created {
        Ok(_) => exercise(&target, &client, &scope, &id, &desired, &next).await,
        Err(err)
            if err
                .as_service_error()
                .is_some_and(|e| e.is_already_exists_exception()) =>
        {
            return Err(err.into());
        }
        Err(err) => Err(err.into()),
    };
    let cleanup = delete_owned_table(&client, &scope, Some((MARKER, scope.table))).await;
    match (result, cleanup) {
        (Err(test), Err(cleanup)) => Err(format!("{test}; cleanup also failed: {cleanup}").into()),
        (test, cleanup) => test.and(cleanup),
    }
}

async fn exercise(
    target: &LiveTarget,
    client: &Client,
    scope: &Scope<'_>,
    id: &TableIdentifier,
    desired: &CatalogProjectionStamp,
    next: &GlueMetadataProjection,
) -> ProbeResult<()> {
    let initial = get_native_delta_table(client, scope).await?;
    let mut backend = GlueDataCatalog::with_config(&target.config).with_catalog_id(&target.catalog);
    if !matches!(
        backend.get_sync_state(id).await,
        Err(CatalogLifecycleError::Unsupported { .. })
    ) {
        return Err("the default Glue client unexpectedly enabled guarded sync".into());
    }
    // This unit-test client only receives the name created above. No same-name
    // replacement is performed and this run cannot produce a qualification report.
    backend.qualified_sync = Some(super::QualifiedGlue {
        client: client.clone(),
        lake_formation_registered: initial.is_registered_with_lake_formation(),
        catalog_id: target.catalog.clone(),
        database: target.database.clone(),
    });
    let lifecycle = CatalogLifecycle::new(backend).with_storage_options(target.storage_options());
    let outcome = lifecycle.sync_table_metadata(id).await?;
    if !matches!(outcome, CatalogSyncOutcome::Updated(ref stamp) if stamp == desired) {
        return Err(format!(
            "expected the current Delta projection to be published, got {outcome:?}"
        )
        .into());
    }
    let after = get_native_delta_table(client, scope).await?;
    if version(&after)? == version(&initial)?
        || after.description() != next.description.as_deref()
        || after.storage_descriptor().map(|sd| sd.columns()) != Some(next.columns.as_slice())
        || after.partition_keys() != next.partitions
        || after
            .parameters()
            .and_then(|p| p.get(MARKER))
            .map(String::as_str)
            != Some(scope.table)
    {
        return Err(
            "Glue did not preserve the expected fields or the unrelated fixture parameter".into(),
        );
    }
    if !matches!(lifecycle.sync_table_metadata(id).await?, CatalogSyncOutcome::Unchanged(ref stamp) if stamp == desired)
    {
        return Err("second sync did not return the same confirmed projection".into());
    }
    let unchanged = get_native_delta_table(client, scope).await?;
    if version(&unchanged)? != version(&after)? {
        return Err("second sync wrote another Glue version".into());
    }
    let external = format!("external edit {}", target.name);
    let change = copy_native_input(&after).description(&external).build()?;
    let update = |token: &str| {
        client
            .update_table()
            .catalog_id(scope.catalog)
            .database_name(scope.database)
            .name(scope.table)
            .table_input(change.clone())
            .version_id(token)
    };
    match update(version(&initial)?).send().await {
        Err(err)
            if err
                .as_service_error()
                .is_some_and(|e| e.is_concurrent_modification_exception()) => {}
        Err(err) => return Err(err.into()),
        Ok(_) => return Err("Glue accepted the stale same-table update".into()),
    }
    let preserved = get_native_delta_table(client, scope).await?;
    if version(&preserved)? != version(&after)? || preserved.description() != after.description() {
        return Err("the rejected stale update changed the catalog record".into());
    }
    update(version(&after)?).send().await?;
    let edited = get_native_delta_table(client, scope).await?;
    if !matches!(
        lifecycle.sync_table_metadata(id).await,
        Err(CatalogLifecycleError::Conflict { .. })
    ) {
        return Err("expected a conflict after an external edit to an owned field".into());
    }
    let final_record = get_native_delta_table(client, scope).await?;
    if final_record.description() != Some(external.as_str())
        || version(&final_record)? != version(&edited)?
    {
        return Err("guarded sync changed the external edit".into());
    }
    println!(
        "delta_version={} glue_versions={}/{}/{} sync=updated repeat=unchanged stale=ConcurrentModificationException external_edit=conflict",
        desired.delta_version(),
        version(&initial)?,
        version(&after)?,
        version(&final_record)?
    );
    Ok(())
}
