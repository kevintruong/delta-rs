//! Register an existing S3 Delta table in Glue using the default AWS configuration.
//!
//! Run with `cargo run -p deltalake --features glue,s3 --example register_glue -- database table s3://bucket/path/`.

use deltalake::data_catalog::{CatalogLifecycle, RegistrationOutcome, TableIdentifier};
use deltalake::glue_catalog::GlueDataCatalog;
use url::Url;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let [database, table, location] = args.as_slice() else {
        return Err("usage: register_glue <database> <table> <s3-uri>".into());
    };

    let id = TableIdentifier::new(vec![database.clone()], table)?;
    let catalog = CatalogLifecycle::new(GlueDataCatalog::from_env().await?);
    let outcome = catalog.register_table(&id, Url::parse(location)?).await?;
    let (status, loaded) = match outcome {
        RegistrationOutcome::Created(table) => ("registered", table),
        RegistrationOutcome::AlreadyRegistered(table) => ("already registered", table),
    };
    println!(
        "{status} {database}.{table} at Delta version {}",
        loaded.version()
    );
    Ok(())
}
