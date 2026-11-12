//! The shared scope-migration cases on native Turso.
use crate::scope_migration_cases::{CASES, run};
use durable_steps::{StoreError, turso_store::TursoStore};
use std::sync::Arc;

#[tokio::test]
async fn turso_scope_migrations() -> Result<(), StoreError> {
    let host = crate::support::TestHost::new();
    let directory = tempfile::tempdir().map_err(StoreError::backend)?;
    let store =
        TursoStore::open_and_migrate(directory.path().join("scope.db"), Arc::clone(&host)).await?;
    let passed = run(&store).await?;
    assert_eq!(passed, CASES);
    Ok(())
}
