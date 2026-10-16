use crate::conformance::{self, BusinessScope};
use durable_steps::{
    AtomicStore, Host, StoreError,
    storage::TransactionScope,
    turso_store::{TursoScope, TursoStore},
};
use sqlx_core::executor::Executor;
use std::sync::Arc;

struct CounterRow {
    value: i64,
}
impl<H: Host> BusinessScope for TursoScope<'_, H> {
    async fn read_counter(&mut self, name: &str) -> Result<Option<u64>, StoreError> {
        sqlx_turso::query_file_as!(
            CounterRow,
            "tests/conformance/queries/read_counter.sql",
            name
        )
        .fetch_optional(self.executor())
        .await?
        .map(|row| u64::try_from(row.value).map_err(StoreError::backend))
        .transpose()
    }
    async fn write_counter(&mut self, name: &str, value: u64) -> Result<(), StoreError> {
        let value = i64::try_from(value).map_err(StoreError::backend)?;
        sqlx_turso::query_file!("tests/conformance/queries/write_counter.sql", name, value)
            .execute(self.executor())
            .await?;
        Ok(())
    }
}

#[tokio::test]
async fn turso_shared_conformance() -> Result<(), StoreError> {
    let host = crate::support::TestHost::new();
    let directory = tempfile::tempdir().map_err(StoreError::backend)?;
    let store = Arc::new(
        TursoStore::open_and_migrate(directory.path().join("journal.db"), Arc::clone(&host))
            .await?,
    );
    store
        .transaction(|scope| {
            Box::pin(async move {
                scope
                    .executor()
                    .execute(include_str!("migrations/1000_business.sql"))
                    .await?;
                Ok(())
            })
        })
        .await?;
    let cases = conformance::run(store, host).await?;
    println!("Turso: {} shared cases passed: {cases:?}", cases.len());
    Ok(())
}
