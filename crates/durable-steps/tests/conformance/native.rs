use crate::conformance::{self, BusinessScope};
use durable_steps::{
    AtomicStore, Host, StoreError,
    storage::TransactionScope,
    turso_store::{TursoScope, TursoStore, TursoStoreOptions},
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

struct ChildCountRow {
    count: i64,
}

#[tokio::test]
async fn turso_options_apply_foreign_keys_before_transactions_and_keep_default_enforcement()
-> Result<(), StoreError> {
    for foreign_keys in [true, false] {
        let directory = tempfile::tempdir().map_err(StoreError::backend)?;
        let path = directory.path().join("journal.db");
        let host = crate::support::TestHost::new();
        let store = if foreign_keys {
            TursoStore::open_and_migrate(path, host).await?
        } else {
            TursoStore::open_with(
                path,
                TursoStoreOptions {
                    foreign_keys: false,
                },
                host,
            )
            .await?
        };
        store
            .transaction(|scope| {
                Box::pin(async move {
                    scope
                        .executor()
                        .execute(include_str!("migrations/1002_foreign_keys.sql"))
                        .await?;
                    Ok(())
                })
            })
            .await?;
        let enforced = store
            .transaction(|scope| {
                Box::pin(async move {
                    match sqlx_turso::query_file!("tests/conformance/queries/insert_fk_child.sql")
                        .execute(scope.executor())
                        .await
                    {
                        Ok(_) => Ok(false),
                        Err(error)
                            if error.as_database_error().is_some_and(|error| {
                                error.kind() == sqlx_core::error::ErrorKind::ForeignKeyViolation
                            }) =>
                        {
                            Ok(true)
                        }
                        Err(error) => Err(StoreError::backend(error)),
                    }
                })
            })
            .await?;
        conformance::check(
            enforced == foreign_keys,
            "connection FK policy must enforce by default and allow orphan apply only when explicitly disabled",
        )?;
        let count = store
            .transaction(|scope| {
                Box::pin(async move {
                    Ok(sqlx_turso::query_file_as!(
                        ChildCountRow,
                        "tests/conformance/queries/fk_child_count.sql"
                    )
                    .fetch_one(scope.executor())
                    .await?
                    .count)
                })
            })
            .await?;
        conformance::check(
            count == i64::from(!foreign_keys),
            "FK policy controls the actual committed business row",
        )?;
    }
    Ok(())
}
