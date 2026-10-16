use crate::conformance::{self, BusinessScope};
use durable_steps::{
    AtomicStore, Host, JournalScope, OperationContext, OperationFailure, StoreError, Task,
    storage::JournalStorage,
    storage::TransactionScope,
    turso_store::{TursoScope, TursoStore, TursoStoreOptions, migrate_journal},
};
use sqlx_core::executor::Executor;
use sqlx_turso::{
    TursoConnectOptions,
    sqlx::{ConnectOptions, Connection},
};
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

struct BorrowedTask;
impl Task for BorrowedTask {
    const KIND: &'static str = "borrowed-connection";
    const VERSION: u32 = 1;
    type Store = TursoStore<crate::support::TestHost>;
    type Input = u64;
    type Output = u64;
    async fn run<C: OperationContext<Store = Self::Store>>(
        &self,
        _context: &mut C,
        input: &u64,
    ) -> Result<u64, OperationFailure> {
        Ok(*input)
    }
}

#[tokio::test]
async fn borrowed_scope_shares_the_callers_commit_and_rollback() -> Result<(), StoreError> {
    let directory = tempfile::tempdir().map_err(StoreError::backend)?;
    let host = crate::support::TestHost::new();
    let mut connection = TursoConnectOptions::new()
        .filename(directory.path().join("borrowed.db"))
        .create_if_missing(true)
        .connect()
        .await
        .map_err(StoreError::backend)?;
    let mut migration = connection.begin_with("BEGIN IMMEDIATE").await?;
    migrate_journal(&mut migration).await?;
    migration
        .execute(include_str!("migrations/1000_business.sql"))
        .await?;
    migration.commit().await?;

    let mut committed = connection.begin_with("BEGIN IMMEDIATE").await?;
    let committed_id = {
        let mut scope = TursoScope::new(&mut committed, &*host);
        scope.write_counter("borrowed-committed", 41).await?;
        scope
            .admit::<BorrowedTask>(&conformance::key("borrowed-committed"), &41)
            .await
            .map_err(StoreError::backend)?
            .id()
    };
    committed.commit().await?;

    let mut rolled_back = connection.begin_with("BEGIN IMMEDIATE").await?;
    let rolled_back_id = {
        let mut scope = TursoScope::new(&mut rolled_back, &*host);
        scope.write_counter("borrowed-rolled-back", 42).await?;
        scope
            .admit::<BorrowedTask>(&conformance::key("borrowed-rolled-back"), &42)
            .await
            .map_err(StoreError::backend)?
            .id()
    };
    rolled_back.rollback().await?;

    let mut scope = TursoScope::new(&mut connection, &*host);
    conformance::check(
        scope.read_counter("borrowed-committed").await? == Some(41)
            && scope.operation(committed_id).await?.is_some(),
        "caller commit retains both business row and admitted operation",
    )?;
    conformance::check(
        scope.read_counter("borrowed-rolled-back").await?.is_none()
            && scope.operation(rolled_back_id).await?.is_none(),
        "caller rollback removes both business row and admitted operation",
    )
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
