//! Validate the old wire records before applying the typed storage corpus.
use crate::{LeaseMigrationError, validate_legacy_rows};
use durable_steps::storage::TransactionScope;
use sqlx_core::{
    arguments::IntoArguments,
    column::ColumnIndex,
    connection::Connection,
    database::Database,
    decode::Decode,
    encode::Encode,
    executor::Executor,
    migrate::{Migrate, Migration, Migrator},
    types::Type,
};

pub struct LeaseMigrator {
    sql: Migrator,
}
pub static LEASE_MIGRATOR: LeaseMigrator = LeaseMigrator {
    sql: sqlx::migrate!("./migrations"),
};
impl LeaseMigrator {
    /// SQL fragments for hosts that run `validate_legacy_rows` in their corpus transaction.
    pub fn sql_migrations(&self) -> impl DoubleEndedIterator<Item = &Migration> {
        self.sql.iter()
    }
    pub fn with_history_table(&self, name: &str) -> Self {
        let mut sql = Migrator::with_migrations(self.sql.iter().cloned().collect::<Vec<_>>());
        sql.dangerous_set_table_name(name.to_owned());
        Self { sql }
    }
    #[expect(
        clippy::manual_async_fn,
        reason = "explicit Send keeps the connection-backed migration usable in host transactions"
    )]
    pub fn run_direct<'a, C>(
        &'a self,
        target: Option<i64>,
        connection: &'a mut C,
        skip: bool,
    ) -> impl std::future::Future<Output = Result<(), LeaseMigrationError>> + Send + 'a
    where
        C: Connection + Migrate,
        for<'q> &'q mut C: Executor<'q, Database = C::Database>,
        <C::Database as Database>::Arguments: IntoArguments<C::Database>,
        for<'v> String: Type<C::Database> + Encode<'v, C::Database>,
        for<'v> Option<String>: Type<C::Database> + Decode<'v, C::Database>,
        for<'r> &'r str: ColumnIndex<<C::Database as Database>::Row>,
    {
        async move {
            let mut transaction = if connection.is_in_transaction() {
                connection.begin().await?
            } else {
                connection.begin_with("BEGIN IMMEDIATE").await?
            };
            let result = async {
                if target.is_none_or(|version| version >= 5) {
                    self.sql
                        .run_direct(Some(4), &mut *transaction, skip)
                        .await?;
                    validate_legacy_rows(&mut ConnectionScope(&mut *transaction)).await?;
                }
                self.sql.run_direct(target, &mut *transaction, skip).await?;
                Ok(())
            }
            .await;
            match result {
                Ok(()) => {
                    transaction.commit().await?;
                    Ok(())
                }
                Err(error) => {
                    transaction.rollback().await?;
                    Err(error)
                }
            }
        }
    }
}
struct ConnectionScope<'a, C>(&'a mut C);
impl<C> TransactionScope for ConnectionScope<'_, C>
where
    C: Connection,
    for<'q> &'q mut C: Executor<'q, Database = C::Database>,
{
    type Database = C::Database;
    type Executor<'q>
        = &'q mut C
    where
        Self: 'q;
    fn executor(&mut self) -> Self::Executor<'_> {
        self.0
    }
}

#[cfg(test)]
#[path = "lease_migration_test.rs"]
mod tests;
