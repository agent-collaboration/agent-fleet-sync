use crate::{
    conformance::check,
    journal_metadata::{history, metadata, operations},
    legacy_rows::{NATURAL_CONSTRAINTS, unique_failure},
    support::TestHost,
};
use durable_steps::{
    AtomicStore, StoreError, durable_object_store::DurableObjectStore, storage::TransactionScope,
};
use sqlx_core::{query::query, query_as::query_as, sql_str::AssertSqlSafe};
use sqlx_durable_object::Do;

#[derive(sqlx::FromRow)]
struct ColumnRow {
    name: String,
    pk: i64,
}

pub(crate) async fn run(store: &DurableObjectStore<TestHost>) -> Result<(), StoreError> {
    for (table, key) in [
        ("_fleet_sync_operations", "operation_id"),
        ("_fleet_sync_steps", "id"),
        ("_fleet_sync_schema", "id"),
    ] {
        store
            .transaction(move |scope| {
                Box::pin(async move {
                    let rows = query_as::<Do, ColumnRow>(AssertSqlSafe(format!(
                        "PRAGMA table_info({table})"
                    )))
                    .fetch_all(scope.executor())
                    .await?;
                    let keys = rows
                        .into_iter()
                        .filter(|row| row.pk > 0)
                        .map(|row| row.name)
                        .collect::<Vec<_>>();
                    check(
                        keys == vec![key.to_owned()],
                        "DO journal must have its actual UUIDv7 primary key",
                    )
                })
            })
            .await?;
    }
    let before = (
        operations(store).await?,
        history(store).await?,
        metadata(store).await?,
    );
    for &(sql, columns) in NATURAL_CONSTRAINTS {
        store
            .transaction(move |scope| {
                Box::pin(async move {
                    match query::<Do>(sql).execute(scope.executor()).await {
                        Err(error) => check(
                            unique_failure(&error.to_string(), columns),
                            "DO duplicate failed for a different reason",
                        )?,
                        Ok(_) => {
                            return Err(StoreError::Aborted {
                                reason: format!("missing DO UNIQUE on {columns}"),
                            });
                        }
                    }
                    Ok(())
                })
            })
            .await?;
        check(
            (
                operations(store).await?,
                history(store).await?,
                metadata(store).await?,
            ) == before,
            "failed DO duplicate changed the original rows",
        )?;
    }
    Ok(())
}
