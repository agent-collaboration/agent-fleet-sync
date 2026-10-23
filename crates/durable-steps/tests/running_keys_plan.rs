#![cfg(feature = "turso")]

use durable_steps::{StoreError, turso_store::migrate_journal};
use sqlx_turso::{
    Turso, TursoConnectOptions,
    sqlx::{ConnectOptions, Connection, Row, query},
};

#[tokio::test]
async fn running_key_query_uses_only_the_live_state_index() -> Result<(), StoreError> {
    let mut connection = TursoConnectOptions::new().connect().await?;
    let mut migration = connection.begin_with("BEGIN IMMEDIATE").await?;
    migrate_journal(&mut migration).await?;
    migration.commit().await?;

    let rows = query::<Turso>(concat!(
        "EXPLAIN QUERY PLAN ",
        include_str!("../queries/running_request_keys.sql")
    ))
    .fetch_all(&mut connection)
    .await?;
    let details = rows
        .iter()
        .map(|row| row.try_get::<String, _>("detail"))
        .collect::<Result<Vec<_>, _>>()?;
    println!("running request keys query plan: {details:?}");

    if !details
        .iter()
        .any(|detail| detail.contains("_fleet_sync_operations_due"))
    {
        return Err(StoreError::backend(format!(
            "running keys must use the state index: {details:?}"
        )));
    }
    if details.iter().any(|detail| {
        detail.contains("MULTI-INDEX")
            || detail.contains("sqlite_autoindex__fleet_sync_operations_2")
            || detail.contains("SCAN _fleet_sync_operations")
    }) {
        return Err(StoreError::backend(format!(
            "running keys must not traverse kind or table history: {details:?}"
        )));
    }
    connection.close().await?;
    Ok(())
}
