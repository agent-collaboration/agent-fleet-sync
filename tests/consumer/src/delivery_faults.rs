use crate::TestResult;
use sqlx_turso::{TursoConnection, sqlx::ConnectOptions};
use std::path::Path;

pub(crate) async fn lock_relay(directory: &Path) -> TestResult<TursoConnection> {
    let mut connection = sqlx_turso::TursoConnectOptions::new()
        .filename(directory.join("registry.db"))
        .create_if_missing(false)
        .connect()
        .await?;
    sqlx_core::query::query::<sqlx_turso::Turso>("BEGIN IMMEDIATE")
        .execute(&mut connection)
        .await?;
    Ok(connection)
}
