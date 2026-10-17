use crate::TestResult;
use fleet_sync_protocol::PeerId;
use sqlx_turso::{TursoConnection, sqlx::ConnectOptions};
use std::path::Path;

pub(crate) async fn lock_mailbox(directory: &Path, mailbox: PeerId) -> TestResult<TursoConnection> {
    let mut connection = sqlx_turso::TursoConnectOptions::new()
        .filename(directory.join(format!("delivery-mailbox-{}.db", mailbox.as_uuid())))
        .create_if_missing(false)
        .connect()
        .await?;
    sqlx_core::query::query::<sqlx_turso::Turso>("BEGIN IMMEDIATE")
        .execute(&mut connection)
        .await?;
    Ok(connection)
}
