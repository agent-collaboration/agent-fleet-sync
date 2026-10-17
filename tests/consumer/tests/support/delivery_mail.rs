#![allow(dead_code, reason = "shared custody helpers are used by separate native test binaries")]
use super::control_request;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::{ControlAnswer, ControlRequest, TestResult};
use fleet_sync_transport_websocket::WebSocketConnection;
use std::path::Path;

pub async fn corrupt(
    control: &mut WebSocketConnection,
    mailbox: PeerId,
    item: ItemId,
) -> TestResult<(String, i64)> {
    match control_request(
        control,
        ControlRequest::CorruptDeliveryCustody { mailbox, item },
    )
    .await?
    {
        ControlAnswer::CorruptedDelivery {
            record,
            payload_bytes,
        } => Ok((record, payload_bytes)),
        _ => Err("no custody fault receipt".into()),
    }
}
pub async fn mail_row(
    control: &mut WebSocketConnection,
    mailbox: PeerId,
    origin: PeerId,
    item: ItemId,
) -> TestResult<(serde_json::Value, u64)> {
    let ControlAnswer::DeliverySnapshot(snapshot) = control_request(
        control,
        ControlRequest::DeliverySnapshot { mailbox, origin },
    )
    .await?
    else {
        return Err("no mailbox snapshot".into());
    };
    let row = snapshot
        .mailbox
        .iter()
        .find(|row| row.category == "mailbox" && row.identity == item.as_uuid().to_string())
        .ok_or("no mailbox row")?;
    Ok((
        serde_json::from_str(row.content.as_deref().ok_or("no mail record")?)?,
        row.payload_bytes
            .as_deref()
            .ok_or("no byte charge")?
            .parse()?,
    ))
}
pub async fn restore_stopped(
    directory: &Path,
    mailbox: PeerId,
    item: ItemId,
    original: (String, i64),
) -> TestResult {
    use sqlx_turso::sqlx::{ConnectOptions, Connection};
    let mut db = sqlx_turso::TursoConnectOptions::new()
        .filename(
            directory
                .join("service")
                .join(format!("delivery-mailbox-{}.db", mailbox.as_uuid())),
        )
        .create_if_missing(false)
        .connect()
        .await?;
    sqlx_core::query::query::<sqlx_turso::Turso>(
        "UPDATE _fleet_sync_mail_items SET record=?1,payload_bytes=?2 WHERE item_id=?3",
    )
    .bind(original.0)
    .bind(original.1)
    .bind(item.as_uuid().to_string())
    .execute(&mut db)
    .await?;
    db.close().await?;
    Ok(())
}
pub async fn stopped_node_row(
    directory: &Path,
    item: ItemId,
    outbox: bool,
) -> TestResult<serde_json::Value> {
    use sqlx_turso::sqlx::{ConnectOptions, Connection, Row};
    let mut db = sqlx_turso::TursoConnectOptions::new()
        .filename(directory.join("delivery-node.db"))
        .create_if_missing(false)
        .connect()
        .await?;
    let sql = if outbox {
        "SELECT record,payload_bytes FROM _fleet_sync_outbox_items WHERE item_id=?1"
    } else {
        "SELECT record,CAST(report_seq AS TEXT) AS payload_bytes FROM _fleet_sync_reports WHERE item_id=?1"
    };
    let row = sqlx_core::query::query::<sqlx_turso::Turso>(sql)
        .bind(item.as_uuid().to_string())
        .fetch_one(&mut db)
        .await?;
    let record: serde_json::Value = serde_json::from_str(&row.try_get::<String, _>("record")?)?;
    let bytes: String = match row.try_get::<String, _>("payload_bytes") {
        Ok(bytes) => bytes,
        Err(_) => row.try_get::<i64, _>("payload_bytes")?.to_string(),
    };
    db.close().await?;
    Ok(serde_json::json!({"record":record,"bytes":bytes}))
}
