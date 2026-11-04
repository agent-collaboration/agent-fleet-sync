#![allow(
    dead_code,
    reason = "shared custody helpers are used by separate native test binaries"
)]
use super::control_request;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::{ControlAnswer, ControlRequest, TestResult};
use fleet_sync_transport_websocket::WebSocketConnection;
use std::path::Path;

pub async fn relay_row(
    control: &mut WebSocketConnection,
    item: ItemId,
) -> TestResult<(serde_json::Value, u64)> {
    let ControlAnswer::DeliverySnapshot(snapshot) =
        control_request(control, ControlRequest::DeliverySnapshot).await?
    else {
        return Err("no relay snapshot".into());
    };
    let row = snapshot
        .relay
        .iter()
        .find(|row| row.category == "outbox" && row.identity == item.as_uuid().to_string())
        .ok_or("no relay row")?;
    Ok((
        serde_json::from_str(row.content.as_deref().ok_or("no mail record")?)?,
        row.payload_bytes
            .as_deref()
            .ok_or("no byte charge")?
            .parse()?,
    ))
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
    if outbox {
        let row = sqlx_core::query::query::<sqlx_turso::Turso>(
            "SELECT item_id,digest,status_kind,CAST(status_version AS TEXT) AS status_version,CAST(last_report_seq AS TEXT) AS last_report_seq,CAST(payload_bytes AS TEXT) AS payload_bytes FROM _fleet_sync_outbox_items WHERE item_id=?1",
        )
        .bind(item.as_uuid().to_string())
        .fetch_one(&mut db)
        .await?;
        let record = serde_json::json!({
            "item_id":row.try_get::<String,_>("item_id")?,
            "digest":row.try_get::<Option<String>,_>("digest")?,
            "status_kind":row.try_get::<Option<String>,_>("status_kind")?,
            "status_version":row.try_get::<Option<String>,_>("status_version")?,
            "last_report_seq":row.try_get::<String,_>("last_report_seq")?,
        });
        let bytes: String = row.try_get("payload_bytes")?;
        db.close().await?;
        return Ok(serde_json::json!({"record":record,"bytes":bytes}));
    }
    let sql = "SELECT json_object('item',item_id,'reportSeq',report_seq,'outcomeKind',outcome_kind) AS record,CAST(report_seq AS TEXT) AS payload_bytes FROM _fleet_sync_reports WHERE item_id=?1";
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

/// Read actual persisted typed columns; does not rebuild the rules' header.
pub async fn stopped_relay_row(
    directory: &Path,
    item: ItemId,
) -> TestResult<(serde_json::Value, u64)> {
    use sqlx_core::column::Column;
    use sqlx_turso::sqlx::{ConnectOptions, Connection, Row};
    let mut db = sqlx_turso::TursoConnectOptions::new()
        .filename(directory.join("service/registry.db"))
        .create_if_missing(false)
        .connect()
        .await?;
    let row = sqlx_core::query::query::<sqlx_turso::Turso>(
        "SELECT * FROM _fleet_sync_outbox_items WHERE item_id=?1",
    )
    .bind(item.as_uuid().to_string())
    .fetch_one(&mut db)
    .await?;
    let mut record = serde_json::Map::new();
    for column in row.columns() {
        let value = match row.try_get::<Option<i64>, _>(column.name()) {
            Ok(value) => serde_json::json!(value),
            Err(_) => serde_json::json!(row.try_get::<Option<String>, _>(column.name())?),
        };
        record.insert(column.name().into(), value);
    }
    let bytes = u64::try_from(row.try_get::<i64, _>("payload_bytes")?)?;
    db.close().await?;
    Ok((serde_json::Value::Object(record), bytes))
}
