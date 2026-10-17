#![cfg(feature = "test-support")]
#![allow(clippy::unwrap_used)]
#[path = "support/delivery_node.rs"]
#[expect(dead_code, reason = "shared helpers also serve process-kill journeys")]
mod node;
#[expect(
    dead_code,
    unused_imports,
    reason = "shared native process helpers serve other journeys"
)]
mod support;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::{
    ControlAnswer, ControlRequest, TestResult,
    delivery_node::{CustodySide, NodeCommand, peer},
};
use node::*;
use support::*;

fn now_millis() -> TestResult<i64> {
    Ok(i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?)
}

#[tokio::test]
async fn offline_recipient_expires_after_relay_restart_without_any_fetch() -> TestResult {
    let directory = tempfile::tempdir()?;
    let relay_path = directory.path().join("relay");
    let mut relay = ServiceProcess::start(&relay_path).await?;
    let mut sender = DeliveryNode::start(&directory.path().join("sender"), 2).await?;
    let recipient = DeliveryNode::start(&directory.path().join("recipient"), 4).await?;
    sender.connect(&relay).await?;
    let item = item_id(810)?;
    let deadline = now_millis()? + 5_000;
    sender
        .command(NodeCommand::Enqueue {
            item,
            recipient: peer(4)?,
            body: "expires while the recipient never connects".into(),
            valid_until: Some(SyncTime::from_unix_millis(deadline)),
        })
        .await?;
    let sent = sender.wait(item, CustodySide::Outbox, "sent").await?;
    assert_eq!(sent["outbox"]["state"]["value"]["version"], 1);
    sender.command(NodeCommand::Disconnect).await?;
    relay.kill().await?;
    let (before, bytes) = relay_row(&relay_path, item).await?;
    assert!(bytes > 0);
    assert_eq!(before["value"]["status"]["kind"], "sent");
    assert_eq!(before["value"]["fetched"], false);
    assert!(now_millis()? < deadline, "relay must restart before expiry");
    relay = ServiceProcess::start(&relay_path).await?;
    assert!(
        now_millis()? < deadline,
        "startup must rederive a future deadline"
    );
    sender.connect(&relay).await?;
    // The actual owner timer and publication signal finish this wait; no Fetch or wall-clock sleep.
    let expired = match tokio::time::timeout(
        std::time::Duration::from_secs(12),
        sender.wait(item, CustodySide::Outbox, "expired"),
    )
    .await
    {
        Ok(result) => result?,
        Err(error) => {
            let mut control = relay.connect("control", "Bearer consumer-control").await?;
            let snapshot = control_request(
                &mut control,
                ControlRequest::DeliverySnapshot {
                    mailbox: peer(4)?,
                    origin: peer(2)?,
                },
            )
            .await?;
            eprintln!(
                "OBS expiry timeout after deadline={deadline}, now={}: {}",
                now_millis()?,
                serde_json::to_string(&snapshot)?
            );
            return Err(error.into());
        }
    };
    assert_eq!(expired["outbox"]["state"]["value"]["version"], 2);
    let query = sender
        .command(NodeCommand::Query {
            mailbox: peer(4)?,
            items: vec![item],
        })
        .await?;
    assert_eq!(
        query["value"]["statuses"][0]["state"]["value"]["status"]["kind"],
        "expired"
    );
    assert_eq!(
        query["value"]["statuses"][0]["state"]["value"]["version"],
        2
    );
    let events = sender.command(NodeCommand::StatusFetch).await?;
    assert!(
        events["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["item"] == item.as_uuid().to_string()
                && event["version"] == 2
                && event["status"]["kind"] == "expired")
    );
    let mut control = relay.connect("control", "Bearer consumer-control").await?;
    let ControlAnswer::DeliverySnapshot(snapshot) = control_request(
        &mut control,
        ControlRequest::DeliverySnapshot {
            mailbox: peer(4)?,
            origin: peer(2)?,
        },
    )
    .await?
    else {
        return Err("missing mailbox snapshot".into());
    };
    let row = snapshot
        .mailbox
        .iter()
        .find(|row| row.category == "mailbox" && row.identity == item.as_uuid().to_string())
        .ok_or("missing expired row")?;
    assert_eq!(row.payload_bytes.as_deref(), Some("0"));
    let record: serde_json::Value =
        serde_json::from_str(row.content.as_deref().ok_or("missing record")?)?;
    assert!(record["value"]["payload"].is_null());
    assert_eq!(record["value"]["fetched"], false);
    assert!(record["value"]["releasedAt"].is_number());
    relay.kill().await?;
    let (disk, bytes) = relay_row(&relay_path, item).await?;
    assert_eq!(disk, record);
    assert_eq!(bytes, 0);
    eprintln!(
        "OBS decision81/E-R2: relay restarted before deadline; recipient never connected or fetched; sender, Query and StatusFetch Expired@2; actual killed relay file payload0, fetched=false"
    );
    recipient.stop().await?;
    sender.stop().await?;
    Ok(())
}

async fn relay_row(
    directory: &std::path::Path,
    item: ItemId,
) -> TestResult<(serde_json::Value, u64)> {
    use sqlx_turso::sqlx::{ConnectOptions, Connection, Row};
    let mut connection = sqlx_turso::TursoConnectOptions::new()
        .filename(
            directory
                .join("service")
                .join(format!("delivery-mailbox-{}.db", peer(4)?.as_uuid())),
        )
        .create_if_missing(false)
        .connect()
        .await?;
    let row = sqlx_core::query::query::<sqlx_turso::Turso>(
        "SELECT record,payload_bytes FROM _fleet_sync_mail_items WHERE item_id=?1",
    )
    .bind(item.as_uuid().to_string())
    .fetch_one(&mut connection)
    .await?;
    let result = (
        serde_json::from_str(&row.try_get::<String, _>("record")?)?,
        u64::try_from(row.try_get::<i64, _>("payload_bytes")?)?,
    );
    connection.close().await?;
    Ok(result)
}
