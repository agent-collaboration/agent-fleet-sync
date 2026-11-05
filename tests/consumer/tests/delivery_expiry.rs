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
    ControlRequest, TestResult,
    delivery_node::{CustodySide, NodeCommand, peer},
};
use node::*;
use support::*;

#[tokio::test]
async fn offline_recipient_expires_at_the_injected_deadline_and_stays_expired_after_restart()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let relay_path = directory.path().join("relay");
    let item = item_id(810)?;
    let created = i64::try_from(item.as_uuid().as_u128() >> 80)?;
    let initial = SyncTime::from_unix_millis(created);
    let deadline = SyncTime::from_unix_millis(created + 600_000);
    let mut relay = ServiceProcess::start_at(&relay_path, initial).await?;
    let mut control = relay.connect("control", "Bearer consumer-control").await?;
    let mut sender = DeliveryNode::start(&directory.path().join("sender"), 2).await?;
    let mut recipient = DeliveryNode::start(&directory.path().join("recipient"), 4).await?;
    sender.connect(&relay).await?;
    sender
        .command(NodeCommand::Enqueue {
            item,
            recipient: peer(4)?,
            body: "offline until after expiry".into(),
            valid_until: Some(deadline),
        })
        .await?;
    let sent = sender.wait(item, CustodySide::Outbox, "sent").await?;
    assert_eq!(sent["outbox"]["state"]["value"]["version"], 1);
    let (_, bytes) = support::delivery_mail::relay_row(&mut control, item).await?;
    assert!(bytes > 0);
    // The actual registry runner is causally awakened after this clock advance.
    control_request(&mut control, ControlRequest::AdvanceClock { to: deadline }).await?;
    let expired = match sender.wait(item, CustodySide::Outbox, "expired").await {
        Ok(expired) => expired,
        Err(error) => {
            eprintln!(
                "expiry relay snapshot={}",
                serde_json::to_string(
                    &control_request(&mut control, ControlRequest::DeliverySnapshot).await?
                )?
            );
            eprintln!(
                "expiry service events={}",
                serde_json::to_string(
                    &control_request(&mut control, ControlRequest::ServiceEvents).await?
                )?
            );
            return Err(error);
        }
    };
    assert_eq!(expired["outbox"]["state"]["value"]["version"], 2);
    let (terminal, bytes) = support::delivery_mail::relay_row(&mut control, item).await?;
    assert_eq!(bytes, 0);
    assert_eq!(terminal["value"]["status"]["kind"], "expired");
    sender.command(NodeCommand::Disconnect).await?;
    relay.kill().await?;
    let (disk, bytes) = support::delivery_mail::stopped_relay_row(&relay_path, item).await?;
    assert_eq!(bytes, 0);
    assert!(disk["payload"].is_null());
    assert_eq!(disk["status_kind"], "expired");
    relay = ServiceProcess::start_at(&relay_path, deadline).await?;
    sender.connect(&relay).await?;
    recipient.connect(&relay).await?;
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
    let untouched = recipient.command(NodeCommand::Inspect { item }).await?;
    assert!(untouched["inbox"].is_null());
    assert_eq!(effect_count(&untouched, item), 0);
    let catchup = sender.command(NodeCommand::StatusFetch).await?;
    assert!(
        catchup["events"]
            .as_array()
            .ok_or("missing events")?
            .iter()
            .any(|event| event["item"] == item.as_uuid().to_string()
                && event["status"]["kind"] == "expired"
                && event["version"] == 2)
    );
    recipient.stop().await?;
    sender.stop().await?;
    relay.kill().await?;
    Ok(())
}
