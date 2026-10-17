#![cfg(feature = "test-support")]
#![allow(clippy::unwrap_used)]
#[path = "support/delivery_node.rs"]
#[expect(dead_code, reason = "shared helpers serve other journeys")]
mod node;
#[expect(
    dead_code,
    unused_imports,
    reason = "shared helpers serve other journeys"
)]
mod support;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::{
    delivery_node::{CustodySide, NodeCommand, peer},
    *,
};
use node::*;
use std::time::Duration;
use support::*;

async fn event(
    control: &mut fleet_sync_transport_websocket::WebSocketConnection,
    mailbox: PeerId,
) -> TestResult<ServiceEvent> {
    match control_request(
        control,
        ControlRequest::WaitMailboxEvent { mailbox, after: 0 },
    )
    .await?
    {
        ControlAnswer::ServiceEvent(event) => Ok(event),
        _ => Err("no service event".into()),
    }
}
async fn assert_body(sender: &mut DeliveryNode, item: ItemId) -> TestResult {
    let snapshot = sender.command(NodeCommand::Snapshot).await?;
    let row = snapshot
        .as_array()
        .ok_or("no snapshot")?
        .iter()
        .find(|row| row["category"] == "outbox" && row["identity"] == item.as_uuid().to_string())
        .ok_or("no outbox")?;
    assert!(
        row["payload_bytes"]
            .as_str()
            .ok_or("no byte charge")?
            .parse::<u64>()?
            > 0
    );
    let header: serde_json::Value =
        serde_json::from_str(row["content"].as_str().ok_or("no header")?)?;
    assert_ne!(header["value"]["status"]["kind"], "refused");
    Ok(())
}

#[tokio::test]
async fn submit_begin_failure_keeps_sender_custody_and_recovers_once_without_quarantining()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let mut relay = ServiceProcess::start(&directory.path().join("relay")).await?;
    let mut control = relay.connect("control", "Bearer consumer-control").await?;
    let mut origin = DeliveryNode::start(&directory.path().join("origin"), 2).await?;
    let mut recipient = DeliveryNode::start(&directory.path().join("recipient"), 4).await?;
    let mut other_origin = DeliveryNode::start(&directory.path().join("other-origin"), 6).await?;
    let mut other_recipient =
        DeliveryNode::start(&directory.path().join("other-recipient"), 8).await?;
    origin.connect(&relay).await?;
    recipient.connect(&relay).await?;
    other_origin.connect(&relay).await?;
    other_recipient.connect(&relay).await?;
    let seed = enqueue(&mut origin, 830, peer(4)?).await?;
    origin.wait(seed, CustodySide::Outbox, "sent").await?;
    control_request(
        &mut control,
        ControlRequest::HoldDeliveryWrite { mailbox: peer(4)? },
    )
    .await?;
    let item = enqueue(&mut origin, 831, peer(4)?).await?;
    let observed = event(&mut control, peer(4)?).await?;
    eprintln!("OBS A10 real BEGIN failure: {:?}", observed.fields);
    assert_ne!(
        observed.fields.get("message").map(String::as_str),
        Some("delivery mailbox unavailable"),
        "a backend lock is not corruption"
    );
    let pending = tokio::time::timeout(
        Duration::from_secs(12),
        origin.command(NodeCommand::WaitPendingCall {
            hop: "submit".into(),
            attempts: 1,
        }),
    )
    .await??;
    assert_body(&mut origin, item).await?;
    let other = enqueue(&mut other_origin, 832, peer(8)?).await?;
    other_recipient
        .command(NodeCommand::Receive {
            stream: DeliveryStream::Items,
        })
        .await?;
    assert_eq!(
        effect_count(
            &other_recipient
                .wait(other, CustodySide::Inbox, "delivered")
                .await?,
            other
        ),
        1
    );
    control_request(
        &mut control,
        ControlRequest::ReleaseDeliveryWrite { mailbox: peer(4)? },
    )
    .await?;
    origin.connect(&relay).await?;
    origin.wait(item, CustodySide::Outbox, "sent").await?;
    recipient
        .command(NodeCommand::Receive {
            stream: DeliveryStream::Items,
        })
        .await?;
    assert_eq!(
        effect_count(
            &recipient
                .wait(item, CustodySide::Inbox, "delivered")
                .await?,
            item
        ),
        1
    );
    origin.wait(item, CustodySide::Outbox, "delivered").await?;
    eprintln!(
        "OBS A10 Submit pending={pending}, body kept under BEGIN failure; backend released, exact item delivered once, other mailbox delivered during failure"
    );
    other_recipient.stop().await?;
    other_origin.stop().await?;
    recipient.stop().await?;
    origin.stop().await?;
    relay.kill().await?;
    Ok(())
}

#[tokio::test]
async fn deadline_scan_backend_lock_retries_after_recovery_and_other_mailboxes_stay_live()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let mut relay = ServiceProcess::start(&directory.path().join("relay")).await?;
    let mut control = relay.connect("control", "Bearer consumer-control").await?;
    let mut origin = DeliveryNode::start(&directory.path().join("origin"), 2).await?;
    origin.connect(&relay).await?;
    let item = item_id(840)?;
    origin
        .command(NodeCommand::Enqueue {
            item,
            recipient: peer(4)?,
            body: "scan backend recovers".into(),
            valid_until: Some(
                runtime_primitives::Clock::now(&runtime_primitives::SystemClock)
                    .after(Duration::from_secs(1)),
            ),
        })
        .await?;
    origin.wait(item, CustodySide::Outbox, "sent").await?;
    control_request(
        &mut control,
        ControlRequest::HoldDeliveryWrite { mailbox: peer(4)? },
    )
    .await?;
    let mut after = 0;
    let observed = loop {
        let ControlAnswer::ServiceEvent(observed) = control_request(
            &mut control,
            ControlRequest::WaitMailboxEvent {
                mailbox: peer(4)?,
                after,
            },
        )
        .await?
        else {
            return Err("no service event".into());
        };
        assert_ne!(
            observed.fields.get("message").map(String::as_str),
            Some("delivery mailbox unavailable")
        );
        after += 1;
        if observed.fields.get("phase").map(String::as_str) == Some("scan") {
            break observed;
        }
    };
    eprintln!(
        "OBS A10 deadline scan transaction actual backend failure: {:?}",
        observed.fields
    );
    assert_ne!(
        observed.fields.get("message").map(String::as_str),
        Some("delivery mailbox unavailable")
    );
    let mut other_origin = DeliveryNode::start(&directory.path().join("other-origin"), 6).await?;
    let mut other_recipient =
        DeliveryNode::start(&directory.path().join("other-recipient"), 8).await?;
    other_origin.connect(&relay).await?;
    other_recipient.connect(&relay).await?;
    let other = enqueue(&mut other_origin, 841, peer(8)?).await?;
    other_recipient
        .command(NodeCommand::Receive {
            stream: DeliveryStream::Items,
        })
        .await?;
    assert_eq!(
        effect_count(
            &other_recipient
                .wait(other, CustodySide::Inbox, "delivered")
                .await?,
            other
        ),
        1
    );
    control_request(
        &mut control,
        ControlRequest::ReleaseDeliveryWrite { mailbox: peer(4)? },
    )
    .await?;
    let expired = origin.wait(item, CustodySide::Outbox, "expired").await?;
    assert_eq!(expired["outbox"]["state"]["value"]["version"], 2);
    eprintln!(
        "OBS A10 scan resumed after real backend BEGIN lock; sender Expired@2, other mailbox delivered while locked"
    );
    other_recipient.stop().await?;
    other_origin.stop().await?;
    origin.stop().await?;
    relay.kill().await?;
    Ok(())
}
