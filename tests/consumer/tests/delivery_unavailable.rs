#![cfg(feature = "test-support")]
#![allow(clippy::unwrap_used)]
#[path = "support/delivery_node.rs"]
mod node;
#[expect(
    dead_code,
    unused_imports,
    reason = "shared native helpers serve other journeys"
)]
mod support;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::{
    delivery_node::{CustodySide, FaultMode, NodeCommand, peer},
    *,
};
use node::*;
use std::time::Duration;
use support::{delivery_mail::*, *};

async fn healthy(origin: &mut DeliveryNode, recipient: &mut DeliveryNode, id: u64) -> TestResult {
    let item = enqueue(origin, id, peer(4)?).await?;
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
    Ok(())
}
async fn body_kept(origin: &mut DeliveryNode, item: ItemId) -> TestResult {
    let snapshot = origin.command(NodeCommand::Snapshot).await?;
    let row = snapshot
        .as_array()
        .ok_or("no snapshot")?
        .iter()
        .find(|row| row["category"] == "outbox" && row["identity"] == item.as_uuid().to_string())
        .ok_or("no outbox row")?;
    assert!(
        row["payload_bytes"]
            .as_str()
            .ok_or("no bytes")?
            .parse::<u64>()?
            > 0
    );
    let record: serde_json::Value =
        serde_json::from_str(row["content"].as_str().ok_or("no record")?)?;
    assert_eq!(record["value"]["status"]["kind"], "queued");
    Ok(())
}

#[tokio::test]
async fn accepted_submit_with_lost_answer_waits_during_quarantine_then_replays_and_delivers_once()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let relay_path = directory.path().join("relay");
    let origin_path = directory.path().join("origin");
    let mut relay = ServiceProcess::start(&relay_path).await?;
    let mut control = relay.connect("control", "Bearer consumer-control").await?;
    let mut origin = DeliveryNode::start(&origin_path, 2).await?;
    let mut seed_origin = DeliveryNode::start(&directory.path().join("seed-origin"), 6).await?;
    let mut recipient = DeliveryNode::start(&directory.path().join("recipient"), 8).await?;
    let mut other = DeliveryNode::start(&directory.path().join("other"), 4).await?;
    origin.connect(&relay).await?;
    seed_origin.connect(&relay).await?;
    other.connect(&relay).await?;
    origin
        .command(NodeCommand::SetStatusPaused { enabled: true })
        .await?;
    origin
        .command(NodeCommand::Arm {
            hop: "submit".into(),
            mode: FaultMode::Lose,
        })
        .await?;
    let item = enqueue(&mut origin, 860, peer(8)?).await?;
    origin.command(NodeCommand::WaitBarrier).await?;
    let first = origin
        .command(NodeCommand::WaitPendingCall {
            hop: "submit".into(),
            attempts: 1,
        })
        .await?;
    body_kept(&mut origin, item).await?;
    let (accepted, bytes) = mail_row(&mut control, peer(8)?, peer(2)?, item).await?;
    assert_eq!(accepted["value"]["status"]["kind"], "sent");
    assert!(bytes > 0);
    let damaged = enqueue(&mut seed_origin, 861, peer(8)?).await?;
    seed_origin
        .wait(damaged, CustodySide::Outbox, "sent")
        .await?;
    let original = corrupt(&mut control, peer(8)?, damaged).await?;
    let unavailable = seed_origin
        .command(NodeCommand::Query {
            mailbox: peer(8)?,
            items: vec![damaged],
        })
        .await?;
    eprintln!("OBS decision83 pre-retry mailbox answer={unavailable}");
    origin.connect(&relay).await?;
    let unavailable_call = origin
        .command(NodeCommand::WaitMailboxUnavailable {
            hop: "submit".into(),
        })
        .await?;
    assert_eq!(unavailable_call["operation"], first["operation"]);
    assert_eq!(unavailable_call["step"], "submit");
    let pending = match tokio::time::timeout(
        Duration::from_secs(12),
        origin.command(NodeCommand::WaitPendingCall {
            hop: "submit".into(),
            attempts: 2,
        }),
    )
    .await
    {
        Ok(result) => result?,
        Err(error) => {
            origin.kill().await?;
            eprintln!(
                "OBS decision83 wrong terminal sender after owner refusal: {}",
                stopped_node_row(&origin_path, item, true).await?
            );
            return Err(error.into());
        }
    };
    assert_eq!(first["operation"], pending["operation"]);
    assert_eq!(first["key"], pending["key"]);
    assert_eq!(first["intent"], pending["intent"]);
    body_kept(&mut origin, item).await?;
    assert_eq!(unavailable["kind"], "mailboxUnavailable");
    healthy(&mut seed_origin, &mut other, 862).await?;
    relay.kill().await?;
    restore_stopped(&relay_path, peer(8)?, damaged, original).await?;
    relay = ServiceProcess::start(&relay_path).await?;
    origin.connect(&relay).await?;
    // With publications still held, Sent can only come from the retried Submit's canonical answer.
    origin.wait(item, CustodySide::Outbox, "sent").await?;
    origin
        .command(NodeCommand::SetStatusPaused { enabled: false })
        .await?;
    recipient.connect(&relay).await?;
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
        "OBS decision83 Submit: accepted/lost answer, unrelated corruption, same pending call and sender body retained, healthy mailbox progressed, repaired relay replayed canonical answer, one effect"
    );
    recipient.stop().await?;
    other.stop().await?;
    seed_origin.stop().await?;
    origin.stop().await?;
    relay.kill().await?;
    Ok(())
}

#[tokio::test]
async fn owed_report_with_lost_answer_waits_during_quarantine_then_retires_without_repeating_effect()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let relay_path = directory.path().join("relay");
    let recipient_path = directory.path().join("recipient");
    let mut relay = ServiceProcess::start(&relay_path).await?;
    let mut control = relay.connect("control", "Bearer consumer-control").await?;
    let mut origin = DeliveryNode::start(&directory.path().join("origin"), 2).await?;
    let mut seed_origin = DeliveryNode::start(&directory.path().join("seed-origin"), 6).await?;
    let mut recipient = DeliveryNode::start(&recipient_path, 8).await?;
    let mut other = DeliveryNode::start(&directory.path().join("other"), 4).await?;
    origin.connect(&relay).await?;
    seed_origin.connect(&relay).await?;
    recipient.connect(&relay).await?;
    other.connect(&relay).await?;
    let item = enqueue(&mut origin, 870, peer(8)?).await?;
    origin.wait(item, CustodySide::Outbox, "sent").await?;
    recipient
        .command(NodeCommand::ArmItem {
            hop: "report".into(),
            item,
            mode: FaultMode::Lose,
        })
        .await?;
    recipient
        .command(NodeCommand::Receive {
            stream: DeliveryStream::Items,
        })
        .await?;
    recipient.command(NodeCommand::WaitBarrier).await?;
    let first = recipient
        .command(NodeCommand::WaitPendingCall {
            hop: "report".into(),
            attempts: 1,
        })
        .await?;
    assert!(
        first["key"]
            .as_str()
            .ok_or("no report key")?
            .contains(&item.as_uuid().to_string())
    );
    assert_eq!(
        effect_count(
            &recipient.command(NodeCommand::Inspect { item }).await?,
            item
        ),
        1
    );
    let damaged = enqueue(&mut seed_origin, 871, peer(8)?).await?;
    seed_origin
        .wait(damaged, CustodySide::Outbox, "sent")
        .await?;
    let original = corrupt(&mut control, peer(8)?, damaged).await?;
    let unavailable = seed_origin
        .command(NodeCommand::Query {
            mailbox: peer(8)?,
            items: vec![damaged],
        })
        .await?;
    recipient.connect(&relay).await?;
    let unavailable_call = recipient
        .command(NodeCommand::WaitMailboxUnavailable {
            hop: "report".into(),
        })
        .await?;
    assert_eq!(unavailable_call["operation"], first["operation"]);
    assert_eq!(unavailable_call["step"], "report");
    let pending = match tokio::time::timeout(
        Duration::from_secs(12),
        recipient.command(NodeCommand::WaitPendingCall {
            hop: "report".into(),
            attempts: 2,
        }),
    )
    .await
    {
        Ok(result) => result?,
        Err(error) => {
            recipient.kill().await?;
            eprintln!(
                "OBS decision83 owed but unscheduled report after owner refusal: {}",
                stopped_node_row(&recipient_path, item, false).await?
            );
            return Err(error.into());
        }
    };
    assert_eq!(first["operation"], pending["operation"]);
    assert_eq!(first["key"], pending["key"]);
    assert_eq!(first["intent"], pending["intent"]);
    assert_eq!(unavailable["kind"], "mailboxUnavailable");
    let snapshot = recipient.command(NodeCommand::Snapshot).await?;
    assert!(snapshot.as_array().ok_or("no snapshot")?.iter().any(|row| row["category"] == "report" && row["identity"] == item.as_uuid().to_string()));
    assert_eq!(
        effect_count(
            &recipient.command(NodeCommand::Inspect { item }).await?,
            item
        ),
        1
    );
    healthy(&mut seed_origin, &mut other, 872).await?;
    relay.kill().await?;
    restore_stopped(&relay_path, peer(8)?, damaged, original).await?;
    relay = ServiceProcess::start(&relay_path).await?;
    recipient.connect(&relay).await?;
    origin.connect(&relay).await?;
    recipient
        .command(NodeCommand::WaitReportRetired { item })
        .await?;
    assert_eq!(
        effect_count(
            &recipient.command(NodeCommand::Inspect { item }).await?,
            item
        ),
        1
    );
    let query = origin
        .command(NodeCommand::Query {
            mailbox: peer(8)?,
            items: vec![item],
        })
        .await?;
    assert_eq!(
        query["value"]["statuses"][0]["state"]["value"]["version"],
        3
    );
    eprintln!(
        "OBS decision83 Report: committed answer lost, same pending report/call and outcome kept during quarantine; after repair idempotent acknowledgment retires report, one effect/version3"
    );
    recipient.stop().await?;
    other.stop().await?;
    seed_origin.stop().await?;
    origin.stop().await?;
    relay.kill().await?;
    Ok(())
}
