#![allow(clippy::unwrap_used)]
#![cfg(feature = "test-support")]
#[path = "support/delivery_node.rs"]
mod node;
#[expect(
    dead_code,
    unused_imports,
    reason = "shared native process helpers cover several consumer journeys"
)]
mod support;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::{
    TestResult,
    delivery_node::{CustodySide, FaultMode, NodeCommand, peer},
};
use node::*;
use support::*;

#[tokio::test]
async fn queued_relay_offline_then_recipient_offline_drains_real_custody_once() -> TestResult {
    let directory = tempfile::tempdir()?;
    let mut sender = DeliveryNode::start(&directory.path().join("sender"), 2).await?;
    let mut recipient = DeliveryNode::start(&directory.path().join("recipient"), 4).await?;
    let first = enqueue(&mut sender, 101, peer(4)?).await?;
    let second = enqueue(&mut sender, 102, peer(4)?).await?;
    sender.wait(first, CustodySide::Outbox, "queued").await?;
    let mut relay = ServiceProcess::start(&directory.path().join("relay")).await?;
    sender.connect(&relay).await?;
    for item in [first, second] {
        let sent = sender.wait(item, CustodySide::Outbox, "sent").await?;
        assert_eq!(effect_count(&sent, item), 0);
        let untouched = recipient.command(NodeCommand::Inspect { item }).await?;
        assert!(untouched["inbox"].is_null());
        eprintln!("OBS relay custody accepted {item:?}; recipient process offline from relay");
    }
    // Disconnect origin while outcomes advance; reconnect uses the explicit StatusFetch gap path.
    sender.command(NodeCommand::Disconnect).await?;
    recipient.connect(&relay).await?;
    for item in [first, second] {
        recipient
            .command(NodeCommand::Arm {
                hop: "report".into(),
                mode: FaultMode::Pause,
            })
            .await?;
        recipient
            .command(NodeCommand::Receive {
                stream: DeliveryStream::Items,
            })
            .await?;
        let delivered = recipient
            .wait(item, CustodySide::Inbox, "delivered")
            .await?;
        assert_eq!(effect_count(&delivered, item), 1);
        // The relay committed its terminal report before the reply barrier opens.
        recipient.command(NodeCommand::WaitBarrier).await?;
        recipient.command(NodeCommand::Resume).await?;
        eprintln!("OBS recipient committed body and exactly one external effect for {item:?}");
    }
    sender.connect(&relay).await?;
    let catchup = sender.command(NodeCommand::StatusFetch).await?;
    assert!(
        catchup["events"]
            .as_array()
            .is_some_and(|events| events.len() == 6)
    );
    assert_eq!(catchup["through"], 6);
    assert_eq!(
        catchup["events"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|event| event["status"]["kind"] == "delivered")
            .count(),
        2
    );
    let query = sender
        .command(NodeCommand::Query {
            mailbox: peer(4)?,
            items: vec![first, second],
        })
        .await?;
    eprintln!("OBS StatusFetch gap catchup={catchup}; Query={query}");
    assert_eq!(
        query["value"]["statuses"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| item["state"]["value"]["status"]["kind"] == "delivered")
            .count(),
        2
    );
    for item in [first, second] {
        sender.wait(item, CustodySide::Outbox, "delivered").await?;
        recipient
            .command(NodeCommand::Receive {
                stream: DeliveryStream::Items,
            })
            .await?;
        let replay = recipient.command(NodeCommand::Inspect { item }).await?;
        assert_eq!(effect_count(&replay, item), 1);
    }
    sender
        .command(NodeCommand::Retire {
            mailbox: peer(4)?,
            items: vec![first, second],
        })
        .await?;
    recipient
        .command(NodeCommand::Receive {
            stream: DeliveryStream::Controls,
        })
        .await?;
    for item in [first, second] {
        let retired = recipient.wait(item, CustodySide::Inbox, "absent").await?;
        assert_eq!(effect_count(&retired, item), 1);
    }
    recipient.stop().await?;
    sender.stop().await?;
    relay.kill().await?;
    Ok(())
}

#[tokio::test]
async fn a_lost_answer_at_every_hop_preserves_one_effect_and_monotone_status() -> TestResult {
    for (index, hop) in [
        "submit",
        "fetch",
        "received",
        "local",
        "report",
        "publication",
    ]
    .into_iter()
    .enumerate()
    {
        let directory = tempfile::tempdir()?;
        let mut relay = ServiceProcess::start(&directory.path().join("relay")).await?;
        let mut sender = DeliveryNode::start(&directory.path().join("sender"), 2).await?;
        let mut recipient = DeliveryNode::start(&directory.path().join("recipient"), 4).await?;
        sender.connect(&relay).await?;
        recipient.connect(&relay).await?;
        let sender_fault = matches!(hop, "submit" | "publication");
        if sender_fault {
            sender
                .command(NodeCommand::Arm {
                    hop: hop.into(),
                    mode: FaultMode::Lose,
                })
                .await?;
        }
        let item = enqueue(&mut sender, 200 + index as u64, peer(4)?).await?;
        if sender_fault {
            sender.command(NodeCommand::WaitBarrier).await?;
            sender.connect(&relay).await?;
        }
        sender.wait(item, CustodySide::Outbox, "sent").await?;
        if !sender_fault {
            recipient
                .command(NodeCommand::Arm {
                    hop: hop.into(),
                    mode: FaultMode::Lose,
                })
                .await?;
        }
        recipient
            .command(NodeCommand::Receive {
                stream: DeliveryStream::Items,
            })
            .await?;
        if !sender_fault {
            recipient.command(NodeCommand::WaitBarrier).await?;
            recipient.connect(&relay).await?;
        }
        let expected = if hop == "local" {
            "uncertain"
        } else {
            "delivered"
        };
        let snapshot = recipient.wait(item, CustodySide::Inbox, expected).await?;
        assert_eq!(effect_count(&snapshot, item), 1, "hop {hop}");
        sender.wait(item, CustodySide::Outbox, expected).await?;
        recipient
            .command(NodeCommand::Receive {
                stream: DeliveryStream::Items,
            })
            .await?;
        let repeated = recipient.command(NodeCommand::Inspect { item }).await?;
        assert_eq!(effect_count(&repeated, item), 1, "hop {hop} repeated");
        eprintln!(
            "OBS lost {hop} answer: recipient+sender {expected}, exactly one external effect"
        );
        recipient.stop().await?;
        sender.stop().await?;
        relay.kill().await?;
    }
    Ok(())
}

#[tokio::test]
async fn killing_a_process_at_every_committed_hop_recovers_without_duplicate_effects() -> TestResult
{
    for (index, hop) in [
        "submit",
        "fetch",
        "received",
        "local",
        "report",
        "publication",
    ]
    .into_iter()
    .enumerate()
    {
        let directory = tempfile::tempdir()?;
        let sender_path = directory.path().join("sender");
        let recipient_path = directory.path().join("recipient");
        let mut relay = ServiceProcess::start(&directory.path().join("relay")).await?;
        let mut control = relay.connect("control", "Bearer consumer-control").await?;
        let mut sender = DeliveryNode::start(&sender_path, 2).await?;
        let mut recipient = DeliveryNode::start(&recipient_path, 4).await?;
        sender.connect(&relay).await?;
        recipient.connect(&relay).await?;
        let sender_fault = matches!(hop, "submit" | "publication");
        if sender_fault {
            sender
                .command(NodeCommand::Arm {
                    hop: hop.into(),
                    mode: FaultMode::Pause,
                })
                .await?;
        }
        let item = enqueue(&mut sender, 300 + index as u64, peer(4)?).await?;
        if sender_fault {
            sender.command(NodeCommand::WaitBarrier).await?;
            inspect_live_hop(&mut control, &mut sender, &mut recipient, item, hop).await?;
            sender.kill().await?;
            inspect_killed_hop(&sender_path, hop).await?;
            sender = DeliveryNode::start(&sender_path, 2).await?;
            sender.connect(&relay).await?;
        }
        sender.wait(item, CustodySide::Outbox, "sent").await?;
        if !sender_fault {
            recipient
                .command(NodeCommand::Arm {
                    hop: hop.into(),
                    mode: FaultMode::Pause,
                })
                .await?;
        }
        recipient
            .command(NodeCommand::Receive {
                stream: DeliveryStream::Items,
            })
            .await?;
        if !sender_fault {
            recipient.command(NodeCommand::WaitBarrier).await?;
            inspect_live_hop(&mut control, &mut sender, &mut recipient, item, hop).await?;
            recipient.kill().await?;
            inspect_killed_hop(&recipient_path, hop).await?;
            recipient = DeliveryNode::start(&recipient_path, 4).await?;
            recipient.connect(&relay).await?;
        }
        let expected = if hop == "local" {
            "uncertain"
        } else {
            "delivered"
        };
        let snapshot = recipient.wait(item, CustodySide::Inbox, expected).await?;
        assert_eq!(effect_count(&snapshot, item), 1, "kill {hop}");
        sender.wait(item, CustodySide::Outbox, expected).await?;
        recipient
            .command(NodeCommand::Receive {
                stream: DeliveryStream::Items,
            })
            .await?;
        assert_eq!(
            effect_count(
                &recipient.command(NodeCommand::Inspect { item }).await?,
                item
            ),
            1
        );
        eprintln!(
            "OBS kill {hop}: recovered from real drive lease, both custodians {expected}, one external effect"
        );
        recipient.stop().await?;
        sender.stop().await?;
        relay.kill().await?;
    }
    Ok(())
}

#[tokio::test]
async fn oversized_receipt_is_bounded_at_durable_checkpoint_across_process_restart() -> TestResult {
    use sqlx_turso::sqlx::{ConnectOptions, Connection, Row};
    let directory = tempfile::tempdir()?;
    let recipient_path = directory.path().join("recipient");
    let mut relay = ServiceProcess::start(&directory.path().join("relay")).await?;
    let mut sender = DeliveryNode::start(&directory.path().join("sender"), 2).await?;
    let mut recipient = DeliveryNode::start(&recipient_path, 4).await?;
    sender.connect(&relay).await?;
    recipient.connect(&relay).await?;
    let item = enqueue(&mut sender, 401, peer(4)?).await?;
    sender.wait(item, CustodySide::Outbox, "sent").await?;
    recipient
        .command(NodeCommand::SetOversizedAcceptance { enabled: true })
        .await?;
    recipient
        .command(NodeCommand::Arm {
            hop: "checkpoint".into(),
            mode: FaultMode::Pause,
        })
        .await?;
    recipient
        .command(NodeCommand::Receive {
            stream: DeliveryStream::Items,
        })
        .await?;
    recipient.command(NodeCommand::WaitBarrier).await?;
    recipient.kill().await?;
    let mut connection = sqlx_turso::TursoConnectOptions::new()
        .filename(recipient_path.join("delivery-node.db"))
        .create_if_missing(false)
        .connect()
        .await?;
    let row = sqlx_core::query::query::<sqlx_turso::Turso>(
        "SELECT output FROM _fleet_sync_steps WHERE step_name='deliver'",
    )
    .fetch_one(&mut connection)
    .await?;
    let output: Vec<u8> = row.try_get("output")?;
    assert!(
        output.len() < 512,
        "oversized durable answer: {}",
        output.len()
    );
    assert!(String::from_utf8_lossy(&output).contains("uncertain"));
    assert!(!String::from_utf8_lossy(&output).contains("invalid receipt"));
    connection.close().await?;
    recipient = DeliveryNode::start(&recipient_path, 4).await?;
    recipient.connect(&relay).await?;
    let snapshot = recipient
        .wait(item, CustodySide::Inbox, "uncertain")
        .await?;
    assert_eq!(effect_count(&snapshot, item), 1);
    sender.wait(item, CustodySide::Outbox, "uncertain").await?;
    recipient
        .command(NodeCommand::Receive {
            stream: DeliveryStream::Items,
        })
        .await?;
    assert_eq!(
        effect_count(
            &recipient.command(NodeCommand::Inspect { item }).await?,
            item
        ),
        1
    );
    eprintln!(
        "OBS decision70: {}-byte durable Uncertain, real process restart, sender+recipient Uncertain, one external effect",
        output.len()
    );
    recipient.stop().await?;
    sender.stop().await?;
    relay.kill().await?;
    Ok(())
}

async fn inspect_live_hop(
    control: &mut fleet_sync_transport_websocket::WebSocketConnection,
    sender: &mut DeliveryNode,
    recipient: &mut DeliveryNode,
    item: ItemId,
    hop: &str,
) -> TestResult {
    use fleet_sync_test_consumer::{ControlAnswer, ControlRequest};
    let ControlAnswer::DeliverySnapshot(relay) = control_request(
        control,
        ControlRequest::DeliverySnapshot {
            mailbox: peer(4)?,
            origin: peer(2)?,
        },
    )
    .await?
    else {
        return Err("missing delivery inspection".into());
    };
    let sender = sender.command(NodeCommand::Snapshot).await?;
    let recipient = recipient.command(NodeCommand::Snapshot).await?;
    let mailbox = relay
        .mailbox
        .iter()
        .find(|row| row.category == "mailbox" && row.identity == item.as_uuid().to_string())
        .ok_or("missing committed mailbox custody")?;
    let bytes = mailbox
        .payload_bytes
        .as_deref()
        .ok_or("missing mailbox bytes")?
        .parse::<u64>()?;
    if matches!(hop, "received" | "local" | "report") {
        assert_eq!(bytes, 0, "mailbox still holds received body at {hop}");
    } else {
        assert!(bytes > 0, "mailbox lost offline-recipient body at {hop}");
    }
    if matches!(hop, "received" | "local" | "report") {
        assert!(
            recipient
                .as_array()
                .is_some_and(|rows| rows.iter().any(|row| row["category"] == "inbox"
                    && row["payload_bytes"]
                        .as_str()
                        .is_some_and(|bytes| bytes.parse::<u64>().is_ok_and(|bytes| bytes > 0))))
        );
    }
    eprintln!(
        "OBS live {hop} committed owner snapshots: mailboxBodyBytes={bytes}, senderRows={}, recipientRows={}, originRows={}",
        sender.as_array().map_or(0, Vec::len),
        recipient.as_array().map_or(0, Vec::len),
        relay.origin.len()
    );
    Ok(())
}

async fn inspect_killed_hop(directory: &std::path::Path, hop: &str) -> TestResult {
    use sqlx_turso::sqlx::{ConnectOptions, Connection, Row};
    // The killed owner's file can be opened directly; live owners use their transaction seam.
    {
        let mut connection = sqlx_turso::TursoConnectOptions::new()
            .filename(directory.join("delivery-node.db"))
            .create_if_missing(false)
            .connect()
            .await?;
        let rows=sqlx_core::query::query::<sqlx_turso::Turso>("SELECT 'mailbox' AS hop,COUNT(*) AS items,COALESCE(SUM(payload_bytes),0) AS bytes FROM _fleet_sync_mail_items UNION ALL SELECT 'outbox',COUNT(*),COALESCE(SUM(payload_bytes),0) FROM _fleet_sync_outbox_items UNION ALL SELECT 'inbox',COUNT(*),COALESCE(SUM(payload_bytes),0) FROM _fleet_sync_inbox_items").fetch_all(&mut connection).await?;
        for row in rows {
            eprintln!(
                "OBS kill barrier {hop}: {} items={} bodyBytes={}",
                row.try_get::<String, _>("hop")?,
                row.try_get::<i64, _>("items")?,
                row.try_get::<i64, _>("bytes")?
            );
        }
        connection.close().await?;
    }
    Ok(())
}
