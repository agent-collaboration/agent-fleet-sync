#![cfg(feature = "test-support")]
#![allow(clippy::unwrap_used)]
#[path = "support/delivery_node.rs"]
#[expect(
    dead_code,
    reason = "node helpers also cover peer process kills in the other journey"
)]
mod node;
#[expect(
    dead_code,
    unused_imports,
    reason = "shared native helpers serve several process journeys"
)]
mod support;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::{
    ControlAnswer, ControlRequest, TestResult,
    delivery_node::{CustodySide, FaultMode, NodeCommand, peer},
};
use node::*;
use support::*;

#[tokio::test]
async fn relay_kills_after_committed_custody_receipt_report_and_publication_preserve_state()
-> TestResult {
    for (index, hop) in ["submit", "push", "report", "publication"]
        .into_iter()
        .enumerate()
    {
        let temporary = tempfile::tempdir()?;
        let directory = std::env::var_os("FLEET_DELIVERY_EVIDENCE")
            .map(std::path::PathBuf::from)
            .map_or_else(|| temporary.path().to_path_buf(), |base| base.join(hop));
        tokio::fs::create_dir_all(&directory).await?;
        let relay_path = directory.join("relay");
        let mut relay = ServiceProcess::start(&relay_path).await?;
        let mut control = relay.connect("control", "Bearer consumer-control").await?;
        let mut sender = DeliveryNode::start(&directory.join("sender"), 2).await?;
        let mut recipient = DeliveryNode::start(&directory.join("recipient"), 4).await?;
        sender.connect(&relay).await?;
        recipient.connect(&relay).await?;
        let sender_barrier = matches!(hop, "submit" | "publication");
        if sender_barrier {
            sender
                .command(NodeCommand::Arm {
                    hop: hop.into(),
                    mode: FaultMode::Pause,
                })
                .await?;
        }
        if !sender_barrier {
            recipient
                .command(NodeCommand::Arm {
                    hop: hop.into(),
                    mode: FaultMode::Pause,
                })
                .await?;
        }
        let item = enqueue(&mut sender, 500 + index as u64, peer(4)?).await?;
        if sender_barrier {
            sender.command(NodeCommand::WaitBarrier).await?;
        } else {
            recipient.command(NodeCommand::WaitBarrier).await?;
        }
        let ControlAnswer::DeliverySnapshot(before) =
            control_request(&mut control, ControlRequest::DeliverySnapshot).await?
        else {
            return Err("missing relay snapshot".into());
        };
        let before = before
            .relay
            .iter()
            .find(|row| row.category == "outbox" && row.identity == item.as_uuid().to_string())
            .ok_or("no relay custody at barrier")?;
        let before_record: serde_json::Value =
            serde_json::from_str(before.content.as_deref().ok_or("missing relay header")?)?;
        let before_bytes = before
            .payload_bytes
            .as_deref()
            .ok_or("missing relay bytes")?
            .parse::<u64>()?;
        if matches!(hop, "submit" | "push") {
            assert!(before_bytes > 0);
        } else if hop == "report" {
            assert_eq!(before_bytes, 0);
        }
        relay.kill().await?;
        // The relay process is gone: read its real committed file, not a copied or mocked store.
        let (disk_record, disk_bytes) = relay_row(&relay_path, item).await?;
        assert_eq!(disk_record["item_id"], before_record["value"]["id"]);
        assert_eq!(disk_record["digest"], before_record["value"]["digest"]);
        assert_eq!(
            disk_record["status_version"],
            before_record["value"]["version"]
        );
        assert_eq!(disk_bytes, before_bytes);
        relay = ServiceProcess::start(&relay_path).await?;
        control = relay.connect("control", "Bearer consumer-control").await?;
        let ControlAnswer::DeliverySnapshot(after) =
            control_request(&mut control, ControlRequest::DeliverySnapshot).await?
        else {
            return Err("no restarted owner snapshot".into());
        };
        let after = after
            .relay
            .iter()
            .find(|row| row.category == "outbox" && row.identity == item.as_uuid().to_string())
            .ok_or("restart did not retain relay custody")?;
        assert_eq!(
            after
                .payload_bytes
                .as_deref()
                .ok_or("missing reopened bytes")?
                .parse::<u64>()?,
            before_bytes
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                after.content.as_deref().ok_or("missing reopened header")?
            )?,
            before_record
        );
        if sender_barrier {
            sender.command(NodeCommand::Resume).await?;
            sender.connect(&relay).await?;
            recipient.connect(&relay).await?;
        } else {
            recipient.command(NodeCommand::Resume).await?;
            recipient.connect(&relay).await?;
            sender.connect(&relay).await?;
        }
        let delivered = recipient
            .wait(item, CustodySide::Inbox, "delivered")
            .await?;
        assert_eq!(effect_count(&delivered, item), 1);
        let settled = sender.wait(item, CustodySide::Outbox, "delivered").await?;
        let final_version = settled["outbox"]["state"]["value"]["version"]
            .as_u64()
            .ok_or("missing final version")?;
        assert!((2..=3).contains(&final_version));
        let catchup = sender.command(NodeCommand::StatusFetch).await?;
        assert!(
            catchup["events"]
                .as_array()
                .is_some_and(|events| events
                    .iter()
                    .any(|event| event["item"] == item.as_uuid().to_string()
                        && event["version"] == final_version
                        && event["status"]["kind"] == "delivered"))
        );
        assert_eq!(
            effect_count(
                &recipient.command(NodeCommand::Inspect { item }).await?,
                item
            ),
            1
        );
        eprintln!(
            "OBS relay SIGKILL at {hop}: exact committed record/bodyBytes={before_bytes} survives disk readback and startup inventory; both peers Delivered v{final_version}, one external effect"
        );
        recipient.stop().await?;
        sender.stop().await?;
        relay.kill().await?;
    }
    Ok(())
}

async fn relay_row(
    directory: &std::path::Path,
    item: ItemId,
) -> TestResult<(serde_json::Value, u64)> {
    support::delivery_mail::stopped_relay_row(directory, item).await
}
