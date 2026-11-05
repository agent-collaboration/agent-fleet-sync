#![cfg(feature = "test-support")]
#[path = "support/delivery_node.rs"]
mod node;
#[expect(
    dead_code,
    unused_imports,
    reason = "shared helpers serve other journeys"
)]
mod support;
use fleet_sync_test_consumer::{
    TestResult,
    delivery_node::{CustodySide, FaultMode, NodeCommand, peer},
};
use node::*;
use support::*;

#[tokio::test]
async fn restart_during_unanswered_local_effect_keeps_one_uncertain_effect() -> TestResult {
    use sqlx_turso::sqlx::{ConnectOptions, Connection, Row};
    let directory = tempfile::tempdir()?;
    let recipient_path = directory.path().join("recipient");
    let mut relay = ServiceProcess::start(&directory.path().join("relay")).await?;
    let mut sender = DeliveryNode::start(&directory.path().join("sender"), 2).await?;
    let mut recipient = DeliveryNode::start(&recipient_path, 4).await?;
    sender.connect(&relay).await?;
    recipient.connect(&relay).await?;
    recipient
        .command(NodeCommand::Arm {
            hop: "local".into(),
            mode: FaultMode::Pause,
        })
        .await?;
    recipient
        .command(NodeCommand::SetHeld { enabled: false })
        .await?;
    let item = enqueue(&mut sender, 910, peer(4)?).await?;
    let local = recipient.command(NodeCommand::WaitBarrier).await?;
    assert_eq!(local["hop"], "local");
    let before = recipient.command(NodeCommand::Inspect { item }).await?;
    assert_eq!(effect_count(&before, item), 1);
    recipient.kill().await?;
    let mut connection = sqlx_turso::TursoConnectOptions::new()
        .filename(recipient_path.join("delivery-node.db"))
        .create_if_missing(false)
        .connect()
        .await?;
    let pending = sqlx_core::query::query::<sqlx_turso::Turso>(
        "SELECT json_extract(s.intent,'$.intent') AS intent,json_extract(s.intent,'$.reach') AS reach FROM _fleet_sync_steps s JOIN _fleet_sync_operations o USING(operation_id) WHERE o.kind='fleet-sync.delivery.deliver-locally' AND s.step_name='deliver' AND s.output IS NULL",
    ).fetch_all(&mut connection).await?;
    assert_eq!(
        pending.len(),
        1,
        "kill must leave exactly one unanswered local effect"
    );
    for row in pending {
        let intent: durable_steps::storage::CallIntent =
            serde_json::from_str(&row.try_get::<String, _>("intent")?)?;
        assert_eq!(intent.replay, durable_steps::CallReplay::NeverRepeat);
        assert_eq!(row.try_get::<String, _>("reach")?, "dispatching");
    }
    connection.close().await?;
    recipient = DeliveryNode::start(&recipient_path, 4).await?;
    recipient.connect(&relay).await?;
    let recovered = recipient
        .wait(item, CustodySide::Inbox, "uncertain")
        .await?;
    assert_eq!(effect_count(&recovered, item), 1);
    sender.wait(item, CustodySide::Outbox, "uncertain").await?;
    assert_eq!(
        effect_count(
            &recipient.command(NodeCommand::Inspect { item }).await?,
            item
        ),
        1
    );
    eprintln!(
        "OBS a real Push admitted a local effect whose answer remained held; killed journal had one NeverRepeat call without output; restart persisted Uncertain and kept one effect"
    );
    recipient.stop().await?;
    sender.stop().await?;
    relay.kill().await?;
    Ok(())
}
