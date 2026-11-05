#![cfg(feature = "test-support")]
#![allow(clippy::unwrap_used)]
#[path = "support/delivery_node.rs"]
#[expect(
    dead_code,
    reason = "shared node helpers serve separate process journeys"
)]
mod node;
#[expect(
    dead_code,
    unused_imports,
    reason = "shared process helpers serve other journeys"
)]
mod support;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::{
    delivery_node::{NodeCommand, peer},
    *,
};
use node::*;
use std::time::Duration;
use support::*;

#[tokio::test]
async fn delivery_node_connect_waits_for_registry_acceptance_and_welcome() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let relay = ServiceProcess::start(&temporary.path().join("relay")).await?;
    let mut control = relay.connect("control", "Bearer consumer-control").await?;
    schema_request(
        &mut control,
        SchemaControlRequest::HoldReport {
            peer: peer(8)?,
            attach_seq: AttachSequence::new(1)?,
        },
    )
    .await?;
    let mut node = DeliveryNode::start(&temporary.path().join("node"), 8).await?;
    let port = relay.port;
    let mut connecting = tokio::spawn(async move {
        let answer = node.command(NodeCommand::Connect { port }).await;
        (node, answer)
    });
    assert!(
        matches!(schema_request(&mut control, SchemaControlRequest::WaitReport).await?, SchemaControlAnswer::Report(report) if report.peer == peer(8)?)
    );
    if let Ok(result) = tokio::time::timeout(Duration::from_millis(50), &mut connecting).await {
        let (mut node, answer) = result?;
        answer?;
        schema_request(&mut control, SchemaControlRequest::ReleaseReport).await?;
        node.kill().await?;
        relay.stop(&mut control).await?;
        return Err("delivery Connect returned before registry acceptance and Welcome".into());
    }
    schema_request(&mut control, SchemaControlRequest::ReleaseReport).await?;
    let (mut node, answer) = connecting.await?;
    answer?;
    let response = node
        .command(NodeCommand::Query {
            mailbox: peer(8)?,
            items: Vec::new(),
        })
        .await?;
    assert!(
        matches!(serde_json::from_value::<DeliveryAnswer<SampleDelivery>>(response)?, DeliveryAnswer::Query(result) if result.statuses.is_empty())
    );
    node.stop().await?;
    relay.stop(&mut control).await?;
    Ok(())
}
