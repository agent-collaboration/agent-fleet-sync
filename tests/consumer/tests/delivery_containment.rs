#![cfg(feature = "test-support")]
#![allow(clippy::unwrap_used)]
#[path = "support/delivery_node.rs"]
mod node;
#[expect(
    dead_code,
    unused_imports,
    reason = "shared process helpers serve other journeys"
)]
mod support;
use fleet_sync_lease::LeasePolicy;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::{
    delivery_node::{CustodySide, NodeCommand, peer},
    *,
};
use fleet_sync_transport_websocket::WebSocketConnection;
use fleet_sync_writer::*;
use node::*;
use runtime_primitives::{Clock, SystemClock};
use std::{path::Path, sync::Arc, time::Duration};
use support::*;
use tokio::sync::Mutex;

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

struct Annotation;
impl PieceAnnotation for Annotation {
    type Protocol = SampleProtocol;
    fn annotate(&self, _: &[CapturedTransaction<'_>]) -> Option<String> {
        Some("healthy copy".into())
    }
}
struct CopyTransport(Mutex<WebSocketConnection>);
impl ReplicationTransport<SampleProtocol> for CopyTransport {
    async fn send_piece(
        &self,
        _: ReceiverId,
        piece: Piece<String>,
    ) -> Result<ReplicationAnswer, SendFailure> {
        match request(
            &mut *self.0.lock().await,
            OperationRequest::Replication(ReplicationRequest::Piece(piece)),
        )
        .await
        .map_err(|error| SendFailure::Unavailable {
            message: error.to_string(),
        })? {
            OperationAnswer::Replication(answer) => Ok(answer),
            _ => Err(SendFailure::AnswerLost),
        }
    }
}

#[tokio::test]
async fn corrupted_mailbox_isolated_across_restart_while_peer_registry_copy_and_other_mailbox_work()
-> TestResult {
    let temporary = tempfile::tempdir()?;
    let relay_path = temporary.path().join("relay");
    let mut relay = ServiceProcess::start(&relay_path).await?;
    let mut origin = DeliveryNode::start(&temporary.path().join("origin"), 2).await?;
    let mut healthy = DeliveryNode::start(&temporary.path().join("healthy"), 4).await?;
    let mut damaged = DeliveryNode::start(&temporary.path().join("damaged"), 8).await?;
    origin.connect(&relay).await?;
    healthy.connect(&relay).await?;
    damaged.connect(&relay).await?;
    let writer = Arc::new(
        ReplicatedDatabase::create(
            &temporary.path().join("writer.db"),
            SampleSchema::new()?,
            peer(6)?,
            LeasePolicy {
                lease_duration: Duration::from_secs(600),
                renew_interval: Duration::from_secs(30),
            },
            WriterOptions::default(),
        )
        .await?,
    );
    let mut socket = relay
        .peer(6, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let lease = match request(
        &mut socket,
        OperationRequest::Lease(LeaseRequest::Register(Registration {
            attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
            database: writer.database(),
            writer: peer(6)?,
            origin: Origin::Empty,
        })),
    )
    .await?
    {
        OperationAnswer::Lease(LeaseAnswer::Register(RegistrationAnswer::Registered {
            lease,
            ..
        })) => lease,
        _ => return Err("no healthy registration".into()),
    };
    let started = SystemClock.now();
    let renewed = match request(
        &mut socket,
        OperationRequest::Lease(LeaseRequest::Renew(RenewRequest {
            attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
            database: writer.database(),
            epoch: lease.epoch,
        })),
    )
    .await?
    {
        OperationAnswer::Lease(LeaseAnswer::Renew(answer)) => answer,
        _ => return Err("no initial healthy renewal".into()),
    };
    writer.observe_renewal(renewed, started).await?;
    let transport = Arc::new(CopyTransport(Mutex::new(socket)));
    let replication = ReplicationSender::new(
        writer.clone(),
        Arc::new(Annotation),
        Arc::new(()),
        transport.clone(),
        SenderOptions::default(),
    );
    let receiver = ReceiverId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000003,
    ))?;
    replication
        .add_copy(
            receiver,
            CopyRole::Authoritative,
            ReplicationPosition::new(lease.epoch, CommitSequence::new(0)),
        )
        .await?;
    replication.replicate_once(receiver).await?;
    let bad_item = enqueue(&mut origin, 820, peer(8)?).await?;
    origin.wait(bad_item, CustodySide::Outbox, "sent").await?;
    let mut control = relay.connect("control", "Bearer consumer-control").await?;
    let ControlAnswer::CorruptedDelivery {
        record: original,
        payload_bytes: bytes,
    } = control_request(
        &mut control,
        ControlRequest::CorruptDeliveryCustody {
            mailbox: peer(8)?,
            item: bad_item,
        },
    )
    .await?
    else {
        return Err("no intentional custody corruption".into());
    };
    let original = serde_json::from_str(&original)?;
    let (corrupt, zero) = snapshot_mail(&mut control, bad_item).await?;
    assert_eq!(zero, 0);
    assert_eq!(corrupt["value"]["fetched"], false);
    assert!(corrupt["value"]["payload"].is_null());
    for phase in 0..2 {
        damaged
            .command(NodeCommand::Receive {
                stream: DeliveryStream::Items,
            })
            .await?;
        let pending = tokio::time::timeout(
            Duration::from_secs(12),
            damaged.command(NodeCommand::WaitPendingCall {
                hop: "fetch".into(),
                attempts: phase as u32 + 1,
            }),
        )
        .await??;
        assert!(pending["intent"].is_string());
        let snapshot = damaged.command(NodeCommand::Snapshot).await?;
        let state = snapshot
            .as_array()
            .ok_or("missing node snapshot")?
            .iter()
            .find(|row| row["category"] == "inboxState")
            .ok_or("missing receive state")?;
        let state: serde_json::Value =
            serde_json::from_str(state["content"].as_str().ok_or("missing state content")?)?;
        assert!(!state["items"]["active"].is_null());
        assert_eq!(state["items"]["received"], 0);
        assert_eq!(state["items"]["acknowledged"], 0);
        // The same peer connection survives its failed mailbox operation.
        let refused = damaged
            .command(NodeCommand::Query {
                mailbox: peer(8)?,
                items: vec![bad_item],
            })
            .await?;
        assert_eq!(refused["kind"], "mailboxUnavailable");
        assert_eq!(refused["value"]["reason"]["kind"], "processingFailed");
        assert_eq!(
            refused["value"]["reason"]["value"]["kind"],
            "custodyCorrupt"
        );
        let good = enqueue(&mut origin, 821 + phase as u64, peer(4)?).await?;
        origin.wait(good, CustodySide::Outbox, "sent").await?;
        healthy
            .command(NodeCommand::Receive {
                stream: DeliveryStream::Items,
            })
            .await?;
        assert_eq!(
            effect_count(
                &healthy.wait(good, CustodySide::Inbox, "delivered").await?,
                good
            ),
            1
        );
        origin.wait(good, CustodySide::Outbox, "delivered").await?;
        // The same registered writer/copy continues after the unrelated owner fails.
        let started = SystemClock.now();
        let renewed = match request(
            &mut *transport.0.lock().await,
            OperationRequest::Lease(LeaseRequest::Renew(RenewRequest {
                attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
                database: writer.database(),
                epoch: lease.epoch,
            })),
        )
        .await?
        {
            OperationAnswer::Lease(LeaseAnswer::Renew(answer)) => answer,
            _ => return Err("no healthy renewal".into()),
        };
        writer.observe_renewal(renewed, started).await?;
        let mut write = writer.begin_write().await?;
        insert_sample(
            &mut write,
            format!("01900000-0000-7000-8000-{:012x}", phase + 900),
            "healthy after corruption".into(),
            phase as i64,
            vec![1, 2],
        )
        .await?;
        write.commit().await?;
        replication.replicate_once(receiver).await?;
        let rows = copy_rows(&mut *transport.0.lock().await, writer.database()).await?;
        assert_eq!(
            rows.get("samples").ok_or("missing healthy copy")?.len(),
            phase + 1
        );
        let mut fresh = relay
            .peer(12, SessionId::new(durable_steps::Uuid::now_v7())?)
            .await?;
        assert!(
            matches!(request(&mut fresh, OperationRequest::Consumer(SampleRequest::Echo { text: "alive".into() })).await?,
            OperationAnswer::Consumer(SampleAnswer::Echo { text, .. }) if text == "alive")
        );
        let kept = snapshot_mail(&mut control, bad_item).await?;
        assert_eq!(kept.0, corrupt);
        assert_eq!(kept.1, 0);
        assert!(relay.child.try_wait()?.is_none());
        let ControlAnswer::ServiceEvents(events) =
            control_request(&mut control, ControlRequest::ServiceEvents).await?
        else {
            return Err("no service events".into());
        };
        assert_eq!(
            events
                .iter()
                .filter(|event| event.fields.get("mailbox").is_some_and(
                    |identity| identity.contains(&peer(8).unwrap().as_uuid().to_string())
                ) && event.fields.get("message").map(String::as_str)
                    == Some("delivery mailbox unavailable"))
                .count(),
            1
        );
        eprintln!(
            "OBS decision83 phase{phase}: damaged mailbox unavailable/CustodyCorrupt and unchanged row; same cycle active/cursors0; same peer Query alive, healthy mailbox one effect, fresh session Echo, renewal and existing copy advance"
        );
        relay.kill().await?;
        if phase == 0 {
            relay = ServiceProcess::start(&relay_path).await?;
            control = relay.connect("control", "Bearer consumer-control").await?;
            origin.connect(&relay).await?;
            healthy.connect(&relay).await?;
            damaged.connect(&relay).await?;
            *transport.0.lock().await = relay
                .peer(6, SessionId::new(durable_steps::Uuid::now_v7())?)
                .await?;
        }
    }
    // A repaired owner is reopened normally; the same recipient's pending cycle works.
    mutate_mail(&relay_path, bad_item, Some((original, bytes))).await?;
    relay = ServiceProcess::start(&relay_path).await?;
    origin.connect(&relay).await?;
    damaged.connect(&relay).await?;
    assert_eq!(
        effect_count(
            &damaged
                .wait(bad_item, CustodySide::Inbox, "delivered")
                .await?,
            bad_item
        ),
        1
    );
    origin
        .wait(bad_item, CustodySide::Outbox, "delivered")
        .await?;
    eprintln!(
        "OBS decision83: exact test-injected row repaired while relay stopped, reopened; same recipient's pending cycle delivers once"
    );
    damaged.stop().await?;
    healthy.stop().await?;
    origin.stop().await?;
    relay.kill().await?;
    Ok(())
}

async fn connection(directory: &Path) -> TestResult<sqlx_turso::TursoConnection> {
    use sqlx_turso::sqlx::ConnectOptions;
    Ok(sqlx_turso::TursoConnectOptions::new()
        .filename(
            directory
                .join("service")
                .join(format!("delivery-mailbox-{}.db", peer(8)?.as_uuid())),
        )
        .create_if_missing(false)
        .connect()
        .await?)
}
async fn snapshot_mail(
    control: &mut WebSocketConnection,
    item: ItemId,
) -> TestResult<(serde_json::Value, i64)> {
    let ControlAnswer::DeliverySnapshot(snapshot) = control_request(
        control,
        ControlRequest::DeliverySnapshot {
            mailbox: peer(8)?,
            origin: peer(2)?,
        },
    )
    .await?
    else {
        return Err("no mailbox snapshot".into());
    };
    let row = snapshot
        .mailbox
        .iter()
        .find(|row| row.category == "mailbox" && row.identity == item.as_uuid().to_string())
        .ok_or("missing damaged row")?;
    Ok((
        serde_json::from_str(row.content.as_deref().ok_or("missing record")?)?,
        row.payload_bytes
            .as_deref()
            .ok_or("missing bytes")?
            .parse()?,
    ))
}
async fn read_mail(directory: &Path, item: ItemId) -> TestResult<(serde_json::Value, i64)> {
    use sqlx_turso::sqlx::{Connection, Row};
    let mut db = connection(directory).await?;
    let row = sqlx_core::query::query::<sqlx_turso::Turso>(
        "SELECT record,payload_bytes FROM _fleet_sync_mail_items WHERE item_id=?1",
    )
    .bind(item.as_uuid().to_string())
    .fetch_one(&mut db)
    .await?;
    let answer = (
        serde_json::from_str(&row.try_get::<String, _>("record")?)?,
        row.try_get("payload_bytes")?,
    );
    db.close().await?;
    Ok(answer)
}
async fn mutate_mail(
    directory: &Path,
    item: ItemId,
    restore: Option<(serde_json::Value, i64)>,
) -> TestResult<(serde_json::Value, i64)> {
    use sqlx_turso::sqlx::Connection;
    let original = read_mail(directory, item).await?;
    let mut db = connection(directory).await?;
    if let Some((record, bytes)) = restore {
        sqlx_core::query::query::<sqlx_turso::Turso>(
            "UPDATE _fleet_sync_mail_items SET record=?1,payload_bytes=?2 WHERE item_id=?3",
        )
        .bind(record.to_string())
        .bind(bytes)
        .bind(item.as_uuid().to_string())
        .execute(&mut db)
        .await?;
    } else {
        sqlx_core::query::query::<sqlx_turso::Turso>("UPDATE _fleet_sync_mail_items SET record=json_set(record,'$.value.payload',NULL),payload_bytes=0 WHERE item_id=?1")
            .bind(item.as_uuid().to_string()).execute(&mut db).await?;
    }
    db.close().await?;
    Ok(original)
}
