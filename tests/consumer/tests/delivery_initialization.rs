#![cfg(feature = "test-support")]
#![allow(clippy::unwrap_used)]
#[path = "support/delivery_node.rs"]
#[expect(dead_code, reason = "shared node helpers serve other journeys")]
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
use std::{sync::Arc, time::Duration};
use support::*;
use tokio::sync::Mutex;

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
async fn mailbox_native_initialization_retries_while_healthy_service_owners_progress() -> TestResult
{
    let directory = tempfile::tempdir()?;
    let relay_path = directory.path().join("relay");
    let mut relay = ServiceProcess::start(&relay_path).await?;
    let mut origin = DeliveryNode::start(&directory.path().join("origin"), 2).await?;
    let mut healthy_origin =
        DeliveryNode::start(&directory.path().join("healthy-origin"), 10).await?;
    let mut healthy = DeliveryNode::start(&directory.path().join("healthy"), 4).await?;
    let mut delayed = DeliveryNode::start(&directory.path().join("delayed"), 8).await?;
    let mut probe = DeliveryNode::start(&directory.path().join("probe"), 12).await?;
    origin.connect(&relay).await?;
    healthy_origin.connect(&relay).await?;
    healthy.connect(&relay).await?;
    // Persist a real registry and authoritative copy before the faulted restart.
    let writer = Arc::new(
        ReplicatedDatabase::create(
            &directory.path().join("writer.db"),
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
        _ => return Err("no writer registration".into()),
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
        _ => return Err("no initial renewal".into()),
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
    let expiring = item_id(880)?;
    origin
        .command(NodeCommand::Enqueue {
            item: expiring,
            recipient: peer(8)?,
            body: "expiry proves autonomous initialization retry".into(),
            valid_until: Some(SystemClock.now().after(Duration::from_secs(5))),
        })
        .await?;
    origin.wait(expiring, CustodySide::Outbox, "sent").await?;
    let retained = enqueue(&mut origin, 881, peer(8)?).await?;
    origin.wait(retained, CustodySide::Outbox, "sent").await?;
    // Make a second mailbox an inventoried healthy owner, too.
    let seed = enqueue(&mut healthy_origin, 882, peer(4)?).await?;
    healthy_origin
        .wait(seed, CustodySide::Outbox, "sent")
        .await?;
    relay.kill().await?;

    // The child acquires a real native BEGIN lock BEFORE FleetService::open.
    // No DeliveryOwner or native ObjectStore entry exists for mailbox8 yet.
    relay = ServiceProcess::start_with_mailbox_init_lock(&relay_path, peer(8)?).await?;
    let mut control = relay.connect("control", "Bearer consumer-control").await?;
    let ControlAnswer::ServiceEvent(initialization) = control_request(
        &mut control,
        ControlRequest::WaitMailboxEvent {
            mailbox: peer(8)?,
            after: 0,
        },
    )
    .await?
    else {
        return Err("missing initialization failure".into());
    };
    assert_eq!(
        initialization.fields.get("phase").map(String::as_str),
        Some("initialization")
    );
    assert_eq!(
        initialization.fields.get("message").map(String::as_str),
        Some("delivery mailbox operation retry")
    );
    eprintln!(
        "OBS A10 startup actual pre-owner native journal initialization failure: {:?}",
        initialization.fields
    );
    let mut waiting_control = relay.connect("control", "Bearer consumer-control").await?;
    let opened = control_request(
        &mut waiting_control,
        ControlRequest::WaitDeliveryOwner { mailbox: peer(8)? },
    );
    tokio::pin!(opened);
    let healthy_progress = async {
        origin.connect(&relay).await?;
        healthy_origin.connect(&relay).await?;
        healthy.connect(&relay).await?;
        probe.connect(&relay).await?;
        assert!(
            probe
                .command(NodeCommand::Query {
                    mailbox: peer(8)?,
                    items: vec![retained]
                })
                .await
                .is_err(),
            "initializing mailbox requests must use backend failure, not an item answer"
        );
        assert_eq!(
            origin
                .command(NodeCommand::Inspect { item: retained })
                .await?["outbox"]["state"]["value"]["status"]["kind"],
            "sent"
        );
        let good = enqueue(&mut healthy_origin, 883, peer(4)?).await?;
        healthy_origin
            .wait(good, CustodySide::Outbox, "sent")
            .await?;
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
        healthy_origin
            .wait(good, CustodySide::Outbox, "delivered")
            .await?;
        let mut fresh = relay
            .peer(14, SessionId::new(durable_steps::Uuid::now_v7())?)
            .await?;
        assert!(
            matches!(request(&mut fresh, OperationRequest::Consumer(SampleRequest::Echo { text: "initialization isolated".into() })).await?,
        OperationAnswer::Consumer(SampleAnswer::Echo { text, .. }) if text == "initialization isolated")
        );
        *transport.0.lock().await = relay
            .peer(6, SessionId::new(durable_steps::Uuid::now_v7())?)
            .await?;
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
            _ => return Err("no renewal during mailbox initialization failure".into()),
        };
        writer.observe_renewal(renewed, started).await?;
        let mut write = writer.begin_write().await?;
        insert_sample(
            &mut write,
            "01900000-0000-7000-8000-000000000390".into(),
            "copy alive during initialization fault".into(),
            1,
            vec![1, 2],
        )
        .await?;
        write.commit().await?;
        replication.replicate_once(receiver).await?;
        assert_eq!(
            copy_rows(&mut *transport.0.lock().await, writer.database())
                .await?
                .get("samples")
                .ok_or("missing healthy copy")?
                .len(),
            1
        );
        let ControlAnswer::ServiceEvents(events) =
            control_request(&mut control, ControlRequest::ServiceEvents).await?
        else {
            return Err("no service events".into());
        };
        assert!(
            !events
                .iter()
                .any(|event| event.fields.get("message").map(String::as_str)
                    == Some("delivery mailbox unavailable"))
        );
        assert!(relay.child.try_wait()?.is_none());
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    tokio::select! {
        biased;
        _ = &mut opened => return Err("owner acquisition observer completed before releasing its BEGIN lock".into()),
        result = healthy_progress => result?,
    }
    control_request(
        &mut control,
        ControlRequest::ReleaseDeliveryWrite { mailbox: peer(8)? },
    )
    .await?;
    assert!(matches!(opened.await?, ControlAnswer::Ok));
    // No Fetch, Query, Submit or peer reconnect touches mailbox8 after release.
    // Its per-key runner must reacquire the native object and expire custody by itself.
    origin
        .wait(expiring, CustodySide::Outbox, "expired")
        .await?;
    let (row, bytes) =
        support::delivery_mail::mail_row(&mut control, peer(8)?, peer(2)?, retained).await?;
    assert_eq!(row["value"]["status"]["kind"], "sent");
    assert!(bytes > 0);
    delayed.connect(&relay).await?;
    delayed
        .command(NodeCommand::Receive {
            stream: DeliveryStream::Items,
        })
        .await?;
    assert_eq!(
        effect_count(
            &delayed
                .wait(retained, CustodySide::Inbox, "delivered")
                .await?,
            retained
        ),
        1
    );
    origin
        .wait(retained, CustodySide::Outbox, "delivered")
        .await?;
    eprintln!(
        "OBS A10 startup: healthy mailbox, fresh session, renewal and existing copy progressed under pre-owner BEGIN fault; release alone enabled autonomous expiry and preserved item delivered once"
    );
    probe.stop().await?;
    delayed.stop().await?;
    healthy.stop().await?;
    healthy_origin.stop().await?;
    origin.stop().await?;
    relay.kill().await?;
    Ok(())
}
