//! A production pump keeps receiving during attach but sends requests after Welcome.
use super::controls::*;
use durable_steps::{CallFailure, NotSentReason};
use fleet_sync_session::{ClientEvent, Endpoint, PeerCredential, SessionClient};
use fleet_sync_test_consumer::{ConsumerStore, SampleAnswer, SampleRequest};
use fleet_sync_tokio_runner::SessionPump;
use fleet_sync_transport_websocket::WebSocketTransport;
use runtime_primitives::{Backoff, UuidV7Ids};
use tokio::sync::watch;

async fn local_timeout(
    root: &Path,
    endpoint: Endpoint,
    observations: &mut Observations,
) -> TestResult {
    let store = ConsumerStore::open(&root.join("unstarted-client.db")).await?;
    let client = SessionClient::<_, _, SampleProtocol>::load(
        WebSocketTransport::default(),
        store.clone(),
        &UuidV7Ids,
        SchemaSupport { max_version: 5 },
        Backoff {
            initial: Duration::from_millis(10),
            max: Duration::from_secs(1),
            factor: 2,
        },
    )
    .await?;
    let (requests, _events, pump) = SessionPump::new(
        client,
        endpoint,
        PeerCredential::new("Bearer peer-4")?,
        Duration::from_millis(50),
    );
    // Keep the production command receiver alive, deliberately without polling
    // the pump. This reaches SessionRequests' real local reply deadline.
    let answer = requests
        .request(OperationRequest::Consumer(SampleRequest::Echo {
            text: "queued without a running pump".into(),
        }))
        .await;
    let passed = matches!(answer, Err(CallFailure::AnswerLost));
    let observed = match answer {
        Ok(answer) => json!({"answer":answer}),
        Err(error) => json!({"failure":format!("{error:?}")}),
    };
    observations.check(
        "local-reply-timeout-stays-answer-lost",
        passed,
        json!({"failure":"AnswerLost","scope":"unstarted production pump; no network dispatch"}),
        observed,
    );
    drop(requests);
    drop(pump);
    store.close().await;
    Ok(())
}

pub(super) async fn run(root: &Path, observations: &mut Observations) -> TestResult {
    let path = root.join("client-readiness");
    let service = start_service(&path).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let peer = new_peer().await?;
    schema_request(
        &mut control,
        SchemaControlRequest::HoldReport {
            peer,
            attach_seq: AttachSequence::new(1)?,
        },
    )
    .await?;
    let store = ConsumerStore::open(&path.join("client.db")).await?;
    let client = SessionClient::<_, _, SampleProtocol>::load(
        WebSocketTransport::default(),
        store.clone(),
        &UuidV7Ids,
        SchemaSupport { max_version: 5 },
        Backoff {
            initial: Duration::from_millis(10),
            max: Duration::from_secs(1),
            factor: 2,
        },
    )
    .await?;
    let session = client.state().session_id;
    let (requests, mut events, pump) = SessionPump::new(
        client,
        Endpoint::new(&format!("ws://127.0.0.1:{}/session", service.port))?,
        PeerCredential::new(format!("Bearer peer-{}", peer.as_uuid().as_u128()))?,
        Duration::from_secs(10),
    );
    let (stop, stopped) = watch::channel(false);
    let pumping = tokio::spawn(pump.run(stopped));
    let result: TestResult = async {
        local_timeout(
            &path,
            Endpoint::new(&format!("ws://127.0.0.1:{}/session", service.port))?,
            observations,
        )
        .await?;
        let report = held_report(&mut control).await?;
        if report.session_id != session {
            return Err("readiness control held the wrong session".into());
        }
        let before = requests
            .request(OperationRequest::Consumer(SampleRequest::Echo {
                text: "before Welcome".into(),
            }))
            .await;
        let count = control_request(&mut control, ControlRequest::RequestCount).await?;
        let not_sent = matches!(before, Err(CallFailure::NotSent {
            reason: NotSentReason::Unreachable { .. }
        }));
        let observed = match before {
            Ok(answer) => json!({"answer":answer}),
            Err(error) => json!({"failure":format!("{error:?}")}),
        };
        observations.check(
            "production-request-before-welcome-is-not-sent",
            not_sent && matches!(count, ControlAnswer::RequestCount(0)),
            json!({"failure":"notSent","requests":0}),
            json!({"result":observed,"count":count}),
        );
        schema_request(&mut control, SchemaControlRequest::ReleaseReport).await?;
        let event = tokio::time::timeout(Duration::from_secs(15), events.recv()).await?;
        if !matches!(event, Some(ClientEvent::Welcome { .. })) {
            return Err("production pump did not receive Welcome".into());
        }
        let after = requests
            .request(OperationRequest::Consumer(SampleRequest::Echo {
                text: "after Welcome".into(),
            }))
            .await?;
        let count = control_request(&mut control, ControlRequest::RequestCount).await?;
        observations.check(
            "production-request-after-welcome-is-served",
            matches!(&after, OperationAnswer::Consumer(SampleAnswer::Echo { text, .. }) if text=="after Welcome")
                && matches!(count, ControlAnswer::RequestCount(1)),
            json!({"text":"after Welcome","requests":1}),
            json!({"answer":after,"count":count}),
        );
        Ok(())
    }
    .await;
    stop.send_replace(true);
    tokio::time::timeout(Duration::from_secs(10), pumping).await???;
    store.close().await;
    // Release is idempotent, including a failure before normal publication.
    schema_request(&mut control, SchemaControlRequest::ReleaseReport).await?;
    service.stop(&mut control).await?;
    result
}
