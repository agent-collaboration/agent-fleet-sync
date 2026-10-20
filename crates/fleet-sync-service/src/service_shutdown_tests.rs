#![allow(clippy::panic_in_result_fn)]
use super::*;
use crate::{ConsumerHandler, ServiceScope, service_state::SourceRunner};
use durable_steps::{CallFailure, CallId, NotSentReason, OperationId, StepName, Uuid};
use fleet_sync_lease::{GateFailure, HolderChangeGate, LeasePolicy, LifecycleGate, RegistryConfig};
use fleet_sync_protocol::*;
use fleet_sync_receiver::{
    AppliedPiece, FollowUpDone, FollowUpFailure, FollowUpHandler, HookError, ReceiverRules,
};
use fleet_sync_session::VerifiedPeer;
use fleet_sync_tokio_runner::{CallEnvelope, CallRouter};
use std::{
    future::{Future, poll_fn},
    sync::atomic::{AtomicUsize, Ordering},
    task::Poll,
    time::Duration,
};
use tokio::sync::oneshot;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct UnusedConsumer {
    routed: AtomicUsize,
}
impl ConsumerHandler<NoConsumer> for UnusedConsumer {
    async fn request(&self, _: PeerId, (): ()) -> Result<(), ServiceError> {
        panic!("shutdown must not route a consumer request")
    }
}
impl ServiceConsumer<NoConsumer> for UnusedConsumer {
    type HookItem = ();
}
impl ReceiverRules<ServiceScope<'_>> for UnusedConsumer {
    type Protocol = NoConsumer;
    type FollowUp = ();
    async fn on_applied(
        &self,
        _: &mut ServiceScope<'_>,
        _: &AppliedPiece<'_, ()>,
    ) -> Result<Vec<()>, HookError> {
        panic!("shutdown must not apply replication")
    }
}
impl FollowUpHandler for UnusedConsumer {
    type Item = ();
    async fn follow_up(&self, (): &(), _: CallId) -> Result<FollowUpDone, FollowUpFailure> {
        self.routed.fetch_add(1, Ordering::SeqCst);
        Ok(FollowUpDone)
    }
}
impl LifecycleGate for UnusedConsumer {
    async fn admit_registration(
        &self,
        _: &DatabaseId,
        _: &PeerId,
        _: CallId,
    ) -> Result<GateAnswer, GateFailure> {
        panic!("shutdown must not register a database")
    }
    async fn admit_retirement(&self, _: &DatabaseId, _: CallId) -> Result<GateAnswer, GateFailure> {
        panic!("shutdown must not retire a database")
    }
}
impl HolderChangeGate for UnusedConsumer {
    async fn reserve(&self, _: &HolderChangeRequest, _: CallId) -> Result<GateAnswer, GateFailure> {
        panic!("shutdown must not reserve a holder")
    }
    async fn settle(
        &self,
        _: &HolderChangeRequest,
        _: SettleOutcome,
        _: CallId,
    ) -> Result<(), GateFailure> {
        panic!("shutdown must not settle a holder")
    }
}
struct UnusedAuthenticator;
impl PeerAuthenticator for UnusedAuthenticator {
    type Material = String;
    type Route = ();
    async fn verify(&self, _: String) -> Result<(VerifiedPeer, ()), AuthRefusal> {
        panic!("shutdown must not authenticate a connection")
    }
}

fn follow_up_call(step: &str) -> TestResult<CallEnvelope> {
    Ok(CallEnvelope {
        kind: "fleet-sync.apply-follow-up",
        target: serde_json::to_value(fleet_sync_receiver::FollowUpCall::<()>::new(
            DatabaseId::new(Uuid::now_v7())?,
        ))?,
        request: serde_json::Value::Null,
        call: CallId {
            operation: OperationId::new(Uuid::now_v7())?,
            step: StepName::new(step)?,
        },
    })
}

#[tokio::test]
async fn shutdown_refuses_buffered_calls_before_joining_their_dispatcher() -> TestResult {
    // Arrange: poll each actual dispatcher to its unanswered receive. No task
    // scheduling or elapsed delay is used as evidence that the calls queued.
    let directory = tempfile::tempdir()?;
    let receiver = ReceiverId::new(Uuid::now_v7())?;
    let consumer = Arc::new(UnusedConsumer {
        routed: AtomicUsize::new(0),
    });
    let service = FleetService::<NoConsumer, _, _>::open(
        ServiceOptions {
            directory: directory.path().to_owned(),
            delivery: None,
            receiver,
            registry: RegistryConfig {
                lease: LeasePolicy {
                    lease_duration: Duration::from_secs(600),
                    renew_interval: Duration::from_secs(30),
                },
                authoritative_receiver: receiver,
                release_unsent_for: Duration::from_secs(60),
                move_needs_renewal_within: None,
            },
            call_timeout: Duration::from_secs(30),
            socket_timeout: Duration::from_secs(2),
        },
        consumer.clone(),
        Arc::new(UnusedAuthenticator),
    )
    .await?;
    let calls = service.state.calls.clone();
    let mut dispatches = Vec::new();
    for step in ["first-buffered", "second-buffered"] {
        let router = calls.clone();
        let envelope = follow_up_call(step)?;
        let mut dispatch = Box::pin(async move { router.dispatch(envelope).await });
        poll_fn(|context| {
            assert!(dispatch.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        dispatches.push(dispatch);
    }
    assert_eq!(service.calls.len(), 2);
    let (completed, answers) = oneshot::channel();
    // Use the existing owned-runner slot to reproduce the join dependency:
    // serve cannot finish while this real dispatcher awaits a queued answer.
    service
        .state
        .source_runners
        .lock()
        .await
        .push(SourceRunner {
            run: Box::pin(async move {
                let mut results = Vec::new();
                for dispatch in dispatches {
                    results.push(dispatch.await);
                }
                let _ = completed.send(results);
                Ok(())
            }),
            stop: watch::channel(false).0,
        });
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    // Act: the public shutdown signal gates routing before the select loop.
    let (_stop, stopped) = watch::channel(true);
    let mut serving = tokio::spawn(service.serve(listener, axum::Router::new(), stopped));
    let (answers, served) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(2), answers),
        tokio::time::timeout(Duration::from_secs(3), &mut serving),
    );
    if served.is_err() {
        serving.abort();
        let _ = serving.await;
    }
    // Assert: both reply and join deadlines are far below the call timeout.
    assert!(
        answers.is_ok() && served.is_ok(),
        "buffered dispatch and serve must finish before call_timeout: replies={answers:?}, serve={served:?}"
    );
    let answers = answers??;
    assert_eq!(answers.len(), 2);
    for answer in answers {
        assert_eq!(
            answer,
            Err(CallFailure::NotSent {
                reason: NotSentReason::Unreachable { last_seen: None },
            })
        );
    }
    served???;
    assert_eq!(consumer.routed.load(Ordering::SeqCst), 0);
    assert_eq!(
        calls.dispatch(follow_up_call("after-close")?).await,
        Err(CallFailure::NotSent {
            reason: NotSentReason::Unreachable { last_seen: None },
        })
    );
    Ok(())
}
