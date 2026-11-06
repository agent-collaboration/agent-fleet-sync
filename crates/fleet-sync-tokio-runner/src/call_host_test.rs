#![allow(clippy::panic_in_result_fn)]
use super::*;
use serde::{Deserialize, Serialize};
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
#[derive(Serialize, Deserialize)]
struct ReceiptTarget {
    destination: u64,
}
impl CallTarget for ReceiptTarget {
    const KIND: &'static str = "receipt-lookup";
    const DEDUPLICATES: bool = false;
    const RECONCILES: bool = true;
    type Request = u64;
    type Answer = u64;
}
struct ReceiptRouter {
    answer: Result<Option<Value>, CallFailure>,
    lookups: Mutex<Vec<CallEnvelope>>,
    dispatches: AtomicU64,
}
impl CallRouter for ReceiptRouter {
    async fn dispatch(&self, _call: CallEnvelope) -> Result<Value, CallFailure> {
        self.dispatches.fetch_add(1, Ordering::SeqCst);
        Ok(serde_json::json!(9))
    }
    async fn reconcile(&self, call: CallEnvelope) -> Result<Option<Value>, CallFailure> {
        self.lookups
            .lock()
            .map_err(|_| CallFailure::AnswerLost)?
            .push(call);
        self.answer.clone()
    }
}
fn call_id() -> Result<CallId, Box<dyn std::error::Error + Send + Sync>> {
    Ok(CallId {
        operation: durable_steps::OperationId::new(Uuid::from_u128(
            0x01900000000070008000000000000001,
        ))?,
        step: durable_steps::StepName::new("recover")?,
    })
}
fn router(answer: Result<Option<Value>, CallFailure>) -> Arc<ReceiptRouter> {
    Arc::new(ReceiptRouter {
        answer,
        lookups: Mutex::new(Vec::new()),
        dispatches: AtomicU64::new(0),
    })
}

#[tokio::test]
async fn reconciliation_forwards_exact_envelope_and_decodes_answer_without_dispatch() -> TestResult
{
    let router = router(Ok(Some(serde_json::json!(42))));
    let host = TokioHost::new(router.clone(), SystemClock);
    let call = call_id()?;
    assert_eq!(
        host.reconcile(ReceiptTarget { destination: 7 }, 21, call.clone())
            .await?,
        Some(42)
    );
    assert_eq!(router.dispatches.load(Ordering::SeqCst), 0);
    let lookups = router.lookups.lock().map_err(|_| "fixture lock poisoned")?;
    assert_eq!(lookups.len(), 1);
    let received = lookups.first().ok_or("missing reconciliation envelope")?;
    assert_eq!(received.kind, "receipt-lookup");
    assert_eq!(received.target, serde_json::json!({"destination":7}));
    assert_eq!(received.request, serde_json::json!(21));
    assert_eq!(received.call, call);
    Ok(())
}

#[tokio::test]
async fn reconciliation_none_and_transport_errors_are_forwarded_without_dispatch() -> TestResult {
    for answer in [
        Ok(None),
        Err(CallFailure::AnswerLost),
        Err(CallFailure::NotSent {
            reason: NotSentReason::Overloaded,
        }),
        Err(CallFailure::Retryable {
            reason: durable_steps::CallRetryReason {
                code: "busy".into(),
                detail: "lookup refused".into(),
            },
        }),
    ] {
        let expected = answer.clone().map(|value| value.map(|_| 0_u64));
        let router = router(answer);
        let host = TokioHost::new(router.clone(), SystemClock);
        assert_eq!(
            host.reconcile(ReceiptTarget { destination: 7 }, 21, call_id()?)
                .await,
            expected
        );
        assert_eq!(
            router
                .lookups
                .lock()
                .map_err(|_| "fixture lock poisoned")?
                .len(),
            1
        );
        assert_eq!(router.dispatches.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

#[tokio::test]
async fn malformed_reconciled_answer_is_answer_lost_and_never_dispatched() -> TestResult {
    let router = router(Ok(Some(serde_json::json!({"wrong":"shape"}))));
    let host = TokioHost::new(router.clone(), SystemClock);
    assert_eq!(
        host.reconcile(ReceiptTarget { destination: 7 }, 21, call_id()?)
            .await,
        Err(CallFailure::AnswerLost)
    );
    assert_eq!(router.dispatches.load(Ordering::SeqCst), 0);
    Ok(())
}

struct DispatchOnlyRouter;
impl CallRouter for DispatchOnlyRouter {
    async fn dispatch(&self, _call: CallEnvelope) -> Result<Value, CallFailure> {
        Ok(serde_json::json!(9))
    }
}
#[tokio::test]
async fn router_default_reconciliation_returns_none() -> TestResult {
    let host = TokioHost::new(Arc::new(DispatchOnlyRouter), SystemClock);
    assert_eq!(
        host.reconcile(ReceiptTarget { destination: 7 }, 21, call_id()?)
            .await?,
        None
    );
    Ok(())
}

#[tokio::test]
async fn ordinary_dispatch_does_not_consult_reconciliation() -> TestResult {
    let router = router(Ok(Some(serde_json::json!(42))));
    let host = TokioHost::new(router.clone(), SystemClock);
    assert_eq!(
        host.dispatch(ReceiptTarget { destination: 7 }, 21, call_id()?)
            .await?,
        9
    );
    assert!(
        router
            .lookups
            .lock()
            .map_err(|_| "fixture lock poisoned")?
            .is_empty()
    );
    assert_eq!(router.dispatches.load(Ordering::SeqCst), 1);
    Ok(())
}
