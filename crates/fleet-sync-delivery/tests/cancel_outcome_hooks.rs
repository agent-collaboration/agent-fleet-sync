#![allow(clippy::unwrap_used)]
#[path = "support/local_cancellation.rs"]
mod local_cancellation;
#[path = "support/projection.rs"]
mod projection;
mod support;

use durable_steps::{AtomicStore, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use local_cancellation::{enqueue_original, outcome, snapshot};
use projection::{
    ProjectionConsumer, enqueue_projection_attempt, prepare_projection_table, projection_engine,
    read_committed_state,
};
use sqlx_turso::Turso;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use support::{network::NetworkFixture, *};

#[derive(Default)]
struct CancelConsumer {
    projection: ProjectionConsumer,
    refuse_cancel: AtomicBool,
}
impl<S: TransactionScope<Database = Turso>> DeliveryConsumer<S> for CancelConsumer {
    type Protocol = TestProtocol;

    async fn on_outcome(
        &self,
        scope: &mut S,
        item: ItemId,
        outcome: &RecipientOutcome<Acceptance>,
        observation: Option<&()>,
    ) -> Result<(), DeliveryError> {
        self.projection
            .on_outcome(scope, item, outcome, observation)
            .await
    }
    async fn on_cancel_outcome(
        &self,
        scope: &mut S,
        cancel: ItemId,
        target: ItemId,
        outcome: &RecipientOutcome<Acceptance>,
    ) -> Result<(), DeliveryError> {
        write_cancel_projection(scope, cancel, target, outcome).await?;
        if self.refuse_cancel.load(Ordering::SeqCst) {
            return Err(DeliveryError::Storage("cancel projection refused".into()));
        }
        Ok(())
    }
    async fn release_gate(
        &self,
        scope: &mut S,
        item: &DeliveryItem<TestTypes>,
    ) -> Result<GateDecision, DeliveryError> {
        TestConsumer.release_gate(scope, item).await
    }
    async fn deliver_locally(
        &self,
        item: &DeliveryItem<TestTypes>,
        call: CallId,
    ) -> LocalDelivery<TestTypes> {
        <TestConsumer as DeliveryConsumer<S>>::deliver_locally(&TestConsumer, item, call).await
    }
    fn may_cancel(&self, requester: &String, item: &DeliveryHeader<TestTypes>) -> bool {
        <TestConsumer as DeliveryConsumer<S>>::may_cancel(&TestConsumer, requester, item)
    }
}

// The explicit Send future follows the existing projection fixture's SQLx
// lifetime boundary for higher-ranked transaction callbacks.
fn write_cancel_projection<'scope, S: TransactionScope<Database = Turso> + 'scope>(
    scope: &'scope mut S,
    cancel: ItemId,
    target: ItemId,
    outcome: &RecipientOutcome<Acceptance>,
) -> impl std::future::Future<Output = Result<(), DeliveryError>> + Send + 'scope {
    let value = serde_json::json!({
        "target": target.as_uuid().to_string(),
        "outcome": outcome,
    });
    async move {
        sqlx_core::query::query::<Turso>(
            "INSERT INTO application_projection(item,kind,value) VALUES(?1,'cancelOutcome',?2)",
        )
        .bind(cancel.as_uuid().to_string())
        .bind(value.to_string())
        .execute(scope.executor())
        .await
        .map_err(|error| DeliveryError::Storage(error.to_string()))?;
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum CancelRoute {
    Local,
    Remote,
}
#[derive(Clone, Copy)]
enum CancelKind {
    Authorized,
    WrongSender,
    Expired,
}
async fn submit_cancel(
    fixture: &Fixture,
    route: CancelRoute,
    replay: Option<ReceiveInput>,
    kind: CancelKind,
    consumer: Arc<CancelConsumer>,
) -> Result<Option<ReceiveInput>, StoreError> {
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let sender = match kind {
                    CancelKind::WrongSender => "other sender",
                    CancelKind::Authorized | CancelKind::Expired => "sender",
                };
                let expiry = match kind {
                    CancelKind::Expired => Some(SyncTime::from_unix_millis(1)),
                    CancelKind::Authorized | CancelKind::WrongSender => None,
                };
                let cancel = DeliveryCancel::<TestTypes>::new(
                    item_id(2),
                    sender.into(),
                    Recipient {
                        peer: peer(2),
                        address: "inbox".into(),
                    },
                    expiry,
                    CancelBody { target: item_id(1) },
                )
                .unwrap();
                let mut inbox = Inbox::<_, TestTypes>::new(scope, peer(2), limits());
                match route {
                    CancelRoute::Local => {
                        inbox
                            .enqueue_local_cancel(
                                cancel,
                                consumer.as_ref(),
                                SyncTime::from_unix_millis(1),
                            )
                            .await
                            .map_err(storage_error)?;
                        Ok(None)
                    }
                    CancelRoute::Remote => {
                        let input = match replay {
                            Some(input) => input,
                            None => inbox
                                .begin_receive(DeliveryStream::Controls)
                                .await
                                .map_err(storage_error)?
                                .unwrap(),
                        };
                        inbox
                            .store_batch(
                                input,
                                projection::projection_batch(DeliveryEnvelope::Cancel(cancel)),
                                consumer.as_ref(),
                                SyncTime::from_unix_millis(1),
                            )
                            .await
                            .map_err(storage_error)?;
                        Ok(Some(input))
                    }
                }
            })
        })
        .await
}

async fn assert_target_projection(fixture: &Fixture) {
    let state = read_committed_state(fixture).await;
    let cancel_key = item_id(2).as_uuid().to_string();
    let events: Vec<_> = state
        .projections
        .iter()
        .filter(|(_, kind, _)| kind == "cancelOutcome")
        .collect();
    assert_eq!(
        events.len(),
        1,
        "one committed target projection per control"
    );
    let (cancel, _, encoded) = events.first().unwrap();
    assert_eq!(cancel, &cancel_key);
    let value: serde_json::Value = serde_json::from_str(encoded).unwrap();
    assert_eq!(
        value.get("target").unwrap(),
        &serde_json::json!(item_id(1).as_uuid().to_string())
    );
    let stored = outcome(fixture, 2).await.unwrap();
    assert_eq!(
        value.get("outcome").unwrap(),
        &serde_json::to_value(stored).unwrap()
    );
    let recorded = state
        .projections
        .iter()
        .position(|(item, kind, _)| item == &cancel_key && kind == "outcome")
        .unwrap();
    let projected = state
        .projections
        .iter()
        .position(|(_, kind, _)| kind == "cancelOutcome")
        .unwrap();
    assert_eq!(
        projected,
        recorded + 1,
        "hooks share the outcome transaction"
    );
}

async fn prove_replay(
    fixture: &Fixture,
    route: CancelRoute,
    input: Option<ReceiveInput>,
    kind: CancelKind,
    consumer: Arc<CancelConsumer>,
) {
    let before = read_committed_state(fixture).await;
    submit_cancel(fixture, route, input, kind, consumer)
        .await
        .unwrap();
    assert_eq!(read_committed_state(fixture).await, before);
    assert_target_projection(fixture).await;
}

async fn prove_too_late(route: CancelRoute) {
    let network = NetworkFixture::new().await;
    let fixture = &network.recipient;
    prepare_projection_table(fixture).await;
    let input = enqueue_projection_attempt(fixture, 1, None).await;
    projection_engine(fixture, Arc::new(ProjectionConsumer::default()))
        .start_and_settle::<projection::ProjectionTask>(
            &delivery_request_key(&(input.item, input.attempt)).unwrap(),
            input,
        )
        .await
        .unwrap();
    assert!(matches!(
        outcome(fixture, 1).await,
        Some(RecipientOutcome::Delivered { .. })
    ));
    let consumer = Arc::new(CancelConsumer::default());
    let receive = submit_cancel(
        fixture,
        route,
        None,
        CancelKind::Authorized,
        consumer.clone(),
    )
    .await
    .unwrap();
    assert!(matches!(
        outcome(fixture, 2).await,
        Some(RecipientOutcome::TooLate { cancel, at: TooLateAt::Delivered })
            if cancel == item_id(2)
    ));
    assert_target_projection(fixture).await;
    match route {
        CancelRoute::Local => assert!(snapshot(fixture).await.reports.is_empty()),
        CancelRoute::Remote => assert_eq!(snapshot(fixture).await.reports.len(), 1),
    }
    prove_replay(fixture, route, receive, CancelKind::Authorized, consumer).await;
}

#[tokio::test]
async fn a_local_too_late_control_projects_its_target_without_a_report() {
    prove_too_late(CancelRoute::Local).await;
}
#[tokio::test]
async fn a_remote_too_late_control_projects_its_target_and_keeps_its_report() {
    prove_too_late(CancelRoute::Remote).await;
}

async fn prove_pending_control(kind: CancelKind) {
    for route in [CancelRoute::Local, CancelRoute::Remote] {
        let fixture = Fixture::new().await;
        prepare_projection_table(&fixture).await;
        require_local_attempt(enqueue_original(&fixture, 1, limits()).await).unwrap();
        let consumer = Arc::new(CancelConsumer::default());
        let receive = submit_cancel(&fixture, route, None, kind, consumer.clone())
            .await
            .unwrap();
        let stored = outcome(&fixture, 2).await.unwrap();
        match kind {
            CancelKind::Authorized => assert!(
                matches!(stored, RecipientOutcome::Prevented { cancel } if cancel == item_id(2))
            ),
            CancelKind::WrongSender => assert!(matches!(
                stored,
                RecipientOutcome::Refused {
                    reason: DeliveryRefusal::CancelSenderMismatch
                }
            )),
            CancelKind::Expired => assert!(matches!(stored, RecipientOutcome::Expired)),
        }
        assert_target_projection(&fixture).await;
        prove_replay(&fixture, route, receive, kind, consumer).await;
    }
}
#[tokio::test]
async fn prevented_controls_project_their_target_on_both_routes() {
    prove_pending_control(CancelKind::Authorized).await;
}
#[tokio::test]
async fn refused_controls_project_their_target_on_both_routes() {
    prove_pending_control(CancelKind::WrongSender).await;
}
#[tokio::test]
async fn expired_controls_project_their_target_on_both_routes() {
    prove_pending_control(CancelKind::Expired).await;
}

#[tokio::test]
async fn a_refused_target_projection_rolls_back_control_and_both_hooks() {
    for route in [CancelRoute::Local, CancelRoute::Remote] {
        let fixture = Fixture::new().await;
        prepare_projection_table(&fixture).await;
        require_local_attempt(enqueue_original(&fixture, 1, limits()).await).unwrap();
        let before = read_committed_state(&fixture).await;
        let custody_before = snapshot(&fixture).await;
        let consumer = Arc::new(CancelConsumer::default());
        consumer.refuse_cancel.store(true, Ordering::SeqCst);
        let error = submit_cancel(
            &fixture,
            route,
            None,
            CancelKind::Authorized,
            consumer.clone(),
        )
        .await
        .err()
        .unwrap();
        assert!(error.to_string().contains("cancel projection refused"));
        assert_eq!(read_committed_state(&fixture).await, before);
        assert_eq!(snapshot(&fixture).await, custody_before);
        consumer.refuse_cancel.store(false, Ordering::SeqCst);
        submit_cancel(&fixture, route, None, CancelKind::Authorized, consumer)
            .await
            .unwrap();
        assert_target_projection(&fixture).await;
    }
}
