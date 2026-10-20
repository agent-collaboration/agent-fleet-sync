#![allow(clippy::unwrap_used, dead_code)]

use crate::support::{network::config, *};
use durable_steps::{AtomicStore, Engine, JournalScope, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::row::Row;
use sqlx_turso::Turso;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

pub(crate) type ProjectionTask = DeliverLocally<TestStore, Turso, TestTypes, ProjectionConsumer>;

#[derive(Default)]
pub(crate) struct ProjectionConsumer {
    pub(crate) refuse_received: AtomicBool,
    pub(crate) refuse_outcome: AtomicBool,
}

impl<S: TransactionScope<Database = Turso>> DeliveryConsumer<S> for ProjectionConsumer {
    type Protocol = TestProtocol;

    async fn on_received(
        &self,
        scope: &mut S,
        item: &DeliveryItem<TestTypes>,
    ) -> Result<(), DeliveryError> {
        write_projection(scope, item.id(), "received", item.body()).await?;
        if self.refuse_received.load(Ordering::SeqCst) {
            return Err(DeliveryError::Storage("received projection refused".into()));
        }
        Ok(())
    }

    async fn on_outcome(
        &self,
        scope: &mut S,
        item: ItemId,
        outcome: &RecipientOutcome<Acceptance>,
        _observation: Option<&()>,
    ) -> Result<(), DeliveryError> {
        let value = serde_json::to_string(outcome).unwrap();
        write_projection(scope, item, "outcome", &value).await?;
        if self.refuse_outcome.load(Ordering::SeqCst) {
            return Err(DeliveryError::Storage("outcome projection refused".into()));
        }
        Ok(())
    }

    async fn release_gate(
        &self,
        _scope: &mut S,
        _item: &DeliveryItem<TestTypes>,
    ) -> Result<GateDecision, DeliveryError> {
        Ok(GateDecision::Release)
    }

    async fn deliver_locally(
        &self,
        item: &DeliveryItem<TestTypes>,
        _call: CallId,
    ) -> LocalDelivery<TestTypes> {
        LocalDelivery {
            outcome: LocalOutcome::Delivered {
                acceptance: Acceptance {
                    receipt: item.body().clone(),
                },
            },
            observation: (),
        }
    }

    fn may_cancel(&self, requester: &String, item: &DeliveryHeader<TestTypes>) -> bool {
        requester == item.sender()
    }
}

#[expect(
    clippy::manual_async_fn,
    reason = "explicit Send future bounds avoid higher-ranked TransactionScope lifetime errors in transaction callbacks"
)]
fn write_projection<'scope, S: TransactionScope<Database = Turso> + 'scope>(
    scope: &'scope mut S,
    item: ItemId,
    kind: &'scope str,
    value: &'scope str,
) -> impl std::future::Future<Output = Result<(), DeliveryError>> + Send + 'scope {
    async move {
        sqlx_core::query::query::<Turso>(
            "INSERT INTO application_projection(item,kind,value) VALUES(?1,?2,?3)",
        )
        .bind(item.as_uuid().to_string())
        .bind(kind)
        .bind(value)
        .execute(scope.executor())
        .await
        .map_err(|error| DeliveryError::Storage(error.to_string()))?;
        Ok(())
    }
}

pub(crate) async fn prepare_projection_table(fixture: &Fixture) {
    fixture.store.transaction(|scope| Box::pin(async move {
        sqlx_core::query::query::<Turso>("CREATE TABLE application_projection(event INTEGER PRIMARY KEY,item TEXT NOT NULL,kind TEXT NOT NULL,value TEXT NOT NULL)")
            .execute(scope.executor()).await?;
        Ok(())
    })).await.unwrap();
}

#[derive(Debug, PartialEq)]
pub(crate) struct CommittedState {
    pub(crate) custody: Vec<(String, Option<String>)>,
    pub(crate) reports: Vec<String>,
    pub(crate) projections: Vec<(String, String, String)>,
}

pub(crate) async fn read_committed_state(fixture: &Fixture) -> CommittedState {
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let custody = sqlx_core::query::query::<Turso>(
                    "SELECT record,payload FROM _fleet_sync_inbox_items ORDER BY item_id",
                )
                .fetch_all(scope.executor())
                .await?
                .into_iter()
                .map(|row| Ok((row.try_get("record")?, row.try_get("payload")?)))
                .collect::<Result<Vec<_>, sqlx_core::Error>>()?;
                let reports = sqlx_core::query::query::<Turso>(
                    "SELECT record FROM _fleet_sync_reports ORDER BY item_id,report_seq",
                )
                .fetch_all(scope.executor())
                .await?
                .into_iter()
                .map(|row| row.try_get("record"))
                .collect::<Result<Vec<_>, _>>()?;
                let projections = sqlx_core::query::query::<Turso>(
                    "SELECT item,kind,value FROM application_projection ORDER BY event",
                )
                .fetch_all(scope.executor())
                .await?
                .into_iter()
                .map(|row| {
                    Ok((
                        row.try_get("item")?,
                        row.try_get("kind")?,
                        row.try_get("value")?,
                    ))
                })
                .collect::<Result<Vec<_>, sqlx_core::Error>>()?;
                Ok(CommittedState {
                    custody,
                    reports,
                    projections,
                })
            })
        })
        .await
        .unwrap()
}

pub(crate) fn projection_batch(envelope: DeliveryEnvelope<TestTypes>) -> FetchResult<TestTypes> {
    FetchResult {
        stream: envelope.stream(),
        entries: vec![FetchEntry::Item {
            seq: DeliverySeq::new(1),
            item: envelope,
        }],
        through: DeliverySeq::new(1),
    }
}

pub(crate) async fn receive_projection_item(
    fixture: &Fixture,
    consumer: Arc<ProjectionConsumer>,
    envelope: DeliveryEnvelope<TestTypes>,
    abort: bool,
) -> Result<ReceiveInput, StoreError> {
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let mut inbox = Inbox::<_, TestTypes>::new(scope, peer(2), limits());
                let input = inbox
                    .begin_receive(envelope.stream())
                    .await
                    .map_err(storage_error)?
                    .unwrap();
                inbox
                    .store_batch(
                        input,
                        projection_batch(envelope),
                        consumer.as_ref(),
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                if abort {
                    return Err(StoreError::backend("abort after custody and projections"));
                }
                Ok(input)
            })
        })
        .await
}

pub(crate) async fn enqueue_projection_attempt(
    fixture: &Fixture,
    id: u64,
    expiry: Option<SyncTime>,
) -> DeliveryAttempt {
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let original = DeliveryItem::<TestTypes>::new(
                    item_id(id),
                    "sender".into(),
                    Recipient {
                        peer: peer(2),
                        address: "inbox".into(),
                    },
                    expiry,
                    "rich receipt: accepted once".into(),
                )
                .unwrap();
                let mut inbox = Inbox::<_, TestTypes>::new(scope, peer(2), limits());
                let attempt = inbox
                    .enqueue_local(original, &TestConsumer, SyncTime::from_unix_millis(0))
                    .await
                    .map_err(storage_error)?
                    .unwrap();
                scope
                    .admit::<ProjectionTask>(
                        &delivery_request_key(&(attempt.item, attempt.attempt)).unwrap(),
                        &attempt,
                    )
                    .await
                    .map_err(StoreError::backend)?;
                Ok(attempt)
            })
        })
        .await
        .unwrap()
}

pub(crate) fn projection_engine(
    fixture: &Fixture,
    consumer: Arc<ProjectionConsumer>,
) -> Engine<TestStore, TestHost> {
    let mut engine = Engine::new(fixture.store.clone(), fixture.host.clone());
    engine
        .register(ProjectionTask::new(
            config().local,
            consumer,
            fixture.host.clone(),
        ))
        .unwrap();
    engine
}

pub(crate) fn outcome_values(state: &CommittedState) -> Vec<serde_json::Value> {
    state
        .projections
        .iter()
        .filter(|(_, kind, _)| kind == "outcome")
        .map(|(_, _, value)| serde_json::from_str(value).unwrap())
        .collect()
}
