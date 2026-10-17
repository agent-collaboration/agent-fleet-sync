#![allow(clippy::unwrap_used, dead_code)]
pub mod interleaving;
pub mod network;
use durable_steps::{
    AtomicStore, CallFailure, CallId, CallTarget, Host, NotSentReason, SyncTime,
    turso_store::TursoStore,
};
use fleet_sync_delivery::*;
use runtime_primitives::{Clock, IdSource, WakeFailure, WakeScheduler};
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

pub type TestStore = TursoStore<TestHost>;
pub struct TestTypes;
pub struct TestProtocol;
impl ConsumerProtocol for TestProtocol {
    type Delivery = TestTypes;
    type Request = ();
    type Answer = ();
    type ServerRequest = ();
    type ClientAnswer = ();
    type Notice = ();
    type Annotation = ();
}
pub struct TestConsumer;
impl<S: durable_steps::storage::TransactionScope> DeliveryConsumer<S> for TestConsumer {
    type Protocol = TestProtocol;
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
    ) -> LocalOutcome<Acceptance> {
        LocalOutcome::Delivered {
            acceptance: Acceptance {
                receipt: format!("accepted {}", item.id().as_uuid()),
            },
        }
    }
    fn may_cancel(&self, requester: &String, item: &DeliveryItem<TestTypes>) -> bool {
        requester == item.sender()
    }
}
impl DeliveryTypes for TestTypes {
    type Body = String;
    type Sender = String;
    type Address = String;
    type Acceptance = Acceptance;
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Acceptance {
    pub receipt: String,
}
pub struct TestHost {
    pub clock: AtomicU64,
    identities: AtomicU64,
    pub routes: std::sync::OnceLock<Arc<network::TestNetwork>>,
}
impl Clock for TestHost {
    fn now(&self) -> SyncTime {
        SyncTime::from_unix_millis(self.clock.load(Ordering::SeqCst) as i64)
    }
}
impl IdSource for TestHost {
    fn new_id(&self) -> durable_steps::Uuid {
        durable_steps::Uuid::from_u128(
            0x01900000000070008000000000000000
                + u128::from(self.identities.fetch_add(1, Ordering::SeqCst))
                + 100,
        )
    }
}
impl WakeScheduler for TestHost {
    async fn wake_at(&self, _at: SyncTime) -> Result<(), WakeFailure> {
        Ok(())
    }
}
impl Host for TestHost {
    async fn dispatch<C: CallTarget>(
        &self,
        target: C,
        request: C::Request,
        call: CallId,
    ) -> Result<C::Answer, CallFailure> {
        let Some(routes) = self.routes.get() else {
            return Err(CallFailure::NotSent {
                reason: NotSentReason::Overloaded,
            });
        };
        let target = serde_json::to_value(target).unwrap();
        let request = serde_json::to_value(request).unwrap();
        let answer = routes.dispatch(C::KIND, target, request, call).await?;
        Ok(serde_json::from_value(answer).unwrap())
    }
}
pub struct Fixture {
    pub store: Arc<TestStore>,
    pub host: Arc<TestHost>,
    pub directory: tempfile::TempDir,
}
impl Fixture {
    pub async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let host = Arc::new(TestHost {
            clock: AtomicU64::new(0),
            identities: AtomicU64::new(0),
            routes: std::sync::OnceLock::new(),
        });
        let store = Arc::new(
            TursoStore::open_and_migrate(directory.path().join("delivery.db"), host.clone())
                .await
                .unwrap(),
        );
        store
            .transaction(|scope| {
                Box::pin(async move {
                    use durable_steps::storage::TransactionScope;
                    sqlx_core::raw_sql::raw_sql(include_str!(
                        "../../migrations/0001_delivery_custody.sql"
                    ))
                    .execute(scope.executor())
                    .await
                    .map_err(durable_steps::StoreError::backend)?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        Self {
            store,
            host,
            directory,
        }
    }
}
pub fn limits() -> DeliveryLimits {
    DeliveryLimits {
        mailbox_payload_bytes: 100_000,
        mailbox_items: 20,
        outbox_bytes: 100_000,
        outbox_items: 20,
        inbox_bytes: 100_000,
        settlement_allowance: 2048,
    }
}
pub fn peer(value: u128) -> PeerId {
    PeerId::new(durable_steps::Uuid::from_u128(value)).unwrap()
}
pub fn item_id(value: u64) -> ItemId {
    ItemId::new(durable_steps::Uuid::from_u128(
        0x01900000000070008000000000000000 + u128::from(value),
    ))
    .unwrap()
}
pub fn item(value: u64, body: &str) -> DeliveryEnvelope<TestTypes> {
    DeliveryEnvelope::Item(
        DeliveryItem::new(
            item_id(value),
            "sender".into(),
            Recipient {
                peer: peer(2),
                address: "inbox".into(),
            },
            None,
            body.into(),
        )
        .unwrap(),
    )
}
pub fn cancel(value: u64, target: u64) -> DeliveryEnvelope<TestTypes> {
    DeliveryEnvelope::Cancel(
        DeliveryCancel::new(
            item_id(value),
            "sender".into(),
            Recipient {
                peer: peer(2),
                address: "inbox".into(),
            },
            None,
            CancelBody {
                target: item_id(target),
            },
        )
        .unwrap(),
    )
}
pub fn call(step: &str) -> CallId {
    CallId {
        operation: OperationId(durable_steps::Uuid::from_u128(99)),
        step: StepName::new(step).unwrap(),
    }
}
pub fn storage_error(error: DeliveryError) -> durable_steps::StoreError {
    durable_steps::StoreError::backend(error)
}
