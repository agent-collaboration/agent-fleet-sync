#![allow(clippy::unwrap_used, dead_code)]
pub mod interleaving;
pub mod network;
pub mod publication_network;
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
    fn may_cancel(&self, requester: &String, item: &DeliveryHeader<TestTypes>) -> bool {
        requester == item.sender()
    }
}
impl DeliveryTypes for TestTypes {
    const MAX_SENDER_BYTES: u32 = 64;
    const MAX_ADDRESS_BYTES: u32 = 64;
    const MAX_ACCEPTANCE_BYTES: u32 = 128;
    const MAX_STATUS_ROUTE_BYTES: u32 = 64;
    type Body = String;
    type Sender = String;
    type Address = String;
    type Acceptance = Acceptance;
    type StatusRoute = String;
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
        let answer = routes
            .dispatch(C::KIND, target, request, call, self.now())
            .await?;
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
                    let mut migrations = sqlx_core::migrate::Migrator::with_migrations(
                        DELIVERY_MIGRATOR.iter().cloned().collect::<Vec<_>>(),
                    );
                    migrations.dangerous_set_table_name("_fleet_sync_delivery_migrations");
                    migrations
                        .run_direct(None, scope.executor(), false)
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
    DeliveryLimits::new::<TestTypes>(
        100_000,
        20,
        100_000,
        20,
        100_000,
        minimum_settlement_allowance::<TestTypes>().unwrap(),
    )
    .unwrap()
}
pub fn configured_limits(
    mailbox_bytes: u64,
    mailbox_items: u64,
    inbox_bytes: u64,
) -> DeliveryLimits {
    DeliveryLimits::new::<TestTypes>(
        mailbox_bytes,
        mailbox_items,
        100_000,
        20,
        inbox_bytes,
        minimum_settlement_allowance::<TestTypes>().unwrap(),
    )
    .unwrap()
}
pub fn peer(value: u128) -> PeerId {
    PeerId::new(durable_steps::Uuid::from_u128(
        (value) | 0x019eef00000070008000000000000000,
    ))
    .unwrap()
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
        operation: OperationId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000063,
        ))
        .unwrap(),
        step: StepName::new(step).unwrap(),
    }
}
pub fn storage_error(error: DeliveryError) -> durable_steps::StoreError {
    durable_steps::StoreError::backend(error)
}
