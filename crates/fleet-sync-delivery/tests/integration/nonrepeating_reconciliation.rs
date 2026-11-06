use super::support::*;
use durable_steps::{
    AtomicStore, CallFailure, CallTarget, Engine, Host, JournalScope, OperationContext,
    OperationFailure, OperationState, RequestKey, StepName, Task, volatile_store::VolatileStore,
};
use fleet_sync_delivery::*;
use runtime_primitives::{Clock, IdSource, WakeFailure, WakeScheduler};
use std::{
    future::{Future, poll_fn},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::Poll,
};

struct DefaultReconciliationHost {
    source: Arc<TestHost>,
    dispatches: AtomicU64,
}
impl Clock for DefaultReconciliationHost {
    fn now(&self) -> SyncTime {
        self.source.now()
    }
}
impl IdSource for DefaultReconciliationHost {
    fn new_id(&self) -> durable_steps::Uuid {
        self.source.new_id()
    }
}
impl WakeScheduler for DefaultReconciliationHost {
    async fn wake_at(&self, at: SyncTime) -> Result<(), WakeFailure> {
        self.source.wake_at(at).await
    }
}
impl Host for DefaultReconciliationHost {
    async fn dispatch<C: CallTarget>(
        &self,
        _target: C,
        _request: C::Request,
        _call: CallId,
    ) -> Result<C::Answer, CallFailure> {
        self.dispatches.fetch_add(1, Ordering::SeqCst);
        std::future::pending().await
    }
}
struct DeliveryProbe;
impl Task for DeliveryProbe {
    const KIND: &'static str = "default-delivery-reconciliation";
    const VERSION: u32 = 1;
    type Store = VolatileStore<DefaultReconciliationHost>;
    type Input = ();
    type Output = ();
    async fn run<C: OperationContext<Store = Self::Store>>(
        &self,
        context: &mut C,
        _input: &(),
    ) -> Result<(), OperationFailure> {
        context
            .call(
                StepName::new("deliver").unwrap(),
                DeliverCall::<TestTypes>::new(),
                DeliveryItemRef {
                    item: item_id(1),
                    digest: item(1, "body").digest().clone(),
                },
            )
            .await?;
        Ok(())
    }
}

#[tokio::test]
async fn opted_in_delivery_keeps_default_host_uncertain_without_redispatch() {
    let host = Arc::new(DefaultReconciliationHost {
        source: host_at(SyncTime::from_unix_millis(0)),
        dispatches: AtomicU64::new(0),
    });
    let store = Arc::new(VolatileStore::new(host.clone()));
    let mut engine = Engine::new(store.clone(), host.clone());
    engine.register(DeliveryProbe).unwrap();
    let id = store
        .transaction(|scope| {
            Box::pin(async move {
                scope
                    .admit::<DeliveryProbe>(&RequestKey::new("delivery").unwrap(), &())
                    .await
                    .map(|admitted| admitted.id())
                    .map_err(durable_steps::StoreError::backend)
            })
        })
        .await
        .unwrap();
    let mut drive = Box::pin(engine.drive_due());
    poll_fn(|context| match drive.as_mut().poll(context) {
        Poll::Pending if host.dispatches.load(Ordering::SeqCst) == 1 => Poll::Ready(Ok(())),
        Poll::Pending => Poll::Pending,
        Poll::Ready(_) => Poll::Ready(Err(durable_steps::StoreError::backend(
            "fixture dispatch did not block",
        ))),
    })
    .await
    .unwrap();
    drop(drive);
    host.source.clock.store(30_001, Ordering::SeqCst);
    engine.drive_due().await.unwrap();
    assert!(
        matches!(engine.status(&id).await.unwrap().state,OperationState::Failed {reason:OperationFailure::Uncertain {step}} if step.as_str()=="deliver")
    );
    assert_eq!(host.dispatches.load(Ordering::SeqCst), 1);
    const {
        assert!(
            <DeliverCall<TestTypes> as CallTarget>::RECONCILES,
            "delivery target must opt in while host keeps the safe default"
        );
    }
}
