#![allow(clippy::panic_in_result_fn)]
mod support;
use durable_steps::{
    AtomicStore, CallTarget, Engine, JournalScope, OperationContext, OperationFailure, RequestKey,
    StepName, StoreError, Task, volatile_store::VolatileStore,
};
use serde::{Deserialize, Serialize};
use std::{
    future::{Future, poll_fn},
    sync::{Arc, atomic::Ordering},
    task::Poll,
};

type Store = VolatileStore<support::TestHost>;
#[derive(Serialize, Deserialize)]
struct Read;
impl CallTarget for Read {
    const KIND: &'static str = "capacity-read";
    type Request = u64;
    type Answer = u64;
}
struct Reader;
impl Task for Reader {
    const KIND: &'static str = "capacity-reader";
    const VERSION: u32 = 1;
    type Store = Store;
    type Input = u64;
    type Output = u64;
    async fn run<C: OperationContext<Store = Store>>(
        &self,
        context: &mut C,
        input: &u64,
    ) -> Result<u64, OperationFailure> {
        let step = StepName::new("read").map_err(|error| OperationFailure::Storage {
            error: StoreError::backend(error),
        })?;
        Ok(context.call(step, Read, *input).await?)
    }
}

#[test]
fn overlapping_drives_share_the_eight_operation_capacity() -> Result<(), StoreError> {
    futures_executor::block_on(async {
        let host = support::TestHost::new();
        host.hang_after_effect.store(true, Ordering::SeqCst);
        let store = Arc::new(Store::new(host.clone()));
        let mut engine = Engine::new(store.clone(), host.clone());
        engine.register(Reader)?;
        for input in 0..10 {
            store
                .transaction(move |scope| {
                    Box::pin(async move {
                        let key = RequestKey::new(format!("reader-{input}"))
                            .map_err(StoreError::backend)?;
                        scope
                            .admit::<Reader>(&key, &input)
                            .await
                            .map_err(StoreError::backend)?;
                        Ok(())
                    })
                })
                .await?;
        }
        // This conformance store's transactions complete synchronously. One
        // poll reaches all available dispatch slots, each held by the host.
        // This proves execution capacity, not native storage or durability.
        let mut first = Box::pin(engine.drive_due());
        assert!(poll_fn(|context| Poll::Ready(first.as_mut().poll(context).is_pending())).await);
        assert_eq!(host.deliveries.load(Ordering::SeqCst), 8);
        let mut overlapping = Box::pin(engine.drive_due());
        assert!(
            poll_fn(|context| Poll::Ready(overlapping.as_mut().poll(context).is_pending())).await
        );
        assert_eq!(
            host.deliveries.load(Ordering::SeqCst),
            8,
            "overlapping drive acquired a second independent capacity pool"
        );
        // Neither future was spawned. Dropping the owned volatile probe models
        // interruption; real native recovery is covered by the existing suite.
        drop(overlapping);
        drop(first);
        Ok(())
    })
}
