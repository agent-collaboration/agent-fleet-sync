mod conformance;
#[cfg(feature = "turso")]
#[path = "conformance/native.rs"]
mod native;
mod support;

use conformance::BusinessScope;
use durable_steps::{
    Host, StoreError,
    volatile_store::{VolatileScope, VolatileStore},
};
use std::sync::Arc;
use support::TestHost;

impl<H: Host> BusinessScope for VolatileScope<'_, H> {
    async fn read_counter(&mut self, key: &str) -> Result<Option<u64>, StoreError> {
        self.read_business(key)
    }
    async fn write_counter(&mut self, key: &str, value: u64) -> Result<(), StoreError> {
        self.write_business(key, &value)
    }
}

#[test]
fn volatile_shared_conformance() -> Result<(), StoreError> {
    futures_executor::block_on(async {
        let host = TestHost::new();
        let store = Arc::new(VolatileStore::new(Arc::clone(&host)));
        let cases = conformance::run(store, host).await?;
        println!("volatile: {} shared cases passed: {cases:?}", cases.len());
        Ok(())
    })
}
