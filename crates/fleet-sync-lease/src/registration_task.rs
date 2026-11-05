use crate::{RegistryConfig, operation_support::fail_lifecycle, registration_rules::register};
use durable_steps::{
    AtomicStore, BoxFuture, OperationContext, OperationFailure, RequestKey, StoreError, Task,
    storage::TransactionScope,
};
use fleet_sync_protocol::{Registration, RegistrationAnswer};
use runtime_primitives::Clock;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::marker::PhantomData;

/// Register this task on the registry engine and admit it with lifecycle_request_key.
pub struct RegistrationTask<S, D, H> {
    config: RegistryConfig,
    clock: std::sync::Arc<dyn Clock>,
    hook: std::sync::Arc<H>,
    store: PhantomData<fn() -> (S, D)>,
}
impl<S, D, H> RegistrationTask<S, D, H> {
    pub const fn new(
        config: RegistryConfig,
        clock: std::sync::Arc<dyn Clock>,
        hook: std::sync::Arc<H>,
    ) -> Self {
        Self {
            config,
            clock,
            hook,
            store: PhantomData,
        }
    }
}
impl<S, D, H> Task for RegistrationTask<S, D, H>
where
    S: AtomicStore,
    for<'t> H: crate::LeaseChangeHook<S::Scope<'t>>,
    D: Database,
    for<'t> S::Scope<'t>: TransactionScope<Database = D>,
    D::Arguments: IntoArguments<D>,
    for<'value> String: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'value> Option<String>: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'row> &'row str: ColumnIndex<D::Row>,
{
    const KIND: &'static str = "fleet-sync.register";
    const VERSION: u32 = 1;
    type Store = S;
    type Input = Registration;
    type Output = RegistrationAnswer;
    async fn run<C: OperationContext<Store = S>>(
        &self,
        ctx: &mut C,
        request: &Registration,
    ) -> Result<RegistrationAnswer, OperationFailure> {
        register::<C, D, H>(
            ctx,
            request,
            &self.config,
            std::sync::Arc::clone(&self.hook),
        )
        .await
        .map_err(Into::into)
    }
    fn on_failed<'s, 't>(
        &'s self,
        scope: &'s mut S::Scope<'t>,
        key: &'s RequestKey,
        failure: &'s OperationFailure,
    ) -> BoxFuture<'s, Result<(), StoreError>> {
        Box::pin(fail_lifecycle(scope, key, failure, self.clock.now()))
    }
}
