use crate::{RegistryConfig, operation_support::fail_lifecycle, registration_rules::register};
use durable_steps::{
    AtomicStore, BoxFuture, OperationContext, OperationFailure, RequestKey, StoreError, Task,
    storage::TransactionScope,
};
use fleet_sync_protocol::{Registration, RegistrationAnswer};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::marker::PhantomData;

/// Register this task on the registry engine and admit it with lifecycle_request_key.
pub struct RegistrationTask<S, D> {
    config: RegistryConfig,
    store: PhantomData<fn() -> (S, D)>,
}
impl<S, D> RegistrationTask<S, D> {
    pub const fn new(config: RegistryConfig) -> Self {
        Self {
            config,
            store: PhantomData,
        }
    }
}
impl<S, D> Task for RegistrationTask<S, D>
where
    S: AtomicStore,
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
        register::<C, D>(ctx, request, &self.config)
            .await
            .map_err(Into::into)
    }
    fn on_failed<'s, 't>(
        &'s self,
        scope: &'s mut S::Scope<'t>,
        key: &'s RequestKey,
        failure: &'s OperationFailure,
    ) -> BoxFuture<'s, Result<(), StoreError>> {
        Box::pin(fail_lifecycle(scope, key, failure))
    }
}
