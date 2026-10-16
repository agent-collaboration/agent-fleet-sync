use crate::{RegistryConfig, operation_support::fail_lifecycle, retirement_rules::retire};
use durable_steps::{
    AtomicStore, BoxFuture, OperationContext, OperationFailure, RequestKey, StoreError, Task,
    storage::TransactionScope,
};
use fleet_sync_protocol::{Retirement, RetirementAnswer};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::marker::PhantomData;

pub struct RetirementTask<S, D> {
    config: RegistryConfig,
    store: PhantomData<fn() -> (S, D)>,
}
impl<S, D> RetirementTask<S, D> {
    pub const fn new(config: RegistryConfig) -> Self {
        Self {
            config,
            store: PhantomData,
        }
    }
}
impl<S, D> Task for RetirementTask<S, D>
where
    S: AtomicStore,
    D: Database,
    for<'t> S::Scope<'t>: TransactionScope<Database = D>,
    D::Arguments: IntoArguments<D>,
    for<'value> String: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'value> Option<String>: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'row> &'row str: ColumnIndex<D::Row>,
{
    const KIND: &'static str = "fleet-sync.retire";
    const VERSION: u32 = 1;
    type Store = S;
    type Input = Retirement;
    type Output = RetirementAnswer;
    async fn run<C: OperationContext<Store = S>>(
        &self,
        ctx: &mut C,
        request: &Retirement,
    ) -> Result<RetirementAnswer, OperationFailure> {
        retire::<C, D>(ctx, request, &self.config)
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
