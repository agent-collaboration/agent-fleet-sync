use crate::{RegistryConfig, import_rules::import, operation_support::fail_lifecycle};
use durable_steps::{
    AtomicStore, BoxFuture, OperationContext, OperationFailure, RequestKey, StoreError, Task,
    storage::TransactionScope,
};
use fleet_sync_protocol::{Import, ImportAnswer};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::marker::PhantomData;

pub struct ImportTask<S, D, H> {
    config: RegistryConfig,
    hook: std::sync::Arc<H>,
    store: PhantomData<fn() -> (S, D)>,
}
impl<S, D, H> ImportTask<S, D, H> {
    pub const fn new(config: RegistryConfig, hook: std::sync::Arc<H>) -> Self {
        Self {
            config,
            hook,
            store: PhantomData,
        }
    }
}
impl<S, D, H> Task for ImportTask<S, D, H>
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
    const KIND: &'static str = "fleet-sync.import";
    const VERSION: u32 = 1;
    type Store = S;
    type Input = Import;
    type Output = ImportAnswer;
    async fn run<C: OperationContext<Store = S>>(
        &self,
        ctx: &mut C,
        request: &Import,
    ) -> Result<ImportAnswer, OperationFailure> {
        import::<C, D, H>(
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
        Box::pin(fail_lifecycle(scope, key, failure))
    }
}
