use crate::{RegistryConfig, preview_rules::preview_takeover};
use durable_steps::{
    AtomicStore, OperationContext, OperationFailure, Task, storage::TransactionScope,
};
use fleet_sync_protocol::{PreviewTakeover, TakeoverPreview};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::marker::PhantomData;

pub struct PreviewTakeoverTask<S, D> {
    config: RegistryConfig,
    store: PhantomData<fn() -> (S, D)>,
}
impl<S, D> PreviewTakeoverTask<S, D> {
    pub const fn new(config: RegistryConfig) -> Self {
        Self {
            config,
            store: PhantomData,
        }
    }
}
impl<S, D> Task for PreviewTakeoverTask<S, D>
where
    S: AtomicStore,
    D: Database,
    for<'t> S::Scope<'t>: TransactionScope<Database = D>,
    D::Arguments: IntoArguments<D>,
    for<'value> String: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'value> Option<String>: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'row> &'row str: ColumnIndex<D::Row>,
{
    const KIND: &'static str = "fleet-sync.preview-takeover";
    const VERSION: u32 = 1;
    type Store = S;
    type Input = PreviewTakeover;
    type Output = TakeoverPreview;
    async fn run<C: OperationContext<Store = S>>(
        &self,
        ctx: &mut C,
        request: &PreviewTakeover,
    ) -> Result<TakeoverPreview, OperationFailure> {
        preview_takeover::<C, D>(ctx, request, &self.config)
            .await
            .map_err(Into::into)
    }
}
