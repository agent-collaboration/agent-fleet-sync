use crate::task_support::*;
use crate::*;
use durable_steps::{
    AtomicStore, BoxFuture, OperationContext, OperationFailure, RequestKey, Resume, StoreError,
    Task, storage::TransactionScope,
};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::marker::PhantomData;

pub struct PublishStatus<S, Db, D> {
    store: TaskTypes<S, Db, D>,
}
impl<S, Db, D> PublishStatus<S, Db, D> {
    pub const fn new() -> Self {
        Self { store: PhantomData }
    }
}
impl<S, Db, D> Default for PublishStatus<S, Db, D> {
    fn default() -> Self {
        Self::new()
    }
}
impl<S, Db, D> Task for PublishStatus<S, Db, D>
where
    S: AtomicStore,
    Db: Database,
    D: DeliveryTypes,
    for<'t> S::Scope<'t>: TransactionScope<Database = Db>,
    Db::Arguments: IntoArguments<Db>,
    for<'v> String: Type<Db> + Encode<'v, Db>,
    for<'v> Option<String>: Type<Db> + Decode<'v, Db> + Encode<'v, Db>,
    for<'r> &'r str: ColumnIndex<Db::Row>,
{
    const KIND: &'static str = "fleet-sync.delivery.publish-status";
    const VERSION: u32 = 1;
    const KEEP_RESULT: bool = false;
    type Store = S;
    type Input = PublicationRef<D::StatusRoute>;
    type Output = ();
    async fn run<O: OperationContext<Store = S>>(
        &self,
        ctx: &mut O,
        input: &PublicationRef<D::StatusRoute>,
    ) -> Result<(), OperationFailure> {
        let input = input.clone();
        let read_reference = input.clone();
        let publication = ctx
            .local(step("read_publication")?, move |scope| {
                Box::pin(async move {
                    resolve_publication::<_, D>(scope, read_reference)
                        .await
                        .map_err(step_error)
                })
            })
            .await?;
        let Some(publication) = publication else {
            return Ok(());
        };
        let answer = ctx
            .call(
                step("publish")?,
                PublishCall::<D>::new(input.target.clone()),
                publication,
            )
            .await?;
        if let StatusPublicationResult::Refused { reason } = answer {
            return Err(step_error(DeliveryError::Refused { reason }).into());
        }
        ctx.local(step("retire_publication")?, move |scope| {
            Box::pin(async move {
                retire_publication::<_, D>(scope, input)
                    .await
                    .map_err(step_error)
            })
        })
        .await?;
        Ok(())
    }
    fn before_resume<'s, 't>(
        &'s self,
        scope: &'s mut S::Scope<'t>,
        key: &'s RequestKey,
    ) -> BoxFuture<'s, Result<Resume<()>, StoreError>> {
        Box::pin(async move {
            let reference: PublicationRef<D::StatusRoute> =
                serde_json::from_str(key.as_str()).map_err(StoreError::backend)?;
            if resolve_publication::<_, D>(scope, reference)
                .await
                .map_err(store_error)?
                .is_none()
            {
                Ok(Resume::Finish(()))
            } else {
                Ok(Resume::Continue)
            }
        })
    }
}
