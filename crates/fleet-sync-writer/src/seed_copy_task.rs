use crate::{
    ReplicatedSchema, SeedCopyInput, SeedFile, SeedFileAnswer, SeedFileRequest, WriterJournalScope,
    WriterJournalStore,
    renewal_task::{renew_inline, step},
};
use durable_steps::{
    BoxFuture, Host, OperationContext, OperationFailure, RequestKey, StoreError, Task,
};
use fleet_sync_protocol::SeedComplete;
use std::{marker::PhantomData, sync::Arc};

pub struct SeedCopy<S, H> {
    host: Arc<H>,
    schema: PhantomData<fn() -> S>,
}
impl<S, H> SeedCopy<S, H> {
    pub fn new(host: Arc<H>) -> Self {
        Self {
            host,
            schema: PhantomData,
        }
    }
}
impl<S: ReplicatedSchema, H: Host> Task for SeedCopy<S, H> {
    const KIND: &'static str = "fleet.writer-seed";
    const VERSION: u32 = 1;
    type Store = WriterJournalStore<S, H>;
    type Input = SeedCopyInput;
    type Output = fleet_sync_protocol::ReplicationPosition;
    async fn run<C: OperationContext<Store = Self::Store>>(
        &self,
        ctx: &mut C,
        input: &SeedCopyInput,
    ) -> Result<Self::Output, OperationFailure> {
        let mut cycle = 0u64;
        loop {
            let host = self.host.clone();
            let generation = ctx
                .local(step(format!("seed-generation:{cycle}"))?, move |_| {
                    Box::pin(async move { Ok(host.new_id()) })
                })
                .await?;
            renew_inline::<S, H, C>(ctx, input.receiver, format!("seed-open-renewal:{cycle}"))
                .await?;
            let opened = ctx
                .call(
                    step(format!("seed-open:{cycle}"))?,
                    SeedFile {
                        receiver: input.receiver,
                    },
                    SeedFileRequest::Open {
                        generation,
                        position: input.position,
                    },
                )
                .await?
                .map_err(OperationFailure::from)?;
            let (seed, database, position, schema) = match opened {
                SeedFileAnswer::Opened {
                    seed,
                    database,
                    position,
                    schema,
                } if position == input.position => (seed, database, position, schema),
                SeedFileAnswer::SourceLost => {
                    cycle = next_cycle(cycle)?;
                    continue;
                }
                _ => return Err(invalid("seed owner returned an unexpected open answer")),
            };
            let mut page = 0u32;
            let mut cursor = None;
            let mut tables = Vec::new();
            let lost = loop {
                renew_inline::<S, H, C>(
                    ctx,
                    input.receiver,
                    format!("seed-page-renewal:{cycle}:{page}"),
                )
                .await?;
                let answer = ctx
                    .call(
                        step(format!("seed-page:{cycle}:{page}"))?,
                        SeedFile {
                            receiver: input.receiver,
                        },
                        SeedFileRequest::Page {
                            generation,
                            seed,
                            page,
                            cursor,
                        },
                    )
                    .await?
                    .map_err(OperationFailure::from)?;
                match answer {
                    SeedFileAnswer::Page {
                        next, completed, ..
                    } => {
                        if let Some(table) = completed {
                            tables.push((table.table, table.digest));
                        }
                        cursor = next;
                        page = page
                            .checked_add(1)
                            .ok_or_else(|| invalid("seed page index exhausted"))?;
                        if cursor.is_none() {
                            break false;
                        }
                    }
                    SeedFileAnswer::SourceLost => break true,
                    _ => return Err(invalid("seed owner returned an unexpected page answer")),
                }
            };
            if lost {
                cycle = next_cycle(cycle)?;
                continue;
            }
            renew_inline::<S, H, C>(
                ctx,
                input.receiver,
                format!("seed-complete-renewal:{cycle}"),
            )
            .await?;
            let answer = ctx
                .call(
                    step(format!("seed-complete:{cycle}"))?,
                    SeedFile {
                        receiver: input.receiver,
                    },
                    SeedFileRequest::Complete {
                        generation,
                        request: SeedComplete {
                            database,
                            seed,
                            position,
                            pages: page,
                            schema,
                            table_digests: tables,
                        },
                    },
                )
                .await?
                .map_err(OperationFailure::from)?;
            match answer {
                SeedFileAnswer::Published {
                    position: published,
                } if published == input.position => return Ok(published),
                SeedFileAnswer::SourceLost => {
                    cycle = next_cycle(cycle)?;
                }
                _ => return Err(invalid("seed owner returned an unexpected complete answer")),
            }
        }
    }
    fn on_failed<'s, 't>(
        &'s self,
        scope: &'s mut WriterJournalScope<'t, H>,
        _: &'s RequestKey,
        failure: &'s OperationFailure,
    ) -> BoxFuture<'s, Result<(), StoreError>> {
        Box::pin(async move {
            scope
                .fail_registration(failure)
                .await
                .map_err(StoreError::backend)
        })
    }
}
fn invalid(message: &str) -> OperationFailure {
    OperationFailure::InvalidPayload {
        message: message.into(),
    }
}
fn next_cycle(cycle: u64) -> Result<u64, OperationFailure> {
    cycle
        .checked_add(1)
        .ok_or_else(|| invalid("seed restart index exhausted"))
}
