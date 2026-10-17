use crate::{ServiceConsumer, ServiceError, ServiceObject, service_state::ServiceState};
use durable_steps::{AtomicStore, StoreError};
use fleet_sync_protocol::*;
use fleet_sync_receiver::{ApplyDecision, ApplyFailure, ReceiverError, admit_follow_ups};
use fleet_sync_session::PeerAuthenticator;
use runtime_primitives::Clock;

enum CopyRuleError {
    Storage(StoreError),
    Apply(ApplyFailure),
    Receiver(ReceiverError),
}
impl From<StoreError> for CopyRuleError {
    fn from(error: StoreError) -> Self {
        Self::Storage(error)
    }
}
impl<P: ConsumerProtocol, C: ServiceConsumer<P>, A: PeerAuthenticator<Material = String>>
    ServiceState<P, C, A>
{
    pub(crate) async fn replicate(
        &self,
        peer: PeerId,
        request: ReplicationRequest<P::Annotation>,
    ) -> Result<ReplicationAnswer, ServiceError> {
        let database = match &request {
            ReplicationRequest::Piece(piece) => piece.database,
            ReplicationRequest::SeedPage(page) => page.contents.database,
            ReplicationRequest::SeedComplete(done) => done.database,
            ReplicationRequest::SeedBegin { database, .. }
            | ReplicationRequest::SnapshotManifest { database, .. }
            | ReplicationRequest::SnapshotPage { database, .. }
            | ReplicationRequest::CopyStatus { database } => *database,
        };
        let copy = self.copy(database).await?;
        let consumer = self.consumer.clone();
        let now = copy.object.host().now();
        let result = copy
            .object
            .rule_transaction(move |scope| {
                Box::pin(async move {
                    Ok(match request {
                        ReplicationRequest::Piece(piece) => match fleet_sync_receiver::apply_piece(
                            scope,
                            &piece,
                            &peer,
                            consumer.as_ref(),
                            now,
                        )
                        .await
                        .map_err(CopyRuleError::Apply)?
                        {
                            ApplyDecision::Applied {
                                through,
                                follow_ups,
                            } => {
                                admit_follow_ups::<ServiceObject, C::HookItem>(
                                    scope,
                                    database,
                                    piece.epoch,
                                    through,
                                    follow_ups,
                                )
                                .await
                                .map_err(|error| {
                                    CopyRuleError::Storage(StoreError::backend(error))
                                })?;
                                ReplicationAnswer::Applied { through }
                            }
                            ApplyDecision::AlreadyApplied { through } => {
                                ReplicationAnswer::AlreadyApplied { through }
                            }
                            ApplyDecision::PartStaged {
                                transaction,
                                index,
                                acked,
                            } => ReplicationAnswer::PartStaged {
                                transaction,
                                index,
                                acked,
                            },
                        },
                        ReplicationRequest::SeedBegin { attempt, .. } => {
                            ReplicationAnswer::SeedStarted {
                                seed: fleet_sync_receiver::seed_begin(scope, database, attempt)
                                    .await
                                    .map_err(CopyRuleError::Receiver)?,
                            }
                        }
                        ReplicationRequest::SeedPage(page) => ReplicationAnswer::SeedPage(
                            fleet_sync_receiver::seed_page(scope, &page)
                                .await
                                .map_err(CopyRuleError::Receiver)?,
                        ),
                        ReplicationRequest::SeedComplete(done) => ReplicationAnswer::SeedComplete(
                            fleet_sync_receiver::seed_complete(scope, &done)
                                .await
                                .map_err(CopyRuleError::Receiver)?,
                        ),
                        ReplicationRequest::SnapshotPage { cursor, .. } => {
                            ReplicationAnswer::SnapshotPage(
                                fleet_sync_receiver::snapshot_page(scope, cursor)
                                    .await
                                    .map_err(CopyRuleError::Receiver)?,
                            )
                        }
                        ReplicationRequest::SnapshotManifest { position, .. } => {
                            ReplicationAnswer::SnapshotManifest(
                                fleet_sync_receiver::snapshot_manifest(scope, position)
                                    .await
                                    .map_err(CopyRuleError::Receiver)?,
                            )
                        }
                        ReplicationRequest::CopyStatus { .. } => ReplicationAnswer::CopyStatus(
                            fleet_sync_receiver::copy_status(scope)
                                .await
                                .map_err(CopyRuleError::Receiver)?,
                        ),
                    })
                })
            })
            .await;
        let answer = match result {
            Ok(answer) => answer,
            Err(CopyRuleError::Apply(ApplyFailure::Refused(reason))) => {
                ReplicationAnswer::Refused(reason)
            }
            Err(CopyRuleError::Apply(ApplyFailure::RefusedDiscardingParts {
                refusal,
                transaction,
            })) => {
                copy.object
                    .transaction(move |scope| {
                        Box::pin(async move {
                            fleet_sync_receiver::discard_staged_parts(scope, &transaction)
                                .await
                                .map_err(StoreError::backend)
                        })
                    })
                    .await?;
                ReplicationAnswer::Refused(refusal)
            }
            Err(CopyRuleError::Receiver(ReceiverError::SeedRefused { answer })) => {
                ReplicationAnswer::SeedComplete(answer)
            }
            Err(CopyRuleError::Receiver(ReceiverError::Retired)) => {
                ReplicationAnswer::Refused(PieceRefusal::Retired)
            }
            Err(CopyRuleError::Receiver(ReceiverError::NotInitialized)) => {
                ReplicationAnswer::Refused(PieceRefusal::NotInitialized)
            }
            Err(CopyRuleError::Storage(error)) => return Err(error.into()),
            Err(CopyRuleError::Apply(error)) => return Err(ServiceError::rule(error)),
            Err(CopyRuleError::Receiver(error)) => return Err(ServiceError::rule(error)),
        };
        copy.changed.send_replace(());
        // New follow-ups were admitted in the apply transaction; wake their
        // actual object owner after the commit, independently from the socket.
        runtime_primitives::WakeScheduler::wake_at(
            copy.object.host().as_ref(),
            copy.object.host().now(),
        )
        .await
        .map_err(ServiceError::rule)?;
        Ok(answer)
    }
}
