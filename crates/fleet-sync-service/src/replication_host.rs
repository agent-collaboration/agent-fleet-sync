use crate::{ServiceConsumer, ServiceError, ServiceObject, service_state::ServiceState};
use durable_steps::{AtomicStore, StoreError};
use fleet_sync_protocol::*;
use fleet_sync_receiver::{ApplyDecision, CopyTransactionFailure, admit_follow_ups};
use fleet_sync_session::PeerAuthenticator;
use runtime_primitives::Clock;

impl<
    P: ConsumerProtocol,
    C: ServiceConsumer<P>,
    A: PeerAuthenticator<
            Material = String,
            Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute,
        >,
> ServiceState<P, C, A>
{
    pub(crate) async fn replicate(
        &self,
        peer: PeerId,
        request: ReplicationRequest<P::Annotation>,
    ) -> Result<ReplicationAnswer, ServiceError> {
        self.replicate_with_authority(fleet_sync_receiver::SeedAuthority::Writer(peer), request)
            .await
    }

    pub(crate) async fn replicate_with_authority(
        &self,
        authority: fleet_sync_receiver::SeedAuthority,
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
        let copy = match self.copy(database).await {
            Ok(copy) => copy,
            Err(error) => {
                return Ok(ReplicationAnswer::OutcomeUnknown {
                    reason: OutcomeUnknownReason::StoreBoundary {
                        detail: DiagnosticText::from_error(error),
                    },
                });
            }
        };
        let consumer = self.consumer.clone();
        let now = copy.object.host().now();
        let result = copy
            .object
            .rule_transaction(move |scope| {
                Box::pin(async move {
                    Ok(match request {
                        ReplicationRequest::Piece(piece) => {
                            let fleet_sync_receiver::SeedAuthority::Writer(peer) = &authority
                            else {
                                return Ok(ReplicationAnswer::Refused(PieceRefusal::NotWriter {
                                    current: None,
                                }));
                            };
                            match fleet_sync_receiver::apply_piece(
                                scope,
                                &piece,
                                peer,
                                consumer.as_ref(),
                                now,
                            )
                            .await
                            .map_err(fleet_sync_receiver::apply_rollback)?
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
                                    .map_err(StoreError::backend)?;
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
                            }
                        }
                        ReplicationRequest::SeedBegin { attempt, .. } => {
                            ReplicationAnswer::SeedStarted {
                                seed: fleet_sync_receiver::seed_begin(
                                    scope, database, attempt, &authority,
                                )
                                .await
                                .map_err(fleet_sync_receiver::receiver_rollback)?,
                            }
                        }
                        ReplicationRequest::SeedPage(page) => ReplicationAnswer::SeedPage(
                            fleet_sync_receiver::seed_page(scope, &page, &authority)
                                .await
                                .map_err(fleet_sync_receiver::receiver_rollback)?,
                        ),
                        ReplicationRequest::SeedComplete(done) => {
                            match fleet_sync_receiver::seed_complete(
                                scope,
                                &done,
                                consumer.as_ref(),
                                &authority,
                            )
                            .await
                            .map_err(fleet_sync_receiver::receiver_rollback)?
                            {
                                fleet_sync_receiver::SeedDecision::Published {
                                    position,
                                    follow_ups,
                                } => {
                                    let installed = fleet_sync_receiver::InstalledSnapshot {
                                        database: done.database,
                                        position,
                                        schema: done.schema.clone(),
                                    };
                                    fleet_sync_receiver::admit_snapshot_follow_ups::<
                                        ServiceObject,
                                        C::HookItem,
                                    >(
                                        scope, &installed, follow_ups
                                    )
                                    .await
                                    .map_err(StoreError::backend)?;
                                    ReplicationAnswer::SeedComplete(SeedAnswer::Published {
                                        position,
                                    })
                                }
                                fleet_sync_receiver::SeedDecision::Answer(answer) => {
                                    ReplicationAnswer::SeedComplete(answer)
                                }
                            }
                        }
                        ReplicationRequest::SnapshotPage { cursor, .. } => {
                            ReplicationAnswer::SnapshotPage(
                                fleet_sync_receiver::snapshot_page(scope, cursor)
                                    .await
                                    .map_err(fleet_sync_receiver::receiver_rollback)?,
                            )
                        }
                        ReplicationRequest::SnapshotManifest { position, .. } => {
                            ReplicationAnswer::SnapshotManifest(
                                fleet_sync_receiver::snapshot_manifest(scope, position)
                                    .await
                                    .map_err(fleet_sync_receiver::receiver_rollback)?,
                            )
                        }
                        ReplicationRequest::CopyStatus { .. } => ReplicationAnswer::CopyStatus(
                            fleet_sync_receiver::copy_status(scope)
                                .await
                                .map_err(fleet_sync_receiver::receiver_rollback)?,
                        ),
                    })
                })
            })
            .await;
        // Callback errors arrive only after successful rollback. A failed
        // storage boundary replaces them with Backend, losing refusal certainty.
        let answer = match result {
            Ok(answer) => answer,
            Err(error) => match fleet_sync_receiver::classify_copy_store_error(error) {
                CopyTransactionFailure::RefusedDiscardingParts {
                    refusal,
                    transaction,
                } => {
                    if let Err(error) = copy
                        .object
                        .transaction(move |scope| {
                            Box::pin(async move {
                                fleet_sync_receiver::discard_staged_parts(scope, &transaction)
                                    .await
                                    .map_err(StoreError::backend)
                            })
                        })
                        .await
                    {
                        return Ok(fleet_sync_receiver::classify_copy_store_error(error).answer());
                    }
                    ReplicationAnswer::Refused(refusal)
                }
                failure => failure.answer(),
            },
        };
        let answer = match answer {
            ReplicationAnswer::CopyStatus(CopyStatusAnswer::NotInitialized) => {
                ReplicationAnswer::CopyUnavailable {
                    reason: CopyUnavailableReason::Uninitialized,
                }
            }
            answer => answer,
        };
        copy.changed.send_replace(());
        // New follow-ups were admitted in the apply transaction; wake their
        // actual object owner after the commit, independently from the socket.
        #[cfg(not(feature = "test-fault"))]
        let _ = runtime_primitives::WakeScheduler::wake_at(
            copy.object.host().as_ref(),
            copy.object.host().now(),
        )
        .await;
        #[cfg(feature = "test-fault")]
        let _ =
            runtime_primitives::WakeScheduler::wake_at(&copy.apply_wake, copy.object.host().now())
                .await;
        // A timer hint cannot undo the committed answer. The admitted work is
        // durable and the next wake or startup drives the same identity.
        Ok(answer)
    }
}
