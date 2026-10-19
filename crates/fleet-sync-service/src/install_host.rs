use crate::{
    ServiceConsumer, ServiceError, ServiceHandle, ServiceHost, ServiceObject, call_router::lost,
    service_state::ServiceState,
};
use durable_steps::CallFailure;
use fleet_sync_protocol::*;
use fleet_sync_receiver::{
    SnapshotInstallError, SnapshotInstallRules, SnapshotReady, admit_snapshot_follow_ups,
};
use fleet_sync_session::PeerAuthenticator;
use fleet_sync_tokio_runner::{
    InstallFile, InstallFileRequest, InstallFileResult, InstallHandle, InstallRecord,
    InstallRefusal, InstallRequest, InstallSource, PublishError, StagingError,
};
use runtime_primitives::{Clock, WakeScheduler};

impl<
    P: ConsumerProtocol,
    C: ServiceConsumer<P>,
    A: PeerAuthenticator<
            Material = String,
            Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute,
        >,
> ServiceHandle<P, C, A>
{
    pub async fn install_snapshot(
        &self,
        attempt: InstallAttemptId,
        database: DatabaseId,
        source: ReceiverId,
    ) -> Result<InstallHandle<ServiceHost>, ServiceError> {
        let copy = self.state.copy(database).await?;
        self.state
            .installs
            .start(
                InstallRequest {
                    attempt,
                    database,
                    source,
                    target: copy.object.key().clone(),
                },
                &copy.object,
            )
            .await
            .map_err(ServiceError::rule)
    }
    pub async fn install_record(&self, install: InstallId) -> Result<InstallRecord, ServiceError> {
        Ok(self.state.installs.record(install).await?)
    }
    pub async fn wait_install(
        &self,
        install: InstallAttemptId,
    ) -> Result<InstallRecord, ServiceError> {
        Ok(self.state.installs.wait_for_attempt(install).await?)
    }
    #[cfg(feature = "test-support")]
    pub async fn replay_snapshot_ready(&self, install: InstallId) -> Result<(), ServiceError> {
        match self
            .state
            .install_file(
                InstallFile {
                    install_id: install,
                },
                InstallFileRequest::Ready,
            )
            .await
            .map_err(ServiceError::rule)?
        {
            Ok(InstallFileResult::Outcome(fleet_sync_tokio_runner::InstallFileAnswer::Ready {
                ..
            })) => Ok(()),
            _ => Err(ServiceError::rule(
                "snapshot Ready replay did not return Ready",
            )),
        }
    }
}
impl<
    P: ConsumerProtocol,
    C: ServiceConsumer<P>,
    A: PeerAuthenticator<
            Material = String,
            Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute,
        >,
> ServiceState<P, C, A>
{
    pub(crate) async fn install_source(
        &self,
        target: InstallSource,
    ) -> Result<Result<CopyStatus, InstallRefusal>, CallFailure> {
        let answer = self
            .source_request(
                target.receiver,
                ReplicationRequest::CopyStatus {
                    database: target.database,
                },
            )
            .await?;
        Ok(match answer {
            ReplicationAnswer::CopyStatus(CopyStatusAnswer::Status(status)) => Ok(status),
            ReplicationAnswer::CopyStatus(CopyStatusAnswer::NotInitialized) => Err(refusal(
                "source_not_initialized",
                "snapshot source is not initialized",
            )),
            ReplicationAnswer::CopyStatus(CopyStatusAnswer::Retired) => {
                Err(refusal("source_retired", "snapshot source is retired"))
            }
            _ => return Err(CallFailure::AnswerLost),
        })
    }
    pub(crate) async fn install_file(
        &self,
        target: InstallFile,
        request: InstallFileRequest,
    ) -> Result<Result<InstallFileResult, InstallRefusal>, CallFailure> {
        let record = self
            .installs
            .record(target.install_id)
            .await
            .map_err(lost)?;
        let result = match request {
            InstallFileRequest::Prepare { source } => {
                self.installs.prepare(target.install_id, &source).await
            }
            InstallFileRequest::Page { page, cursor } => {
                let source = record.request.source;
                let database = record.request.database;
                let fetch_cursor = cursor.clone();
                self.installs
                    .fetch_page(target.install_id, page, cursor.as_ref(), || async move {
                        match self
                            .source_request(
                                source,
                                ReplicationRequest::SnapshotPage {
                                    database,
                                    cursor: fetch_cursor,
                                },
                            )
                            .await
                            .map_err(|failure| StagingError::SourceCall { failure })?
                        {
                            ReplicationAnswer::SnapshotPage(page) => Ok(page),
                            _ => Err(StagingError::SourceCall {
                                failure: CallFailure::AnswerLost,
                            }),
                        }
                    })
                    .await
            }
            InstallFileRequest::Verify => {
                let source = record.request.source;
                let database = record.request.database;
                let Some(position) = record.status.snapshot().map(|snapshot| snapshot.position)
                else {
                    return Ok(Err(refusal("not_pinned", "install has no pinned snapshot")));
                };
                self.installs
                    .verify_from(target.install_id, || async move {
                        match self
                            .source_request(
                                source,
                                ReplicationRequest::SnapshotManifest { database, position },
                            )
                            .await
                            .map_err(|failure| StagingError::SourceCall { failure })?
                        {
                            ReplicationAnswer::SnapshotManifest(manifest) => Ok(manifest),
                            _ => Err(StagingError::SourceCall {
                                failure: CallFailure::AnswerLost,
                            }),
                        }
                    })
                    .await
            }
            InstallFileRequest::Publish => {
                let copy = self.copy(record.request.database).await.map_err(lost)?;
                self.installs
                    .publish(target.install_id, &copy.object)
                    .await
                    .map(InstallFileResult::Outcome)
            }
            InstallFileRequest::Ready => {
                let copy = self.copy(record.request.database).await.map_err(lost)?;
                let consumer = self.consumer.clone();
                let answer = self
                    .installs
                    .mark_ready_with(target.install_id, &copy.object, move |scope, snapshot| {
                        Box::pin(async move {
                            if let SnapshotReady::Ready {
                                installed,
                                follow_ups,
                            } = SnapshotInstallRules(scope)
                                .mark_ready(&snapshot, consumer.as_ref())
                                .await?
                            {
                                admit_snapshot_follow_ups::<ServiceObject, C::HookItem>(
                                    scope, &installed, follow_ups,
                                )
                                .await
                                .map_err(durable_steps::StoreError::backend)?;
                            }
                            Ok(())
                        })
                    })
                    .await
                    .map(InstallFileResult::Outcome);
                copy.changed.send_replace(());
                // Ready admits work in the copy file, which has its own timer.
                // Wake even if saving the later control receipt failed after
                // that target transaction committed. A rollback admits nothing.
                copy.object
                    .host()
                    .wake_at(copy.object.host().now())
                    .await
                    .unwrap_or_else(|error| tracing::warn!(%error, "copy Ready wake failed after target transaction"));
                answer
            }
        };
        #[cfg(feature = "test-support")]
        if let Ok(InstallFileResult::Page(fleet_sync_tokio_runner::StagedPage::Receipt(receipt))) =
            &result
        {
            self.install_page_probe.after_commit(receipt.clone()).await;
        }
        match result {
            Ok(answer) => Ok(Ok(answer)),
            Err(StagingError::SourceCall { failure }) => Err(failure),
            Err(
                StagingError::Storage(_)
                | StagingError::Io { .. }
                | StagingError::Publish(
                    PublishError::CheckpointBusy { .. }
                    | PublishError::Storage(_)
                    | PublishError::Io { .. },
                )
                | StagingError::Rules(SnapshotInstallError::Storage(_)),
            ) => Err(CallFailure::AnswerLost),
            Err(error) => Ok(Err(refusal("snapshot_install_refused", error))),
        }
    }
}
fn refusal(code: &str, detail: impl std::fmt::Display) -> InstallRefusal {
    InstallRefusal {
        code: code.into(),
        detail: detail.to_string(),
    }
}
