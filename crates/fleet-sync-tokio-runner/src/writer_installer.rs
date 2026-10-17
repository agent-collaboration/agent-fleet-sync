use crate::{
    CallEnvelope, InstallCoordinator, InstallFile, InstallFileRequest, InstallFileResult,
    InstallHandle, InstallRecord, InstallRefusal, InstallRequest, InstallSnapshot, InstallSource,
    ObjectKey, ObjectOptions, ObjectStore, PublishError, SessionRequests, SnapshotTarget,
    StagingError, StagingStore, TokioHost, TokioObject, WriterHost, WriterInstallTarget,
    WriterSessionCalls,
    writer_session_calls::{decode, encode, unavailable},
};
use durable_steps::{CallFailure, CallTarget, Engine, NotSentReason, StoreError, Uuid};
use fleet_sync_protocol::*;
use fleet_sync_receiver::SnapshotInstallError;
use fleet_sync_writer::{BootstrapError, OpenError, ReplicatedDatabase, ReplicatedSchema};
use runtime_primitives::SystemClock;
use std::{path::Path, sync::Arc};

type WriterInstallEngine<S, P> = Engine<TokioObject<WriterHost<S, P>>, WriterHost<S, P>>;

pub(crate) struct WriterInstaller<S: ReplicatedSchema, P: ConsumerProtocol> {
    pub objects: ObjectStore<WriterHost<S, P>>,
    pub control: Arc<TokioObject<WriterHost<S, P>>>,
    pub host: Arc<WriterHost<S, P>>,
    pub engine: Arc<WriterInstallEngine<S, P>>,
    coordinator: Arc<InstallCoordinator<WriterHost<S, P>>>,
    target: WriterInstallTarget<S>,
    receiver: ReceiverId,
    requests: SessionRequests<P>,
}
impl<S: ReplicatedSchema, P: ConsumerProtocol> WriterInstaller<S, P> {
    pub async fn open(
        directory: &Path,
        database: Arc<ReplicatedDatabase<S>>,
        receiver: ReceiverId,
        calls: Arc<WriterSessionCalls<S, P>>,
    ) -> Result<Arc<Self>, StoreError> {
        let objects = ObjectStore::open(directory.join("control")).await?;
        // Each durable store owns its timer; sharing the call router does not
        // make two runners compete to consume the same wake.
        let host = Arc::new(TokioHost::new(calls.clone(), SystemClock));
        let control = objects
            .object(
                ObjectKey::new("installs").map_err(StoreError::backend)?,
                ObjectOptions::default(),
                host.clone(),
            )
            .await?;
        let staging = Arc::new(
            StagingStore::open(directory.join("staging"))
                .await
                .map_err(StoreError::backend)?,
        );
        let coordinator = Arc::new(InstallCoordinator::open(control.clone(), staging).await?);
        let key = ObjectKey::new(format!("writer-{}", database.database().as_uuid()))
            .map_err(StoreError::backend)?;
        let target = WriterInstallTarget::new(key, database);
        let mut engine = Engine::new(control.clone(), host.clone());
        engine.register(InstallSnapshot::new(host.clone()))?;
        Ok(Arc::new(Self {
            objects,
            control,
            host,
            engine: Arc::new(engine),
            coordinator,
            target,
            receiver,
            requests: calls.requests(),
        }))
    }
    pub async fn start(
        &self,
        attempt: Uuid,
    ) -> Result<InstallHandle<WriterHost<S, P>>, durable_steps::StartRefusal> {
        self.coordinator
            .start(
                InstallRequest {
                    attempt,
                    target: self.target.key().clone(),
                    database: self.target.database().database(),
                    source: self.receiver,
                },
                &self.target,
            )
            .await
    }
    pub async fn wait(&self, attempt: Uuid) -> Result<InstallRecord, StoreError> {
        self.coordinator.wait_for_attempt(attempt).await
    }
    pub async fn record(&self, id: Uuid) -> Result<InstallRecord, StoreError> {
        self.coordinator.record(id).await
    }
    async fn replicate(
        &self,
        request: ReplicationRequest<P::Annotation>,
    ) -> Result<ReplicationAnswer, CallFailure> {
        match self
            .requests
            .request(OperationRequest::Replication(request))
            .await?
        {
            OperationAnswer::Replication(answer) => Ok(answer),
            _ => Err(CallFailure::AnswerLost),
        }
    }
    pub async fn dispatch(&self, call: CallEnvelope) -> Result<serde_json::Value, CallFailure> {
        match call.kind {
            InstallSource::KIND => {
                let target: InstallSource = decode(call.target)?;
                if target.receiver != self.receiver
                    || target.database != self.target.database().database()
                {
                    return Err(unavailable());
                }
                let answer = self
                    .replicate(ReplicationRequest::CopyStatus {
                        database: target.database,
                    })
                    .await?;
                let answer: Result<CopyStatus, InstallRefusal> = match answer {
                    ReplicationAnswer::CopyStatus(CopyStatusAnswer::Status(status)) => Ok(status),
                    ReplicationAnswer::CopyStatus(CopyStatusAnswer::NotInitialized) => {
                        Err(refusal(
                            "source_not_initialized",
                            "snapshot source is not initialized",
                        ))
                    }
                    ReplicationAnswer::CopyStatus(CopyStatusAnswer::Retired) => {
                        Err(refusal("source_retired", "snapshot source is retired"))
                    }
                    _ => return Err(CallFailure::AnswerLost),
                };
                encode(answer)
            }
            InstallFile::KIND => {
                let target: InstallFile = decode(call.target)?;
                let request: InstallFileRequest = decode(call.request)?;
                encode(self.file(target, request).await?)
            }
            _ => Err(unavailable()),
        }
    }
    async fn file(
        &self,
        target: InstallFile,
        request: InstallFileRequest,
    ) -> Result<Result<InstallFileResult, InstallRefusal>, CallFailure> {
        let record = self
            .coordinator
            .record(target.install_id)
            .await
            .map_err(|_| CallFailure::AnswerLost)?;
        if record.request.target != *self.target.key()
            || record.request.source != self.receiver
            || record.request.database != self.target.database().database()
        {
            return Err(unavailable());
        }
        let database = record.request.database;
        let result = match request {
            InstallFileRequest::Prepare { source } => {
                self.coordinator.prepare(target.install_id, &source).await
            }
            InstallFileRequest::Page { page, cursor } => {
                let fetching = cursor.clone();
                self.coordinator
                    .fetch_page(target.install_id, page, cursor.as_ref(), || async move {
                        match self
                            .replicate(ReplicationRequest::SnapshotPage {
                                database,
                                cursor: fetching,
                            })
                            .await
                            .map_err(|failure| StagingError::SourceCall { failure })?
                        {
                            ReplicationAnswer::SnapshotPage(answer) => Ok(answer),
                            _ => Err(StagingError::SourceCall {
                                failure: CallFailure::AnswerLost,
                            }),
                        }
                    })
                    .await
            }
            InstallFileRequest::Verify => {
                let Some(position) = record.status.snapshot().map(|snapshot| snapshot.position)
                else {
                    return Ok(Err(refusal("not_pinned", "install has no pinned snapshot")));
                };
                self.coordinator
                    .verify_from(target.install_id, || async move {
                        match self
                            .replicate(ReplicationRequest::SnapshotManifest { database, position })
                            .await
                            .map_err(|failure| StagingError::SourceCall { failure })?
                        {
                            ReplicationAnswer::SnapshotManifest(answer) => Ok(answer),
                            _ => Err(StagingError::SourceCall {
                                failure: CallFailure::AnswerLost,
                            }),
                        }
                    })
                    .await
            }
            InstallFileRequest::Publish => self
                .coordinator
                .publish(target.install_id, &self.target)
                .await
                .map(InstallFileResult::Outcome),
            InstallFileRequest::Ready => self
                .coordinator
                .mark_writer_ready(target.install_id, &self.target)
                .await
                .map(InstallFileResult::Outcome),
        };
        match result {
            Ok(answer) => Ok(Ok(answer)),
            Err(StagingError::SourceCall { failure }) => Err(failure),
            Err(StagingError::Bootstrap(BootstrapError::Publish(PublishError::ActiveReaders))) => {
                Err(CallFailure::NotSent {
                    reason: NotSentReason::Overloaded,
                })
            }
            Err(
                StagingError::Storage(_)
                | StagingError::Io { .. }
                | StagingError::Rules(SnapshotInstallError::Storage(_))
                | StagingError::Publish(
                    PublishError::Storage(_)
                    | PublishError::Io { .. }
                    | PublishError::CheckpointBusy { .. },
                )
                | StagingError::Bootstrap(
                    BootstrapError::Writer(OpenError::Storage { .. })
                    | BootstrapError::Snapshot(SnapshotInstallError::Storage(_))
                    | BootstrapError::Publish(
                        PublishError::Storage(_)
                        | PublishError::Io { .. }
                        | PublishError::CheckpointBusy { .. },
                    ),
                ),
            ) => Err(CallFailure::AnswerLost),
            Err(error) => Ok(Err(refusal("writer_snapshot_refused", error))),
        }
    }
}
fn refusal(code: &str, detail: impl std::fmt::Display) -> InstallRefusal {
    InstallRefusal {
        code: code.into(),
        detail: detail.to_string(),
    }
}
