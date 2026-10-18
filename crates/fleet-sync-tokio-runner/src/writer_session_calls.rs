use crate::{CallEnvelope, CallRouter, SessionRequests};
use durable_steps::{CallFailure, CallTarget, Uuid};
use fleet_sync_protocol::*;
use fleet_sync_writer::{
    OpenError, RegistryRegistration, RegistryRenewal, ReplicatedDatabase, ReplicatedSchema,
    ReplicationTransport, SeedCopyRefusal, SeedFile, SeedFileAnswer, SeedFileRequest, SendFailure,
    WriterSnapshot,
};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock, Weak},
};
use tokio::sync::Mutex;

enum SeedStart {
    AwaitingBegin,
    Started(SeedId),
}
struct SeedSession {
    generation: Uuid,
    start: SeedStart,
    snapshot: WriterSnapshot,
    page: u32,
    receipts: BTreeMap<u32, (Option<SnapshotCursor>, SeedFileAnswer)>,
    table: Option<(String, TableDigestBuilder, u64)>,
}
pub struct WriterSessionCalls<S: ReplicatedSchema, P: ConsumerProtocol> {
    database: Arc<ReplicatedDatabase<S>>,
    receiver: ReceiverId,
    requests: SessionRequests<P>,
    seed: Mutex<Option<SeedSession>>,
    installer: OnceLock<Weak<crate::writer_installer::WriterInstaller<S, P>>>,
    #[cfg(feature = "test-support")]
    probe: crate::seed_answer_probe::SeedAnswerProbe,
    #[cfg(feature = "test-support")]
    pub(crate) transport_probe: crate::writer_transport_probe::WriterTransportProbe,
}
impl<S: ReplicatedSchema, P: ConsumerProtocol> WriterSessionCalls<S, P> {
    pub fn new(
        database: Arc<ReplicatedDatabase<S>>,
        receiver: ReceiverId,
        requests: SessionRequests<P>,
    ) -> Self {
        Self {
            database,
            receiver,
            requests,
            seed: Mutex::new(None),
            installer: OnceLock::new(),
            #[cfg(feature = "test-support")]
            probe: Default::default(),
            #[cfg(feature = "test-support")]
            transport_probe: Default::default(),
        }
    }
    pub(crate) fn requests(&self) -> SessionRequests<P> {
        self.requests.clone()
    }
    pub(crate) fn attach_installer(
        &self,
        installer: &Arc<crate::writer_installer::WriterInstaller<S, P>>,
    ) -> Result<(), durable_steps::StoreError> {
        self.installer.set(Arc::downgrade(installer)).map_err(|_| {
            durable_steps::StoreError::Aborted {
                reason: "writer install owner is already attached".into(),
            }
        })
    }
    pub(crate) async fn answer_release(
        &self,
        frame: FrameId,
        answer: ReleaseAnswer,
    ) -> Result<(), fleet_sync_session::ClientError> {
        self.requests
            .answer(
                frame,
                ClientAnswer::Crate(ServerCallAnswer::Release(answer)),
            )
            .await
    }
    pub(crate) async fn answer_withdrawal(
        &self,
        frame: FrameId,
        answer: WithdrawAnswer,
    ) -> Result<(), fleet_sync_session::ClientError> {
        self.requests
            .answer(
                frame,
                ClientAnswer::Crate(ServerCallAnswer::WithdrawRelease(answer)),
            )
            .await
    }
    pub async fn close_seed(&self) {
        self.seed.lock().await.take();
    }
    #[cfg(feature = "test-support")]
    pub fn hold_release_answer(&self) {
        self.transport_probe.arm(1);
    }
    #[cfg(feature = "test-support")]
    pub fn hold_withdrawal_answer(&self) {
        self.transport_probe.arm(2);
    }
    #[cfg(feature = "test-support")]
    pub fn hold_piece(&self) {
        self.transport_probe.arm(3);
    }
    #[cfg(feature = "test-support")]
    pub async fn wait_transport_call(
        &self,
    ) -> Result<crate::WriterTransportObservation, durable_steps::StoreError> {
        self.transport_probe.wait().await
    }
    #[cfg(feature = "test-support")]
    pub fn release_transport_call(&self) {
        self.transport_probe.release();
    }
    async fn close_seed_generation(&self, generation: Uuid) {
        let mut owner = self.seed.lock().await;
        if owner
            .as_ref()
            .is_some_and(|current| current.generation == generation)
        {
            owner.take();
        }
    }
    #[cfg(feature = "test-support")]
    pub fn hold_next_seed_page_answer(&self) {
        self.probe.arm();
    }
    #[cfg(feature = "test-support")]
    pub fn hold_seed_page_answer(&self, page: u32) {
        self.probe.arm_page(page);
    }
    #[cfg(feature = "test-support")]
    pub async fn wait_seed_page(
        &self,
    ) -> Result<crate::SeedPageObservation, durable_steps::StoreError> {
        self.probe.wait().await
    }
    #[cfg(feature = "test-support")]
    pub fn release_seed_page_answer(&self) {
        self.probe.release();
    }
    #[cfg(feature = "test-support")]
    pub async fn wait_periodic_renewal(&self) -> Result<(), durable_steps::StoreError> {
        self.probe.wait_periodic_renewal().await
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
            OperationAnswer::Replication(ReplicationAnswer::OutcomeUnknown { .. }) => {
                Err(CallFailure::AnswerLost)
            }
            OperationAnswer::Replication(answer) => Ok(answer),
            _ => Err(CallFailure::AnswerLost),
        }
    }
    async fn seed_file(&self, request: SeedFileRequest) -> Result<SeedFileAnswer, SeedHostError> {
        match request {
            SeedFileRequest::Open {
                generation,
                position,
            } => {
                let mut owner = self.seed.lock().await;
                if owner
                    .as_ref()
                    .is_none_or(|current| current.generation != generation)
                {
                    let snapshot = self.database.snapshot().await?;
                    if snapshot.position() != position {
                        return Err(refused(
                            "seed_position_moved",
                            "writer changed after its registration seed was pinned",
                        ));
                    }
                    *owner = Some(SeedSession {
                        generation,
                        start: SeedStart::AwaitingBegin,
                        snapshot,
                        page: 0,
                        receipts: BTreeMap::new(),
                        table: None,
                    });
                }
                let current = owner
                    .as_mut()
                    .ok_or_else(|| refused("seed_owner", "pinned read disappeared"))?;
                if let SeedStart::Started(seed) = current.start {
                    return Ok(SeedFileAnswer::Opened {
                        seed,
                        database: self.database.database(),
                        position: current.snapshot.position(),
                        schema: current.snapshot.schema().clone(),
                    });
                }
                let seed = match self
                    .replicate(ReplicationRequest::SeedBegin {
                        database: self.database.database(),
                        attempt: SeedAttemptId::new(generation)
                            .map_err(|error| refused("seed_attempt", error))?,
                    })
                    .await?
                {
                    ReplicationAnswer::SeedStarted { seed } => seed,
                    ReplicationAnswer::CopyUnavailable { reason } => {
                        return Err(SeedHostError::Call(fleet_sync_receiver::retryable_copy(
                            reason,
                        )));
                    }
                    ReplicationAnswer::Refused(reason) => {
                        return Err(refused("seed_begin_refused", format!("{reason:?}")));
                    }
                    _ => return Err(SeedHostError::Call(CallFailure::AnswerLost)),
                };
                current.start = SeedStart::Started(seed);
                let answer = SeedFileAnswer::Opened {
                    seed,
                    database: self.database.database(),
                    position,
                    schema: current.snapshot.schema().clone(),
                };
                Ok(answer)
            }
            SeedFileRequest::Page {
                generation,
                seed,
                page,
                cursor,
            } => {
                let mut owner = self.seed.lock().await;
                let Some(current) = owner.as_mut() else {
                    return Ok(SeedFileAnswer::SourceLost);
                };
                if current.generation != generation
                    || !matches!(current.start, SeedStart::Started(current_seed) if current_seed == seed)
                {
                    return Err(refused(
                        "seed_superseded",
                        "another seed owns the pinned read",
                    ));
                }
                if let Some((saved_cursor, answer)) = current.receipts.get(&page) {
                    if *saved_cursor != cursor {
                        return Err(refused("seed_page_conflict", "page cursor changed"));
                    }
                    return Ok(answer.clone());
                }
                if current.page != page {
                    return Err(refused(
                        "seed_page_gap",
                        "page is not the next page of the pinned read",
                    ));
                }
                let contents = current.snapshot.page(cursor.clone()).await?;
                let digest =
                    PageDigest::of(&contents).map_err(|error| refused("seed_encoding", error))?;
                let sent = SeedPage {
                    seed,
                    page,
                    contents: contents.clone(),
                    digest: digest.clone(),
                };
                match self
                    .replicate(ReplicationRequest::SeedPage(Box::new(sent)))
                    .await?
                {
                    ReplicationAnswer::SeedPage(
                        SeedPageAnswer::Staged | SeedPageAnswer::AlreadyStaged,
                    ) => {}
                    ReplicationAnswer::CopyUnavailable { reason } => {
                        return Err(SeedHostError::Call(fleet_sync_receiver::retryable_copy(
                            reason,
                        )));
                    }
                    ReplicationAnswer::SeedPage(reason) => {
                        return Err(refused("seed_page_refused", format!("{reason:?}")));
                    }
                    ReplicationAnswer::Refused(reason) => {
                        return Err(refused("seed_page_refused", format!("{reason:?}")));
                    }
                    _ => return Err(SeedHostError::Call(CallFailure::AnswerLost)),
                }
                let completed = if contents.table.is_empty() {
                    None
                } else {
                    let (name, builder, count) = current.table.get_or_insert_with(|| {
                        (contents.table.clone(), TableDigestBuilder::new(), 0)
                    });
                    if *name != contents.table {
                        return Err(refused(
                            "seed_table_order",
                            "pinned page changed table before its digest ended",
                        ));
                    }
                    for row in &contents.rows {
                        builder
                            .push_row(row)
                            .map_err(|error| refused("seed_digest", error))?;
                    }
                    *count = count
                        .checked_add(
                            u64::try_from(contents.rows.len())
                                .map_err(|error| refused("seed_rows", error))?,
                        )
                        .ok_or_else(|| refused("seed_rows", "row count overflow"))?;
                    if contents
                        .next
                        .as_ref()
                        .is_none_or(|next| next.table != contents.table)
                    {
                        let (name, builder, count) = current
                            .table
                            .take()
                            .ok_or_else(|| refused("seed_digest", "table digest disappeared"))?;
                        Some(TableManifest {
                            table: TableName::new(name)
                                .map_err(|error| refused("seed_table", error))?,
                            row_count: count,
                            digest: builder.finish(),
                        })
                    } else {
                        None
                    }
                };
                let answer = SeedFileAnswer::Page {
                    digest,
                    rows: u64::try_from(contents.rows.len())
                        .map_err(|error| refused("seed_rows", error))?,
                    next: contents.next,
                    completed,
                };
                current.page = current
                    .page
                    .checked_add(1)
                    .ok_or_else(|| refused("seed_pages", "page index overflow"))?;
                current.receipts.insert(page, (cursor, answer.clone()));
                Ok(answer)
            }
            SeedFileRequest::Complete {
                generation,
                request,
            } => {
                if request.database != self.database.database() {
                    return Err(refused(
                        "seed_identity",
                        "completion belongs to another database",
                    ));
                }
                let position = request.position;
                // Completion contains only the digests from journaled page
                // receipts. It can replay the receiver's terminal receipt even
                // after the writer process lost its now-finished read transaction.
                match self
                    .replicate(ReplicationRequest::SeedComplete(request))
                    .await?
                {
                    ReplicationAnswer::SeedComplete(SeedAnswer::Published {
                        position: published,
                    }) if published == position => {
                        let mut owner = self.seed.lock().await;
                        if owner
                            .as_ref()
                            .is_some_and(|current| current.generation == generation)
                        {
                            owner.take();
                        }
                        Ok(SeedFileAnswer::Published { position })
                    }
                    ReplicationAnswer::CopyUnavailable { reason } => Err(SeedHostError::Call(
                        fleet_sync_receiver::retryable_copy(reason),
                    )),
                    ReplicationAnswer::SeedComplete(reason) => {
                        Err(refused("seed_complete_refused", format!("{reason:?}")))
                    }
                    ReplicationAnswer::Refused(reason) => {
                        Err(refused("seed_complete_refused", format!("{reason:?}")))
                    }
                    _ => Err(SeedHostError::Call(CallFailure::AnswerLost)),
                }
            }
        }
    }
}
impl<S: ReplicatedSchema, P: ConsumerProtocol> CallRouter for WriterSessionCalls<S, P> {
    async fn dispatch(&self, call: CallEnvelope) -> Result<serde_json::Value, CallFailure> {
        match call.kind {
            crate::InstallSource::KIND | crate::InstallFile::KIND => {
                let installer = self
                    .installer
                    .get()
                    .and_then(Weak::upgrade)
                    .ok_or_else(unavailable)?;
                installer.dispatch(call).await
            }
            RegistryRegistration::KIND => {
                let target: RegistryRegistration = decode(call.target)?;
                if target.receiver != self.receiver {
                    return Err(unavailable());
                }
                let request: Registration = decode(call.request)?;
                if request.database != self.database.database() {
                    return Err(unavailable());
                }
                match self
                    .requests
                    .request(OperationRequest::Lease(LeaseRequest::Register(request)))
                    .await?
                {
                    OperationAnswer::Lease(LeaseAnswer::Register(answer)) => encode(answer),
                    _ => Err(CallFailure::AnswerLost),
                }
            }
            RegistryRenewal::KIND => {
                let target: RegistryRenewal = decode(call.target)?;
                if target.receiver != self.receiver {
                    return Err(unavailable());
                }
                let request: RenewRequest = decode(call.request)?;
                if request.database != self.database.database() {
                    return Err(unavailable());
                }
                match self
                    .requests
                    .request(OperationRequest::Lease(LeaseRequest::Renew(request)))
                    .await?
                {
                    OperationAnswer::Lease(LeaseAnswer::Renew(answer)) => {
                        #[cfg(feature = "test-support")]
                        if target.cause == fleet_sync_writer::RenewalCause::Periodic {
                            self.probe.periodic_renewed();
                        }
                        encode(answer)
                    }
                    _ => Err(CallFailure::AnswerLost),
                }
            }
            SeedFile::KIND => {
                let target: SeedFile = decode(call.target)?;
                if target.receiver != self.receiver {
                    return Err(unavailable());
                }
                let request: SeedFileRequest = decode(call.request)?;
                let generation = match &request {
                    SeedFileRequest::Open { generation, .. }
                    | SeedFileRequest::Page { generation, .. }
                    | SeedFileRequest::Complete { generation, .. } => *generation,
                };
                #[cfg(feature = "test-support")]
                let page = match &request {
                    SeedFileRequest::Page {
                        generation,
                        seed,
                        page,
                        ..
                    } => Some((*generation, *seed, *page)),
                    _ => None,
                };
                match self.seed_file(request).await {
                    Ok(answer) => {
                        #[cfg(feature = "test-support")]
                        if let (
                            Some((generation, seed, page)),
                            SeedFileAnswer::Page { digest, rows, .. },
                        ) = (page, &answer)
                        {
                            self.probe
                                .after_commit(crate::SeedPageObservation {
                                    generation,
                                    seed,
                                    page,
                                    digest: digest.clone(),
                                    rows: *rows,
                                })
                                .await;
                        }
                        encode(Ok::<_, SeedCopyRefusal>(answer))
                    }
                    Err(SeedHostError::Call(failure)) => Err(failure),
                    Err(SeedHostError::Refused(reason)) => {
                        self.close_seed_generation(generation).await;
                        encode(Err::<SeedFileAnswer, _>(reason))
                    }
                }
            }
            _ => Err(unavailable()),
        }
    }
}
impl<S: ReplicatedSchema, P: ConsumerProtocol> ReplicationTransport<P>
    for WriterSessionCalls<S, P>
{
    async fn send_piece(
        &self,
        receiver: ReceiverId,
        piece: Piece<P::Annotation>,
    ) -> Result<ReplicationAnswer, SendFailure> {
        if receiver != self.receiver {
            return Err(SendFailure::Unavailable {
                message: "receiver has no attached session".into(),
            });
        }
        #[cfg(feature = "test-support")]
        self.transport_probe
            .at(
                3,
                crate::WriterTransportObservation::SendingPiece {
                    receiver,
                    through: piece.through,
                },
            )
            .await;
        self.replicate(ReplicationRequest::Piece(piece))
            .await
            .map_err(|error| match error {
                CallFailure::AnswerLost => SendFailure::AnswerLost,
                error => SendFailure::Unavailable {
                    message: error.to_string(),
                },
            })
    }
}
enum SeedHostError {
    Call(CallFailure),
    Refused(SeedCopyRefusal),
}
impl From<CallFailure> for SeedHostError {
    fn from(failure: CallFailure) -> Self {
        Self::Call(failure)
    }
}
impl From<OpenError> for SeedHostError {
    fn from(error: OpenError) -> Self {
        match error {
            OpenError::Storage { .. } => Self::Call(CallFailure::AnswerLost),
            error => refused("seed_source_refused", error),
        }
    }
}
fn refused(code: &str, detail: impl std::fmt::Display) -> SeedHostError {
    SeedHostError::Refused(SeedCopyRefusal {
        code: code.into(),
        detail: detail.to_string(),
    })
}
pub(crate) fn decode<T: DeserializeOwned>(value: serde_json::Value) -> Result<T, CallFailure> {
    serde_json::from_value(value).map_err(|_| CallFailure::AnswerLost)
}
pub(crate) fn encode<T: Serialize>(value: T) -> Result<serde_json::Value, CallFailure> {
    serde_json::to_value(value).map_err(|_| CallFailure::AnswerLost)
}
pub(crate) fn unavailable() -> CallFailure {
    CallFailure::NotSent {
        reason: durable_steps::NotSentReason::Unreachable { last_seen: None },
    }
}
