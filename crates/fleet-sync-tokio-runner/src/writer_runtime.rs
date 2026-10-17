use crate::{SessionRequests, TokioHost, TokioRunner, WriterSessionCalls};
use durable_steps::{Engine, OperationId, StoreError};
use fleet_sync_protocol::{ConsumerProtocol, ReceiverId};
use fleet_sync_writer::{
    OpenError, RegisterDatabase, RenewLease, ReplicatedDatabase, ReplicatedSchema, SeedCopy,
    WriterJournalStore, WriterRegistrationOutcome,
};
use runtime_primitives::SystemClock;
use std::sync::Arc;
use tokio::sync::watch;

pub type WriterHost<S, P> = TokioHost<WriterSessionCalls<S, P>>;
pub type NativeWriterStore<S, P> = WriterJournalStore<S, WriterHost<S, P>>;
/// The consumer owns this run future and joins it before closing the writer.
/// Session ownership stays with the attached source pump; this owner only uses
/// its typed request handle for registration, seeding, renewals and pieces.
pub struct NativeWriterRuntime<S: ReplicatedSchema, P: ConsumerProtocol> {
    database: Arc<ReplicatedDatabase<S>>,
    receiver: ReceiverId,
    calls: Arc<WriterSessionCalls<S, P>>,
    host: Arc<WriterHost<S, P>>,
    store: Arc<NativeWriterStore<S, P>>,
    engine: Arc<Engine<NativeWriterStore<S, P>, WriterHost<S, P>>>,
    installer: Option<Arc<crate::writer_installer::WriterInstaller<S, P>>>,
}
impl<S: ReplicatedSchema, P: ConsumerProtocol> NativeWriterRuntime<S, P> {
    pub async fn new(
        database: Arc<ReplicatedDatabase<S>>,
        receiver: ReceiverId,
        requests: SessionRequests<P>,
    ) -> Result<Self, StoreError> {
        let calls = Arc::new(WriterSessionCalls::new(
            database.clone(),
            receiver,
            requests,
        ));
        let host = Arc::new(TokioHost::new(calls.clone(), SystemClock));
        let store = Arc::new(
            database
                .journal_store(host.clone())
                .await
                .map_err(StoreError::backend)?,
        );
        let mut engine = Engine::new(store.clone(), host.clone());
        engine.register(RegisterDatabase::new(host.clone()))?;
        engine.register(SeedCopy::new(host.clone()))?;
        engine.register(RenewLease::new())?;
        Ok(Self {
            database,
            receiver,
            calls,
            host,
            store,
            engine: Arc::new(engine),
            installer: None,
        })
    }
    pub async fn enable_snapshot_installs(
        &mut self,
        directory: &std::path::Path,
    ) -> Result<(), StoreError> {
        if self.installer.is_some() {
            return Err(StoreError::Aborted {
                reason: "writer install owner is already configured".into(),
            });
        }
        let installer = crate::writer_installer::WriterInstaller::open(
            directory,
            self.database.clone(),
            self.receiver,
            self.calls.clone(),
        )
        .await?;
        self.calls.attach_installer(&installer)?;
        self.installer = Some(installer);
        Ok(())
    }
    pub async fn install_snapshot(
        &self,
        attempt: durable_steps::Uuid,
    ) -> Result<crate::InstallHandle<WriterHost<S, P>>, durable_steps::StartRefusal> {
        self.installer
            .as_ref()
            .ok_or_else(|| StoreError::Aborted {
                reason: "writer install owner is not configured".into(),
            })?
            .start(attempt)
            .await
    }
    pub async fn wait_install(
        &self,
        attempt: durable_steps::Uuid,
    ) -> Result<crate::InstallRecord, StoreError> {
        self.installer
            .as_ref()
            .ok_or_else(|| StoreError::Aborted {
                reason: "writer install owner is not configured".into(),
            })?
            .wait(attempt)
            .await
    }
    pub async fn install_record(
        &self,
        install: durable_steps::Uuid,
    ) -> Result<crate::InstallRecord, StoreError> {
        self.installer
            .as_ref()
            .ok_or_else(|| StoreError::Aborted {
                reason: "writer install owner is not configured".into(),
            })?
            .record(install)
            .await
    }
    pub async fn answer_release(
        &self,
        frame: fleet_sync_protocol::FrameId,
        call: durable_steps::CallId,
        request: &fleet_sync_protocol::ReleaseRequest,
    ) -> Result<(), StoreError> {
        let answer = self
            .database
            .request_release(request, call)
            .await
            .map_err(StoreError::backend)?;
        self.calls
            .answer_release(frame, answer)
            .await
            .map_err(StoreError::backend)
    }
    pub fn transport(&self) -> Arc<WriterSessionCalls<S, P>> {
        self.calls.clone()
    }
    pub async fn answer_withdrawal(
        &self,
        frame: fleet_sync_protocol::FrameId,
        call: durable_steps::CallId,
        request: &fleet_sync_protocol::WithdrawRelease,
    ) -> Result<(), StoreError> {
        let answer = self
            .database
            .withdraw_release(request, call)
            .await
            .map_err(StoreError::backend)?;
        self.calls
            .answer_withdrawal(frame, answer)
            .await
            .map_err(StoreError::backend)
    }
    pub fn store(&self) -> Arc<NativeWriterStore<S, P>> {
        self.store.clone()
    }
    pub fn engine(&self) -> Arc<Engine<NativeWriterStore<S, P>, WriterHost<S, P>>> {
        self.engine.clone()
    }
    pub async fn start_registration(&self) -> Result<OperationId, durable_steps::StartRefusal> {
        self.store.start_registration(self.receiver).await
    }
    #[cfg(feature = "test-support")]
    pub async fn journal_steps(
        &self,
        operation: OperationId,
    ) -> Result<Vec<durable_steps::storage::StepRecord>, StoreError> {
        self.store.journal_steps(operation).await
    }
    pub async fn prompt_renewal(&self) -> Result<OperationId, durable_steps::StartRefusal> {
        self.store.prompt_renewal(self.receiver).await
    }
    pub async fn wait_registration(&self) -> Result<WriterRegistrationOutcome, OpenError> {
        let mut changed = self.database.subscribe_authority_changes();
        loop {
            let outcome = self.database.registration_outcome().await?;
            if !matches!(outcome, WriterRegistrationOutcome::Pending) {
                return Ok(outcome);
            }
            changed
                .changed()
                .await
                .map_err(|error| OpenError::Storage {
                    message: error.to_string(),
                })?;
        }
    }
    pub async fn run(&self, shutdown: watch::Receiver<bool>) -> Result<(), StoreError> {
        self.store.ensure_renewal(self.receiver).await?;
        let runner = TokioRunner::new(
            self.engine.clone(),
            self.host.clone(),
            self.database.engine_lock(),
        );
        let result = if let Some(installer) = &self.installer {
            let installing = TokioRunner::new(
                installer.engine.clone(),
                installer.host.clone(),
                installer.control.engine_lock(),
            );
            let result = tokio::try_join!(runner.run(shutdown.clone()), installing.run(shutdown))
                .map(|_| ());
            let closed = installer.objects.close().await;
            result.and(closed)
        } else {
            runner.run(shutdown).await
        };
        self.calls.close_seed().await;
        result
    }
}
