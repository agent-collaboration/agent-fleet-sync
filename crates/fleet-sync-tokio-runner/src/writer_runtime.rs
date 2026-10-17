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
        })
    }
    pub fn transport(&self) -> Arc<WriterSessionCalls<S, P>> {
        self.calls.clone()
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
        let result = runner.run(shutdown).await;
        self.calls.close_seed().await;
        result
    }
}
