#![allow(clippy::panic_in_result_fn)]
use crate::*;
use durable_steps::{
    AtomicStore, CallFailure, NotSentReason, StoreError, storage::TransactionScope,
};
use fleet_sync_lease::LeasePolicy;
use fleet_sync_protocol::*;
use fleet_sync_receiver::{
    AppliedPiece, ApplyDecision, FollowUpDone, FollowUpFailure, ReceiverHook, SnapshotInstall,
};
use fleet_sync_writer::{
    BootstrapGrant, Migration, ReplicatedDatabase, ReplicatedSchema, ReplicationTransport,
    SendFailure, WriterOptions,
};
use runtime_primitives::{Clock, SystemClock};
use sqlx_core::{migrate::MigrationType, sql_str::SqlSafeStr};
use std::{
    path::Path,
    sync::{Arc, LazyLock},
    time::Duration,
};
pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
static TABLES: LazyLock<Vec<TableName>> =
    LazyLock::new(|| vec![TableName::new("records").expect("valid fixture table")]);
static LATER_TABLES: LazyLock<Vec<TableName>> = LazyLock::new(|| {
    ["records", "later_records"]
        .into_iter()
        .map(|name| TableName::new(name).expect("valid fixture table"))
        .collect()
});
static MIGRATIONS: LazyLock<Vec<Migration>> = LazyLock::new(|| {
    vec![
    Migration::new(1,"records".into(),MigrationType::Simple,"CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY,amount INTEGER); INSERT INTO records VALUES('019eef00-0000-7000-8000-000000000001',1)".into_sql_str(),false),
    Migration::new(2,"backfill".into(),MigrationType::Simple,"UPDATE records SET amount=amount+1".into_sql_str(),false),
    Migration::new(3,"later".into(),MigrationType::Simple,"CREATE TABLE later_records(id TEXT NOT NULL PRIMARY KEY); ALTER TABLE records ADD COLUMN note TEXT; UPDATE records SET note='after bootstrap'".into_sql_str(),false),
]
});
static DIFFERENT: LazyLock<Vec<Migration>> = LazyLock::new(|| {
    vec![
    Migration::new(1,"different".into(),MigrationType::Simple,"CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY,amount INTEGER,extra TEXT); INSERT INTO records VALUES('019eef00-0000-7000-8000-000000000001',1,NULL)".into_sql_str(),false),
    MIGRATIONS[1].clone(),
]
});
pub(super) struct Schema {
    pub count: usize,
    pub different: bool,
}
impl ReplicatedSchema for Schema {
    fn migrations(&self) -> &'static [Migration] {
        if self.different {
            &DIFFERENT
        } else {
            &MIGRATIONS[..self.count]
        }
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        if self.count == 3 {
            &LATER_TABLES
        } else {
            &TABLES
        }
    }
}
pub(super) fn policy() -> LeasePolicy {
    LeasePolicy {
        lease_duration: Duration::from_secs(600),
        renew_interval: Duration::from_secs(60),
    }
}
pub(super) struct NoCalls;
impl CallRouter for NoCalls {
    async fn dispatch(&self, _: CallEnvelope) -> Result<serde_json::Value, CallFailure> {
        Err(CallFailure::NotSent {
            reason: NotSentReason::Overloaded,
        })
    }
}
pub(super) type TestHost = TokioHost<NoCalls>;
pub(super) struct Hook;
impl<S: TransactionScope> ReceiverHook<S> for Hook {
    type Protocol = NoConsumer;
    type FollowUp = ();
    async fn on_applied(
        &self,
        _: &mut S,
        _: &AppliedPiece<'_, ()>,
    ) -> Result<Vec<()>, HookRefusal> {
        Ok(Vec::new())
    }
    async fn follow_up(
        &self,
        _: &(),
        _: durable_steps::CallId,
    ) -> Result<FollowUpDone, FollowUpFailure> {
        Ok(FollowUpDone)
    }
}
pub(super) struct CopyTransport {
    pub copy: Arc<TokioObject<TestHost>>,
    pub sender: PeerId,
}
impl ReplicationTransport<NoConsumer> for CopyTransport {
    async fn send_piece(
        &self,
        _: ReceiverId,
        piece: Piece<()>,
    ) -> Result<ReplicationAnswer, SendFailure> {
        let sender = self.sender;
        self.copy
            .transaction(move |scope| {
                Box::pin(async move {
                    let answer = fleet_sync_receiver::apply_piece(
                        scope,
                        &piece,
                        &sender,
                        &Hook,
                        SystemClock.now(),
                    )
                    .await
                    .map_err(StoreError::backend)?;
                    Ok(match answer {
                        ApplyDecision::Applied { through, .. } => {
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
                    })
                })
            })
            .await
            .map_err(|error| SendFailure::Unavailable {
                message: error.to_string(),
            })
    }
}
pub(super) async fn source(path: &Path) -> TestResult<ReplicatedDatabase<Schema>> {
    Ok(ReplicatedDatabase::create(
        path,
        Schema {
            count: 2,
            different: false,
        },
        PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000002,
        ))?,
        policy(),
        WriterOptions::default(),
    )
    .await?)
}
pub(super) async fn stage(
    store: &StagingStore,
    source: &ReplicatedDatabase<Schema>,
) -> TestResult<(SnapshotInstall, CopyStatus)> {
    let mut read = source.snapshot().await?;
    let install = SnapshotInstall {
        install_id: fleet_sync_protocol::InstallId::new(durable_steps::Uuid::now_v7())?,
        database: source.database(),
        position: read.position(),
        schema: read.schema().clone(),
    };
    let status = CopyStatus {
        database: source.database(),
        role: CopyRole::Authoritative,
        fence: install.position.epoch,
        holder: Some(PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000002,
        ))?),
        start: ReplicationPosition::new(install.position.epoch, CommitSequence::new(0)),
        acked: install.position,
        schema: install.schema.clone(),
    };
    store.prepare(&install, &status).await?;
    let mut cursor = None;
    let mut page = 0;
    loop {
        let contents = read.page(cursor.clone()).await?;
        store
            .stage_page(&install, page, cursor.as_ref(), &contents)
            .await?;
        cursor = contents.next;
        page += 1;
        if cursor.is_none() {
            break;
        }
    }
    store.verify(&install, &read.manifest().await?).await?;
    read.close().await?;
    Ok((install, status))
}
pub(super) fn grant(install: &SnapshotInstall) -> TestResult<BootstrapGrant> {
    let now = SystemClock.now();
    Ok(BootstrapGrant {
        lease: LeaseStatus {
            database: install.database,
            holder: PeerId::new(durable_steps::Uuid::from_u128(
                0x019eef00000070008000000000000004,
            ))?,
            epoch: Epoch::new(2)?,
            state: LeaseState::Held,
            expires_at: now.after(Duration::from_secs(600)),
            remaining_ms: 600_000,
        },
        request_started: now,
        start: install.position,
    })
}

#[cfg(feature = "test-support")]
struct PendingAfterConversion;
#[cfg(feature = "test-support")]
impl durable_steps::Task for PendingAfterConversion {
    const KIND: &'static str = "test.pending-after-bootstrap-conversion";
    const VERSION: u32 = 1;
    type Store = fleet_sync_writer::WriterJournalStore<Schema, TestHost>;
    type Input = ();
    type Output = ();
    async fn run<C: durable_steps::OperationContext<Store = Self::Store>>(
        &self,
        _: &mut C,
        _: &(),
    ) -> Result<(), durable_steps::OperationFailure> {
        Ok(())
    }
}
#[cfg(feature = "test-support")]
pub(super) async fn admit_pending(database: &Arc<ReplicatedDatabase<Schema>>) -> TestResult {
    use durable_steps::{JournalScope, RequestKey};
    let store = database
        .journal_store(Arc::new(TokioHost::new(Arc::new(NoCalls), SystemClock)))
        .await?;
    store
        .transaction(|scope| {
            Box::pin(async move {
                scope
                    .admit::<PendingAfterConversion>(
                        &RequestKey::new("after-conversion").map_err(StoreError::backend)?,
                        &(),
                    )
                    .await
                    .map_err(StoreError::backend)?;
                Ok(())
            })
        })
        .await?;
    Ok(())
}
