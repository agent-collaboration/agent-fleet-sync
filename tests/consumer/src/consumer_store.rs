use crate::{EffectReceipt, HookEffect, SavedEffect, TestResult, checked_queries};
use durable_steps::{CallId, storage::TransactionScope};
use fleet_sync_session::{ClientSessionState, SessionClientStore, StoreError};
use sqlx_core::{
    connection::Connection,
    pool::{Pool, PoolOptions},
};
use sqlx_turso::{Turso, TursoConnectOptions, TursoConnection};
use std::{path::Path, sync::Arc};
use tokio::sync::watch;

pub(crate) struct ConsumerScope<'a>(pub &'a mut TursoConnection);
impl TransactionScope for ConsumerScope<'_> {
    type Database = Turso;
    type Executor<'a>
        = &'a mut TursoConnection
    where
        Self: 'a;
    fn executor(&mut self) -> Self::Executor<'_> {
        self.0
    }
}
#[derive(Clone)]
pub struct ConsumerStore {
    pub(crate) pool: Pool<Turso>,
    pub(crate) changes: Arc<watch::Sender<()>>,
}
impl ConsumerStore {
    pub async fn open(path: &Path) -> TestResult<Self> {
        let pool = PoolOptions::<Turso>::new()
            .max_connections(1)
            .connect_with(
                TursoConnectOptions::new()
                    .filename(path)
                    .create_if_missing(true),
            )
            .await?;
        let mut connection = pool.acquire().await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::migrate!("./migrations")
            .run_direct(None, &mut *transaction, false)
            .await?;
        transaction.commit().await?;
        let (changes, _) = watch::channel(());
        Ok(Self {
            pool,
            changes: Arc::new(changes),
        })
    }
    pub async fn save_effect(&self, call: CallId, effect: HookEffect) -> TestResult {
        let call_id = serde_json::to_string(&call)?;
        let saved = SavedEffect { call, effect };
        let record = serde_json::to_string(&saved)?;
        let mut connection = self.pool.acquire().await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let mut scope = ConsumerScope(&mut transaction);
        checked_queries::insert_effect(&mut scope, call_id.clone(), record.clone()).await?;
        let row = checked_queries::read_effect(&mut scope, call_id.clone())
            .await?
            .ok_or("effect receipt missing")?;
        if text(row.record, row.record_class)? != record {
            return Err("CallId has conflicting effect".into());
        }
        checked_queries::increment_effect_attempts(&mut scope, call_id).await?;
        transaction.commit().await?;
        self.changes.send_replace(());
        Ok(())
    }
    pub async fn effects(&self) -> TestResult<Vec<EffectReceipt>> {
        let mut connection = self.pool.acquire().await?;
        checked_queries::read_effects(&mut ConsumerScope(&mut connection))
            .await?
            .into_iter()
            .map(|row| {
                if row.attempts_class.as_deref() != Some("integer") {
                    return Err("invalid effect attempts".into());
                }
                Ok(EffectReceipt {
                    saved: serde_json::from_str(&text(row.record, row.record_class)?)?,
                    attempts: row.attempts.ok_or("missing effect attempts")?.parse()?,
                })
            })
            .collect()
    }
    pub async fn wait_effects(&self, count: usize) -> TestResult<Vec<EffectReceipt>> {
        let mut changes = self.changes.subscribe();
        loop {
            let effects = self.effects().await?;
            if effects.len() >= count {
                return Ok(effects);
            }
            changes.changed().await?;
        }
    }
    pub async fn close(&self) {
        self.pool.close().await;
    }
    pub async fn wait_attempts(&self, call: CallId, count: u64) -> TestResult<EffectReceipt> {
        let mut changes = self.changes.subscribe();
        loop {
            if let Some(receipt) = self
                .effects()
                .await?
                .into_iter()
                .find(|receipt| receipt.saved.call == call && receipt.attempts >= count)
            {
                return Ok(receipt);
            }
            changes.changed().await?;
        }
    }
}
fn text(value: Option<String>, class: Option<String>) -> TestResult<String> {
    if class.as_deref() != Some("text") {
        return Err("invalid stored text".into());
    }
    value.ok_or_else(|| "missing stored text".into())
}
impl SessionClientStore for ConsumerStore {
    async fn load(&self) -> Result<Option<ClientSessionState>, StoreError> {
        let result: TestResult<_> = async {
            let mut connection = self.pool.acquire().await?;
            checked_queries::read_client_session(&mut ConsumerScope(&mut connection))
                .await?
                .map(|row| Ok(serde_json::from_str(&text(row.record, row.record_class)?)?))
                .transpose()
        }
        .await;
        result.map_err(unavailable)
    }
    async fn save(&self, state: &ClientSessionState) -> Result<(), StoreError> {
        let result: TestResult = async {
            let mut connection = self.pool.acquire().await?;
            checked_queries::save_client_session(
                &mut ConsumerScope(&mut connection),
                serde_json::to_string(state)?,
            )
            .await?;
            Ok(())
        }
        .await;
        result.map_err(unavailable)
    }
}
fn unavailable(error: impl std::fmt::Display) -> StoreError {
    StoreError::Unavailable {
        message: error.to_string(),
    }
}
