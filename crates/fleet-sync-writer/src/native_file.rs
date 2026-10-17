use crate::{ReplicatedDatabase, ReplicatedSchema};
use durable_steps::{BoxFuture, StoreError};
use sqlx_core::{
    column::Column,
    query::query,
    row::Row,
    sql_str::{AssertSqlSafe, SqlSafeStr},
    type_info::TypeInfo,
    value::ValueRef,
};
use sqlx_turso::{Turso, TursoConnection};
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("writer file is closed")]
    Closed,
    #[error("live writer receipt differs from the old or expected publication")]
    InvalidWriterReceipt,
    #[error("writer snapshot publication requires Seeding")]
    NotSeeding,
    #[error("writer snapshot publication is blocked by a guarded reader")]
    ActiveReaders,
    #[error(transparent)]
    Storage(#[from] StoreError),
    #[error("invalid native checkpoint result: {detail}")]
    InvalidCheckpoint { detail: String },
    #[error("native checkpoint is busy: busy={busy}, log={log:?}, checkpointed={checkpointed:?}")]
    CheckpointBusy {
        busy: i64,
        log: Option<i64>,
        checkpointed: Option<i64>,
    },
    #[error("committed WAL data remains after checkpoint: {bytes} bytes")]
    WalRemains { bytes: u64 },
    #[error("native publish I/O failed: {message}")]
    Io { message: String },
}
impl PublishError {
    pub fn io(error: impl std::fmt::Display) -> Self {
        Self::Io {
            message: error.to_string(),
        }
    }
}

pub async fn checkpoint_for_publish(connection: &mut TursoConnection) -> Result<(), PublishError> {
    // This one host-maintenance exception is deliberate (design decision 40).
    // Checked queries catch SQL/schema drift; this shape is fixed by the engine.
    // Turso describes these columns as NULL, so validate every actual named
    // integer cell instead of weakening query generation for application SQL.
    let rows =
        query::<Turso>(AssertSqlSafe(include_str!("../sql/wal_checkpoint.sql")).into_sql_str())
            .fetch_all(connection)
            .await
            .map_err(StoreError::backend)?;
    if rows.len() != 1 {
        return Err(invalid("expected exactly one row"));
    }
    let row = rows
        .first()
        .ok_or_else(|| invalid("missing checkpoint row"))?;
    let mut columns = row.columns().iter().map(Column::name).collect::<Vec<_>>();
    columns.sort_unstable();
    if columns != ["busy", "checkpointed", "log"] {
        return Err(invalid("unexpected checkpoint column set"));
    }
    let integer = |name: &str| -> Result<i64, PublishError> {
        let raw = row
            .try_get_raw(name)
            .map_err(|_| invalid("missing checkpoint cell"))?;
        if raw.is_null() || raw.type_info().name() != "INTEGER" {
            return Err(invalid(&format!(
                "checkpoint.{name}: expected INTEGER, found {}",
                raw.type_info().name()
            )));
        }
        let value = row
            .try_get::<i64, _>(name)
            .map_err(|_| invalid("checkpoint integer could not be decoded"))?;
        if value < 0 {
            return Err(invalid("negative checkpoint counter"));
        }
        Ok(value)
    };
    let busy = integer("busy")?;
    // Unlike SQLite's integer counters, Turso 0.8.1 leaves both counters
    // NULL on checkpoint failure: turso_core-0.8.1/vdbe/execute.rs:799-806.
    // Re-check this exception at the next driver bump (decision 40 refinement).
    let optional_integer = |name: &str| -> Result<Option<i64>, PublishError> {
        if row
            .try_get_raw(name)
            .map_err(|_| invalid("missing checkpoint cell"))?
            .is_null()
        {
            Ok(None)
        } else {
            integer(name).map(Some)
        }
    };
    let log = optional_integer("log")?;
    let checkpointed = optional_integer("checkpointed")?;
    if busy != 0 {
        return Err(PublishError::CheckpointBusy {
            busy,
            log,
            checkpointed,
        });
    }
    let log = log.ok_or_else(|| invalid("checkpoint.log: expected INTEGER, found NULL"))?;
    let checkpointed = checkpointed
        .ok_or_else(|| invalid("checkpoint.checkpointed: expected INTEGER, found NULL"))?;
    if log != checkpointed {
        return Err(PublishError::CheckpointBusy {
            busy,
            log: Some(log),
            checkpointed: Some(checkpointed),
        });
    }
    Ok(())
}
fn invalid(detail: &str) -> PublishError {
    PublishError::InvalidCheckpoint {
        detail: detail.into(),
    }
}

impl<S: ReplicatedSchema> ReplicatedDatabase<S> {
    /// Run a closed-path callback while this writer owns Seeding admission.
    pub async fn with_drained_file<T, F>(&self, publish: F) -> Result<T, PublishError>
    where
        T: Send + 'static,
        F: for<'path> FnOnce(&'path Path) -> BoxFuture<'path, Result<T, PublishError>> + Send,
    {
        self.with_drained_snapshot(None, publish).await
    }

    pub(crate) async fn with_drained_snapshot<T, F>(
        &self,
        expected: Option<fleet_sync_receiver::SnapshotInstall>,
        publish: F,
    ) -> Result<T, PublishError>
    where
        T: Send + 'static,
        F: for<'path> FnOnce(&'path Path) -> BoxFuture<'path, Result<T, PublishError>> + Send,
    {
        let _engine = self.engine_work.lock().await;
        let mut control = self.control.lock().await;
        self.native_owner.recover().await?;
        let _readers = self
            .readers
            .readers
            .clone()
            .try_write_owned()
            .map_err(|_| PublishError::ActiveReaders)?;
        let record = {
            let mut connection = self
                .maintenance
                .acquire()
                .await
                .map_err(StoreError::backend)?;
            crate::writer_authority::restore_authority(&mut connection, &mut control.holder)
                .await
                .map_err(StoreError::backend)?;
            crate::writer_file_record::read_file_record(&mut connection)
                .await
                .map_err(StoreError::backend)?
                .ok_or(PublishError::InvalidWriterReceipt)?
        };
        if !matches!(
            control.holder.writer_state(self.clock.now()),
            Ok(fleet_sync_lease::WriterState::Seeding { .. })
        ) {
            return Err(PublishError::NotSeeding);
        }
        self.native_owner.publish(&record, expected, publish).await
    }
}

pub(crate) async fn ensure_empty_wal(path: &Path) -> Result<(), PublishError> {
    let mut wal = path.as_os_str().to_owned();
    wal.push("-wal");
    match tokio::fs::metadata(wal).await {
        Ok(metadata) if metadata.len() != 0 => Err(PublishError::WalRemains {
            bytes: metadata.len(),
        }),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(PublishError::io(error)),
    }
}
