use crate::{
    OpenError, ReadPool, ReplicatedSchema, StatementGuard, WriteRefusal, WriteTransaction,
    WriterOptions,
    schema_inspection::schema_entries,
    write_transaction::WriterControl,
    writer_file_record::{WriterFileRecord, read_file_record, write_file_record},
};
use fleet_sync_lease::{LeaseHolder, LeasePolicy, WriterState};
use fleet_sync_protocol::{DatabaseId, Epoch, PeerId};
use runtime_primitives::{Clock, IdSource, SystemClock, UuidV7Ids};
use sqlx_core::{
    connection::Connection,
    executor::Executor,
    migrate::Migrator,
    pool::{Pool, PoolOptions},
};
use sqlx_turso::{Turso, TursoConnectOptions};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::Mutex;

pub struct ReplicatedDatabase<S: ReplicatedSchema> {
    pub(crate) path: PathBuf,
    pub(crate) database: DatabaseId,
    pub(crate) schema: S,
    pub(crate) writer: Pool<Turso>,
    pub(crate) maintenance: Pool<Turso>,
    pub(crate) readers: ReadPool,
    pub(crate) control: Arc<Mutex<WriterControl>>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) authority_changes: tokio::sync::watch::Sender<()>,
    pub(crate) engine_work: Arc<Mutex<()>>,
}

#[derive(Clone, Copy)]
enum CaptureMode {
    Adoption,
    Writer,
    Maintenance,
    Reader,
}
async fn open_pool(path: &Path, mode: CaptureMode) -> Result<Pool<Turso>, OpenError> {
    let limit = if matches!(mode, CaptureMode::Reader) {
        4
    } else {
        1
    };
    Ok(PoolOptions::<Turso>::new()
        .min_connections(1)
        .max_connections(limit)
        .after_connect(move |connection, _| {
            Box::pin(async move {
                let statement = match mode {
                    CaptureMode::Adoption => return Ok(()),
                    CaptureMode::Writer => "PRAGMA capture_data_changes_conn('full')",
                    CaptureMode::Maintenance | CaptureMode::Reader => {
                        "PRAGMA capture_data_changes_conn('off')"
                    }
                };
                connection.execute(statement).await?;
                Ok(())
            })
        })
        .connect_with(
            TursoConnectOptions::new()
                .filename(path)
                .create_if_missing(!matches!(mode, CaptureMode::Adoption))
                .foreign_keys(true),
        )
        .await?)
}

impl<S: ReplicatedSchema> ReplicatedDatabase<S> {
    /// Adopt a pre-capture file whose SQLx history matches the schema bundle.
    pub async fn adopt(
        path: &Path,
        schema: S,
        peer: PeerId,
        policy: LeasePolicy,
        options: WriterOptions,
    ) -> Result<Self, OpenError> {
        let database =
            DatabaseId::new(UuidV7Ids.new_id()).map_err(|_| OpenError::corrupt("database.id"))?;
        Self::open_internal(
            path,
            database,
            schema,
            LeaseHolder::provisional(database, peer, policy),
            Arc::new(SystemClock),
            options,
            true,
        )
        .await
    }

    pub async fn create(
        path: &Path,
        schema: S,
        peer: PeerId,
        policy: LeasePolicy,
        options: WriterOptions,
    ) -> Result<Self, OpenError> {
        let database =
            DatabaseId::new(UuidV7Ids.new_id()).map_err(|_| OpenError::corrupt("database.id"))?;
        Self::open_with(
            path,
            database,
            schema,
            LeaseHolder::provisional(database, peer, policy),
            Arc::new(SystemClock),
            options,
        )
        .await
    }

    pub async fn open(
        path: &Path,
        database: DatabaseId,
        schema: S,
        holder: LeaseHolder,
    ) -> Result<Self, OpenError> {
        Self::open_with(
            path,
            database,
            schema,
            holder,
            Arc::new(SystemClock),
            WriterOptions::default(),
        )
        .await
    }

    pub async fn open_with(
        path: &Path,
        database: DatabaseId,
        schema: S,
        holder: LeaseHolder,
        clock: Arc<dyn Clock>,
        options: WriterOptions,
    ) -> Result<Self, OpenError> {
        Self::open_internal(path, database, schema, holder, clock, options, false).await
    }

    async fn open_internal(
        path: &Path,
        database: DatabaseId,
        schema: S,
        holder: LeaseHolder,
        clock: Arc<dyn Clock>,
        options: WriterOptions,
        adopting: bool,
    ) -> Result<Self, OpenError> {
        if holder.database() != database {
            return Err(OpenError::corrupt("writer.database"));
        }
        let maintenance = open_pool(
            path,
            if adopting {
                CaptureMode::Adoption
            } else {
                CaptureMode::Maintenance
            },
        )
        .await?;
        let setup = async {
            let mut connection = maintenance.acquire().await?;
            let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
            let entries = schema_entries(&mut transaction).await?;
            let managed = entries
                .iter()
                .any(|(entry, _)| entry.name == "_fleet_sync_writer");
            if managed || !adopting {
                crate::schema_inspection::reject_invalid_key_schemas(
                    &entries,
                    schema.replicated_tables(),
                )?;
            }
            if managed {
                crate::migration_execution::inspect_present_tables(
                    &mut transaction,
                    schema.replicated_tables(),
                )
                .await?;
            }
            if !managed
                && !adopting
                && entries
                    .iter()
                    .any(|(entry, _)| !fleet_sync_protocol::reserved_namespace(&entry.name))
            {
                return Err(OpenError::UnmanagedFile);
            }
            let adopted_layout = if adopting && !managed {
                crate::writer_adoption::validate_adoption(&mut transaction, &schema, &entries)
                    .await?
            } else {
                Vec::new()
            };
            let mut bookkeeping = sqlx::migrate!("./migrations");
            bookkeeping.dangerous_set_table_name("_fleet_sync_writer_engine_history");
            bookkeeping
                .run_direct(None, &mut *transaction, false)
                .await
                .map_err(|error| OpenError::Storage {
                    message: error.to_string(),
                })?;
            let mut history = Migrator::with_migrations(Vec::new());
            history.set_ignore_missing(true);
            history
                .run_direct(None, &mut *transaction, false)
                .await
                .map_err(|error| OpenError::Storage {
                    message: error.to_string(),
                })?;
            match read_file_record(&mut transaction).await? {
                Some(record) if record.database != database || record.peer != holder.peer() => {
                    return Err(OpenError::corrupt("writer.identity"));
                }
                Some(record) => {
                    let epoch = match holder.writer_state(clock.now()) {
                        Ok(
                            WriterState::Seeding { epoch }
                            | WriterState::Writing { epoch }
                            | WriterState::Paused { epoch, .. }
                            | WriterState::Draining { epoch, .. }
                            | WriterState::Released { epoch, .. },
                        ) => Some(epoch),
                        _ => None,
                    };
                    if epoch.is_some_and(|epoch| epoch != record.epoch) {
                        return Err(OpenError::corrupt("writer.epoch"));
                    }
                }
                None => {
                    let epoch = match holder
                        .writer_state(clock.now())
                        .map_err(|_| OpenError::corrupt("writer.initial_state"))?
                    {
                        WriterState::Provisional => {
                            Epoch::new(1).map_err(|_| OpenError::corrupt("writer.epoch"))?
                        }
                        WriterState::Seeding { epoch }
                        | WriterState::Writing { epoch }
                        | WriterState::Paused { epoch, .. }
                        | WriterState::Draining { epoch, .. }
                        | WriterState::Released { epoch, .. } => epoch,
                        WriterState::Superseded { .. } => {
                            return Err(OpenError::corrupt("writer.initial_state"));
                        }
                    };
                    let mut record = WriterFileRecord::new(database, holder.peer(), epoch);
                    record.retained_layout = adopted_layout;
                    if adopting {
                        record.origin = crate::writer_file_record::WriterFileOrigin::Adopted;
                    }
                    write_file_record(&mut transaction, &record).await?;
                }
            }
            transaction.commit().await?;
            Ok::<(), OpenError>(())
        }
        .await;
        if let Err(error) = setup {
            maintenance.close().await;
            return Err(error);
        }
        let writer = open_pool(path, CaptureMode::Writer).await?;
        let readers = ReadPool::new(open_pool(path, CaptureMode::Reader).await?);
        let (authority_changes, _) = tokio::sync::watch::channel(());
        let database = Self {
            path: path.into(),
            database,
            schema,
            writer,
            maintenance,
            readers,
            control: Arc::new(Mutex::new(WriterControl {
                holder,
                statements: StatementGuard::new(Vec::new()),
                provisional_writes: options.allow_provisional_writes,
            })),
            clock,
            authority_changes,
            engine_work: Arc::new(Mutex::new(())),
        };
        if let Err(error) = database.migrate().await {
            database.close().await;
            return Err(error);
        }
        Ok(database)
    }

    pub const fn database(&self) -> DatabaseId {
        self.database
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn reader(&self) -> &ReadPool {
        &self.readers
    }
    pub fn engine_lock(&self) -> Arc<Mutex<()>> {
        self.engine_work.clone()
    }
    pub fn subscribe_authority_changes(&self) -> tokio::sync::watch::Receiver<()> {
        self.authority_changes.subscribe()
    }
    pub async fn registration_outcome(
        &self,
    ) -> Result<crate::WriterRegistrationOutcome, OpenError> {
        let mut connection = self.maintenance.acquire().await?;
        let record = read_file_record(&mut connection)
            .await?
            .ok_or_else(|| OpenError::corrupt("writer.record"))?;
        Ok(crate::registration_bookkeeping::outcome(&record))
    }
    pub async fn begin_write(&self) -> Result<WriteTransaction, WriteRefusal> {
        if self.writer.is_closed() {
            return Err(WriteRefusal::Closed);
        }
        let mut control = self.control.clone().lock_owned().await;
        {
            let mut connection = self
                .maintenance
                .acquire()
                .await
                .map_err(WriteRefusal::from_sqlx)?;
            crate::writer_authority::restore_authority(&mut connection, &mut control.holder)
                .await
                .map_err(|error| WriteRefusal::Storage {
                    message: error.to_string(),
                })?;
        }
        WriteTransaction::begin(&self.writer, control, self.clock.clone()).await
    }
    pub async fn writer_state(&self) -> Result<WriterState, OpenError> {
        let mut control = self.control.lock().await;
        let mut connection = self.maintenance.acquire().await?;
        crate::writer_authority::restore_authority(&mut connection, &mut control.holder).await?;
        control
            .holder
            .writer_state(self.clock.now())
            .map_err(|error| OpenError::Holder { error })
    }
    pub async fn close(&self) {
        self.writer.close().await;
        self.readers.pool.close().await;
        self.maintenance.close().await;
    }
}
