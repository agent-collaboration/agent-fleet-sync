use crate::{
    OpenError, ReplicatedDatabase, ReplicatedSchema, StatementGuard, WriterOptions,
    bootstrap_schema::validate_prefix,
    migration_execution::inspect_present_tables,
    native_file::{PublishError, checkpoint_for_publish},
    snapshot_schema::snapshot_schema,
    writer_file_record::{RetainedFileOriginKind, read_file_record, write_file_record},
    writer_registration_state::{RegistrationSeed, RegistrationState},
    writer_scope::WriterScope,
};
use durable_steps::BoxFuture;
use fleet_sync_lease::{LeaseHolder, LeasePolicy};
use fleet_sync_protocol::{LeaseState, LeaseStatus, ReplicationPosition, SyncTime};
use fleet_sync_receiver::{SnapshotInstall, SnapshotInstallRules, SnapshotInstallState};
use runtime_primitives::SystemClock;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
    migrate::{Migration, Migrator},
};
use sqlx_turso::{TursoConnectOptions, TursoConnection};
use std::{path::Path, sync::Arc};

#[cfg(test)]
#[path = "bootstrap_catalog_tests.rs"]
mod catalog_tests;

/// The observed grant and the fenced predecessor's final position. The original
/// request time anchors lease admission across a restart.
#[derive(Clone, Debug)]
pub struct BootstrapGrant {
    pub lease: LeaseStatus,
    pub request_started: SyncTime,
    pub start: ReplicationPosition,
}
#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    #[error(transparent)]
    Writer(#[from] OpenError),
    #[error(transparent)]
    Publish(#[from] PublishError),
    #[error(transparent)]
    Snapshot(#[from] fleet_sync_receiver::SnapshotInstallError),
}
impl From<sqlx_core::Error> for BootstrapError {
    fn from(error: sqlx_core::Error) -> Self {
        OpenError::from(error).into()
    }
}

impl<S: ReplicatedSchema> ReplicatedDatabase<S> {
    #[cfg(feature = "test-support")]
    pub fn hold_bootstrap_before_rename(&self) {
        self.bootstrap_publish_probe.arm();
    }
    #[cfg(feature = "test-support")]
    pub fn hold_bootstrap_after_rename(&self) {
        self.bootstrap_publish_probe.arm_published();
    }
    #[cfg(feature = "test-support")]
    pub async fn wait_bootstrap_published(&self) -> Result<SnapshotInstall, OpenError> {
        self.bootstrap_publish_probe.wait().await
    }
    #[cfg(feature = "test-support")]
    pub async fn wait_bootstrap_converted(&self) -> Result<SnapshotInstall, OpenError> {
        self.bootstrap_publish_probe.wait().await
    }
    #[cfg(feature = "test-support")]
    pub fn release_bootstrap_rename(&self) {
        self.bootstrap_publish_probe.release();
    }
    #[cfg(feature = "test-support")]
    pub fn block_bootstrap_reopen(&self, blocked: bool) {
        self.bootstrap_publish_probe.block_reopen(blocked);
    }
    /// Open a new writer behind the seed gate without running pending consumer
    /// migrations against its empty file.
    pub async fn seeding(
        path: &Path,
        schema: S,
        grant: BootstrapGrant,
        policy: LeasePolicy,
    ) -> Result<Self, OpenError> {
        if grant.start.epoch >= grant.lease.epoch || grant.lease.state != LeaseState::Held {
            return Err(OpenError::corrupt("bootstrap.grant"));
        }
        let holder = LeaseHolder::seeding(
            grant.lease.database,
            grant.lease.holder,
            grant.lease.epoch,
            policy,
        );
        Self::open_internal(
            path,
            grant.lease.database,
            schema,
            holder,
            Arc::new(SystemClock),
            WriterOptions::default(),
            crate::replicated_database::OpenMode::Bootstrap(grant),
        )
        .await
    }

    pub async fn installed_snapshot(&self) -> Result<Option<SnapshotInstallState>, OpenError> {
        let mut connection = self.maintenance.acquire().await?;
        Ok(read_file_record(&mut connection)
            .await?
            .ok_or_else(|| OpenError::corrupt("writer.record"))?
            .bootstrap)
    }

    pub async fn replicated_objects(
        &self,
    ) -> Result<std::collections::BTreeSet<String>, OpenError> {
        let _control = self.control.lock().await;
        let mut connection = self.maintenance.acquire().await?;
        let layouts = crate::capture_stream::committed_layout(&mut connection).await?;
        let tables = layouts
            .iter()
            .map(|layout| {
                fleet_sync_protocol::TableName::new(layout.name())
                    .map_err(|_| OpenError::corrupt("migration.membership"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(
            crate::snapshot_schema::replicated_entries(&mut connection, &tables)
                .await?
                .into_iter()
                .map(|entry| entry.name)
                .collect(),
        )
    }

    /// The staging owner holds its install lock and supplies the closed-file
    /// carry. Conversion, checkpoint, rename and reopen stay with this writer.
    pub async fn publish_bootstrap<F>(
        &self,
        staging: &Path,
        install: &SnapshotInstall,
        carry: F,
    ) -> Result<(), BootstrapError>
    where
        F: for<'path> FnOnce(
                &'path Path,
                &'path Path,
            ) -> BoxFuture<'path, Result<(), PublishError>>
            + Send
            + 'static,
    {
        let prefix = validate_prefix(&self.schema, &install.schema).await?;
        let staged_path = staging.to_path_buf();
        let expected = install.clone();
        let probe = self.bootstrap_publish_probe.clone();
        self.with_drained_snapshot(Some(install.clone()), move |live| {
            Box::pin(async move {
                Ok(Self::publish_closed_bootstrap(
                    live,
                    &staged_path,
                    &expected,
                    prefix.migrations,
                    prefix.tables,
                    carry,
                    probe,
                )
                .await)
            })
        })
        .await??;
        let mut control = self.control.lock().await;
        let mut connection = self.maintenance.acquire().await?;
        crate::writer_authority::restore_authority(&mut connection, &mut control.holder).await?;
        control.statements =
            StatementGuard::new(crate::capture_stream::committed_layout(&mut connection).await?);
        Ok(())
    }

    async fn publish_closed_bootstrap<F>(
        live: &Path,
        staging: &Path,
        install: &SnapshotInstall,
        prefix: Vec<Migration>,
        tables: Vec<fleet_sync_protocol::TableName>,
        carry: F,
        probe: crate::bootstrap_publish_probe::BootstrapPublishProbe,
    ) -> Result<(), BootstrapError>
    where
        F: for<'path> FnOnce(
                &'path Path,
                &'path Path,
            ) -> BoxFuture<'path, Result<(), PublishError>>
            + Send,
    {
        let live_admission = crate::file_admission::FileAdmission::writer(live, false).await?;
        let mut source = open_closed(live, &live_admission).await?;
        let record = read_file_record(&mut source).await;
        source.close().await?;
        let record = record?.ok_or_else(|| OpenError::corrupt("bootstrap.source_record"))?;
        if record
            .bootstrap
            .as_ref()
            .is_some_and(|state| state.install == *install && state.verified)
        {
            return Ok(());
        }
        if record.origin != RetainedFileOriginKind::Bootstrap
            || record.database != install.database
            || record.start != install.position
            || install.position.epoch >= record.epoch
        {
            return Err(OpenError::corrupt("bootstrap.snapshot_identity").into());
        }
        let staged_admission = crate::file_admission::FileAdmission::writer(staging, false).await?;
        let mut staged = open_closed(staging, &staged_admission).await?;
        let state = SnapshotInstallRules(&mut WriterScope(&mut staged))
            .state()
            .await;
        staged.close().await?;
        if !state?.is_some_and(|state| state.install == *install && state.verified) {
            return Err(fleet_sync_receiver::SnapshotInstallError::NotVerified.into());
        }
        carry(live, staging).await?;
        let converted_admission =
            crate::file_admission::FileAdmission::writer(staging, false).await?;
        let mut staged = open_closed(staging, &converted_admission).await?;
        let converted = Self::convert_staged_bootstrap(&mut staged, install, prefix, &tables).await;
        let closed = staged.close().await;
        converted?;
        closed?;
        probe.converted(install).await;
        tokio::fs::rename(staging, live)
            .await
            .map_err(PublishError::io)?;
        let parent = live
            .parent()
            .ok_or_else(|| PublishError::io("live file has no parent"))?;
        tokio::fs::File::open(parent)
            .await
            .map_err(PublishError::io)?
            .sync_all()
            .await
            .map_err(PublishError::io)?;
        probe.published(install).await;
        Ok(())
    }

    async fn convert_staged_bootstrap(
        staged: &mut TursoConnection,
        install: &SnapshotInstall,
        prefix: Vec<Migration>,
        tables: &[fleet_sync_protocol::TableName],
    ) -> Result<(), BootstrapError> {
        let mut transaction = staged.begin_with("BEGIN IMMEDIATE").await?;
        let mut record = read_file_record(&mut transaction)
            .await?
            .ok_or_else(|| OpenError::corrupt("bootstrap.carried_record"))?;
        if record.database != install.database
            || record.start != install.position
            || record.origin != RetainedFileOriginKind::Bootstrap
        {
            return Err(OpenError::corrupt("bootstrap.carried_identity").into());
        }
        let layouts = inspect_present_tables(&mut transaction, tables).await?;
        // `skip=true` records the proved prefix without repeating its DML on
        // received data. SQLx owns its migration table and checksum encoding.
        Migrator::with_migrations(prefix)
            .run_direct(None, &mut *transaction, true)
            .await
            .map_err(|error| OpenError::Storage {
                message: error.to_string(),
            })?;
        let (found, statements) = snapshot_schema(&mut transaction, tables).await?;
        if found != install.schema {
            return Err(OpenError::BootstrapSchemaMismatch {
                expected: install.schema.clone(),
                found,
            }
            .into());
        }
        record.retained_after = install.position;
        record.retained_change_id = 0;
        record.retained_layout = layouts.clone();
        record.bootstrap = Some(SnapshotInstallState {
            install: install.clone(),
            verified: true,
            ready: false,
        });
        if install.schema.version > 0
            && !crate::capture_stream::schema_step_exists(&mut transaction, install.schema.version)
                .await?
        {
            crate::checked_queries::write_schema_step(
                &mut WriterScope(&mut transaction),
                i64::try_from(install.schema.version)
                    .map_err(|_| OpenError::corrupt("bootstrap.version"))?,
                serde_json::to_string(&statements)
                    .map_err(|_| OpenError::corrupt("bootstrap.statements"))?,
                serde_json::to_string(&layouts)
                    .map_err(|_| OpenError::corrupt("bootstrap.layouts"))?,
            )
            .await?;
        }
        write_file_record(&mut transaction, &record).await?;
        transaction.commit().await?;
        checkpoint_for_publish(staged).await?;
        Ok(())
    }

    pub async fn mark_bootstrap_ready(
        &self,
        install: &SnapshotInstall,
        receiver: fleet_sync_protocol::ReceiverId,
    ) -> Result<(), OpenError> {
        let mut control = self.control.lock().await;
        let mut connection = self.maintenance.acquire().await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let mut record = read_file_record(&mut transaction)
            .await?
            .ok_or_else(|| OpenError::corrupt("writer.record"))?;
        let marker = record
            .bootstrap
            .as_mut()
            .ok_or_else(|| OpenError::corrupt("bootstrap.marker"))?;
        if marker.install != *install || !marker.verified {
            return Err(OpenError::corrupt("bootstrap.ready_identity"));
        }
        if marker.ready {
            transaction.rollback().await?;
            return Ok(());
        }
        let RegistrationState::Granted {
            grant,
            seed: Some(seed),
        } = &mut record.registration
        else {
            return Err(OpenError::corrupt("bootstrap.registration"));
        };
        let mut holder = control.holder.clone();
        crate::writer_authority::restore_authority(&mut transaction, &mut holder).await?;
        holder
            .complete_seed(grant.lease.epoch)
            .map_err(crate::writer_registration_state::holder_error)?;
        *seed = RegistrationSeed::Ready {
            position: install.position,
        };
        marker.ready = true;
        write_file_record(&mut transaction, &record).await?;
        crate::registration_bookkeeping::register_sender_copy(
            &mut transaction,
            receiver,
            install.position,
        )
        .await?;
        let mut scope = WriterScope(&mut transaction);
        crate::checked_queries::clear_bootstrap_marker(&mut scope).await?;
        crate::checked_queries::clear_bootstrap_pages(&mut scope).await?;
        crate::checked_queries::clear_bootstrap_receiver_history(&mut scope).await?;
        crate::checked_queries::clear_bootstrap_membership(&mut scope).await?;
        transaction.commit().await?;
        control.holder = holder;
        self.authority_changes.send_replace(());
        self.copies_changes.send_replace(());
        Ok(())
    }
}
async fn open_closed(
    path: &Path,
    _admission: &crate::file_admission::FileAdmission,
) -> Result<TursoConnection, OpenError> {
    let mut connection = TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .foreign_keys(false)
        .connect()
        .await?;
    connection
        .execute("PRAGMA capture_data_changes_conn('off')")
        .await?;
    Ok(connection)
}
