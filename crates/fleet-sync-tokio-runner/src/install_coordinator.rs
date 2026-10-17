use crate::{
    InstallFileAnswer, InstallProgress, InstallRecord, InstallRequest, InstallStatus, ObjectKey,
    StagingError, StagingStore, TokioObject, install_contract::corrupt,
    install_records::InstallRecords, native_carry_schema::FileScope,
    native_object::ObjectConnection,
};
use durable_steps::{
    Admitted, BoxFuture, Host, JournalScope, OperationId, StartRefusal, StoreError, Uuid,
    turso_store::TursoScope,
};
use fleet_sync_receiver::{SnapshotInstall, SnapshotInstallRules};
use sqlx_core::{connection::Connection, migrate::Migrator};
use std::sync::Arc;

/// Owns install ordering in the host control file. Its connection owner spans
/// publication, but no control SQL transaction spans file or network I/O.
pub struct InstallCoordinator<H: Host> {
    pub(crate) control: Arc<TokioObject<H>>,
    pub(crate) staging: Arc<StagingStore>,
}
pub struct InstallHandle<H: Host> {
    coordinator: Arc<InstallCoordinator<H>>,
    request: InstallRequest,
    pub operation: OperationId,
}
impl<H: Host> InstallHandle<H> {
    pub async fn wait(&self) -> Result<InstallRecord, StoreError> {
        self.coordinator
            .wait_for_attempt(self.request.attempt)
            .await
    }
    pub async fn status(&self) -> Result<InstallRecord, StoreError> {
        let request = self.request.clone();
        self.coordinator
            .control
            .rule_transaction(move |scope| {
                Box::pin(async move {
                    InstallRecords(scope)
                        .history(&request.target)
                        .await?
                        .into_iter()
                        .rev()
                        .find(|record| record.request.attempt == request.attempt)
                        .ok_or_else(|| corrupt("install handle has no record"))
                })
            })
            .await
    }
}
impl<H: Host> InstallCoordinator<H> {
    pub async fn wait_for_attempt(&self, install: Uuid) -> Result<InstallRecord, StoreError> {
        let initial = self.record(install).await?;
        let mut changes = self.control.subscribe_commits();
        loop {
            let request = initial.request.clone();
            let record = self
                .control
                .rule_transaction(move |scope| {
                    Box::pin(async move {
                        InstallRecords(scope)
                            .history(&request.target)
                            .await?
                            .into_iter()
                            .rev()
                            .find(|record| record.request.attempt == request.attempt)
                            .ok_or_else(|| corrupt("install attempt has no record"))
                    })
                })
                .await?;
            if !matches!(record.status, InstallStatus::Running { .. }) {
                return Ok(record);
            }
            changes.changed().await.map_err(StoreError::backend)?;
        }
    }
    pub async fn open(
        control: Arc<TokioObject<H>>,
        staging: Arc<StagingStore>,
    ) -> Result<Self, StoreError> {
        control
            .rule_transaction(|scope| {
                Box::pin(async move {
                    use durable_steps::storage::TransactionScope;
                    let mut migrator: Migrator = sqlx::migrate!("./control-migrations");
                    migrator.dangerous_set_table_name("_fleet_sync_control_migrations");
                    migrator
                        .run_direct(None, scope.executor(), false)
                        .await
                        .map_err(StoreError::backend)
                })
            })
            .await?;
        Ok(Self { control, staging })
    }
    pub async fn start(
        self: &Arc<Self>,
        request: InstallRequest,
        target: &TokioObject<H>,
    ) -> Result<InstallHandle<H>, StartRefusal> {
        if request.target != *target.key() || target.path() == self.control.path() {
            return Err(corrupt("install target differs or replaces its control journal").into());
        }
        let input = request.clone();
        let key = crate::install_task::request_key(&request)?;
        let mut owner = self.control.connection.lock().await;
        let ObjectConnection::Open(connection) = &mut *owner else {
            return Err(corrupt("control file is closed").into());
        };
        // A process can die between rename/ready and its control receipt. Read
        // that file-owned receipt before a newer admission can hide its marker.
        let current = InstallRecords(&mut FileScope(connection))
            .current(&request.target)
            .await?;
        let reconciled = if let Some(mut record) = current {
            if matches!(record.status, InstallStatus::Running { .. })
                && record.status.snapshot().is_some()
            {
                let marker = target
                    .rule_transaction(|scope| {
                        Box::pin(async move {
                            SnapshotInstallRules(scope)
                                .state()
                                .await
                                .map_err(StoreError::backend)
                        })
                    })
                    .await?;
                if let Some(marker) = marker
                    && record.status.snapshot() == Some(&marker.install)
                    && marker.verified
                {
                    record.status = if marker.ready {
                        InstallStatus::Ready {
                            snapshot: marker.install,
                        }
                    } else {
                        InstallStatus::Running {
                            progress: InstallProgress::Published {
                                snapshot: marker.install,
                            },
                        }
                    };
                }
            }
            Some(record)
        } else {
            None
        };
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(StoreError::backend)?;
        let host = self.control.host();
        let mut scope = TursoScope::new(&mut transaction, host.as_ref());
        if let Some(record) = reconciled {
            InstallRecords(&mut scope).save(&record).await?;
        }
        let admitted = scope
            .admit::<crate::InstallSnapshot<H>>(&key, &input)
            .await?;
        if matches!(admitted, Admitted::New(_)) {
            InstallRecords(&mut scope)
                .reserve(&input, input.attempt)
                .await?;
        }
        let operation = admitted.id();
        transaction.commit().await.map_err(StoreError::backend)?;
        self.control.notify_committed();
        drop(owner);
        let host = self.control.host();
        host.wake_at(host.now())
            .await
            .map_err(StoreError::backend)?;
        Ok(InstallHandle {
            coordinator: self.clone(),
            request,
            operation,
        })
    }
    pub async fn record(&self, id: Uuid) -> Result<InstallRecord, StoreError> {
        self.control
            .rule_transaction(move |scope| {
                Box::pin(async move { InstallRecords(scope).get(id).await })
            })
            .await
    }
    pub async fn needs_snapshot(&self, target: &ObjectKey) -> Result<bool, StoreError> {
        let target = target.clone();
        self.control
            .rule_transaction(move |scope| {
                Box::pin(async move { InstallRecords(scope).needs_snapshot(&target).await })
            })
            .await
    }
    #[cfg(test)]
    pub(crate) async fn pin(&self, snapshot: SnapshotInstall) -> Result<InstallRecord, StoreError> {
        self.control
            .rule_transaction(move |scope| {
                Box::pin(async move {
                    InstallRecords(scope)
                        .pin(snapshot.install_id, snapshot)
                        .await
                })
            })
            .await
    }
    pub async fn publish(
        &self,
        id: Uuid,
        target: &TokioObject<H>,
    ) -> Result<InstallFileAnswer, StagingError> {
        let mut owner = self.control.connection.lock().await;
        let ObjectConnection::Open(connection) = &mut *owner else {
            return Err(corrupt("control file is closed").into());
        };
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let mut records = InstallRecords(&mut FileScope(&mut transaction));
        let mut record = records.get(id).await?;
        check_target(&record, target)?;
        // A historical publish receipt answers the old call even after a newer
        // install replaced its live marker. It must never rename the old file.
        if let Some(snapshot) = record.status.published().cloned() {
            transaction.commit().await?;
            return Ok(InstallFileAnswer::Published { snapshot });
        }
        if let Some(answer) = terminal_answer(&record.status) {
            transaction.commit().await?;
            return Ok(answer);
        }
        require_current(&mut records, &record).await?;
        let snapshot = record
            .status
            .snapshot()
            .cloned()
            .ok_or_else(|| corrupt("install has not pinned a snapshot"))?;
        transaction.commit().await?;
        self.staging.publish(&snapshot, target).await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        record.status = InstallStatus::Running {
            progress: InstallProgress::Published {
                snapshot: snapshot.clone(),
            },
        };
        InstallRecords(&mut FileScope(&mut transaction))
            .save(&record)
            .await?;
        transaction.commit().await?;
        Ok(InstallFileAnswer::Published { snapshot })
    }
    pub async fn mark_ready_with<F>(
        &self,
        id: Uuid,
        target: &TokioObject<H>,
        ready: F,
    ) -> Result<InstallFileAnswer, StagingError>
    where
        F: for<'s, 't> FnOnce(
                &'s mut TursoScope<'t, H>,
                SnapshotInstall,
            ) -> BoxFuture<'s, Result<(), StagingError>>
            + Send
            + 'static,
    {
        let mut owner = self.control.connection.lock().await;
        let ObjectConnection::Open(connection) = &mut *owner else {
            return Err(corrupt("control file is closed").into());
        };
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let mut records = InstallRecords(&mut FileScope(&mut transaction));
        let mut record = records.get(id).await?;
        check_target(&record, target)?;
        if let InstallStatus::Ready { snapshot } = &record.status {
            let answer = InstallFileAnswer::Ready {
                snapshot: snapshot.clone(),
            };
            transaction.commit().await?;
            return Ok(answer);
        }
        if let Some(answer) = terminal_answer(&record.status) {
            transaction.commit().await?;
            return Ok(answer);
        }
        require_current(&mut records, &record).await?;
        let snapshot = record
            .status
            .published()
            .cloned()
            .ok_or_else(|| corrupt("install has not published"))?;
        transaction.commit().await?;
        let expected = snapshot.clone();
        target
            .rule_transaction(move |scope| {
                Box::pin(async move {
                    ready(scope, expected.clone()).await?;
                    let marker = SnapshotInstallRules(scope)
                        .state()
                        .await?
                        .ok_or_else(|| corrupt("ready marker is missing"))?;
                    if marker.install != expected || !marker.ready {
                        return Err(
                            corrupt("ready callback did not make this install ready").into()
                        );
                    }
                    Ok::<_, StagingError>(())
                })
            })
            .await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        record.status = InstallStatus::Ready {
            snapshot: snapshot.clone(),
        };
        InstallRecords(&mut FileScope(&mut transaction))
            .save(&record)
            .await?;
        transaction.commit().await?;
        self.control.notify_committed();
        Ok(InstallFileAnswer::Ready { snapshot })
    }
}
fn check_target<H: Host>(
    record: &InstallRecord,
    target: &TokioObject<H>,
) -> Result<(), StoreError> {
    if record.request.target != *target.key() {
        return Err(corrupt("install belongs to another target"));
    }
    Ok(())
}
async fn require_current<
    S: durable_steps::storage::TransactionScope<Database = sqlx_turso::Turso>,
>(
    records: &mut InstallRecords<'_, S>,
    record: &InstallRecord,
) -> Result<(), StoreError> {
    if records
        .current(&record.request.target)
        .await?
        .as_ref()
        .map(|current| current.install_id)
        != Some(record.install_id)
    {
        return Err(corrupt("nonterminal install is not current"));
    }
    Ok(())
}
fn terminal_answer(status: &InstallStatus) -> Option<InstallFileAnswer> {
    match status {
        InstallStatus::Superseded { by, .. } => Some(InstallFileAnswer::Superseded { by: *by }),
        InstallStatus::Failed { reason, .. } => Some(InstallFileAnswer::Failed {
            reason: reason.clone(),
        }),
        _ => None,
    }
}
