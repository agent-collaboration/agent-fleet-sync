use crate::{
    LocalStateSelection, StagingError, StagingStore, TokioObject, carry_local_state,
    native_carry_schema::FileScope, native_publish::checkpoint_for_publish,
    staging_store::open_stage,
};
use durable_steps::Host;
use fleet_sync_receiver::{
    SNAPSHOT_REPLACED_TABLES, SnapshotInstall, SnapshotInstallError, SnapshotInstallRules,
};
use sqlx_core::connection::Connection;
use std::path::Path;

impl StagingStore {
    pub async fn publish_writer<S: fleet_sync_writer::ReplicatedSchema>(
        &self,
        install: &SnapshotInstall,
        target: &fleet_sync_writer::ReplicatedDatabase<S>,
    ) -> Result<(), StagingError> {
        let _owner = self.lock(install).await?;
        let before = target
            .replicated_objects()
            .await
            .map_err(fleet_sync_writer::BootstrapError::from)?;
        target
            .publish_bootstrap(&self.path(install), install, move |live, staged| {
                Box::pin(async move {
                    carry_writer_local(live, staged, before)
                        .await
                        .map_err(|error| {
                            crate::PublishError::Storage(durable_steps::StoreError::backend(error))
                        })
                })
            })
            .await?;
        Ok(())
    }
    /// Publication is receipted by the marker in the live file itself. After a
    /// crash, that marker distinguishes a completed rename from a staging file
    /// that still needs the target's latest journal, hook rows and fence.
    pub(crate) async fn publish<H: Host>(
        &self,
        install: &SnapshotInstall,
        target: &TokioObject<H>,
    ) -> Result<(), StagingError> {
        let _owner = self.lock(install).await?;
        let staging = self.path(install);
        let saved_install = install.clone();
        target
            .with_drained_file(move |live| {
                Box::pin(
                    async move { Ok(publish_closed_files(live, &staging, &saved_install).await) },
                )
            })
            .await??;
        let install = install.clone();
        target
            .rule_transaction(move |scope| {
                Box::pin(async move {
                    let state = SnapshotInstallRules(scope)
                        .state()
                        .await?
                        .ok_or(SnapshotInstallError::NotPrepared)?;
                    if state.install != install || !state.verified {
                        return Err(SnapshotInstallError::IdentityConflict.into());
                    }
                    Ok::<_, StagingError>(())
                })
            })
            .await
    }
}

async fn carry_writer_local(
    live: &Path,
    staged: &Path,
    before: std::collections::BTreeSet<String>,
) -> Result<(), StagingError> {
    let mut source = open_stage(live, false).await?;
    let mut destination = match open_stage(staged, false).await {
        Ok(connection) => connection,
        Err(error) => {
            source.close().await?;
            return Err(error);
        }
    };
    let result = async {
        let mut transaction = destination.begin_with("BEGIN IMMEDIATE").await?;
        let after = SnapshotInstallRules(&mut FileScope(&mut transaction))
            .replicated_objects()
            .await?;
        carry_local_state(
            &mut source,
            &mut transaction,
            &LocalStateSelection {
                replicated_before: before,
                replicated_after: after,
                // Keep the verified receipt while the writer's own tables replace
                // receiver bookkeeping, including their different schema-step form.
                replaced_tables: [
                    "_fleet_sync_install",
                    "_fleet_sync_install_pages",
                    "_fleet_sync_copy_migrations",
                    "_fleet_sync_copy_tables",
                ]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            },
        )
        .await?;
        transaction.commit().await?;
        Ok::<_, StagingError>(())
    }
    .await;
    let source_closed = source.close().await;
    let destination_closed = destination.close().await;
    result?;
    source_closed?;
    destination_closed?;
    Ok(())
}

async fn publish_closed_files(
    live: &Path,
    staging: &Path,
    install: &SnapshotInstall,
) -> Result<(), StagingError> {
    let mut source = open_stage(live, false).await?;
    let read = async {
        let mut rules = SnapshotInstallRules(&mut FileScope(&mut source));
        if let Some(state) = rules.state().await?
            && state.install == *install
        {
            return if state.verified {
                Ok(None)
            } else {
                Err(SnapshotInstallError::NotVerified.into())
            };
        }
        let names = rules.replicated_objects().await?;
        let fence = rules.read_old_fence().await?;
        Ok::<_, StagingError>(Some((names, fence)))
    }
    .await;
    let read = match read {
        Ok(Some(read)) => read,
        Ok(None) => {
            source.close().await?;
            return Ok(());
        }
        Err(error) => {
            source.close().await?;
            return Err(error);
        }
    };
    let mut destination = match open_stage(staging, false).await {
        Ok(connection) => connection,
        Err(error) => {
            source.close().await?;
            return Err(error);
        }
    };
    let result = async {
        let mut transaction = destination.begin_with("BEGIN IMMEDIATE").await?;
        let mut scope = FileScope(&mut transaction);
        let mut rules = SnapshotInstallRules(&mut scope);
        let state = rules
            .state()
            .await?
            .ok_or(SnapshotInstallError::NotPrepared)?;
        if state.install != *install {
            return Err(SnapshotInstallError::IdentityConflict.into());
        }
        if !state.verified {
            return Err(SnapshotInstallError::NotVerified.into());
        }
        let replicated_after = rules.replicated_objects().await?;
        rules.merge_old_fence(install, read.1.as_ref()).await?;
        carry_local_state(
            &mut source,
            &mut transaction,
            &LocalStateSelection {
                replicated_before: read.0,
                replicated_after,
                replaced_tables: SNAPSHOT_REPLACED_TABLES
                    .iter()
                    .map(|name| (*name).into())
                    .collect(),
            },
        )
        .await?;
        transaction.commit().await?;
        checkpoint_for_publish(&mut destination).await?;
        Ok::<_, StagingError>(())
    }
    .await;
    let source_closed = source.close().await;
    let destination_closed = destination.close().await;
    result?;
    source_closed?;
    destination_closed?;
    tokio::fs::rename(staging, live)
        .await
        .map_err(StagingError::io)?;
    let parent = live
        .parent()
        .ok_or_else(|| StagingError::io("live file has no parent"))?;
    tokio::fs::File::open(parent)
        .await
        .map_err(StagingError::io)?
        .sync_all()
        .await
        .map_err(StagingError::io)?;
    Ok(())
}
