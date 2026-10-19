//! Transfer a completed follower generation into its immutable guarded owner.
use crate::{InstallCoordinator, InstallFileAnswer, InstallStatus, StagingError, TokioObject};
use durable_steps::{Host, storage::TransactionScope};
use fleet_sync_protocol::{InstallId, NoConsumer};
use fleet_sync_receiver::{AppliedPiece, HookError, ReceiverRules, SnapshotInstallRules};

struct ReadCopyRules;
impl<S: TransactionScope> ReceiverRules<S> for ReadCopyRules {
    type Protocol = NoConsumer;
    type FollowUp = ();
    async fn on_applied(&self, _: &mut S, _: &AppliedPiece<'_, ()>) -> Result<Vec<()>, HookError> {
        Ok(Vec::new())
    }
}

impl<H: Host> InstallCoordinator<H> {
    /// Finish a verified follower install without writer authority or consumer hooks.
    pub async fn mark_read_copy_ready(
        &self,
        id: InstallId,
        target: &TokioObject<H>,
    ) -> Result<InstallFileAnswer, StagingError> {
        self.mark_ready_with(id, target, |scope, expected| {
            Box::pin(async move {
                SnapshotInstallRules(scope)
                    .mark_ready(&expected, &ReadCopyRules)
                    .await?;
                Ok(())
            })
        })
        .await
    }

    /// Consume one Ready follower target as a disposable immutable generation.
    ///
    /// The caller supplies a fresh file per generation. Older ReadCopy handles
    /// and pins are independent; this operation never drains their files. The
    /// target becomes permanently closed, and the returned handle owns cleanup.
    pub async fn read_copy(
        &self,
        id: InstallId,
        target: &TokioObject<H>,
    ) -> Result<fleet_sync_writer::ReadCopy, StagingError> {
        let record = self.record(id).await?;
        if record.request.target != *target.key() {
            return Err(
                crate::install_contract::corrupt("read copy belongs to another target").into(),
            );
        }
        let InstallStatus::Ready { snapshot } = record.status else {
            return Err(fleet_sync_receiver::SnapshotInstallError::NotVerified.into());
        };
        target.close_for_read_copy().await?;
        Ok(fleet_sync_writer::ReadCopy::open_verified(target.path(), &snapshot).await?)
    }
}
