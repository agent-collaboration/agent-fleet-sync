use crate::{
    InstallCoordinator, InstallFileAnswer, InstallFileResult, InstallRecord, InstallStatus,
    StagedVerification, StagingError, install_contract::corrupt,
};
use durable_steps::{Host, Uuid};
use fleet_sync_protocol::{CopyStatus, SnapshotCursor, SnapshotManifestAnswer, SnapshotRead};
use std::future::Future;

impl<H: Host> InstallCoordinator<H> {
    pub async fn prepare(
        &self,
        id: Uuid,
        source: &CopyStatus,
    ) -> Result<InstallFileResult, StagingError> {
        let record = self.record(id).await?;
        if let Some(answer) = inactive(&record) {
            return Ok(answer);
        }
        let snapshot = record
            .status
            .snapshot()
            .ok_or_else(|| corrupt("install has not pinned a snapshot"))?;
        self.staging.prepare(snapshot, source).await?;
        Ok(InstallFileResult::Prepared)
    }
    pub async fn fetch_page<F, Fetch>(
        &self,
        id: Uuid,
        page: u32,
        cursor: Option<&SnapshotCursor>,
        fetch: F,
    ) -> Result<InstallFileResult, StagingError>
    where
        F: FnOnce() -> Fetch + Send,
        Fetch: Future<Output = Result<SnapshotRead, StagingError>> + Send,
    {
        let record = self.record(id).await?;
        if let Some(answer) = inactive(&record) {
            return Ok(answer);
        }
        let snapshot = record
            .status
            .snapshot()
            .ok_or_else(|| corrupt("install has not pinned a snapshot"))?;
        Ok(InstallFileResult::Page(
            self.staging
                .fetch_page(snapshot, page, cursor, fetch)
                .await?,
        ))
    }
    pub async fn verify_from<F, Fetch>(
        &self,
        id: Uuid,
        fetch: F,
    ) -> Result<InstallFileResult, StagingError>
    where
        F: FnOnce() -> Fetch + Send,
        Fetch: Future<Output = Result<SnapshotManifestAnswer, StagingError>> + Send,
    {
        let record = self.record(id).await?;
        if let Some(answer) = inactive(&record) {
            return Ok(answer);
        }
        let snapshot = record
            .status
            .snapshot()
            .ok_or_else(|| corrupt("install has not pinned a snapshot"))?;
        Ok(match self.staging.verify_from(snapshot, fetch).await? {
            StagedVerification::Verified => InstallFileResult::Verified,
            StagedVerification::Moved { position } => InstallFileResult::Moved { position },
        })
    }
}
fn inactive(record: &InstallRecord) -> Option<InstallFileResult> {
    let answer = match &record.status {
        InstallStatus::Running { .. } => return None,
        InstallStatus::Ready { snapshot } => InstallFileAnswer::Ready {
            snapshot: snapshot.clone(),
        },
        InstallStatus::Superseded { by, .. } => InstallFileAnswer::Superseded { by: *by },
        InstallStatus::Failed { reason, .. } => InstallFileAnswer::Failed {
            reason: reason.clone(),
        },
    };
    Some(InstallFileResult::Outcome(answer))
}
