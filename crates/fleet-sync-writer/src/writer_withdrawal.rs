use crate::{
    OpenError, ReplicatedDatabase, ReplicatedSchema,
    writer_release_record::{ReleaseState, WriterReleaseRecord, read_releases, save_release},
};
use durable_steps::CallId;
use fleet_sync_protocol::{ReleaseRequest, WithdrawAnswer, WithdrawRelease};
use sqlx_core::connection::Connection;

impl<S: ReplicatedSchema> ReplicatedDatabase<S> {
    /// Persist the transfer's withdrawal before reopening its write gate. A
    /// repeat uses the receipt, while durable authority still outranks it.
    pub async fn withdraw_release(
        &self,
        request: &WithdrawRelease,
        call: CallId,
    ) -> Result<WithdrawAnswer, OpenError> {
        if request.database != self.database {
            return Err(OpenError::corrupt("withdrawal.database"));
        }
        let identity = ReleaseRequest {
            database: request.database,
            epoch: request.epoch,
            transfer: request.transfer,
        };
        let mut control = self.control.lock().await;
        let mut connection = self.maintenance.acquire().await?;
        let records = read_releases(&mut connection, self.database).await?;
        let existing = records.iter().find(|record| record.call == call);
        if existing.is_some_and(|record| {
            record.request != identity
                || !matches!(record.state, ReleaseState::WithdrawalAnswered { .. })
        }) {
            return Err(OpenError::corrupt("withdrawal.call_conflict"));
        }
        crate::writer_authority::restore_authority(&mut connection, &mut control.holder).await?;
        if let Some(WriterReleaseRecord {
            state: ReleaseState::WithdrawalAnswered { answer },
            ..
        }) = existing
        {
            return Ok(*answer);
        }
        let mut holder = control.holder.clone();
        let answer = holder.withdraw_release(request.epoch, request.transfer);
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        save_release(
            &mut transaction,
            &WriterReleaseRecord {
                request: identity,
                call,
                state: ReleaseState::WithdrawalAnswered { answer },
            },
        )
        .await?;
        transaction.commit().await?;
        control.holder = holder;
        self.authority_changes.send_replace(());
        Ok(answer)
    }
}
