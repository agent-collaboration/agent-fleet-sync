#![allow(clippy::unwrap_used, clippy::panic_in_result_fn)]
use super::*;
use crate::turso_registry_fixture::*;
use durable_steps::{AtomicStore, storage::TransactionScope};
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;

#[tokio::test]
async fn typed_workflow_rows_keep_full_u64_and_phase_history_across_reopen() -> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(&directory.path().join("registry.db"), host.clone(), true).await?;
    let attempt = TransferId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000555,
    ))?;
    let request = HolderChangeRequest::Claim {
        attempt,
        database: database()?,
        claimant: peer(1)?,
    };
    let expected = store
        .transaction(move |scope| {
            Box::pin(async move {
                let report = ReportSchemaSupport {
                    peer: peer(1).map_err(storage)?,
                    session_id: SessionId::new(durable_steps::Uuid::from_u128(
                        0x019eef00000070008000000000000556,
                    ))
                    .map_err(storage)?,
                    attach_seq: AttachSequence::new(2).map_err(storage)?,
                    support: SchemaSupport {
                        max_version: u64::MAX,
                    },
                };
                crate::report_schema_support(
                    scope,
                    &report,
                    fleet_sync_protocol::SyncTime::from_unix_millis(1000),
                )
                .await
                .map_err(storage)?;
                crate::admit_holder_change::<TestStore, Turso>(
                    scope,
                    &request,
                    &ActorRef::new("test owner").map_err(storage)?,
                    SyncTime::from_unix_millis(1000),
                )
                .await
                .map_err(storage)?;
                let mut registry = RegistryStorage(scope);
                let admission = registry.admission(attempt).await.map_err(storage)?.unwrap();
                let mut transfer = TransferRecord::requested(
                    TransferIntent {
                        request,
                        new_epoch: Some(Epoch::new(u64::MAX).map_err(storage)?),
                        target_schema: admission.target_schema,
                    },
                    SyncTime::from_unix_millis(1000),
                );
                registry
                    .save_transfer(
                        &transfer,
                        fleet_sync_protocol::SyncTime::from_unix_millis(1000),
                    )
                    .await
                    .map_err(storage)?;
                transfer
                    .enter(
                        HolderChangePhase::Reserved,
                        SyncTime::from_unix_millis(1100),
                    )
                    .map_err(storage)?;
                transfer
                    .enter(HolderChangePhase::Fencing, SyncTime::from_unix_millis(1200))
                    .map_err(storage)?;
                registry
                    .save_transfer(
                        &transfer,
                        fleet_sync_protocol::SyncTime::from_unix_millis(1000),
                    )
                    .await
                    .map_err(storage)?;
                let epoch = Epoch::new(u64::MAX).map_err(storage)?;
                transfer.outcome = Some(HolderChangeAnswer::Granted {
                    epoch,
                    expires_at: SyncTime::from_unix_millis(3000),
                    start: ReplicationPosition::new(epoch, CommitSequence::new(u64::MAX)),
                });
                transfer
                    .enter(HolderChangePhase::Ended, SyncTime::from_unix_millis(1300))
                    .map_err(storage)?;
                registry
                    .save_transfer(
                        &transfer,
                        fleet_sync_protocol::SyncTime::from_unix_millis(1000),
                    )
                    .await
                    .map_err(storage)?;
                let renewal = RenewalAttempt {
                    request: RenewRequest {
                        attempt: RequestId::new(durable_steps::Uuid::from_u128(
                            0x019eef00000070008000000000000557,
                        ))
                        .map_err(storage)?,
                        database: database().map_err(storage)?,
                        epoch,
                    },
                    holder: peer(1).map_err(storage)?,
                    answer: RenewAnswer::Renewed {
                        recorded: RenewResult {
                            epoch,
                            expires_at: SyncTime::from_unix_millis(3000),
                        },
                        current: LeaseStatus {
                            database: database().map_err(storage)?,
                            epoch,
                            holder: peer(1).map_err(storage)?,
                            state: LeaseState::Held,
                            expires_at: SyncTime::from_unix_millis(3000),
                            remaining_ms: u64::MAX,
                        },
                        transfer_in_progress: Some(attempt),
                        replayed: false,
                    },
                };
                registry
                    .save_renewal(
                        &renewal,
                        fleet_sync_protocol::SyncTime::from_unix_millis(1000),
                    )
                    .await
                    .map_err(storage)?;
                Ok((admission, transfer, renewal))
            })
        })
        .await?;
    drop(store);
    let store = open_store(&directory.path().join("registry.db"), host, true).await?;
    store.transaction(move |scope| Box::pin(async move {
        let mut registry=RegistryStorage(scope);
        assert_eq!(registry.admission(attempt).await.map_err(storage)?,Some(expected.0));
        assert_eq!(registry.transfer(attempt).await.map_err(storage)?,Some(expected.1));
        assert_eq!(registry.renewal(expected.2.request.database,expected.2.request.attempt).await.map_err(storage)?,Some(expected.2));
        let row=query::<Turso>("SELECT new_epoch,granted_start_sequence FROM _fleet_sync_transfers").fetch_one(registry.0.executor()).await?;
        assert_eq!(row.try_get::<String,_>("new_epoch")?,"18446744073709551615");
        assert_eq!(row.try_get::<String,_>("granted_start_sequence")?,"18446744073709551615");
        let row=query::<Turso>("SELECT count(*) AS phases,count(DISTINCT id) AS ids FROM _fleet_sync_transfer_phase_history").fetch_one(registry.0.executor()).await?;
        assert_eq!(row.try_get::<i64,_>("phases")?,4);
        assert_eq!(row.try_get::<i64,_>("ids")?,4);
        Ok(())
    })).await?;
    Ok(())
}
