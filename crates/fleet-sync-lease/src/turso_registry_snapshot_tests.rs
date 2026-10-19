//! State revisions follow literal committed facts, including equal-facts renewals.
#![allow(clippy::panic_in_result_fn)]
use crate::{
    turso_epoch_history_fixture::{move_to, registered},
    turso_registry_fixture::*,
    turso_registry_snapshot_fixture::*,
    turso_workflow_tests::{attempt, cancel, move_request, view},
    *,
};
use durable_steps::{AtomicStore, storage::TransactionScope};
use fleet_sync_protocol::*;
use sqlx_turso::Turso;
use std::sync::{Arc, atomic::Ordering};

#[tokio::test]
async fn registry_snapshot_changes_for_expiry_but_not_equal_fresh_renewals_replay_or_read_clock()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(
        &directory.path().join("registry.db"),
        Arc::clone(&host),
        true,
    )
    .await?;
    assert_eq!(snapshot(&store).await?, None);
    assert!(
        store
            .transaction(|scope| Box::pin(async move {
                registry_snapshot_ids(scope).await.map_err(storage)
            }))
            .await?
            .is_empty()
    );
    let engine = engine(Arc::clone(&store), Arc::clone(&host))?;
    crate::turso_change_tests::register_first(&engine).await?;
    let initial = required_snapshot(&store).await?;
    assert_revision(&initial, 1, 1, 1000);
    assert_eq!(
        initial.grant,
        Some(RegistryGrant {
            epoch: Epoch::new(1)?,
            holder: peer(1)?,
            expires_at: SyncTime::from_unix_millis(3000)
        })
    );
    assert!(matches!(
        renew_at(&store, 4001, 1500, 2000).await?,
        RenewAnswer::Renewed {
            replayed: false,
            ..
        }
    ));
    let changed = required_snapshot(&store).await?;
    assert_revision(&changed, 1, 2, 1500);
    assert_eq!(
        changed.grant.as_ref().map(|grant| grant.expires_at),
        Some(SyncTime::from_unix_millis(3500))
    );
    let bytes = serde_json::to_vec(&changed)?;
    assert!(matches!(
        renew_at(&store, 4002, 1500, 2000).await?,
        RenewAnswer::Renewed {
            replayed: false,
            ..
        }
    ));
    assert_eq!(
        serde_json::to_vec(&required_snapshot(&store).await?)?,
        bytes
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_renewal_attempts"
        )
        .await?,
        "2"
    );
    host.now.store(9000, Ordering::SeqCst);
    let database = database()?;
    assert!(matches!(
        store
            .transaction(move |scope| Box::pin(async move {
                lease_status(scope, database, SyncTime::from_unix_millis(9000))
                    .await
                    .map_err(storage)
            }))
            .await?,
        Some(LeaseStatus {
            state: LeaseState::Expired,
            ..
        })
    ));
    assert!(matches!(
        renew_at(&store, 4001, 9000, 2000).await?,
        RenewAnswer::Renewed { replayed: true, .. }
    ));
    assert_eq!(
        serde_json::to_vec(&required_snapshot(&store).await?)?,
        bytes
    );
    assert!(matches!(
        renew_at(&store, 4003, 9001, 10).await?,
        RenewAnswer::Renewed {
            replayed: false,
            ..
        }
    ));
    assert_revision(&required_snapshot(&store).await?, 1, 3, 9001);
    assert!(matches!(
        renew_at(&store, 4004, 9002, 1).await?,
        RenewAnswer::Renewed {
            replayed: false,
            ..
        }
    ));
    let shorter = required_snapshot(&store).await?;
    assert_revision(&shorter, 1, 4, 9002);
    assert_eq!(
        shorter.grant.as_ref().map(|grant| grant.expires_at),
        Some(SyncTime::from_unix_millis(9003))
    );
    drop(engine);
    drop(store);
    let reopened = open_store(&directory.path().join("registry.db"), host, false).await?;
    assert_eq!(required_snapshot(&reopened).await?, shorter);
    Ok(())
}

#[tokio::test]
async fn registry_snapshot_open_move_renews_old_epoch_but_release_reservation_and_cancel_do_not_version()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let (host, store, engine) = registered(directory.path()).await?;
    let before = required_snapshot(&store).await?;
    *host.release_answer.lock().await = Some(ReleaseAnswer::Released {
        final_position: ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(9)),
    });
    let id = attempt(4005)?;
    let operation = engine
        .start::<HolderChangeTask<TestStore, Turso>>(
            &holder_change_request_key(id)?,
            move_request(id)?,
        )
        .await?;
    assert!(matches!(
        view(&store, id).await?.phase,
        HolderChangePhase::Released { .. }
    ));
    assert_eq!(
        scalar(&store, "SELECT epoch AS value FROM _fleet_sync_epoch_floor").await?,
        "2"
    );
    assert_eq!(required_snapshot(&store).await?, before);
    assert!(
        matches!(renew_at(&store,4006,1500,2000).await?,RenewAnswer::Renewed { transfer_in_progress:Some(active), .. } if active==id)
    );
    let renewed = required_snapshot(&store).await?;
    assert_revision(&renewed, 1, 2, 1500);
    assert!(matches!(
        cancel(&store, id).await?,
        CancelAnswer::Accepted { .. }
    ));
    host.advance();
    assert!(engine.drive_due().await?.completed.contains(&operation));
    engine.drive_due().await?;
    assert!(matches!(
        view(&store, id).await?.outcome,
        Some(HolderChangeAnswer::Cancelled { .. })
    ));
    assert_eq!(required_snapshot(&store).await?, renewed);
    Ok(())
}

#[tokio::test]
async fn registry_snapshot_holder_changes_reset_once_at_actual_successor_grants() -> TestResult {
    let directory = tempfile::tempdir()?;
    let (host, store, engine) = registered(directory.path()).await?;
    renew_at(&store, 4007, 1100, 2000).await?;
    assert_revision(&required_snapshot(&store).await?, 1, 2, 1100);
    host.now.store(1500, Ordering::SeqCst);
    let id = attempt(4008)?;
    assert!(
        matches!(move_to(&engine,id,1,2).await?,HolderChangeAnswer::Granted { epoch, .. } if epoch==Epoch::new(2)?)
    );
    let granted = required_snapshot(&store).await?;
    assert_revision(&granted, 2, 1, 1500);
    assert_eq!(
        granted.grant,
        Some(RegistryGrant {
            epoch: Epoch::new(2)?,
            holder: peer(2)?,
            expires_at: SyncTime::from_unix_millis(3500)
        })
    );
    assert!(matches!(
        move_to(&engine, id, 1, 2).await?,
        HolderChangeAnswer::Granted { .. }
    ));
    assert_eq!(required_snapshot(&store).await?, granted);
    crate::turso_floor_repair_tests::advance_copy(&host, 2, 7).await?;
    assert_eq!(required_snapshot(&store).await?, granted);
    host.advance();
    let takeover_at = host.now.load(Ordering::SeqCst);
    assert!(
        matches!(crate::turso_epoch_history_fixture::takeover(&engine,attempt(4010)?,3).await?,HolderChangeAnswer::Granted {epoch,..} if epoch==Epoch::new(8)?)
    );
    let fenced = required_snapshot(&store).await?;
    assert_revision(&fenced, 8, 1, takeover_at);
    assert_eq!(
        fenced.grant,
        Some(RegistryGrant {
            epoch: Epoch::new(8)?,
            holder: peer(3)?,
            expires_at: SyncTime::from_unix_millis(takeover_at + 2000)
        })
    );
    Ok(())
}

#[tokio::test]
async fn registry_snapshot_corrupt_revisions_fail_closed_and_exhaustion_changes_no_fact_or_receipt()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let (_, store, _engine) = registered(directory.path()).await?;
    required_snapshot(&store).await?;
    let original = scalar(&store, "SELECT record AS value FROM _fleet_sync_databases").await?;
    for corruption in [
        "missing revision",
        "zero",
        "epoch mismatch",
        "missing time",
        "exhausted",
    ] {
        let mut value: serde_json::Value = serde_json::from_str(&original)?;
        match corruption {
            "missing revision" => {
                value
                    .as_object_mut()
                    .ok_or("record not object")?
                    .remove("revision");
            }
            "zero" => value["revision"]["change"] = serde_json::json!(0),
            "epoch mismatch" => value["revision"]["epoch"] = serde_json::json!(2),
            "missing time" => {
                value
                    .as_object_mut()
                    .ok_or("record not object")?
                    .remove("changed_at");
            }
            _ => value["revision"]["change"] = serde_json::json!(u64::MAX),
        }
        let damaged = serde_json::to_string(&value)?;
        let result=store.transaction(move|scope|Box::pin(async move {
            sqlx_core::query::query::<Turso>("UPDATE _fleet_sync_databases SET record=?1").bind(damaged.clone()).execute(scope.executor()).await?;
            use sqlx_core::row::Row;
            let authority_before=sqlx_core::query::query::<Turso>("SELECT (SELECT record FROM _fleet_sync_leases) AS lease,(SELECT CAST(count(*) AS TEXT) FROM _fleet_sync_renewal_attempts) AS attempts").fetch_one(scope.executor()).await?;
            let authority_before=(authority_before.try_get::<String,_>("lease")?,authority_before.try_get::<String,_>("attempts")?);
            if corruption=="exhausted" {
                let request=RenewRequest {attempt:import_request(4009,"a").map_err(storage)?.attempt,database:database().map_err(storage)?,epoch:Epoch::new(1).map_err(storage)?};
                let answer=renew(scope,&request,&peer(1).map_err(storage)?,SyncTime::from_unix_millis(1500),&config().map_err(storage)?.lease).await;
                assert!(matches!(answer,Err(LeaseError::RevisionExhausted { .. })));
            } else {
                assert!(matches!(registry_snapshot(scope,database().map_err(storage)?).await,Err(LeaseError::Corrupt { .. })),"accepted {corruption}");
                assert!(matches!(registry_snapshot_ids(scope).await,Err(LeaseError::Corrupt { .. })),"enumerated {corruption}");
            }
            let after=sqlx_core::query::query::<Turso>("SELECT record FROM _fleet_sync_databases").fetch_one(scope.executor()).await?.try_get::<String,_>("record")?;
            assert_eq!(after,damaged);
            let authority_after=sqlx_core::query::query::<Turso>("SELECT (SELECT record FROM _fleet_sync_leases) AS lease,(SELECT CAST(count(*) AS TEXT) FROM _fleet_sync_renewal_attempts) AS attempts").fetch_one(scope.executor()).await?;
            assert_eq!((authority_after.try_get::<String,_>("lease")?,authority_after.try_get::<String,_>("attempts")?),authority_before);
            Err::<(),_>(storage("rollback damaged revision probe"))
        })).await;
        assert!(result.is_err());
        assert_eq!(
            scalar(&store, "SELECT record AS value FROM _fleet_sync_databases").await?,
            original
        );
        assert_eq!(
            scalar(
                &store,
                "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_renewal_attempts"
            )
            .await?,
            "0"
        );
        assert_eq!(
            required_snapshot(&store)
                .await?
                .grant
                .as_ref()
                .map(|grant| grant.expires_at),
            Some(SyncTime::from_unix_millis(3000))
        );
    }
    Ok(())
}
