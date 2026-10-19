//! Each mutator's real database transaction rolls the revision and facts back together.
#![allow(clippy::panic_in_result_fn)]
use crate::{
    turso_epoch_history_fixture::registered, turso_registry_fixture::*,
    turso_registry_snapshot_fixture::*, turso_workflow_tests::attempt, *,
};
use durable_steps::{AtomicStore, StoreError};
use fleet_sync_protocol::*;
use sqlx_turso::Turso;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

#[tokio::test]
async fn registry_snapshot_renewal_and_its_attempt_receipt_roll_back_with_the_revision()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let (_, store, _engine) = registered(directory.path()).await?;
    let before = required_snapshot(&store).await?;
    let request = RenewRequest {
        attempt: import_request(4200, "a")?.attempt,
        database: database()?,
        epoch: Epoch::new(1)?,
    };
    let holder = peer(1)?;
    let error = store
        .transaction(move |scope| {
            Box::pin(async move {
                assert!(matches!(
                    renew(
                        scope,
                        &request,
                        &holder,
                        SyncTime::from_unix_millis(1500),
                        &LeasePolicy {
                            lease_duration: Duration::from_millis(2000),
                            renew_interval: Duration::from_millis(500)
                        }
                    )
                    .await
                    .map_err(storage)?,
                    RenewAnswer::Renewed { .. }
                ));
                let during = registry_snapshot(scope, request.database)
                    .await
                    .map_err(storage)?
                    .ok_or_else(|| storage("missing in-transaction snapshot"))?;
                assert_eq!(during.revision.change.get(), 2);
                assert_eq!(during.changed_at, SyncTime::from_unix_millis(1500));
                Err::<(), _>(StoreError::Aborted {
                    reason: "injected after actual renewal before commit".into(),
                })
            })
        })
        .await;
    assert!(matches!(error, Err(StoreError::Aborted { .. })));
    assert_eq!(required_snapshot(&store).await?, before);
    assert_eq!(
        scalar(
            &store,
            "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_renewal_attempts"
        )
        .await?,
        "0"
    );
    renew_at(&store, 4200, 1500, 2000).await?;
    assert_revision(&required_snapshot(&store).await?, 1, 2, 1500);
    Ok(())
}

#[tokio::test]
async fn registry_snapshot_lifecycle_and_holder_mutators_rollback_then_commit_one_revision()
-> TestResult {
    for mutation in [
        "registration",
        "import",
        "move",
        "claim",
        "retiring",
        "retired",
    ] {
        let directory = tempfile::tempdir()?;
        let host = TestHost::open(directory.path(), false).await?;
        let store = open_store(
            &directory.path().join("registry.db"),
            Arc::clone(&host),
            true,
        )
        .await?;
        let setup = engine(Arc::clone(&store), Arc::clone(&host))?;
        let retirement = Retirement {
            attempt: import_request(4202, "a")?.attempt,
            database: database()?,
            expect_fence: Epoch::new(1)?,
            expect_acked: ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(0)),
        };
        let mut retire_request = retirement;
        match mutation {
            "move" => crate::turso_change_tests::register_first(&setup).await?,
            "claim" | "retiring" | "retired" => {
                let request = import_request(4201, "a")?;
                assert_eq!(
                    task_answer::<ImportTask<TestStore, Turso>>(
                        &setup,
                        &lifecycle_request_key(request.attempt)?,
                        request
                    )
                    .await?,
                    ImportAnswer::Imported
                );
                retire_request.expect_acked = crate::turso_copy_fixture::seed_import(&host).await?;
            }
            _ => {}
        }
        if mutation == "retired" {
            host.retire_unreachable.store(true, Ordering::SeqCst);
            setup
                .start::<RetirementTask<TestStore, Turso>>(
                    &lifecycle_request_key(retire_request.attempt)?,
                    retire_request.clone(),
                )
                .await?;
            assert!(matches!(
                required_snapshot(&store).await?.lifecycle,
                DatabaseLifecycle::Retiring { .. }
            ));
        }
        let before = snapshot(&store).await?;
        let (epoch, change) = match mutation {
            "move" | "claim" => (2, 1),
            "retiring" => (1, 2),
            "retired" => (1, 3),
            _ => (1, 1),
        };
        let fault = rollback_store(Arc::clone(&store), epoch, change)?;
        let faulty = rollback_engine(Arc::clone(&fault), Arc::clone(&host))?;
        let registration = registration(4203, 1)?;
        let import = import_request(4204, "a")?;
        let change_id = attempt(4205)?;
        if mutation == "move" || mutation == "claim" {
            let request = if mutation == "move" {
                HolderChangeRequest::Move {
                    reason: fleet_sync_protocol::HolderChangeReason::new("exercise holder change")?,
                    attempt: change_id,
                    database: database()?,
                    from_epoch: Epoch::new(1)?,
                    to: peer(2)?,
                }
            } else {
                HolderChangeRequest::Claim {
                    attempt: change_id,
                    database: database()?,
                    claimant: peer(2)?,
                }
            };
            store
                .transaction(move |scope| {
                    Box::pin(async move {
                        admit_holder_change::<RollbackRegistryStore, Turso>(
                            scope,
                            &request,
                            &ActorRef::new("snapshot-admin").map_err(storage)?,
                            SyncTime::from_unix_millis(1000),
                        )
                        .await
                        .map_err(storage)
                    })
                })
                .await?;
        }
        match mutation {
            "registration" => {
                let _ = faulty
                    .start::<RegistrationTask<RollbackRegistryStore, Turso>>(
                        &lifecycle_request_key(registration.attempt)?,
                        registration.clone(),
                    )
                    .await;
            }
            "import" => {
                let _ = faulty
                    .start::<ImportTask<RollbackRegistryStore, Turso>>(
                        &lifecycle_request_key(import.attempt)?,
                        import.clone(),
                    )
                    .await;
            }
            "retiring" => {
                let _ = faulty
                    .start::<RetirementTask<RollbackRegistryStore, Turso>>(
                        &lifecycle_request_key(retire_request.attempt)?,
                        retire_request.clone(),
                    )
                    .await;
            }
            "retired" => {
                host.retire_unreachable.store(false, Ordering::SeqCst);
                host.advance();
                let _ = faulty.drive_due().await;
            }
            _ => {
                let _ = faulty.drive_due().await;
            }
        }
        assert!(
            fault.rollbacks.load(Ordering::SeqCst) > 0,
            "did not reach {mutation} commit boundary"
        );
        assert_eq!(
            snapshot(&store).await?,
            before,
            "escaped rollback for {mutation}"
        );
        fault.fail.store(false, Ordering::SeqCst);
        host.advance();
        faulty.drive_due().await?;
        let committed = required_snapshot(&store).await?;
        let expected_change = if mutation == "retiring" { 3 } else { change };
        assert_eq!(committed.revision.epoch, Epoch::new(epoch)?, "{mutation}");
        assert_eq!(
            committed.revision.change.get(),
            expected_change,
            "{mutation}"
        );
        assert_ne!(Some(committed.clone()), before, "{mutation} did not commit");
        drop(faulty);
        drop(fault);
        drop(setup);
        drop(store);
        let reopened = open_store(&directory.path().join("registry.db"), host, false).await?;
        assert_eq!(required_snapshot(&reopened).await?, committed, "{mutation}");
    }
    Ok(())
}
