//! Lifecycle commits and replay are observed through one clock-free public snapshot.
#![allow(clippy::panic_in_result_fn)]
use crate::{
    turso_registry_fixture::*, turso_registry_snapshot_fixture::*, turso_workflow_tests::attempt, *,
};
use durable_steps::AtomicStore;
use fleet_sync_protocol::*;
use sqlx_turso::Turso;
use std::sync::{Arc, atomic::Ordering};

#[tokio::test]
async fn registry_snapshot_registration_and_import_replays_preserve_bytes_then_claim_resets_epoch()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(
        &directory.path().join("registry.db"),
        Arc::clone(&host),
        true,
    )
    .await?;
    let engine = engine(Arc::clone(&store), Arc::clone(&host))?;
    let imported = import_request(4100, "a")?;
    let key = lifecycle_request_key(imported.attempt)?;
    assert_eq!(
        task_answer::<ImportTask<TestStore, Turso>>(&engine, &key, imported.clone()).await?,
        ImportAnswer::Imported
    );
    let first = required_snapshot(&store).await?;
    assert_revision(&first, 1, 1, 1000);
    assert_eq!(
        first.lifecycle,
        DatabaseLifecycle::Unheld {
            import_digest: imported.digest.clone()
        }
    );
    assert_eq!(first.grant, None);
    host.now.store(1200, Ordering::SeqCst);
    assert_eq!(
        task_answer::<ImportTask<TestStore, Turso>>(&engine, &key, imported).await?,
        ImportAnswer::Imported
    );
    let same = import_request(4101, "a")?;
    assert_eq!(
        task_answer::<ImportTask<TestStore, Turso>>(
            &engine,
            &lifecycle_request_key(same.attempt)?,
            same
        )
        .await?,
        ImportAnswer::AlreadyImported
    );
    assert_eq!(
        serde_json::to_vec(&required_snapshot(&store).await?)?,
        serde_json::to_vec(&first)?
    );
    let position = crate::turso_copy_fixture::seed_import(&host).await?;
    host.now.store(1500, Ordering::SeqCst);
    let id = attempt(4102)?;
    let request = HolderChangeRequest::Claim {
        attempt: id,
        database: database()?,
        claimant: peer(2)?,
    };
    assert!(
        matches!(task_answer::<HolderChangeTask<TestStore,Turso>>(&engine,&holder_change_request_key(id)?,request).await?,HolderChangeAnswer::Granted {epoch,start,..} if epoch==Epoch::new(2)? && start==position)
    );
    let claimed = required_snapshot(&store).await?;
    assert_revision(&claimed, 2, 1, 1500);
    assert!(matches!(
        claimed.lifecycle,
        DatabaseLifecycle::Registered {
            imported_from: Some(_),
            ..
        }
    ));
    assert_eq!(
        claimed.grant,
        Some(RegistryGrant {
            epoch: Epoch::new(2)?,
            holder: peer(2)?,
            expires_at: SyncTime::from_unix_millis(3500)
        })
    );
    Ok(())
}

#[tokio::test]
async fn registry_snapshot_registration_replay_and_another_same_writer_attempt_do_not_version()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let (host, store, engine) =
        crate::turso_epoch_history_fixture::registered(directory.path()).await?;
    let before = required_snapshot(&store).await?;
    host.now.store(1500, Ordering::SeqCst);
    for identity in [201, 4103] {
        let request = registration(identity, 1)?;
        assert!(matches!(
            task_answer::<RegistrationTask<TestStore, Turso>>(
                &engine,
                &lifecycle_request_key(request.attempt)?,
                request
            )
            .await?,
            RegistrationAnswer::Registered { .. }
        ));
        assert_eq!(required_snapshot(&store).await?, before);
    }
    Ok(())
}

#[tokio::test]
async fn registry_snapshot_retiring_and_retired_survive_reopen_and_only_repair_enumerates_retired()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    let path = directory.path().join("registry.db");
    let store = open_store(&path, Arc::clone(&host), true).await?;
    let running = engine(Arc::clone(&store), Arc::clone(&host))?;
    let import = import_request(4104, "a")?;
    task_answer::<ImportTask<TestStore, Turso>>(
        &running,
        &lifecycle_request_key(import.attempt)?,
        import,
    )
    .await?;
    assert_revision(&required_snapshot(&store).await?, 1, 1, 1000);
    let position = crate::turso_copy_fixture::seed_import(&host).await?;
    host.now.store(1500, Ordering::SeqCst);
    host.retire_unreachable.store(true, Ordering::SeqCst);
    let request = Retirement {
        attempt: import_request(4105, "a")?.attempt,
        database: database()?,
        expect_fence: Epoch::new(1)?,
        expect_acked: position,
    };
    let operation = running
        .start::<RetirementTask<TestStore, Turso>>(
            &lifecycle_request_key(request.attempt)?,
            request.clone(),
        )
        .await?;
    let retiring = required_snapshot(&store).await?;
    assert_revision(&retiring, 1, 2, 1500);
    assert_eq!(
        retiring.lifecycle,
        DatabaseLifecycle::Retiring {
            attempt: request.attempt
        }
    );
    assert_eq!(retiring.grant, None);
    drop(running);
    drop(store);
    let store = open_store(&path, Arc::clone(&host), false).await?;
    assert_eq!(required_snapshot(&store).await?, retiring);
    assert_eq!(
        store
            .transaction(|scope| Box::pin(async move {
                registry_snapshot_ids(scope).await.map_err(storage)
            }))
            .await?,
        vec![database()?]
    );
    host.retire_unreachable.store(false, Ordering::SeqCst);
    host.now.store(2000, Ordering::SeqCst);
    host.advance();
    let finished_at = host.now.load(Ordering::SeqCst);
    let resumed = engine(Arc::clone(&store), Arc::clone(&host))?;
    assert!(resumed.drive_due().await?.completed.contains(&operation));
    let retired = required_snapshot(&store).await?;
    assert_revision(&retired, 1, 3, finished_at);
    assert_eq!(
        retired.lifecycle,
        DatabaseLifecycle::Retired {
            at: SyncTime::from_unix_millis(finished_at)
        }
    );
    assert_eq!(retired.grant, None);
    let ids = store
        .transaction(|scope| {
            Box::pin(async move {
                Ok((
                    registry_snapshot_ids(scope).await.map_err(storage)?,
                    database_ids(scope).await.map_err(storage)?,
                ))
            })
        })
        .await?;
    assert_eq!(ids.0, vec![database()?]);
    assert!(ids.1.is_empty());
    assert!(matches!(
        task_answer::<RetirementTask<TestStore, Turso>>(
            &resumed,
            &lifecycle_request_key(request.attempt)?,
            request
        )
        .await?,
        RetirementAnswer::Retired { .. }
    ));
    assert_eq!(required_snapshot(&store).await?, retired);
    drop(resumed);
    drop(store);
    let reopened = open_store(&path, host, false).await?;
    assert_eq!(required_snapshot(&reopened).await?, retired);
    Ok(())
}
