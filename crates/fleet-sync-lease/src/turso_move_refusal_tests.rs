//! Typed release answers drive durable refusals; a release ahead of the copy waits for real rows.
//! Release itself is the receipted writer stand-in, not the D2 writer implementation.
#![allow(clippy::panic_in_result_fn)]
use crate::{
    turso_change_tests::register_first, turso_copy_fixture::push_rows, turso_registry_fixture::*, *,
};
use durable_steps::{OperationState, Uuid};
use fleet_sync_protocol::*;
use sqlx_turso::Turso;
use std::sync::Arc;

#[tokio::test]
async fn typed_release_refusals_replay_without_fencing_or_changing_the_lease() -> TestResult {
    let cases = [
        (
            ReleaseRefusal::MigrationPending {
                migration: 2,
                state: MigrationState::Pending,
            },
            HolderChangeRefusal::MigrationPending {
                migration: 2,
                state: MigrationState::Pending,
            },
        ),
        (
            ReleaseRefusal::MigrationPending {
                migration: 2,
                state: MigrationState::Failed,
            },
            HolderChangeRefusal::MigrationPending {
                migration: 2,
                state: MigrationState::Failed,
            },
        ),
        (
            ReleaseRefusal::SchemaTooNew {
                store: SchemaIdentity::new(2, "newer-schema".into())?,
                supported: None,
            },
            HolderChangeRefusal::SchemaTooNew {
                store: SchemaIdentity::new(2, "newer-schema".into())?,
                supported: None,
            },
        ),
        (
            ReleaseRefusal::Superseded {
                current: Epoch::new(3)?,
            },
            HolderChangeRefusal::PreviewStale,
        ),
    ];
    for (release, expected) in cases {
        let expected_wire = match &release {
            ReleaseRefusal::MigrationPending { migration, state } => {
                let state = match state {
                    MigrationState::Pending => "pending",
                    MigrationState::Failed => "failed",
                };
                serde_json::json!({"kind":"refused","value":{"kind":"migrationPending","value":{"migration":migration,"state":state}}})
            }
            _ => serde_json::to_value(HolderChangeAnswer::Refused(expected.clone()))?,
        };
        let directory = tempfile::tempdir()?;
        let host = TestHost::open(directory.path(), false).await?;
        let store = open_store(
            &directory.path().join("registry.db"),
            Arc::clone(&host),
            true,
        )
        .await?;
        let engine = engine(Arc::clone(&store), Arc::clone(&host))?;
        register_first(&engine).await?;
        let lease = scalar(&store, "SELECT record AS value FROM _fleet_sync_leases").await?;
        *host.release_answer.lock().await = Some(ReleaseAnswer::ReleaseRefused(release));
        let attempt = TransferId::new(Uuid::from_u128(0x019eef00000070008000000000000641))?;
        let request = HolderChangeRequest::Move {
            attempt,
            database: database()?,
            from_epoch: Epoch::new(1)?,
            to: peer(2)?,
        };
        let key = holder_change_request_key(attempt)?;
        let first =
            task_answer::<HolderChangeTask<TestStore, Turso>>(&engine, &key, request.clone())
                .await?;
        assert_eq!(first, HolderChangeAnswer::Refused(expected.clone()));
        assert_eq!(serde_json::to_value(&first)?, expected_wire);
        let changed_state = match &expected {
            HolderChangeRefusal::MigrationPending {
                state: MigrationState::Failed,
                ..
            } => MigrationState::Pending,
            _ => MigrationState::Failed,
        };
        *host.release_answer.lock().await = Some(ReleaseAnswer::ReleaseRefused(
            ReleaseRefusal::MigrationPending {
                migration: 999,
                state: changed_state,
            },
        ));
        host.advance();
        let repeated =
            task_answer::<HolderChangeTask<TestStore, Turso>>(&engine, &key, request).await?;
        assert_eq!(repeated, HolderChangeAnswer::Refused(expected));
        assert_eq!(serde_json::to_value(repeated)?, expected_wire);
        assert_eq!(
            scalar(&store, "SELECT record AS value FROM _fleet_sync_leases").await?,
            lease
        );
        assert_eq!(
            scalar(
                &store,
                "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_epoch_history"
            )
            .await?,
            "1"
        );
        assert_eq!(
            host.calls
                .lock()
                .await
                .iter()
                .filter(|(kind, _)| kind == "fleet-sync.holder-release")
                .count(),
            1
        );
        assert!(
            host.calls
                .lock()
                .await
                .iter()
                .all(|(kind, _)| kind != "fleet-sync.copy-fence")
        );
    }
    Ok(())
}

#[tokio::test]
async fn an_already_held_claim_replays_its_recorded_holder_after_takeover_and_restart() -> TestResult
{
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("registry.db");
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(&path, host.clone(), true).await?;
    let running = engine(store.clone(), host.clone())?;
    register_first(&running).await?;
    let attempt = TransferId::new(Uuid::from_u128(0x019eef00000070008000000000000901))?;
    let claim = HolderChangeRequest::Claim {
        attempt,
        database: database()?,
        claimant: peer(2)?,
    };
    let key = holder_change_request_key(attempt)?;
    let expected = serde_json::json!({"kind":"refused","value":{"kind":"alreadyHeld","value":{"holder":peer(1)?.to_string()}}});
    let refused =
        task_answer::<HolderChangeTask<TestStore, Turso>>(&running, &key, claim.clone()).await?;
    assert_eq!(serde_json::to_value(&refused)?, expected);
    let recorded = scalar(&store, "SELECT outcome AS value FROM _fleet_sync_transfers").await?;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&recorded)?,
        expected
    );
    let preview = task_answer::<PreviewTakeoverTask<TestStore, Turso>>(
        &running,
        &durable_steps::RequestKey::new("refused-claim-takeover")?,
        PreviewTakeover {
            database: database()?,
        },
    )
    .await?;
    let takeover_attempt = TransferId::new(Uuid::from_u128(0x019eef00000070008000000000000902))?;
    let takeover = HolderChangeRequest::Takeover {
        attempt: takeover_attempt,
        database: database()?,
        preview,
        to: peer(3)?,
    };
    let answer = task_answer::<HolderChangeTask<TestStore, Turso>>(
        &running,
        &holder_change_request_key(takeover_attempt)?,
        takeover,
    )
    .await?;
    assert!(matches!(answer, HolderChangeAnswer::Granted { .. }));
    let current: serde_json::Value = serde_json::from_str(
        &scalar(&store, "SELECT record AS value FROM _fleet_sync_leases").await?,
    )?;
    assert_eq!(
        current.get("holder"),
        Some(&serde_json::json!(peer(3)?.to_string()))
    );
    let replay =
        task_answer::<HolderChangeTask<TestStore, Turso>>(&running, &key, claim.clone()).await?;
    assert_eq!(serde_json::to_value(replay)?, expected);
    drop(running);
    drop(store);
    let reopened = open_store(&path, host.clone(), false).await?;
    let resumed = engine(reopened, host)?;
    let replay = task_answer::<HolderChangeTask<TestStore, Turso>>(&resumed, &key, claim).await?;
    assert_eq!(serde_json::to_value(replay)?, expected);
    Ok(())
}

#[tokio::test]
async fn released_final_position_waits_for_the_real_copy_then_grants_once() -> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), true).await?;
    let store = open_store(
        &directory.path().join("registry.db"),
        Arc::clone(&host),
        true,
    )
    .await?;
    let engine = engine(Arc::clone(&store), Arc::clone(&host))?;
    register_first(&engine).await?;
    let final_position = ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(9));
    *host.release_answer.lock().await = Some(ReleaseAnswer::Released { final_position });
    let attempt = TransferId::new(Uuid::from_u128(0x019eef00000070008000000000000642))?;
    let request = HolderChangeRequest::Move {
        attempt,
        database: database()?,
        from_epoch: Epoch::new(1)?,
        to: peer(2)?,
    };
    let key = holder_change_request_key(attempt)?;
    let id = engine
        .start::<HolderChangeTask<TestStore, Turso>>(&key, request.clone())
        .await?;
    assert!(matches!(
        engine.status(&id).await?.state,
        OperationState::Running { .. }
    ));
    assert_eq!(
        scalar(
            &store,
            "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_epoch_history"
        )
        .await?,
        "1"
    );
    assert_eq!(push_rows(&host, 9).await?, final_position);
    host.advance();
    assert_eq!(engine.drive_due().await?.completed, vec![id]);
    let answer =
        task_answer::<HolderChangeTask<TestStore, Turso>>(&engine, &key, request.clone()).await?;
    assert!(
        matches!(answer,HolderChangeAnswer::Granted{epoch,start,..} if epoch==Epoch::new(2)? && start==final_position)
    );
    host.advance();
    assert_eq!(
        task_answer::<HolderChangeTask<TestStore, Turso>>(&engine, &key, request).await?,
        answer
    );
    let calls = host
        .calls
        .lock()
        .await
        .iter()
        .filter(|(kind, _)| kind == "fleet-sync.copy-acknowledged")
        .map(|(_, call)| call.clone())
        .collect::<Vec<_>>();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls.first(), calls.last());
    assert_eq!(
        scalar(
            &store,
            "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_epoch_history"
        )
        .await?,
        "2"
    );
    Ok(())
}
