//! Renewal-age admission is independent of the unsent release timeout.
#![allow(clippy::panic_in_result_fn)]
use crate::{turso_change_tests::register_first, turso_registry_fixture::*, *};
use durable_steps::Uuid;
use fleet_sync_protocol::*;
use sqlx_turso::Turso;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

#[tokio::test]
async fn move_refuses_just_past_renewal_age_without_calling_the_holder() -> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(
        &directory.path().join("registry.db"),
        Arc::clone(&host),
        true,
    )
    .await?;
    let mut configuration = config()?;
    configuration.move_needs_renewal_within = Some(Duration::from_secs(1800));
    let mut engine = TestEngine::new(Arc::clone(&store), Arc::clone(&host));
    engine.register(RegistrationTask::<TestStore, Turso>::new(configuration))?;
    engine.register(HolderChangeTask::<TestStore, Turso>::new(
        configuration,
        host.clone(),
    ))?;
    register_first(&engine).await?;
    let calls = host.calls.lock().await.len();
    let lease = scalar(&store, "SELECT record AS value FROM _fleet_sync_leases").await?;
    host.now.store(1_801_001, Ordering::SeqCst);
    let attempt = TransferId::new(Uuid::from_u128(2501))?;
    let request = HolderChangeRequest::Move {
        attempt,
        database: database()?,
        from_epoch: Epoch::new(1)?,
        to: peer(2)?,
    };
    let key = holder_change_request_key(attempt)?;
    let expected = HolderChangeAnswer::Refused(HolderChangeRefusal::HolderStale {
        last_renewed_at: SyncTime::from_unix_millis(1000),
    });
    assert_eq!(
        task_answer::<HolderChangeTask<TestStore, Turso>>(&engine, &key, request.clone()).await?,
        expected
    );
    host.advance();
    assert_eq!(
        task_answer::<HolderChangeTask<TestStore, Turso>>(&engine, &key, request).await?,
        expected
    );
    assert_eq!(host.calls.lock().await.len(), calls);
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
        scalar(&store, "SELECT epoch AS value FROM _fleet_sync_epoch_floor").await?,
        "1"
    );
    Ok(())
}

#[tokio::test]
async fn move_admits_just_inside_and_exactly_at_the_renewal_age_bound() -> TestResult {
    for now in [1_800_999, 1_801_000] {
        let directory = tempfile::tempdir()?;
        let host = TestHost::open(directory.path(), false).await?;
        let store = open_store(
            &directory.path().join("registry.db"),
            Arc::clone(&host),
            true,
        )
        .await?;
        let mut configuration = config()?;
        configuration.move_needs_renewal_within = Some(Duration::from_secs(1800));
        let mut engine = TestEngine::new(Arc::clone(&store), Arc::clone(&host));
        engine.register(RegistrationTask::<TestStore, Turso>::new(configuration))?;
        engine.register(HolderChangeTask::<TestStore, Turso>::new(
            configuration,
            host.clone(),
        ))?;
        register_first(&engine).await?;
        host.now.store(now, Ordering::SeqCst);
        let attempt = TransferId::new(Uuid::from_u128(2502))?;
        let request = HolderChangeRequest::Move {
            attempt,
            database: database()?,
            from_epoch: Epoch::new(1)?,
            to: peer(2)?,
        };
        let answer = task_answer::<HolderChangeTask<TestStore, Turso>>(
            &engine,
            &holder_change_request_key(attempt)?,
            request,
        )
        .await?;
        assert!(
            matches!(answer,HolderChangeAnswer::Granted{epoch,start,..} if epoch==Epoch::new(2)?&&start==ReplicationPosition::new(Epoch::new(1)?,CommitSequence::new(0)))
        );
        assert_eq!(
            scalar(
                &store,
                "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_epoch_history"
            )
            .await?,
            "2"
        );
    }
    Ok(())
}
