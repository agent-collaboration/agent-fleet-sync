//! Capability ordering and admission freezing use the real registry and copy rules.
#![allow(clippy::panic_in_result_fn)]
use crate::{
    turso_change_tests::register_first, turso_copy_fixture::push_rows, turso_registry_fixture::*, *,
};
use durable_steps::{AtomicStore, RequestKey, Uuid};
use fleet_sync_protocol::*;
use sqlx_turso::Turso;

fn report(identity: u128, sequence: u64, maximum: u64) -> TestResult<ReportSchemaSupport> {
    Ok(ReportSchemaSupport {
        peer: peer(identity)?,
        session_id: SessionId::new(Uuid::from_u128(
            0x019eef00000070008000000000001000 | u128::from(sequence),
        ))?,
        attach_seq: AttachSequence::new(sequence)?,
        support: SchemaSupport {
            max_version: maximum,
        },
    })
}
async fn publish(store: &TestStore, report: ReportSchemaSupport) -> TestResult {
    store
        .transaction(move |scope| {
            Box::pin(async move {
                report_schema_support(scope, &report)
                    .await
                    .map_err(storage)?;
                Ok(())
            })
        })
        .await?;
    Ok(())
}
async fn projected(store: &TestStore, target: PeerId) -> TestResult<Option<ReportSchemaSupport>> {
    Ok(store
        .transaction(move |scope| {
            Box::pin(async move {
                accepted_schema_support(scope, target)
                    .await
                    .map_err(storage)
            })
        })
        .await?)
}
async fn answer(store: &TestStore, request: HolderChangeRequest) -> TestResult<HolderChangeAnswer> {
    Ok(store
        .transaction(move |scope| {
            Box::pin(async move {
                holder_change_answer(scope, &request)
                    .await
                    .map_err(storage)?
                    .ok_or_else(|| storage("canonical outcome missing"))
            })
        })
        .await?)
}

#[tokio::test]
async fn newer_attach_wins_even_when_an_older_report_arrives_late_and_after_reopen() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("registry.db");
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(&path, host.clone(), true).await?;
    let newest = report(2, 3, 4)?;
    publish(&store, newest.clone()).await?;
    publish(&store, report(2, 2, 5)?).await?;
    publish(&store, newest.clone()).await?;
    assert_eq!(projected(&store, peer(2)?).await?, Some(newest.clone()));
    let conflict = report(2, 3, 99)?;
    let refused = store
        .transaction(move |scope| {
            Box::pin(async move { Ok(report_schema_support(scope, &conflict).await) })
        })
        .await?;
    assert!(matches!(refused, Err(LeaseError::InvalidInput { .. })));
    assert_eq!(projected(&store, peer(2)?).await?, Some(newest.clone()));
    drop(store);
    let reopened = open_store(&path, host, false).await?;
    publish(&reopened, report(2, 1, 100)?).await?;
    assert_eq!(projected(&reopened, peer(2)?).await?, Some(newest));
    Ok(())
}

#[tokio::test]
async fn admission_freezes_support_before_check_even_across_registry_reopen() -> TestResult {
    for restart in [false, true] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("registry.db");
        let host = TestHost::open(directory.path(), true).await?;
        let store = open_store(&path, host.clone(), true).await?;
        let running = engine(store.clone(), host.clone())?;
        register_first(&running).await?;
        let through = push_rows(&host, 9).await?;
        publish(&store, report(2, 2, 1)?).await?;
        let attempt = TransferId::new(Uuid::from_u128(0x019eef00000070008000000000001101))?;
        let change = HolderChangeRequest::Move {
            reason: fleet_sync_protocol::HolderChangeReason::new("exercise holder change")?,
            attempt,
            database: database()?,
            from_epoch: Epoch::new(1)?,
            to: peer(2)?,
        };
        let input = change.clone();
        let now = SyncTime::from_unix_millis(1000);
        store
            .transaction(move |scope| {
                Box::pin(async move {
                    admit_holder_change::<TestStore, Turso>(
                        scope,
                        &input,
                        &ActorRef::new("capability-admission").map_err(storage)?,
                        now,
                    )
                    .await
                    .map_err(storage)?;
                    Ok(())
                })
            })
            .await?;
        publish(&store, report(2, 3, 0)?).await?;
        let (store, running) = if restart {
            drop(running);
            drop(store);
            let reopened = open_store(&path, host.clone(), false).await?;
            let resumed = engine(reopened.clone(), host.clone())?;
            (reopened, resumed)
        } else {
            (store, running)
        };
        running.drive_due().await?;
        assert!(
            matches!(answer(&store, change.clone()).await?, HolderChangeAnswer::Granted { epoch, start, .. } if epoch == Epoch::new(2)? && start == through)
        );
        let admission = store
            .transaction(move |scope| {
                Box::pin(async move {
                    Ok(crate::registry_storage::RegistryStorage(scope)
                        .admission(attempt)
                        .await
                        .map_err(storage)?
                        .ok_or_else(|| storage("admission missing"))?
                        .target_schema)
                })
            })
            .await?;
        assert_eq!(admission, Some(SchemaSupport { max_version: 1 }));
        assert_eq!(
            projected(&store, peer(2)?)
                .await?
                .map(|report| report.support),
            Some(SchemaSupport { max_version: 0 })
        );
        assert!(
            matches!(answer(&store, change).await?, HolderChangeAnswer::Granted { epoch, .. } if epoch == Epoch::new(2)?)
        );
    }
    Ok(())
}

#[tokio::test]
async fn missing_report_is_refused_even_for_schema_zero_then_a_known_zero_target_can_grant()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(&directory.path().join("registry.db"), host.clone(), true).await?;
    let running = engine(store.clone(), host)?;
    register_first(&running).await?;
    assert_eq!(projected(&store, peer(99)?).await?, None);
    let preview = task_answer::<PreviewTakeoverTask<TestStore, Turso>>(
        &running,
        &RequestKey::new("unknown-target")?,
        PreviewTakeover {
            database: database()?,
        },
    )
    .await?;
    let attempt = TransferId::new(Uuid::from_u128(0x019eef00000070008000000000001201))?;
    let change = HolderChangeRequest::Takeover {
        reason: fleet_sync_protocol::HolderChangeReason::new("exercise holder change")?,
        attempt,
        database: database()?,
        preview,
        to: peer(99)?,
    };
    task_answer::<HolderChangeTask<TestStore, Turso>>(
        &running,
        &holder_change_request_key(attempt)?,
        change.clone(),
    )
    .await?;
    assert!(
        matches!(answer(&store, change).await?, HolderChangeAnswer::Refused(HolderChangeRefusal::SchemaTooNew { store, supported: None }) if store.version == 0)
    );
    publish(&store, report(99, 1, 0)?).await?;
    let preview = task_answer::<PreviewTakeoverTask<TestStore, Turso>>(
        &running,
        &RequestKey::new("known-zero-target")?,
        PreviewTakeover {
            database: database()?,
        },
    )
    .await?;
    let attempt = TransferId::new(Uuid::from_u128(0x019eef00000070008000000000001202))?;
    let change = HolderChangeRequest::Takeover {
        reason: fleet_sync_protocol::HolderChangeReason::new("exercise holder change")?,
        attempt,
        database: database()?,
        preview,
        to: peer(99)?,
    };
    task_answer::<HolderChangeTask<TestStore, Turso>>(
        &running,
        &holder_change_request_key(attempt)?,
        change.clone(),
    )
    .await?;
    assert!(
        matches!(answer(&store, change).await?, HolderChangeAnswer::Granted { epoch, .. } if epoch == Epoch::new(3)?)
    );
    Ok(())
}

#[tokio::test]
async fn admitted_absence_stays_absent_when_a_report_appears_before_check() -> TestResult {
    for restart in [false, true] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("registry.db");
        let host = TestHost::open(directory.path(), true).await?;
        let store = open_store(&path, host.clone(), true).await?;
        let running = engine(store.clone(), host.clone())?;
        register_first(&running).await?;
        push_rows(&host, 9).await?;
        assert_eq!(projected(&store, peer(99)?).await?, None);
        let attempt = TransferId::new(Uuid::from_u128(0x019eef00000070008000000000001301))?;
        let change = HolderChangeRequest::Move {
            reason: fleet_sync_protocol::HolderChangeReason::new("exercise holder change")?,
            attempt,
            database: database()?,
            from_epoch: Epoch::new(1)?,
            to: peer(99)?,
        };
        let input = change.clone();
        store
            .transaction(move |scope| {
                Box::pin(async move {
                    admit_holder_change::<TestStore, Turso>(
                        scope,
                        &input,
                        &ActorRef::new("absent-capability-admission").map_err(storage)?,
                        SyncTime::from_unix_millis(1000),
                    )
                    .await
                    .map_err(storage)?;
                    Ok(())
                })
            })
            .await?;
        publish(&store, report(99, 1, 10)?).await?;
        let (store, running) = if restart {
            drop(running);
            drop(store);
            let reopened = open_store(&path, host.clone(), false).await?;
            let resumed = engine(reopened.clone(), host.clone())?;
            (reopened, resumed)
        } else {
            (store, running)
        };
        running.drive_due().await?;
        assert!(
            matches!(answer(&store, change).await?, HolderChangeAnswer::Refused(HolderChangeRefusal::SchemaTooNew { store, supported: None }) if store.version == 1)
        );
        let frozen = store
            .transaction(move |scope| {
                Box::pin(async move {
                    Ok(crate::registry_storage::RegistryStorage(scope)
                        .admission(attempt)
                        .await
                        .map_err(storage)?
                        .ok_or_else(|| storage("admission missing"))?
                        .target_schema)
                })
            })
            .await?;
        assert_eq!(frozen, None);
        assert_eq!(
            projected(&store, peer(99)?)
                .await?
                .map(|report| report.support),
            Some(SchemaSupport { max_version: 10 })
        );
    }
    Ok(())
}
