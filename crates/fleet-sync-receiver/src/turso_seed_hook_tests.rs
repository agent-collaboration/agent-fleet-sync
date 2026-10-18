//! Snapshot hook writes, retained piece work, and new follow-ups share the seed's actual commit.
#![allow(clippy::panic_in_result_fn)]
use crate::{turso_follow_up_fixture::*, *};
use durable_steps::{
    AtomicStore, Engine, StoreError as JournalError, storage::TransactionScope,
    turso_store::TursoStore,
};
use fleet_sync_protocol::*;
use sqlx_core::row::Row;
use sqlx_turso::Turso;
use std::sync::{Arc, atomic::Ordering};

async fn staged_seed(store: &TursoStore<TestHost>, hook: Arc<Hook>) -> TestResult<SeedComplete> {
    Ok(store
        .transaction(move |scope| {
            Box::pin(async move {
                sqlx_core::raw_sql::raw_sql(concat!(
                    include_str!("../migrations/0001_copy_rules.sql"),
                    "\n",
                    include_str!("../migrations/0002_replicated_schema.sql"),
                    "\n",
                    include_str!("../migrations/0003_snapshot_install.sql")
                ))
                .execute(scope.executor())
                .await?;
                initialize_hook(scope, &*hook).await.map_err(storage)?;
                let database = database().map_err(storage)?;
                let position = ReplicationPosition::new(
                    Epoch::new(1).map_err(storage)?,
                    CommitSequence::new(7),
                );
                create_copy(
                    scope,
                    &CreateCopy {
                        database,
                        epoch: position.epoch,
                        holder: None,
                        origin: CopyOrigin::AwaitingSeed {
                            expected_position: Some(position),
                        },
                    },
                )
                .await
                .map_err(storage)?;
                let seed = seed_begin(
                    scope,
                    database,
                    SeedAttemptId::new(durable_steps::Uuid::from_u128(
                        0x019eef000000700080000000000007d1,
                    ))
                    .map_err(storage)?,
                    &crate::SeedAuthority::Admin,
                )
                .await
                .map_err(storage)?;
                let sql = "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY,body TEXT)";
                let schema = SchemaIdentity::new(
                    3,
                    schema_fingerprint(&[SchemaEntry {
                        kind: SchemaObjectKind::Table,
                        name: "records".into(),
                        sql: sql.into(),
                    }])
                    .map_err(storage)?,
                )
                .map_err(storage)?;
                let rows = vec![vec![
                    crate::turso_rule_fixture::uuid_key(1),
                    SqlValue::Text("installed record".into()),
                ]];
                let mut digest = TableDigestBuilder::new();
                for row in &rows {
                    digest.push_row(row).map_err(storage)?;
                }
                let contents = SnapshotPage {
                    database,
                    position,
                    schema: schema.clone(),
                    schema_steps: vec![sql.into()],
                    table: "records".into(),
                    rows,
                    next: None,
                };
                let page = SeedPage {
                    seed,
                    page: 0,
                    digest: PageDigest::of(&contents).map_err(storage)?,
                    contents,
                };
                if seed_page(scope, &page, &crate::SeedAuthority::Admin)
                    .await
                    .map_err(storage)?
                    != SeedPageAnswer::Staged
                {
                    return Err(storage("seed page not staged"));
                }
                // A pending piece follow-up retained at this position must not collide with the snapshot's item0.
                admit_follow_ups::<TursoStore<TestHost>, String>(
                    scope,
                    database,
                    position.epoch,
                    position,
                    vec!["earlier-piece".into()],
                )
                .await
                .map_err(storage)?;
                Ok(SeedComplete {
                    database,
                    seed,
                    position,
                    pages: 1,
                    schema,
                    table_digests: vec![(
                        TableName::new("records").map_err(storage)?,
                        digest.finish(),
                    )],
                })
            })
        })
        .await?)
}
async fn publish(
    store: &TursoStore<TestHost>,
    hook: Arc<Hook>,
    done: SeedComplete,
) -> Result<SeedAnswer, JournalError> {
    store
        .transaction(move |scope| {
            Box::pin(async move {
                match seed_complete(scope, &done, &*hook, &crate::SeedAuthority::Admin).await {
                    Ok(SeedDecision::Published {
                        position,
                        follow_ups,
                    }) => {
                        admit_snapshot_follow_ups::<TursoStore<TestHost>, String>(
                            scope,
                            &InstalledSnapshot {
                                database: done.database,
                                position,
                                schema: done.schema,
                            },
                            follow_ups,
                        )
                        .await
                        .map_err(storage)?;
                        Ok(SeedAnswer::Published { position })
                    }
                    Ok(SeedDecision::Answer(answer)) => Ok(answer),
                    Err(ReceiverError::SeedRefused { answer }) => Err(JournalError::Aborted {
                        reason: serde_json::to_string(&answer).map_err(storage)?,
                    }),
                    Err(error) => Err(storage(error)),
                }
            })
        })
        .await
}
async fn committed_counts(store: &TursoStore<TestHost>) -> TestResult<(String, String, String)> {
    Ok(store.transaction(|scope|Box::pin(async move{
        let row=sqlx_core::query::query::<Turso>("SELECT CAST((SELECT count(*) FROM records) AS TEXT) AS records,CAST((SELECT count(*) FROM consumer_applied) AS TEXT) AS hooks,CAST((SELECT count(*) FROM _fleet_sync_operations) AS TEXT) AS operations").fetch_one(scope.executor()).await?;
        Ok((row.try_get("records")?,row.try_get("hooks")?,row.try_get("operations")?))
    })).await?)
}

#[tokio::test]
async fn seed_hook_and_follow_up_run_once_while_retained_piece_work_keeps_its_own_identity()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("copy.db");
    let host = host(&directory.path().join("consumer.db")).await?;
    let store = Arc::new(TursoStore::open_and_migrate(&path, Arc::clone(&host)).await?);
    let done = staged_seed(&store, Arc::clone(&host.hook)).await?;
    assert_eq!(
        publish(&store, Arc::clone(&host.hook), done.clone()).await?,
        SeedAnswer::Published {
            position: done.position
        }
    );
    assert_eq!(
        committed_counts(&store).await?,
        ("1".into(), "1".into(), "2".into())
    );
    assert_eq!(host.hook.snapshot_calls.load(Ordering::SeqCst), 1);
    let projection = store
        .transaction(|scope| {
            Box::pin(async move {
                let row = sqlx_core::query::query::<Turso>("SELECT value FROM consumer_applied")
                    .fetch_one(scope.executor())
                    .await?;
                Ok(row.try_get::<String, _>("value")?)
            })
        })
        .await?;
    assert_eq!(projection, "snapshot:1:7:v3:1");
    assert_eq!(
        publish(&store, Arc::clone(&host.hook), done.clone()).await?,
        SeedAnswer::Published {
            position: done.position
        }
    );
    assert_eq!(host.hook.snapshot_calls.load(Ordering::SeqCst), 1);
    assert_eq!(committed_counts(&store).await?.2, "2");
    let mut engine = Engine::new(Arc::clone(&store), Arc::clone(&host));
    engine.register(ApplyFollowUp::<TursoStore<TestHost>, String>::new())?;
    let first = engine.drive_due().await?;
    assert_eq!(first.completed.len(), 1);
    assert_eq!(first.retried.len(), 1);
    drop(engine);
    drop(store);
    host.now.store(1_000_000, Ordering::SeqCst);
    let store = Arc::new(TursoStore::open_and_migrate(&path, Arc::clone(&host)).await?);
    let mut engine = Engine::new(Arc::clone(&store), Arc::clone(&host));
    engine.register(ApplyFollowUp::<TursoStore<TestHost>, String>::new())?;
    assert_eq!(engine.drive_due().await?.completed.len(), 1);
    assert_eq!(
        publish(&store, Arc::clone(&host.hook), done.clone()).await?,
        SeedAnswer::Published {
            position: done.position
        }
    );
    assert!(engine.drive_due().await?.completed.is_empty());
    assert_eq!(
        committed_counts(&store).await?,
        ("1".into(), "1".into(), "0".into())
    );
    let calls = host.calls.lock().await.clone();
    assert_eq!(calls.len(), 3);
    assert_eq!(calls.first(), calls.last());
    let mut delivered = host.hook.delivered.lock().await;
    let rows = sqlx_core::query::query::<Turso>(
        "SELECT item,CAST(count(*) AS TEXT) AS effects FROM delivered GROUP BY item ORDER BY item",
    )
    .fetch_all(&mut *delivered)
    .await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows.first()
            .ok_or("piece item")?
            .try_get::<String, _>("item")?,
        "earlier-piece"
    );
    assert_eq!(
        rows.last()
            .ok_or("snapshot item")?
            .try_get::<String, _>("item")?,
        "snapshot:1:7:v3:1"
    );
    for row in rows {
        assert_eq!(row.try_get::<String, _>("effects")?, "1");
    }
    Ok(())
}

#[tokio::test]
async fn a_snapshot_hook_refusal_rolls_back_rows_ready_state_hook_writes_and_new_admission()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let host = host(&directory.path().join("consumer.db")).await?;
    let store = Arc::new(
        TursoStore::open_and_migrate(&directory.path().join("copy.db"), Arc::clone(&host)).await?,
    );
    let done = staged_seed(&store, Arc::clone(&host.hook)).await?;
    host.hook.snapshot_refuse.store(true, Ordering::SeqCst);
    let result = publish(&store, Arc::clone(&host.hook), done.clone()).await;
    let Err(JournalError::Aborted { reason }) = result else {
        return Err("snapshot hook did not refuse publication".into());
    };
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&reason)?,
        serde_json::json!({"kind":"hook","value":{"code":"snapshot_policy","detail":"snapshot projection refused"}})
    );
    assert_eq!(host.hook.snapshot_calls.load(Ordering::SeqCst), 1);
    store.transaction(|scope|Box::pin(async move {
        assert_eq!(copy_status(scope).await.map_err(storage)?,CopyStatusAnswer::NotInitialized);
        let row=sqlx_core::query::query::<Turso>("SELECT CAST((SELECT count(*) FROM sqlite_schema WHERE name='records') AS TEXT) AS tables,CAST((SELECT count(*) FROM consumer_applied) AS TEXT) AS hooks,CAST((SELECT count(*) FROM _fleet_sync_operations) AS TEXT) AS operations,CAST((SELECT count(*) FROM _fleet_sync_seed_pages) AS TEXT) AS pages").fetch_one(scope.executor()).await?;
        assert_eq!(row.try_get::<String,_>("tables")?,"0");assert_eq!(row.try_get::<String,_>("hooks")?,"0");assert_eq!(row.try_get::<String,_>("operations")?,"1");assert_eq!(row.try_get::<String,_>("pages")?,"1");Ok(())
    })).await?;
    host.hook.snapshot_refuse.store(false, Ordering::SeqCst);
    assert_eq!(
        publish(&store, Arc::clone(&host.hook), done.clone()).await?,
        SeedAnswer::Published {
            position: done.position
        }
    );
    assert_eq!(
        committed_counts(&store).await?,
        ("1".into(), "1".into(), "2".into())
    );
    assert_eq!(host.hook.snapshot_calls.load(Ordering::SeqCst), 2);
    Ok(())
}

async fn seed_identity_state(store: &TursoStore<TestHost>) -> TestResult<Vec<String>> {
    Ok(store.transaction(|scope| Box::pin(async move {
        let rows = sqlx_core::query::query::<Turso>("SELECT 'copy:'||record AS value FROM _fleet_sync_copy UNION ALL SELECT 'seed:'||record FROM _fleet_sync_seed UNION ALL SELECT 'attempt:'||attempt||':'||record FROM _fleet_sync_seed_attempts UNION ALL SELECT 'page:'||seed||':'||CAST(page AS TEXT)||':'||digest||':'||payload FROM _fleet_sync_seed_pages UNION ALL SELECT 'creation:'||request FROM _fleet_sync_creation UNION ALL SELECT 'hook:'||value FROM consumer_applied UNION ALL SELECT 'operation:'||operation_id||':'||state||':'||hex(input_digest) FROM _fleet_sync_operations UNION ALL SELECT 'schema:'||type||':'||name||':'||coalesce(sql,'') FROM sqlite_schema ORDER BY value").fetch_all(scope.executor()).await?;
        rows.iter().map(|row| row.try_get("value").map_err(JournalError::from)).collect()
    })).await?)
}

#[derive(Clone, Copy)]
enum SeedIdentityAction {
    Begin(SeedAttemptId),
    Page(u32),
    Complete,
}

async fn reject_stored_seed_database_mismatch(actions: &[SeedIdentityAction]) -> TestResult {
    let directory = tempfile::tempdir()?;
    let host = host(&directory.path().join("consumer.db")).await?;
    let store =
        TursoStore::open_and_migrate(&directory.path().join("copy.db"), Arc::clone(&host)).await?;
    let done = staged_seed(&store, Arc::clone(&host.hook)).await?;
    let seed = done.seed;
    let page = store
        .transaction(move |scope| {
            Box::pin(async move {
                let mut pages = crate::seed_storage::SeedStorage(scope)
                    .pages(seed)
                    .await
                    .map_err(storage)?;
                let page = pages
                    .pop()
                    .ok_or_else(|| storage("missing staged fixture page"))?;
                let row = sqlx_core::query::query::<Turso>("SELECT record FROM _fleet_sync_seed")
                    .fetch_one(scope.executor())
                    .await?;
                let mut record: crate::seed_storage::SeedRecord =
                    serde_json::from_str(&row.try_get::<String, _>("record")?).map_err(storage)?;
                record.database = DatabaseId::new(durable_steps::Uuid::from_u128(
                    0x019eef00000070008000000000000961,
                ))
                .map_err(storage)?;
                sqlx_core::query::query::<Turso>("UPDATE _fleet_sync_seed SET record = ?")
                    .bind(serde_json::to_string(&record).map_err(storage)?)
                    .execute(scope.executor())
                    .await?;
                Ok(page)
            })
        })
        .await?;
    let before = seed_identity_state(&store).await?;

    for action in actions {
        let action = *action;
        let mut page = page.clone();
        let done = done.clone();
        let hook = Arc::clone(&host.hook);
        // Return the typed rule error as data so the enclosing transaction commits.
        let error = store
            .transaction(move |scope| {
                Box::pin(async move {
                    let error = match action {
                        SeedIdentityAction::Begin(attempt) => {
                            seed_begin(scope, done.database, attempt, &SeedAuthority::Admin)
                                .await
                                .err()
                        }
                        SeedIdentityAction::Page(index) => {
                            page.page = index;
                            seed_page(scope, &page, &SeedAuthority::Admin).await.err()
                        }
                        SeedIdentityAction::Complete => {
                            match seed_complete(scope, &done, &*hook, &SeedAuthority::Admin).await {
                                Ok(SeedDecision::Published {
                                    position,
                                    follow_ups,
                                }) => {
                                    admit_snapshot_follow_ups::<TursoStore<TestHost>, String>(
                                        scope,
                                        &InstalledSnapshot {
                                            database: done.database,
                                            position,
                                            schema: done.schema,
                                        },
                                        follow_ups,
                                    )
                                    .await
                                    .map_err(storage)?;
                                    None
                                }
                                Ok(SeedDecision::Answer(_)) => None,
                                Err(error) => Some(error),
                            }
                        }
                    };
                    Ok(error)
                })
            })
            .await?;
        assert_eq!(
            error,
            Some(ReceiverError::Storage(StoreError::Corrupt {
                field: "seed record database".into(),
            }))
        );
        assert_eq!(seed_identity_state(&store).await?, before);
        assert_eq!(host.hook.snapshot_calls.load(Ordering::SeqCst), 0);
        assert_eq!(host.hook.follow_up_calls.load(Ordering::SeqCst), 0);
        store
            .transaction(|scope| {
                Box::pin(async move {
                    assert_eq!(
                        copy_status(scope).await.map_err(storage)?,
                        CopyStatusAnswer::NotInitialized
                    );
                    Ok(())
                })
            })
            .await?;
    }
    Ok(())
}

#[tokio::test]
async fn seed_begin_rejects_another_stored_database_before_attempt_replay_or_generation()
-> TestResult {
    reject_stored_seed_database_mismatch(&[
        SeedIdentityAction::Begin(
            SeedAttemptId::new(durable_steps::Uuid::from_u128(
                0x019eef000000700080000000000007d1,
            ))
            .map_err(storage)?,
        ),
        SeedIdentityAction::Begin(
            SeedAttemptId::new(durable_steps::Uuid::from_u128(
                0x019eef00000070008000000000000962,
            ))
            .map_err(storage)?,
        ),
    ])
    .await
}

#[tokio::test]
async fn seed_page_rejects_another_stored_database_before_page_replay_or_staging() -> TestResult {
    reject_stored_seed_database_mismatch(&[
        SeedIdentityAction::Page(0),
        SeedIdentityAction::Page(1),
    ])
    .await
}

#[tokio::test]
async fn seed_complete_rejects_another_stored_database_before_publication_hook_or_admission()
-> TestResult {
    reject_stored_seed_database_mismatch(&[SeedIdentityAction::Complete]).await
}
