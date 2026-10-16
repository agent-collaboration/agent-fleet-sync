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
                    include_str!("../migrations/0002_replicated_schema.sql")
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
                let seed = seed_begin(scope, database, durable_steps::Uuid::from_u128(2001))
                    .await
                    .map_err(storage)?;
                let sql = "CREATE TABLE records(id INTEGER PRIMARY KEY,body TEXT)";
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
                    SqlValue::Integer(1),
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
                if seed_page(scope, &page).await.map_err(storage)? != SeedPageAnswer::Staged {
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
                match seed_complete(scope, &done, &*hook).await {
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
