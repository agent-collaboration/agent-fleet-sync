use super::support::{legacy_submitters::*, *};
use durable_steps::{AtomicStore, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::sync::Arc;

#[tokio::test]
async fn released_consumer_sources_map_exactly_and_completion_is_a_cheap_stable_noop() {
    let fixture = legacy_store(0).await;
    assert_eq!(startup(&fixture.store).await.unwrap(), 4);
    fixture.store.transaction(|scope|Box::pin(async move {
        for (table,identity,owner) in [
            ("_fleet_sync_outbox_items",1,Submitter::Machine(peer(1))),
            ("_fleet_sync_outbox_items",2,project()),
            ("_fleet_sync_inbox_items",3,Submitter::Machine(peer(1))),
            ("_fleet_sync_inbox_items",4,project()),
        ] {
            let sql=format!("SELECT submitter_kind,submitter_id,consumer_sender,created_at FROM {table} WHERE item_id=?1");
            let row=query::<Turso>(sqlx_core::sql_str::AssertSqlSafe(sql)).bind(item_id(identity).as_uuid().to_string()).fetch_one(scope.executor()).await?;
            assert_eq!(row.try_get::<String,_>("submitter_kind")?,match owner {Submitter::Machine(_)=>"machine",Submitter::Project(_)=>"project"});
            assert_eq!(row.try_get::<String,_>("submitter_id")?,owner.as_uuid().to_string());
            let source:String=serde_json::from_str(&row.try_get::<String,_>("consumer_sender")?).map_err(StoreError::backend)?;
            assert_eq!(source,owned_sender(owner,"legacy/session"));
            assert_eq!(row.try_get::<i64,_>("created_at")?,BIRTH);
        }
        Ok(())
    })).await.unwrap();
    let first = snapshot(&fixture.store).await;
    assert_eq!(startup(&fixture.store).await.unwrap(), 0);
    assert_eq!(snapshot(&fixture.store).await, first);
    // Completion must stop scanning, not silently repair later corruption.
    fixture.store.transaction(|scope|Box::pin(async move {
        query::<Turso>("UPDATE _fleet_sync_outbox_items SET submitter_kind=NULL,submitter_id=NULL WHERE item_id=?1").bind(item_id(1).as_uuid().to_string()).execute(scope.executor()).await?;
        Ok(())
    })).await.unwrap();
    let corrupted = snapshot(&fixture.store).await;
    assert_eq!(startup(&fixture.store).await.unwrap(), 0);
    assert_eq!(snapshot(&fixture.store).await, corrupted);
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert!(matches!(
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                        .relay_submitter(item_id(1))
                        .await,
                    Err(DeliveryError::CorruptRow {
                        table: "_fleet_sync_outbox_items",
                        field: "submitter",
                        ..
                    })
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn undecodable_source_names_its_row_and_writes_nothing_even_if_the_caller_commits() {
    let fixture = legacy_store(0).await;
    fixture.store.transaction(|scope|Box::pin(async move {
        query::<Turso>("UPDATE _fleet_sync_outbox_items SET consumer_sender='not JSON' WHERE item_id=?1").bind(item_id(2).as_uuid().to_string()).execute(scope.executor()).await?;
        Ok(())
    })).await.unwrap();
    let before = snapshot(&fixture.store).await;
    let outcome = fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Ok(backfill_submitters::<TestTypes, _, _>(
                    scope,
                    fleet_sync_delivery::DeliveryHostRole::Machine,
                    &FixtureBindings,
                    startup_time(),
                )
                .await)
            })
        })
        .await
        .unwrap();
    assert!(
        matches!(outcome,Err(DeliveryError::CorruptRow {table:"_fleet_sync_outbox_items",key,field:"consumer_sender"}) if key.contains(&item_id(2).as_uuid().to_string()))
    );
    assert_eq!(snapshot(&fixture.store).await, before);
}

#[tokio::test]
async fn handed_off_and_terminal_legacy_rows_keep_status_release_capacity_and_keep_their_deadline()
{
    let fixture = legacy_store(100).await;
    // Include a real compacted terminal row alongside old live/Sent rows.
    fixture.store.transaction(|scope|Box::pin(async move {
        query::<Turso>("UPDATE _fleet_sync_outbox_items SET status_kind='delivered',status_acceptance='{\"receipt\":\"old\"}',live=0,settlement_charge_bytes=0 WHERE item_id=?1")
            .bind(item_id(100).as_uuid().to_string()).execute(scope.executor()).await?;
        Ok(())
    })).await.unwrap();
    assert_eq!(startup(&fixture.store).await.unwrap(), 104);
    fixture.store.transaction(|scope|Box::pin(async move {
        let mut outbox=Outbox::<_,TestTypes, _>::new(scope,limits(), &FixtureBindings);
        assert!(matches!(outbox.status(item_id(101)).await.map_err(storage_error)?.state,OutboxStatusState::Known {status:DeliveryStatus::Sent,version,..} if version.value()==1));
        assert!(matches!(outbox.status(item_id(100)).await.map_err(storage_error)?.state,OutboxStatusState::Known {status:DeliveryStatus::Delivered {..},version,..} if version.value()==1));
        assert!(matches!(outbox.relay_query(Submitter::Machine(peer(1)),item_id(101)).await,Err(DeliveryError::Refused {..})));
        assert!(matches!(outbox.relay_settle::<RecordRemoval>(Submitter::Machine(peer(1)),item_id(101),ItemDigest::try_from("a".repeat(64)).unwrap(),startup_time()).await,Err(DeliveryError::Refused {..})));
        assert!(matches!(outbox.resolve_push(&DeliveryItemRef {item:item_id(101),digest:ItemDigest::try_from("a".repeat(64)).unwrap()}, fleet_sync_protocol::SyncTime::from_unix_millis(0)).await,Err(DeliveryError::Refused {..})));
        let DeliveryEnvelope::Item(item)=item(2000,"new traffic") else {return Err(StoreError::backend("fixture item"));};
        assert!(matches!(outbox.enqueue(item,peer(3),Submitter::Machine(peer(1)),startup_time()).await.map_err(storage_error)?,OutboxAdmission::Queued {..}));
        Ok(())
    })).await.unwrap();
    assert_eq!(
        removal_deadline(item_id(101)).unix_millis(),
        BIRTH + 60 * DAY
    );
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert_eq!(
                    admit_existing_record_removals::<_, RecordRemoval>(
                        scope,
                        fleet_sync_delivery::DeliveryHostRole::Machine,
                        startup_time()
                    )
                    .await
                    .map_err(storage_error)?,
                    105
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    let mut engine = durable_steps::Engine::new(fixture.store.clone(), fixture.host.clone());
    engine
        .register(RecordRemoval::new(
            fleet_sync_delivery::DeliveryHostRole::Machine,
        ))
        .unwrap();
    fixture.host.clock.store(
        u64::try_from(BIRTH + 60 * DAY - 1).unwrap(),
        std::sync::atomic::Ordering::SeqCst,
    );
    assert!(engine.drive_due().await.unwrap().completed.is_empty());
    fixture.host.clock.store(
        u64::try_from(BIRTH + 60 * DAY).unwrap(),
        std::sync::atomic::Ordering::SeqCst,
    );
    let removed = engine.drive_due().await.unwrap();
    assert!(removed.failed.is_empty());
    assert_eq!(removed.completed.len(), 105);
    fixture.store.transaction(|scope|Box::pin(async move {
        let row=query::<Turso>("SELECT count(*) AS count FROM _fleet_sync_outbox_items WHERE legacy_read_only=1").fetch_one(scope.executor()).await?;
        assert_eq!(row.try_get::<i64,_>("count")?,0);
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn backfill_child_checkpoint() -> Result<(), Box<dyn std::error::Error>> {
    use super::support::backfill_checkpoint_scope::BackfillCheckpointScope;
    use durable_steps::turso_store::TursoScope;
    use sqlx_core::connection::{ConnectOptions, Connection};
    let Some(path) = std::env::var_os("FLEET_SUBMITTER_BACKFILL_CHILD") else {
        return Ok(());
    };
    let mut connection = sqlx_turso::TursoConnectOptions::new()
        .filename(path)
        .foreign_keys(true)
        .connect()
        .await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let host = host_at(startup_time());
    let mut real = TursoScope::new(&mut transaction, host.as_ref());
    let mut scope = BackfillCheckpointScope { inner: &mut real };
    backfill_submitters::<TestTypes, _, _>(
        &mut scope,
        fleet_sync_delivery::DeliveryHostRole::Machine,
        &FixtureBindings,
        startup_time(),
    )
    .await?;
    Err("backfill returned without observing an uncommitted mapped row".into())
}

#[cfg(unix)]
#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assert the causal process outcome and durable state in this integration test"
)]
async fn sigkill_after_a_real_mapping_rolls_back_and_the_next_startup_completes()
-> Result<(), Box<dyn std::error::Error>> {
    use super::support::backfill_checkpoint_scope::CHECKPOINT;
    use std::{os::unix::process::ExitStatusExt, process::Stdio, time::Duration};
    use tokio::io::{AsyncBufReadExt, BufReader};
    let Fixture {
        store,
        host,
        directory,
    } = legacy_store(0).await;
    let before = snapshot(&store).await;
    drop(store);
    let path = directory.path().join("delivery.db");
    // The current executable was already signed by the configured Cargo runner.
    let mut child = tokio::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "submitter_backfill::backfill_child_checkpoint",
            "--nocapture",
        ])
        .env("FLEET_SUBMITTER_BACKFILL_CHILD", &path)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?;
    let mut lines = BufReader::new(child.stdout.take().ok_or("missing child stdout")?).lines();
    let observed = tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(line) = lines.next_line().await? {
            if line == CHECKPOINT {
                return Ok::<_, std::io::Error>(true);
            }
        }
        Ok(false)
    })
    .await;
    let killed = child.kill().await;
    let status = child.wait().await?;
    killed?;
    assert!(
        observed??,
        "child exited before observing an uncommitted mapped row"
    );
    assert_eq!(status.signal(), Some(9));
    let store = Arc::new(TestStore::open_and_migrate(&path, host).await?);
    assert_eq!(snapshot(&store).await, before);
    assert_eq!(startup(&store).await?, 4);
    assert_eq!(startup(&store).await?, 0);
    Ok(())
}
