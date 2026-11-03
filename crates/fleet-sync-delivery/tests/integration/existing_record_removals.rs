use super::support::{
    existing_removals::*,
    startup_checkpoint_scope::{CHECKPOINT, StartupCheckpointScope},
    *,
};
use durable_steps::{
    AtomicStore, JournalScope, StoreError,
    storage::{JournalStorage, OperationExecution, TransactionScope},
    turso_store::TursoScope,
};
use fleet_sync_delivery::*;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    query::query,
};
use sqlx_turso::{Turso, TursoConnectOptions};
use std::{
    process::Stdio,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::io::{AsyncBufReadExt, BufReader};

#[tokio::test]
async fn initial_admission_retry_clears_its_private_token_before_reuse() {
    let fixture = legacy_store().await;
    fixture.store.transaction(|scope|Box::pin(async move {
        let item=old_item(10);
        admit_record_removal::<_,RecordRemoval>(scope,item).await.map_err(storage_error)?;
        let key=delivery_request_key(&item).map_err(storage_error)?;
        let id=scope.admit::<RecordRemoval>(&key,&item).await.map_err(StoreError::backend)?.id();
        let before=scope.operation(id).await?.ok_or(StoreError::MissingOperation{id})?;
        let stale=scope.retry(id,id.as_uuid(),SyncTime::from_unix_millis(START),99).await;
        assert!(matches!(stale,Err(StoreError::StaleDriveToken{id:rejected}) if rejected==id));
        assert!(matches!(&before.execution,OperationExecution::Running{next_attempt_at,drive:None,..} if *next_attempt_at==removal_deadline(item)));
        assert_eq!(scope.operation(id).await?,Some(before.clone()));
        admit_record_removal::<_,RecordRemoval>(scope,item).await.map_err(storage_error)?;
        assert_eq!(scope.operation(id).await?,Some(before));
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn startup_backfills_old_copies_once_and_each_keeps_its_creation_deadline() {
    let fixture = legacy_store().await;
    assert_eq!(custody(&fixture.store).await.len(), 3);
    assert!(schedule(&fixture.store).await.is_empty());
    startup(&fixture.store).await;
    let first = schedule(&fixture.store).await;
    assert_eq!(
        first.len(),
        2,
        "inbox/relay duplicates need only one removal operation"
    );
    let deadlines = first
        .iter()
        .map(|(_, _, deadline)| *deadline)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        deadlines,
        std::collections::BTreeSet::from([90 * DAY, 150 * DAY])
    );
    startup(&fixture.store).await;
    assert_eq!(schedule(&fixture.store).await, first);
    let report = engine(&fixture).drive_due().await.unwrap();
    assert_eq!(report.completed.len(), 1);
    assert!(report.failed.is_empty());
    assert_eq!(
        custody(&fixture.store).await,
        vec![old_item(10).as_uuid().to_string()]
    );
    let pending = schedule(&fixture.store).await;
    assert_eq!(pending.len(), 1);
    startup(&fixture.store).await;
    assert_eq!(schedule(&fixture.store).await, pending);
    fixture
        .host
        .clock
        .store(u64::try_from(149 * DAY).unwrap(), Ordering::SeqCst);
    assert!(
        engine(&fixture)
            .drive_due()
            .await
            .unwrap()
            .completed
            .is_empty()
    );
    assert_eq!(custody(&fixture.store).await.len(), 1);
    fixture
        .host
        .clock
        .store(u64::try_from(150 * DAY).unwrap(), Ordering::SeqCst);
    assert_eq!(
        engine(&fixture).drive_due().await.unwrap().completed.len(),
        1
    );
    assert!(custody(&fixture.store).await.is_empty());
    startup(&fixture.store).await;
    assert!(schedule(&fixture.store).await.is_empty());
}

#[tokio::test]
async fn startup_validates_all_keys_before_admitting_any_removal() {
    let fixture = legacy_store().await;
    fixture.store.transaction(|scope|Box::pin(async move {
        query::<Turso>("UPDATE _fleet_sync_outbox_items SET item_id='01900000-0000-4000-8000-000000000050' WHERE item_id=?1").bind(old_item(70).as_uuid().to_string()).execute(scope.executor()).await?;
        Ok(())
    })).await.unwrap();
    let result = fixture
        .store
        .transaction(|scope| {
            Box::pin(
                async move { Ok(admit_existing_record_removals::<_, RecordRemoval>(scope).await) },
            )
        })
        .await
        .unwrap();
    assert!(matches!(
        result,
        Err(DeliveryError::CorruptRow {
            table: "_fleet_sync_outbox_items",
            field: "item_id",
            ..
        })
    ));
    assert!(schedule(&fixture.store).await.is_empty());
}

#[tokio::test]
async fn startup_cleanup_does_not_need_a_decodable_payload() {
    let fixture = legacy_store().await;
    fixture.store.transaction(|scope|Box::pin(async move {
        query::<Turso>("UPDATE _fleet_sync_outbox_items SET payload='corrupt',payload_bytes=7 WHERE item_id=?1").bind(old_item(70).as_uuid().to_string()).execute(scope.executor()).await?;
        Ok(())
    })).await.unwrap();
    startup(&fixture.store).await;
    assert_eq!(
        engine(&fixture).drive_due().await.unwrap().completed.len(),
        1
    );
    assert_eq!(
        custody(&fixture.store).await,
        vec![old_item(10).as_uuid().to_string()]
    );
}

#[tokio::test]
async fn startup_backfill_child_checkpoint() -> Result<(), Box<dyn std::error::Error>> {
    let Some(path) = std::env::var_os("FLEET_REMOVAL_STARTUP_CHILD") else {
        return Ok(());
    };
    let host = host_at(SyncTime::from_unix_millis(START));
    let mut connection = TursoConnectOptions::new()
        .filename(path)
        .foreign_keys(true)
        .connect()
        .await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let mut real = TursoScope::new(&mut transaction, host.as_ref());
    let mut scope = StartupCheckpointScope { inner: &mut real };
    admit_existing_record_removals::<_, RecordRemoval>(&mut scope).await?;
    Err("startup backfill returned without the causal admission checkpoint".into())
}

#[cfg(unix)]
#[tokio::test]
#[allow(
    clippy::panic_in_result_fn,
    reason = "Assertions are the failure oracle in this integration test"
)]
async fn sigkill_during_startup_backfill_restarts_without_partial_scheduling()
-> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::process::ExitStatusExt;
    let Fixture {
        store,
        host,
        directory,
    } = legacy_store().await;
    let before = custody(&store).await;
    drop(store);
    let path = directory.path().join("delivery.db");
    // The current integration binary was already signed by the Cargo runner.
    let mut child = tokio::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "existing_record_removals::startup_backfill_child_checkpoint",
            "--nocapture",
        ])
        .env("FLEET_REMOVAL_STARTUP_CHILD", &path)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?;
    let stdout = child.stdout.take().ok_or("child stdout was not piped")?;
    let mut lines = BufReader::new(stdout).lines();
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
        "child exited before its real admission checkpoint"
    );
    assert_eq!(status.signal(), Some(9));
    let store = Arc::new(TestStore::open_and_migrate(&path, host.clone()).await?);
    assert_eq!(custody(&store).await, before);
    assert!(
        schedule(&store).await.is_empty(),
        "uncommitted scheduling survived the child crash"
    );
    startup(&store).await;
    assert_eq!(schedule(&store).await.len(), 2);
    let fixture = Fixture {
        store,
        host,
        directory,
    };
    assert_eq!(engine(&fixture).drive_due().await?.completed.len(), 1);
    assert_eq!(
        custody(&fixture.store).await,
        vec![old_item(10).as_uuid().to_string()]
    );
    Ok(())
}
