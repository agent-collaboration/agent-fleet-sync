#![allow(clippy::unwrap_used, dead_code)]
use crate::support::*;
use durable_steps::{AtomicStore, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::row::Row;

pub fn local_item(id: u64) -> DeliveryItem<TestTypes> {
    DeliveryItem::new(
        item_id(id),
        "sender".into(),
        Recipient {
            peer: peer(2),
            address: "inbox".into(),
        },
        None,
        "local body".into(),
    )
    .unwrap()
}
pub fn local_cancel(id: u64, target: u64, sender: &str) -> DeliveryCancel<TestTypes> {
    DeliveryCancel::new(
        item_id(id),
        sender.into(),
        Recipient {
            peer: peer(2),
            address: "inbox".into(),
        },
        None,
        CancelBody {
            target: item_id(target),
        },
    )
    .unwrap()
}
pub async fn enqueue_original(
    fixture: &Fixture,
    id: u64,
    limits: DeliveryLimits,
) -> Option<DeliveryAttempt> {
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes>::new(scope, peer(2), limits)
                    .enqueue_local(local_item(id), &TestConsumer, SyncTime::from_unix_millis(0))
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap()
}
pub async fn enqueue_cancel(fixture: &Fixture, id: u64, target: u64) -> LocalCancelAdmission {
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .enqueue_local_cancel(
                        local_cancel(id, target, "sender"),
                        &TestConsumer,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap()
}
pub async fn outcome(fixture: &Fixture, id: u64) -> Option<RecipientOutcome<Acceptance>> {
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .local_outcome(item_id(id))
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap()
}
#[derive(Debug, PartialEq)]
pub struct LocalSnapshot {
    pub custody: Vec<(String, Option<String>)>,
    pub links: Vec<(String, String)>,
    pub state: Vec<String>,
    pub reports: Vec<String>,
    pub live_bytes: i64,
}
pub async fn snapshot(fixture: &Fixture) -> LocalSnapshot {
    fixture.store.transaction(|scope| Box::pin(async move {
        let custody=sqlx_core::query::query::<sqlx_turso::Turso>("SELECT record,payload FROM _fleet_sync_inbox_items ORDER BY item_id").fetch_all(scope.executor()).await?.into_iter().map(|row| Ok((row.try_get("record")?,row.try_get("payload")?))).collect::<Result<Vec<_>,sqlx_core::Error>>()?;
        let links=sqlx_core::query::query::<sqlx_turso::Turso>("SELECT target,cancel_item FROM _fleet_sync_inbox_cancel_links ORDER BY target").fetch_all(scope.executor()).await?.into_iter().map(|row| Ok((row.try_get("target")?,row.try_get("cancel_item")?))).collect::<Result<Vec<_>,sqlx_core::Error>>()?;
        let state=sqlx_core::query::query::<sqlx_turso::Turso>("SELECT record FROM _fleet_sync_inbox_state").fetch_all(scope.executor()).await?.into_iter().map(|row| row.try_get("record")).collect::<Result<Vec<_>,_>>()?;
        let reports=sqlx_core::query::query::<sqlx_turso::Turso>("SELECT record FROM _fleet_sync_reports ORDER BY item_id").fetch_all(scope.executor()).await?.into_iter().map(|row| row.try_get("record")).collect::<Result<Vec<_>,_>>()?;
        let row=sqlx_core::query::query::<sqlx_turso::Turso>("SELECT COALESCE(SUM(CASE WHEN live=1 THEN payload_bytes+settlement_bytes ELSE 0 END),0) AS bytes FROM _fleet_sync_inbox_items").fetch_one(scope.executor()).await?;
        Ok(LocalSnapshot { custody, links, state, reports, live_bytes: row.try_get("bytes")? })
    })).await.unwrap()
}
pub fn encoded_cancel_bytes(id: u64, target: u64) -> u64 {
    serde_json::to_vec(&DeliveryEnvelope::Cancel(local_cancel(
        id, target, "sender",
    )))
    .unwrap()
    .len()
    .try_into()
    .unwrap()
}
pub fn encoded_original_bytes(id: u64) -> u64 {
    serde_json::to_vec(&DeliveryEnvelope::Item(local_item(id)))
        .unwrap()
        .len()
        .try_into()
        .unwrap()
}
pub async fn reopen(fixture: Fixture) -> Fixture {
    let Fixture {
        store,
        host,
        directory,
    } = fixture;
    drop(store);
    let store = std::sync::Arc::new(
        TestStore::open_and_migrate(directory.path().join("delivery.db"), host.clone())
            .await
            .unwrap(),
    );
    Fixture {
        store,
        host,
        directory,
    }
}
pub async fn assert_no_pending_operations(fixture: &Fixture) {
    let engine = durable_steps::Engine::new(fixture.store.clone(), fixture.host.clone());
    let report = engine.drive_due().await.unwrap();
    assert!(
        report.completed.is_empty()
            && report.failed.is_empty()
            && report.retried.is_empty()
            && report.yielded.is_empty(),
        "{report:?}"
    );
}
pub fn assert_no_remote_activity(snapshot: &LocalSnapshot) {
    assert!(
        snapshot.state.is_empty(),
        "local custody wrote receive/ack state"
    );
    assert!(
        snapshot.reports.is_empty(),
        "local custody created a remote report"
    );
}
