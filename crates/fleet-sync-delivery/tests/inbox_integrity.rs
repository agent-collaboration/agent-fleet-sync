#![allow(clippy::unwrap_used)]
mod support;
use durable_steps::{AtomicStore, SyncTime, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::row::Row;
use support::*;

const PENDING_CANCEL: u64 = 0xabc;

#[derive(Debug, Eq, PartialEq)]
struct InboxReceiveSnapshot {
    headers: Vec<String>,
    reports: Vec<String>,
    receive_state: Vec<String>,
}

async fn receive_snapshot(store: &TestStore) -> InboxReceiveSnapshot {
    store
        .transaction(|scope| {
            Box::pin(async move {
                let headers = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT record FROM _fleet_sync_inbox_items ORDER BY item_id",
                )
                .fetch_all(scope.executor())
                .await?
                .into_iter()
                .map(|row| row.try_get("record"))
                .collect::<Result<Vec<String>, _>>()?;
                let reports = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT record FROM _fleet_sync_reports ORDER BY item_id",
                )
                .fetch_all(scope.executor())
                .await?
                .into_iter()
                .map(|row| row.try_get("record"))
                .collect::<Result<Vec<String>, _>>()?;
                let receive_state = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT record FROM _fleet_sync_inbox_state ORDER BY singleton",
                )
                .fetch_all(scope.executor())
                .await?
                .into_iter()
                .map(|row| row.try_get("record"))
                .collect::<Result<Vec<String>, _>>()?;
                Ok(InboxReceiveSnapshot {
                    headers,
                    reports,
                    receive_state,
                })
            })
        })
        .await
        .unwrap()
}

async fn receive_cancel_before_target(store: &TestStore) {
    store
        .transaction(|scope| {
            Box::pin(async move {
                let mut inbox = Inbox::<_, TestTypes>::new(scope, peer(2), limits());
                let input = inbox
                    .begin_receive(DeliveryStream::Controls)
                    .await
                    .map_err(storage_error)?
                    .unwrap();
                let effects = inbox
                    .store_batch(
                        input,
                        FetchResult {
                            stream: DeliveryStream::Controls,
                            entries: vec![FetchEntry::Item {
                                seq: DeliverySeq::new(1),
                                item: cancel(PENDING_CANCEL, 1),
                            }],
                            through: DeliverySeq::new(1),
                        },
                        &TestConsumer,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                assert!(effects.attempts.is_empty() && effects.reports.is_empty());
                Ok(())
            })
        })
        .await
        .unwrap();
}

async fn receive_original_after_cancel(
    store: &TestStore,
) -> Result<InboxEffects, durable_steps::StoreError> {
    store
        .transaction(|scope| {
            Box::pin(async move {
                let mut inbox = Inbox::<_, TestTypes>::new(scope, peer(2), limits());
                let input = inbox
                    .begin_receive(DeliveryStream::Items)
                    .await
                    .map_err(storage_error)?
                    .unwrap();
                match inbox
                    .store_batch(
                        input,
                        FetchResult {
                            stream: DeliveryStream::Items,
                            entries: vec![FetchEntry::Item {
                                seq: DeliverySeq::new(1),
                                item: item(1, "original after cancel"),
                            }],
                            through: DeliverySeq::new(1),
                        },
                        &TestConsumer,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                {
                    // Preserve the public DeliveryError variant across the store's
                    // error boundary while letting the real transaction roll back.
                    Err(DeliveryError::Corrupt { field }) => {
                        Err(durable_steps::StoreError::Corrupt {
                            field: field.into(),
                            message: "public inbox receive refused corrupt custody".into(),
                        })
                    }
                    result => result.map_err(storage_error),
                }
            })
        })
        .await
}

#[tokio::test]
async fn malformed_pending_cancel_identity_is_refused_without_committed_effects() {
    let cancel_id = item_id(PENDING_CANCEL).as_uuid();
    let canonical = cancel_id.to_string();
    let cases = [
        ("uppercase", canonical.to_uppercase()),
        ("compact", cancel_id.simple().to_string()),
        ("braced", cancel_id.braced().to_string()),
        ("urn", cancel_id.urn().to_string()),
        (
            "canonical non-v7",
            durable_steps::Uuid::from_u128(0x01900000000040008000000000000abc).to_string(),
        ),
    ];
    let mut failures = Vec::new();
    for (name, spelling) in cases {
        assert_ne!(spelling, canonical, "noncanonical control must change text");
        let fixture = Fixture::new().await;
        receive_cancel_before_target(&fixture.store).await;
        fixture
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    let changed = sqlx_core::query::query::<sqlx_turso::Turso>(
                        "UPDATE _fleet_sync_inbox_cancel_links SET cancel_item=?1 WHERE target=?2",
                    )
                    .bind(spelling)
                    .bind(item_id(1).as_uuid().to_string())
                    .execute(scope.executor())
                    .await?;
                    assert_eq!(changed.rows_affected(), 1);
                    Ok(())
                })
            })
            .await
            .unwrap();
        let before = receive_snapshot(&fixture.store).await;
        assert_eq!(before.headers.len(), 1);
        assert!(before.reports.is_empty());
        let result = receive_original_after_cancel(&fixture.store).await;
        let refused = matches!(result, Err(durable_steps::StoreError::Corrupt { ref field, .. }) if field == "inbox cancel identity");
        let after = receive_snapshot(&fixture.store).await;
        println!(
            "OBS pending cancel {name}: Corrupt={refused}, unchanged={}, reports={}",
            before == after,
            after.reports.len()
        );
        if !refused || before != after {
            failures.push(format!("{name}: public receive must return Corrupt and retain exact inbox/report/receive state"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn canonical_pending_cancel_applies_when_its_original_arrives() {
    let fixture = Fixture::new().await;
    receive_cancel_before_target(&fixture.store).await;
    let effects = receive_original_after_cancel(&fixture.store).await.unwrap();
    assert!(effects.attempts.is_empty());
    assert_eq!(effects.reports.len(), 2);
    fixture.store.transaction(|scope| Box::pin(async move {
        let mut inbox = Inbox::<_, TestTypes>::new(scope, peer(2), limits());
        assert!(matches!(inbox.local_outcome(item_id(1)).await.map_err(storage_error)?, Some(RecipientOutcome::Prevented { cancel }) if cancel == item_id(PENDING_CANCEL)));
        assert!(inbox.pending_report(item_id(1)).await.map_err(storage_error)?.is_some());
        assert!(inbox.pending_report(item_id(PENDING_CANCEL)).await.map_err(storage_error)?.is_some());
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn empty_cycles_are_fresh_and_overlapping_triggers_coalesce() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut inbox = Inbox::<_, TestTypes>::new(scope, peer(2), limits());
                let first = inbox
                    .begin_receive(DeliveryStream::Items)
                    .await
                    .map_err(storage_error)?
                    .unwrap();
                assert!(
                    inbox
                        .begin_receive(DeliveryStream::Items)
                        .await
                        .map_err(storage_error)?
                        .is_none()
                );
                assert!(
                    inbox
                        .begin_receive(DeliveryStream::Items)
                        .await
                        .map_err(storage_error)?
                        .is_none()
                );
                let rerun = inbox
                    .finish_receive(first)
                    .await
                    .map_err(storage_error)?
                    .unwrap();
                assert_eq!(rerun.cycle.value(), first.cycle.value() + 1);
                assert!(
                    inbox
                        .finish_receive(rerun)
                        .await
                        .map_err(storage_error)?
                        .is_none()
                );
                let fresh = inbox
                    .begin_receive(DeliveryStream::Items)
                    .await
                    .map_err(storage_error)?
                    .unwrap();
                assert!(fresh.cycle > rerun.cycle);
                assert_eq!(fresh.after.value(), 0);
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn a_full_inbox_still_stores_a_reserved_cancel_from_controls() {
    let fixture = Fixture::new().await;
    fixture.store.transaction(|scope|Box::pin(async move {
        let original=item(1,"held original");
        let configured=configured_limits(100_000,20,serde_json::to_vec(&original).unwrap().len() as u64+u64::from(limits().settlement_allowance()));
        let DeliveryEnvelope::Item(original)=original else {return Err(durable_steps::StoreError::backend("fixture is not an item"));};
        let mut inbox=Inbox::<_,TestTypes>::new(scope,peer(2),configured);
        inbox.enqueue_local(original,SyncTime::from_unix_millis(0)).await.map_err(storage_error)?;
        let items=inbox.begin_receive(DeliveryStream::Items).await.map_err(storage_error)?.unwrap();
        let controls=inbox.begin_receive(DeliveryStream::Controls).await.map_err(storage_error)?.unwrap();
        assert_eq!(inbox.receive_budget(items,10,100_000).await.map_err(storage_error)?.0,0);
        assert_eq!(inbox.receive_budget(controls,10,100_000).await.map_err(storage_error)?.0,10);
        let effects=inbox.store_batch(controls,FetchResult{stream:DeliveryStream::Controls,entries:vec![FetchEntry::Item{seq:DeliverySeq::new(1),item:cancel(2,1)}],through:DeliverySeq::new(1)},&TestConsumer,SyncTime::from_unix_millis(0)).await.map_err(storage_error)?;
        assert!(effects.attempts.is_empty());
        assert!(matches!(inbox.local_outcome(item_id(1)).await.map_err(storage_error)?,Some(RecipientOutcome::Prevented{cancel}) if cancel==item_id(2)));
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn failure_outcome_survives_corrupted_recipient_custody() {
    let fixture = Fixture::new().await;
    let attempt = fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let DeliveryEnvelope::Item(original) = item(1, "original") else {
                    return Err(durable_steps::StoreError::backend("fixture is not an item"));
                };
                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .enqueue_local(original, SyncTime::from_unix_millis(0))
                    .await
                    .map_err(storage_error)?
                    .ok_or_else(|| durable_steps::StoreError::backend("fixture did not admit"))
            })
        })
        .await
        .unwrap();
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT payload FROM _fleet_sync_inbox_items WHERE item_id=?1",
                )
                .bind(item_id(1).as_uuid().to_string())
                .fetch_one(scope.executor())
                .await
                .map_err(durable_steps::StoreError::backend)?;
                let raw: String = row
                    .try_get("payload")
                    .map_err(durable_steps::StoreError::backend)?;
                let mut record: serde_json::Value = serde_json::from_str(&raw).unwrap();
                record["value"]["body"] = serde_json::json!("corrupted recipient body");
                sqlx_core::query::query::<sqlx_turso::Turso>(
                    "UPDATE _fleet_sync_inbox_items SET payload=?1 WHERE item_id=?2",
                )
                .bind(record.to_string())
                .bind(item_id(1).as_uuid().to_string())
                .execute(scope.executor())
                .await
                .map_err(durable_steps::StoreError::backend)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    fixture.store.transaction(move|scope|Box::pin(async move {
        let mut inbox=Inbox::<_,TestTypes>::new(scope,peer(2),limits());
        inbox.record_failure(attempt,DeliveryFailureKind::CustodyCorrupt,&TestConsumer,SyncTime::from_unix_millis(0)).await.map_err(storage_error)?;
        assert!(matches!(inbox.local_outcome(item_id(1)).await.map_err(storage_error)?,Some(RecipientOutcome::Refused{reason:DeliveryRefusal::ProcessingFailed{kind}}) if kind.as_str()=="custodyCorrupt"));
        Ok(())
    })).await.unwrap();
}
