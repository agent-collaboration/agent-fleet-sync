#![allow(clippy::unwrap_used)]
#[path = "support/local_cancellation.rs"]
mod local_cancellation;
mod support;
use durable_steps::{AtomicStore, storage::TransactionScope};
use fleet_sync_delivery::*;
use local_cancellation::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use support::{network::*, *};

fn reference(id: u64) -> DeliveryItemRef {
    let item = local_item(id);
    DeliveryItemRef {
        item: item.id(),
        digest: item.digest().clone(),
    }
}
async fn retire(fixture: &Fixture, reference: DeliveryItemRef) -> LocalRetirement {
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .retire_local(&reference)
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap()
}
async fn assert_refused(
    fixture: &Fixture,
    reference: DeliveryItemRef,
    reason: LocalRetirementRefusal,
) {
    let before = snapshot(fixture).await;
    assert_eq!(
        retire(fixture, reference).await,
        LocalRetirement::Refused(reason)
    );
    assert_eq!(snapshot(fixture).await, before, "refusal changed custody");
}
async fn fail_original(fixture: &Fixture, id: u64) {
    let input = require_local_attempt(enqueue_original(fixture, id, limits()).await).unwrap();
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .record_failure(
                        input,
                        DeliveryFailureKind::ExecutionFailed,
                        &TestConsumer,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn terminal_local_originals_retire_repeat_and_reopen() {
    // The real delivery task records each terminal outcome before retirement.
    for outcome in [
        LocalOutcome::Delivered {
            acceptance: Acceptance {
                receipt: "delivered".into(),
            },
        },
        LocalOutcome::Uncertain,
        LocalOutcome::Expired,
        LocalOutcome::Dropped,
        LocalOutcome::Refused {
            reason: DeliveryRefusal::DroppedByPolicy,
        },
    ] {
        let network = NetworkFixture::new().await;
        network
            .routes
            .local_answers
            .lock()
            .unwrap()
            .push_back(outcome);
        network
            .recipient
            .store
            .transaction(|scope| {
                Box::pin(async move {
                    Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .enqueue_local_and_admit::<DeliverTask, _>(
                            local_item(1),
                            &TestConsumer,
                            SyncTime::from_unix_millis(0),
                        )
                        .await
                        .map_err(storage_error)?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        drain(&network.engine(&network.recipient)).await;
        assert!(outcome_is_terminal(&network.recipient, 1).await);
        assert_eq!(
            retire(&network.recipient, reference(1)).await,
            LocalRetirement::Retired
        );
        assert_eq!(
            retire(&network.recipient, reference(1)).await,
            LocalRetirement::Absent
        );
        let fixture = reopen(network.recipient).await;
        assert_eq!(
            retire(&fixture, reference(1)).await,
            LocalRetirement::Absent
        );
        assert!(snapshot(&fixture).await.custody.is_empty());
        assert_no_pending_operations(&fixture).await;
    }
}
async fn outcome_is_terminal(fixture: &Fixture, id: u64) -> bool {
    matches!(
        outcome(fixture, id).await,
        Some(
            RecipientOutcome::Delivered { .. }
                | RecipientOutcome::Uncertain
                | RecipientOutcome::Expired
                | RecipientOutcome::Dropped
                | RecipientOutcome::Refused { .. }
        )
    )
}

#[tokio::test]
async fn absent_is_current_absence_without_digest_history() {
    let fixture = Fixture::new().await;
    assert_eq!(
        retire(&fixture, reference(1)).await,
        LocalRetirement::Absent
    );
    let mut other_digest = reference(1);
    other_digest.digest = item(1, "different contents").digest().clone();
    assert_eq!(
        retire(&fixture, other_digest).await,
        LocalRetirement::Absent
    );
    assert!(snapshot(&fixture).await.custody.is_empty());
}

#[tokio::test]
async fn pending_original_is_refused_without_mutation() {
    let fixture = Fixture::new().await;
    require_local_attempt(enqueue_original(&fixture, 1, limits()).await).unwrap();
    assert_refused(&fixture, reference(1), LocalRetirementRefusal::Pending).await;
}
#[tokio::test]
async fn attempting_original_is_refused_without_mutation() {
    let fixture = Fixture::new().await;
    let input = require_local_attempt(enqueue_original(&fixture, 1, limits()).await).unwrap();
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                assert!(matches!(
                    Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .check_delivery(input, SyncTime::from_unix_millis(0))
                        .await
                        .map_err(storage_error)?,
                    DeliveryCheck::Deliver { .. }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_refused(&fixture, reference(1), LocalRetirementRefusal::Attempting).await;
}
#[tokio::test]
async fn held_original_is_refused_without_mutation() {
    let network = NetworkFixture::new().await;
    network
        .routes
        .local_answers
        .lock()
        .unwrap()
        .push_back(LocalOutcome::Held { until: None });
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .enqueue_local_and_admit::<DeliverTask, _>(
                        local_item(1),
                        &TestConsumer,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    drain(&network.engine(&network.recipient)).await;
    assert!(matches!(
        outcome(&network.recipient, 1).await,
        Some(RecipientOutcome::Held)
    ));
    assert_refused(
        &network.recipient,
        reference(1),
        LocalRetirementRefusal::Held,
    )
    .await;
}
#[tokio::test]
async fn terminal_relayed_original_is_refused_without_mutation() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut inbox = Inbox::<_, TestTypes>::new(scope, peer(2), limits());
                let cycle = inbox
                    .begin_receive(DeliveryStream::Items)
                    .await
                    .map_err(storage_error)?
                    .unwrap();
                let effects = inbox
                    .store_batch(
                        cycle,
                        FetchResult {
                            stream: DeliveryStream::Items,
                            entries: vec![FetchEntry::Item {
                                seq: DeliverySeq::new(1),
                                item: DeliveryEnvelope::Item(local_item(1)),
                            }],
                            through: DeliverySeq::new(1),
                        },
                        &TestConsumer,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                inbox
                    .record_failure(
                        effects.attempts[0],
                        DeliveryFailureKind::ExecutionFailed,
                        &TestConsumer,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(snapshot(&fixture).await.reports.len(), 1);
    assert_refused(&fixture, reference(1), LocalRetirementRefusal::Relayed).await;
}
#[tokio::test]
async fn terminal_local_control_is_refused_without_mutation() {
    let fixture = Fixture::new().await;
    fail_original(&fixture, 1).await;
    let control = enqueue_cancel(&fixture, 2, 1).await.reference;
    assert!(outcome(&fixture, 2).await.is_some());
    assert_refused(&fixture, control, LocalRetirementRefusal::ControlItem).await;
}
#[tokio::test]
async fn mismatched_digest_is_refused_without_mutation() {
    let fixture = Fixture::new().await;
    fail_original(&fixture, 1).await;
    let mut wrong = reference(1);
    wrong.digest = item(1, "other original").digest().clone();
    assert_refused(&fixture, wrong, LocalRetirementRefusal::DigestMismatch).await;
}

async fn reports(fixture: &Fixture) -> Vec<String> {
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                query::<Turso>("SELECT item_id FROM _fleet_sync_reports ORDER BY item_id")
                    .fetch_all(scope.executor())
                    .await?
                    .into_iter()
                    .map(|row| row.try_get("item_id"))
                    .collect::<Result<Vec<String>, _>>()
                    .map_err(durable_steps::StoreError::backend)
            })
        })
        .await
        .unwrap()
}
async fn seed_report(fixture: &Fixture, id: u64) {
    // Local outcomes normally owe no remote report. Seed a valid retained row
    // explicitly to prove retirement removes any report, independent of seq.
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let report = RecipientReport::<TestTypes> {
                    item: item_id(id),
                    report_seq: ReportSeq::new(17),
                    outcome: RecipientOutcome::Expired,
                };
                query::<Turso>(
                    "INSERT INTO _fleet_sync_reports(item_id,report_seq,record) VALUES(?1,17,?2)",
                )
                .bind(item_id(id).as_uuid().to_string())
                .bind(serde_json::to_string(&report).unwrap())
                .execute(scope.executor())
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
}
#[tokio::test]
async fn retirement_removes_only_original_links_reports_and_accounting_across_reopen() {
    let fixture = Fixture::new().await;
    require_local_attempt(enqueue_original(&fixture, 1, limits()).await).unwrap();
    enqueue_cancel(&fixture, 2, 1).await;
    assert!(matches!(
        outcome(&fixture, 1).await,
        Some(RecipientOutcome::Prevented { .. })
    ));
    require_local_attempt(enqueue_original(&fixture, 50, limits()).await).unwrap();
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .enqueue_local_cancel(
                        local_cancel(51, 50, "other sender"),
                        &TestConsumer,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    seed_report(&fixture, 1).await;
    seed_report(&fixture, 50).await;
    let before = snapshot(&fixture).await;
    assert_eq!(before.custody.len(), 4);
    assert_eq!(before.links.len(), 2);
    assert!(before.live_bytes > 0);
    assert_eq!(
        retire(&fixture, reference(1)).await,
        LocalRetirement::Retired
    );
    let after = snapshot(&fixture).await;
    assert_eq!(after.custody, before.custody[1..]);
    assert_eq!(
        after.links,
        vec![(
            item_id(50).as_uuid().to_string(),
            item_id(51).as_uuid().to_string()
        )]
    );
    assert_eq!(
        reports(&fixture).await,
        vec![item_id(50).as_uuid().to_string()]
    );
    assert_eq!(
        after.live_bytes, before.live_bytes,
        "cancel and unrelated live charges must remain exact"
    );
    assert!(
        after.state.is_empty(),
        "retirement created receive/ack state"
    );
    assert!(
        fixture.host.routes.get().is_none(),
        "local retirement needed no central route"
    );
    let fixture = reopen(fixture).await;
    assert_eq!(snapshot(&fixture).await, after);
    assert_eq!(
        retire(&fixture, reference(1)).await,
        LocalRetirement::Absent
    );
}

#[tokio::test]
async fn corrupt_key_and_header_identities_are_rejected_without_mutation() {
    for change_key in [false, true] {
        let fixture = Fixture::new().await;
        fail_original(&fixture, 1).await;
        let mut requested = reference(1);
        fixture
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    if change_key {
                        query::<Turso>(
                            "UPDATE _fleet_sync_inbox_items SET item_id=?1 WHERE item_id=?2",
                        )
                        .bind(item_id(2).as_uuid().to_string())
                        .bind(item_id(1).as_uuid().to_string())
                        .execute(scope.executor())
                        .await?;
                    } else {
                        let row = query::<Turso>(
                            "SELECT record FROM _fleet_sync_inbox_items WHERE item_id=?1",
                        )
                        .bind(item_id(1).as_uuid().to_string())
                        .fetch_one(scope.executor())
                        .await?;
                        let mut header: serde_json::Value =
                            serde_json::from_str(&row.try_get::<String, _>("record")?).unwrap();
                        header["id"] = serde_json::to_value(item_id(2)).unwrap();
                        query::<Turso>(
                            "UPDATE _fleet_sync_inbox_items SET record=?1 WHERE item_id=?2",
                        )
                        .bind(serde_json::to_string(&header).unwrap())
                        .bind(item_id(1).as_uuid().to_string())
                        .execute(scope.executor())
                        .await?;
                    }
                    Ok(())
                })
            })
            .await
            .unwrap();
        if change_key {
            requested.item = item_id(2);
        }
        let before = snapshot(&fixture).await;
        let result = fixture
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    // Preserve the typed pre-write corruption refusal for inspection.
                    Ok(Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .retire_local(&requested)
                        .await)
                })
            })
            .await
            .unwrap();
        assert!(
            matches!(result, Err(DeliveryError::Corrupt { .. })),
            "{result:?}"
        );
        assert_eq!(snapshot(&fixture).await, before);
    }
}
#[tokio::test]
async fn retirement_rolls_back_with_the_owning_transaction() {
    let fixture = Fixture::new().await;
    fail_original(&fixture, 1).await;
    enqueue_cancel(&fixture, 2, 1).await;
    seed_report(&fixture, 1).await;
    let before = snapshot(&fixture).await;
    let result = fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert_eq!(
                    Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .retire_local(&reference(1))
                        .await
                        .map_err(storage_error)?,
                    LocalRetirement::Retired
                );
                Err::<(), _>(durable_steps::StoreError::backend(
                    "abort retirement transaction",
                ))
            })
        })
        .await;
    assert!(result.is_err());
    assert_eq!(snapshot(&fixture).await, before);
}
