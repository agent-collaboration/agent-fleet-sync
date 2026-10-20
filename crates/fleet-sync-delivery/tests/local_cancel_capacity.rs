#![allow(clippy::unwrap_used)]
#[path = "support/local_cancellation.rs"]
mod local_cancellation;
mod support;
use durable_steps::{AtomicStore, storage::TransactionScope};
use fleet_sync_delivery::*;
use local_cancellation::*;
use support::*;

#[tokio::test]
async fn local_cancel_before_original_is_charged_and_replay_works_at_capacity() {
    let fixture = Fixture::new().await;
    let bytes = encoded_cancel_bytes(2, 1);
    let cap = configured_limits(100_000, 20, bytes);
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let mut inbox = Inbox::<_, TestTypes>::new(scope, peer(2), cap);
                inbox
                    .enqueue_local_cancel(
                        local_cancel(2, 1, "sender"),
                        &TestConsumer,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                for replay_id in [2, 3] {
                    let replay = inbox
                        .enqueue_local_cancel(
                            local_cancel(replay_id, 1, "sender"),
                            &TestConsumer,
                            SyncTime::from_unix_millis(0),
                        )
                        .await
                        .map_err(storage_error)?;
                    assert_eq!(replay.reference.item, item_id(2));
                }
                assert!(matches!(
                    inbox
                        .enqueue_local_cancel(
                            local_cancel(4, 5, "sender"),
                            &TestConsumer,
                            SyncTime::from_unix_millis(0)
                        )
                        .await,
                    Err(DeliveryError::Refused {
                        reason: DeliveryRefusal::MailboxFull
                    })
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
    let state = snapshot(&fixture).await;
    assert_eq!(state.live_bytes, i64::try_from(bytes).unwrap());
    assert_eq!(state.custody.len(), 1);
    assert_eq!(state.links.len(), 1);
    assert_no_remote_activity(&state);
}

#[tokio::test]
async fn later_local_original_does_not_double_charge_pending_cancel_bytes() {
    let fixture = Fixture::new().await;
    let cap = configured_limits(
        100_000,
        20,
        encoded_original_bytes(1) + u64::from(limits().settlement_allowance()),
    );
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let mut inbox = Inbox::<_, TestTypes>::new(scope, peer(2), cap);
                inbox
                    .enqueue_local_cancel(
                        local_cancel(2, 1, "sender"),
                        &TestConsumer,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                // Without the pending-cancel debit this exact cap rejects the original.
                assert!(
                    inbox
                        .enqueue_local(local_item(1), &TestConsumer, SyncTime::from_unix_millis(0))
                        .await
                        .map_err(storage_error)?
                        .is_none()
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(
        snapshot(&fixture).await.live_bytes,
        i64::try_from(encoded_cancel_bytes(2, 1)).unwrap()
    );
}

#[tokio::test]
async fn local_cancel_can_use_a_live_originals_reserved_capacity() {
    let fixture = Fixture::new().await;
    let cap = configured_limits(
        100_000,
        20,
        encoded_original_bytes(1) + u64::from(limits().settlement_allowance()),
    );
    enqueue_original(&fixture, 1, cap).await.unwrap();
    let full = snapshot(&fixture).await;
    assert_eq!(full.live_bytes, i64::try_from(cap.inbox_bytes()).unwrap());
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes>::new(scope, peer(2), cap)
                    .enqueue_local_cancel(
                        local_cancel(2, 1, "sender"),
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
    assert!(matches!(
        outcome(&fixture, 1).await,
        Some(RecipientOutcome::Prevented { .. })
    ));
    let compact = snapshot(&fixture).await;
    assert_eq!(
        compact.live_bytes,
        i64::try_from(encoded_cancel_bytes(2, 1)).unwrap()
    );
    assert_no_remote_activity(&compact);
}

#[tokio::test]
async fn compact_local_originals_cannot_spend_an_uncharged_reserve() {
    let fixture = Fixture::new().await;
    let input = enqueue_original(&fixture, 1, limits()).await.unwrap();
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
    let before = snapshot(&fixture).await;
    assert_eq!(before.live_bytes, 0);
    let cap = configured_limits(100_000, 20, encoded_cancel_bytes(2, 1) - 1);
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let answer = Inbox::<_, TestTypes>::new(scope, peer(2), cap)
                    .enqueue_local_cancel(
                        local_cancel(2, 1, "sender"),
                        &TestConsumer,
                        SyncTime::from_unix_millis(0),
                    )
                    .await;
                assert!(matches!(
                    answer,
                    Err(DeliveryError::Refused {
                        reason: DeliveryRefusal::MailboxFull
                    })
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(snapshot(&fixture).await, before);
    let admission = enqueue_cancel(&fixture, 2, 1).await;
    assert_eq!(admission.reference.item, item_id(2));
    assert_eq!(
        snapshot(&fixture).await.live_bytes,
        i64::try_from(encoded_cancel_bytes(2, 1)).unwrap()
    );
    let fixture = reopen(fixture).await;
    assert_eq!(
        snapshot(&fixture).await.live_bytes,
        i64::try_from(encoded_cancel_bytes(2, 1)).unwrap()
    );
}

#[tokio::test]
async fn local_cancel_rejects_wrong_recipient_and_fails_closed_on_corrupt_accounting() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let wrong = DeliveryCancel::<TestTypes>::new(
                    item_id(2),
                    "sender".into(),
                    Recipient {
                        peer: peer(9),
                        address: "inbox".into(),
                    },
                    None,
                    CancelBody { target: item_id(1) },
                )
                .unwrap();
                assert!(matches!(
                    Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .enqueue_local_cancel(wrong, &TestConsumer, SyncTime::from_unix_millis(0))
                        .await,
                    Err(DeliveryError::InvalidBatch)
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
    enqueue_original(&fixture, 1, limits()).await.unwrap();
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                sqlx_core::query::query::<sqlx_turso::Turso>(
                    "UPDATE _fleet_sync_inbox_items SET payload_bytes=-1",
                )
                .execute(scope.executor())
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    let before = snapshot(&fixture).await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                // An unrelated target forces the global accounting validation as well.
                assert!(matches!(
                    Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .enqueue_local_cancel(
                            local_cancel(2, 3, "sender"),
                            &TestConsumer,
                            SyncTime::from_unix_millis(0)
                        )
                        .await,
                    Err(DeliveryError::Corrupt { .. })
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(snapshot(&fixture).await, before);
}
