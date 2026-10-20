#![allow(clippy::unwrap_used)]
mod support;
use durable_steps::{AtomicStore, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::row::Row;
use support::{network::*, *};

#[tokio::test]
async fn compact_local_headers_authorize_cancels_and_preserve_identity() {
    for authorized in [true, false] {
        let network = NetworkFixture::new().await;
        network
            .recipient
            .store
            .transaction(|scope| {
                Box::pin(async move {
                    let DeliveryEnvelope::Item(original) = item(1, "local body") else {
                        return Err(durable_steps::StoreError::backend("fixture item"));
                    };
                    Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .enqueue_local_and_admit::<DeliverTask, _>(
                            original,
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
        network
            .recipient
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    let cancel = DeliveryEnvelope::Cancel(
                        DeliveryCancel::<TestTypes>::new(
                            item_id(2),
                            if authorized {
                                "sender"
                            } else {
                                "another sender"
                            }
                            .into(),
                            Recipient {
                                peer: peer(2),
                                address: "inbox".into(),
                            },
                            None,
                            CancelBody { target: item_id(1) },
                        )
                        .unwrap(),
                    );
                    let mut inbox = Inbox::<_, TestTypes>::new(scope, peer(2), limits());
                    let cycle = inbox
                        .begin_receive(DeliveryStream::Controls)
                        .await
                        .map_err(storage_error)?
                        .unwrap();
                    let effects = inbox
                        .store_batch(
                            cycle,
                            FetchResult {
                                stream: DeliveryStream::Controls,
                                entries: vec![FetchEntry::Item {
                                    seq: DeliverySeq::new(1),
                                    item: cancel,
                                }],
                                through: DeliverySeq::new(1),
                            },
                            &TestConsumer,
                            SyncTime::from_unix_millis(0),
                        )
                        .await
                        .map_err(storage_error)?;
                    assert!(effects.attempts.is_empty());
                    assert_eq!(effects.reports.len(), 1);
                    let report = inbox
                        .resolve_report(effects.reports[0])
                        .await
                        .map_err(storage_error)?;
                    if authorized {
                        assert!(matches!(
                            report.outcome,
                            RecipientOutcome::TooLate {
                                at: TooLateAt::Delivered,
                                ..
                            }
                        ));
                    } else {
                        assert!(matches!(
                            report.outcome,
                            RecipientOutcome::Refused {
                                reason: DeliveryRefusal::CancelSenderMismatch
                            }
                        ));
                    }
                    let DeliveryEnvelope::Item(repeat) = item(1, "local body") else {
                        return Err(durable_steps::StoreError::backend("fixture item"));
                    };
                    let stored_digest = repeat.digest().clone();
                    assert!(matches!(
                        inbox
                            .enqueue_local(repeat, &TestConsumer, SyncTime::from_unix_millis(0))
                            .await
                            .map_err(storage_error)?,
                        LocalItemAdmission::Replayed
                    ));
                    let DeliveryEnvelope::Item(changed) = item(1, "other local body") else {
                        return Err(durable_steps::StoreError::backend("fixture item"));
                    };
                    assert!(matches!(
                        inbox
                            .enqueue_local(changed, &TestConsumer, SyncTime::from_unix_millis(0))
                            .await,
                        Err(DeliveryError::Refused {
                            reason: DeliveryRefusal::ItemConflict { existing }
                        }) if existing == stored_digest
                    ));
                    let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                        "SELECT payload_bytes,record FROM _fleet_sync_inbox_items WHERE item_id=?1",
                    )
                    .bind(item_id(1).as_uuid().to_string())
                    .fetch_one(scope.executor())
                    .await
                    .map_err(durable_steps::StoreError::backend)?;
                    assert_eq!(row.try_get::<i64, _>("payload_bytes").unwrap(), 0);
                    let header: serde_json::Value =
                        serde_json::from_str(&row.try_get::<String, _>("record").unwrap()).unwrap();
                    assert_eq!(header["sender"], "sender");
                    assert!(header.get("payload").is_none());
                    assert!(header.get("body").is_none());
                    Ok(())
                })
            })
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn local_enqueue_conflict_returns_the_stored_digest() {
    let fixture = Fixture::new().await;
    let reference = fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let DeliveryEnvelope::Item(original) = item(1, "original local body") else {
                    return Err(durable_steps::StoreError::backend("fixture item"));
                };
                let reference = DeliveryItemRef {
                    item: original.id(),
                    digest: original.digest().clone(),
                };
                assert!(matches!(
                    Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .enqueue_local(original, &TestConsumer, SyncTime::from_unix_millis(0))
                        .await
                        .map_err(storage_error)?,
                    LocalItemAdmission::Attempt(_)
                ));
                Ok(reference)
            })
        })
        .await
        .unwrap();
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let DeliveryEnvelope::Item(changed) = item(1, "changed local body") else {
                    return Err(durable_steps::StoreError::backend("fixture item"));
                };
                assert_ne!(changed.digest(), &reference.digest);
                let mut inbox = Inbox::<_, TestTypes>::new(scope, peer(2), limits());
                let conflict = inbox
                    .enqueue_local(changed, &TestConsumer, SyncTime::from_unix_millis(0))
                    .await;
                assert!(
                    matches!(&conflict, Err(DeliveryError::Refused {
                        reason: DeliveryRefusal::ItemConflict { existing }
                    }) if existing == &reference.digest),
                    "expected ItemConflict with the stored digest; got {:?}",
                    conflict.as_ref().err()
                );
                let retained = inbox
                    .resolve_delivery(&reference)
                    .await
                    .map_err(storage_error)?;
                assert_eq!(retained.body(), "original local body");
                let DeliveryEnvelope::Item(repeat) = item(1, "original local body") else {
                    return Err(durable_steps::StoreError::backend("fixture item"));
                };
                assert!(matches!(
                    inbox
                        .enqueue_local(repeat, &TestConsumer, SyncTime::from_unix_millis(0))
                        .await
                        .map_err(storage_error)?,
                    LocalItemAdmission::Replayed
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn local_enqueue_capacity_returns_mailbox_full_without_losing_custody() {
    let fixture = Fixture::new().await;
    let capacity = u64::try_from(serde_json::to_vec(&item(1, "local body")).unwrap().len())
        .unwrap()
        + u64::from(limits().settlement_allowance());
    let configured = configured_limits(100_000, 20, capacity);
    let reference = fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let DeliveryEnvelope::Item(original) = item(1, "local body") else {
                    return Err(durable_steps::StoreError::backend("fixture item"));
                };
                let reference = DeliveryItemRef {
                    item: original.id(),
                    digest: original.digest().clone(),
                };
                assert!(matches!(
                    Inbox::<_, TestTypes>::new(scope, peer(2), configured)
                        .enqueue_local(original, &TestConsumer, SyncTime::from_unix_millis(0))
                        .await
                        .map_err(storage_error)?,
                    LocalItemAdmission::Attempt(_)
                ));
                Ok(reference)
            })
        })
        .await
        .unwrap();
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let DeliveryEnvelope::Item(blocked) = item(2, "local body") else {
                    return Err(durable_steps::StoreError::backend("fixture item"));
                };
                let mut inbox = Inbox::<_, TestTypes>::new(scope, peer(2), configured);
                let full = inbox
                    .enqueue_local(blocked, &TestConsumer, SyncTime::from_unix_millis(0))
                    .await;
                assert!(
                    matches!(
                        &full,
                        Err(DeliveryError::Refused {
                            reason: DeliveryRefusal::MailboxFull
                        })
                    ),
                    "expected MailboxFull from a full local inbox; got {:?}",
                    full.as_ref().err()
                );
                let retained = inbox
                    .resolve_delivery(&reference)
                    .await
                    .map_err(storage_error)?;
                assert_eq!(retained.body(), "local body");
                let DeliveryEnvelope::Item(repeat) = item(1, "local body") else {
                    return Err(durable_steps::StoreError::backend("fixture item"));
                };
                assert!(
                    matches!(
                        inbox
                            .enqueue_local(repeat, &TestConsumer, SyncTime::from_unix_millis(0))
                            .await
                            .map_err(storage_error)?,
                        LocalItemAdmission::Replayed
                    ),
                    "same-digest replay still succeeds when the inbox is full"
                );
                let DeliveryEnvelope::Item(mismatched) = item(3, "local body") else {
                    return Err(durable_steps::StoreError::backend("fixture item"));
                };
                assert!(matches!(
                    Inbox::<_, TestTypes>::new(scope, peer(3), configured)
                        .enqueue_local(mismatched, &TestConsumer, SyncTime::from_unix_millis(0))
                        .await,
                    Err(DeliveryError::InvalidBatch)
                ));
                let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT COUNT(*) AS count FROM _fleet_sync_inbox_items",
                )
                .fetch_one(scope.executor())
                .await
                .map_err(durable_steps::StoreError::backend)?;
                assert_eq!(row.try_get::<i64, _>("count").unwrap(), 1);
                Ok(())
            })
        })
        .await
        .unwrap();
}
