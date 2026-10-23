#![allow(clippy::unwrap_used)]
mod support;
use durable_steps::{AtomicStore, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::row::Row;
use std::sync::atomic::Ordering;
use support::{network::*, *};

async fn corrupt_sender(network: &NetworkFixture, id: u64, drop_header: bool) {
    network
        .sender
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let statement = if drop_header {
                    "DELETE FROM _fleet_sync_outbox_items WHERE item_id=?1"
                } else {
                    "UPDATE _fleet_sync_outbox_items SET payload='corrupt JSON' WHERE item_id=?1"
                };
                sqlx_core::query::query::<sqlx_turso::Turso>(statement)
                    .bind(item_id(id).as_uuid().to_string())
                    .execute(scope.executor())
                    .await
                    .map_err(durable_steps::StoreError::backend)?;
                Ok(())
            })
        })
        .await
        .unwrap();
}
#[tokio::test]
async fn relay_down_offline_recipient_and_all_real_engine_hops_preserve_custody() {
    let network = NetworkFixture::new().await;
    network.routes.relay_up.store(false, Ordering::SeqCst);
    network.enqueue(1, "unique body only in custody").await;
    let sender = network.engine(&network.sender);
    let report = sender.drive_due().await.unwrap();
    assert_eq!(report.retried.len(), 1);
    assert!(matches!(
        sender_status(&network, 1).await,
        OutboxStatusState::Known {
            status: DeliveryStatus::Queued,
            ..
        }
    ));
    // Pending input and step output may contain identity and receipts, never the body.
    network.sender.store.transaction(|scope|Box::pin(async move {
        let rows=sqlx_core::query::query::<sqlx_turso::Turso>("SELECT input AS bytes FROM _fleet_sync_operations UNION ALL SELECT output AS bytes FROM _fleet_sync_steps WHERE output IS NOT NULL").fetch_all(scope.executor()).await.map_err(durable_steps::StoreError::backend)?;
        for row in rows {let bytes:Option<Vec<u8>>=row.try_get("bytes").map_err(durable_steps::StoreError::backend)?;assert!(!String::from_utf8_lossy(&bytes.unwrap_or_default()).contains("unique body only in custody"));}Ok(())
    })).await.unwrap();
    network.routes.relay_up.store(true, Ordering::SeqCst);
    network.advance();
    drain(&sender).await;
    assert!(matches!(
        sender_status(&network, 1).await,
        OutboxStatusState::Known {
            status: DeliveryStatus::Sent { .. },
            ..
        }
    ));
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 0);
    network
        .mailbox
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT payload_bytes FROM _fleet_sync_mail_items",
                )
                .fetch_one(scope.executor())
                .await
                .map_err(durable_steps::StoreError::backend)?;
                assert!(row.try_get::<i64, _>("payload_bytes").unwrap() > 0);
                Ok(())
            })
        })
        .await
        .unwrap();
    network.receive(DeliveryStream::Items).await;
    drain(&network.engine(&network.recipient)).await;
    drain(&network.engine(&network.mailbox)).await;
    drain(&network.engine(&network.status)).await;
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
    assert!(
        matches!(sender_status(&network,1).await,OutboxStatusState::Known{status:DeliveryStatus::Delivered{acceptance},..} if acceptance.receipt=="unique body only in custody")
    );
    network
        .mailbox
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT payload_bytes FROM _fleet_sync_mail_items",
                )
                .fetch_one(scope.executor())
                .await
                .map_err(durable_steps::StoreError::backend)?;
                assert_eq!(row.try_get::<i64, _>("payload_bytes").unwrap(), 0);
                Ok(())
            })
        })
        .await
        .unwrap();
}
#[tokio::test]
async fn accepted_answer_lost_then_corrupt_body_reconciles_without_resending() {
    let network = NetworkFixture::new().await;
    network.enqueue(1, "accepted once").await;
    network.routes.lose_submit.store(true, Ordering::SeqCst);
    assert_eq!(
        network
            .engine(&network.sender)
            .drive_due()
            .await
            .unwrap()
            .retried
            .len(),
        1
    );
    corrupt_sender(&network, 1, false).await;
    network.advance();
    drain(&network.engine(&network.sender)).await;
    assert_eq!(network.routes.sent_bodies.load(Ordering::SeqCst), 1);
    assert_eq!(network.routes.settlements.load(Ordering::SeqCst), 1);
    assert!(matches!(
        sender_status(&network, 1).await,
        OutboxStatusState::Known {
            status: DeliveryStatus::Sent { .. },
            ..
        }
    ));
}
#[tokio::test]
async fn unknown_corrupt_body_fails_before_send_and_stays_failed_after_restart() {
    let network = NetworkFixture::new().await;
    network.enqueue(1, "never sent").await;
    corrupt_sender(&network, 1, false).await;
    let report = network.engine(&network.sender).drive_due().await.unwrap();
    assert_eq!(report.failed.len(), 1);
    assert!(report.retried.is_empty());
    assert_eq!(network.routes.sent_bodies.load(Ordering::SeqCst), 0);
    assert_eq!(network.routes.settlements.load(Ordering::SeqCst), 1);
    assert!(matches!(
        sender_status(&network, 1).await,
        OutboxStatusState::Known {
            status: DeliveryStatus::Refused {
                reason: DeliveryRefusal::ProcessingFailed { .. }
            },
            ..
        }
    ));
    network.advance();
    drain(&network.engine(&network.sender)).await;
    assert_eq!(network.routes.sent_bodies.load(Ordering::SeqCst), 0);
    assert_eq!(network.routes.settlements.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn header_loss_before_first_gate_reconciles_same_digest_conflict_and_unknown() {
    for relay_body in [Some("same"), Some("different"), None] {
        let network = NetworkFixture::new().await;
        network.enqueue(1, "same").await;
        if let Some(body) = relay_body {
            let body = body.to_owned();
            network
                .mailbox
                .store
                .transaction(move |scope| {
                    Box::pin(async move {
                        Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                            .submit(
                                peer(1),
                                item(1, &body),
                                SyncTime::from_unix_millis(0),
                                "fixture-route".to_owned(),
                                None,
                            )
                            .await
                            .map_err(storage_error)?;
                        Ok(())
                    })
                })
                .await
                .unwrap();
        }
        corrupt_sender(&network, 1, true).await;
        let report = network.engine(&network.sender).drive_due().await.unwrap();
        assert_eq!(network.routes.sent_bodies.load(Ordering::SeqCst), 0);
        assert_eq!(network.routes.settlements.load(Ordering::SeqCst), 1);
        match relay_body {
            Some("same") => {
                assert!(report.failed.is_empty());
                assert!(matches!(
                    sender_status(&network, 1).await,
                    OutboxStatusState::Known {
                        status: DeliveryStatus::Sent { .. },
                        ..
                    }
                ));
            }
            Some(_) => {
                assert!(report.failed.is_empty());
                assert!(matches!(
                    sender_status(&network, 1).await,
                    OutboxStatusState::Known {
                        status: DeliveryStatus::Refused {
                            reason: DeliveryRefusal::ItemConflict { .. }
                        },
                        ..
                    }
                ));
            }
            None => {
                assert_eq!(report.failed.len(), 1);
                assert!(matches!(
                    sender_status(&network, 1).await,
                    OutboxStatusState::Known {
                        status: DeliveryStatus::Refused {
                            reason: DeliveryRefusal::ProcessingFailed { .. }
                        },
                        ..
                    }
                ));
            }
        }
        network
            .sender
            .store
            .transaction(|scope| {
                Box::pin(async move {
                    let DeliveryEnvelope::Item(item) = item(1, "later different content") else {
                        return Err(durable_steps::StoreError::backend(
                            "fixture must be an item",
                        ));
                    };
                    assert!(matches!(
                        Outbox::<_, TestTypes>::new(scope, limits())
                            .enqueue(item, peer(3))
                            .await
                            .map_err(storage_error)?,
                        OutboxAdmission::Refused {
                            reason: DeliveryRefusal::ItemConflict { .. }
                        }
                    ));
                    Ok(())
                })
            })
            .await
            .unwrap();
    }
}
#[tokio::test]
async fn malformed_header_before_gate_and_on_resume_routes_to_reconciliation() {
    for prior_attempt in [false, true] {
        let network = NetworkFixture::new().await;
        network.enqueue(1, "bad header").await;
        if prior_attempt {
            network.routes.lose_submit.store(true, Ordering::SeqCst);
            assert_eq!(
                network
                    .engine(&network.sender)
                    .drive_due()
                    .await
                    .unwrap()
                    .retried
                    .len(),
                1
            );
        }
        network
            .sender
            .store
            .transaction(|scope| {
                Box::pin(async move {
                    sqlx_core::query::query::<sqlx_turso::Turso>(
                        "UPDATE _fleet_sync_outbox_items SET record='malformed' WHERE item_id=?1",
                    )
                    .bind(item_id(1).as_uuid().to_string())
                    .execute(scope.executor())
                    .await
                    .map_err(durable_steps::StoreError::backend)?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        network.advance();
        let report = network.engine(&network.sender).drive_due().await.unwrap();
        assert!(report.retried.is_empty());
        assert_eq!(network.routes.settlements.load(Ordering::SeqCst), 1);
        assert_eq!(
            network.routes.sent_bodies.load(Ordering::SeqCst),
            u64::from(prior_attempt)
        );
        assert!(matches!(
            sender_status(&network, 1).await,
            OutboxStatusState::Known { .. }
        ));
    }
}

#[tokio::test]
async fn atomic_settlement_fences_a_submit_that_was_already_in_flight() {
    let network = NetworkFixture::new().await;
    network.enqueue(1, "delayed in transport").await;
    network.routes.defer_submit.store(true, Ordering::SeqCst);
    assert_eq!(
        network
            .engine(&network.sender)
            .drive_due()
            .await
            .unwrap()
            .retried
            .len(),
        1
    );
    corrupt_sender(&network, 1, false).await;
    network.advance();
    assert_eq!(
        network
            .engine(&network.sender)
            .drive_due()
            .await
            .unwrap()
            .failed
            .len(),
        1
    );
    let delayed = network.routes.deferred.lock().unwrap().take().unwrap();
    network.mailbox.store.transaction(move|scope|Box::pin(async move {
        assert!(matches!(Mailbox::<_,TestTypes>::new(scope,peer(2),limits()).submit(peer(1),delayed.item,SyncTime::from_unix_millis(0), "fixture-route".to_owned(), None).await.map_err(storage_error)?,SubmitResult::Replayed{status:DeliveryStatus::Refused{reason:DeliveryRefusal::ProcessingFailed{kind}},version,..} if kind.as_str()=="custodyUnresolvable"&&version.value()==1));
        let row=sqlx_core::query::query::<sqlx_turso::Turso>("SELECT stream,seq,live,capacity_slot,payload_bytes FROM _fleet_sync_mail_items").fetch_one(scope.executor()).await.map_err(durable_steps::StoreError::backend)?;
        assert_eq!(row.try_get::<Option<String>,_>("stream").unwrap(),None);assert_eq!(row.try_get::<Option<i64>,_>("seq").unwrap(),None);assert_eq!(row.try_get::<i64,_>("live").unwrap(),0);assert_eq!(row.try_get::<i64,_>("capacity_slot").unwrap(),0);assert_eq!(row.try_get::<i64,_>("payload_bytes").unwrap(),0);Ok(())
    })).await.unwrap();
    network.receive(DeliveryStream::Items).await;
    drain(&network.engine(&network.recipient)).await;
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 0);
    assert!(
        matches!(sender_status(&network,1).await,OutboxStatusState::Known{status:DeliveryStatus::Refused{reason:DeliveryRefusal::ProcessingFailed{kind}},..} if kind.as_str()=="custodyUnresolvable")
    );
}

#[tokio::test]
async fn a_terminal_local_item_releases_body_custody_without_a_relay_retirement() {
    let network = NetworkFixture::new().await;
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let DeliveryEnvelope::Item(item) = item(1, "local body") else {
                    return Err(durable_steps::StoreError::backend("fixture item"));
                };
                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .enqueue_local_and_admit::<DeliverTask, _>(
                        item,
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
        .transaction(|scope| {
            Box::pin(async move {
                let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT payload_bytes FROM _fleet_sync_inbox_items WHERE item_id=?1",
                )
                .bind(item_id(1).as_uuid().to_string())
                .fetch_one(scope.executor())
                .await
                .map_err(durable_steps::StoreError::backend)?;
                assert_eq!(row.try_get::<i64, _>("payload_bytes").unwrap(), 0);
                Ok(())
            })
        })
        .await
        .unwrap();
}
