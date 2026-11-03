use super::support::{network::*, push_expiry::*, *};
use durable_steps::{AtomicStore, storage::TransactionScope};
use durable_steps::{Engine, storage::JournalStorage};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn cached_acceptance_survives_abort_before_receipt_and_reopen_after_the_deadline() {
    cached_push_answer(false).await;
}
#[tokio::test]
async fn cached_replayed_delivery_survives_abort_before_receipt_and_reopen_after_the_deadline() {
    cached_push_answer(true).await;
}
async fn cached_push_answer(replayed: bool) {
    use super::support::push_checkpoint::PausedPush;
    use durable_steps::storage::StepState;
    use std::{sync::Arc, time::Duration};
    let mut network = NetworkFixture::new().await;
    seed(&network).await;
    if replayed {
        let pushed = network
            .mailbox
            .store
            .transaction(|scope| {
                Box::pin(async move {
                    let row = query::<Turso>(
                        "SELECT digest FROM _fleet_sync_outbox_items WHERE item_id=?1",
                    )
                    .bind(item_id(1).as_uuid().to_string())
                    .fetch_one(scope.executor())
                    .await?;
                    let reference = DeliveryItemRef {
                        item: item_id(1),
                        digest: ItemDigest::try_from(row.try_get::<String, _>("digest")?)
                            .map_err(durable_steps::StoreError::backend)?,
                    };
                    Outbox::<_, TestTypes>::new(scope, limits())
                        .resolve_push(&reference, at(0))
                        .await
                        .map_err(storage_error)
                })
            })
            .await
            .unwrap()
            .unwrap();
        network
            .recipient
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .accept_push::<DeliverTask, ReportTask, RecordRemoval, _>(
                            pushed.submitter,
                            pushed.item,
                            peer(3),
                            &TestConsumer,
                            at(0),
                        )
                        .await
                        .map_err(storage_error)?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        assert_eq!(
            network
                .engine(&network.recipient)
                .drive_due()
                .await
                .unwrap()
                .completed
                .len(),
            1
        );
    }
    let operation = push_operation(&network.mailbox.store).await;
    let reached = Arc::new(tokio::sync::Notify::new());
    let mut engine = Engine::new(network.mailbox.store.clone(), network.mailbox.host.clone());
    engine
        .register(PausedPush {
            inner: PushTask::new(limits(), network.mailbox.host.clone()),
            reached: reached.clone(),
        })
        .unwrap();
    let driver = tokio::spawn(async move { engine.drive_due().await });
    tokio::time::timeout(Duration::from_secs(10), reached.notified())
        .await
        .unwrap();
    network
        .mailbox
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                assert!(matches!(
                    scope.step(operation, 0).await?.unwrap().state,
                    StepState::CallDone { .. }
                ));
                let row =
                    query::<Turso>("SELECT handed_off_at,payload FROM _fleet_sync_outbox_items")
                        .fetch_one(scope.executor())
                        .await?;
                assert!(row.try_get::<Option<i64>, _>("handed_off_at")?.is_none());
                assert!(row.try_get::<Option<String>, _>("payload")?.is_some());
                Ok(())
            })
        })
        .await
        .unwrap();
    driver.abort();
    assert!(driver.await.unwrap_err().is_cancelled());
    let Fixture {
        store,
        host,
        directory,
    } = network.mailbox;
    assert_eq!(Arc::strong_count(&store), 1);
    drop(store);
    let store = Arc::new(
        TestStore::open_and_migrate(directory.path().join("delivery.db"), host.clone())
            .await
            .unwrap(),
    );
    *network.routes.mailbox.lock().unwrap() = Arc::downgrade(&store);
    network.mailbox = Fixture {
        store,
        host,
        directory,
    };
    clock(&network, 100_000);
    let resumed = network.engine(&network.mailbox).drive_due().await.unwrap();
    assert_eq!(resumed.completed.len(), 1);
    assert!(resumed.failed.is_empty());
    publish(&network).await;
    let status = sender_status(&network, 1).await;
    assert!(if replayed {
        matches!(
            status,
            OutboxStatusState::Known {
                status: DeliveryStatus::Delivered { .. },
                ..
            }
        )
    } else {
        matches!(
            status,
            OutboxStatusState::Known {
                status: DeliveryStatus::AtRecipient,
                ..
            }
        )
    });
    assert_eq!(recipient_rows(&network).await.0, 1);
    assert_eq!(
        network.routes.push_bodies.load(Ordering::SeqCst),
        1,
        "cached acceptance must not send another payload"
    );
}

#[tokio::test]
async fn lost_receipt_at_deadline_keeps_sent_and_a_later_real_delivered_report_reaches_the_submitter()
 {
    use durable_steps::storage::{CallReach, StepState};
    let network = NetworkFixture::new().await;
    seed(&network).await;
    network.routes.lose_receipt.store(true, Ordering::SeqCst);
    let engine = network.engine(&network.mailbox);
    assert_eq!(engine.drive_due().await.unwrap().retried.len(), 1);
    let operation = push_operation(&network.mailbox.store).await;
    network.mailbox.store.transaction(move |scope|Box::pin(async move {
        assert!(matches!(scope.step(operation,0).await?.unwrap().state,StepState::CallPending {attempt,..} if attempt.reach==CallReach::MayHaveReached));
        Ok(())
    })).await.unwrap();
    // B completes its effect before expiry, while its report is still queued.
    assert_eq!(
        network
            .engine(&network.recipient)
            .drive_due()
            .await
            .unwrap()
            .completed
            .len(),
        1
    );
    clock(&network, 100_000);
    let finished = engine.drive_due().await.unwrap();
    assert_eq!(finished.completed.len(), 1);
    assert!(finished.failed.is_empty());
    assert_eq!(
        network.routes.push_bodies.load(Ordering::SeqCst),
        1,
        "uncertain reach must not resend payload after expiry"
    );
    network.mailbox.store.transaction(|scope|Box::pin(async move {
        let row=query::<Turso>("SELECT status_kind,payload,submit_attempted,handed_off_at FROM _fleet_sync_outbox_items").fetch_one(scope.executor()).await?;
        assert_eq!(row.try_get::<String,_>("status_kind")?,"sent");
        assert!(row.try_get::<Option<String>,_>("payload")?.is_none());
        assert_eq!(row.try_get::<i64,_>("submit_attempted")?,1);
        assert!(row.try_get::<Option<i64>,_>("handed_off_at")?.is_none());
        Ok(())
    })).await.unwrap();
    assert_eq!(recipient_rows(&network).await.0, 1);
    drain(&network.engine(&network.recipient)).await;
    publish(&network).await;
    assert!(matches!(
        sender_status(&network, 1).await,
        OutboxStatusState::Known {
            status: DeliveryStatus::Delivered { .. },
            ..
        }
    ));
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn lost_receipt_without_a_report_stays_sent_until_its_original_removal_deadline() {
    let network = NetworkFixture::new().await;
    seed(&network).await;
    network.routes.lose_receipt.store(true, Ordering::SeqCst);
    let engine = network.engine(&network.mailbox);
    assert_eq!(engine.drive_due().await.unwrap().retried.len(), 1);
    clock(&network, 100_000);
    assert_eq!(engine.drive_due().await.unwrap().completed.len(), 1);
    assert_eq!(network.routes.push_bodies.load(Ordering::SeqCst), 1);
    assert!(matches!(
        sender_status(&network, 1).await,
        OutboxStatusState::Known {
            status: DeliveryStatus::Sent,
            ..
        }
    ));
    network
        .mailbox
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let row =
                    query::<Turso>("SELECT status_kind,payload FROM _fleet_sync_outbox_items")
                        .fetch_one(scope.executor())
                        .await?;
                assert_eq!(row.try_get::<String, _>("status_kind")?, "sent");
                assert!(row.try_get::<Option<String>, _>("payload")?.is_none());
                Ok(())
            })
        })
        .await
        .unwrap();
    let deadline = removal_deadline(item_id(1)).unix_millis();
    clock(&network, deadline - 1);
    assert!(engine.drive_due().await.unwrap().completed.is_empty());
    clock(&network, deadline);
    assert_eq!(engine.drive_due().await.unwrap().completed.len(), 1);
    network
        .mailbox
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let row = query::<Turso>("SELECT count(*) AS count FROM _fleet_sync_outbox_items")
                    .fetch_one(scope.executor())
                    .await?;
                assert_eq!(row.try_get::<i64, _>("count")?, 0);
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn interrupted_dispatch_at_the_deadline_keeps_sent_without_resending_then_accepts_bs_report()
{
    use durable_steps::storage::{CallReach, StepState};
    use std::{sync::Arc, time::Duration};
    let network = NetworkFixture::new().await;
    seed(&network).await;
    let operation = push_operation(&network.mailbox.store).await;
    let reached = Arc::new(tokio::sync::Notify::new());
    *network.routes.push_barrier.lock().unwrap() = Some(FetchBarrier {
        reached: reached.clone(),
        release: Arc::new(tokio::sync::Notify::new()),
    });
    let driver = tokio::spawn({
        let engine = network.engine(&network.mailbox);
        async move { engine.drive_due().await }
    });
    tokio::time::timeout(Duration::from_secs(10), reached.notified())
        .await
        .unwrap();
    network.mailbox.store.transaction(move |scope|Box::pin(async move {
        assert!(matches!(scope.step(operation,0).await?.unwrap().state,StepState::CallPending {attempt,..} if attempt.reach==CallReach::Dispatching));
        Ok(())
    })).await.unwrap();
    driver.abort();
    assert!(driver.await.unwrap_err().is_cancelled());
    clock(&network, 100_000);
    let recovered = network.engine(&network.mailbox).drive_due().await.unwrap();
    assert_eq!(recovered.completed.len(), 1);
    network
        .mailbox
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert!(matches!(
                    Outbox::<_, TestTypes>::new(scope, limits())
                        .status(item_id(1))
                        .await
                        .map_err(storage_error)?
                        .state,
                    OutboxStatusState::Known {
                        status: DeliveryStatus::Sent,
                        ..
                    }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(network.routes.push_bodies.load(Ordering::SeqCst), 1);
    assert_eq!(recipient_rows(&network).await.0, 1);
    drain(&network.engine(&network.recipient)).await;
    publish(&network).await;
    assert!(matches!(
        sender_status(&network, 1).await,
        OutboxStatusState::Known {
            status: DeliveryStatus::Expired,
            ..
        }
    ));
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 0);
}
