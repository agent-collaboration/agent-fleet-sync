use super::support::{network::*, push_expiry::*, *};
use durable_steps::{AtomicStore, storage::TransactionScope};
use durable_steps::{
    Task,
    storage::{JournalStorage, OperationExecution},
};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn offline_push_is_due_at_its_deadline_and_publishes_expired_without_a_recipient_row() {
    let network = NetworkFixture::new().await;
    seed(&network).await;
    network.routes.relay_up.store(false, Ordering::SeqCst);
    let engine = network.engine(&network.mailbox);
    assert_eq!(engine.drive_due().await.unwrap().retried.len(), 1);
    let operation = push_operation(&network.mailbox.store).await;
    network.mailbox.store.transaction(move |scope|Box::pin(async move {
        assert!(matches!(scope.operation(operation).await?.unwrap().execution,OperationExecution::Running {next_attempt_at,..} if next_attempt_at==at(DEADLINE)),"backoff must be bounded by valid_until");
        Ok(())
    })).await.unwrap();
    clock(&network, DEADLINE - 1);
    assert!(engine.drive_due().await.unwrap().completed.is_empty());
    clock(&network, DEADLINE);
    let expired = engine.drive_due().await.unwrap();
    assert_eq!(expired.completed.len(), 1);
    assert!(expired.failed.is_empty());
    network.mailbox.store.transaction(|scope|Box::pin(async move {
        let row=query::<Turso>("SELECT status_kind,status_version,payload,handed_off_at FROM _fleet_sync_outbox_items").fetch_one(scope.executor()).await?;
        assert_eq!(row.try_get::<String,_>("status_kind")?,"expired");
        assert_eq!(row.try_get::<i64,_>("status_version")?,2);
        assert!(row.try_get::<Option<String>,_>("payload")?.is_none());
        assert!(row.try_get::<Option<i64>,_>("handed_off_at")?.is_none());
        Ok(())
    })).await.unwrap();
    assert_eq!(recipient_rows(&network).await, (0, 0));
    network.routes.relay_up.store(true, Ordering::SeqCst);
    publish(&network).await;
    assert!(
        matches!(sender_status(&network,1).await,OutboxStatusState::Known {status:DeliveryStatus::Expired,version,..} if version.value()==2)
    );
    clock(&network, 100_000);
    publish(&network).await;
    assert_eq!(
        recipient_rows(&network).await,
        (0, 0),
        "reconnecting B must receive neither custody nor work"
    );
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 0);
    assert_eq!(network.routes.push_bodies.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn expiry_before_the_first_push_dispatch_leaves_no_recipient_custody_or_work() {
    let network = NetworkFixture::new().await;
    seed(&network).await;
    clock(&network, DEADLINE);
    let completed = network.engine(&network.mailbox).drive_due().await.unwrap();
    assert_eq!(completed.completed.len(), 1);
    assert!(completed.failed.is_empty());
    publish(&network).await;
    assert!(matches!(
        sender_status(&network, 1).await,
        OutboxStatusState::Known {
            status: DeliveryStatus::Expired,
            ..
        }
    ));
    assert_eq!(recipient_rows(&network).await, (0, 0));
    assert_eq!(network.routes.push_bodies.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn after_a_real_handoff_the_recipient_still_owns_deadline_expiry() {
    let network = NetworkFixture::new().await;
    seed(&network).await;
    assert_eq!(
        network
            .engine(&network.mailbox)
            .drive_due()
            .await
            .unwrap()
            .completed
            .len(),
        1
    );
    publish(&network).await;
    assert!(matches!(
        sender_status(&network, 1).await,
        OutboxStatusState::Known {
            status: DeliveryStatus::AtRecipient,
            ..
        }
    ));
    clock(&network, DEADLINE);
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
    assert_eq!(network.routes.push_bodies.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn payload_resolved_before_expiry_arrives_after_it_without_admitting_local_delivery() {
    let network = NetworkFixture::new().await;
    seed(&network).await;
    let reference = network
        .mailbox
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let row =
                    query::<Turso>("SELECT digest FROM _fleet_sync_outbox_items WHERE item_id=?1")
                        .bind(item_id(1).as_uuid().to_string())
                        .fetch_one(scope.executor())
                        .await?;
                Ok(DeliveryItemRef {
                    item: item_id(1),
                    digest: ItemDigest::try_from(row.try_get::<String, _>("digest")?)
                        .map_err(durable_steps::StoreError::backend)?,
                })
            })
        })
        .await
        .unwrap();
    let lookup = reference.clone();
    let pushed = network
        .mailbox
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .resolve_push(&lookup, fleet_sync_protocol::SyncTime::from_unix_millis(0))
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap()
        .unwrap();
    clock(&network, DEADLINE);
    let expired_lookup = reference.clone();
    network
        .mailbox
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                assert!(
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                        .resolve_push(&expired_lookup, at(DEADLINE))
                        .await
                        .map_err(storage_error)?
                        .is_none(),
                    "host resolution at the deadline must expose no payload"
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    let answer=network.recipient.store.transaction(move |scope|Box::pin(async move {
        let answer=Inbox::<_,TestTypes, _>::new(scope,peer(2),limits(), &FixtureBindings).accept_push::<DeliverTask,ReportTask,RecordRemoval,_>(pushed.submitter,pushed.item,peer(3),&TestConsumer,at(DEADLINE)).await.map_err(storage_error)?;
        let row=query::<Turso>("SELECT outcome_kind,payload,(SELECT count(*) FROM _fleet_sync_operations WHERE kind=?1) AS deliveries FROM _fleet_sync_inbox_items WHERE item_id=?2").bind(DeliverTask::KIND).bind(item_id(1).as_uuid().to_string()).fetch_one(scope.executor()).await?;
        assert_eq!(row.try_get::<String,_>("outcome_kind")?,"expired");
        assert!(row.try_get::<Option<String>,_>("payload")?.is_none());
        assert_eq!(row.try_get::<i64,_>("deliveries")?,0);
        Ok(answer)
    })).await.unwrap();
    network
        .mailbox
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .receipt::<PublishTask>(peer(2), &reference, answer, at(DEADLINE))
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
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
