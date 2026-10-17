#![allow(clippy::unwrap_used)]
mod support;
use durable_steps::{AtomicStore, SyncTime, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::row::Row;
use std::sync::atomic::Ordering;
use support::network::*;
use support::*;

#[tokio::test]
async fn an_old_publication_completion_cannot_retire_a_newer_owed_revision() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .submit(peer(1), item(1, "body"), SyncTime::from_unix_millis(0))
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    let old = PublicationRef {
        item: item_id(1),
        version: StatusVersion::new(1),
        target: PublicationTarget::StatusStream { sender: peer(1) },
    };
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                assert!(
                    resolve_publication::<_, TestTypes>(scope, old)
                        .await
                        .map_err(storage_error)?
                        .is_some()
                );
                let mut mailbox = Mailbox::<_, TestTypes>::new(scope, peer(2), limits());
                mailbox
                    .fetch(
                        peer(2),
                        DeliveryStream::Items,
                        DeliverySeq::new(0),
                        1,
                        100_000,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                mailbox
                    .received(
                        peer(2),
                        DeliveryStream::Items,
                        DeliverySeq::new(1),
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let newer = PublicationRef {
                    version: StatusVersion::new(2),
                    ..old
                };
                retire_publication(scope, old)
                    .await
                    .map_err(storage_error)?;
                assert!(
                    resolve_publication::<_, TestTypes>(scope, old)
                        .await
                        .map_err(storage_error)?
                        .is_none()
                );
                let saved = resolve_publication::<_, TestTypes>(scope, newer)
                    .await
                    .map_err(storage_error)?
                    .unwrap();
                assert!(matches!(saved.status, DeliveryStatus::AtRecipient));
                retire_publication(scope, newer)
                    .await
                    .map_err(storage_error)?;
                assert!(
                    resolve_publication::<_, TestTypes>(scope, newer)
                        .await
                        .map_err(storage_error)?
                        .is_none()
                );
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn held_then_delivered_coalesces_one_owed_report_and_old_completion_preserves_newer() {
    let network = NetworkFixture::new().await;
    network.enqueue(1, "body").await;
    drain(&network.engine(&network.sender)).await;
    network
        .routes
        .local_answers
        .lock()
        .unwrap()
        .push_back(LocalOutcome::Held { until: None });
    network.receive(DeliveryStream::Items).await;
    let recipient = network.engine(&network.recipient);
    recipient.drive_due().await.unwrap(); // Receive stores body and admits local delivery.
    recipient.drive_due().await.unwrap(); // Held outcome owes first report.
    network.routes.relay_up.store(false, Ordering::SeqCst);
    let waiting = recipient.drive_due().await.unwrap();
    assert!(waiting.failed.is_empty());
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .release_held::<DeliverTask>(item_id(1), SyncTime::from_unix_millis(0))
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    // New local delivery can finish while the earlier report waits for connectivity.
    let delivered = recipient.drive_due().await.unwrap();
    assert!(delivered.failed.is_empty());
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT COUNT(*) AS count,MAX(report_seq) AS seq FROM _fleet_sync_reports",
                )
                .fetch_one(scope.executor())
                .await
                .map_err(durable_steps::StoreError::backend)?;
                assert_eq!(row.try_get::<i64, _>("count").unwrap(), 1);
                let sequence = row.try_get::<i64, _>("seq").unwrap();
                assert!(sequence > 1);
                let mut inbox = Inbox::<_, TestTypes>::new(scope, peer(2), limits());
                inbox
                    .retire_report(ReportRef {
                        item: item_id(1),
                        report_seq: ReportSeq::new(1),
                    })
                    .await
                    .map_err(storage_error)?;
                let pending = inbox
                    .pending_report(item_id(1))
                    .await
                    .map_err(storage_error)?
                    .unwrap();
                assert_eq!(pending.report_seq.value(), sequence as u64);
                assert!(matches!(
                    inbox
                        .resolve_report(pending)
                        .await
                        .map_err(storage_error)?
                        .outcome,
                    RecipientOutcome::Delivered { .. }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
    network.routes.relay_up.store(true, Ordering::SeqCst);
    network.advance();
    drain(&recipient).await;
    drain(&network.engine(&network.mailbox)).await;
    drain(&network.engine(&network.status)).await;
    network
        .sender
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
                        status: DeliveryStatus::Delivered { .. },
                        ..
                    }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
}
