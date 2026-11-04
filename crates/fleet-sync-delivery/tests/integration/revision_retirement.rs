#![allow(clippy::unwrap_used)]
use super::support;
use durable_steps::{AtomicStore, SyncTime, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::row::Row;
use std::sync::atomic::Ordering;
use support::network::*;
use support::*;

#[tokio::test]
async fn owed_publication_refuses_a_stored_non_v7_peer_before_admitting_a_task() {
    let fixture = Fixture::new().await;
    fixture.store.transaction(|scope|Box::pin(async move {
        StatusStream::<_,TestTypes>::new(scope,Submitter::Machine(peer(1)),"fixture-route".into()).append(&StatusPublication {item:item_id(1),status:DeliveryStatus::Sent,version:StatusVersion::new(1),cancel:None},SyncTime::from_unix_millis(0)).await.map_err(storage_error)?;
        sqlx_core::query::query::<sqlx_turso::Turso>("UPDATE _fleet_sync_status_owed SET submitter_id='019eef00-0000-4000-8000-000000000001'").execute(scope.executor()).await?;
        assert!(matches!(admit_publications::<_,TestTypes,PublishTask>(scope).await,Err(DeliveryError::Corrupt {..})));
        let row=sqlx_core::query::query::<sqlx_turso::Turso>("SELECT count(*) AS count FROM _fleet_sync_operations").fetch_one(scope.executor()).await?;
        assert_eq!(row.try_get::<i64,_>("count")?,0);
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn an_old_publication_completion_cannot_retire_a_newer_owed_revision() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut stream = StatusStream::<_, TestTypes>::new(
                    scope,
                    Submitter::Machine(peer(1)),
                    "fixture-route".into(),
                );
                let (_, old) = stream
                    .append(
                        &StatusPublication {
                            item: item_id(1),
                            status: DeliveryStatus::Sent,
                            version: StatusVersion::new(1),
                            cancel: None,
                        },
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                let old = old.unwrap();
                assert!(
                    resolve_publication::<_, TestTypes>(scope, old.clone())
                        .await
                        .map_err(storage_error)?
                        .is_some()
                );
                let (_, newer) = StatusStream::<_, TestTypes>::new(
                    scope,
                    Submitter::Machine(peer(1)),
                    "fixture-route".into(),
                )
                .append(
                    &StatusPublication {
                        item: item_id(1),
                        status: DeliveryStatus::AtRecipient,
                        version: StatusVersion::new(2),
                        cancel: None,
                    },
                    SyncTime::from_unix_millis(0),
                )
                .await
                .map_err(storage_error)?;
                let newer = newer.unwrap();
                retire_publication::<_, TestTypes>(scope, old.clone())
                    .await
                    .map_err(storage_error)?;
                assert!(
                    resolve_publication::<_, TestTypes>(scope, old)
                        .await
                        .map_err(storage_error)?
                        .is_none()
                );
                let saved = resolve_publication::<_, TestTypes>(scope, newer.clone())
                    .await
                    .map_err(storage_error)?
                    .unwrap();
                assert!(matches!(saved.status, DeliveryStatus::AtRecipient));
                retire_publication::<_, TestTypes>(scope, newer.clone())
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
    drain(&network.engine(&network.relay)).await;
    let recipient = network.engine(&network.recipient);
    recipient.drive_due().await.unwrap(); // Pushed custody now records Held and owes its report.
    network.routes.relay_up.store(false, Ordering::SeqCst);
    let waiting = recipient.drive_due().await.unwrap();
    assert!(waiting.failed.is_empty());
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings)
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
                let mut inbox =
                    Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings);
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
    drain(&network.engine(&network.relay)).await;
    drain(&network.engine(&network.status)).await;
    network
        .sender
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert!(matches!(
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
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
