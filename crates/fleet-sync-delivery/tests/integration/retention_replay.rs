use super::support::{
    network::{DeliverTask, PushTask, ReportTask},
    *,
};
use durable_steps::{AtomicStore, Engine, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::sync::{Arc, atomic::Ordering};

use super::support::retention::*;
#[tokio::test]
async fn pending_hop_and_local_tasks_finish_after_removal_without_redispatch_or_recreating_rows() {
    let mut network = super::support::network::NetworkFixture::new().await;
    network.enqueue(1, "pending sender").await;
    network
        .relay
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .submit_relay::<PushTask, RecordRemoval>(
                        fleet_sync_delivery::Submitter::Machine(peer(1)),
                        item(1, "pending sender"),
                        peer(3),
                        "route".into(),
                        day(1),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings)
                    .accept_push::<DeliverTask, ReportTask, RecordRemoval, _>(
                        fleet_sync_delivery::Submitter::Machine(peer(1)),
                        item(1, "pending sender"),
                        peer(3),
                        &TestConsumer,
                        day(1),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    for fixture in [&network.sender, &network.relay, &network.recipient] {
        defer_delivery_work(fixture, day(61)).await;
    }
    for role in 0..3 {
        let fixture = match role {
            0 => &network.sender,
            1 => &network.relay,
            _ => &network.recipient,
        };
        fixture
            .host
            .clock
            .store(day(60).unix_millis() as u64, Ordering::SeqCst);
        let removed = retention_engine(fixture).drive_due().await.unwrap();
        assert_eq!(removed.completed.len(), 1);
        assert!(removed.failed.is_empty());
        assert!(identities(&fixture.store, "outbox").await.is_empty());
        assert!(identities(&fixture.store, "inbox").await.is_empty());
        let fixture = match role {
            0 => {
                network.sender = reopen(network.sender).await;
                *network.routes.sender.lock().unwrap() = Arc::downgrade(&network.sender.store);
                &network.sender
            }
            1 => {
                network.relay = reopen(network.relay).await;
                *network.routes.relay.lock().unwrap() = Arc::downgrade(&network.relay.store);
                &network.relay
            }
            _ => {
                network.recipient = reopen(network.recipient).await;
                *network.routes.recipient.lock().unwrap() =
                    Arc::downgrade(&network.recipient.store);
                &network.recipient
            }
        };
        fixture
            .host
            .clock
            .store(day(61).unix_millis() as u64, Ordering::SeqCst);
        let result = network.engine(fixture).drive_due().await.unwrap();
        assert!(
            result.failed.is_empty(),
            "old tasks must finish without reading removed custody: {result:?}"
        );
        assert!(result.retried.is_empty());
        assert!(identities(&fixture.store, "outbox").await.is_empty());
        assert!(identities(&fixture.store, "inbox").await.is_empty());
    }
    assert_eq!(network.routes.sent_bodies.load(Ordering::SeqCst), 0);
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn actual_pending_report_finishes_after_removal_and_reopen_without_recreating_custody() {
    let mut network = super::support::network::NetworkFixture::new().await;
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let admission =
                    Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings)
                        .accept_push::<DeliverTask, ReportTask, RecordRemoval, _>(
                            fleet_sync_delivery::Submitter::Machine(peer(1)),
                            item(1, "reported once"),
                            peer(3),
                            &TestConsumer,
                            day(1),
                        )
                        .await
                        .map_err(storage_error)?;
                assert!(matches!(admission, PushResult::Accepted { .. }));
                Ok(())
            })
        })
        .await
        .unwrap();
    // A real local effect creates the Report debt and task. Do not drive Report.
    network
        .recipient
        .host
        .clock
        .store(day(1).unix_millis() as u64, Ordering::SeqCst);
    let mut delivery = Engine::new(
        network.recipient.store.clone(),
        network.recipient.host.clone(),
    );
    delivery
        .register(DeliverTask::new(
            super::support::network::config().local,
            Arc::new(TestConsumer),
            network.recipient.host.clone(),
            std::sync::Arc::new(FixtureBindings),
        ))
        .unwrap();
    let delivered = delivery.drive_due().await.unwrap();
    assert_eq!(delivered.completed.len(), 1);
    assert!(delivered.failed.is_empty());
    drop(delivery);
    network.recipient.store.transaction(|scope| Box::pin(async move {
        let row=query::<Turso>("SELECT count(*) AS pending FROM _fleet_sync_operations WHERE kind='fleet-sync.delivery.report-outcome'").fetch_one(scope.executor()).await?;
        assert_eq!(row.try_get::<i64,_>("pending")?, 1);
        let report = Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings).resolve_report(ReportRef {item:item_id(1), report_seq:ReportSeq::new(1)}).await.map_err(storage_error)?;
        assert!(matches!(report.outcome, RecipientOutcome::Delivered {..}));
        Ok(())
    })).await.unwrap();
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
    defer_delivery_work(&network.recipient, day(61)).await;
    network
        .recipient
        .host
        .clock
        .store(day(60).unix_millis() as u64, Ordering::SeqCst);
    let removed = retention_engine(&network.recipient)
        .drive_due()
        .await
        .unwrap();
    assert_eq!(removed.completed.len(), 1);
    assert!(removed.failed.is_empty());
    network.recipient = reopen(network.recipient).await;
    *network.routes.recipient.lock().unwrap() = Arc::downgrade(&network.recipient.store);
    network
        .recipient
        .host
        .clock
        .store(day(61).unix_millis() as u64, Ordering::SeqCst);
    let mut report_engine = Engine::new(
        network.recipient.store.clone(),
        network.recipient.host.clone(),
    );
    report_engine
        .register(ReportTask::new(
            super::support::network::config(),
            network.recipient.host.clone(),
            std::sync::Arc::new(FixtureBindings),
        ))
        .unwrap();
    let result = report_engine.drive_due().await.unwrap();
    assert_eq!(
        result.completed.len(),
        1,
        "pending Report must finish at retention: {result:?}"
    );
    assert!(result.failed.is_empty() && result.retried.is_empty());
    assert!(
        identities(&network.recipient.store, "inbox")
            .await
            .is_empty()
    );
    assert!(identities(&network.relay.store, "outbox").await.is_empty());
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let row = query::<Turso>("SELECT count(*) AS debts FROM _fleet_sync_reports")
                    .fetch_one(scope.executor())
                    .await?;
                assert_eq!(row.try_get::<i64, _>("debts")?, 0);
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn sixty_one_day_receipt_is_refused_even_after_custody_was_removed() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let reference = DeliveryItemRef {
                    item: item_id(1),
                    digest: item(1, "old").digest().clone(),
                };
                let result = Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .receipt::<super::support::network::PublishTask>(
                        peer(2),
                        &reference,
                        PushResult::Accepted {
                            accepted_at: day(61),
                        },
                        day(61),
                    )
                    .await
                    .map_err(storage_error)?;
                assert!(matches!(
                    result,
                    StatusPublicationResult::Refused {
                        reason: DeliveryRefusal::TooOld
                    }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
    assert!(identities(&fixture.store, "outbox").await.is_empty());
}

#[tokio::test]
async fn sixty_one_day_recipient_report_is_refused_even_after_custody_was_removed() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let report = RecipientReport {
                    item: item_id(1),
                    report_seq: ReportSeq::new(1),
                    outcome: RecipientOutcome::Held,
                };
                let result = Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .recipient_report::<super::support::network::PublishTask>(
                        peer(2),
                        report,
                        day(61),
                    )
                    .await
                    .map_err(storage_error)?;
                assert!(matches!(
                    result,
                    ReportResult::Refused {
                        reason: DeliveryRefusal::TooOld
                    }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
    assert!(identities(&fixture.store, "outbox").await.is_empty());
}
