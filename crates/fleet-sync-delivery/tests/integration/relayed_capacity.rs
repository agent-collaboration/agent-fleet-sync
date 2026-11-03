use super::support::{
    network::{DeliverTask, ReportTask},
    *,
};
use durable_steps::{AtomicStore, Engine, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::sync::Arc;

fn now() -> SyncTime {
    SyncTime::from_unix_millis(0)
}
fn project() -> Submitter {
    Submitter::Project(DatabaseId::new(peer(1).as_uuid()).unwrap())
}
fn packet(identity: u64, source: &str) -> DeliveryEnvelope<TestTypes> {
    DeliveryEnvelope::Item(
        DeliveryItem::new(
            item_id(identity),
            owned_sender(project(), source),
            Recipient {
                peer: peer(2),
                address: "inbox".into(),
            },
            None,
            "body".into(),
        )
        .unwrap(),
    )
}
fn engine(fixture: &super::support::network::NetworkFixture) -> Engine<TestStore, TestHost> {
    let mut engine = Engine::new(
        fixture.recipient.store.clone(),
        fixture.recipient.host.clone(),
    );
    engine
        .register(DeliverTask::new(
            LocalDeliveryConfig {
                owner: peer(2),
                limits: limits(),
            },
            Arc::new(TestConsumer),
            fixture.recipient.host.clone(),
            std::sync::Arc::new(FixtureBindings),
        ))
        .unwrap();
    engine
}

#[tokio::test]
async fn delivered_relayed_originals_release_payload_and_capacity_but_keep_replay_identity() {
    let fixture = super::support::network::NetworkFixture::new().await;
    let room = u64::try_from(serde_json::to_vec(&packet(1, "session/a")).unwrap().len()).unwrap()
        + u64::from(limits().settlement_allowance());
    let limited = DeliveryLimits::new::<TestTypes>(
        100_000,
        20,
        100_000,
        20,
        room * 2,
        limits().settlement_allowance(),
    )
    .unwrap();
    fixture
        .recipient
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let mut inbox =
                    Inbox::<_, TestTypes, _>::new(scope, peer(2), limited, &FixtureBindings);
                for identity in 1..=2 {
                    assert!(matches!(
                        inbox
                            .accept_push::<DeliverTask, ReportTask, RecordRemoval, _>(
                                project(),
                                packet(identity, "session/a"),
                                peer(3),
                                &TestConsumer,
                                now()
                            )
                            .await
                            .map_err(storage_error)?,
                        PushResult::Accepted { .. }
                    ));
                }
                assert!(matches!(
                    inbox
                        .accept_push::<DeliverTask, ReportTask, RecordRemoval, _>(
                            project(),
                            packet(3, "session/a"),
                            peer(3),
                            &TestConsumer,
                            now()
                        )
                        .await
                        .map_err(storage_error)?,
                    PushResult::Refused {
                        reason: DeliveryRefusal::MailboxFull
                    }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
    let result = engine(&fixture).drive_due().await.unwrap();
    assert_eq!(result.completed.len(), 2);
    assert!(result.failed.is_empty());
    fixture.recipient.store.transaction(move |scope|Box::pin(async move {
        let rows=query::<Turso>("SELECT item_id,digest,payload,live,payload_bytes,settlement_charge_bytes,submitter_kind,submitter_id,consumer_sender,created_at,accepted_at,delivered_at,outcome_kind FROM _fleet_sync_inbox_items ORDER BY item_id").fetch_all(scope.executor()).await?;
        assert_eq!(rows.len(),2);
        for (row,identity) in rows.iter().zip(1..=2) {
            assert_eq!(row.try_get::<String,_>("item_id")?,item_id(identity).as_uuid().to_string());
            assert_eq!(row.try_get::<String,_>("digest")?,packet(identity,"session/a").digest().as_str());
            assert!(row.try_get::<Option<String>,_>("payload")?.is_none());
            for column in ["live","payload_bytes","settlement_charge_bytes"] {assert_eq!(row.try_get::<i64,_>(column)?,0);}
            assert_eq!(row.try_get::<String,_>("submitter_kind")?,"project");
            assert_eq!(row.try_get::<String,_>("submitter_id")?,project().as_uuid().to_string());
            assert_eq!(row.try_get::<String,_>("consumer_sender")?,serde_json::to_string(&owned_sender(project(),"session/a")).unwrap());
            assert_eq!(row.try_get::<i64,_>("created_at")?,1_717_986_918_400);
            assert_eq!(row.try_get::<i64,_>("accepted_at")?,0);
            assert_eq!(row.try_get::<i64,_>("delivered_at")?,0);
            assert_eq!(row.try_get::<String,_>("outcome_kind")?,"delivered");
        }
        let mut inbox=Inbox::<_,TestTypes, _>::new(scope,peer(2),limited, &FixtureBindings);
        assert!(matches!(inbox.accept_push::<DeliverTask,ReportTask,RecordRemoval,_>(project(),packet(1,"session/a"),peer(3),&TestConsumer,now()).await.map_err(storage_error)?,PushResult::Replayed {status:DeliveryStatus::Delivered {..},..}));
        assert!(matches!(inbox.accept_push::<DeliverTask,ReportTask,RecordRemoval,_>(project(),packet(3,"session/a"),peer(3),&TestConsumer,now()).await.map_err(storage_error)?,PushResult::Accepted {..}));
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn compacted_project_custody_keeps_the_full_consumer_source_for_cancellation_authority() {
    for (requester, allowed) in [("session/a", true), ("session/b", false)] {
        let fixture = super::support::network::NetworkFixture::new().await;
        fixture
            .recipient
            .store
            .transaction(|scope| {
                Box::pin(async move {
                    Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings)
                        .accept_push::<DeliverTask, ReportTask, RecordRemoval, _>(
                            project(),
                            packet(1, "session/a"),
                            peer(3),
                            &TestConsumer,
                            now(),
                        )
                        .await
                        .map_err(storage_error)?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        assert_eq!(
            engine(&fixture).drive_due().await.unwrap().completed.len(),
            1
        );
        fixture
            .recipient
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    let cancel = DeliveryCancel::new(
                        item_id(2),
                        owned_sender(project(), requester),
                        Recipient {
                            peer: peer(2),
                            address: "inbox".into(),
                        },
                        None,
                        CancelBody { target: item_id(1) },
                    )
                    .unwrap();
                    let mut inbox =
                        Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings);
                    inbox
                        .enqueue_local_cancel(cancel, &TestConsumer, now())
                        .await
                        .map_err(storage_error)?;
                    let outcome = inbox
                        .local_outcome(item_id(2))
                        .await
                        .map_err(storage_error)?
                        .ok_or(StoreError::backend("missing cancellation outcome"))?;
                    assert!(if allowed {
                        matches!(
                            outcome,
                            RecipientOutcome::TooLate {
                                at: TooLateAt::Delivered,
                                ..
                            }
                        )
                    } else {
                        matches!(
                            outcome,
                            RecipientOutcome::Refused {
                                reason: DeliveryRefusal::CancelSenderMismatch
                            }
                        )
                    });
                    Ok(())
                })
            })
            .await
            .unwrap();
    }
}
