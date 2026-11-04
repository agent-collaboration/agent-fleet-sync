#![allow(clippy::unwrap_used)]
use crate::custody_fixture::*;
use crate::*;
use durable_steps::{AtomicStore, Engine, StoreError, storage::TransactionScope};
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::sync::atomic::Ordering;

type RelayPublication = PublishStatus<TestStore, Turso, TestTypes>;
type RelayPush = PushItem<TestStore, Turso, TestTypes, RelayPublication, FixtureBindings>;
type RecipientAttempt = DeliverLocally<TestStore, Turso, TestTypes, TestConsumer, FixtureBindings>;
type RecipientReportTask = ReportOutcome<TestStore, Turso, TestTypes, FixtureBindings>;

async fn retired_tables(store: &TestStore) -> Vec<String> {
    store.transaction(|scope|Box::pin(async move {
        let rows=query::<Turso>("SELECT name FROM sqlite_schema WHERE type='table' AND name IN ('_fleet_sync_mail_items','_fleet_sync_mail_controls','_fleet_sync_mail_state','_fleet_sync_mail_cancel_links','_fleet_sync_delivery_receipts','_fleet_sync_inbox_state','_fleet_sync_outbox_status') ORDER BY name").fetch_all(scope.executor()).await?;
        rows.iter().map(|row|row.try_get("name")).collect::<Result<Vec<String>,_>>().map_err(StoreError::backend)
    })).await.unwrap()
}

async fn retire(store: &TestStore) {
    store
        .transaction(|scope| {
            Box::pin(async move {
                retire_relay_host_tables(scope)
                    .await
                    .map_err(delivery_rollback)
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn relay_retirement_preserves_relay_api_calls_across_reopen() {
    let relay = Fixture::new().await;
    let recipient = Fixture::new().await;
    assert_eq!(retired_tables(&relay.store).await.len(), 7);
    retire(&relay.store).await;
    assert!(
        retired_tables(&relay.store).await.is_empty(),
        "relay bootstrap still contains retired tables"
    );
    let reference = DeliveryItemRef {
        item: item_id(1),
        digest: item(1, "relay schema").digest().clone(),
    };
    relay
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert!(matches!(
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                        .submit_relay::<RelayPush, RecordRemoval>(
                            Submitter::Machine(peer(1)),
                            item(1, "relay schema"),
                            peer(3),
                            "route".into(),
                            SyncTime::from_unix_millis(0)
                        )
                        .await
                        .map_err(delivery_rollback)?,
                    SubmitResult::Accepted { .. }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
    let lookup = reference.clone();
    let pushed = relay
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .resolve_push(&lookup, SyncTime::from_unix_millis(0))
                    .await
                    .map_err(delivery_rollback)
            })
        })
        .await
        .unwrap()
        .unwrap();
    let answer = recipient
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings)
                    .accept_push::<RecipientAttempt, RecipientReportTask, RecordRemoval, _>(
                        pushed.submitter,
                        pushed.item,
                        peer(3),
                        &TestConsumer,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(delivery_rollback)
            })
        })
        .await
        .unwrap();
    relay
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let mut outbox = Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings);
                outbox
                    .receipt::<RelayPublication>(
                        peer(2),
                        &reference,
                        answer,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(delivery_rollback)?;
                let answer = outbox
                    .recipient_report::<RelayPublication>(
                        peer(2),
                        RecipientReport {
                            item: item_id(1),
                            report_seq: ReportSeq::new(1),
                            outcome: RecipientOutcome::Delivered {
                                acceptance: Acceptance {
                                    receipt: "real report".into(),
                                },
                            },
                        },
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(delivery_rollback)?;
                assert!(matches!(answer, ReportResult::Ack { .. }));
                let settled = outbox
                    .relay_settle::<RecordRemoval>(
                        Submitter::Machine(peer(1)),
                        item_id(2),
                        ItemDigest::try_from("a".repeat(64)).unwrap(),
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(delivery_rollback)?;
                assert!(matches!(settled, SettleResult::Settled { .. }));
                backfill_submitters::<TestTypes, _, _>(
                    scope,
                    &FixtureBindings,
                    SyncTime::from_unix_millis(0),
                )
                .await
                .map_err(delivery_rollback)?;
                assert_eq!(
                    admit_existing_record_removals::<_, RecordRemoval>(
                        scope,
                        SyncTime::from_unix_millis(0)
                    )
                    .await
                    .map_err(delivery_rollback)?,
                    0
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    retire(&relay.store).await;
    let Fixture {
        store,
        host,
        directory,
    } = relay;
    drop(store);
    let reopened = TestStore::open_and_migrate(directory.path().join("delivery.db"), host.clone())
        .await
        .unwrap();
    reopened.transaction(|scope|Box::pin(async move {
        let mut migrations=sqlx_core::migrate::Migrator::with_migrations(DELIVERY_MIGRATOR.iter().cloned().collect::<Vec<_>>());
        migrations.dangerous_set_table_name("_fleet_sync_delivery_migrations");
        migrations.run_direct(None,scope.executor(),false).await.map_err(StoreError::backend)?;
        retire_relay_host_tables(scope).await.map_err(delivery_rollback)?;
        backfill_submitters::<TestTypes,_,_>(scope,&FixtureBindings,SyncTime::from_unix_millis(0)).await.map_err(delivery_rollback)?;
        assert_eq!(admit_existing_record_removals::<_,RecordRemoval>(scope,SyncTime::from_unix_millis(0)).await.map_err(delivery_rollback)?,0);
        let status=Outbox::<_,TestTypes,_>::new(scope,limits(),&FixtureBindings).relay_query(Submitter::Machine(peer(1)),item_id(1)).await.map_err(delivery_rollback)?;
        assert!(matches!(status.state,ItemStatusState::Known {status:DeliveryStatus::Delivered {acceptance},version,..} if acceptance.receipt=="real report"&&version.value()==3));
        let stream=StatusStream::<_,TestTypes>::new(scope,Submitter::Machine(peer(1)),"route".into()).fetch(Submitter::Machine(peer(1)),StatusCursor::new(0),10).await.map_err(delivery_rollback)?;
        assert_eq!(stream.events.len(),2);
        let reference=PublicationRef {item:item_id(1),version:StatusVersion::new(3),target:PublicationTarget::Peer {peer:peer(1),route:"route".to_owned()}};
        assert!(resolve_publication::<_,TestTypes>(scope,reference).await.map_err(delivery_rollback)?.is_some());
        Ok(())
    })).await.unwrap();
    assert!(retired_tables(&reopened).await.is_empty());
    host.clock.store(
        u64::try_from(removal_deadline(item_id(1)).unix_millis()).unwrap(),
        Ordering::SeqCst,
    );
    let reopened = std::sync::Arc::new(reopened);
    let mut engine = Engine::new(reopened.clone(), host);
    engine.register(RecordRemoval::new()).unwrap();
    for identity in [1, 2] {
        engine
            .start_and_settle::<RecordRemoval>(
                &delivery_request_key(&item_id(identity)).unwrap(),
                item_id(identity),
            )
            .await
            .unwrap();
    }
    reopened
        .transaction(|scope| {
            Box::pin(async move {
                assert!(matches!(
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                        .relay_query(Submitter::Machine(peer(1)), item_id(1))
                        .await
                        .map_err(delivery_rollback)?
                        .state,
                    ItemStatusState::Unknown
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn relay_retirement_is_transactional_and_leaves_other_tables_unchanged() {
    let fixture = Fixture::new().await;
    let failed=fixture.store.transaction::<(),_>(|scope|Box::pin(async move {
        retire_relay_host_tables(scope).await.map_err(delivery_rollback)?;
        let row=query::<Turso>("SELECT count(*) AS legacy FROM sqlite_schema WHERE name='_fleet_sync_mail_items'").fetch_one(scope.executor()).await?;
        assert_eq!(row.try_get::<i64,_>("legacy")?,0);
        Err(StoreError::backend("caller aborts retirement"))
    })).await;
    assert!(failed.is_err());
    assert_eq!(retired_tables(&fixture.store).await.len(), 7);
    retire(&fixture.store).await;
    fixture.store.transaction(|scope|Box::pin(async move {
        let row=query::<Turso>("SELECT count(*) AS retained FROM sqlite_schema WHERE type='table' AND name IN ('_fleet_sync_outbox_items','_fleet_sync_outbox_cancel_links','_fleet_sync_status_owed','_fleet_sync_status_events','_fleet_sync_status_state','_fleet_sync_delivery_backfills','_fleet_sync_delivery_migrations','_fleet_sync_operations','_fleet_sync_steps')").fetch_one(scope.executor()).await?;
        assert_eq!(row.try_get::<i64,_>("retained")?,9);
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn relay_retirement_accepts_a_store_with_no_delivery_tables() {
    let directory = tempfile::tempdir().unwrap();
    let store = TestStore::open_and_migrate(
        directory.path().join("journal.db"),
        host_at(SyncTime::from_unix_millis(0)),
    )
    .await
    .unwrap();
    retire(&store).await;
    retire(&store).await;
    assert!(retired_tables(&store).await.is_empty());
}
