#![allow(clippy::unwrap_used)]
use super::*;
use crate::Outbox;
use crate::custody_fixture as support;
use durable_steps::{AtomicStore, storage::TransactionScope};
use sqlx_core::{query::query, row::Row};
use support::*;

#[tokio::test]
async fn sender_not_enrolled_submit_receipt_remains_terminal_after_reopen() {
    let Fixture {
        store,
        host,
        directory,
    } = Fixture::new().await;
    store
        .transaction(|scope| {
            Box::pin(async move {
                let DeliveryEnvelope::Item(original) = item(1, "refused sender") else {
                    return Err(durable_steps::StoreError::backend("original fixture"));
                };
                let input = SubmitInput {
                    item: original.id(),
                    digest: original.digest().clone(),
                    relay: peer(3),
                    mailbox: peer(2),
                };
                let mut outbox = Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings);
                outbox
                    .enqueue(
                        original,
                        peer(3),
                        Submitter::Machine(peer(1)),
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                outbox
                    .mark_submitted(
                        &input,
                        SubmitResult::Refused {
                            reason: DeliveryRefusal::SenderNotEnrolled,
                        },
                        SyncTime::from_unix_millis(1),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    drop(store);
    let reopened = TestStore::open_and_migrate(directory.path().join("delivery.db"), host)
        .await
        .unwrap();
    reopened.transaction(|scope|Box::pin(async move {
        let answer=Outbox::<_,TestTypes,_>::new(scope,limits(),&FixtureBindings).status(item_id(1)).await.map_err(storage_error)?;
        assert!(matches!(answer.state,OutboxStatusState::Known {status:DeliveryStatus::Refused {reason:DeliveryRefusal::SenderNotEnrolled},..}));
        let row=query::<sqlx_turso::Turso>("SELECT refusal_kind,payload,payload_bytes FROM _fleet_sync_outbox_items WHERE item_id=?1").bind(item_key(item_id(1))).fetch_one(scope.executor()).await?;
        assert_eq!(row.try_get::<String,_>("refusal_kind")?,"senderNotEnrolled");
        assert!(row.try_get::<Option<String>,_>("payload")?.is_none());
        assert_eq!(row.try_get::<i64,_>("payload_bytes")?,0);
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn stored_relay_route_bounds_are_checked_before_status_read() {
    for raw in [serde_json::to_string(&"x".repeat(63)).unwrap(), "{".into()] {
        let fixture = Fixture::new().await;
        fixture.store.transaction(move |scope|Box::pin(async move {
            Outbox::<_,TestTypes,_>::new(scope,limits(),&FixtureBindings)
                .submit_relay::<FixturePush<TestTypes>,RecordRemoval>(crate::Submitter::Machine(peer(1)),item(1,"route integrity"),peer(3),"valid".into(),SyncTime::from_unix_millis(0)).await.map_err(storage_error)?;
            query::<sqlx_turso::Turso>("UPDATE _fleet_sync_outbox_items SET status_route=?1").bind(raw).execute(scope.executor()).await?;
            let answer=Outbox::<_,TestTypes,_>::new(scope,limits(),&FixtureBindings)
                .relay_query(crate::Submitter::Machine(peer(1)),item_id(1)).await;
            assert!(matches!(answer,Err(crate::DeliveryError::Corrupt {..})),"a stored invalid relay route must be corruption before any status is returned");
            Ok(())
        })).await.unwrap();
    }
}

#[tokio::test]
async fn report_sequence_and_cancel_identity_survive_real_release_and_reopen() {
    let Fixture {
        store,
        host,
        directory,
    } = Fixture::new().await;
    store
        .transaction(|scope| {
            Box::pin(async move {
                let DeliveryEnvelope::Item(original) = item(90, "held before receipt") else {
                    return Err(durable_steps::StoreError::backend("original fixture"));
                };
                let mut outbox = Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings);
                outbox
                    .enqueue(
                        original,
                        peer(9),
                        crate::Submitter::Machine(peer(1)),
                        SyncTime::from_unix_millis(10),
                    )
                    .await
                    .map_err(storage_error)?;
                let Some(OutboxHeaderState::Item(mut header)) = outbox
                    .header_state(item_id(90))
                    .await
                    .map_err(storage_error)?
                else {
                    return Err(durable_steps::StoreError::backend("outbox header"));
                };
                // A report can overtake the Push answer. Construct that retained state
                // here; L2 drives its public report operation independently.
                header.status = DeliveryStatus::Held;
                header.version = StatusVersion::new(11);
                header.last_report_seq = ReportSeq::new(9);
                header.submit_attempted = true;
                outbox
                    .update_header(&header, fleet_sync_protocol::SyncTime::from_unix_millis(0))
                    .await
                    .map_err(storage_error)?;
                let DeliveryEnvelope::Cancel(control) = cancel(91, 90) else {
                    return Err(durable_steps::StoreError::backend("cancel fixture"));
                };
                outbox
                    .enqueue_cancel(
                        control,
                        peer(9),
                        crate::Submitter::Machine(peer(1)),
                        SyncTime::from_unix_millis(12),
                        &TestConsumer,
                    )
                    .await
                    .map_err(storage_error)?;
                for (identity, status, version, cancel_outcome) in [
                    (90, DeliveryStatus::Held, 11, None),
                    (91, DeliveryStatus::Sent, 1, Some(CancelResult::Pending)),
                ] {
                    let Some(OutboxHeaderState::Item(header)) = outbox
                        .header_state(item_id(identity))
                        .await
                        .map_err(storage_error)?
                    else {
                        return Err(durable_steps::StoreError::backend("outbox header"));
                    };
                    outbox
                        .mark_submitted(
                            &SubmitInput {
                                item: item_id(identity),
                                digest: stored_enum(Some(header.digest), "digest")
                                    .map_err(storage_error)?,
                                relay: peer(9),
                                mailbox: peer(2),
                            },
                            SubmitResult::Replayed {
                                status,
                                version: StatusVersion::new(version),
                                cancel: cancel_outcome,
                            },
                            SyncTime::from_unix_millis(20),
                        )
                        .await
                        .map_err(storage_error)?;
                }
                Ok(())
            })
        })
        .await
        .unwrap();
    drop(store);
    let reopened = TestStore::open_and_migrate(directory.path().join("delivery.db"), host)
        .await
        .unwrap();
    reopened.transaction(|scope| Box::pin(async move {
        let rows = query::<sqlx_turso::Turso>("SELECT item_id,payload,last_report_seq,cancel_outcome,cancel_target_item_id FROM _fleet_sync_outbox_items ORDER BY item_id")
            .fetch_all(scope.executor()).await?;
        assert_eq!(rows.len(), 2);
        for row in rows {
            assert!(row.try_get::<Option<String>, _>("payload")?.is_none());
            let identity: String = row.try_get("item_id")?;
            if identity == item_key(item_id(90)) {
                assert_eq!(row.try_get::<i64, _>("last_report_seq")?, 9);
            } else {
                assert_eq!(row.try_get::<String, _>("cancel_outcome")?, "pending");
                assert_eq!(row.try_get::<String, _>("cancel_target_item_id")?, item_key(item_id(90)));
            }
        }
        assert!(matches!(Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings).status(item_id(90)).await.map_err(storage_error)?.state,
            crate::OutboxStatusState::Known { status: DeliveryStatus::Held, version, .. } if version.value()==11));
        Ok(())
    })).await.unwrap();
}
