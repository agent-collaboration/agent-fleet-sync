#![allow(clippy::unwrap_used)]
use super::*;
use crate::Outbox;
use crate::custody_fixture as support;
use durable_steps::{AtomicStore, storage::TransactionScope};
use sqlx_core::{query::query, row::Row};
use support::*;

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
                let mut outbox = Outbox::<_, TestTypes>::new(scope, limits());
                outbox
                    .enqueue(original, peer(9), peer(1), SyncTime::from_unix_millis(10))
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
                outbox.update_header(&header).await.map_err(storage_error)?;
                let DeliveryEnvelope::Cancel(control) = cancel(91, 90) else {
                    return Err(durable_steps::StoreError::backend("cancel fixture"));
                };
                outbox
                    .enqueue_cancel(
                        control,
                        peer(9),
                        peer(1),
                        SyncTime::from_unix_millis(12),
                        &TestConsumer,
                    )
                    .await
                    .map_err(storage_error)?;
                for (identity, status, version, cancel_outcome) in [
                    (90, DeliveryStatus::Held, 11, None),
                    (
                        91,
                        DeliveryStatus::Sent {
                            seq: DeliverySeq::new(3),
                        },
                        1,
                        Some(CancelResult::Pending),
                    ),
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
        assert!(matches!(Outbox::<_, TestTypes>::new(scope, limits()).status(item_id(90)).await.map_err(storage_error)?.state,
            crate::OutboxStatusState::Known { status: DeliveryStatus::Held, version, .. } if version.value()==11));
        Ok(())
    })).await.unwrap();
}
