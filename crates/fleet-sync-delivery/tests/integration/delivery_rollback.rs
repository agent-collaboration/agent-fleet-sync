#![allow(clippy::unwrap_used)]
use super::support::*;
use durable_steps::{
    AtomicStore, OperationFailure, StoreError, storage::TransactionScope, turso_store::TursoScope,
};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;

async fn custody_counts(fixture: &Fixture) -> (i64, i64, i64, i64) {
    fixture.store.transaction(|scope|Box::pin(async move {
        let row=query::<Turso>("SELECT (SELECT count(*) FROM _fleet_sync_outbox_items) AS items,(SELECT count(*) FROM _fleet_sync_status_owed) AS owed,(SELECT count(*) FROM _fleet_sync_status_events) AS events,(SELECT count(*) FROM _fleet_sync_operations) AS operations").fetch_one(scope.executor()).await?;
        Ok((row.try_get("items")?,row.try_get("owed")?,row.try_get("events")?,row.try_get("operations")?))
    })).await.unwrap()
}

async fn admit_and_receipt(scope: &mut TursoScope<'_, TestHost>) -> Result<(), DeliveryError> {
    let envelope = item(1, "pending custody");
    let reference = DeliveryItemRef {
        item: envelope.id(),
        digest: envelope.digest().clone(),
    };
    let mut relay = Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings);
    relay
        .submit_relay::<FixturePush<TestTypes>, RecordRemoval>(
            Submitter::Machine(peer(1)),
            envelope,
            peer(3),
            "route".into(),
            SyncTime::from_unix_millis(0),
        )
        .await?;
    relay
        .receipt::<FixturePublication<TestTypes>>(
            peer(2),
            &reference,
            PushResult::Accepted {
                accepted_at: SyncTime::from_unix_millis(0),
            },
            SyncTime::from_unix_millis(0),
        )
        .await?;
    Ok(())
}

async fn assert_fresh_relay_after_rollback(fixture: &Fixture) {
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let answer = Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .submit_relay::<FixturePush<TestTypes>, RecordRemoval>(
                        Submitter::Machine(peer(1)),
                        item(3, "after rollback"),
                        peer(3),
                        "route".into(),
                        SyncTime::from_unix_millis(1),
                    )
                    .await
                    .map_err(delivery_rollback)?;
                assert!(matches!(answer,SubmitResult::Accepted {version,..} if version.value()==1));
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn refusal_after_custody_change_survives_atomic_rollback() {
    let fixture = Fixture::new().await;
    let result = fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                admit_and_receipt(scope).await.map_err(delivery_rollback)?;
                Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .recipient_report::<FixturePublication<TestTypes>>(
                        peer(2),
                        RecipientReport {
                            item: item_id(1),
                            report_seq: ReportSeq::new(1),
                            outcome: RecipientOutcome::Delivered {
                                acceptance: Acceptance {
                                    receipt: "x".repeat(128),
                                },
                            },
                        },
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(delivery_rollback)?;
                Ok(())
            })
        })
        .await;
    assert_eq!(custody_counts(&fixture).await, (0, 0, 0, 0));
    assert!(
        matches!(result, Err(StoreError::StepRollback { .. })),
        "typed refusal was lost: {result:?}"
    );
    assert!(matches!(
        classify_delivery_store_error(result.unwrap_err()),
        DeliveryTransactionFailure::Refused(DeliveryRefusal::TooLarge {
            field: DeliverySizeField::Acceptance
        })
    ));
    assert_fresh_relay_after_rollback(&fixture).await;
}

#[tokio::test]
async fn corrupt_row_after_custody_change_survives_atomic_rollback() {
    let fixture = Fixture::new().await;
    let result = fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                admit_and_receipt(scope).await.map_err(delivery_rollback)?;
                query::<Turso>(
                    "UPDATE _fleet_sync_outbox_items SET recipient_machine_id='broken recipient'",
                )
                .execute(scope.executor())
                .await?;
                Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .relay_query(Submitter::Machine(peer(1)), item_id(1))
                    .await
                    .map_err(delivery_rollback)?;
                Ok(())
            })
        })
        .await;
    assert_eq!(custody_counts(&fixture).await, (0, 0, 0, 0));
    assert!(
        matches!(result, Err(StoreError::StepRollback { .. })),
        "typed corruption was lost: {result:?}"
    );
    assert!(
        matches!(classify_delivery_store_error(result.unwrap_err()),DeliveryTransactionFailure::Corrupt {field} if !field.is_empty())
    );
    assert_fresh_relay_after_rollback(&fixture).await;
}

#[tokio::test]
async fn failed_store_rollback_overrides_the_delivery_marker() {
    let fixture = Fixture::new().await;
    let error = fixture
        .store
        .transaction::<(), _>(|scope| {
            Box::pin(async move {
                admit_and_receipt(scope).await.map_err(delivery_rollback)?;
                // End the real transaction so the store's required rollback itself fails.
                query::<Turso>("ROLLBACK").execute(scope.executor()).await?;
                Err(delivery_rollback(DeliveryError::Refused {
                    reason: DeliveryRefusal::MailboxFull,
                }))
            })
        })
        .await
        .unwrap_err();
    assert!(matches!(&error, StoreError::Backend { .. }));
    assert!(matches!(
        classify_delivery_store_error(error),
        DeliveryTransactionFailure::OutcomeUnknown(_)
    ));
    assert_eq!(custody_counts(&fixture).await, (0, 0, 0, 0));
}

#[test]
fn malformed_and_lookalike_markers_never_refuse_or_quarantine() {
    for (code, detail) in [
        ("delivery_refused", "{}"),
        ("delivery_refused", r#"{"kind":"mailboxFull","extra":1}"#),
        (
            "delivery_refused",
            r#"{"kind":"tooLarge","value":{"field":"unknown"}}"#,
        ),
        ("delivery_custody_corrupt", r#"{"field":""}"#),
        ("delivery_custody_corrupt", r#"{"field":"custody\nstate"}"#),
        (
            "delivery_custody_corrupt",
            r#"{"field":"custody","extra":1}"#,
        ),
        ("delivery_custody_corrupt", "null"),
    ] {
        let failure = classify_delivery_store_error(StoreError::StepRollback {
            failure: Box::new(OperationFailure::Refused {
                code: code.into(),
                detail: detail.into(),
            }),
        });
        assert!(matches!(
            failure,
            DeliveryTransactionFailure::OutcomeUnknown(StoreError::Corrupt { .. })
        ));
    }
    for error in [
        StoreError::backend(r#"delivery_refused {"kind":"mailboxFull"}"#),
        StoreError::Corrupt {
            field: "delivery_custody_corrupt".into(),
            message: "custody state".into(),
        },
        StoreError::StepRollback {
            failure: Box::new(OperationFailure::Refused {
                code: "copy_piece_refused".into(),
                detail: r#"{"kind":"retired"}"#.into(),
            }),
        },
    ] {
        assert!(matches!(
            classify_delivery_store_error(error),
            DeliveryTransactionFailure::OutcomeUnknown(_)
        ));
    }
}
