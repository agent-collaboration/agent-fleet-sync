#![allow(clippy::unwrap_used)]
mod support;
use durable_steps::{AtomicStore, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::row::Row;
use support::*;

use fleet_sync_delivery::delivery_rollback as rollback_error;

async fn custody_counts(fixture: &Fixture) -> (i64, i64, i64) {
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let items = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT count(*) AS count FROM _fleet_sync_mail_items",
                )
                .fetch_one(scope.executor())
                .await?
                .try_get("count")?;
                let publications = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT count(*) AS count FROM _fleet_sync_status_owed",
                )
                .fetch_one(scope.executor())
                .await?
                .try_get("count")?;
                let operations = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT count(*) AS count FROM _fleet_sync_operations",
                )
                .fetch_one(scope.executor())
                .await?
                .try_get("count")?;
                Ok((items, publications, operations))
            })
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn refusal_after_custody_change_survives_atomic_rollback() {
    let fixture = Fixture::new().await;
    let result = fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut mailbox = Mailbox::<_, TestTypes>::new(scope, peer(2), limits());
                mailbox
                    .submit(
                        peer(1),
                        item(1, "pending custody"),
                        SyncTime::from_unix_millis(0),
                        "route".into(),
                        None,
                    )
                    .await
                    .map_err(rollback_error)?;
                admit_publications::<
                    _,
                    TestTypes,
                    PublishStatus<TestStore, sqlx_turso::Turso, TestTypes>,
                >(scope)
                .await
                .map_err(rollback_error)?;
                let oversized = DeliveryEnvelope::Item(
                    DeliveryItem::<TestTypes>::new(
                        item_id(2),
                        "x".repeat(100),
                        Recipient {
                            peer: peer(2),
                            address: "inbox".into(),
                        },
                        None,
                        "small".into(),
                    )
                    .unwrap(),
                );
                Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .submit(
                        peer(1),
                        oversized,
                        SyncTime::from_unix_millis(0),
                        "route".into(),
                        None,
                    )
                    .await
                    .map_err(rollback_error)?;
                Ok(())
            })
        })
        .await;
    assert_eq!(custody_counts(&fixture).await, (0, 0, 0));
    assert!(
        matches!(result, Err(StoreError::StepRollback { .. })),
        "typed refusal was lost: {result:?}"
    );
    let failure = classify_delivery_store_error(result.unwrap_err());
    let disposition =
        mailbox_failure_disposition::<TestTypes>(peer(2), MailboxOperation::Submit, failure);
    assert!(matches!(
        disposition,
        MailboxFailureDisposition::Answer {
            answer: DeliveryAnswer::Submit(SubmitResult::Refused {
                reason: DeliveryRefusal::TooLarge {
                    field: DeliverySizeField::Sender
                }
            }),
            corruption: None,
        }
    ));
    assert_sequence_restarts(&fixture).await;
}

#[tokio::test]
async fn corrupt_row_after_custody_change_survives_atomic_rollback() {
    let fixture = Fixture::new().await;
    let result = fixture.store.transaction(|scope| Box::pin(async move {
        Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
            .submit(peer(1), item(1, "pending custody"), SyncTime::from_unix_millis(0),
                "route".into(), None).await.map_err(rollback_error)?;
        admit_publications::<_, TestTypes, PublishStatus<TestStore, sqlx_turso::Turso, TestTypes>>(scope)
            .await.map_err(rollback_error)?;
        sqlx_core::query::query::<sqlx_turso::Turso>(
            "UPDATE _fleet_sync_mail_items SET record=json_set(record,'$.value.payload',NULL),payload_bytes=0",
        ).execute(scope.executor()).await?;
        Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
            .query(&[item_id(1)]).await.map_err(rollback_error)?;
        Ok(())
    })).await;
    assert_eq!(custody_counts(&fixture).await, (0, 0, 0));
    assert!(
        matches!(result, Err(StoreError::StepRollback { .. })),
        "typed corruption was lost: {result:?}"
    );
    let failure = classify_delivery_store_error(result.unwrap_err());
    assert!(
        matches!(&failure, DeliveryTransactionFailure::Corrupt { field } if field == "mail custody state")
    );
    match mailbox_failure_disposition::<TestTypes>(peer(2), MailboxOperation::Query, failure) {
        MailboxFailureDisposition::Answer { answer, corruption } => {
            assert!(matches!(
                answer,
                DeliveryAnswer::MailboxUnavailable {
                    reason: DeliveryRefusal::ProcessingFailed {
                        kind: DeliveryFailureKind::CustodyCorrupt
                    }
                }
            ));
            let corruption = corruption.unwrap();
            assert_eq!(corruption.mailbox, peer(2));
            assert_ne!(corruption.mailbox, peer(3));
            assert_eq!(corruption.field, "mail custody state");
        }
        disposition => assert!(
            matches!(disposition, MailboxFailureDisposition::Answer { .. }),
            "custody corruption must answer unavailable"
        ),
    }
    assert_sequence_restarts(&fixture).await;
}

async fn assert_sequence_restarts(fixture: &Fixture) {
    fixture.store.transaction(|scope| Box::pin(async move {
        let answer = Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
            .submit(peer(1), item(3, "after rollback"), SyncTime::from_unix_millis(1),
                "route".into(), None).await.map_err(rollback_error)?;
        assert!(matches!(answer, SubmitResult::Accepted { seq, version, .. } if seq.value() == 1 && version.value() == 1));
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn failed_store_rollback_overrides_the_delivery_marker() {
    let fixture = Fixture::new().await;
    let error = fixture
        .store
        .transaction::<(), _>(|scope| {
            Box::pin(async move {
                Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .submit(
                        peer(1),
                        item(1, "must disappear"),
                        SyncTime::from_unix_millis(0),
                        "route".into(),
                        None,
                    )
                    .await
                    .map_err(rollback_error)?;
                // End the real transaction so the store's required rollback itself fails.
                sqlx_core::query::query::<sqlx_turso::Turso>("ROLLBACK")
                    .execute(scope.executor())
                    .await?;
                Err(rollback_error(DeliveryError::Refused {
                    reason: DeliveryRefusal::MailboxFull,
                }))
            })
        })
        .await
        .unwrap_err();
    assert!(matches!(&error, StoreError::Backend { .. }));
    assert!(matches!(
        mailbox_failure_disposition::<TestTypes>(
            peer(2),
            MailboxOperation::Submit,
            classify_delivery_store_error(error)
        ),
        MailboxFailureDisposition::Unhandled(DeliveryTransactionFailure::OutcomeUnknown(_))
    ));
    assert_eq!(custody_counts(&fixture).await, (0, 0, 0));
}

#[test]
fn malformed_and_lookalike_markers_never_refuse_or_quarantine() {
    use durable_steps::OperationFailure;
    for (code, detail) in [
        ("delivery_refused", "{}"),
        ("delivery_refused", r#"{"kind":"mailboxFull","extra":1}"#),
        (
            "delivery_refused",
            r#"{"kind":"tooLarge","value":{"field":"unknown"}}"#,
        ),
        ("delivery_custody_corrupt", r#"{"field":""}"#),
        ("delivery_custody_corrupt", r#"{"field":"mail\nstate"}"#),
        ("delivery_custody_corrupt", r#"{"field":"mail","extra":1}"#),
        ("delivery_custody_corrupt", "null"),
    ] {
        let failure = classify_delivery_store_error(StoreError::StepRollback {
            failure: Box::new(OperationFailure::Refused {
                code: code.into(),
                detail: detail.into(),
            }),
        });
        assert!(matches!(
            &failure,
            DeliveryTransactionFailure::OutcomeUnknown(StoreError::Corrupt { .. })
        ));
        assert!(matches!(
            mailbox_failure_disposition::<TestTypes>(peer(2), MailboxOperation::Submit, failure),
            MailboxFailureDisposition::Unhandled(_)
        ));
    }
    for error in [
        StoreError::backend(r#"delivery_refused {"kind":"mailboxFull"}"#),
        StoreError::Corrupt {
            field: "delivery_custody_corrupt".into(),
            message: "mail state".into(),
        },
        StoreError::StepRollback {
            failure: Box::new(OperationFailure::Refused {
                code: "copy_piece_refused".into(),
                detail: r#"{"kind":"retired"}"#.into(),
            }),
        },
    ] {
        assert!(matches!(
            mailbox_failure_disposition::<TestTypes>(
                peer(2),
                MailboxOperation::Submit,
                classify_delivery_store_error(error)
            ),
            MailboxFailureDisposition::Unhandled(DeliveryTransactionFailure::OutcomeUnknown(_))
        ));
    }
}

#[test]
fn disposition_preserves_the_operation_and_leaves_infrastructure_unhandled() {
    for operation in [
        MailboxOperation::Submit,
        MailboxOperation::Report,
        MailboxOperation::Fetch,
        MailboxOperation::Received,
        MailboxOperation::Query,
        MailboxOperation::Retire,
        MailboxOperation::Settle,
    ] {
        let failure = classify_delivery_store_error(rollback_error(DeliveryError::Refused {
            reason: DeliveryRefusal::MailboxFull,
        }));
        let disposition = mailbox_failure_disposition::<TestTypes>(peer(2), operation, failure);
        match operation {
            MailboxOperation::Submit => assert!(matches!(
                disposition,
                MailboxFailureDisposition::Answer {
                    answer: DeliveryAnswer::Submit(SubmitResult::Refused {
                        reason: DeliveryRefusal::MailboxFull
                    }),
                    corruption: None
                }
            )),
            MailboxOperation::Report => assert!(matches!(
                disposition,
                MailboxFailureDisposition::Answer {
                    answer: DeliveryAnswer::Report(ReportResult::Refused {
                        reason: DeliveryRefusal::MailboxFull
                    }),
                    corruption: None
                }
            )),
            _ => assert!(matches!(
                disposition,
                MailboxFailureDisposition::Unhandled(DeliveryTransactionFailure::Refused(
                    DeliveryRefusal::MailboxFull
                ))
            )),
        }
        let corrupt = classify_delivery_store_error(rollback_error(DeliveryError::Corrupt {
            field: "mail state",
        }));
        assert!(
            matches!(mailbox_failure_disposition::<TestTypes>(peer(2), operation, corrupt),
            MailboxFailureDisposition::Answer { corruption: Some(MailboxCorruption { mailbox, .. }), .. } if mailbox == peer(2))
        );
        for error in [
            DeliveryError::InvalidBatch,
            DeliveryError::CallConflict,
            DeliveryError::Encoding("encoding".into()),
            DeliveryError::SequenceExhausted,
            DeliveryError::InboxFull,
            DeliveryError::CustodyMissing,
            DeliveryError::DigestMismatch,
            DeliveryError::ProcessingFailed {
                kind: DeliveryFailureKind::CustodyCorrupt,
            },
            DeliveryError::Storage("storage".into()),
        ] {
            assert!(matches!(
                mailbox_failure_disposition::<TestTypes>(
                    peer(2),
                    operation,
                    classify_delivery_store_error(rollback_error(error))
                ),
                MailboxFailureDisposition::Unhandled(DeliveryTransactionFailure::OutcomeUnknown(_))
            ));
        }
    }
}
