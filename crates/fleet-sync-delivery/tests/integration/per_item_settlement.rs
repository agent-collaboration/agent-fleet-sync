#![allow(clippy::unwrap_used)]
use super::per_item;
use super::support;
use durable_steps::{AtomicStore, storage::TransactionScope};
use fleet_sync_delivery::*;
use per_item::*;
use sqlx_core::row::Row;
use support::*;

#[tokio::test]
async fn ordinary_admission_uses_small_bound_while_large_kind_reserves_global() {
    for sender in [true, false] {
        let fixture = Fixture::new().await;
        fixture
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    let configured = variable_limits(50_000);
                    for identity in 1..=3 {
                        let mut outbox =
                            Outbox::<_, VariableTypes, _>::new(scope, configured, &FixtureBindings);
                        if sender {
                            assert!(matches!(
                                outbox
                                    .enqueue(
                                        variable_item(identity, "small"),
                                        peer(3),
                                        Submitter::Machine(peer(1)),
                                        now()
                                    )
                                    .await
                                    .map_err(storage_error)?,
                                OutboxAdmission::Queued { .. }
                            ));
                        } else {
                            assert!(matches!(
                                outbox
                                    .submit_relay::<FixturePush<VariableTypes>, RecordRemoval>(
                                        Submitter::Machine(peer(1)),
                                        DeliveryEnvelope::Item(variable_item(identity, "small")),
                                        peer(3),
                                        "route".into(),
                                        now()
                                    )
                                    .await
                                    .map_err(storage_error)?,
                                SubmitResult::Accepted { .. }
                            ));
                        }
                    }
                    let mut outbox =
                        Outbox::<_, VariableTypes, _>::new(scope, configured, &FixtureBindings);
                    if sender {
                        assert!(matches!(
                            outbox
                                .enqueue(
                                    variable_item(4, "large"),
                                    peer(3),
                                    Submitter::Machine(peer(1)),
                                    now()
                                )
                                .await
                                .map_err(storage_error)?,
                            OutboxAdmission::Refused {
                                reason: DeliveryRefusal::MailboxFull
                            }
                        ));
                    } else {
                        assert!(matches!(
                            outbox
                                .submit_relay::<FixturePush<VariableTypes>, RecordRemoval>(
                                    Submitter::Machine(peer(1)),
                                    DeliveryEnvelope::Item(variable_item(4, "large")),
                                    peer(3),
                                    "route".into(),
                                    now()
                                )
                                .await
                                .map_err(storage_error)?,
                            SubmitResult::Refused {
                                reason: DeliveryRefusal::MailboxFull
                            }
                        ));
                    }
                    Ok(())
                })
            })
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn invalid_body_bound_is_refused_before_any_custody_write() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert!(matches!(
                    Outbox::<_, VariableTypes, _>::new(
                        scope,
                        variable_limits(100_000_000),
                        &FixtureBindings
                    )
                    .enqueue(
                        variable_item(1, "invalid"),
                        peer(3),
                        fleet_sync_delivery::Submitter::Machine(peer(1)),
                        SyncTime::from_unix_millis(0)
                    )
                    .await,
                    Err(DeliveryError::Config(_))
                ));
                assert!(matches!(
                    Outbox::<_, VariableTypes, _>::new(
                        scope,
                        variable_limits(100_000_000),
                        &FixtureBindings
                    )
                    .submit_relay::<FixturePush<VariableTypes>, RecordRemoval>(
                        Submitter::Machine(peer(1)),
                        DeliveryEnvelope::Item(variable_item(1, "invalid")),
                        peer(3),
                        "route".into(),
                        now()
                    )
                    .await,
                    Err(DeliveryError::Config(_))
                ));
                let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT COUNT(*) AS count FROM _fleet_sync_outbox_items",
                )
                .fetch_one(scope.executor())
                .await?;
                assert_eq!(row.try_get::<i64, _>("count")?, 0);
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn stored_bound_rejects_oversized_acceptance_after_sender_and_relay_release_bodies() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let limits = variable_limits(100_000_000);
                Outbox::<_, VariableTypes, _>::new(scope, limits, &FixtureBindings)
                    .enqueue(
                        variable_item(1, "small"),
                        peer(3),
                        fleet_sync_delivery::Submitter::Machine(peer(1)),
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                Outbox::<_, VariableTypes, _>::new(scope, limits, &FixtureBindings)
                    .publish_status(
                        StatusPublication {
                            item: item_id(1),
                            status: DeliveryStatus::Sent,
                            version: StatusVersion::new(1),
                            cancel: None,
                        },
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT payload FROM _fleet_sync_outbox_items",
                )
                .fetch_one(scope.executor())
                .await?;
                assert!(row.try_get::<Option<String>, _>("payload")?.is_none());
                let oversized = StatusPublication {
                    item: item_id(1),
                    status: DeliveryStatus::Delivered {
                        acceptance: Acceptance {
                            receipt: "x".repeat(100),
                        },
                    },
                    version: StatusVersion::new(2),
                    cancel: None,
                };
                assert!(matches!(
                    Outbox::<_, VariableTypes, _>::new(scope, limits, &FixtureBindings)
                        .publish_status(oversized, SyncTime::from_unix_millis(0))
                        .await,
                    Err(DeliveryError::Refused {
                        reason: DeliveryRefusal::TooLarge {
                            field: DeliverySizeField::Acceptance
                        }
                    })
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn pushed_items_use_actual_reserves_and_refuse_at_room_boundary() {
    type Attempt = DeliverLocally<
        TestStore,
        sqlx_turso::Turso,
        VariableTypes,
        VariableConsumer,
        FixtureBindings,
    >;
    type Report = ReportOutcome<TestStore, sqlx_turso::Turso, VariableTypes, FixtureBindings>;
    let fixture = Fixture::new().await;
    fixture.store.transaction(|scope|Box::pin(async move {
        let answer=Inbox::<_,VariableTypes,_>::new(scope,peer(2),variable_limits(100_000_000),&FixtureBindings)
            .accept_push::<Attempt,Report,RecordRemoval,_>(Submitter::Machine(peer(1)),DeliveryEnvelope::Item(variable_item(1,"small")),peer(3),&VariableConsumer,now()).await.map_err(storage_error)?;
        assert!(matches!(answer,PushResult::Accepted {..}));
        let row=sqlx_core::query::query::<sqlx_turso::Turso>("SELECT payload_bytes,settlement_charge_bytes,acceptance_bound FROM _fleet_sync_inbox_items").fetch_one(scope.executor()).await?;
        let payload=row.try_get::<i64,_>("payload_bytes")?;
        let reserve=row.try_get::<i64,_>("settlement_charge_bytes")?;
        assert_eq!(row.try_get::<i64,_>("acceptance_bound")?,64);
        assert!(u64::try_from(reserve).unwrap()<u64::from(minimum_settlement_allowance::<VariableTypes>().unwrap())/10,"small receipt uses its own bound, not the global ceiling");
        let limited=variable_limits(u64::try_from(payload+reserve).unwrap());
        for (identity,body) in [(2,"large"),(3,"small")] {
            let refused=Inbox::<_,VariableTypes,_>::new(scope,peer(2),limited,&FixtureBindings)
                .accept_push::<Attempt,Report,RecordRemoval,_>(Submitter::Machine(peer(1)),DeliveryEnvelope::Item(variable_item(identity,body)),peer(3),&VariableConsumer,now()).await.map_err(storage_error)?;
            assert!(matches!(refused,PushResult::Refused {reason:DeliveryRefusal::MailboxFull}));
        }
        let row=sqlx_core::query::query::<sqlx_turso::Turso>("SELECT count(*) AS rows FROM _fleet_sync_inbox_items").fetch_one(scope.executor()).await?;
        assert_eq!(row.try_get::<i64,_>("rows")?,1);
        let control=DeliveryCancel::<VariableTypes>::new(item_id(4),"sender".into(),Recipient {peer:peer(2),address:"inbox".into()},None,CancelBody {target:item_id(1)}).unwrap();
        let accepted=Inbox::<_,VariableTypes,_>::new(scope,peer(2),limited,&FixtureBindings)
            .accept_push::<Attempt,Report,RecordRemoval,_>(Submitter::Machine(peer(1)),DeliveryEnvelope::Cancel(control),peer(3),&VariableConsumer,now()).await.map_err(storage_error)?;
        assert!(matches!(accepted,PushResult::Accepted {..}),"the original reserve still pays for its control at capacity");
        assert!(matches!(Inbox::<_,VariableTypes,_>::new(scope,peer(2),limited,&FixtureBindings).local_outcome(item_id(1)).await.map_err(storage_error)?,Some(RecipientOutcome::Prevented {cancel}) if cancel==item_id(4)));
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn per_item_bound_survives_reopen_and_legacy_absence_uses_global() {
    let Fixture {
        store,
        host,
        directory,
    } = Fixture::new().await;
    store
        .transaction(|scope| {
            Box::pin(async move {
                let limits = variable_limits(100_000_000);
                for identity in [1, 2] {
                    Outbox::<_, VariableTypes, _>::new(scope, limits, &FixtureBindings)
                        .enqueue(
                            variable_item(identity, "small"),
                            peer(3),
                            fleet_sync_delivery::Submitter::Machine(peer(1)),
                            SyncTime::from_unix_millis(0),
                        )
                        .await
                        .map_err(storage_error)?;
                    Outbox::<_, VariableTypes, _>::new(scope, limits, &FixtureBindings)
                        .publish_status(
                            StatusPublication {
                                item: item_id(identity),
                                status: DeliveryStatus::AtRecipient,
                                version: StatusVersion::new(1),
                                cancel: None,
                            },
                            SyncTime::from_unix_millis(0),
                        )
                        .await
                        .map_err(storage_error)?;
                }
                // Emulate the exact old JSON shape, including the released body.
                sqlx_core::query::query::<sqlx_turso::Turso>(
                    "UPDATE _fleet_sync_outbox_items SET acceptance_bound=NULL WHERE item_id=?1",
                )
                .bind(item_id(2).as_uuid().to_string())
                .execute(scope.executor())
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    drop(store);
    let reopened = TestStore::open_and_migrate(directory.path().join("delivery.db"), host)
        .await
        .unwrap();
    reopened
        .transaction(|scope| {
            Box::pin(async move {
                let limits = variable_limits(100_000_000);
                for identity in [1, 2] {
                    let result =
                        Outbox::<_, VariableTypes, _>::new(scope, limits, &FixtureBindings)
                            .publish_status(
                                StatusPublication {
                                    item: item_id(identity),
                                    status: DeliveryStatus::Delivered {
                                        acceptance: Acceptance {
                                            receipt: "x".repeat(100),
                                        },
                                    },
                                    version: StatusVersion::new(2),
                                    cancel: None,
                                },
                                SyncTime::from_unix_millis(0),
                            )
                            .await;
                    if identity == 1 {
                        assert!(matches!(
                            result,
                            Err(DeliveryError::Refused {
                                reason: DeliveryRefusal::TooLarge {
                                    field: DeliverySizeField::Acceptance
                                }
                            })
                        ));
                    } else {
                        result.map_err(storage_error)?;
                    }
                }
                let row = sqlx_core::query::query::<sqlx_turso::Turso>(include_str!(
                    "../support/sql/read_outbox_header.sql"
                ))
                .bind(item_id(1).as_uuid().to_string())
                .fetch_one(scope.executor())
                .await?;
                let header: serde_json::Value =
                    serde_json::from_str(&row.try_get::<String, _>("record")?).unwrap();
                assert_eq!(header["value"]["acceptanceBound"], 64);
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn relay_validates_stored_bound_after_push_receipt_releases_body() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let configured = variable_limits(100_000_000);
                let envelope = DeliveryEnvelope::Item(variable_item(1, "small"));
                let reference = DeliveryItemRef {
                    item: envelope.id(),
                    digest: envelope.digest().clone(),
                };
                let mut relay =
                    Outbox::<_, VariableTypes, _>::new(scope, configured, &FixtureBindings);
                assert!(matches!(
                    relay
                        .submit_relay::<FixturePush<VariableTypes>, RecordRemoval>(
                            Submitter::Machine(peer(1)),
                            envelope,
                            peer(3),
                            "route".into(),
                            now()
                        )
                        .await
                        .map_err(storage_error)?,
                    SubmitResult::Accepted { .. }
                ));
                relay
                    .receipt::<FixturePublication<VariableTypes>>(
                        peer(2),
                        &reference,
                        PushResult::Accepted { accepted_at: now() },
                        now(),
                    )
                    .await
                    .map_err(storage_error)?;
                let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT payload,acceptance_bound FROM _fleet_sync_outbox_items",
                )
                .fetch_one(scope.executor())
                .await?;
                assert!(row.try_get::<Option<String>, _>("payload")?.is_none());
                assert_eq!(row.try_get::<i64, _>("acceptance_bound")?, 64);
                assert!(matches!(
                    Outbox::<_, VariableTypes, _>::new(scope, configured, &FixtureBindings)
                        .recipient_report::<FixturePublication<VariableTypes>>(
                            peer(2),
                            RecipientReport {
                                item: item_id(1),
                                report_seq: ReportSeq::new(1),
                                outcome: RecipientOutcome::Delivered {
                                    acceptance: Acceptance {
                                        receipt: "x".repeat(100)
                                    }
                                }
                            },
                            now()
                        )
                        .await,
                    Err(DeliveryError::Refused {
                        reason: DeliveryRefusal::TooLarge {
                            field: DeliverySizeField::Acceptance
                        }
                    })
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn cancel_consumes_original_small_reserve_without_global_charge() {
    for sender in [true, false] {
        let fixture = Fixture::new().await;
        fixture
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    let configured = variable_limits(50_000);
                    let mut outbox =
                        Outbox::<_, VariableTypes, _>::new(scope, configured, &FixtureBindings);
                    if sender {
                        outbox
                            .enqueue(
                                variable_item(1, "small"),
                                peer(3),
                                Submitter::Machine(peer(1)),
                                now(),
                            )
                            .await
                            .map_err(storage_error)?;
                    } else {
                        outbox
                            .submit_relay::<FixturePush<VariableTypes>, RecordRemoval>(
                                Submitter::Machine(peer(1)),
                                DeliveryEnvelope::Item(variable_item(1, "small")),
                                peer(3),
                                "route".into(),
                                now(),
                            )
                            .await
                            .map_err(storage_error)?;
                    }
                    let before = sqlx_core::query::query::<sqlx_turso::Turso>(
                        "SELECT settlement_bytes FROM _fleet_sync_outbox_items",
                    )
                    .fetch_one(scope.executor())
                    .await?
                    .try_get::<i64, _>("settlement_bytes")?;
                    let control = DeliveryEnvelope::Cancel(
                        DeliveryCancel::<VariableTypes>::new(
                            item_id(2),
                            "sender".into(),
                            Recipient {
                                peer: peer(2),
                                address: "inbox".into(),
                            },
                            None,
                            CancelBody { target: item_id(1) },
                        )
                        .unwrap(),
                    );
                    let charge =
                        i64::try_from(serde_json::to_vec(&control).unwrap().len()).unwrap();
                    let mut outbox =
                        Outbox::<_, VariableTypes, _>::new(scope, configured, &FixtureBindings);
                    if sender {
                        let DeliveryEnvelope::Cancel(control) = control else {
                            return Err(durable_steps::StoreError::backend("fixture control"));
                        };
                        assert!(matches!(
                            outbox
                                .enqueue_cancel(
                                    control,
                                    peer(3),
                                    Submitter::Machine(peer(1)),
                                    now(),
                                    &VariableConsumer
                                )
                                .await
                                .map_err(storage_error)?,
                            OutboxAdmission::Queued { .. }
                        ));
                    } else {
                        assert!(matches!(
                            outbox
                                .submit_relay::<FixturePush<VariableTypes>, RecordRemoval>(
                                    Submitter::Machine(peer(1)),
                                    control,
                                    peer(3),
                                    "route".into(),
                                    now()
                                )
                                .await
                                .map_err(storage_error)?,
                            SubmitResult::Accepted { .. }
                        ));
                    }
                    let rows = sqlx_core::query::query::<sqlx_turso::Turso>(include_str!(
                        "../support/sql/read_outbox_headers.sql"
                    ))
                    .fetch_all(scope.executor())
                    .await?;
                    assert_eq!(rows.len(), 2);
                    let original: serde_json::Value =
                        serde_json::from_str(&rows[0].try_get::<String, _>("record")?).unwrap();
                    assert_eq!(original["value"]["settlementBytes"], before - charge);
                    assert_eq!(rows[1].try_get::<i64, _>("settlement_bytes")?, 0);
                    let control: serde_json::Value =
                        serde_json::from_str(&rows[1].try_get::<String, _>("record")?).unwrap();
                    assert_eq!(control["value"]["acceptanceBound"], 64);
                    Ok(())
                })
            })
            .await
            .unwrap();
    }
}
