#![allow(clippy::unwrap_used)]
mod support;
use durable_steps::{AtomicStore, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::row::Row;
use support::*;
#[path = "support/per_item.rs"]
mod per_item;
use per_item::*;

#[tokio::test]
async fn ordinary_admission_uses_small_bound_while_large_kind_reserves_global() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let small_capacity = variable_limits(50_000);
                for identity in 1..=3 {
                    assert!(matches!(
                        Outbox::<_, VariableTypes>::new(scope, small_capacity)
                            .enqueue(variable_item(identity, "small"), peer(3))
                            .await
                            .map_err(storage_error)?,
                        OutboxAdmission::Queued { .. }
                    ));
                    assert!(matches!(
                        Mailbox::<_, VariableTypes>::new(scope, peer(2), small_capacity)
                            .submit(
                                peer(1),
                                DeliveryEnvelope::Item(variable_item(identity, "small")),
                                now(),
                                "route".into(),
                                None
                            )
                            .await
                            .map_err(storage_error)?,
                        SubmitResult::Accepted { .. }
                    ));
                }
                assert!(matches!(
                    Outbox::<_, VariableTypes>::new(scope, small_capacity)
                        .enqueue(variable_item(4, "large"), peer(3))
                        .await
                        .map_err(storage_error)?,
                    OutboxAdmission::Refused {
                        reason: DeliveryRefusal::MailboxFull
                    }
                ));
                assert!(matches!(
                    Mailbox::<_, VariableTypes>::new(scope, peer(2), small_capacity)
                        .submit(
                            peer(1),
                            DeliveryEnvelope::Item(variable_item(4, "large")),
                            now(),
                            "route".into(),
                            None
                        )
                        .await
                        .map_err(storage_error)?,
                    SubmitResult::Refused {
                        reason: DeliveryRefusal::MailboxFull
                    }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn invalid_body_bound_is_refused_before_any_custody_write() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert!(matches!(
                    Outbox::<_, VariableTypes>::new(scope, variable_limits(100_000_000))
                        .enqueue(variable_item(1, "invalid"), peer(3))
                        .await,
                    Err(DeliveryError::Config(_))
                ));
                assert!(matches!(
                    Mailbox::<_, VariableTypes>::new(scope, peer(2), variable_limits(100_000_000))
                        .submit(
                            peer(1),
                            DeliveryEnvelope::Item(variable_item(1, "invalid")),
                            now(),
                            "route".into(),
                            None
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
                Outbox::<_, VariableTypes>::new(scope, limits)
                    .enqueue(variable_item(1, "small"), peer(3))
                    .await
                    .map_err(storage_error)?;
                Outbox::<_, VariableTypes>::new(scope, limits)
                    .publish_status(StatusPublication {
                        item: item_id(1),
                        status: DeliveryStatus::Sent {
                            seq: DeliverySeq::new(1),
                        },
                        version: StatusVersion::new(1),
                        cancel: None,
                    })
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
                    Outbox::<_, VariableTypes>::new(scope, limits)
                        .publish_status(oversized)
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
async fn mixed_fetch_uses_actual_reserves_and_stops_exactly_at_room_boundary() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let limits = variable_limits(100_000_000);
                for (identity, body) in [(1, "small"), (2, "large"), (3, "small")] {
                    Mailbox::<_, VariableTypes>::new(scope, peer(2), limits)
                        .submit(
                            peer(1),
                            DeliveryEnvelope::Item(variable_item(identity, body)),
                            now(),
                            "route".into(),
                            None,
                        )
                        .await
                        .map_err(storage_error)?;
                }
                let rows = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT settlement_bytes FROM _fleet_sync_mail_items ORDER BY item_id",
                )
                .fetch_all(scope.executor())
                .await?;
                let mut first_two_room = 0;
                for (index, body) in ["small", "large"].into_iter().enumerate() {
                    let seq = index as u64 + 1;
                    let entry = FetchEntry::Item {
                        seq: DeliverySeq::new(seq),
                        item: DeliveryEnvelope::Item(variable_item(seq, body)),
                    };
                    first_two_room += serde_json::to_vec(&entry).unwrap().len() as u64
                        + rows[index].try_get::<i64, _>("settlement_bytes")? as u64;
                }
                let request = FetchInput {
                    stream: DeliveryStream::Items,
                    after: DeliverySeq::new(0),
                    max_items: 10,
                    max_bytes: 100_000_000,
                    inbox_room_bytes: Some(first_two_room - 1),
                };
                let below = Mailbox::<_, VariableTypes>::new(scope, peer(2), limits)
                    .fetch_with_budget(peer(2), request, now())
                    .await
                    .map_err(storage_error)?;
                assert_eq!(below.entries.len(), 1);
                let exact = Mailbox::<_, VariableTypes>::new(scope, peer(2), limits)
                    .fetch_with_budget(
                        peer(2),
                        FetchInput {
                            inbox_room_bytes: Some(first_two_room),
                            ..request
                        },
                        now(),
                    )
                    .await
                    .map_err(storage_error)?;
                assert_eq!(exact.entries.len(), 2);
                assert_eq!(exact.through, DeliverySeq::new(2));
                let mut inbox = Inbox::<_, VariableTypes>::new(scope, peer(2), limits);
                let cycle = inbox
                    .begin_receive(DeliveryStream::Items)
                    .await
                    .map_err(storage_error)?
                    .unwrap();
                let budget = inbox
                    .receive_request(cycle, 10, 100_000_000)
                    .await
                    .map_err(storage_error)?;
                assert_eq!(budget.max_items, 10);
                assert_eq!(budget.inbox_room_bytes, Some(100_000_000));
                let effects = inbox
                    .store_batch(cycle, exact, &VariableConsumer, now())
                    .await
                    .map_err(storage_error)?;
                assert_eq!(effects.attempts.len(), 2);
                let rows = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT settlement_bytes,record FROM _fleet_sync_inbox_items ORDER BY item_id",
                )
                .fetch_all(scope.executor())
                .await?;
                assert!(rows[0].try_get::<i64, _>("settlement_bytes")? < 50_000);
                assert!(rows[1].try_get::<i64, _>("settlement_bytes")? > 1_049_600);
                let header: serde_json::Value =
                    serde_json::from_str(&rows[0].try_get::<String, _>("record")?).unwrap();
                assert_eq!(header["acceptanceBound"], 64);
                // A head item larger than total room stays unfetched, as with the legacy byte cap.
                let stalled = Mailbox::<_, VariableTypes>::new(scope, peer(2), limits)
                    .fetch_with_budget(
                        peer(2),
                        FetchInput {
                            after: DeliverySeq::new(1),
                            inbox_room_bytes: Some(50_000),
                            ..request
                        },
                        now(),
                    )
                    .await
                    .map_err(storage_error)?;
                assert!(stalled.entries.is_empty());
                assert_eq!(stalled.through, DeliverySeq::new(1));
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn per_item_bound_survives_reopen_and_legacy_absence_uses_global() {
    let Fixture {
        store,
        host,
        directory,
    } = Fixture::new().await;
    store.transaction(|scope| Box::pin(async move {
        let limits=variable_limits(100_000_000);
        for identity in [1,2] {
            Outbox::<_,VariableTypes>::new(scope,limits).enqueue(variable_item(identity,"small"),peer(3)).await.map_err(storage_error)?;
            Outbox::<_,VariableTypes>::new(scope,limits).publish_status(StatusPublication { item:item_id(identity),status:DeliveryStatus::AtRecipient,version:StatusVersion::new(1),cancel:None }).await.map_err(storage_error)?;
        }
        // Emulate the exact old JSON shape, including the released body.
        sqlx_core::query::query::<sqlx_turso::Turso>("UPDATE _fleet_sync_outbox_items SET record=json_remove(record,'$.value.acceptanceBound') WHERE item_id=?1").bind(item_id(2).as_uuid().to_string()).execute(scope.executor()).await?;
        Ok(())
    })).await.unwrap();
    drop(store);
    let reopened = TestStore::open_and_migrate(directory.path().join("delivery.db"), host)
        .await
        .unwrap();
    reopened
        .transaction(|scope| {
            Box::pin(async move {
                let limits = variable_limits(100_000_000);
                for identity in [1, 2] {
                    let result = Outbox::<_, VariableTypes>::new(scope, limits)
                        .publish_status(StatusPublication {
                            item: item_id(identity),
                            status: DeliveryStatus::Delivered {
                                acceptance: Acceptance {
                                    receipt: "x".repeat(100),
                                },
                            },
                            version: StatusVersion::new(2),
                            cancel: None,
                        })
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
                let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT record FROM _fleet_sync_outbox_items WHERE item_id=?1",
                )
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
async fn relay_validates_stored_bound_after_received_releases_body() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let limits = variable_limits(100_000_000);
                let mut mailbox = Mailbox::<_, VariableTypes>::new(scope, peer(2), limits);
                mailbox
                    .submit(
                        peer(1),
                        DeliveryEnvelope::Item(variable_item(1, "small")),
                        now(),
                        "route".into(),
                        None,
                    )
                    .await
                    .map_err(storage_error)?;
                let batch = mailbox
                    .fetch(
                        peer(2),
                        DeliveryStream::Items,
                        DeliverySeq::new(0),
                        1,
                        10_000,
                        now(),
                    )
                    .await
                    .map_err(storage_error)?;
                mailbox
                    .received(peer(2), DeliveryStream::Items, batch.through, now())
                    .await
                    .map_err(storage_error)?;
                let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT record FROM _fleet_sync_mail_items",
                )
                .fetch_one(scope.executor())
                .await?;
                let record: serde_json::Value =
                    serde_json::from_str(&row.try_get::<String, _>("record")?).unwrap();
                assert!(record["value"]["payload"].is_null());
                assert_eq!(record["value"]["acceptanceBound"], 64);
                assert!(matches!(
                    Mailbox::<_, VariableTypes>::new(scope, peer(2), limits)
                        .report(
                            peer(2),
                            RecipientReport {
                                item: item_id(1),
                                report_seq: ReportSeq::new(1),
                                outcome: RecipientOutcome::Delivered {
                                    acceptance: Acceptance {
                                        receipt: "x".repeat(100)
                                    }
                                }
                            }
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
    let fixture = Fixture::new().await;
    fixture.store.transaction(|scope| Box::pin(async move {
        let limits=variable_limits(50_000);
        Outbox::<_,VariableTypes>::new(scope,limits).enqueue(variable_item(1,"small"),peer(3)).await.map_err(storage_error)?;
        Mailbox::<_,VariableTypes>::new(scope,peer(2),limits).submit(peer(1),DeliveryEnvelope::Item(variable_item(1,"small")),now(),"route".into(),None).await.map_err(storage_error)?;
        for owner in ["outbox","mail"] {
            let query=if owner=="outbox" { "SELECT settlement_bytes FROM _fleet_sync_outbox_items" } else { "SELECT settlement_bytes FROM _fleet_sync_mail_items" };
            let before=sqlx_core::query::query::<sqlx_turso::Turso>(query).fetch_one(scope.executor()).await?.try_get::<i64,_>("settlement_bytes")?;
            let control=DeliveryCancel::<VariableTypes>::new(item_id(2),"sender".into(),Recipient {peer:peer(2),address:"inbox".into()},None,CancelBody { target:item_id(1) }).unwrap();
            let charge=serde_json::to_vec(&DeliveryEnvelope::Cancel(DeliveryCancel::<VariableTypes>::new(item_id(2),"sender".into(),Recipient {peer:peer(2),address:"inbox".into()},None,CancelBody { target:item_id(1) }).unwrap())).unwrap().len() as i64;
            if owner=="outbox" { assert!(matches!(Outbox::<_,VariableTypes>::new(scope,limits).enqueue_cancel(control,peer(3),&VariableConsumer).await.map_err(storage_error)?,OutboxAdmission::Queued { .. })); }
            else { assert!(matches!(Mailbox::<_,VariableTypes>::new(scope,peer(2),limits).submit(peer(1),DeliveryEnvelope::Cancel(control),now(),"route".into(),None).await.map_err(storage_error)?,SubmitResult::Accepted { .. })); }
            let query=if owner=="outbox" { "SELECT settlement_bytes,record FROM _fleet_sync_outbox_items ORDER BY item_id" } else { "SELECT settlement_bytes,record FROM _fleet_sync_mail_items ORDER BY item_id" };
            let rows=sqlx_core::query::query::<sqlx_turso::Turso>(query).fetch_all(scope.executor()).await?;
            assert_eq!(rows.len(),2);
            // The withdrawn sender's SQL charge is zero; its durable balance still pays the control.
            let original:serde_json::Value=serde_json::from_str(&rows[0].try_get::<String,_>("record")?).unwrap();
            assert_eq!(original["value"]["settlementBytes"],before-charge);
            assert_eq!(rows[1].try_get::<i64,_>("settlement_bytes")?,0);
            let cancel:serde_json::Value=serde_json::from_str(&rows[1].try_get::<String,_>("record")?).unwrap();
            assert_eq!(cancel["value"]["acceptanceBound"],64);
        }
        Ok(())
    })).await.unwrap();
}
