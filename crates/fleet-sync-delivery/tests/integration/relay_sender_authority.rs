use super::support::{
    network::{NetworkFixture, PublishTask, PushTask, sender_status},
    *,
};
use durable_steps::{AtomicStore, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::sync::{Arc, atomic::Ordering};

fn now() -> SyncTime {
    SyncTime::from_unix_millis(0)
}

fn digest(identity: u64) -> ItemDigest {
    item(identity, "owned body").digest().clone()
}

async fn admit(store: &TestStore, caller: u128, identity: u64) -> SubmitResult<Acceptance> {
    store
        .transaction(move |scope| {
            Box::pin(async move {
                Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .submit_relay::<PushTask, RecordRemoval>(
                        fleet_sync_delivery::Submitter::Machine(peer(caller)),
                        item(identity, "owned body"),
                        peer(3),
                        "fixture-route".into(),
                        now(),
                    )
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap()
}
async fn settle(store: &TestStore, caller: u128, identity: u64) -> SettleResult<Acceptance> {
    store
        .transaction(move |scope| {
            Box::pin(async move {
                Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .relay_settle::<RecordRemoval>(
                        fleet_sync_delivery::Submitter::Machine(peer(caller)),
                        item_id(identity),
                        digest(identity),
                        now(),
                    )
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap()
}
async fn snapshot(store: &TestStore) -> Vec<String> {
    store.transaction(|scope|Box::pin(async move {
        Ok(query::<Turso>("SELECT json_array(item_id,digest,submitter_kind,submitter_id,record_kind,status_kind,status_version,cancel_outcome,payload,updated_at) AS state FROM _fleet_sync_outbox_items ORDER BY item_id")
            .fetch_all(scope.executor()).await?.iter().map(|row|row.try_get("state")).collect::<Result<_,_>>()?)
    })).await.unwrap()
}

#[tokio::test]
async fn query_and_settle_keep_submitter_authority_after_content_release() {
    let fixture = Fixture::new().await;
    assert!(matches!(
        admit(&fixture.store, 1, 1).await,
        SubmitResult::Accepted { .. }
    ));
    for released in [false, true] {
        if released {
            fixture
                .store
                .transaction(|scope| {
                    Box::pin(async move {
                        Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                            .receipt::<PublishTask>(
                                peer(2),
                                &DeliveryItemRef {
                                    item: item_id(1),
                                    digest: digest(1),
                                },
                                PushResult::Accepted { accepted_at: now() },
                                now(),
                            )
                            .await
                            .map_err(storage_error)?;
                        let row = query::<Turso>("SELECT payload FROM _fleet_sync_outbox_items")
                            .fetch_one(scope.executor())
                            .await?;
                        assert!(row.try_get::<Option<String>, _>("payload")?.is_none());
                        Ok(())
                    })
                })
                .await
                .unwrap();
        }
        let before = snapshot(&fixture.store).await;
        fixture
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    let mut relay =
                        Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings);
                    let owned = relay
                        .relay_query(fleet_sync_delivery::Submitter::Machine(peer(1)), item_id(1))
                        .await
                        .map_err(storage_error)?;
                    assert!(matches!(
                        &owned.state,
                        ItemStatusState::Known { digest: stored, .. } if stored == &digest(1)
                    ));
                    assert!(
                        matches!(
                            owned.state,
                            ItemStatusState::Known {
                                status: DeliveryStatus::Sent,
                                ..
                            }
                        ) && !released
                            || matches!(
                                owned.state,
                                ItemStatusState::Known {
                                    status: DeliveryStatus::AtRecipient,
                                    ..
                                }
                            ) && released
                    );
                    assert!(matches!(
                        relay
                            .relay_query(
                                fleet_sync_delivery::Submitter::Machine(peer(9)),
                                item_id(1)
                            )
                            .await,
                        Err(DeliveryError::Refused {
                            reason: DeliveryRefusal::NotSubmitter
                        })
                    ));
                    assert!(matches!(
                        relay
                            .relay_settle::<RecordRemoval>(
                                fleet_sync_delivery::Submitter::Machine(peer(9)),
                                item_id(1),
                                digest(1),
                                now()
                            )
                            .await
                            .map_err(storage_error)?,
                        SettleResult::ItemConflict { .. }
                    ));
                    assert!(matches!(
                        relay
                            .relay_settle::<RecordRemoval>(
                                fleet_sync_delivery::Submitter::Machine(peer(1)),
                                item_id(1),
                                digest(2),
                                now()
                            )
                            .await
                            .map_err(storage_error)?,
                        SettleResult::ItemConflict { .. }
                    ));
                    assert!(matches!(
                        relay
                            .relay_settle::<RecordRemoval>(
                                fleet_sync_delivery::Submitter::Machine(peer(1)),
                                item_id(1),
                                digest(1),
                                now()
                            )
                            .await
                            .map_err(storage_error)?,
                        SettleResult::Replayed { .. }
                    ));
                    Ok(())
                })
            })
            .await
            .unwrap();
        assert_eq!(
            snapshot(&fixture.store).await,
            before,
            "queries, refusals and replay must not mutate custody"
        );
    }
}

#[tokio::test]
async fn first_absent_settlement_binds_the_caller_and_survives_reopen() {
    let Fixture {
        store,
        host,
        directory,
    } = Fixture::new().await;
    store
        .transaction(|scope| {
            Box::pin(async move {
                let result = Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .relay_query(fleet_sync_delivery::Submitter::Machine(peer(9)), item_id(1))
                    .await
                    .map_err(storage_error)?;
                assert!(matches!(result.state, ItemStatusState::Unknown));
                Ok(())
            })
        })
        .await
        .unwrap();
    assert!(snapshot(&store).await.is_empty());
    assert!(
        matches!(settle(&store,1,1).await,SettleResult::Settled{reason:DeliveryFailureKind::CustodyUnresolvable,version} if version.value()==1)
    );
    let before = snapshot(&store).await;
    drop(store);
    let store = Arc::new(
        TestStore::open_and_migrate(directory.path().join("delivery.db"), host)
            .await
            .unwrap(),
    );
    store
        .transaction(|scope| {
            Box::pin(async move {
                let mut relay = Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings);
                let owned = relay
                    .relay_query(fleet_sync_delivery::Submitter::Machine(peer(1)), item_id(1))
                    .await
                    .map_err(storage_error)?;
                assert!(matches!(
                    owned.state,
                    ItemStatusState::Known { digest: stored, .. } if stored == digest(1)
                ));
                assert!(matches!(
                    relay
                        .relay_query(fleet_sync_delivery::Submitter::Machine(peer(9)), item_id(1))
                        .await,
                    Err(DeliveryError::Refused {
                        reason: DeliveryRefusal::NotSubmitter
                    })
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
    assert!(matches!(
        admit(&store, 9, 1).await,
        SubmitResult::Refused {
            reason: DeliveryRefusal::NotSubmitter
        }
    ));
    assert!(matches!(
        settle(&store, 9, 1).await,
        SettleResult::ItemConflict { .. }
    ));
    assert!(
        matches!(settle(&store,1,1).await,SettleResult::Replayed{status:DeliveryStatus::Refused{reason:DeliveryRefusal::ProcessingFailed{kind:DeliveryFailureKind::CustodyUnresolvable}},version,cancel:None} if version.value()==1)
    );
    assert!(matches!(
        admit(&store, 1, 1).await,
        SubmitResult::Replayed { .. }
    ));
    assert_eq!(snapshot(&store).await, before);
}

#[tokio::test]
async fn unbound_recovery_states_bind_once_without_changing_saved_outcomes() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut outbox = Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings);
                outbox
                    .mark_submitted(
                        &SubmitInput {
                            item: item_id(1),
                            digest: digest(1),
                            relay: peer(3),
                            mailbox: peer(2),
                        },
                        SubmitResult::Accepted {
                            status: DeliveryStatus::Sent,
                            version: StatusVersion::new(4),
                            cancel: None,
                        },
                        now(),
                    )
                    .await
                    .map_err(storage_error)?;
                outbox
                    .record_failure(item_id(2), DeliveryFailureKind::CustodyCorrupt, now())
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    for identity in [1, 2] {
        fixture
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    assert!(matches!(
                        Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                            .relay_query(
                                fleet_sync_delivery::Submitter::Machine(peer(1)),
                                item_id(identity)
                            )
                            .await,
                        Err(DeliveryError::Refused {
                            reason: DeliveryRefusal::NotSubmitter
                        })
                    ));
                    Ok(())
                })
            })
            .await
            .unwrap();
        let bound = settle(&fixture.store, 1, identity).await;
        assert!(match bound {
            SettleResult::Replayed {
                status: DeliveryStatus::Sent,
                version,
                cancel: None,
            } => identity == 1 && version.value() == 4,
            SettleResult::Replayed {
                status:
                    DeliveryStatus::Refused {
                        reason:
                            DeliveryRefusal::ProcessingFailed {
                                kind: DeliveryFailureKind::CustodyCorrupt,
                            },
                    },
                version,
                cancel: None,
            } => identity == 2 && version.value() == 0,
            _ => false,
        });
        let before = snapshot(&fixture.store).await;
        assert!(matches!(
            settle(&fixture.store, 9, identity).await,
            SettleResult::ItemConflict { .. }
        ));
        assert!(matches!(
            settle(&fixture.store, 1, identity).await,
            SettleResult::Replayed { .. }
        ));
        assert_eq!(snapshot(&fixture.store).await, before);
    }
}

#[tokio::test]
async fn a_failed_caller_transaction_rolls_back_the_first_submitter_binding() {
    let fixture = Fixture::new().await;
    let aborted = fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .relay_settle::<RecordRemoval>(
                        fleet_sync_delivery::Submitter::Machine(peer(1)),
                        item_id(1),
                        digest(1),
                        now(),
                    )
                    .await
                    .map_err(storage_error)?;
                Err::<(), _>(StoreError::Aborted {
                    reason: "caller rollback".into(),
                })
            })
        })
        .await;
    assert!(matches!(aborted, Err(StoreError::Aborted { .. })));
    assert!(snapshot(&fixture.store).await.is_empty());
    assert!(matches!(
        settle(&fixture.store, 9, 1).await,
        SettleResult::Settled { .. }
    ));
    assert!(matches!(
        settle(&fixture.store, 1, 1).await,
        SettleResult::ItemConflict { .. }
    ));
}

#[tokio::test]
async fn missing_or_corrupt_sender_custody_reaches_the_existing_unresolvable_outcome() {
    for corrupt in [false, true] {
        let network = NetworkFixture::new().await;
        network.routes.fleet_push.store(true, Ordering::SeqCst);
        network.enqueue(1, "owned body").await;
        network
            .sender
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    let sql = if corrupt {
                        "UPDATE _fleet_sync_outbox_items SET payload='corrupt',payload_bytes=7"
                    } else {
                        "DELETE FROM _fleet_sync_outbox_items"
                    };
                    query::<Turso>(sql).execute(scope.executor()).await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        let report = network.engine(&network.sender).drive_due().await.unwrap();
        assert_eq!(report.failed.len(), 1);
        assert_eq!(network.routes.settlements.load(Ordering::SeqCst), 1);
        assert_eq!(network.routes.sent_bodies.load(Ordering::SeqCst), 0);
        assert!(matches!(
            sender_status(&network, 1).await,
            OutboxStatusState::Known {
                status: DeliveryStatus::Refused {
                    reason: DeliveryRefusal::ProcessingFailed {
                        kind: DeliveryFailureKind::CustodyUnresolvable
                    }
                },
                ..
            }
        ));
        assert!(matches!(
            settle(&network.mailbox.store, 9, 1).await,
            SettleResult::ItemConflict { .. }
        ));
    }
}

#[tokio::test]
async fn cancellation_query_and_settlement_replay_the_saved_cancel_result() {
    let fixture = Fixture::new().await;
    admit(&fixture.store, 1, 1).await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .submit_relay::<PushTask, RecordRemoval>(
                        fleet_sync_delivery::Submitter::Machine(peer(1)),
                        cancel(2, 1),
                        peer(3),
                        "fixture-route".into(),
                        now(),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    for completed in [false, true] {
        fixture
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    let mut relay =
                        Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings);
                    if completed {
                        relay
                            .recipient_report::<PublishTask>(
                                peer(2),
                                RecipientReport::<TestTypes> {
                                    item: item_id(2),
                                    report_seq: ReportSeq::new(1),
                                    outcome: RecipientOutcome::Prevented { cancel: item_id(2) },
                                },
                                now(),
                            )
                            .await
                            .map_err(storage_error)?;
                    }
                    let digest = cancel(2, 1).digest().clone();
                    let queried = relay
                        .relay_query(fleet_sync_delivery::Submitter::Machine(peer(1)), item_id(2))
                        .await
                        .map_err(storage_error)?;
                    let settled = relay
                        .relay_settle::<RecordRemoval>(
                            fleet_sync_delivery::Submitter::Machine(peer(1)),
                            item_id(2),
                            digest,
                            now(),
                        )
                        .await
                        .map_err(storage_error)?;
                    let expected = if completed {
                        CancelResult::Prevented
                    } else {
                        CancelResult::Pending
                    };
                    let ItemStatusState::Known {
                        status,
                        version,
                        cancel,
                        ..
                    } = queried.state
                    else {
                        return Err(StoreError::backend("known cancel missing"));
                    };
                    assert_eq!(cancel, Some(expected));
                    let SettleResult::Replayed {
                        status: settled_status,
                        version: settled_version,
                        cancel: settled_cancel,
                    } = settled
                    else {
                        return Err(StoreError::backend("cancel settlement was not replayed"));
                    };
                    assert_eq!(settled_version, version);
                    assert_eq!(settled_cancel, Some(expected));
                    assert_eq!(
                        serde_json::to_value(status).unwrap(),
                        serde_json::to_value(settled_status).unwrap()
                    );
                    Ok(())
                })
            })
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn settlement_at_removal_deadline_cannot_recreate_custody() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let deadline = removal_deadline(item_id(1));
                let result = Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .relay_settle::<RecordRemoval>(
                        fleet_sync_delivery::Submitter::Machine(peer(1)),
                        item_id(1),
                        digest(1),
                        deadline,
                    )
                    .await;
                assert!(matches!(
                    result,
                    Err(DeliveryError::Refused {
                        reason: DeliveryRefusal::TooOld
                    })
                ));
                let row = query::<Turso>("SELECT count(*) AS count FROM _fleet_sync_operations")
                    .fetch_one(scope.executor())
                    .await?;
                assert_eq!(row.try_get::<i64, _>("count")?, 0);
                Ok(())
            })
        })
        .await
        .unwrap();
    assert!(snapshot(&fixture.store).await.is_empty());
}
