#![allow(clippy::unwrap_used)]
mod support;
use durable_steps::AtomicStore;
use fleet_sync_delivery::*;
use std::sync::{Arc, atomic::Ordering};
use support::{network::*, *};

fn held_publication(item: u64, version: u64) -> StatusPublication<Acceptance> {
    StatusPublication {
        item: item_id(item),
        status: DeliveryStatus::Held,
        version: StatusVersion::new(version),
        cancel: None,
    }
}

async fn append_status_event(fixture: &Fixture, item: u64, version: u64) -> StatusCursor {
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let (event, _) =
                    StatusStream::<_, TestTypes>::new(scope, peer(1), "fixture-route".into())
                        .append(&held_publication(item, version))
                        .await
                        .map_err(storage_error)?;
                Ok(event.cursor)
            })
        })
        .await
        .unwrap()
}

async fn acknowledge_events(fixture: &Fixture, through: u64) -> StatusAckResult {
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                StatusStream::<_, TestTypes>::new(scope, peer(1), "fixture-route".into())
                    .acknowledge(StatusCursor::new(through))
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap()
}

async fn fetch_status_event_identities(
    fixture: &Fixture,
    after: u64,
) -> Vec<(StatusCursor, ItemId, StatusVersion)> {
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let batch =
                    StatusStream::<_, TestTypes>::new(scope, peer(1), "fixture-route".into())
                        .fetch(peer(1), StatusCursor::new(after), 20)
                        .await
                        .map_err(storage_error)?;
                Ok(batch
                    .events
                    .into_iter()
                    .map(|event| (event.cursor, event.item, event.version))
                    .collect())
            })
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn acknowledgement_retires_only_the_fetched_prefix_and_later_appends_remain_visible() {
    let fixture = Fixture::new().await;
    for item in 1..=3 {
        assert_eq!(
            append_status_event(&fixture, item, 1).await,
            StatusCursor::new(item)
        );
    }
    assert_eq!(fetch_status_event_identities(&fixture, 0).await.len(), 3);
    assert_eq!(
        acknowledge_events(&fixture, 2).await,
        StatusAckResult::Acknowledged {
            floor: StatusCursor::new(2)
        }
    );
    assert_eq!(
        fetch_status_event_identities(&fixture, 0).await,
        vec![(StatusCursor::new(3), item_id(3), StatusVersion::new(1))]
    );
    assert_eq!(
        append_status_event(&fixture, 4, 1).await,
        StatusCursor::new(4)
    );
    assert_eq!(
        fetch_status_event_identities(&fixture, 0).await,
        vec![
            (StatusCursor::new(3), item_id(3), StatusVersion::new(1)),
            (StatusCursor::new(4), item_id(4), StatusVersion::new(1)),
        ]
    );
}

#[tokio::test]
async fn equal_and_lower_acknowledgements_keep_the_floor_across_reopen() {
    let fixture = Fixture::new().await;
    for item in 1..=3 {
        append_status_event(&fixture, item, 1).await;
    }
    acknowledge_events(&fixture, 2).await;
    let expected = fetch_status_event_identities(&fixture, 0).await;
    for through in [2, 1, 0] {
        assert_eq!(
            acknowledge_events(&fixture, through).await,
            StatusAckResult::Acknowledged {
                floor: StatusCursor::new(2)
            }
        );
        assert_eq!(fetch_status_event_identities(&fixture, 0).await, expected);
    }
    let Fixture {
        store,
        host,
        directory,
    } = fixture;
    assert_eq!(Arc::strong_count(&store), 1);
    drop(store);
    let store = Arc::new(
        TestStore::open_and_migrate(directory.path().join("delivery.db"), host.clone())
            .await
            .unwrap(),
    );
    let fixture = Fixture {
        store,
        host,
        directory,
    };
    for through in [2, 1] {
        assert_eq!(
            acknowledge_events(&fixture, through).await,
            StatusAckResult::Acknowledged {
                floor: StatusCursor::new(2)
            }
        );
        assert_eq!(fetch_status_event_identities(&fixture, 0).await, expected);
    }
}

#[tokio::test]
async fn above_head_acknowledgement_is_diagnostic_and_preserves_unread_events_and_floor() {
    let fixture = Fixture::new().await;
    for item in 1..=3 {
        append_status_event(&fixture, item, 1).await;
    }
    acknowledge_events(&fixture, 1).await;
    let unread = fetch_status_event_identities(&fixture, 1).await;
    assert_eq!(
        unread,
        vec![
            (StatusCursor::new(2), item_id(2), StatusVersion::new(1)),
            (StatusCursor::new(3), item_id(3), StatusVersion::new(1)),
        ]
    );
    assert_eq!(
        acknowledge_events(&fixture, 4).await,
        StatusAckResult::Refused {
            head: StatusCursor::new(3)
        }
    );
    assert_eq!(fetch_status_event_identities(&fixture, 0).await, unread);
    assert_eq!(
        acknowledge_events(&fixture, 0).await,
        StatusAckResult::Acknowledged {
            floor: StatusCursor::new(1)
        }
    );
    if let StatusAckResult::Refused { head } = acknowledge_events(&fixture, u64::MAX).await {
        assert_eq!(head, StatusCursor::new(3));
    } else {
        panic!("out-of-range acknowledgement was accepted");
    }
    assert_eq!(fetch_status_event_identities(&fixture, 1).await, unread);
}

#[tokio::test]
async fn empty_stream_acknowledgement_refuses_future_cursors_and_keeps_zero_floor() {
    let fixture = Fixture::new().await;
    assert_eq!(
        acknowledge_events(&fixture, 1).await,
        StatusAckResult::Refused {
            head: StatusCursor::new(0)
        }
    );
    assert_eq!(
        acknowledge_events(&fixture, 0).await,
        StatusAckResult::Acknowledged {
            floor: StatusCursor::new(0)
        }
    );
    assert_eq!(
        append_status_event(&fixture, 1, 1).await,
        StatusCursor::new(1)
    );
    assert_eq!(fetch_status_event_identities(&fixture, 0).await.len(), 1);
}

#[tokio::test]
async fn lost_first_hop_retry_after_ack_reappends_above_floor_and_forward_only_consumer_ignores_old_version()
 {
    let network = NetworkFixture::new().await;
    network.enqueue(1, "body").await;
    drain(&network.engine(&network.sender)).await;
    network
        .routes
        .lose_publication
        .store(true, Ordering::SeqCst);
    let lost = network.engine(&network.mailbox).drive_due().await.unwrap();
    assert_eq!(lost.retried.len(), 1);
    assert!(lost.failed.is_empty());
    let first = fetch_status_event_identities(&network.status, 0).await;
    assert_eq!(
        first,
        vec![(StatusCursor::new(1), item_id(1), StatusVersion::new(1))]
    );
    assert_eq!(
        acknowledge_events(&network.status, 1).await,
        StatusAckResult::Acknowledged {
            floor: StatusCursor::new(1)
        }
    );
    network
        .sender
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Outbox::<_, TestTypes>::new(scope, limits())
                    .publish_status(held_publication(1, 2))
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    network.advance();
    drain(&network.engine(&network.mailbox)).await;
    assert_eq!(
        fetch_status_event_identities(&network.status, 0).await,
        vec![(StatusCursor::new(2), item_id(1), StatusVersion::new(1))]
    );
    drain(&network.engine(&network.status)).await;
    assert!(
        matches!(sender_status(&network, 1).await, OutboxStatusState::Known {
        status: DeliveryStatus::Held, version, ..
    } if version == StatusVersion::new(2))
    );
}

#[tokio::test]
async fn empty_fetch_preserves_the_requested_cursor_when_the_floor_is_higher() {
    let fixture = Fixture::new().await;
    append_status_event(&fixture, 1, 1).await;
    assert_eq!(
        acknowledge_events(&fixture, 1).await,
        StatusAckResult::Acknowledged {
            floor: StatusCursor::new(1)
        }
    );
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let batch =
                    StatusStream::<_, TestTypes>::new(scope, peer(1), "fixture-route".into())
                        .fetch(peer(1), StatusCursor::new(0), 10)
                        .await
                        .map_err(storage_error)?;
                assert!(batch.events.is_empty());
                assert_eq!(batch.through, StatusCursor::new(0));
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn a_stored_acknowledged_floor_above_head_is_corrupt() {
    use durable_steps::storage::TransactionScope;
    use sqlx_core::row::Row;
    let fixture = Fixture::new().await;
    append_status_event(&fixture, 1, 1).await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT record FROM _fleet_sync_status_state",
                )
                .fetch_one(scope.executor())
                .await?;
                let mut state: serde_json::Value =
                    serde_json::from_str(&row.try_get::<String, _>("record")?).unwrap();
                state["floor"] = serde_json::json!(2);
                sqlx_core::query::query::<sqlx_turso::Turso>(
                    "UPDATE _fleet_sync_status_state SET record=?",
                )
                .bind(state.to_string())
                .execute(scope.executor())
                .await?;
                let result =
                    StatusStream::<_, TestTypes>::new(scope, peer(1), "fixture-route".into())
                        .acknowledge(StatusCursor::new(1))
                        .await;
                assert!(matches!(result, Err(DeliveryError::Corrupt { .. })));
                Ok(())
            })
        })
        .await
        .unwrap();
}
