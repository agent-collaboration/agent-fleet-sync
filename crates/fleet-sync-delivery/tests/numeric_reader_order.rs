#![allow(clippy::unwrap_used)]
mod support;
use durable_steps::AtomicStore;
use fleet_sync_delivery::*;
use support::*;

const COUNT: u64 = 101;

#[tokio::test]
async fn status_fetch_keeps_numeric_cursor_order_across_both_decimal_boundaries() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut stream = StatusStream::<_, TestTypes>::new(
                    scope,
                    fleet_sync_delivery::Submitter::Machine(peer(1)),
                    "route".into(),
                );
                for identity in 1..=COUNT {
                    stream
                        .append(
                            &StatusPublication {
                                item: item_id(identity),
                                status: DeliveryStatus::Held,
                                version: StatusVersion::new(1),
                                cancel: None,
                            },
                            fleet_sync_protocol::SyncTime::from_unix_millis(0),
                        )
                        .await
                        .map_err(storage_error)?;
                }
                let result = stream
                    .fetch(
                        fleet_sync_delivery::Submitter::Machine(peer(1)),
                        StatusCursor::new(0),
                        101,
                    )
                    .await
                    .map_err(storage_error)?;
                assert_eq!(
                    result
                        .events
                        .iter()
                        .map(|event| event.cursor.value())
                        .collect::<Vec<_>>(),
                    (1..=COUNT).collect::<Vec<_>>()
                );
                assert_eq!(result.through.value(), COUNT);
                Ok(())
            })
        })
        .await
        .unwrap();
}
