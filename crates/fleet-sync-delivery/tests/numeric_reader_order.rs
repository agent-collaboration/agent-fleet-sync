#![allow(clippy::unwrap_used)]
mod support;
use durable_steps::AtomicStore;
use fleet_sync_delivery::*;
use support::*;

const COUNT: u64 = 101;

fn batch_limits() -> DeliveryLimits {
    DeliveryLimits::new::<TestTypes>(
        10_000_000,
        200,
        10_000_000,
        200,
        10_000_000,
        minimum_settlement_allowance::<TestTypes>().unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn status_fetch_keeps_numeric_cursor_order_across_both_decimal_boundaries() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut stream = StatusStream::<_, TestTypes>::new(scope, peer(1), "route".into());
                for identity in 1..=COUNT {
                    stream
                        .append(&StatusPublication {
                            item: item_id(identity),
                            status: DeliveryStatus::Held,
                            version: StatusVersion::new(1),
                            cancel: None,
                        })
                        .await
                        .map_err(storage_error)?;
                }
                let result = stream
                    .fetch(peer(1), StatusCursor::new(0), 101)
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

#[tokio::test]
async fn mailbox_fetch_paginates_every_numeric_sequence_across_both_decimal_boundaries() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut mailbox = Mailbox::<_, TestTypes>::new(scope, peer(2), batch_limits());
                for identity in 1..=COUNT {
                    assert!(matches!(
                        mailbox
                            .submit(
                                peer(1),
                                item(identity, "ordered"),
                                SyncTime::from_unix_millis(0),
                                "route".into(),
                                None
                            )
                            .await
                            .map_err(storage_error)?,
                        SubmitResult::Accepted { .. }
                    ));
                }
                assert_complete_pages(&mut mailbox, DeliveryStream::Items).await?;
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn retirement_controls_paginate_every_numeric_sequence_across_both_decimal_boundaries() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut mailbox = Mailbox::<_, TestTypes>::new(scope, peer(2), batch_limits());
                for identity in 1..=COUNT {
                    let expired = DeliveryEnvelope::Item(
                        DeliveryItem::<TestTypes>::new(
                            item_id(identity),
                            "sender".into(),
                            Recipient {
                                peer: peer(2),
                                address: "inbox".into(),
                            },
                            Some(SyncTime::from_unix_millis(0)),
                            "expired".into(),
                        )
                        .unwrap(),
                    );
                    assert!(matches!(
                        mailbox
                            .submit(
                                peer(1),
                                expired,
                                SyncTime::from_unix_millis(0),
                                "route".into(),
                                None
                            )
                            .await
                            .map_err(storage_error)?,
                        SubmitResult::Accepted { .. }
                    ));
                    let retired = mailbox
                        .retire(
                            peer(1),
                            &call(&format!("retire-{identity}")),
                            &[item_id(identity)],
                        )
                        .await
                        .map_err(storage_error)?;
                    assert_eq!(retired.retired, vec![item_id(identity)]);
                }
                assert_complete_pages(&mut mailbox, DeliveryStream::Controls).await?;
                Ok(())
            })
        })
        .await
        .unwrap();
}

async fn assert_complete_pages(
    mailbox: &mut Mailbox<'_, durable_steps::turso_store::TursoScope<'_, TestHost>, TestTypes>,
    stream: DeliveryStream,
) -> Result<(), durable_steps::StoreError> {
    let mut after = DeliverySeq::new(0);
    let mut actual = Vec::new();
    for _ in 0..COUNT {
        let batch = mailbox
            .fetch(
                peer(2),
                stream,
                after,
                7,
                1_000_000,
                SyncTime::from_unix_millis(0),
            )
            .await
            .map_err(storage_error)?;
        actual.extend(batch.entries.iter().map(|entry| entry.sequence().value()));
        if batch.through == after {
            break;
        }
        after = batch.through;
    }
    assert_eq!(actual, (1..=COUNT).collect::<Vec<_>>());
    assert_eq!(after.value(), COUNT);
    Ok(())
}
