use super::support::*;
use durable_steps::AtomicStore;
use fleet_sync_delivery::*;

#[tokio::test]
async fn recovered_terminal_status_survives_a_stale_submit_answer() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let input = SubmitInput {
                    item: item_id(1),
                    digest: item(1, "lost custody").digest().clone(),
                    relay: peer(3),
                    mailbox: peer(2),
                };
                let mut outbox = Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings);
                let now = SyncTime::from_unix_millis(0);

                // Recover a missing row, then let a canonical publication arrive before
                // the delayed submit reply. Both writes use the public custody API.
                outbox
                    .mark_submitted(
                        &input,
                        SubmitResult::Accepted {
                            status: DeliveryStatus::Sent,
                            version: StatusVersion::new(1),
                            cancel: None,
                        },
                        now,
                    )
                    .await
                    .map_err(storage_error)?;
                outbox
                    .publish_status(
                        StatusPublication {
                            item: item_id(1),
                            status: DeliveryStatus::Delivered {
                                acceptance: Acceptance {
                                    receipt: "canonical result".into(),
                                },
                            },
                            version: StatusVersion::new(2),
                            cancel: None,
                        },
                        now,
                    )
                    .await
                    .map_err(storage_error)?;

                outbox
                    .mark_submitted(
                        &input,
                        SubmitResult::Accepted {
                            status: DeliveryStatus::Sent,
                            version: StatusVersion::new(1),
                            cancel: None,
                        },
                        now,
                    )
                    .await
                    .map_err(storage_error)?;

                assert!(
                    matches!(outbox.status(item_id(1)).await.map_err(storage_error)?.state,
            OutboxStatusState::Known {status:DeliveryStatus::Delivered {acceptance},version,..}
            if acceptance.receipt=="canonical result" && version.value()==2)
                );
                // A later failure must preserve the same already-terminal result too.
                outbox
                    .record_submission_failure(&input, DeliveryFailureKind::CustodyMissing, now)
                    .await
                    .map_err(storage_error)?;
                assert!(
                    matches!(outbox.status(item_id(1)).await.map_err(storage_error)?.state,
            OutboxStatusState::Known {status:DeliveryStatus::Delivered {acceptance},version,..}
            if acceptance.receipt=="canonical result" && version.value()==2)
                );
                Ok(())
            })
        })
        .await
        .unwrap();
}
