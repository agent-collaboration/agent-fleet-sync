#![allow(clippy::unwrap_used)]
mod support;

use durable_steps::{AtomicStore, StoreError, SyncTime};
use fleet_sync_delivery::*;
use support::*;

async fn open_mailbox(fixture: &Fixture) {
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .open()
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap();
}

fn refused_publication() -> PublicationRef {
    PublicationRef {
        item: item_id(1),
        version: StatusVersion::new(2),
        target: PublicationTarget::StatusStream { sender: peer(1) },
    }
}

#[tokio::test]
async fn opening_an_absent_or_open_mailbox_is_idempotent_and_preserves_sequences() {
    let fixture = Fixture::new().await;
    open_mailbox(&fixture).await;
    open_mailbox(&fixture).await;
    for identity in 1..=2 {
        fixture
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    assert!(matches!(
                        Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                            .submit(
                                peer(1),
                                item(identity, "new enrollment"),
                                SyncTime::from_unix_millis(0),
                            )
                            .await
                            .map_err(storage_error)?,
                        SubmitResult::Accepted { seq, version, .. }
                            if seq.value() == identity && version.value() == 1
                    ));
                    Ok(())
                })
            })
            .await
            .unwrap();
        open_mailbox(&fixture).await;
    }
}

#[tokio::test]
async fn readmission_accepts_new_ids_without_reviving_refused_items_or_losing_publications() {
    let fixture = Fixture::new().await;
    let publication_before = fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                {
                    let mut mailbox = Mailbox::<_, TestTypes>::new(scope, peer(2), limits());
                    mailbox
                        .submit(peer(1), item(1, "old item"), SyncTime::from_unix_millis(0))
                        .await
                        .map_err(storage_error)?;
                    mailbox.close().await.map_err(storage_error)?;
                    assert!(matches!(
                        mailbox
                            .submit(peer(1), item(2, "new item"), SyncTime::from_unix_millis(1))
                            .await
                            .map_err(storage_error)?,
                        SubmitResult::Refused {
                            reason: DeliveryRefusal::RecipientRevoked
                        }
                    ));
                    assert!(matches!(
                        mailbox
                            .query(&[item_id(2)])
                            .await
                            .map_err(storage_error)?
                            .statuses[0]
                            .state,
                        ItemStatusState::Unknown
                    ));
                }
                let publication = resolve_publication::<_, TestTypes>(scope, refused_publication())
                    .await
                    .map_err(storage_error)?
                    .unwrap();
                assert!(matches!(
                    publication.status,
                    DeliveryStatus::Refused {
                        reason: DeliveryRefusal::RecipientRevoked
                    }
                ));
                serde_json::to_value(publication).map_err(StoreError::backend)
            })
        })
        .await
        .unwrap();

    open_mailbox(&fixture).await;
    open_mailbox(&fixture).await;

    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                {
                    let mut mailbox = Mailbox::<_, TestTypes>::new(scope, peer(2), limits());
                    assert!(matches!(
                        mailbox.query(&[item_id(1)]).await.map_err(storage_error)?.statuses[0].state,
                        ItemStatusState::Known {
                            status: DeliveryStatus::Refused { reason: DeliveryRefusal::RecipientRevoked },
                            version, ..
                        } if version.value() == 2
                    ));
                    assert!(matches!(
                        mailbox.submit(peer(1), item(1, "old item"), SyncTime::from_unix_millis(2))
                            .await.map_err(storage_error)?,
                        SubmitResult::Replayed {
                            status: DeliveryStatus::Refused { reason: DeliveryRefusal::RecipientRevoked },
                            version, ..
                        } if version.value() == 2
                    ));
                    assert!(matches!(
                        mailbox.submit(peer(1), item(2, "new item"), SyncTime::from_unix_millis(2))
                            .await.map_err(storage_error)?,
                        SubmitResult::Accepted { seq, version, .. }
                            if seq.value() == 2 && version.value() == 1
                    ));
                    let fetched = mailbox.fetch(peer(2), DeliveryStream::Items, DeliverySeq::new(0),
                        10, 100_000, SyncTime::from_unix_millis(2)).await.map_err(storage_error)?;
                    assert_eq!(fetched.entries.len(), 1);
                    assert!(matches!(fetched.entries.first(),
                        Some(FetchEntry::Item { seq, item }) if seq.value() == 2 && item.id() == item_id(2)));
                    assert_eq!(fetched.through.value(), 2);
                }
                let publication = resolve_publication::<_, TestTypes>(scope, refused_publication())
                    .await.map_err(storage_error)?.unwrap();
                assert_eq!(serde_json::to_value(publication).map_err(StoreError::backend)?, publication_before);
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn reopening_preserves_compacted_dedup_answers_and_retirement_controls() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut mailbox = Mailbox::<_, TestTypes>::new(scope, peer(2), limits());
                mailbox
                    .submit(peer(1), item(1, "completed"), SyncTime::from_unix_millis(0))
                    .await
                    .map_err(storage_error)?;
                mailbox
                    .report(
                        peer(2),
                        RecipientReport {
                            item: item_id(1),
                            report_seq: ReportSeq::new(1),
                            outcome: RecipientOutcome::Delivered {
                                acceptance: Acceptance {
                                    receipt: "one effect".into(),
                                },
                            },
                        },
                    )
                    .await
                    .map_err(storage_error)?;
                let retired = mailbox
                    .retire(peer(1), &call("before readmission"), &[item_id(1)])
                    .await
                    .map_err(storage_error)?;
                assert_eq!(retired.retired, [item_id(1)]);
                mailbox.close().await.map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();

    open_mailbox(&fixture).await;
    open_mailbox(&fixture).await;

    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut mailbox = Mailbox::<_, TestTypes>::new(scope, peer(2), limits());
                assert!(matches!(
                    mailbox.submit(peer(1), item(1, "completed"), SyncTime::from_unix_millis(1))
                        .await.map_err(storage_error)?,
                    SubmitResult::Replayed { status: DeliveryStatus::Delivered { acceptance }, version, .. }
                        if acceptance.receipt == "one effect" && version.value() == 2
                ));
                assert!(matches!(
                    mailbox.submit(peer(1), item(1, "conflict"), SyncTime::from_unix_millis(1))
                        .await.map_err(storage_error)?,
                    SubmitResult::Refused { reason: DeliveryRefusal::ItemConflict { .. } }
                ));
                let controls = mailbox.fetch(peer(2), DeliveryStream::Controls, DeliverySeq::new(0),
                    10, 100_000, SyncTime::from_unix_millis(1)).await.map_err(storage_error)?;
                assert_eq!(controls.entries.len(), 1);
                assert!(matches!(controls.entries.first(), Some(FetchEntry::Retired { seq, items })
                    if seq.value() == 1 && items == &[item_id(1)]));
                Ok(())
            })
        })
        .await
        .unwrap();
}
