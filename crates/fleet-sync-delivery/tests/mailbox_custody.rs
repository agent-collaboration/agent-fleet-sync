#![allow(clippy::unwrap_used)]
mod support;
use durable_steps::{AtomicStore, SyncTime};
use fleet_sync_delivery::*;
use support::*;

#[tokio::test]
async fn repeat_preserves_sequence_and_conflicting_content_changes_nothing() {
    let fixture = Fixture::new().await;
    fixture.store.transaction(|scope|Box::pin(async move {
        let mut mailbox=Mailbox::<_,TestTypes>::new(scope,peer(2),limits());
        assert!(matches!(mailbox.submit(peer(1),item(1,"original"),SyncTime::from_unix_millis(0)).await.map_err(storage_error)?,SubmitResult::Accepted{seq,status:DeliveryStatus::Sent{seq:status_seq},version,cancel:None} if seq.value()==1&&status_seq==seq&&version.value()==1));
        assert!(matches!(mailbox.submit(peer(1),item(1,"original"),SyncTime::from_unix_millis(0)).await.map_err(storage_error)?,SubmitResult::Replayed{status:DeliveryStatus::Sent{seq},version,..} if seq.value()==1&&version.value()==1));
        assert!(matches!(mailbox.submit(peer(1),item(1,"different"),SyncTime::from_unix_millis(0)).await.map_err(storage_error)?,SubmitResult::Refused{reason:DeliveryRefusal::ItemConflict{..}}));
        assert!(matches!(mailbox.submit(peer(1),item(2,"next"),SyncTime::from_unix_millis(0)).await.map_err(storage_error)?,SubmitResult::Accepted{seq,..} if seq.value()==2));
        let fetched=mailbox.fetch(peer(2),DeliveryStream::Items,DeliverySeq::new(0),10,100_000,SyncTime::from_unix_millis(0)).await.map_err(storage_error)?;
        let bodies:Vec<_>=fetched.entries.into_iter().map(|entry|match entry {FetchEntry::Item{item:DeliveryEnvelope::Item(item),..}=>item.body().to_owned(),_=>"unexpected".into()}).collect();
        assert_eq!(bodies,["original","next"]);
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn count_and_bytes_refuse_independently_before_sequence_allocation() {
    for count_limited in [true, false] {
        let fixture = Fixture::new().await;
        fixture
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    let configured = if count_limited {
                        configured_limits(100_000, 1, 100_000)
                    } else {
                        let mailbox_bytes =
                            u64::try_from(serde_json::to_vec(&item(1, "body")).unwrap().len())
                                .unwrap()
                                + u64::from(limits().settlement_allowance());
                        configured_limits(mailbox_bytes, 20, 100_000)
                    };
                    let mut mailbox = Mailbox::<_, TestTypes>::new(scope, peer(2), configured);
                    assert!(matches!(
                        mailbox
                            .submit(peer(1), item(1, "body"), SyncTime::from_unix_millis(0))
                            .await
                            .map_err(storage_error)?,
                        SubmitResult::Accepted { .. }
                    ));
                    assert!(matches!(
                        mailbox
                            .submit(peer(1), item(2, "body"), SyncTime::from_unix_millis(0))
                            .await
                            .map_err(storage_error)?,
                        SubmitResult::Refused {
                            reason: DeliveryRefusal::MailboxFull
                        }
                    ));
                    let found = mailbox.query(&[item_id(2)]).await.map_err(storage_error)?;
                    assert!(matches!(
                        found.statuses.first().map(|status| &status.state),
                        Some(ItemStatusState::Unknown)
                    ));
                    Ok(())
                })
            })
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn receipt_releases_payload_once_and_a_late_report_cannot_regress_status() {
    let fixture = Fixture::new().await;
    fixture.store.transaction(|scope|Box::pin(async move {
        let mut mailbox=Mailbox::<_,TestTypes>::new(scope,peer(2),limits());
        mailbox.submit(peer(1),item(1,"body"),SyncTime::from_unix_millis(0)).await.map_err(storage_error)?;
        mailbox.fetch(peer(2),DeliveryStream::Items,DeliverySeq::new(0),10,100_000,SyncTime::from_unix_millis(0)).await.map_err(storage_error)?;
        let newer=mailbox.report(peer(2),RecipientReport{item:item_id(1),report_seq:ReportSeq::new(2),outcome:RecipientOutcome::Delivered{acceptance:Acceptance{receipt:"one effect".into()}}}).await.map_err(storage_error)?;
        assert!(matches!(newer,ReportResult::Applied{version,..} if version.value()==2));
        let received=mailbox.received(peer(2),DeliveryStream::Items,DeliverySeq::new(1),SyncTime::from_unix_millis(1)).await.map_err(storage_error)?;
        assert_eq!(received.released_through.value(),1);
        let received_again=mailbox.received(peer(2),DeliveryStream::Items,DeliverySeq::new(1),SyncTime::from_unix_millis(2)).await.map_err(storage_error)?;
        assert_eq!(received_again.released_through.value(),1);
        let old=mailbox.report(peer(2),RecipientReport{item:item_id(1),report_seq:ReportSeq::new(1),outcome:RecipientOutcome::Held}).await.map_err(storage_error)?;
        assert!(matches!(old,ReportResult::Applied{status:DeliveryStatus::Delivered{acceptance},version,..} if version.value()==2&&acceptance.receipt=="one effect"));
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn only_the_authenticated_origin_retires_and_compacted_identity_replays() {
    let fixture = Fixture::new().await;
    fixture.store.transaction(|scope|Box::pin(async move {
        let mut mailbox=Mailbox::<_,TestTypes>::new(scope,peer(2),limits());
        mailbox.submit(peer(1),item(1,"body"),SyncTime::from_unix_millis(0)).await.map_err(storage_error)?;
        mailbox.report(peer(2),RecipientReport{item:item_id(1),report_seq:ReportSeq::new(1),outcome:RecipientOutcome::Delivered{acceptance:Acceptance{receipt:"done".into()}}}).await.map_err(storage_error)?;
        let denied=mailbox.retire(peer(3),&call("wrong origin"),&[item_id(1)]).await.map_err(storage_error)?;
        assert_eq!(denied.unknown,[item_id(1)]);
        assert!(denied.retired.is_empty());
        assert!(mailbox.fetch(peer(2),DeliveryStream::Controls,DeliverySeq::new(0),10,100_000,SyncTime::from_unix_millis(0)).await.map_err(storage_error)?.entries.is_empty());
        let retired=mailbox.retire(peer(1),&call("retire"),&[item_id(1)]).await.map_err(storage_error)?;
        assert_eq!(retired.retired,[item_id(1)]);
        let repeat=mailbox.retire(peer(1),&call("retire"),&[item_id(1)]).await.map_err(storage_error)?;
        assert_eq!(repeat.retired,[item_id(1)]);
        assert!(matches!(mailbox.submit(peer(1),item(1,"body"),SyncTime::from_unix_millis(0)).await.map_err(storage_error)?,SubmitResult::Replayed{status:DeliveryStatus::Delivered{..},..}));
        let controls=mailbox.fetch(peer(2),DeliveryStream::Controls,DeliverySeq::new(0),10,100_000,SyncTime::from_unix_millis(0)).await.map_err(storage_error)?;
        assert_eq!(controls.entries.len(),1);
        assert!(matches!(controls.entries.first(),Some(FetchEntry::Retired{seq,items}) if seq.value()==1&&items==&[item_id(1)]));
        mailbox.received(peer(2),DeliveryStream::Controls,DeliverySeq::new(1),SyncTime::from_unix_millis(0)).await.map_err(storage_error)?;
        assert!(mailbox.fetch(peer(2),DeliveryStream::Controls,DeliverySeq::new(1),10,100_000,SyncTime::from_unix_millis(0)).await.map_err(storage_error)?.entries.is_empty());
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn a_full_mailbox_uses_the_originals_reserved_cancel_and_replays_a_second_cancel() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let configured = configured_limits(100_000, 1, 100_000);
                let mut mailbox = Mailbox::<_, TestTypes>::new(scope, peer(2), configured);
                mailbox
                    .submit(
                        peer(1),
                        item(1, "held original"),
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                let first = mailbox
                    .submit(peer(1), cancel(2, 1), SyncTime::from_unix_millis(0))
                    .await
                    .map_err(storage_error)?;
                assert!(matches!(first,SubmitResult::Accepted{seq,status:DeliveryStatus::Sent {seq:status_seq},cancel:Some(CancelResult::Pending),..} if seq.value()==1&&status_seq==seq));
                let second = mailbox
                    .submit(peer(1), cancel(3, 1), SyncTime::from_unix_millis(0))
                    .await
                    .map_err(storage_error)?;
                assert!(matches!(
                    second,
                    SubmitResult::Replayed {
                        cancel: Some(CancelResult::Pending),
                        ..
                    }
                ));
                let controls = mailbox
                    .fetch(
                        peer(2),
                        DeliveryStream::Controls,
                        DeliverySeq::new(0),
                        10,
                        100_000,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                assert_eq!(controls.entries.len(), 1);
                let originals = mailbox
                    .fetch(
                        peer(2),
                        DeliveryStream::Items,
                        DeliverySeq::new(0),
                        10,
                        100_000,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                assert_eq!(originals.entries.len(), 1);
                assert_eq!(originals.through.value(), 1);
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn a_sender_above_its_declared_bound_is_refused_before_custody() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let sender = "sender".repeat(800);
                let original = DeliveryItem::<TestTypes>::new(
                    item_id(1),
                    sender,
                    Recipient {
                        peer: peer(2),
                        address: "inbox".into(),
                    },
                    None,
                    "small body".into(),
                )
                .unwrap();
                let mut mailbox = Mailbox::<_, TestTypes>::new(scope, peer(2), limits());
                assert!(matches!(
                    mailbox
                        .submit(
                            peer(1),
                            DeliveryEnvelope::Item(original),
                            SyncTime::from_unix_millis(0)
                        )
                        .await,
                    Err(DeliveryError::Refused {
                        reason: DeliveryRefusal::TooLarge {
                            field: DeliverySizeField::Sender
                        }
                    })
                ));
                assert!(matches!(
                    mailbox
                        .query(&[item_id(1)])
                        .await
                        .map_err(storage_error)?
                        .statuses[0]
                        .state,
                    ItemStatusState::Unknown
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
}
#[tokio::test]
async fn a_cancel_at_the_requester_bound_fits_the_originals_derived_reserve() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut mailbox = Mailbox::<_, TestTypes>::new(
                    scope,
                    peer(2),
                    configured_limits(100_000, 1, 100_000),
                );
                assert!(matches!(
                    mailbox
                        .submit(peer(1), item(1, "small"), SyncTime::from_unix_millis(0))
                        .await
                        .map_err(storage_error)?,
                    SubmitResult::Accepted { .. }
                ));
                let requester =
                    "x".repeat(usize::try_from(TestTypes::MAX_SENDER_BYTES).unwrap() - 2);
                let cancel = DeliveryCancel::<TestTypes>::new(
                    item_id(2),
                    requester,
                    Recipient {
                        peer: peer(2),
                        address: "inbox".into(),
                    },
                    None,
                    CancelBody { target: item_id(1) },
                )
                .unwrap();
                assert!(matches!(
                    mailbox
                        .submit(
                            peer(1),
                            DeliveryEnvelope::Cancel(cancel),
                            SyncTime::from_unix_millis(0)
                        )
                        .await
                        .map_err(storage_error)?,
                    SubmitResult::Accepted { .. }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
}
#[test]
fn a_configured_allowance_below_the_derived_minimum_is_rejected() {
    let minimum = minimum_settlement_allowance::<TestTypes>().unwrap();
    assert!(
        matches!(DeliveryLimits::new::<TestTypes>(100_000,20,100_000,20,100_000,minimum-1),Err(DeliveryConfigError::SettlementAllowanceTooSmall{supplied,minimum:required}) if supplied==minimum-1&&required==minimum)
    );
}
