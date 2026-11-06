use super::support::{outbox_projection::*, *};
use durable_steps::AtomicStore;
use fleet_sync_delivery::*;

fn failed_status() -> DeliveryStatus<Acceptance> {
    DeliveryStatus::Refused {
        reason: DeliveryRefusal::ProcessingFailed {
            kind: DeliveryFailureKind::ExecutionFailed,
        },
    }
}
fn expected_failure() -> serde_json::Value {
    serde_json::json!({"kind":"refused","value":{"reason":{"kind":"processingFailed","value":{"kind":"executionFailed"}}}})
}

#[tokio::test]
async fn newer_recovered_terminal_versions_do_not_repeat_projection() {
    let fixture = Fixture::new().await;
    prepare(&fixture).await;
    fixture.store.transaction(|scope|Box::pin(async move {
        let mut outbox=Outbox::<_,TestTypes,_>::new(scope,limits(),&OUTCOME_BINDINGS);
        let now=SyncTime::from_unix_millis(0);
        outbox.record_submission_failure(&submit_input(),DeliveryFailureKind::ExecutionFailed,now).await.map_err(storage_error)?;
        for version in [2,3] {
            outbox.publish_status(StatusPublication {item:item_id(1),status:failed_status(),version:StatusVersion::new(version),cancel:None},now).await.map_err(storage_error)?;
        }
        assert!(matches!(outbox.status(item_id(1)).await.map_err(storage_error)?.state,OutboxStatusState::Known {status:DeliveryStatus::Refused {reason:DeliveryRefusal::ProcessingFailed {kind:DeliveryFailureKind::ExecutionFailed}},version,..} if version.value()==3));
        Ok(())
    })).await.unwrap();
    assert_eq!(
        projected(&fixture).await,
        vec![(item_id(1).as_uuid().to_string(), expected_failure())]
    );
}

#[tokio::test]
async fn binding_unresolvable_settlement_does_not_repeat_projection() {
    let fixture = Fixture::new().await;
    prepare(&fixture).await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut outbox = Outbox::<_, TestTypes, _>::new(scope, limits(), &OUTCOME_BINDINGS);
                let now = SyncTime::from_unix_millis(0);
                outbox
                    .record_failure(item_id(1), DeliveryFailureKind::ExecutionFailed, now)
                    .await
                    .map_err(storage_error)?;
                outbox
                    .relay_settle::<RecordRemoval>(
                        Submitter::Machine(peer(1)),
                        item_id(1),
                        submit_input().digest,
                        now,
                    )
                    .await
                    .map_err(storage_error)?;
                assert!(matches!(
                    outbox
                        .status(item_id(1))
                        .await
                        .map_err(storage_error)?
                        .state,
                    OutboxStatusState::Known {
                        status: DeliveryStatus::Refused {
                            reason: DeliveryRefusal::ProcessingFailed {
                                kind: DeliveryFailureKind::ExecutionFailed
                            }
                        },
                        ..
                    }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(
        projected(&fixture).await,
        vec![(item_id(1).as_uuid().to_string(), expected_failure())]
    );
}

#[tokio::test]
async fn submission_failure_preserves_terminal_item_and_its_projection() {
    let fixture = Fixture::new().await;
    prepare(&fixture).await;
    enqueue(&fixture).await;
    fixture.store.transaction(|scope|Box::pin(async move {
        let mut outbox=Outbox::<_,TestTypes,_>::new(scope,limits(),&OUTCOME_BINDINGS);
        let now=SyncTime::from_unix_millis(0);
        outbox.mark_submitted(&submit_input(),SubmitResult::Accepted {status:DeliveryStatus::Delivered {acceptance:Acceptance {receipt:"original result".into()}},version:StatusVersion::new(2),cancel:None},now).await.map_err(storage_error)?;
        outbox.record_submission_failure(&submit_input(),DeliveryFailureKind::CustodyMissing,now).await.map_err(storage_error)?;
        assert!(matches!(outbox.status(item_id(1)).await.map_err(storage_error)?.state,OutboxStatusState::Known {status:DeliveryStatus::Delivered {acceptance},version,..} if acceptance.receipt=="original result" && version.value()==2));
        Ok(())
    })).await.unwrap();
    assert_eq!(
        projected(&fixture).await,
        vec![(
            item_id(1).as_uuid().to_string(),
            serde_json::json!({"kind":"delivered","value":{"acceptance":{"receipt":"original result"}}})
        )]
    );
}

#[tokio::test]
async fn submission_answers_preserve_unresolvable_result_and_its_projection() {
    let fixture = Fixture::new().await;
    prepare(&fixture).await;
    fixture.store.transaction(|scope|Box::pin(async move {
        let mut outbox=Outbox::<_,TestTypes,_>::new(scope,limits(),&OUTCOME_BINDINGS);
        let now=SyncTime::from_unix_millis(0);
        outbox.record_failure(item_id(1),DeliveryFailureKind::ExecutionFailed,now).await.map_err(storage_error)?;
        outbox.record_submission_failure(&submit_input(),DeliveryFailureKind::CustodyMissing,now).await.map_err(storage_error)?;
        outbox.mark_submitted(&submit_input(),SubmitResult::Replayed {status:DeliveryStatus::Sent,version:StatusVersion::new(1),cancel:None},now).await.map_err(storage_error)?;
        assert!(matches!(outbox.status(item_id(1)).await.map_err(storage_error)?.state,OutboxStatusState::Known {status:DeliveryStatus::Refused {reason:DeliveryRefusal::ProcessingFailed {kind:DeliveryFailureKind::ExecutionFailed}},version,..} if version.value()==0));
        Ok(())
    })).await.unwrap();
    assert_eq!(
        projected(&fixture).await,
        vec![(item_id(1).as_uuid().to_string(), expected_failure())]
    );
}
