use super::support::{legacy_submitters::*, outbox_projection::*, *};
use durable_steps::{AtomicStore, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;

#[tokio::test]
async fn legacy_sent_recovery_never_projects_a_skipped_status_write() {
    let fixture = legacy_store(2).await;
    startup(&fixture.store).await.unwrap();
    prepare(&fixture).await;
    let before = snapshot(&fixture.store).await;
    fixture.store.transaction(|scope|Box::pin(async move {
        let legacy=query::<Turso>("SELECT legacy_read_only FROM _fleet_sync_outbox_items WHERE item_id=?1")
            .bind(item_id(101).as_uuid().to_string()).fetch_one(scope.executor()).await?;
        assert_eq!(legacy.try_get::<i64,_>("legacy_read_only")?,1);
        let input=SubmitInput {item:item_id(101),digest:ItemDigest::try_from("a".repeat(64)).unwrap(),relay:peer(3),mailbox:peer(2)};
        let mut outbox=Outbox::<_,TestTypes,_>::new(scope,limits(),&OUTCOME_BINDINGS);
        for _ in 0..2 {
            outbox.record_submission_failure(&input,DeliveryFailureKind::CustodyMissing,startup_time()).await.map_err(storage_error)?;
        }
        outbox.mark_submitted(&input,SubmitResult::Replayed {status:DeliveryStatus::Delivered {acceptance:Acceptance {receipt:"legacy replay".into()}},version:StatusVersion::new(2),cancel:None},startup_time()).await.map_err(storage_error)?;
        assert!(matches!(outbox.status(item_id(101)).await.map_err(storage_error)?.state,OutboxStatusState::Known {status:DeliveryStatus::Sent,version,..} if version.value()==1));
        Ok(())
    })).await.unwrap();
    assert_eq!(
        snapshot(&fixture.store).await,
        before,
        "legacy custody must remain unchanged"
    );
    assert!(
        projected(&fixture).await.is_empty(),
        "skipped legacy writes must never project terminal outcomes"
    );
}
