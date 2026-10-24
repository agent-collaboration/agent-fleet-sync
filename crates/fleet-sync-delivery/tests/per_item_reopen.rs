#![allow(clippy::unwrap_used)]
#[path = "support/per_item.rs"]
mod per_item;
mod support;
use durable_steps::{AtomicStore, storage::TransactionScope};
use fleet_sync_delivery::*;
use per_item::*;
use sqlx_core::row::Row;
use support::*;

#[tokio::test]
async fn released_and_compacted_mail_reopen_with_explicit_and_legacy_bounds() {
    for compact in [false, true] {
        let Fixture {
            store,
            host,
            directory,
        } = Fixture::new().await;
        store.transaction(move |scope| Box::pin(async move {
            let limits=variable_limits(100_000_000);
            let mut mailbox=Mailbox::<_,VariableTypes>::new(scope,peer(2),limits);
            for identity in [1,2] {
                mailbox.submit(peer(1),DeliveryEnvelope::Item(variable_item(identity,"small")),now(),"route".into(),None).await.map_err(storage_error)?;
            }
            let batch=mailbox.fetch(peer(2),DeliveryStream::Items,DeliverySeq::new(0),2,10_000,now()).await.map_err(storage_error)?;
            mailbox.received(peer(2),DeliveryStream::Items,batch.through,now()).await.map_err(storage_error)?;
            if compact {
                for identity in [1,2] {
                    mailbox.report(peer(2),RecipientReport { item:item_id(identity),report_seq:ReportSeq::new(1),outcome:RecipientOutcome::Delivered { acceptance:Acceptance { receipt:"ok".into() } } }).await.map_err(storage_error)?;
                }
                let result=mailbox.retire(peer(1),&call("compact-bounds"),&[item_id(1),item_id(2)]).await.map_err(storage_error)?;
                assert_eq!(result.retired.len(),2);
            }
            // Persist an old-format record alongside the explicit-bound record.
            sqlx_core::query::query::<sqlx_turso::Turso>("UPDATE _fleet_sync_mail_items SET record=json_remove(record,'$.value.acceptanceBound') WHERE item_id=?1").bind(item_id(2).as_uuid().to_string()).execute(scope.executor()).await?;
            if !compact {
                let legacy=minimum_settlement_allowance::<VariableTypes>().unwrap();
                sqlx_core::query::query::<sqlx_turso::Turso>("UPDATE _fleet_sync_mail_items SET record=json_set(record,'$.value.settlementBytes',?1),settlement_bytes=?1 WHERE item_id=?2").bind(i64::from(legacy)).bind(item_id(2).as_uuid().to_string()).execute(scope.executor()).await?;
            }
            Ok(())
        })).await.unwrap();
        drop(store);
        let reopened = TestStore::open_and_migrate(directory.path().join("delivery.db"), host)
            .await
            .unwrap();
        reopened.transaction(move |scope| Box::pin(async move {
            let limits=variable_limits(100_000_000);
            for identity in [1,2] {
                let row=sqlx_core::query::query::<sqlx_turso::Turso>("SELECT record FROM _fleet_sync_mail_items WHERE item_id=?1").bind(item_id(identity).as_uuid().to_string()).fetch_one(scope.executor()).await?;
                let record:serde_json::Value=serde_json::from_str(&row.try_get::<String,_>("record")?).unwrap();
                assert_eq!(record["kind"],if compact {"compacted"} else {"live"});
                assert!(record["value"]["payload"].is_null());
                assert_eq!(record["value"].get("acceptanceBound").and_then(serde_json::Value::as_u64),if identity==1 {Some(64)} else {None});
                Mailbox::<_,VariableTypes>::new(scope,peer(2),limits).query(&[item_id(identity)]).await.map_err(storage_error)?;
                let result=Mailbox::<_,VariableTypes>::new(scope,peer(2),limits).report(peer(2),RecipientReport { item:item_id(identity),report_seq:ReportSeq::new(2),outcome:RecipientOutcome::Delivered { acceptance:Acceptance { receipt:"x".repeat(100) } } }).await;
                if identity==1 { assert!(matches!(result,Err(DeliveryError::Refused { reason:DeliveryRefusal::TooLarge { field:DeliverySizeField::Acceptance } }))); }
                else { result.map_err(storage_error)?; }
            }
            if compact {
                // Challenge the compacted-row decoder too: the old shape accepts
                // this globally valid receipt, the explicit small bound rejects it.
                let status=serde_json::to_string(&DeliveryStatus::Delivered {acceptance:Acceptance {receipt:"x".repeat(100)}}).unwrap();
                sqlx_core::query::query::<sqlx_turso::Turso>("UPDATE _fleet_sync_mail_items SET record=json_set(record,'$.value.status',json(?1))").bind(status).execute(scope.executor()).await?;
                assert!(matches!(Mailbox::<_,VariableTypes>::new(scope,peer(2),limits).query(&[item_id(1)]).await,Err(DeliveryError::Corrupt { .. })));
                Mailbox::<_,VariableTypes>::new(scope,peer(2),limits).query(&[item_id(2)]).await.map_err(storage_error)?;
            }
            Ok(())
        })).await.unwrap();
    }
}

#[tokio::test]
async fn compact_local_inbox_reopens_with_explicit_and_legacy_bounds() {
    let Fixture {
        store,
        host,
        directory,
    } = Fixture::new().await;
    store.transaction(|scope| Box::pin(async move {
        let mut inbox=Inbox::<_,VariableTypes>::new(scope,peer(2),variable_limits(100_000_000));
        for identity in [1,2] {
            inbox.enqueue_local(variable_item(identity,"small"),&VariableConsumer,now()).await.map_err(storage_error)?;
            let cancel=DeliveryCancel::new(item_id(identity+10),"sender".into(),Recipient {peer:peer(2),address:"inbox".into()},None,CancelBody {target:item_id(identity)}).unwrap();
            inbox.enqueue_local_cancel(cancel,&VariableConsumer,now()).await.map_err(storage_error)?;
        }
        sqlx_core::query::query::<sqlx_turso::Turso>("UPDATE _fleet_sync_inbox_items SET record=json_remove(record,'$.acceptanceBound') WHERE item_id=?1").bind(item_id(2).as_uuid().to_string()).execute(scope.executor()).await?;
        Ok(())
    })).await.unwrap();
    drop(store);
    let reopened = TestStore::open_and_migrate(directory.path().join("delivery.db"), host)
        .await
        .unwrap();
    reopened.transaction(|scope| Box::pin(async move {
        let limits=variable_limits(100_000_000);
        for identity in [1,2] {
            let row=sqlx_core::query::query::<sqlx_turso::Turso>("SELECT record,payload,live,settlement_bytes FROM _fleet_sync_inbox_items WHERE item_id=?1").bind(item_id(identity).as_uuid().to_string()).fetch_one(scope.executor()).await?;
            assert!(row.try_get::<Option<String>,_>("payload")?.is_none());
            assert_eq!(row.try_get::<i64,_>("live")?,0);
            assert_eq!(row.try_get::<i64,_>("settlement_bytes")?,0);
            let record:serde_json::Value=serde_json::from_str(&row.try_get::<String,_>("record")?).unwrap();
            assert_eq!(record.get("acceptanceBound").and_then(serde_json::Value::as_u64),if identity==1 {Some(64)} else {None});
            assert!(matches!(Inbox::<_,VariableTypes>::new(scope,peer(2),limits).local_outcome(item_id(identity)).await.map_err(storage_error)?,Some(RecipientOutcome::Prevented { .. })));
            assert!(matches!(Inbox::<_,VariableTypes>::new(scope,peer(2),limits).enqueue_local(variable_item(identity,"small"),&VariableConsumer,now()).await.map_err(storage_error)?,LocalItemAdmission::Replayed));
        }
        // A controlled stored-outcome challenge exercises both public header
        // reads and full custody decoding after real local compaction/reopen.
        let outcome=serde_json::to_string(&RecipientOutcome::Delivered {acceptance:Acceptance {receipt:"x".repeat(100)}}).unwrap();
        sqlx_core::query::query::<sqlx_turso::Turso>("UPDATE _fleet_sync_inbox_items SET record=json_set(record,'$.progress.value.outcome',json(?1)) WHERE item_id=?2 OR item_id=?3").bind(outcome).bind(item_id(1).as_uuid().to_string()).bind(item_id(2).as_uuid().to_string()).execute(scope.executor()).await?;
        assert!(matches!(Inbox::<_,VariableTypes>::new(scope,peer(2),limits).local_outcome(item_id(1)).await,Err(DeliveryError::Corrupt { .. })));
        assert!(matches!(Inbox::<_,VariableTypes>::new(scope,peer(2),limits).local_outcome(item_id(2)).await.map_err(storage_error)?,Some(RecipientOutcome::Delivered { .. })));
        assert!(matches!(Inbox::<_,VariableTypes>::new(scope,peer(2),limits).enqueue_local(variable_item(1,"small"),&VariableConsumer,now()).await,Err(DeliveryError::Refused {reason:DeliveryRefusal::TooLarge {field:DeliverySizeField::Acceptance}})));
        assert!(matches!(Inbox::<_,VariableTypes>::new(scope,peer(2),limits).enqueue_local(variable_item(2,"small"),&VariableConsumer,now()).await.map_err(storage_error)?,LocalItemAdmission::Replayed));
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn recovered_outbox_reopens_with_explicit_and_legacy_bounds() {
    let Fixture {
        store,
        host,
        directory,
    } = Fixture::new().await;
    store.transaction(|scope| Box::pin(async move {
        let limits=variable_limits(100_000_000);
        for identity in [1,2] {
            let item=variable_item(identity,"small");
            let input=SubmitInput {item:item.id(),digest:item.digest().clone(),relay:peer(3),mailbox:peer(2)};
            Outbox::<_,VariableTypes>::new(scope,limits).enqueue(item,peer(3)).await.map_err(storage_error)?;
            Outbox::<_,VariableTypes>::new(scope,limits).record_submission_failure(&input,DeliveryFailureKind::CustodyMissing).await.map_err(storage_error)?;
        }
        sqlx_core::query::query::<sqlx_turso::Turso>("UPDATE _fleet_sync_outbox_items SET record=json_remove(record,'$.value.acceptanceBound') WHERE item_id=?1").bind(item_id(2).as_uuid().to_string()).execute(scope.executor()).await?;
        Ok(())
    })).await.unwrap();
    drop(store);
    let reopened = TestStore::open_and_migrate(directory.path().join("delivery.db"), host)
        .await
        .unwrap();
    reopened.transaction(|scope| Box::pin(async move {
        let limits=variable_limits(100_000_000);
        for identity in [1,2] {
            let row=sqlx_core::query::query::<sqlx_turso::Turso>("SELECT record,payload FROM _fleet_sync_outbox_items WHERE item_id=?1").bind(item_id(identity).as_uuid().to_string()).fetch_one(scope.executor()).await?;
            assert!(row.try_get::<Option<String>,_>("payload")?.is_none());
            let record:serde_json::Value=serde_json::from_str(&row.try_get::<String,_>("record")?).unwrap();
            assert_eq!(record["kind"],"recovered");
            assert_eq!(record["value"].get("acceptanceBound").and_then(serde_json::Value::as_u64),if identity==1 {Some(64)} else {None});
            Outbox::<_,VariableTypes>::new(scope,limits).status(item_id(identity)).await.map_err(storage_error)?;
        }
        let status=serde_json::to_string(&DeliveryStatus::Delivered {acceptance:Acceptance {receipt:"x".repeat(100)}}).unwrap();
        sqlx_core::query::query::<sqlx_turso::Turso>("UPDATE _fleet_sync_outbox_items SET record=json_set(record,'$.value.status',json(?1))").bind(status).execute(scope.executor()).await?;
        assert!(matches!(Outbox::<_,VariableTypes>::new(scope,limits).status(item_id(1)).await,Err(DeliveryError::Refused {reason:DeliveryRefusal::TooLarge {field:DeliverySizeField::Acceptance}})));
        assert!(matches!(Outbox::<_,VariableTypes>::new(scope,limits).status(item_id(2)).await.map_err(storage_error)?.state,OutboxStatusState::Known {status:DeliveryStatus::Delivered { .. },..}));
        Ok(())
    })).await.unwrap();
}
