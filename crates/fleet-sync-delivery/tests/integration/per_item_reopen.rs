#![allow(clippy::unwrap_used)]
use super::per_item;
use super::support;
use durable_steps::{AtomicStore, storage::TransactionScope};
use fleet_sync_delivery::*;
use per_item::*;
use sqlx_core::row::Row;
use support::*;

#[tokio::test]
async fn released_and_terminal_relay_records_reopen_with_explicit_and_legacy_bounds() {
    for terminal in [false, true] {
        let Fixture {
            store,
            host,
            directory,
        } = Fixture::new().await;
        store.transaction(move |scope|Box::pin(async move {
            let configured=variable_limits(100_000_000);
            for identity in [1,2] {
                let envelope=DeliveryEnvelope::Item(variable_item(identity,"small"));
                let reference=DeliveryItemRef {item:envelope.id(),digest:envelope.digest().clone()};
                let mut relay=Outbox::<_,VariableTypes,_>::new(scope,configured,&FixtureBindings);
                relay.submit_relay::<FixturePush<VariableTypes>,RecordRemoval>(Submitter::Machine(peer(1)),envelope,peer(3),"route".into(),now()).await.map_err(storage_error)?;
                relay.receipt::<FixturePublication<VariableTypes>>(peer(2),&reference,PushResult::Accepted {accepted_at:now()},now()).await.map_err(storage_error)?;
                if terminal {relay.recipient_report::<FixturePublication<VariableTypes>>(peer(2),RecipientReport {item:item_id(identity),report_seq:ReportSeq::new(1),outcome:RecipientOutcome::Delivered {acceptance:Acceptance {receipt:"ok".into()}}},now()).await.map_err(storage_error)?;}
            }
            sqlx_core::query::query::<sqlx_turso::Turso>("UPDATE _fleet_sync_outbox_items SET acceptance_bound=NULL WHERE item_id=?1").bind(item_id(2).as_uuid().to_string()).execute(scope.executor()).await?;
            if !terminal {
                let global=i64::from(minimum_settlement_allowance::<VariableTypes>().unwrap());
                sqlx_core::query::query::<sqlx_turso::Turso>("UPDATE _fleet_sync_outbox_items SET settlement_bytes=?1,settlement_charge_bytes=?1 WHERE item_id=?2").bind(global).bind(item_id(2).as_uuid().to_string()).execute(scope.executor()).await?;
            }
            Ok(())
        })).await.unwrap();
        drop(store);
        let reopened = TestStore::open_and_migrate(directory.path().join("delivery.db"), host)
            .await
            .unwrap();
        reopened.transaction(move |scope|Box::pin(async move {
            let configured=variable_limits(100_000_000);
            for identity in [1,2] {
                let row=sqlx_core::query::query::<sqlx_turso::Turso>("SELECT payload,acceptance_bound,status_kind FROM _fleet_sync_outbox_items WHERE item_id=?1").bind(item_id(identity).as_uuid().to_string()).fetch_one(scope.executor()).await?;
                assert!(row.try_get::<Option<String>,_>("payload")?.is_none());
                assert_eq!(row.try_get::<Option<i64>,_>("acceptance_bound")?,if identity==1 {Some(64)} else {None});
                assert_eq!(row.try_get::<String,_>("status_kind")?,if terminal {"delivered"} else {"atRecipient"});
                let mut relay=Outbox::<_,VariableTypes,_>::new(scope,configured,&FixtureBindings);
                relay.relay_query(Submitter::Machine(peer(1)),item_id(identity)).await.map_err(storage_error)?;
                let result=relay.recipient_report::<FixturePublication<VariableTypes>>(peer(2),RecipientReport {item:item_id(identity),report_seq:ReportSeq::new(2),outcome:RecipientOutcome::Delivered {acceptance:Acceptance {receipt:"x".repeat(100)}}},now()).await;
                if identity==1 {assert!(matches!(result,Err(DeliveryError::Refused {reason:DeliveryRefusal::TooLarge {field:DeliverySizeField::Acceptance}})));}
                else {result.map_err(storage_error)?;}
            }
            if terminal {
                let acceptance=serde_json::to_string(&Acceptance {receipt:"x".repeat(100)}).unwrap();
                sqlx_core::query::query::<sqlx_turso::Turso>("UPDATE _fleet_sync_outbox_items SET status_acceptance=?1").bind(acceptance).execute(scope.executor()).await?;
                assert!(matches!(Outbox::<_,VariableTypes,_>::new(scope,configured,&FixtureBindings).relay_query(Submitter::Machine(peer(1)),item_id(1)).await,Err(DeliveryError::Corrupt {..})));
                Outbox::<_,VariableTypes,_>::new(scope,configured,&FixtureBindings).relay_query(Submitter::Machine(peer(1)),item_id(2)).await.map_err(storage_error)?;
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
    store
        .transaction(|scope| {
            Box::pin(async move {
                let mut inbox = Inbox::<_, VariableTypes, _>::new(
                    scope,
                    peer(2),
                    variable_limits(100_000_000),
                    &FixtureBindings,
                );
                for identity in [1, 2] {
                    inbox
                        .enqueue_local(variable_item(identity, "small"), &VariableConsumer, now())
                        .await
                        .map_err(storage_error)?;
                    let cancel = DeliveryCancel::new(
                        item_id(identity + 10),
                        "sender".into(),
                        Recipient {
                            peer: peer(2),
                            address: "inbox".into(),
                        },
                        None,
                        CancelBody {
                            target: item_id(identity),
                        },
                    )
                    .unwrap();
                    inbox
                        .enqueue_local_cancel(cancel, &VariableConsumer, now())
                        .await
                        .map_err(storage_error)?;
                }
                sqlx_core::query::query::<sqlx_turso::Turso>(
                    "UPDATE _fleet_sync_inbox_items SET acceptance_bound=NULL WHERE item_id=?1",
                )
                .bind(item_id(2).as_uuid().to_string())
                .execute(scope.executor())
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    drop(store);
    let reopened = TestStore::open_and_migrate(directory.path().join("delivery.db"), host)
        .await
        .unwrap();
    reopened.transaction(|scope| Box::pin(async move {
        let limits=variable_limits(100_000_000);
        for identity in [1,2] {
            let row=sqlx_core::query::query::<sqlx_turso::Turso>(include_str!("../support/sql/read_inbox_header.sql")).bind(item_id(identity).as_uuid().to_string()).fetch_one(scope.executor()).await?;
            assert!(row.try_get::<Option<String>,_>("payload")?.is_none());
            assert_eq!(row.try_get::<i64,_>("live")?,0);
            assert_eq!(row.try_get::<i64,_>("settlement_bytes")?,0);
            let record:serde_json::Value=serde_json::from_str(&row.try_get::<String,_>("record")?).unwrap();
            assert_eq!(record.get("acceptanceBound").and_then(serde_json::Value::as_u64),if identity==1 {Some(64)} else {None});
            assert!(matches!(Inbox::<_,VariableTypes, _>::new(scope,peer(2),limits, &FixtureBindings).local_outcome(item_id(identity)).await.map_err(storage_error)?,Some(RecipientOutcome::Prevented { .. })));
            assert!(matches!(Inbox::<_,VariableTypes, _>::new(scope,peer(2),limits, &FixtureBindings).enqueue_local(variable_item(identity,"small"),&VariableConsumer,now()).await.map_err(storage_error)?,LocalItemAdmission::Replayed));
        }
        // A controlled stored-outcome challenge exercises both public header
        // reads and full custody decoding after real local compaction/reopen.
        let outcome=serde_json::to_string(&RecipientOutcome::Delivered {acceptance:Acceptance {receipt:"x".repeat(100)}}).unwrap();
        sqlx_core::query::query::<sqlx_turso::Turso>("UPDATE _fleet_sync_inbox_items SET outcome_kind=json_extract(?1,'$.kind'), outcome_acceptance=?1 -> '$.value.acceptance' WHERE item_id=?2 OR item_id=?3").bind(outcome).bind(item_id(1).as_uuid().to_string()).bind(item_id(2).as_uuid().to_string()).execute(scope.executor()).await?;
        assert!(matches!(Inbox::<_,VariableTypes, _>::new(scope,peer(2),limits, &FixtureBindings).local_outcome(item_id(1)).await,Err(DeliveryError::Corrupt { .. })));
        assert!(matches!(Inbox::<_,VariableTypes, _>::new(scope,peer(2),limits, &FixtureBindings).local_outcome(item_id(2)).await.map_err(storage_error)?,Some(RecipientOutcome::Delivered { .. })));
        assert!(matches!(Inbox::<_,VariableTypes, _>::new(scope,peer(2),limits, &FixtureBindings).enqueue_local(variable_item(1,"small"),&VariableConsumer,now()).await,Err(DeliveryError::Refused {reason:DeliveryRefusal::TooLarge {field:DeliverySizeField::Acceptance}})));
        assert!(matches!(Inbox::<_,VariableTypes, _>::new(scope,peer(2),limits, &FixtureBindings).enqueue_local(variable_item(2,"small"),&VariableConsumer,now()).await.map_err(storage_error)?,LocalItemAdmission::Replayed));
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
    store
        .transaction(|scope| {
            Box::pin(async move {
                let limits = variable_limits(100_000_000);
                for identity in [1, 2] {
                    let item = variable_item(identity, "small");
                    let input = SubmitInput {
                        item: item.id(),
                        digest: item.digest().clone(),
                        relay: peer(3),
                        mailbox: peer(2),
                    };
                    Outbox::<_, VariableTypes, _>::new(scope, limits, &FixtureBindings)
                        .enqueue(
                            item,
                            peer(3),
                            fleet_sync_delivery::Submitter::Machine(peer(1)),
                            SyncTime::from_unix_millis(0),
                        )
                        .await
                        .map_err(storage_error)?;
                    Outbox::<_, VariableTypes, _>::new(scope, limits, &FixtureBindings)
                        .record_submission_failure(
                            &input,
                            DeliveryFailureKind::CustodyMissing,
                            SyncTime::from_unix_millis(0),
                        )
                        .await
                        .map_err(storage_error)?;
                }
                sqlx_core::query::query::<sqlx_turso::Turso>(
                    "UPDATE _fleet_sync_outbox_items SET acceptance_bound=NULL WHERE item_id=?1",
                )
                .bind(item_id(2).as_uuid().to_string())
                .execute(scope.executor())
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    drop(store);
    let reopened = TestStore::open_and_migrate(directory.path().join("delivery.db"), host)
        .await
        .unwrap();
    reopened.transaction(|scope| Box::pin(async move {
        let limits=variable_limits(100_000_000);
        for identity in [1,2] {
            let row=sqlx_core::query::query::<sqlx_turso::Turso>(include_str!("../support/sql/read_outbox_header.sql")).bind(item_id(identity).as_uuid().to_string()).fetch_one(scope.executor()).await?;
            assert!(row.try_get::<Option<String>,_>("payload")?.is_none());
            let record:serde_json::Value=serde_json::from_str(&row.try_get::<String,_>("record")?).unwrap();
            assert_eq!(record["kind"],"recovered");
            assert_eq!(record["value"].get("acceptanceBound").and_then(serde_json::Value::as_u64),if identity==1 {Some(64)} else {None});
            Outbox::<_,VariableTypes, _>::new(scope,limits, &FixtureBindings).status(item_id(identity)).await.map_err(storage_error)?;
        }
        let status=serde_json::to_string(&DeliveryStatus::Delivered {acceptance:Acceptance {receipt:"x".repeat(100)}}).unwrap();
        sqlx_core::query::query::<sqlx_turso::Turso>("UPDATE _fleet_sync_outbox_items SET status_kind=json_extract(?1,'$.kind'), status_acceptance=?1 -> '$.value.acceptance'").bind(status).execute(scope.executor()).await?;
        assert!(matches!(Outbox::<_,VariableTypes, _>::new(scope,limits, &FixtureBindings).status(item_id(1)).await,Err(DeliveryError::Refused {reason:DeliveryRefusal::TooLarge {field:DeliverySizeField::Acceptance}})));
        assert!(matches!(Outbox::<_,VariableTypes, _>::new(scope,limits, &FixtureBindings).status(item_id(2)).await.map_err(storage_error)?.state,OutboxStatusState::Known {status:DeliveryStatus::Delivered { .. },..}));
        Ok(())
    })).await.unwrap();
}
