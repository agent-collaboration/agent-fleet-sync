#![allow(clippy::unwrap_used)]
mod support;
use durable_steps::{AtomicStore, SyncTime, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::row::Row;
use support::{network::PublishTask, *};

#[tokio::test]
async fn allowance_covers_four_maximum_routes_for_both_item_and_cancel() {
    let fixture = Fixture::new().await;
    fixture.store.transaction(|scope| Box::pin(async move {
        let mut mailbox = Mailbox::<_, TestTypes>::new(scope, peer(2), limits());
        for (_, envelope) in [(1, item(1, "body")), (2, cancel(2, 1))] {
            let route = "a".repeat(62);
            assert_eq!(serde_json::to_vec(&route).unwrap().len(), TestTypes::MAX_STATUS_ROUTE_BYTES as usize);
            mailbox.submit(peer(1), envelope, SyncTime::from_unix_millis(0), route, Some(0)).await.map_err(storage_error)?;
        }
        // Each status advance is committed against a different maximum-length route.
        for (rank, marker) in [(1, 'b'), (2, 'c'), (3, 'd')] {
            for (identity, envelope) in [(1, item(1, "body")), (2, cancel(2, 1))] {
                mailbox.submit(peer(u128::from(rank)+2), envelope, SyncTime::from_unix_millis(0), marker.to_string().repeat(62), Some(rank)).await.map_err(storage_error)?;
                if rank == 1 {
                    let stream = if identity == 1 { DeliveryStream::Items } else { DeliveryStream::Controls };
                    let batch = mailbox.fetch(peer(2), stream, DeliverySeq::new(0), 10, 100_000, SyncTime::from_unix_millis(0)).await.map_err(storage_error)?;
                    mailbox.received(peer(2), stream, batch.through, SyncTime::from_unix_millis(0)).await.map_err(storage_error)?;
                } else {
                    let outcome = if rank == 2 { RecipientOutcome::Held } else { RecipientOutcome::Delivered { acceptance:Acceptance { receipt:"x".repeat(114) } } };
                    mailbox.report(peer(2), RecipientReport { item:item_id(identity), report_seq:ReportSeq::new(rank), outcome }).await.map_err(storage_error)?;
                }
            }
        }
        let rows = sqlx_core::query::query::<sqlx_turso::Turso>("SELECT item_id,target,record,version FROM _fleet_sync_status_owed ORDER BY item_id,version").fetch_all(scope.executor()).await?;
        assert_eq!(rows.len(), 8, "four historical targets for item and cancel");
        let mut retained_bytes = 0usize;
        for (index, row) in rows.iter().enumerate() {
            let target:String = row.try_get("target")?;
            let record:String = row.try_get("record")?;
            let target_value:serde_json::Value = serde_json::from_str(&target).unwrap();
            let publication:serde_json::Value = serde_json::from_str(&record).unwrap();
            assert_eq!(serde_json::to_vec(&target_value["value"]["route"]).unwrap().len(), 64);
            assert_eq!(publication["version"], (index % 4 + 1) as u64);
            retained_bytes += target.len() + record.len() + 3; // tuple brackets and comma
        }
        let headers = sqlx_core::query::query::<sqlx_turso::Turso>("SELECT record FROM _fleet_sync_mail_items ORDER BY item_id").fetch_all(scope.executor()).await?;
        assert_eq!(headers.len(), 2);
        for row in headers { retained_bytes += row.try_get::<String,_>("record")?.len(); }
        retained_bytes += serde_json::to_vec(&cancel(2, 1)).unwrap().len();
        let allowance = minimum_settlement_allowance::<TestTypes>().unwrap() as usize;
        assert!(retained_bytes <= allowance, "retained routed settlement {retained_bytes} exceeds computed allowance {allowance}");
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn oversized_route_is_refused_before_admission_and_cannot_replace_a_live_target() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut mailbox = Mailbox::<_, TestTypes>::new(scope, peer(2), limits());
                assert!(matches!(
                    mailbox
                        .submit(
                            peer(1),
                            item(1, "body"),
                            SyncTime::from_unix_millis(0),
                            "x".repeat(63),
                            None
                        )
                        .await,
                    Err(DeliveryError::Refused {
                        reason: DeliveryRefusal::TooLarge {
                            field: DeliverySizeField::StatusRoute
                        }
                    })
                ));
                let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT COUNT(*) AS count FROM _fleet_sync_mail_items",
                )
                .fetch_one(scope.executor())
                .await?;
                assert_eq!(row.try_get::<i64, _>("count")?, 0);
                Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .submit(
                        peer(1),
                        item(1, "body"),
                        SyncTime::from_unix_millis(0),
                        "valid".into(),
                        Some(0),
                    )
                    .await
                    .map_err(storage_error)?;
                let before = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT record FROM _fleet_sync_mail_items",
                )
                .fetch_one(scope.executor())
                .await?
                .try_get::<String, _>("record")?;
                assert!(matches!(
                    Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .submit(
                            peer(3),
                            item(1, "body"),
                            SyncTime::from_unix_millis(0),
                            "x".repeat(63),
                            Some(1)
                        )
                        .await,
                    Err(DeliveryError::Refused {
                        reason: DeliveryRefusal::TooLarge {
                            field: DeliverySizeField::StatusRoute
                        }
                    })
                ));
                let after = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT record FROM _fleet_sync_mail_items",
                )
                .fetch_one(scope.executor())
                .await?
                .try_get::<String, _>("record")?;
                assert_eq!(before, after);
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn authoritative_route_bounds_reject_corrupt_mail_publication_and_stream_state() {
    for owner in ["mail", "publication", "stream"] {
        let fixture = Fixture::new().await;
        fixture.store.transaction(move |scope| Box::pin(async move {
            Mailbox::<_, TestTypes>::new(scope, peer(2), limits()).submit(peer(1), item(1, "body"), SyncTime::from_unix_millis(0), "valid".into(), None).await.map_err(storage_error)?;
            let publication = StatusPublication { item:item_id(1), status:DeliveryStatus::Sent { seq:DeliverySeq::new(1) }, version:StatusVersion::new(1), cancel:None };
            StatusStream::<_, TestTypes>::new(scope, peer(1), "valid".into()).append(&publication).await.map_err(storage_error)?;
            let statement = match owner {
                "mail" => "UPDATE _fleet_sync_mail_items SET record=json_set(record,'$.value.statusTarget.route',?1)",
                "publication" => "UPDATE _fleet_sync_status_owed SET target=json_set(target,'$.value.route',?1)",
                _ => "UPDATE _fleet_sync_status_state SET record=json_set(record,'$.ownerRoute',?1)",
            };
            sqlx_core::query::query::<sqlx_turso::Turso>(statement).bind("x".repeat(63)).execute(scope.executor()).await?;
            let result = match owner {
                "mail" => Mailbox::<_, TestTypes>::new(scope, peer(2), limits()).query(&[item_id(1)]).await.map(|_| ()),
                "publication" => admit_publications::<_, TestTypes, PublishTask>(scope).await.map(|_| ()),
                _ => StatusStream::<_, TestTypes>::new(scope, peer(1), "valid".into()).fetch(peer(1), StatusCursor::new(0), 10).await.map(|_| ()),
            };
            assert!(matches!(result, Err(DeliveryError::Corrupt { .. })), "{owner} accepted corrupt route: {result:?}");
            Ok(())
        })).await.unwrap();
    }
}
