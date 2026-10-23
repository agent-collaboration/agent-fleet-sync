#![allow(clippy::unwrap_used)]
mod support;
use durable_steps::{AtomicStore, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::row::Row;
use support::*;

const HISTORY: u64 = 1024;
const DEADLINES: &str = include_str!("../sql/read_mail_deadlines.sql");
const DEADLINE_PLAN: &str = concat!(
    "EXPLAIN QUERY PLAN ",
    include_str!("../sql/read_mail_deadlines.sql")
);
const RECEIPTS: &str = include_str!("../sql/read_mail_receipt_batch.sql");
const RECEIPT_PLAN: &str = concat!(
    "EXPLAIN QUERY PLAN ",
    include_str!("../sql/read_mail_receipt_batch.sql")
);

async fn history_fixture() -> Fixture {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut mailbox = Mailbox::<_, TestTypes>::new(scope, peer(2), limits());
                for id in 1..=HISTORY {
                    let expired = DeliveryEnvelope::Item(
                        DeliveryItem::<TestTypes>::new(
                            item_id(id),
                            "sender".into(),
                            Recipient {
                                peer: peer(2),
                                address: "inbox".into(),
                            },
                            Some(SyncTime::from_unix_millis(0)),
                            "history".into(),
                        )
                        .unwrap(),
                    );
                    mailbox
                        .submit(
                            peer(1),
                            expired,
                            SyncTime::from_unix_millis(0),
                            "fixture-route".to_owned(),
                            None,
                        )
                        .await
                        .map_err(storage_error)?;
                    mailbox
                        .retire(peer(1), &call(&format!("retire-{id}")), &[item_id(id)])
                        .await
                        .map_err(storage_error)?;
                }
                mailbox
                    .received(
                        peer(2),
                        DeliveryStream::Items,
                        DeliverySeq::new(HISTORY),
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                let fetched = DeliveryEnvelope::Item(
                    DeliveryItem::<TestTypes>::new(
                        item_id(2001),
                        "sender".into(),
                        Recipient {
                            peer: peer(2),
                            address: "inbox".into(),
                        },
                        Some(SyncTime::from_unix_millis(20)),
                        "fetched".into(),
                    )
                    .unwrap(),
                );
                mailbox
                    .submit(
                        peer(1),
                        fetched,
                        SyncTime::from_unix_millis(0),
                        "fixture-route".to_owned(),
                        None,
                    )
                    .await
                    .map_err(storage_error)?;
                mailbox
                    .fetch(
                        peer(2),
                        DeliveryStream::Items,
                        DeliverySeq::new(HISTORY),
                        1,
                        100_000,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                for (id, until) in [(2002, Some(50)), (2003, None), (2004, Some(100))] {
                    let live = DeliveryEnvelope::Item(
                        DeliveryItem::<TestTypes>::new(
                            item_id(id),
                            "sender".into(),
                            Recipient {
                                peer: peer(2),
                                address: "inbox".into(),
                            },
                            until.map(SyncTime::from_unix_millis),
                            "live".into(),
                        )
                        .unwrap(),
                    );
                    mailbox
                        .submit(
                            peer(1),
                            live,
                            SyncTime::from_unix_millis(0),
                            "fixture-route".to_owned(),
                            None,
                        )
                        .await
                        .map_err(storage_error)?;
                }
                Ok(())
            })
        })
        .await
        .unwrap();
    fixture
}

#[tokio::test]
async fn deadline_selection_returns_only_the_fixed_live_unfetched_set_and_uses_its_index() {
    let fixture = history_fixture().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let rows = sqlx_core::query::query::<sqlx_turso::Turso>(DEADLINES)
                    .fetch_all(scope.executor())
                    .await
                    .map_err(StoreError::backend)?;
                assert_eq!(
                    rows.len(),
                    3,
                    "permanent compacts and fetched custody are not deadline work"
                );
                let ids: Vec<_> = rows
                    .iter()
                    .map(|row| row.try_get::<String, _>("item_id").unwrap())
                    .collect();
                assert_eq!(
                    ids,
                    [item_id(2003), item_id(2002), item_id(2004)]
                        .map(|id| id.as_uuid().to_string())
                );
                let plan = sqlx_core::query::query::<sqlx_turso::Turso>(DEADLINE_PLAN)
                    .fetch_all(scope.executor())
                    .await
                    .map_err(StoreError::backend)?;
                let details: Vec<String> = plan
                    .iter()
                    .map(|row| row.try_get("detail").unwrap())
                    .collect();
                assert!(
                    details
                        .iter()
                        .any(|detail| detail.contains("_fleet_sync_mail_deadlines")),
                    "{details:?}"
                );
                assert!(
                    !details.iter().any(|detail| detail.contains("TEMP B-TREE")),
                    "{details:?}"
                );
                eprintln!(
                    "OBS E-R4 deadlines: history={HISTORY}, selected={}, plan={details:?}",
                    rows.len()
                );
                assert_eq!(
                    Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .expire_due(SyncTime::from_unix_millis(49))
                        .await
                        .map_err(storage_error)?,
                    Some(SyncTime::from_unix_millis(50))
                );
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn receipt_selection_reads_only_the_new_interval_after_a_high_acknowledged_cursor() {
    let fixture = history_fixture().await;
    fixture.store.transaction(|scope| Box::pin(async move {
        let rows = sqlx_core::query::query::<sqlx_turso::Turso>(RECEIPTS).bind("items")
            .bind(HISTORY.to_string()).bind((HISTORY + 1).to_string()).fetch_all(scope.executor()).await.map_err(StoreError::backend)?;
        assert_eq!(rows.len(), 1, "only previous < seq <= through may enter receipt decoding");
        assert_eq!(rows[0].try_get::<String, _>("item_id").unwrap(), item_id(2001).as_uuid().to_string());
        let plan = sqlx_core::query::query::<sqlx_turso::Turso>(RECEIPT_PLAN).bind("items")
            .bind(HISTORY.to_string()).bind((HISTORY + 1).to_string()).fetch_all(scope.executor()).await.map_err(StoreError::backend)?;
        let details: Vec<String> = plan.iter().map(|row| row.try_get("detail").unwrap()).collect();
        assert!(details.iter().any(|detail| detail.contains("INDEX")), "{details:?}");
        let receipt = Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
            .received(peer(2), DeliveryStream::Items, DeliverySeq::new(HISTORY + 1), SyncTime::from_unix_millis(1)).await.map_err(storage_error)?;
        assert_eq!(receipt.released_through.value(), HISTORY + 1);
        eprintln!("OBS E-R4 receipts: previous={HISTORY}, through={}, selected={}, plan={details:?}", HISTORY + 1, rows.len());
        Ok(())
    })).await.unwrap();
}
