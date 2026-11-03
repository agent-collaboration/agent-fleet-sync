use super::*;
use durable_steps::{AtomicStore, StoreError, storage::TransactionScope};
use sqlx_core::{migrate::Migrator, query::query, row::Row};
use sqlx_turso::Turso;

pub const BIRTH: i64 = 1_717_986_918_400;
pub const DAY: i64 = 86_400_000;
pub fn startup_time() -> SyncTime {
    SyncTime::from_unix_millis(BIRTH + 30 * DAY)
}
pub fn project() -> Submitter {
    Submitter::Project(
        DatabaseId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000001,
        ))
        .unwrap(),
    )
}

pub async fn legacy_store(compacted: usize) -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let host = host_at(startup_time());
    let store = Arc::new(
        TestStore::open_and_migrate(directory.path().join("delivery.db"), host.clone())
            .await
            .unwrap(),
    );
    store.transaction(move |scope|Box::pin(async move {
        let mut prefix=Migrator::with_migrations(DELIVERY_MIGRATOR.iter().filter(|migration|migration.version<=3).cloned().collect());
        prefix.dangerous_set_table_name("_fleet_sync_delivery_migrations");
        prefix.run_direct(None,scope.executor(),false).await.map_err(StoreError::backend)?;
        for (identity,owner,inbox) in [(1,Submitter::Machine(peer(1)),false),(2,project(),false),(3,Submitter::Machine(peer(1)),true),(4,project(),true)] {
            let packet=DeliveryEnvelope::<TestTypes>::Item(DeliveryItem::new(item_id(identity),owned_sender(owner,"legacy/session"),
                Recipient {peer:peer(2),address:"inbox".into()},None,"legacy body".into()).unwrap());
            let payload=serde_json::to_string(&packet).map_err(StoreError::backend)?;
            let reserve=limits().settlement_allowance();
            if inbox {
                let header=serde_json::json!({"id":item_id(identity),"digest":packet.digest(),"sender":packet.sender(),"recipient":packet.recipient(),"relay":peer(3),"cancelTarget":null,"validUntil":null,"stream":"items","route":"relayed","progress":{"kind":"pending","value":{"attempt":0,"notBefore":0}},"reportSeq":0,"settlementBytes":reserve,"releaseRequested":false}).to_string();
                query::<Turso>("INSERT INTO _fleet_sync_inbox_items(item_id,live,payload_bytes,settlement_bytes,record,payload) VALUES(?1,1,?2,?3,?4,?5)")
                    .bind(item_id(identity).as_uuid().to_string()).bind(i64::try_from(payload.len()).map_err(StoreError::backend)?).bind(i64::from(reserve)).bind(header).bind(payload).execute(scope.executor()).await?;
            } else {
                let header=serde_json::json!({"kind":"item","value":{"id":item_id(identity),"digest":packet.digest(),"relay":peer(3),"recipient":peer(2),"validUntil":null,"status":{"kind":"queued"},"version":0,"submitAttempted":false,"cancel":null,"cancelTarget":null,"settlementBytes":reserve,"capacitySlot":true}}).to_string();
                query::<Turso>("INSERT INTO _fleet_sync_outbox_items(item_id,capacity_slot,live,payload_bytes,settlement_bytes,record,payload) VALUES(?1,1,1,?2,?3,?4,?5)")
                    .bind(item_id(identity).as_uuid().to_string()).bind(i64::try_from(payload.len()).map_err(StoreError::backend)?).bind(i64::from(reserve)).bind(header).bind(payload).execute(scope.executor()).await?;
            }
        }
        for index in 0..compacted {
            let identity=100+u64::try_from(index).map_err(StoreError::backend)?;
            // Frozen v0.4.0 mark_submitted result: Sent is live, but save_header(None)
            // has released its only envelope/source. It is not a queued body.
            let header=serde_json::json!({"kind":"item","value":{"id":item_id(identity),"digest":"a".repeat(64),"relay":peer(3),"recipient":peer(2),"validUntil":null,"status":{"kind":"sent","value":{"seq":index+1}},"version":1,"submitAttempted":true,"cancel":null,"cancelTarget":null,"settlementBytes":limits().settlement_allowance(),"capacitySlot":true}}).to_string();
            query::<Turso>("INSERT INTO _fleet_sync_outbox_items(item_id,capacity_slot,live,payload_bytes,settlement_bytes,record,payload) VALUES(?1,1,1,0,?2,?3,NULL)")
                .bind(item_id(identity).as_uuid().to_string()).bind(i64::from(limits().settlement_allowance())).bind(header).execute(scope.executor()).await?;
        }
        let mut current=Migrator::with_migrations(DELIVERY_MIGRATOR.iter().cloned().collect());
        current.dangerous_set_table_name("_fleet_sync_delivery_migrations");
        current.run_direct(None,scope.executor(),false).await.map_err(StoreError::backend)?;
        Ok(())
    })).await.unwrap();
    Fixture {
        store,
        host,
        directory,
    }
}

pub async fn startup(store: &TestStore) -> Result<usize, DeliveryError> {
    store
        .transaction(|scope| {
            Box::pin(async move {
                backfill_submitters::<TestTypes, _>(scope, startup_time())
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .map_err(|error| DeliveryError::Storage(error.to_string()))
}
pub async fn snapshot(store: &TestStore) -> Vec<String> {
    store.transaction(|scope|Box::pin(async move {
        let rows=query::<Turso>("SELECT json_array('outbox',item_id,submitter_kind,submitter_id,consumer_sender,legacy_read_only,live,payload,status_kind,status_version,updated_at) AS row FROM _fleet_sync_outbox_items UNION ALL SELECT json_array('inbox',item_id,submitter_kind,submitter_id,consumer_sender,legacy_read_only,live,payload,progress,report_seq,updated_at) FROM _fleet_sync_inbox_items UNION ALL SELECT json_array('marker',id,name,created_at,updated_at,NULL,NULL,NULL,NULL,NULL,NULL) FROM _fleet_sync_delivery_backfills ORDER BY row")
            .fetch_all(scope.executor()).await?;
        rows.iter().map(|row|row.try_get("row").map_err(StoreError::backend)).collect()
    })).await.unwrap()
}
