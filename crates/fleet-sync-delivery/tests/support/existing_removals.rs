use super::*;
use durable_steps::{AtomicStore, Engine, StoreError, storage::TransactionScope};
use sqlx_core::{migrate::Migrator, query::query, row::Row};
use sqlx_turso::Turso;

pub const DAY: i64 = 24 * 60 * 60 * 1000;
pub const START: i64 = 100 * DAY;
pub fn old_item(days_old: i64) -> ItemId {
    let created = u128::try_from(START - days_old * DAY).unwrap();
    ItemId::new(durable_steps::Uuid::from_u128(
        (created << 80) | 0x70008000000000000000 | u128::try_from(days_old).unwrap(),
    ))
    .unwrap()
}
pub async fn legacy_store() -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let host = host_at(SyncTime::from_unix_millis(START));
    let store = Arc::new(
        TestStore::open_and_migrate(directory.path().join("delivery.db"), host.clone())
            .await
            .unwrap(),
    );
    store.transaction(|scope|Box::pin(async move {
        let mut old=Migrator::with_migrations(DELIVERY_MIGRATOR.iter().filter(|migration|migration.version<4).cloned().collect());
        old.dangerous_set_table_name("_fleet_sync_delivery_migrations");
        old.run_direct(None,scope.executor(),false).await.map_err(StoreError::backend)?;
        for age in [10,70] {
            let item=DeliveryItem::<TestTypes>::new(old_item(age),"sender".into(),Recipient{peer:peer(2),address:"inbox".into()},None,"old custody".into()).unwrap();
            let digest=item.digest().clone();
            let payload=serde_json::to_string(&DeliveryEnvelope::<TestTypes>::Item(item)).unwrap();
            let reserve=limits().settlement_allowance();
            let record=serde_json::json!({"kind":"item","value":{"id":old_item(age),"digest":digest,"relay":peer(3),"recipient":peer(2),"validUntil":null,"status":{"kind":"queued"},"version":0,"submitAttempted":false,"cancel":null,"cancelTarget":null,"settlementBytes":reserve,"capacitySlot":true}}).to_string();
            query::<Turso>("INSERT INTO _fleet_sync_outbox_items(item_id,capacity_slot,live,payload_bytes,settlement_bytes,record,payload) VALUES(?1,1,1,?2,?3,?4,?5)")
                .bind(old_item(age).as_uuid().to_string()).bind(i64::try_from(payload.len()).map_err(StoreError::backend)?).bind(i64::from(reserve)).bind(record).bind(payload.clone()).execute(scope.executor()).await?;
            if age==70 {
                let record=serde_json::json!({"id":old_item(age),"digest":digest,"sender":"sender","recipient":{"peer":peer(2),"address":"inbox"},"relay":peer(3),"cancelTarget":null,"validUntil":null,"stream":"items","route":"relayed","progress":{"kind":"pending","value":{"attempt":0,"notBefore":0}},"reportSeq":0,"settlementBytes":reserve,"releaseRequested":false}).to_string();
                query::<Turso>("INSERT INTO _fleet_sync_inbox_items(item_id,live,payload_bytes,settlement_bytes,record,payload) VALUES(?1,1,?2,?3,?4,?5)")
                    .bind(old_item(age).as_uuid().to_string()).bind(i64::try_from(payload.len()).map_err(StoreError::backend)?).bind(i64::from(reserve)).bind(record).bind(payload).execute(scope.executor()).await?;
            }
        }
        let mut all=Migrator::with_migrations(DELIVERY_MIGRATOR.iter().cloned().collect());
        all.dangerous_set_table_name("_fleet_sync_delivery_migrations");
        all.run_direct(None,scope.executor(),false).await.map_err(StoreError::backend)?;
        Ok(())
    })).await.unwrap();
    Fixture {
        store,
        host,
        directory,
    }
}
pub async fn startup(store: &TestStore) -> usize {
    store
        .transaction(|scope| {
            Box::pin(async move {
                admit_existing_record_removals::<_, RecordRemoval>(
                    scope,
                    SyncTime::from_unix_millis(START),
                )
                .await
                .map_err(storage_error)
            })
        })
        .await
        .unwrap()
}
pub async fn schedule(store: &TestStore) -> Vec<(String, String, i64)> {
    store.transaction(|scope|Box::pin(async move {
        Ok(query::<Turso>("SELECT operation_id,request_key,next_attempt_at FROM _fleet_sync_operations WHERE kind='fleet-sync.delivery.remove-record' ORDER BY request_key").fetch_all(scope.executor()).await?.iter().map(|row|Ok((row.try_get("operation_id")?,row.try_get("request_key")?,row.try_get("next_attempt_at")?))).collect::<Result<Vec<_>,sqlx_core::Error>>()?)
    })).await.unwrap()
}
pub async fn custody(store: &TestStore) -> Vec<String> {
    store.transaction(|scope|Box::pin(async move {Ok(query::<Turso>("SELECT item_id FROM _fleet_sync_outbox_items UNION ALL SELECT item_id FROM _fleet_sync_inbox_items ORDER BY item_id").fetch_all(scope.executor()).await?.iter().map(|row|row.try_get("item_id")).collect::<Result<Vec<_>,_>>()?)})).await.unwrap()
}
pub fn engine(fixture: &Fixture) -> Engine<TestStore, TestHost> {
    let mut engine = Engine::new(fixture.store.clone(), fixture.host.clone());
    engine.register(RecordRemoval::new()).unwrap();
    engine
}
