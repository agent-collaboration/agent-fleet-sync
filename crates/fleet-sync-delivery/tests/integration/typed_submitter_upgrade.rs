use super::support::*;
use durable_steps::{AtomicStore, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::{migrate::Migrator, query::query, row::Row};
use sqlx_turso::Turso;

#[tokio::test]
async fn intermediate_typed_status_and_custody_upgrades_preserve_both_owner_kinds_and_sources() {
    let fixture = Fixture::new().await;
    let store = TestStore::open_and_migrate(
        fixture.directory.path().join("typed-prefix.db"),
        fixture.host.clone(),
    )
    .await
    .unwrap();
    store.transaction(|scope|Box::pin(async move {
        let mut prefix=Migrator::with_migrations(DELIVERY_MIGRATOR.iter().filter(|migration|migration.version<=6).cloned().collect());
        prefix.dangerous_set_table_name("_fleet_sync_delivery_migrations");
        prefix.run_direct(None,scope.executor(),false).await.map_err(StoreError::backend)?;
        let owner=peer(1).as_uuid().to_string();
        for (kind,identity) in [("machine",1),("project",2)] {
            let source=owned_sender(if kind=="machine" {Submitter::Machine(peer(1))} else {Submitter::Project(DatabaseId::new(peer(1).as_uuid()).unwrap())},"session/exact");
            query::<Turso>("INSERT INTO _fleet_sync_outbox_items(item_id,live,payload_bytes,settlement_charge_bytes,capacity_slot,record_kind,consumer_sender,submitter_kind,submitter_id,last_report_seq,created_at) VALUES(?1,0,0,0,0,'recovered',?2,?3,?4,0,1717986918400)")
                .bind(item_id(identity).as_uuid().to_string()).bind(serde_json::to_string(&source).unwrap()).bind(kind).bind(owner.clone()).execute(scope.executor()).await?;
            let state=serde_json::json!({"ownerRoute":"typed route","head":7,"floor":5}).to_string();
            query::<Turso>("INSERT INTO _fleet_sync_status_state(submitter_kind,submitter_id,record) VALUES(?1,?2,?3)").bind(kind).bind(owner.clone()).bind(state).execute(scope.executor()).await?;
            let event=StatusEvent::<Acceptance> {cursor:StatusCursor::new(6),item:item_id(identity),version:StatusVersion::new(2),status:DeliveryStatus::Held,cancel:None};
            let event=serde_json::to_string(&event).unwrap();
            query::<Turso>("INSERT INTO _fleet_sync_status_events(submitter_kind,submitter_id,cursor,item_id,version,record) VALUES(?1,?2,6,?3,2,?4)").bind(kind).bind(owner.clone()).bind(item_id(identity).as_uuid().to_string()).bind(event.clone()).execute(scope.executor()).await?;
            let target=serde_json::json!({"kind":"statusStream","value":{"sender":{"kind":kind,"value":owner},"route":"typed route"}}).to_string();
            query::<Turso>("INSERT INTO _fleet_sync_status_owed(item_id,target,version,record,submitter_kind,submitter_id) VALUES(?1,?2,2,?3,?4,?5)").bind(item_id(identity).as_uuid().to_string()).bind(target).bind(event).bind(kind).bind(owner.clone()).execute(scope.executor()).await?;
        }
        let mut current=Migrator::with_migrations(DELIVERY_MIGRATOR.iter().cloned().collect());
        current.dangerous_set_table_name("_fleet_sync_delivery_migrations");
        current.run_direct(None,scope.executor(),false).await.map_err(StoreError::backend)?;
        for (kind,identity) in [("machine",1),("project",2)] {
            let submitter=if kind=="machine" {Submitter::Machine(peer(1))} else {Submitter::Project(DatabaseId::new(peer(1).as_uuid()).unwrap())};
            let custody=query::<Turso>("SELECT submitter_kind,submitter_id,consumer_sender,created_at FROM _fleet_sync_outbox_items WHERE item_id=?1").bind(item_id(identity).as_uuid().to_string()).fetch_one(scope.executor()).await?;
            assert_eq!(custody.try_get::<String,_>("submitter_kind")?,kind);
            assert_eq!(custody.try_get::<String,_>("submitter_id")?,owner);
            assert_eq!(custody.try_get::<String,_>("consumer_sender")?,serde_json::to_string(&owned_sender(submitter,"session/exact")).unwrap());
            assert_eq!(custody.try_get::<i64,_>("created_at")?,1_717_986_918_400);
            let batch=StatusStream::<_,TestTypes>::new(scope,submitter,"typed route".into()).fetch(submitter,StatusCursor::new(0),10).await.map_err(storage_error)?;
            assert_eq!(batch.events.len(),1);
            let event=batch.events.first().unwrap();
            assert_eq!((event.cursor.value(),event.item,event.version.value()),(6,item_id(identity),2));
            assert!(matches!(event.status,DeliveryStatus::Held));
            assert!(matches!(StatusStream::<_,TestTypes>::new(scope,submitter,"typed route".into()).acknowledge(StatusCursor::new(4),SyncTime::from_unix_millis(0)).await.map_err(storage_error)?,StatusAckResult::Acknowledged {floor} if floor.value()==5));
            let publication=resolve_publication::<_,TestTypes>(scope,PublicationRef {item:item_id(identity),version:StatusVersion::new(2),target:PublicationTarget::StatusStream {sender:submitter,route:"typed route".into()}}).await.map_err(storage_error)?.unwrap();
            assert!(matches!(publication.status,DeliveryStatus::Held));
        }
        Ok(())
    })).await.unwrap();
}
