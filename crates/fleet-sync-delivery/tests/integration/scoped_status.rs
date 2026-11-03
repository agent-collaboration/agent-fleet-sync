use super::support::*;
use durable_steps::{AtomicStore, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};

fn publication(
    item: u64,
    version: u64,
    status: DeliveryStatus<Acceptance>,
) -> StatusPublication<Acceptance> {
    StatusPublication {
        item: item_id(item),
        version: StatusVersion::new(version),
        status,
        cancel: None,
    }
}

#[tokio::test]
async fn two_submitters_keep_independent_events_acknowledgements_and_publications_after_reopen() {
    let Fixture {
        store,
        host,
        directory,
    } = Fixture::new().await;
    let (first_reference, second_reference) = store.transaction(|scope| Box::pin(async move {
        let (first, _) = StatusStream::<_, TestTypes>::new(scope, peer(1), "route-a".into())
            .append(&publication(1,1,DeliveryStatus::AtRecipient), fleet_sync_protocol::SyncTime::from_unix_millis(0)).await.map_err(storage_error)?;
        let (second, _) = StatusStream::<_, TestTypes>::new(scope, peer(3), "route-c".into())
            .append(&publication(2,1,DeliveryStatus::AtRecipient), fleet_sync_protocol::SyncTime::from_unix_millis(0)).await.map_err(storage_error)?;
        assert_eq!(first.cursor.value(),1);
        assert_eq!(second.cursor.value(),1);
        let (first, first_reference) = StatusStream::<_, TestTypes>::new(scope, peer(1), "route-a".into())
            .append(&publication(1,2,DeliveryStatus::Held), fleet_sync_protocol::SyncTime::from_unix_millis(0)).await.map_err(storage_error)?;
        let (second, second_reference) = StatusStream::<_, TestTypes>::new(scope, peer(3), "route-c".into())
            .append(&publication(2,2,DeliveryStatus::Uncertain), fleet_sync_protocol::SyncTime::from_unix_millis(0)).await.map_err(storage_error)?;
        assert_eq!(first.cursor.value(),2);
        assert_eq!(second.cursor.value(),2);
        assert!(matches!(StatusStream::<_, TestTypes>::new(scope, peer(1), "route-a".into())
            .acknowledge(StatusCursor::new(1), fleet_sync_protocol::SyncTime::from_unix_millis(0)).await.map_err(storage_error)?, StatusAckResult::Acknowledged { floor } if floor.value()==1));
        Ok((first_reference.unwrap(),second_reference.unwrap()))
    })).await.unwrap();

    let sender = Fixture::new().await;
    sender
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let DeliveryEnvelope::Item(original) = item(1, "sender custody") else {
                    return Err(StoreError::backend("item fixture"));
                };
                Outbox::<_, TestTypes>::new(scope, limits())
                    .enqueue(original, peer(9), peer(1), SyncTime::from_unix_millis(10))
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    let first_publication = store
        .transaction({
            let reference = first_reference.clone();
            move |scope| {
                Box::pin(async move {
                    Ok(resolve_publication::<_, TestTypes>(scope, reference)
                        .await
                        .map_err(storage_error)?
                        .unwrap())
                })
            }
        })
        .await
        .unwrap();
    sender.store.transaction(move |scope| Box::pin(async move {
        Outbox::<_,TestTypes>::new(scope,limits()).publish_status(first_publication,SyncTime::from_unix_millis(20)).await.map_err(storage_error)?;
        assert!(matches!(Outbox::<_,TestTypes>::new(scope,limits()).status(item_id(1)).await.map_err(storage_error)?.state,
            OutboxStatusState::Known { status:DeliveryStatus::Held,version,.. } if version.value()==2));
        Ok(())
    })).await.unwrap();
    store
        .transaction(move |scope| {
            Box::pin(async move {
                retire_publication::<_, TestTypes>(scope, first_reference)
                    .await
                    .map_err(storage_error)?;
                StatusStream::<_, TestTypes>::new(scope, peer(1), "route-a".into())
                    .acknowledge(
                        StatusCursor::new(2),
                        fleet_sync_protocol::SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    drop(store);
    let reopened = TestStore::open_and_migrate(directory.path().join("delivery.db"), host)
        .await
        .unwrap();
    reopened.transaction(move |scope| Box::pin(async move {
        let first = StatusStream::<_,TestTypes>::new(scope,peer(1),"route-a".into()).fetch(peer(1),StatusCursor::new(0),10).await.map_err(storage_error)?;
        assert!(first.events.is_empty());
        let second = StatusStream::<_,TestTypes>::new(scope,peer(3),"route-c".into()).fetch(peer(3),StatusCursor::new(0),10).await.map_err(storage_error)?;
        assert_eq!(second.events.len(),2);
        assert_eq!(second.through.value(),2);
        assert_eq!(second.events.first().unwrap().item,item_id(2));
        assert_eq!(second.events.first().unwrap().cursor.value(),1);
        assert_eq!(second.events.first().unwrap().version.value(),1);
        assert!(matches!(second.events.first().unwrap().status,DeliveryStatus::AtRecipient));
        assert_eq!(second.events.last().unwrap().item,item_id(2));
        assert_eq!(second.events.last().unwrap().cursor.value(),2);
        assert_eq!(second.events.last().unwrap().version.value(),2);
        assert!(matches!(second.events.last().unwrap().status,DeliveryStatus::Uncertain));
        assert!(matches!(&second_reference.target,PublicationTarget::Peer {peer:owner,route} if *owner==peer(3)&&route=="route-c"));
        let owed = resolve_publication::<_,TestTypes>(scope,second_reference).await.map_err(storage_error)?.unwrap();
        assert_eq!(owed.item,item_id(2));
        assert_eq!(owed.version.value(),2);
        assert!(matches!(owed.status,DeliveryStatus::Uncertain));
        let rows = query::<sqlx_turso::Turso>("SELECT machine_id FROM _fleet_sync_status_owed").fetch_all(scope.executor()).await?;
        assert_eq!(rows.len(),1);
        assert_eq!(rows.first().unwrap().try_get::<String,_>("machine_id")?,peer(3).as_uuid().to_string());
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn scoped_upgrade_preserves_an_existing_floor_events_and_both_publication_hops() {
    let fixture = Fixture::new().await;
    let store = TestStore::open_and_migrate(
        fixture.directory.path().join("old-status.db"),
        fixture.host.clone(),
    )
    .await
    .unwrap();
    store.transaction(|scope| Box::pin(async move {
        let mut old = sqlx_core::migrate::Migrator::with_migrations(DELIVERY_MIGRATOR.iter().filter(|migration| migration.version<5).cloned().collect::<Vec<_>>());
        old.dangerous_set_table_name("_fleet_sync_delivery_migrations");
        old.run_direct(None,scope.executor(),false).await.map_err(StoreError::backend)?;
        let state = serde_json::json!({"sender":peer(1),"ownerRoute":"route-a","head":7,"floor":5}).to_string();
        query::<sqlx_turso::Turso>("INSERT INTO _fleet_sync_status_state(singleton,record) VALUES(1,?1)")
            .bind(state).execute(scope.executor()).await?;
        for (cursor,identity,version,status) in [(6,30,1,DeliveryStatus::AtRecipient),(7,31,2,DeliveryStatus::Held)] {
            let event = StatusEvent::<Acceptance> {cursor:StatusCursor::new(cursor),item:item_id(identity),version:StatusVersion::new(version),status,cancel:None};
            query::<sqlx_turso::Turso>("INSERT INTO _fleet_sync_status_events(cursor,item_id,version,record) VALUES(?1,?2,?3,?4)")
                .bind(cursor as i64).bind(item_id(identity).as_uuid().to_string()).bind(version as i64).bind(serde_json::to_string(&event).unwrap())
                .execute(scope.executor()).await?;
        }
        for target in [PublicationTarget::Peer {peer:peer(1),route:"route-a".to_owned()},PublicationTarget::StatusStream {sender:peer(1),route:"route-a".to_owned()}] {
            query::<sqlx_turso::Turso>("INSERT INTO _fleet_sync_status_owed(item_id,target,version,record) VALUES(?1,?2,2,?3)")
                .bind(item_id(31).as_uuid().to_string()).bind(serde_json::to_value(target).unwrap().to_string())
                .bind(serde_json::to_string(&publication(31,2,DeliveryStatus::Held)).unwrap())
                .execute(scope.executor()).await?;
        }
        let mut current = sqlx_core::migrate::Migrator::with_migrations(DELIVERY_MIGRATOR.iter().cloned().collect::<Vec<_>>());
        current.dangerous_set_table_name("_fleet_sync_delivery_migrations");
        current.run_direct(None,scope.executor(),false).await.map_err(StoreError::backend)?;
        Ok(())
    })).await.unwrap();
    drop(store);
    let reopened = TestStore::open_and_migrate(
        fixture.directory.path().join("old-status.db"),
        fixture.host.clone(),
    )
    .await
    .unwrap();
    reopened.transaction(|scope| Box::pin(async move {
        let mut stream = StatusStream::<_,TestTypes>::new(scope,peer(1),"route-a".into());
        let batch = stream.fetch(peer(1),StatusCursor::new(0),10).await.map_err(storage_error)?;
        assert_eq!(batch.through.value(),7);
        assert_eq!(batch.events.iter().map(|event|(event.cursor.value(),event.item,event.version.value())).collect::<Vec<_>>(),
            vec![(6,item_id(30),1),(7,item_id(31),2)]);
        assert!(matches!(stream.acknowledge(StatusCursor::new(4), fleet_sync_protocol::SyncTime::from_unix_millis(0)).await.map_err(storage_error)?,StatusAckResult::Acknowledged {floor} if floor.value()==5));
        for target in [PublicationTarget::Peer {peer:peer(1),route:"route-a".to_owned()},PublicationTarget::StatusStream {sender:peer(1),route:"route-a".to_owned()}] {
            let publication = resolve_publication::<_,TestTypes>(scope,PublicationRef {item:item_id(31),version:StatusVersion::new(2),target}).await.map_err(storage_error)?.unwrap();
            assert!(matches!(publication.status,DeliveryStatus::Held));
        }
        let (new_event,_) = StatusStream::<_,TestTypes>::new(scope,peer(3),"route-c".into())
            .append(&publication(32,1,DeliveryStatus::Uncertain), fleet_sync_protocol::SyncTime::from_unix_millis(0)).await.map_err(storage_error)?;
        assert_eq!(new_event.cursor.value(),1);
        Ok(())
    })).await.unwrap();
}
