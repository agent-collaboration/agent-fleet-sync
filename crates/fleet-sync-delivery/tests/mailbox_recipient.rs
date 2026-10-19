#![allow(clippy::unwrap_used)]
mod support;
use durable_steps::{AtomicStore, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::row::Row;
use sqlx_turso::Turso;
use support::*;

async fn stored_recipient(store: &TestStore) -> Option<PeerId> {
    store
        .transaction(|scope| {
            Box::pin(async move {
                Mailbox::<_, TestTypes>::stored_recipient(scope)
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap()
}

async fn custody_rows(store: &TestStore) -> Vec<String> {
    store.transaction(|scope| Box::pin(async move {
        sqlx_core::query::query::<Turso>(
            "SELECT 'state:' || json_array(singleton,record) AS value FROM _fleet_sync_mail_state
             UNION ALL SELECT 'mail:' || json_array(item_id,stream,seq,live,capacity_slot,payload_bytes,settlement_bytes,record) FROM _fleet_sync_mail_items
             UNION ALL SELECT 'owed:' || json_array(item_id,target,version,record) FROM _fleet_sync_status_owed
             ORDER BY value"
        ).fetch_all(scope.executor()).await?.into_iter()
            .map(|row| row.try_get("value").map_err(StoreError::backend)).collect()
    })).await.unwrap()
}

#[tokio::test]
async fn stored_recipient_is_read_only_and_each_store_retains_one_owner() {
    let first = Fixture::new().await;
    let second = Fixture::new().await;
    assert!(custody_rows(&first.store).await.is_empty());
    assert_eq!(stored_recipient(&first.store).await, None);
    assert!(custody_rows(&first.store).await.is_empty());
    assert_eq!(stored_recipient(&second.store).await, None);
    assert!(custody_rows(&second.store).await.is_empty());
    for (store, owner) in [(&first.store, peer(2)), (&second.store, peer(3))] {
        store
            .transaction(move |scope| {
                Box::pin(async move {
                    Mailbox::<_, TestTypes>::new(scope, owner, limits())
                        .open()
                        .await
                        .map_err(storage_error)
                })
            })
            .await
            .unwrap();
        assert_eq!(stored_recipient(store).await, Some(owner));
    }
    let before = custody_rows(&first.store).await;
    let refused = first
        .store
        .transaction(|scope| {
            Box::pin(async move {
                // Commit a refusal rather than letting rollback conceal a partial owner change.
                Ok(Mailbox::<_, TestTypes>::new(scope, peer(4), limits())
                    .open()
                    .await)
            })
        })
        .await
        .unwrap();
    assert!(matches!(refused, Err(DeliveryError::Corrupt { .. })));
    assert_eq!(custody_rows(&first.store).await, before);
    assert_eq!(stored_recipient(&first.store).await, Some(peer(2)));
    assert_eq!(stored_recipient(&second.store).await, Some(peer(3)));
}

fn expiring_item(identity: u64, deadline: i64) -> DeliveryEnvelope<TestTypes> {
    DeliveryEnvelope::Item(
        DeliveryItem::new(
            item_id(identity),
            "sender".into(),
            Recipient {
                peer: peer(2),
                address: "inbox".into(),
            },
            Some(SyncTime::from_unix_millis(deadline)),
            "never fetched".into(),
        )
        .unwrap(),
    )
}

#[tokio::test]
async fn reopened_alarm_discovers_owner_and_expires_custody_with_its_owed_publication() {
    let Fixture {
        store,
        host,
        directory,
    } = Fixture::new().await;
    store.transaction(|scope| Box::pin(async move {
        let mut mailbox = Mailbox::<_, TestTypes>::new(scope, peer(2), limits());
        for (identity, deadline) in [(1, 50), (2, 75)] {
            let admitted = mailbox.submit(
                peer(1), expiring_item(identity, deadline), SyncTime::from_unix_millis(0),
                "fixture-route".into(), None,
            ).await.map_err(storage_error)?;
            assert!(matches!(admitted, SubmitResult::Accepted { status: DeliveryStatus::Sent { .. }, version, .. } if version.value() == 1));
        }
        Ok(())
    })).await.unwrap();
    drop(store);
    let reopened = TestStore::open_and_migrate(directory.path().join("delivery.db"), host)
        .await
        .unwrap();
    let before = custody_rows(&reopened).await;
    for rollback in [true, false] {
        let result = reopened.transaction(move |scope| Box::pin(async move {
            // No request supplies a peer. Discovery and expiry borrow this one real transaction.
            let recipient = Mailbox::<_, TestTypes>::stored_recipient(scope)
                .await.map_err(storage_error)?
                .ok_or_else(|| StoreError::backend("missing durable recipient"))?;
            assert_eq!(recipient, peer(2));
            let mut mailbox = Mailbox::<_, TestTypes>::new(scope, recipient, limits());
            let next = mailbox.expire_due(SyncTime::from_unix_millis(50))
                .await.map_err(storage_error)?;
            assert_eq!(next, Some(SyncTime::from_unix_millis(75)));
            let statuses = mailbox.query(&[item_id(1), item_id(2)])
                .await.map_err(storage_error)?.statuses;
            assert_eq!(statuses.len(), 2);
            assert!(matches!(statuses.first().unwrap().state,
                ItemStatusState::Known { status: DeliveryStatus::Expired, version, .. } if version.value() == 2));
            assert!(matches!(statuses.get(1).unwrap().state,
                ItemStatusState::Known { status: DeliveryStatus::Sent { .. }, version, .. } if version.value() == 1));
            let row = sqlx_core::query::query::<Turso>(
                "SELECT version,record FROM _fleet_sync_status_owed WHERE item_id=?1",
            ).bind(item_id(1).as_uuid().to_string()).fetch_one(scope.executor()).await?;
            assert_eq!(row.try_get::<i64, _>("version").unwrap(), 2);
            let publication: StatusPublication<Acceptance> =
                serde_json::from_str(&row.try_get::<String, _>("record").unwrap()).unwrap();
            assert_eq!(publication.item, item_id(1));
            assert_eq!(publication.version.value(), 2);
            assert!(matches!(publication.status, DeliveryStatus::Expired));
            if rollback {
                return Err(StoreError::Aborted { reason: "after observed expiry and publication before commit".into() });
            }
            Ok(next)
        })).await;
        if rollback {
            assert!(matches!(result, Err(StoreError::Aborted { .. })));
            assert_eq!(custody_rows(&reopened).await, before);
        } else {
            assert_eq!(result.unwrap(), Some(SyncTime::from_unix_millis(75)));
        }
    }
    let committed = custody_rows(&reopened).await;
    assert_ne!(committed, before);
    reopened
        .transaction(|scope| {
            Box::pin(async move {
                let recipient = Mailbox::<_, TestTypes>::stored_recipient(scope)
                    .await
                    .map_err(storage_error)?
                    .unwrap();
                assert_eq!(
                    Mailbox::<_, TestTypes>::new(scope, recipient, limits())
                        .expire_due(SyncTime::from_unix_millis(50))
                        .await
                        .map_err(storage_error)?,
                    Some(SyncTime::from_unix_millis(75))
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(custody_rows(&reopened).await, committed);
}

#[tokio::test]
async fn stored_recipient_refuses_corrupt_identity_and_sequence_state_without_repair() {
    for damage in [
        "UPDATE _fleet_sync_mail_state SET record='{}'",
        "UPDATE _fleet_sync_mail_state SET record=json_set(record,'$.peer','not-a-peer')",
        "UPDATE _fleet_sync_mail_state SET record=replace(record,'\"items\":0','\"items\":9223372036854775808')",
        "UPDATE _fleet_sync_mail_state SET record=replace(record,'\"controls\":0','\"controls\":9223372036854775808')",
        "UPDATE _fleet_sync_mail_state SET record=json_set(record,'$.receivedItems',1)",
        "UPDATE _fleet_sync_mail_state SET record=json_set(record,'$.receivedControls',1)",
    ] {
        let fixture = Fixture::new().await;
        fixture
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .open()
                        .await
                        .map_err(storage_error)?;
                    sqlx_core::raw_sql::raw_sql(damage)
                        .execute(scope.executor())
                        .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        let before = custody_rows(&fixture.store).await;
        fixture
            .store
            .transaction(|scope| {
                Box::pin(async move {
                    assert!(matches!(
                        Mailbox::<_, TestTypes>::stored_recipient(scope).await,
                        Err(DeliveryError::Corrupt { .. })
                    ));
                    assert!(matches!(
                        Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                            .open()
                            .await,
                        Err(DeliveryError::Corrupt { .. })
                    ));
                    Ok(())
                })
            })
            .await
            .unwrap();
        assert_eq!(custody_rows(&fixture.store).await, before);
    }
}
