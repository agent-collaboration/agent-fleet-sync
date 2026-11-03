use super::support::*;
use durable_steps::{AtomicStore, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};

#[tokio::test]
async fn custody_schema_exposes_owned_state_without_record_json() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                for (table, statement, admission_time) in [
                    (
                        "_fleet_sync_outbox_items",
                        "PRAGMA table_info(_fleet_sync_outbox_items)",
                        "admitted_at",
                    ),
                    (
                        "_fleet_sync_inbox_items",
                        "PRAGMA table_info(_fleet_sync_inbox_items)",
                        "accepted_at",
                    ),
                ] {
                    let rows = query::<sqlx_turso::Turso>(statement)
                        .fetch_all(scope.executor())
                        .await
                        .map_err(StoreError::backend)?;
                    let columns = rows
                        .iter()
                        .map(|row| row.try_get::<String, _>("name"))
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(StoreError::backend)?;
                    assert!(
                        !columns.iter().any(|column| column == "record"),
                        "{table}: record JSON remains"
                    );
                    for required in [
                        "digest",
                        "created_at",
                        admission_time,
                        "valid_until",
                        "settlement_bytes",
                    ] {
                        assert!(
                            columns.iter().any(|column| column == required),
                            "{table}: missing {required}"
                        );
                    }
                }
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn released_outbox_retains_deadline_identity_reserve_and_timestamps_after_reopen() {
    let Fixture {
        store,
        host,
        directory,
    } = Fixture::new().await;
    let reserve = limits().settlement_allowance();
    store
        .transaction(move |scope| {
            Box::pin(async move {
                let original = DeliveryItem::new(
                    item_id(71),
                    "sender".into(),
                    Recipient {
                        peer: peer(2),
                        address: "inbox".into(),
                    },
                    Some(SyncTime::from_unix_millis(1_800_000_000_000)),
                    "retained".into(),
                )
                .unwrap();
                let digest = original.digest().clone();
                let mut outbox = Outbox::<_, TestTypes>::new(scope, limits());
                outbox
                    .enqueue(
                        original,
                        peer(9),
                        peer(1),
                        SyncTime::from_unix_millis(1_730_000_000_011),
                    )
                    .await
                    .map_err(storage_error)?;
                outbox
                    .mark_submitted(
                        &SubmitInput {
                            item: item_id(71),
                            digest,
                            relay: peer(9),
                            mailbox: peer(2),
                        },
                        SubmitResult::Accepted {
                            seq: DeliverySeq::new(17),
                            status: DeliveryStatus::Sent {
                                seq: DeliverySeq::new(17),
                            },
                            version: StatusVersion::new(1),
                            cancel: None,
                        },
                        SyncTime::from_unix_millis(1_730_000_000_023),
                    )
                    .await
                    .map_err(storage_error)?;
                let row = query::<sqlx_turso::Turso>(
                    "SELECT payload FROM _fleet_sync_outbox_items WHERE item_id=?1",
                )
                .bind(item_id(71).as_uuid().to_string())
                .fetch_one(scope.executor())
                .await?;
                assert!(row.try_get::<Option<String>, _>("payload")?.is_none());
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
        let row = query::<sqlx_turso::Turso>("SELECT * FROM _fleet_sync_outbox_items WHERE item_id=?1")
            .bind(item_id(71).as_uuid().to_string()).fetch_one(scope.executor()).await?;
        assert!(row.try_get::<Option<String>, _>("payload")?.is_none());
        assert_eq!(row.try_get::<String, _>("submitter_machine_id")?, peer(1).as_uuid().to_string());
        assert_eq!(row.try_get::<String, _>("recipient_machine_id")?, peer(2).as_uuid().to_string());
        assert_eq!(row.try_get::<i64, _>("created_at")?, 1_717_986_918_400);
        assert_eq!(row.try_get::<i64, _>("admitted_at")?, 1_730_000_000_011);
        assert_eq!(row.try_get::<i64, _>("handed_off_at")?, 1_730_000_000_023);
        assert_eq!(row.try_get::<i64, _>("valid_until")?, 1_800_000_000_000);
        assert_eq!(row.try_get::<i64, _>("settlement_bytes")?, i64::from(reserve));
        assert_eq!(row.try_get::<i64, _>("submit_attempted")?, 1);
        assert_eq!(row.try_get::<i64, _>("capacity_slot")?, 1);
        assert!(matches!(Outbox::<_, TestTypes>::new(scope, limits()).status(item_id(71)).await.map_err(storage_error)?.state,
            OutboxStatusState::Known { status: DeliveryStatus::Sent { seq }, version, .. } if seq.value()==17 && version.value()==1));
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn typed_upgrade_preserves_existing_custody_and_marks_unknown_historical_times() {
    let fixture = Fixture::new().await;
    let store = TestStore::open_and_migrate(
        fixture.directory.path().join("legacy.db"),
        fixture.host.clone(),
    )
    .await
    .unwrap();
    store.transaction(|scope| Box::pin(async move {
        let mut migrations = sqlx_core::migrate::Migrator::with_migrations(
            DELIVERY_MIGRATOR.iter().filter(|migration| migration.version < 4).cloned().collect::<Vec<_>>());
        migrations.dangerous_set_table_name("_fleet_sync_delivery_migrations");
        migrations.run_direct(None, scope.executor(), false).await.map_err(StoreError::backend)?;
        let payload = item(100, "migrated content");
        let digest = payload.digest().clone();
        let payload = serde_json::to_string(&payload).unwrap();
        let reserve = limits().settlement_allowance();
        let record = serde_json::json!({"kind":"item","value":{
            "id":item_id(100), "digest":digest, "relay":peer(9), "recipient":peer(2),
            "validUntil":null, "status":{"kind":"queued"}, "version":0,
            "submitAttempted":false,"cancel":null,"cancelTarget":null,
            "settlementBytes":reserve,"capacitySlot":true
        }}).to_string();
        query::<sqlx_turso::Turso>("INSERT INTO _fleet_sync_outbox_items(item_id,capacity_slot,live,payload_bytes,settlement_bytes,record,payload) VALUES(?1,1,1,?2,?3,?4,?5)")
            .bind(item_id(100).as_uuid().to_string()).bind(payload.len() as i64).bind(i64::from(reserve))
            .bind(record).bind(payload).execute(scope.executor()).await?;
        let mut migrations = sqlx_core::migrate::Migrator::with_migrations(DELIVERY_MIGRATOR.iter().cloned().collect::<Vec<_>>());
        migrations.dangerous_set_table_name("_fleet_sync_delivery_migrations");
        migrations.run_direct(None, scope.executor(), false).await.map_err(StoreError::backend)?;
        let row = query::<sqlx_turso::Turso>("SELECT created_at,admitted_at,handed_off_at,last_report_seq FROM _fleet_sync_outbox_items WHERE item_id=?1")
            .bind(item_id(100).as_uuid().to_string()).fetch_one(scope.executor()).await?;
        assert_eq!(row.try_get::<i64,_>("created_at")?,1_717_986_918_400);
        assert!(row.try_get::<Option<i64>,_>("admitted_at")?.is_none());
        assert!(row.try_get::<Option<i64>,_>("handed_off_at")?.is_none());
        assert_eq!(row.try_get::<i64,_>("last_report_seq")?,0);
        let resolved = Outbox::<_,TestTypes>::new(scope,limits()).resolve_submit(&DeliveryItemRef { item:item_id(100), digest:item(100,"migrated content").digest().clone() })
            .await.map_err(storage_error)?;
        assert_eq!(serde_json::to_string(&resolved.item).unwrap(),serde_json::to_string(&item(100,"migrated content")).unwrap());
        Ok(())
    })).await.unwrap();
}
