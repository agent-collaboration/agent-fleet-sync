use super::support::*;
use durable_steps::{AtomicStore, storage::TransactionScope};
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;

#[tokio::test]
async fn portable_sql_metadata_mints_distinct_uuidv7_keys_and_preserves_keys_on_upsert() {
    let fixture = Fixture::new().await;
    fixture.store.transaction(|scope|Box::pin(async move {
        query::<Turso>(include_str!("../support/sql/row_metadata_probe.sql")).execute(scope.executor()).await?;
        query::<Turso>("INSERT INTO test_row_metadata(natural_key) VALUES('first'),('second')").execute(scope.executor()).await?;
        let rows=query::<Turso>("SELECT id,created_at,updated_at FROM test_row_metadata ORDER BY natural_key").fetch_all(scope.executor()).await?;
        let first=rows.first().unwrap().try_get::<String,_>("id")?;
        let second=rows.last().unwrap().try_get::<String,_>("id")?;
        assert_ne!(first,second);
        assert!(fleet_sync_protocol::ItemId::try_from(first.clone()).is_ok());
        assert!(fleet_sync_protocol::ItemId::try_from(second).is_ok());
        let created=rows.first().unwrap().try_get::<i64,_>("created_at")?;
        assert!(created>0);
        query::<Turso>("INSERT INTO test_row_metadata(natural_key) VALUES('first') ON CONFLICT(natural_key) DO UPDATE SET updated_at=excluded.updated_at").execute(scope.executor()).await?;
        let row=query::<Turso>("SELECT id,created_at,updated_at FROM test_row_metadata WHERE natural_key='first'").fetch_one(scope.executor()).await?;
        assert_eq!(row.try_get::<String,_>("id")?,first);
        assert_eq!(row.try_get::<i64,_>("created_at")?,created);
        assert!(row.try_get::<i64,_>("updated_at")?>=created);
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn surviving_delivery_tables_have_uuid_keys_and_created_updated_columns() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                for (statement, key) in [
                    ("PRAGMA table_info(_fleet_sync_outbox_items)", "item_id"),
                    ("PRAGMA table_info(_fleet_sync_inbox_items)", "item_id"),
                    ("PRAGMA table_info(_fleet_sync_status_state)", "id"),
                    ("PRAGMA table_info(_fleet_sync_status_events)", "id"),
                    ("PRAGMA table_info(_fleet_sync_status_owed)", "id"),
                    ("PRAGMA table_info(_fleet_sync_reports)", "item_id"),
                    (
                        "PRAGMA table_info(_fleet_sync_outbox_cancel_links)",
                        "target",
                    ),
                    (
                        "PRAGMA table_info(_fleet_sync_inbox_cancel_links)",
                        "target",
                    ),
                    ("PRAGMA table_info(_fleet_sync_outbox_status)", "id"),
                ] {
                    let rows = query::<Turso>(statement)
                        .fetch_all(scope.executor())
                        .await?;
                    let mut primary = Vec::new();
                    let mut columns = Vec::new();
                    for row in rows {
                        let name: String = row.try_get("name")?;
                        if row.try_get::<i64, _>("pk")? > 0 {
                            primary.push(name.clone());
                        }
                        columns.push(name);
                    }
                    assert_eq!(primary, vec![key], "{statement}");
                    for required in ["created_at", "updated_at"] {
                        assert!(
                            columns.iter().any(|name| name == required),
                            "{statement} missing {required}"
                        );
                    }
                    assert!(
                        !columns.iter().any(|name| name == "record"),
                        "{statement} retains owned state JSON"
                    );
                }
                Ok(())
            })
        })
        .await
        .unwrap();
}
