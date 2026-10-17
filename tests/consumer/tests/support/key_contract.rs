use super::copy_rows;
use fleet_sync_lease::LeasePolicy;
use fleet_sync_protocol::{PeerId, TableName};
use fleet_sync_test_consumer::{SampleSchema, TestResult, insert_sample};
use fleet_sync_transport_websocket::WebSocketConnection;
use fleet_sync_writer::{
    Migration, OpenError, ReplicatedDatabase, ReplicatedSchema, SchemaForm, WriteRefusal,
    WriterOptions,
};
use sqlx_core::{migrate::MigrationType, query::query, sql_str::SqlSafeStr};
use sqlx_turso::Turso;
use std::{path::Path, sync::LazyLock, time::Duration};

struct RejectedKeySchema {
    migrations: &'static [Migration],
    tables: &'static [TableName],
}
impl ReplicatedSchema for RejectedKeySchema {
    fn migrations(&self) -> &'static [Migration] {
        self.migrations
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        self.tables
    }
}

pub async fn assert_key_refusals(
    writer: &ReplicatedDatabase<SampleSchema>,
    socket: &mut WebSocketConnection,
    directory: &Path,
    peer: PeerId,
) -> TestResult {
    static BUNDLES: LazyLock<Vec<Vec<Migration>>> = LazyLock::new(|| {
        [
        "CREATE TABLE invalid_keys(id INTEGER PRIMARY KEY,payload TEXT)",
        "CREATE TABLE invalid_keys(source_id TEXT NOT NULL,sequence INTEGER NOT NULL,PRIMARY KEY(source_id,sequence))",
    ].into_iter().map(|sql|vec![Migration::new(1,"rejected identity".into(),MigrationType::Simple,sql.into_sql_str(),false)]).collect()
    });
    static TABLES: LazyLock<Result<Vec<TableName>, fleet_sync_protocol::TextValueError>> =
        LazyLock::new(|| Ok(vec![TableName::new("invalid_keys")?]));
    for (index, migrations) in BUNDLES.iter().enumerate() {
        let result = ReplicatedDatabase::create(
            &directory.join(format!("invalid-key-schema-{index}.db")),
            RejectedKeySchema {
                migrations,
                tables: TABLES.as_ref().map_err(Clone::clone)?,
            },
            peer,
            LeasePolicy {
                lease_duration: Duration::from_secs(600),
                renew_interval: Duration::from_secs(30),
            },
            WriterOptions::default(),
        )
        .await;
        let error = match result {
            Ok(database) => {
                database.close().await;
                return Err("non-UUID key schema admitted".into());
            }
            Err(error) => error,
        };
        assert!(
            matches!(
                error,
                OpenError::SchemaUnsupported {
                    form: SchemaForm::KeyNotUuidV7 { .. }
                }
            ),
            "{error}"
        );
    }
    let before = copy_rows(socket, writer.database()).await?;
    for key in ["not-a-uuid", "019eef00-0000-4000-8000-000000000001"] {
        let mut write = writer.begin_write().await?;
        insert_sample(&mut write, key.into(), "refused".into(), 42, vec![]).await?;
        assert!(matches!(
            write.commit().await,
            Err(WriteRefusal::InvalidKey { .. })
        ));
    }
    for sql in [
        "UPDATE samples SET id=?",
        "INSERT INTO samples VALUES(?,'refused',42,x'00') ON CONFLICT(id) DO UPDATE SET id=excluded.id",
    ] {
        let mut write = writer.begin_write().await?;
        let error = query::<Turso>(sql)
            .bind("019eef00-0000-7000-8000-000000000099")
            .execute(&mut write)
            .await
            .err()
            .ok_or("immutable key assignment admitted")?;
        assert!(matches!(
            WriteRefusal::from_sqlx(error),
            WriteRefusal::KeyChange { .. }
        ));
        write.rollback().await?;
    }
    assert!(
        query::<Turso>("SELECT id FROM samples")
            .fetch_all(writer.reader())
            .await?
            .is_empty()
    );
    assert_eq!(copy_rows(socket, writer.database()).await?, before);
    println!(
        "OBS integer/mixed key schemas refused before replication; non-v7 keys roll back; UPDATE/upsert key assignments refused; native writer and WebSocket copy remain empty"
    );
    Ok(())
}
