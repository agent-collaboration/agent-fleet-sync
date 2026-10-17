use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
struct UpgradingSchema {
    index: usize,
    upgrade: Arc<AtomicBool>,
}
impl ReplicatedSchema for UpgradingSchema {
    fn migrations(&self) -> &'static [Migration] {
        static UPGRADES: LazyLock<Vec<Vec<Migration>>> = LazyLock::new(|| {
            let mut offered = SCHEMAS
                .iter()
                .map(|sql| {
                    vec![
                        BUNDLES
                            .first()
                            .and_then(|bundle| bundle.first())
                            .expect("base migration fixture")
                            .clone(),
                        Migration::new(
                            2,
                            "replace key schema".into(),
                            MigrationType::Simple,
                            sqlx_core::sql_str::AssertSqlSafe(format!("DROP TABLE records; {sql}"))
                                .into_sql_str(),
                            false,
                        ),
                    ]
                })
                .collect::<Vec<_>>();
            for sql in [
                "UPDATE records SET id=id",
                "INSERT INTO records VALUES('019eef00-0000-7000-8000-000000000001','new') ON CONFLICT(id) DO UPDATE SET id=excluded.id",
                "INSERT INTO records VALUES('invalid','bad backfill')",
            ] {
                offered.push(vec![
                    BUNDLES
                        .first()
                        .and_then(|bundle| bundle.first())
                        .expect("base migration fixture")
                        .clone(),
                    Migration::new(
                        2,
                        "refused key mutation".into(),
                        MigrationType::Simple,
                        sql.into_sql_str(),
                        false,
                    ),
                ]);
            }
            offered
        });
        if self.upgrade.load(Ordering::SeqCst) {
            UPGRADES
                .get(self.index)
                .map(Vec::as_slice)
                .unwrap_or_default()
        } else {
            BUNDLES.first().map(Vec::as_slice).unwrap_or_default()
        }
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        &TABLES
    }
}

#[tokio::test]
async fn migrations_refuse_key_assignments_and_non_uuid_backfills_before_committing() -> TestResult
{
    for offset in 0..3 {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("writer.db");
        let flag = Arc::new(AtomicBool::new(false));
        let writer = ReplicatedDatabase::create(
            &path,
            UpgradingSchema {
                index: SCHEMAS.len() + offset,
                upgrade: flag.clone(),
            },
            peer()?,
            policy(),
            WriterOptions {
                allow_provisional_writes: true,
            },
        )
        .await?;
        let mut transaction = writer.begin_write().await?;
        query::<Turso>("INSERT INTO records VALUES(?,'retained')")
            .bind(KEY)
            .execute(&mut transaction)
            .await?;
        transaction.commit().await?;
        let database = writer.database();
        writer.close().await;
        let writer = ReplicatedDatabase::open(
            &path,
            database,
            UpgradingSchema {
                index: SCHEMAS.len() + offset,
                upgrade: flag.clone(),
            },
            LeaseHolder::provisional(database, peer()?, policy()),
        )
        .await?;
        let before = tokio::fs::read(&path).await?;
        flag.store(true, Ordering::SeqCst);
        let error = writer
            .migrate()
            .await
            .err()
            .ok_or("key mutation migration admitted")?;
        if offset < 2 {
            assert!(
                matches!(
                    error,
                    OpenError::WriteRefused {
                        refusal: WriteRefusal::KeyChange { .. }
                    }
                ),
                "{error}"
            );
        } else {
            assert!(
                matches!(
                    error,
                    OpenError::WriteRefused {
                        refusal: WriteRefusal::InvalidKey { .. }
                    }
                ),
                "{error}"
            );
        }
        assert_eq!(
            query::<Turso>("SELECT value FROM records")
                .fetch_one(writer.reader())
                .await?
                .try_get::<String, _>("value")?,
            "retained"
        );
        assert_eq!(
            query::<Turso>("SELECT version FROM _sqlx_migrations")
                .fetch_all(writer.reader())
                .await?
                .len(),
            1
        );
        writer.close().await;
        assert_eq!(tokio::fs::read(&path).await?, before);
    }
    Ok(())
}
#[tokio::test]
async fn incompatible_key_migrations_leave_rows_history_capture_and_file_unchanged() -> TestResult {
    for index in 1..6 {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("writer.db");
        let flag = Arc::new(AtomicBool::new(false));
        let schema = UpgradingSchema {
            index,
            upgrade: flag.clone(),
        };
        let writer = ReplicatedDatabase::create(
            &path,
            schema,
            peer()?,
            policy(),
            WriterOptions {
                allow_provisional_writes: true,
            },
        )
        .await?;
        let database = writer.database();
        let mut transaction = writer.begin_write().await?;
        query::<Turso>("INSERT INTO records VALUES(?,'retained')")
            .bind(KEY)
            .execute(&mut transaction)
            .await?;
        transaction.commit().await?;
        writer.close().await;
        let writer = ReplicatedDatabase::open(
            &path,
            database,
            UpgradingSchema {
                index,
                upgrade: flag.clone(),
            },
            LeaseHolder::provisional(database, peer()?, policy()),
        )
        .await?;
        let before = tokio::fs::read(&path).await?;
        flag.store(true, Ordering::SeqCst);
        assert!(matches!(
            writer.migrate().await,
            Err(OpenError::SchemaUnsupported {
                form: SchemaForm::KeyNotUuidV7 { .. }
            })
        ));
        assert_eq!(
            query::<Turso>("SELECT value FROM records")
                .fetch_one(writer.reader())
                .await?
                .try_get::<String, _>("value")?,
            "retained"
        );
        assert_eq!(
            query::<Turso>("SELECT version FROM _sqlx_migrations")
                .fetch_all(writer.reader())
                .await?
                .len(),
            1
        );
        assert!(
            query::<Turso>("SELECT version FROM _fleet_sync_schema_steps WHERE version=2")
                .fetch_all(writer.reader())
                .await?
                .is_empty()
        );
        writer.close().await;
        assert_eq!(tokio::fs::read(&path).await?, before);
    }
    Ok(())
}
