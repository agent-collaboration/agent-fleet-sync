use super::journal_metadata::{Result, connect};
use durable_steps::turso_store::{journal_migrations, migrate_journal};
use sqlx_core::{connection::Connection, query::query, row::Row};
use sqlx_turso::Turso;

#[tokio::test]
async fn exported_corpus_is_exactly_what_migrate_journal_applies() -> Result {
    let directory = tempfile::tempdir()?;
    let mut connection = connect(&directory.path().join("corpus.sqlite")).await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    migrate_journal(&mut transaction).await?;
    transaction.commit().await?;

    let applied = query::<Turso>("SELECT version FROM _fleet_sync_migrations ORDER BY version")
        .fetch_all(&mut connection)
        .await?
        .iter()
        .map(|row| row.try_get::<i64, _>("version"))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let exported = journal_migrations()
        .map(|migration| migration.version)
        .collect::<Vec<_>>();
    assert!(!exported.is_empty());
    assert_eq!(applied, exported);
    connection.close().await?;
    Ok(())
}
