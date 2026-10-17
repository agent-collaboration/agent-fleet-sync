#![allow(clippy::panic_in_result_fn)]
use crate::turso_rule_fixture::{Hook, TursoScope, connection, counts, database, position, writer};
use crate::*;
use fleet_sync_protocol::*;
use sqlx_core::{connection::Connection, query::query};
use sqlx_turso::Turso;
#[tokio::test]
async fn autoincrement_schema_piece_is_refused_without_table_hook_or_watermark_changes()
-> Result<(), Box<dyn std::error::Error>> {
    let mut connection = connection().await?;
    let before = counts(&mut connection).await?;
    let status = copy_status(&mut TursoScope(&mut connection)).await?;
    let piece = Piece::<()>::new(PieceContents {
        database: database()?,
        epoch: Epoch::new(1)?,
        continues_from: position(0)?,
        through: position(1)?,
        transactions: vec![ReplicatedTransaction::SchemaStep {
            commit_id: position(1)?.sequence,
            migration_version: 2,
            statements: vec![
                "CREATE TABLE auto_records(id INTEGER PRIMARY KEY AUTOINCREMENT,payload TEXT)"
                    .into(),
            ],
        }],
        part: None,
        annotation: None,
    })?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert!(matches!(
        apply_piece(
            &mut TursoScope(&mut transaction),
            &piece,
            &writer()?,
            &Hook { refuse: false },
            SyncTime::from_unix_millis(0)
        )
        .await,
        Err(ApplyFailure::Refused(PieceRefusal::SchemaUnsupported {
            form: SchemaStatementForm::CreateTable
        }))
    ));
    transaction.rollback().await?;
    assert_eq!(copy_status(&mut TursoScope(&mut connection)).await?, status);
    assert_eq!(counts(&mut connection).await?, before);
    assert!(
        query::<Turso>("SELECT name FROM sqlite_schema WHERE name='auto_records'")
            .fetch_optional(&mut connection)
            .await?
            .is_none()
    );
    Ok(())
}
