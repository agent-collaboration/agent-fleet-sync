#![allow(clippy::panic_in_result_fn)]
use crate::{turso_rule_fixture::*, *};
use fleet_sync_protocol::*;
use sqlx_core::{connection::Connection, query::query};
use sqlx_turso::Turso;

#[tokio::test]
async fn invalid_uuid_piece_keys_are_corrupt_and_do_not_advance_rows_receipts_or_hook() -> TestResult
{
    let mut connection = connection().await?;
    let before = counts(&mut connection).await?;
    for value in [
        SqlValue::Text("not-a-uuid".into()),
        SqlValue::Text("019eef00-0000-4000-8000-000000000001".into()),
        SqlValue::Text("019EEF00-0000-7000-8000-000000000001".into()),
        SqlValue::Integer(7),
    ] {
        let incoming = Piece::<()>::new(PieceContents {
            database: database()?,
            epoch: Epoch::new(1)?,
            continues_from: position(0)?,
            through: position(1)?,
            transactions: vec![ReplicatedTransaction::Rows {
                commit_id: position(1)?.sequence,
                rows: vec![RowChange::upsert(
                    "records",
                    vec![value.clone()],
                    vec![value, SqlValue::Text("invalid".into())],
                )?],
            }],
            part: None,
            annotation: None,
        })?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let result = apply_piece(
            &mut TursoScope(&mut transaction),
            &incoming,
            &writer()?,
            &Hook { refuse: false },
            SyncTime::from_unix_millis(1),
        )
        .await;
        assert!(
            matches!(
                result,
                Err(ApplyFailure::Storage(StoreError::Corrupt { .. }))
            ),
            "{result:?}"
        );
        transaction.rollback().await?;
        assert_eq!(counts(&mut connection).await?, before);
        assert_eq!(
            ready_status(copy_status(&mut TursoScope(&mut connection)).await?)?.acked,
            position(0)?
        );
    }
    Ok(())
}

#[tokio::test]
async fn non_uuid_key_schema_steps_are_refused_before_creating_any_table() -> TestResult {
    let mut connection = connection().await?;
    for sql in [
        "CREATE TABLE invalid_key(id INTEGER PRIMARY KEY)",
        "CREATE TABLE invalid_key(id TEXT NOT NULL,other INTEGER NOT NULL,PRIMARY KEY(id,other))",
        "CREATE TABLE invalid_key(id TEXT PRIMARY KEY)",
    ] {
        let incoming = Piece::<()>::new(PieceContents {
            database: database()?,
            epoch: Epoch::new(1)?,
            continues_from: position(0)?,
            through: position(1)?,
            transactions: vec![ReplicatedTransaction::SchemaStep {
                commit_id: position(1)?.sequence,
                migration_version: 2,
                statements: vec![sql.into()],
            }],
            part: None,
            annotation: None,
        })?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        assert!(matches!(
            apply_piece(
                &mut TursoScope(&mut transaction),
                &incoming,
                &writer()?,
                &Hook { refuse: false },
                SyncTime::from_unix_millis(1)
            )
            .await,
            Err(ApplyFailure::Refused(
                PieceRefusal::SchemaUnsupported { .. }
            ))
        ));
        transaction.rollback().await?;
        assert!(
            query::<Turso>("SELECT name FROM sqlite_schema WHERE name='invalid_key'")
                .fetch_optional(&mut connection)
                .await?
                .is_none()
        );
    }
    Ok(())
}

#[tokio::test]
async fn non_uuid_seed_schema_is_schema_unsupported_and_never_publishes() -> TestResult {
    let mut connection = crate::turso_seed_tests::setup(None).await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let database = crate::turso_seed_tests::database()?;
    let seed = seed_begin(
        &mut TursoScope(&mut transaction),
        database,
        uuid::Uuid::from_u128(85),
        &SeedAuthority::Admin,
    )
    .await?;
    let (mut page, mut done) = crate::turso_seed_tests::snapshot(seed)?;
    let sql = "CREATE TABLE records(id INTEGER PRIMARY KEY,body TEXT)";
    let schema = SchemaIdentity::new(
        done.schema.version,
        schema_fingerprint(&[SchemaEntry {
            kind: SchemaObjectKind::Table,
            name: "records".into(),
            sql: sql.into(),
        }])?,
    )?;
    page.contents.schema_steps = vec![sql.into()];
    page.contents.schema = schema.clone();
    page.contents.rows.clear();
    page.digest = PageDigest::of(&page.contents)?;
    done.schema = schema;
    done.table_digests = vec![(
        TableName::new("records")?,
        TableDigestBuilder::new().finish(),
    )];
    assert_eq!(
        seed_page(
            &mut TursoScope(&mut transaction),
            &page,
            &SeedAuthority::Admin
        )
        .await?,
        SeedPageAnswer::Staged
    );
    assert!(matches!(
        seed_complete(
            &mut TursoScope(&mut transaction),
            &done,
            &Hook { refuse: false },
            &SeedAuthority::Admin
        )
        .await,
        Err(ReceiverError::SchemaUnsupported {
            form: SchemaStatementForm::CreateTable
        })
    ));
    transaction.rollback().await?;
    assert!(
        query::<Turso>("SELECT name FROM sqlite_schema WHERE name='records'")
            .fetch_optional(&mut connection)
            .await?
            .is_none()
    );
    assert_eq!(
        copy_status(&mut TursoScope(&mut connection)).await?,
        CopyStatusAnswer::NotInitialized
    );
    Ok(())
}

#[tokio::test]
async fn invalid_uuid_seed_row_and_schema_data_step_are_corrupt_without_advancing() -> TestResult {
    let mut connection = crate::turso_seed_tests::setup(None).await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let seed = seed_begin(
        &mut TursoScope(&mut transaction),
        crate::turso_seed_tests::database()?,
        uuid::Uuid::from_u128(86),
        &SeedAuthority::Admin,
    )
    .await?;
    let (mut page, done) = crate::turso_seed_tests::snapshot(seed)?;
    *page
        .contents
        .rows
        .first_mut()
        .and_then(|row| row.first_mut())
        .ok_or("missing first key")? = SqlValue::Text("invalid".into());
    page.digest = PageDigest::of(&page.contents)?;
    seed_page(
        &mut TursoScope(&mut transaction),
        &page,
        &SeedAuthority::Admin,
    )
    .await?;
    assert!(matches!(
        seed_complete(
            &mut TursoScope(&mut transaction),
            &done,
            &Hook { refuse: false },
            &SeedAuthority::Admin
        )
        .await,
        Err(ReceiverError::Storage(StoreError::Corrupt { .. }))
    ));
    transaction.rollback().await?;
    let mut connection = crate::turso_rule_fixture::connection().await?;
    let before = counts(&mut connection).await?;
    let incoming = Piece::<()>::new(PieceContents {
        database: database()?,
        epoch: Epoch::new(1)?,
        continues_from: position(0)?,
        through: position(1)?,
        transactions: vec![ReplicatedTransaction::SchemaStep {
            commit_id: position(1)?.sequence,
            migration_version: 2,
            statements: vec!["INSERT INTO records VALUES('invalid','migration data')".into()],
        }],
        part: None,
        annotation: None,
    })?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert!(matches!(
        apply_piece(
            &mut TursoScope(&mut transaction),
            &incoming,
            &writer()?,
            &Hook { refuse: false },
            SyncTime::from_unix_millis(1)
        )
        .await,
        Err(ApplyFailure::Storage(StoreError::Corrupt { .. }))
    ));
    transaction.rollback().await?;
    assert_eq!(counts(&mut connection).await?, before);
    Ok(())
}
