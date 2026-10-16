//! Seed generation, staging and atomic publication on the owned native driver.
use crate::*;
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::*;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    row::Row,
};
use sqlx_turso::{Turso, TursoConnectOptions, TursoConnection};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
struct TursoScope<'a>(&'a mut TursoConnection);
impl TransactionScope for TursoScope<'_> {
    type Database = Turso;
    type Executor<'q>
        = &'q mut TursoConnection
    where
        Self: 'q;
    fn executor(&mut self) -> Self::Executor<'_> {
        self.0
    }
}
fn database() -> TestResult<DatabaseId> {
    Ok(DatabaseId::new(
        "019f0000-0000-7000-8000-000000000021".parse()?,
    )?)
}
fn position() -> TestResult<ReplicationPosition> {
    Ok(ReplicationPosition::new(
        Epoch::new(1)?,
        CommitSequence::new(7),
    ))
}
async fn setup(expected_position: Option<ReplicationPosition>) -> TestResult<TursoConnection> {
    let mut connection = TursoConnectOptions::new().connect().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    sqlx_core::raw_sql::raw_sql(include_str!("../migrations/0001_copy_rules.sql"))
        .execute(&mut *transaction)
        .await?;
    create_copy(
        &mut TursoScope(&mut transaction),
        &CreateCopy {
            database: database()?,
            epoch: Epoch::new(1)?,
            holder: None,
            origin: CopyOrigin::AwaitingSeed { expected_position },
        },
    )
    .await?;
    transaction.commit().await?;
    Ok(connection)
}
fn snapshot(seed: SeedId) -> TestResult<(SeedPage, SeedComplete)> {
    let sql = "CREATE TABLE records(id INTEGER PRIMARY KEY, body TEXT)";
    let schema = SchemaIdentity::new(
        3,
        schema_fingerprint(&[SchemaEntry {
            kind: SchemaObjectKind::Table,
            name: "records".into(),
            sql: sql.into(),
        }])?,
    )?;
    let rows = vec![
        vec![SqlValue::Integer(2), SqlValue::Text("two".into())],
        vec![SqlValue::Integer(i64::MAX), SqlValue::Text("max".into())],
    ];
    let mut digest = TableDigestBuilder::new();
    for row in &rows {
        digest.push_row(row)?;
    }
    let contents = SnapshotPage {
        database: database()?,
        position: position()?,
        schema: schema.clone(),
        schema_steps: vec![sql.into()],
        table: "records".into(),
        rows,
        next: None,
    };
    let page = SeedPage {
        seed,
        page: 0,
        digest: PageDigest::of(&contents)?,
        contents,
    };
    let done = SeedComplete {
        database: database()?,
        seed,
        position: position()?,
        pages: 1,
        schema,
        table_digests: vec![(TableName::new("records")?, digest.finish())],
    };
    Ok((page, done))
}

#[tokio::test]
async fn copy_issues_rising_generations_and_repeat_attempt_does_not_replace_newer_staging()
-> TestResult {
    let mut connection = setup(None).await?;
    // Reverse UUID order: only copy-issued generations define freshness.
    let older_attempt: Uuid = "ffffffff-ffff-7fff-8fff-ffffffffffff".parse()?;
    let newer_attempt: Uuid = "00000000-0000-7000-8000-000000000001".parse()?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let first = seed_begin(
        &mut TursoScope(&mut transaction),
        database()?,
        older_attempt,
    )
    .await?;
    assert_eq!(first, SeedId { generation: 1 });
    let (first_page, _) = snapshot(first)?;
    assert_eq!(
        seed_page(&mut TursoScope(&mut transaction), &first_page).await?,
        SeedPageAnswer::Staged
    );
    let second = seed_begin(
        &mut TursoScope(&mut transaction),
        database()?,
        newer_attempt,
    )
    .await?;
    assert_eq!(second, SeedId { generation: 2 });
    let (second_page, done) = snapshot(second)?;
    assert_eq!(
        seed_page(&mut TursoScope(&mut transaction), &second_page).await?,
        SeedPageAnswer::Staged
    );
    assert_eq!(
        seed_begin(
            &mut TursoScope(&mut transaction),
            database()?,
            older_attempt
        )
        .await?,
        first
    );
    assert_eq!(
        seed_page(&mut TursoScope(&mut transaction), &first_page).await?,
        SeedPageAnswer::StaleSeed
    );
    assert_eq!(
        seed_page(&mut TursoScope(&mut transaction), &second_page).await?,
        SeedPageAnswer::AlreadyStaged
    );
    assert_eq!(
        copy_status(&mut TursoScope(&mut transaction)).await.err(),
        Some(ReceiverError::NotInitialized)
    );
    assert_eq!(
        seed_complete(&mut TursoScope(&mut transaction), &done).await?,
        SeedAnswer::Published {
            position: position()?
        }
    );
    assert_eq!(
        seed_complete(&mut TursoScope(&mut transaction), &done).await?,
        SeedAnswer::Published {
            position: position()?
        }
    );
    assert_eq!(
        seed_page(&mut TursoScope(&mut transaction), &second_page).await?,
        SeedPageAnswer::NotSeedable
    );
    let copy = copy_status(&mut TursoScope(&mut transaction)).await?;
    assert_eq!(copy.acked, position()?);
    assert_eq!(copy.holder, None);
    assert_eq!(copy.fence, Epoch::new(1)?);
    transaction.commit().await?;
    let rows = sqlx_core::query::query::<Turso>(
        "SELECT CAST(id AS TEXT) AS id,body FROM records ORDER BY id",
    )
    .fetch_all(&mut connection)
    .await?;
    let values = rows
        .iter()
        .map(|row| {
            Ok((
                row.try_get::<String, _>("id")?,
                row.try_get::<String, _>("body")?,
            ))
        })
        .collect::<Result<Vec<_>, sqlx_core::Error>>()?;
    assert_eq!(
        values,
        vec![
            ("2".into(), "two".into()),
            ("9223372036854775807".into(), "max".into())
        ]
    );
    Ok(())
}

#[tokio::test]
async fn digest_refusal_rolls_back_created_schema_and_preserves_staging_for_correct_resend()
-> TestResult {
    let mut connection = setup(None).await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let seed = seed_begin(
        &mut TursoScope(&mut transaction),
        database()?,
        "019f0000-0000-7000-8000-000000000022".parse()?,
    )
    .await?;
    let (page, done) = snapshot(seed)?;
    assert_eq!(
        seed_page(&mut TursoScope(&mut transaction), &page).await?,
        SeedPageAnswer::Staged
    );
    transaction.commit().await?;
    let mut bad = done.clone();
    bad.table_digests = vec![(
        TableName::new("records")?,
        TableDigest::new("0".repeat(64))?,
    )];
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert_eq!(
        seed_complete(&mut TursoScope(&mut transaction), &bad)
            .await
            .err(),
        Some(ReceiverError::SeedRefused {
            answer: SeedAnswer::DigestMismatch {
                table: TableName::new("records")?
            }
        })
    );
    transaction.rollback().await?;
    let row = sqlx_core::query::query::<Turso>(
        "SELECT CAST(count(*) AS TEXT) AS tables FROM sqlite_schema WHERE name='records'",
    )
    .fetch_one(&mut connection)
    .await?;
    assert_eq!(row.try_get::<String, _>("tables")?, "0");
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert_eq!(
        copy_status(&mut TursoScope(&mut transaction)).await.err(),
        Some(ReceiverError::NotInitialized)
    );
    assert_eq!(
        seed_page(&mut TursoScope(&mut transaction), &page).await?,
        SeedPageAnswer::AlreadyStaged
    );
    assert_eq!(
        seed_complete(&mut TursoScope(&mut transaction), &done).await?,
        SeedAnswer::Published {
            position: position()?
        }
    );
    transaction.commit().await?;
    Ok(())
}

#[tokio::test]
async fn missing_pages_changed_page_and_expected_position_are_refused_without_publishing()
-> TestResult {
    let expected = ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(8));
    let mut connection = setup(Some(expected)).await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let seed = seed_begin(
        &mut TursoScope(&mut transaction),
        database()?,
        "019f0000-0000-7000-8000-000000000023".parse()?,
    )
    .await?;
    let (page, mut done) = snapshot(seed)?;
    assert_eq!(
        seed_complete(&mut TursoScope(&mut transaction), &done).await?,
        SeedAnswer::PositionMismatch {
            expected,
            offered: position()?
        }
    );
    done.position = expected;
    assert_eq!(
        seed_complete(&mut TursoScope(&mut transaction), &done).await?,
        SeedAnswer::Incomplete { missing: vec![0] }
    );
    assert_eq!(
        seed_page(&mut TursoScope(&mut transaction), &page).await?,
        SeedPageAnswer::Staged
    );
    let mut conflict = page.clone();
    conflict.contents.rows.clear();
    conflict.digest = PageDigest::of(&conflict.contents)?;
    assert_eq!(
        seed_page(&mut TursoScope(&mut transaction), &conflict).await?,
        SeedPageAnswer::SeedConflict
    );
    transaction.commit().await?;
    Ok(())
}
