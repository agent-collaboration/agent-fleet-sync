#![allow(clippy::panic_in_result_fn)]
#![cfg(feature = "native")]
#[path = "support/checked_test_queries.rs"]
mod checked;

use durable_steps::storage::TransactionScope;
use fleet_sync_query_gen::{generate_manifest, verify_manifest};
use sqlx_turso::{
    Turso, TursoConnectOptions, TursoConnection,
    sqlx::{ConnectOptions, Connection},
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
struct BorrowedScope<'a>(&'a mut TursoConnection);
impl TransactionScope for BorrowedScope<'_> {
    type Database = Turso;
    type Executor<'query>
        = &'query mut TursoConnection
    where
        Self: 'query;
    fn executor(&mut self) -> Self::Executor<'_> {
        self.0
    }
}

#[tokio::test]
async fn generated_calls_compile_and_preserve_values_in_the_callers_transaction() -> TestResult {
    let manifest =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("checked-test-queries.json");
    generate_manifest(&manifest, true).await?;
    verify_manifest(&manifest)?;
    let mut connection = TursoConnectOptions::new().connect().await?;
    let migrations = sqlx_core::migrate::Migrator::new(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/corpus/migrations")
            .as_path(),
    )
    .await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    migrations
        .run_direct(None, &mut *transaction, false)
        .await?;
    transaction.commit().await?;

    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let mut scope = BorrowedScope(&mut transaction);
    checked::insert_example(&mut scope, "exact".into(), i64::MAX, 1.25, vec![0, 255]).await?;
    let inside = checked::read_example(&mut scope, "exact".into())
        .await?
        .ok_or("missing uncommitted record")?;
    assert_eq!(inside.record_id.as_deref(), Some("exact"));
    assert_eq!(inside.amount, Some(i64::MAX));
    assert_eq!(inside.ratio, Some(1.25));
    assert_eq!(inside.payload, Some(vec![0, 255]));
    let exact = checked::read_exact_integer(&mut scope, "exact".into())
        .await?
        .ok_or("exact integer projection missing")?;
    assert_eq!(exact.amount_class.as_deref(), Some("integer"));
    assert_eq!(exact.amount_text.as_deref(), Some("9223372036854775807"));
    transaction.rollback().await?;
    assert!(
        checked::read_examples(&mut BorrowedScope(&mut connection))
            .await?
            .is_empty()
    );

    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    checked::insert_example(
        &mut BorrowedScope(&mut transaction),
        "committed".into(),
        i64::MIN,
        -1.5,
        vec![255, 0],
    )
    .await?;
    transaction.commit().await?;
    let committed = checked::read_example(&mut BorrowedScope(&mut connection), "committed".into())
        .await?
        .ok_or("committed record absent")?;
    assert_eq!(committed.amount, Some(i64::MIN));
    Ok(())
}
