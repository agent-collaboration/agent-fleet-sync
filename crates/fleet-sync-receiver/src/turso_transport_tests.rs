//! The dynamic row boundary against the owned SQLx Turso driver, inside real transactions.
// Result propagates fixture/storage failures; assertions identify contract violations.
#![allow(clippy::panic_in_result_fn)]
use crate::{
    BoundStatement, StoreError,
    row_apply::{TableColumn, plan_row_change},
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::{RowChange, SqlValue};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    row::Row,
};
use sqlx_turso::{Turso, TursoConnectOptions, TursoConnection};

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

async fn connection() -> TestResult<TursoConnection> {
    let mut connection = TursoConnectOptions::new().connect().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    sqlx_core::raw_sql::raw_sql(include_str!("../tests/migrations/0001_consumer_rows.sql"))
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    Ok(connection)
}

#[tokio::test]
async fn integer_real_text_blob_and_null_keep_exact_values_and_storage_classes() -> TestResult {
    // Arrange: literal values from SO4, including both signed endpoints and an integer beyond 2^53.
    let mut connection = connection().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let mut scope = TursoScope(&mut transaction);
    for id in [i64::MIN, 9_007_199_254_740_993, i64::MAX] {
        BoundStatement::new(
            "INSERT INTO exact_values VALUES (?, ?, ?, ?, ?)",
            vec![
                SqlValue::Integer(id),
                SqlValue::Real(1.0),
                SqlValue::Text("quotes '?'; λ".into()),
                SqlValue::Blob(vec![0, 255, 1]),
                SqlValue::Null,
            ],
        )
        .execute(&mut scope)
        .await?;
    }
    // Act: read through the exact-value projection over the same real SQL transaction.
    let rows = BoundStatement::query(
        "SELECT id, real_value, text_value, blob_value, null_value FROM exact_values ORDER BY id",
        Vec::new(),
        ["id", "real_value", "text_value", "blob_value", "null_value"]
            .map(str::to_owned)
            .into(),
    )
    .fetch_rows(&mut scope)
    .await?;
    // Assert: expected values are literals, never derived from the decoder.
    let ids = rows
        .iter()
        .map(|row| row.integer("id"))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(ids, vec![i64::MIN, 9_007_199_254_740_993, i64::MAX]);
    for row in rows {
        assert_eq!(row.value("real_value")?, &SqlValue::Real(1.0));
        assert_eq!(
            row.value("text_value")?,
            &SqlValue::Text("quotes '?'; λ".into())
        );
        assert_eq!(row.value("blob_value")?, &SqlValue::Blob(vec![0, 255, 1]));
        assert_eq!(row.value("null_value")?, &SqlValue::Null);
    }
    transaction.commit().await?;
    let row = sqlx_core::query::query::<Turso>("SELECT typeof(id) AS id_class, typeof(real_value) AS real_class, typeof(blob_value) AS blob_class, typeof(null_value) AS null_class FROM exact_values LIMIT 1")
        .fetch_one(&mut connection).await?;
    assert_eq!(row.try_get::<String, _>("id_class")?, "integer");
    assert_eq!(row.try_get::<String, _>("real_class")?, "real");
    assert_eq!(row.try_get::<String, _>("blob_class")?, "blob");
    assert_eq!(row.try_get::<String, _>("null_class")?, "null");
    Ok(())
}

#[tokio::test]
async fn positive_and_negative_infinity_remain_real_through_row_apply_and_exact_readback()
-> TestResult {
    let mut connection = connection().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let mut scope = TursoScope(&mut transaction);
    let columns = [
        TableColumn {
            name: "id".into(),
            key_order: 1,
        },
        TableColumn {
            name: "real_value".into(),
            key_order: 0,
        },
        TableColumn {
            name: "text_value".into(),
            key_order: 0,
        },
        TableColumn {
            name: "blob_value".into(),
            key_order: 0,
        },
        TableColumn {
            name: "null_value".into(),
            key_order: 0,
        },
    ];
    for (id, value) in [(1, f64::INFINITY), (2, f64::NEG_INFINITY)] {
        let change = RowChange::upsert(
            "real_values",
            vec![crate::turso_rule_fixture::uuid_key(id)],
            vec![
                crate::turso_rule_fixture::uuid_key(id),
                SqlValue::Real(value),
                SqlValue::Null,
                SqlValue::Null,
                SqlValue::Null,
            ],
        )?;
        plan_row_change(&columns, &change)?
            .execute(&mut scope)
            .await?;
    }
    let rows = BoundStatement::query(
        "SELECT real_value FROM real_values ORDER BY id",
        Vec::new(),
        vec!["real_value".into()],
    )
    .fetch_rows(&mut scope)
    .await?;
    assert_eq!(
        rows.iter()
            .map(|row| row.value("real_value").cloned())
            .collect::<Result<Vec<_>, _>>()?,
        vec![
            SqlValue::Real(f64::INFINITY),
            SqlValue::Real(f64::NEG_INFINITY)
        ]
    );
    transaction.commit().await?;
    let rows = sqlx_core::query::query::<Turso>(
        "SELECT typeof(real_value) AS storage_class,real_value FROM real_values ORDER BY id",
    )
    .fetch_all(&mut connection)
    .await?;
    assert_eq!(rows.len(), 2);
    for (row, expected) in rows.iter().zip([f64::INFINITY, f64::NEG_INFINITY]) {
        assert_eq!(row.try_get::<String, _>("storage_class")?, "real");
        assert_eq!(row.try_get::<f64, _>("real_value")?, expected);
    }
    Ok(())
}

#[tokio::test]
async fn composite_upsert_and_delete_use_declared_primary_key_order_on_turso() -> TestResult {
    let mut connection = connection().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let mut scope = TursoScope(&mut transaction);
    let columns = [
        TableColumn {
            name: "second_key".into(),
            key_order: 2,
        },
        TableColumn {
            name: "body".into(),
            key_order: 0,
        },
        TableColumn {
            name: "first_key".into(),
            key_order: 1,
        },
    ];
    for body in ["first", "updated"] {
        let row = RowChange::upsert(
            "composite_keys",
            vec![
                SqlValue::Text("019eef00-0000-7000-8000-000000000001".into()),
                SqlValue::Text("019eef00-0000-7000-8000-000000000002".into()),
            ],
            vec![
                SqlValue::Text("019eef00-0000-7000-8000-000000000002".into()),
                SqlValue::Text(body.into()),
                SqlValue::Text("019eef00-0000-7000-8000-000000000001".into()),
            ],
        )?;
        plan_row_change(&columns, &row)?.execute(&mut scope).await?;
    }
    let rows = BoundStatement::query(
        "SELECT body FROM composite_keys",
        Vec::new(),
        vec!["body".into()],
    )
    .fetch_rows(&mut scope)
    .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows.first().map(|row| row.text("body")).transpose()?,
        Some("updated")
    );
    let delete = RowChange::delete(
        "composite_keys",
        vec![
            SqlValue::Text("019eef00-0000-7000-8000-000000000001".into()),
            SqlValue::Text("019eef00-0000-7000-8000-000000000002".into()),
        ],
    )?;
    plan_row_change(&columns, &delete)?
        .execute(&mut scope)
        .await?;
    assert!(
        BoundStatement::query(
            "SELECT body FROM composite_keys",
            Vec::new(),
            vec!["body".into()]
        )
        .fetch_rows(&mut scope)
        .await?
        .is_empty()
    );
    transaction.commit().await?;
    Ok(())
}

#[tokio::test]
async fn escaped_identifiers_and_data_cannot_change_the_statement() -> TestResult {
    let mut connection = connection().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let mut scope = TursoScope(&mut transaction);
    let data = "'); DROP TABLE exact_values; --";
    let row = RowChange::upsert(
        "odd\"table",
        vec![SqlValue::Text(
            "019eef00-0000-7000-8000-000000000001".into(),
        )],
        vec![
            SqlValue::Text("019eef00-0000-7000-8000-000000000001".into()),
            SqlValue::Text(data.into()),
        ],
    )?;
    let columns = [
        TableColumn {
            name: "odd\"key".into(),
            key_order: 1,
        },
        TableColumn {
            name: "body".into(),
            key_order: 0,
        },
    ];
    plan_row_change(&columns, &row)?.execute(&mut scope).await?;
    let rows = BoundStatement::query(
        "SELECT body FROM \"odd\"\"table\"",
        Vec::new(),
        vec!["body".into()],
    )
    .fetch_rows(&mut scope)
    .await?;
    assert_eq!(
        rows.first().map(|row| row.text("body")).transpose()?,
        Some(data)
    );
    BoundStatement::new(
        "INSERT INTO exact_values(id) VALUES (?)",
        vec![SqlValue::Integer(1)],
    )
    .execute(&mut scope)
    .await?;
    transaction.commit().await?;
    Ok(())
}

#[tokio::test]
async fn a_late_constraint_error_rolls_back_earlier_rows_in_the_real_scope() -> TestResult {
    let mut connection = connection().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let mut scope = TursoScope(&mut transaction);
    BoundStatement::new(
        "INSERT INTO exact_values(id) VALUES (?)",
        vec![SqlValue::Integer(1)],
    )
    .execute(&mut scope)
    .await?;
    let error = BoundStatement::new(
        "INSERT INTO exact_values(id) VALUES (?)",
        vec![SqlValue::Integer(1)],
    )
    .execute(&mut scope)
    .await;
    assert!(matches!(error, Err(StoreError::Constraint { .. })));
    transaction.rollback().await?;
    let row = sqlx_core::query::query::<Turso>("SELECT count(*) AS count FROM exact_values")
        .fetch_one(&mut connection)
        .await?;
    assert_eq!(row.try_get::<i64, _>("count")?, 0);
    Ok(())
}
