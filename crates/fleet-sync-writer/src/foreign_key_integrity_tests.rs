#![allow(clippy::panic_in_result_fn)] // Assertions decide behavior; Result propagates setup failures.

use super::{orphan_statement, quote_identifier, read_foreign_keys, reject_orphans};
use crate::{OpenError, SchemaForm};
use fleet_sync_protocol::TableName;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
    query::query,
    sql_str::{AssertSqlSafe, SqlSafeStr},
};
use sqlx_turso::{Turso, TursoConnectOptions};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy, Debug)]
enum KeyCase {
    Valid,
    Orphan,
    NullMatching,
    NullNonmatching,
    CrossRow,
    ParentNoCase,
    ChildNoCase,
}

impl KeyCase {
    fn is_orphan(self) -> bool {
        matches!(self, Self::Orphan | Self::CrossRow | Self::ChildNoCase)
    }

    fn child_value(self, column: usize, width: usize) -> &'static str {
        match self {
            Self::Valid => "'left'",
            Self::Orphan if column + 1 == width => "'missing'",
            Self::Orphan => "'left'",
            Self::NullMatching | Self::NullNonmatching if column + 1 == width => "NULL",
            Self::NullMatching => "'left'",
            Self::NullNonmatching => "'missing'",
            Self::CrossRow if width == 1 => "'missing'",
            Self::CrossRow if column == 0 => "'left'",
            Self::CrossRow => "'right'",
            Self::ParentNoCase | Self::ChildNoCase => "'LEFT'",
        }
    }
}

async fn check_key_width(width: usize) -> TestResult {
    let parent_table = quote_identifier("parent_rows");
    let child_table = TableName::new("child_rows")?;
    let child_name = quote_identifier(child_table.as_str());
    let parent_columns = (0..width)
        .map(|column| quote_identifier(&format!("parent \"key {column}")))
        .collect::<Vec<_>>();
    let child_columns = (0..width)
        .map(|column| quote_identifier(&format!("child \"key {column}")))
        .collect::<Vec<_>>();
    for implicit in [false, true] {
        for case in [
            KeyCase::Valid,
            KeyCase::Orphan,
            KeyCase::NullMatching,
            KeyCase::NullNonmatching,
            KeyCase::CrossRow,
            KeyCase::ParentNoCase,
            KeyCase::ChildNoCase,
        ] {
            let directory = tempfile::tempdir()?;
            let path = directory.path().join("orphan.db");
            let mut connection = TursoConnectOptions::new()
                .filename(&path)
                .create_if_missing(true)
                .foreign_keys(false)
                .connect()
                .await?;
            let parent_collation = if matches!(case, KeyCase::ParentNoCase) {
                "NOCASE"
            } else {
                "BINARY"
            };
            let child_collation = if matches!(case, KeyCase::ChildNoCase) {
                "NOCASE"
            } else {
                "BINARY"
            };
            let parent_definition = parent_columns
                .iter()
                .map(|name| format!("{name} TEXT NOT NULL COLLATE {parent_collation}"))
                .collect::<Vec<_>>()
                .join(",");
            let child_definition = child_columns
                .iter()
                .map(|name| format!("{name} TEXT COLLATE {child_collation}"))
                .collect::<Vec<_>>()
                .join(",");
            let parent_key = parent_columns.join(",");
            let child_key = child_columns.join(",");
            let target = if implicit {
                parent_table.clone()
            } else {
                format!("{parent_table}({parent_key})")
            };
            let first_child = child_columns.first().ok_or("missing child key")?;
            connection
                .execute(AssertSqlSafe(format!(
                    "CREATE TABLE {parent_table}({parent_definition},PRIMARY KEY({parent_key})); \
                     CREATE TABLE {child_name}({child_definition},FOREIGN KEY({child_key}) REFERENCES {target}); \
                     CREATE INDEX partial_children ON {child_name}({first_child}) WHERE {first_child} IS NOT NULL"
                )).into_sql_str())
                .await?;
            let left_row = vec!["'left'"; width].join(",");
            let right_row = vec!["'right'"; width].join(",");
            let child_row = (0..width)
                .map(|column| case.child_value(column, width))
                .collect::<Vec<_>>()
                .join(",");
            connection
                .execute(
                    AssertSqlSafe(format!(
                        "INSERT INTO {parent_table} VALUES({left_row}),({right_row}); \
                     INSERT INTO {child_name} VALUES({child_row})"
                    ))
                    .into_sql_str(),
                )
                .await?;

            // This historical comparator uses fixture names, independently of
            // the metadata reader and the production statement builder.
            let non_null = child_columns
                .iter()
                .map(|column| format!("child.{column} IS NOT NULL"))
                .collect::<Vec<_>>()
                .join(" AND ");
            let matching = parent_columns
                .iter()
                .zip(&child_columns)
                .map(|(parent, child)| format!("parent.{parent}=child.{child}"))
                .collect::<Vec<_>>()
                .join(" AND ");
            let historical = format!(
                "SELECT 1 FROM {child_name} AS child WHERE {non_null} AND NOT EXISTS(SELECT 1 FROM {parent_table} AS parent WHERE {matching}) LIMIT 1"
            );
            for reopened in [false, true] {
                if reopened {
                    connection.close().await?;
                    connection = TursoConnectOptions::new()
                        .filename(&path)
                        .foreign_keys(false)
                        .connect()
                        .await?;
                }
                let foreign_keys = read_foreign_keys(&mut connection, &child_table).await?;
                assert_eq!(foreign_keys.len(), 1);
                let foreign_key = foreign_keys.first().ok_or("missing foreign key")?;
                for (shape, statement) in [
                    ("historical", historical.clone()),
                    ("production", orphan_statement(&child_table, foreign_key)?),
                ] {
                    let found = query::<Turso>(AssertSqlSafe(statement).into_sql_str())
                        .fetch_optional(&mut connection)
                        .await?
                        .is_some();
                    assert_eq!(
                        found,
                        case.is_orphan(),
                        "width={width} implicit={implicit} case={case:?} reopened={reopened} shape={shape}"
                    );
                }
                let result =
                    reject_orphans(&mut connection, std::slice::from_ref(&child_table)).await;
                match result {
                    Ok(()) => assert!(!case.is_orphan(), "accepted {width}/{case:?}/{reopened}"),
                    Err(OpenError::SchemaUnsupported {
                        form: SchemaForm::ForeignKeyOrphan { table, parent },
                    }) => {
                        assert!(case.is_orphan(), "refused {width}/{case:?}/{reopened}");
                        assert_eq!(table, child_table);
                        assert_eq!(parent.as_str(), "parent_rows");
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            connection.close().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn orphan_meaning_at_width_one() -> TestResult {
    check_key_width(1).await
}

#[tokio::test]
async fn orphan_meaning_at_width_two() -> TestResult {
    check_key_width(2).await
}

#[tokio::test]
async fn orphan_meaning_at_width_ninety_eight() -> TestResult {
    check_key_width(98).await
}
