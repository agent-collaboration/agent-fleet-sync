use crate::{BoundStatement, StoreError, bound_sql::quoted_identifier};
use fleet_sync_protocol::{RowChange, SqlValue, reserved_namespace};

/// The declared column order and primary-key order read from the copy's schema.
#[derive(Clone, Debug)]
pub(crate) struct TableColumn {
    pub name: String,
    pub key_order: i64,
}

pub(crate) fn primary_columns(columns: &[TableColumn]) -> Vec<&TableColumn> {
    let mut keys = columns
        .iter()
        .filter(|column| column.key_order > 0)
        .collect::<Vec<_>>();
    keys.sort_by_key(|column| column.key_order);
    keys
}

/// Build the admitted row statement; the caller executes it in the apply transaction.
pub(crate) fn plan_row_change(
    columns: &[TableColumn],
    row: &RowChange,
) -> Result<BoundStatement, StoreError> {
    let invalid = |message: &str| StoreError::Constraint {
        message: message.into(),
    };
    let (table, key) = match row {
        RowChange::Upsert { table, key, .. } | RowChange::Delete { table, key } => (table, key),
    };
    if reserved_namespace(table) {
        return Err(invalid("replicated table is reserved"));
    }
    let keys = primary_columns(columns);
    if columns.is_empty() || keys.is_empty() {
        return Err(invalid("replicated table must have a declared primary key"));
    }
    if key.len() != keys.len() || key.iter().any(|value| matches!(value, SqlValue::Null)) {
        return Err(invalid("row key does not match the declared primary key"));
    }
    let table_name = quoted_identifier(table);
    match row {
        RowChange::Upsert { values, .. } => {
            if values.len() != columns.len() {
                return Err(invalid("row values do not match column count"));
            }
            for (column, key_value) in keys.iter().zip(key) {
                let index = columns
                    .iter()
                    .position(|candidate| candidate.name == column.name)
                    .ok_or_else(|| StoreError::Corrupt {
                        field: "primary_key".into(),
                    })?;
                if values.get(index) != Some(key_value) {
                    return Err(invalid("row image and key disagree"));
                }
            }
            let names = columns
                .iter()
                .map(|column| quoted_identifier(&column.name))
                .collect::<Vec<_>>()
                .join(", ");
            let placeholders = columns.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
            let key_names = keys
                .iter()
                .map(|column| quoted_identifier(&column.name))
                .collect::<Vec<_>>()
                .join(", ");
            let updates = columns
                .iter()
                .filter(|column| column.key_order == 0)
                .map(|column| {
                    let name = quoted_identifier(&column.name);
                    format!("{name}=excluded.{name}")
                })
                .collect::<Vec<_>>();
            let on_conflict = if updates.is_empty() {
                "DO NOTHING".into()
            } else {
                format!("DO UPDATE SET {}", updates.join(", "))
            };
            Ok(BoundStatement::new(
                format!(
                    "INSERT INTO {table_name}({names}) VALUES({placeholders}) ON CONFLICT({key_names}) {on_conflict}"
                ),
                values.clone(),
            ))
        }
        RowChange::Delete { .. } => {
            let predicates = keys
                .iter()
                .map(|column| format!("{}=?", quoted_identifier(&column.name)))
                .collect::<Vec<_>>()
                .join(" AND ");
            Ok(BoundStatement::new(
                format!("DELETE FROM {table_name} WHERE {predicates}"),
                key.clone(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;
    use crate::BoundValue;

    fn columns() -> Vec<TableColumn> {
        vec![
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
        ]
    }

    #[test]
    fn composite_delete_uses_declared_key_order_instead_of_column_order() {
        // The contract's key tuple is ordered by the declared PK, not by SELECT column order.
        let row = RowChange::delete(
            "records",
            vec![SqlValue::Text("first".into()), SqlValue::Integer(i64::MAX)],
        )
        .expect("delete");
        let planned = plan_row_change(&columns(), &row).expect("statement");
        assert_eq!(
            planned.sql().expect("SQL"),
            "DELETE FROM \"records\" WHERE \"first_key\" = ?1 AND \"second_key\" = CAST(?2 AS INTEGER)"
        );
        assert_eq!(
            planned.parameters(),
            &[
                BoundValue::Text("first".into()),
                BoundValue::IntegerText(i64::MAX.to_string())
            ]
        );
    }

    #[test]
    fn upsert_updates_non_key_columns_and_preserves_the_full_row_image() {
        let values = vec![
            SqlValue::Integer(7),
            SqlValue::Blob(vec![0, 255]),
            SqlValue::Text("first".into()),
        ];
        let row = RowChange::upsert(
            "records",
            vec![SqlValue::Text("first".into()), SqlValue::Integer(7)],
            values,
        )
        .expect("upsert");
        let planned = plan_row_change(&columns(), &row).expect("statement");
        let sql = planned.sql().expect("SQL");
        assert!(sql.contains("ON CONFLICT (\"first_key\", \"second_key\") DO UPDATE SET \"body\" = excluded.\"body\""), "{sql}");
        assert_eq!(
            planned.parameters(),
            &[
                BoundValue::IntegerText("7".into()),
                BoundValue::Blob(vec![0, 255]),
                BoundValue::Text("first".into())
            ]
        );
    }

    #[test]
    fn null_short_and_disagreeing_keys_are_refused_before_execution() {
        let images = vec![
            SqlValue::Integer(7),
            SqlValue::Text("body".into()),
            SqlValue::Text("first".into()),
        ];
        for key in [
            vec![SqlValue::Null, SqlValue::Integer(7)],
            vec![SqlValue::Text("first".into())],
            vec![SqlValue::Text("other".into()), SqlValue::Integer(7)],
        ] {
            let row = RowChange::upsert("records", key, images.clone()).expect("row shape");
            assert!(matches!(
                plan_row_change(&columns(), &row),
                Err(StoreError::Constraint { .. })
            ));
        }
    }

    #[test]
    fn missing_primary_key_wrong_width_and_reserved_tables_are_refused() {
        let row = RowChange::upsert(
            "records",
            vec![SqlValue::Integer(7)],
            vec![SqlValue::Integer(7)],
        )
        .expect("row");
        assert!(
            plan_row_change(
                &[TableColumn {
                    name: "id".into(),
                    key_order: 0
                }],
                &row
            )
            .is_err()
        );
        assert!(plan_row_change(&columns(), &row).is_err());
        let reserved =
            RowChange::delete("_FlEeT_SyNc_copy", vec![SqlValue::Integer(7)]).expect("shape");
        assert!(
            plan_row_change(
                &[TableColumn {
                    name: "id".into(),
                    key_order: 1
                }],
                &reserved
            )
            .is_err()
        );
    }

    #[test]
    fn a_table_of_only_key_columns_uses_do_nothing_and_quotes_identifiers() {
        let row = RowChange::upsert(
            "odd\"table",
            vec![SqlValue::Integer(7)],
            vec![SqlValue::Integer(7)],
        )
        .expect("row");
        let planned = plan_row_change(
            &[TableColumn {
                name: "odd\"key".into(),
                key_order: 1,
            }],
            &row,
        )
        .expect("statement");
        let sql = planned.sql().expect("SQL");
        assert!(sql.contains("\"odd\"\"table\""), "{sql}");
        assert!(sql.contains("\"odd\"\"key\""), "{sql}");
        assert!(sql.ends_with("DO NOTHING"), "{sql}");
    }
}
