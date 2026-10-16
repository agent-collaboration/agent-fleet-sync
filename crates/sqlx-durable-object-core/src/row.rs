use std::sync::Arc;

use sqlx_core::error::Error;

use crate::{TypeInfo, Value};

/// The rows of one result, from its column names and its rows of values.
///
/// Each column is typed by its first non-NULL value, since neither backend
/// reports declared types; `column` builds a driver's column from its name,
/// ordinal and that type, and every row shares the one column list. `row`
/// builds a driver's row from the columns and its values.
///
/// # Errors
///
/// When a row does not have one value per column.
pub fn rows<C, R>(
    names: Vec<String>,
    rows: Vec<Vec<Value>>,
    column: impl Fn(String, usize, TypeInfo) -> C,
    row: impl Fn(Arc<[C]>, Box<[Value]>) -> R,
) -> Result<Vec<R>, Error> {
    let columns: Arc<[C]> = names
        .into_iter()
        .enumerate()
        .map(|(ordinal, name)| {
            let type_info = rows
                .iter()
                .filter_map(|row| row.get(ordinal))
                .map(Value::type_info)
                .find(|type_info| *type_info != TypeInfo::Null)
                .unwrap_or(TypeInfo::Null);

            column(name, ordinal, type_info)
        })
        .collect();

    rows.into_iter()
        .map(|values| {
            if values.len() != columns.len() {
                return Err(Error::Protocol(format!(
                    "a row of {} values came back for {} columns",
                    values.len(),
                    columns.len()
                )));
            }

            Ok(row(Arc::clone(&columns), values.into_boxed_slice()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::rows;
    use crate::{TypeInfo, Value};

    fn types(names: &[&str], values: Vec<Vec<Value>>) -> Vec<TypeInfo> {
        let names = names.iter().map(|name| (*name).to_owned()).collect();
        let rows = rows(
            names,
            values,
            |_, _, type_info| type_info,
            |columns, _| columns,
        )
        .unwrap();
        rows[0].to_vec()
    }

    #[test]
    fn a_column_is_typed_by_its_first_non_null_value() {
        let types = types(
            &["id", "name", "gone"],
            vec![
                vec![Value::Integer(1), Value::Null, Value::Null],
                vec![Value::Integer(2), Value::Text("b".into()), Value::Null],
            ],
        );

        assert_eq!(types, [TypeInfo::Integer, TypeInfo::Text, TypeInfo::Null]);
    }

    #[test]
    fn a_short_row_is_an_error() {
        assert!(
            rows(
                vec!["a".into(), "b".into()],
                vec![vec![Value::Null]],
                |_, _, _| (),
                |_, _| ()
            )
            .is_err()
        );
    }
}
