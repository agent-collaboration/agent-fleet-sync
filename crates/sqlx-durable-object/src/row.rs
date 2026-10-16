use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use sqlx_core::column::ColumnIndex;
use sqlx_core::error::Error;
use sqlx_core::row::Row;
use sqlx_durable_object_core::Value;

use crate::{Do, DoColumn, DoValueRef};

/// A row of a Durable Object storage result.
pub struct DoRow {
    /// Shared by every row of one result.
    pub(crate) columns: Arc<[DoColumn]>,
    pub(crate) values: Box<[Value]>,
}

impl DoRow {
    /// The rows of one result, from the cursor's `columnNames` and the rows
    /// `raw()` yields.
    ///
    /// Each column is typed by its first non-NULL value, since the cursor
    /// reports no declared types. Every row shares the one column list.
    pub(crate) fn from_result(
        names: Vec<String>,
        rows: Vec<Vec<Value>>,
    ) -> Result<Vec<DoRow>, Error> {
        sqlx_durable_object_core::rows(
            names,
            rows,
            |name, ordinal, type_info| DoColumn {
                name,
                ordinal,
                type_info,
            },
            |columns, values| DoRow { columns, values },
        )
    }
}

impl Row for DoRow {
    type Database = Do;

    fn columns(&self) -> &[DoColumn] {
        &self.columns
    }

    fn try_get_raw<I>(&self, index: I) -> Result<DoValueRef<'_>, Error>
    where
        I: ColumnIndex<Self>,
    {
        let index = index.index(self)?;
        self.values
            .get(index)
            .map(DoValueRef)
            .ok_or(Error::ColumnIndexOutOfBounds {
                index,
                len: self.values.len(),
            })
    }
}

impl ColumnIndex<DoRow> for &'_ str {
    /// The *last* column with this name, which is what sqlx-sqlite returns
    /// for `SELECT a.id, b.id`: it indexes names into a map, and the later
    /// insert wins. A linear scan is fine at the width of a real row.
    fn index(&self, row: &DoRow) -> Result<usize, Error> {
        row.columns
            .iter()
            .rposition(|column| column.name == *self)
            .ok_or_else(|| Error::ColumnNotFound((*self).into()))
    }
}

impl Debug for DoRow {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut map = f.debug_map();

        for (column, value) in self.columns.iter().zip(self.values.iter()) {
            map.entry(&column.name, value);
        }

        map.finish()
    }
}

#[cfg(test)]
mod tests {
    use sqlx_core::row::Row;
    use sqlx_core::type_info::TypeInfo;
    use sqlx_core::value::ValueRef;
    use sqlx_durable_object_core::Value;

    use super::DoRow;
    use crate::DoTypeInfo;

    #[test]
    fn a_column_is_typed_by_its_first_non_null_value() {
        let rows = DoRow::from_result(
            vec!["id".into(), "name".into()],
            vec![
                vec![Value::Integer(1), Value::Null],
                vec![Value::Integer(2), Value::Text("b".into())],
            ],
        )
        .unwrap();

        let types: Vec<_> = rows[0]
            .columns()
            .iter()
            .map(|column| column.type_info)
            .collect();
        assert_eq!(types, [DoTypeInfo::Integer, DoTypeInfo::Text]);
    }

    #[test]
    fn a_name_finds_the_last_column_that_has_it() {
        let rows = DoRow::from_result(
            vec!["id".into(), "id".into()],
            vec![vec![Value::Integer(1), Value::Integer(2)]],
        )
        .unwrap();

        let row = &rows[0];
        assert_eq!(row.try_get::<i64, _>(0).unwrap(), 1);
        assert_eq!(row.try_get::<i64, _>("id").unwrap(), 2);
        assert!(row.try_get_raw("missing").is_err());
    }

    #[test]
    fn a_value_carries_its_own_type() {
        let rows = DoRow::from_result(
            vec!["n".into()],
            vec![vec![Value::Integer(1)], vec![Value::Real(1.5)]],
        )
        .unwrap();

        // The column says INTEGER, from the first row; the second row's value
        // is still a REAL, and that is what decoding checks.
        assert_eq!(rows[1].try_get_raw(0).unwrap().type_info().name(), "REAL");
        assert!(rows[1].try_get::<i64, _>(0).is_err());
        assert!((rows[1].try_get::<f64, _>(0).unwrap() - 1.5).abs() < f64::EPSILON);
    }
}
