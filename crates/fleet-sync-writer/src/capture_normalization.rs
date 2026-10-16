use crate::TableLayout;
use fleet_sync_protocol::{RowChange, SqlValue};
use turso::core::{
    Numeric,
    types::{ValueIterator, ValueRef},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CaptureChange {
    Insert {
        before: Option<Vec<u8>>,
        after: Option<Vec<u8>>,
    },
    Update {
        before: Vec<u8>,
        after: Vec<u8>,
    },
    Delete {
        before: Vec<u8>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CaptureError {
    #[error("malformed capture image: {detail}")]
    InvalidImage { detail: String },
    #[error("captured row does not match table {table}")]
    RowShape { table: String },
    #[error("insert has no supported after image for table {table}")]
    MissingInsertImage { table: String },
    #[error("capture contains a NaN real")]
    NaNReal,
}

pub fn normalize_capture_change(
    table: &TableLayout,
    change: CaptureChange,
) -> Result<Vec<RowChange>, CaptureError> {
    match change {
        CaptureChange::Insert { before, after } => {
            // Turso 0.8.1 puts an INTEGER PRIMARY KEY-changing UPDATE's INSERT
            // image in before. Restrict this workaround to that table shape.
            let image = after
                .or_else(|| table.has_integer_primary_key().then_some(before).flatten())
                .ok_or_else(|| CaptureError::MissingInsertImage {
                    table: table.name().into(),
                })?;
            Ok(vec![upsert_row(table, decode_row(table, &image)?)?])
        }
        CaptureChange::Update { before, after } => {
            let old_values = decode_row(table, &before)?;
            let new_values = decode_row(table, &after)?;
            let old_key = row_key(table, &old_values)?;
            let new_key = row_key(table, &new_values)?;
            let mut changes = Vec::new();
            if old_key != new_key {
                changes.push(RowChange::Delete {
                    table: table.name().into(),
                    key: old_key,
                });
            }
            changes.push(RowChange::Upsert {
                table: table.name().into(),
                key: new_key,
                values: new_values,
            });
            Ok(changes)
        }
        CaptureChange::Delete { before } => {
            let values = decode_row(table, &before)?;
            Ok(vec![RowChange::Delete {
                table: table.name().into(),
                key: row_key(table, &values)?,
            }])
        }
    }
}

fn decode_row(table: &TableLayout, image: &[u8]) -> Result<Vec<SqlValue>, CaptureError> {
    let values = decode_image(image)?;
    if values.len() != table.columns().len() {
        return Err(CaptureError::RowShape {
            table: table.name().into(),
        });
    }
    Ok(values)
}

pub(crate) fn decode_image(image: &[u8]) -> Result<Vec<SqlValue>, CaptureError> {
    let malformed = |error: turso::core::LimboError| CaptureError::InvalidImage {
        detail: error.to_string(),
    };
    let mut values = Vec::new();
    for cell in ValueIterator::new(image).map_err(malformed)? {
        values.push(match cell.map_err(malformed)? {
            ValueRef::Null => SqlValue::Null,
            ValueRef::Numeric(Numeric::Integer(value)) => SqlValue::Integer(value),
            ValueRef::Numeric(Numeric::Float(value)) => {
                let value = f64::from(value);
                if value.is_nan() {
                    return Err(CaptureError::NaNReal);
                }
                SqlValue::Real(value)
            }
            ValueRef::Text(value) => SqlValue::Text(value.as_str().into()),
            ValueRef::Blob(value) => SqlValue::Blob(value.to_vec()),
        });
    }
    Ok(values)
}

fn row_key(table: &TableLayout, values: &[SqlValue]) -> Result<Vec<SqlValue>, CaptureError> {
    table
        .primary_key()
        .iter()
        .map(|index| {
            values
                .get(*index)
                .cloned()
                .ok_or_else(|| CaptureError::RowShape {
                    table: table.name().into(),
                })
        })
        .collect()
}

fn upsert_row(table: &TableLayout, values: Vec<SqlValue>) -> Result<RowChange, CaptureError> {
    Ok(RowChange::Upsert {
        table: table.name().into(),
        key: row_key(table, &values)?,
        values,
    })
}
