//! Exact storage classes at the SQLx boundary. Integer cells cross as validated decimal text.
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::SqlValue;
use sqlx_core::{
    arguments::IntoArguments,
    column::ColumnIndex,
    database::Database,
    decode::Decode,
    encode::Encode,
    query::{Query, query},
    row::Row,
    types::Type,
};

#[derive(Clone, Debug, PartialEq)]
enum ReadProjection {
    Columns(Vec<String>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct BoundStatement {
    sql: String,
    parameters: Vec<BoundValue>,
    read_projection: Option<ReadProjection>,
}

impl BoundStatement {
    pub(crate) fn new(sql: impl Into<String>, parameters: Vec<SqlValue>) -> Self {
        Self {
            sql: sql.into(),
            parameters: parameters.into_iter().map(BoundValue::from).collect(),
            read_projection: None,
        }
    }

    pub(crate) fn query(
        sql: impl Into<String>,
        parameters: Vec<SqlValue>,
        columns: Vec<String>,
    ) -> Self {
        let mut statement = Self::new(sql, parameters);
        statement.read_projection = Some(ReadProjection::Columns(columns));
        statement
    }
    pub fn sql(&self) -> Result<String, StoreError> {
        let encoded = crate::bound_sql::encode_bindings(&self.sql, &self.parameters)?;
        Ok(match &self.read_projection {
            Some(ReadProjection::Columns(columns)) => {
                crate::bound_sql::project_transport_columns(&encoded, columns)
            }
            None => encoded,
        })
    }
    pub fn decode_row(&self, raw: Vec<(String, BoundValue)>) -> Result<SqlRow, StoreError> {
        let columns = match &self.read_projection {
            Some(ReadProjection::Columns(columns)) => columns,
            _ => {
                return Err(StoreError::Corrupt {
                    field: "query read plan".into(),
                });
            }
        };
        if raw.len() != columns.len() * 2 {
            return Err(StoreError::Corrupt {
                field: "query column cardinality".into(),
            });
        }
        let mut decoded = Vec::with_capacity(columns.len());
        for (index, name) in columns.iter().enumerate() {
            let find = |key: &str| -> Result<&BoundValue, StoreError> {
                let mut values = raw
                    .iter()
                    .filter(|(column, _)| column == key)
                    .map(|(_, value)| value);
                let value = values
                    .next()
                    .ok_or_else(|| StoreError::Corrupt { field: key.into() })?;
                if values.next().is_some() {
                    return Err(StoreError::Corrupt { field: key.into() });
                }
                Ok(value)
            };
            let kind = find(&format!("_copy_type_{index}"))?;
            let value = find(&format!("_copy_value_{index}"))?;
            let encoded = match (kind, value) {
                (BoundValue::Text(kind), BoundValue::Text(value)) if kind == "integer" => {
                    BoundValue::IntegerText(value.clone())
                }
                (BoundValue::Text(kind), BoundValue::Real(value)) if kind == "real" => {
                    BoundValue::Real(*value)
                }
                (BoundValue::Text(kind), BoundValue::Text(value)) if kind == "text" => {
                    BoundValue::Text(value.clone())
                }
                (BoundValue::Text(kind), BoundValue::Blob(value)) if kind == "blob" => {
                    BoundValue::Blob(value.clone())
                }
                (BoundValue::Text(kind), BoundValue::Null) if kind == "null" => BoundValue::Null,
                _ => {
                    return Err(StoreError::Corrupt {
                        field: name.clone(),
                    });
                }
            };
            decoded.push((name.clone(), encoded));
        }
        SqlRow::from_transport(decoded)
    }

    #[must_use]
    #[cfg(test)]
    pub fn parameters(&self) -> &[BoundValue] {
        &self.parameters
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum BoundValue {
    IntegerText(String),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
    Null,
}
impl From<SqlValue> for BoundValue {
    fn from(value: SqlValue) -> Self {
        match value {
            SqlValue::Integer(value) => Self::IntegerText(value.to_string()),
            SqlValue::Real(value) => Self::Real(value),
            SqlValue::Text(value) => Self::Text(value),
            SqlValue::Blob(value) => Self::Blob(value),
            SqlValue::Null => Self::Null,
        }
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct SqlRow {
    columns: Vec<(String, SqlValue)>,
}

impl SqlRow {
    pub fn from_transport(columns: Vec<(String, BoundValue)>) -> Result<Self, StoreError> {
        let columns = columns
            .into_iter()
            .map(|(name, value)| {
                let value = match value {
                    BoundValue::IntegerText(value) => {
                        SqlValue::Integer(value.parse().map_err(|_| StoreError::Corrupt {
                            field: name.clone(),
                        })?)
                    }
                    BoundValue::Real(value) if !value.is_nan() => SqlValue::Real(value),
                    BoundValue::Real(_) => return Err(StoreError::Corrupt { field: name }),
                    BoundValue::Text(value) => SqlValue::Text(value),
                    BoundValue::Blob(value) => SqlValue::Blob(value),
                    BoundValue::Null => SqlValue::Null,
                };
                Ok((name, value))
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        Ok(Self { columns })
    }
    pub fn value(&self, name: &str) -> Result<&SqlValue, StoreError> {
        self.columns
            .iter()
            .find_map(|(column, value)| (column == name).then_some(value))
            .ok_or_else(|| StoreError::Corrupt { field: name.into() })
    }

    #[cfg(test)]
    pub fn text(&self, name: &str) -> Result<&str, StoreError> {
        match self.value(name)? {
            SqlValue::Text(value) => Ok(value),
            _ => Err(StoreError::Corrupt { field: name.into() }),
        }
    }

    #[cfg(test)]
    pub fn integer(&self, name: &str) -> Result<i64, StoreError> {
        match self.value(name)? {
            SqlValue::Integer(value) => Ok(*value),
            _ => Err(StoreError::Corrupt { field: name.into() }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StoreError {
    #[error("storage capacity exhausted")]
    Full,
    #[error("storage constraint: {message}")]
    Constraint { message: String },
    #[error("storage unavailable: {message}")]
    Unavailable { message: String },
    #[error("invalid stored field {field}")]
    Corrupt { field: String },
}

pub(crate) fn sql_error(error: sqlx_core::error::Error) -> StoreError {
    match &error {
        sqlx_core::error::Error::Database(database) => match database.code().as_deref() {
            Some("SQLITE_FULL") => StoreError::Full,
            Some("SQLITE_CONSTRAINT") => StoreError::Constraint {
                message: error.to_string(),
            },
            Some("SQLITE_CORRUPT" | "SQLITE_NOTADB") => StoreError::Corrupt {
                field: "copy storage".into(),
            },
            _ => StoreError::Unavailable {
                message: error.to_string(),
            },
        },
        sqlx_core::error::Error::ColumnDecode { index, .. } => StoreError::Corrupt {
            field: index.clone(),
        },
        _ => StoreError::Unavailable {
            message: error.to_string(),
        },
    }
}

fn bind_values<D: Database>(
    sql: String,
    parameters: &[BoundValue],
) -> Query<'static, D, D::Arguments>
where
    D::Arguments: IntoArguments<D>,
    for<'value> String: Type<D> + Encode<'value, D>,
    for<'value> Option<String>: Type<D> + Encode<'value, D>,
    for<'value> f64: Type<D> + Encode<'value, D>,
    for<'value> Vec<u8>: Type<D> + Encode<'value, D>,
{
    // Identifiers were quoted by the row planner; values remain bound parameters.
    // Schema SQL is admitted by the protocol before reaching this boundary.
    let mut statement = query::<D>(sqlx_core::sql_str::AssertSqlSafe(sql));
    for parameter in parameters {
        statement = match parameter {
            BoundValue::IntegerText(value) | BoundValue::Text(value) => {
                statement.bind(value.clone())
            }
            BoundValue::Real(value) => statement.bind(*value),
            BoundValue::Blob(value) => statement.bind(value.clone()),
            BoundValue::Null => statement.bind(Option::<String>::None),
        };
    }
    statement
}

impl BoundStatement {
    pub(crate) async fn execute<S>(&self, scope: &mut S) -> Result<(), StoreError>
    where
        S: TransactionScope,
        <S::Database as Database>::Arguments: IntoArguments<S::Database>,
        for<'value> String: Type<S::Database> + Encode<'value, S::Database>,
        for<'value> Option<String>: Type<S::Database> + Encode<'value, S::Database>,
        for<'value> f64: Type<S::Database> + Encode<'value, S::Database>,
        for<'value> Vec<u8>: Type<S::Database> + Encode<'value, S::Database>,
    {
        let sql = self.sql()?;
        bind_values::<S::Database>(sql, &self.parameters)
            .execute(scope.executor())
            .await
            .map_err(sql_error)?;
        Ok(())
    }

    /// Dynamic replicated row reads use the exact-value projection. Static bookkeeping
    /// reads use their generated checked private rows instead of this path.
    pub(crate) async fn fetch_rows<S>(&self, scope: &mut S) -> Result<Vec<SqlRow>, StoreError>
    where
        S: TransactionScope,
        <S::Database as Database>::Arguments: IntoArguments<S::Database>,
        for<'value> String:
            Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
        for<'value> Option<String>:
            Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
        for<'value> f64:
            Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
        for<'value> Vec<u8>:
            Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
        for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
    {
        let sql = self.sql()?;
        let rows = bind_values::<S::Database>(sql, &self.parameters)
            .fetch_all(scope.executor())
            .await
            .map_err(sql_error)?;
        rows.iter()
            .map(|row| {
                let columns = match &self.read_projection {
                    Some(ReadProjection::Columns(columns)) => columns,
                    None => {
                        return Err(StoreError::Corrupt {
                            field: "query read plan".into(),
                        });
                    }
                };
                if row.columns().len() != columns.len() * 2 {
                    return Err(StoreError::Corrupt {
                        field: "query column cardinality".into(),
                    });
                }
                let mut raw = Vec::with_capacity(columns.len() * 2);
                for index in 0..columns.len() {
                    let type_name = format!("_copy_type_{index}");
                    let value_name = format!("_copy_value_{index}");
                    let kind: String = row.try_get(type_name.as_str()).map_err(sql_error)?;
                    let value = match kind.as_str() {
                        "integer" | "text" => {
                            BoundValue::Text(row.try_get(value_name.as_str()).map_err(sql_error)?)
                        }
                        "real" => {
                            BoundValue::Real(row.try_get(value_name.as_str()).map_err(sql_error)?)
                        }
                        "blob" => {
                            BoundValue::Blob(row.try_get(value_name.as_str()).map_err(sql_error)?)
                        }
                        "null" => {
                            let value: Option<String> =
                                row.try_get(value_name.as_str()).map_err(sql_error)?;
                            if value.is_some() {
                                return Err(StoreError::Corrupt { field: value_name });
                            }
                            BoundValue::Null
                        }
                        _ => return Err(StoreError::Corrupt { field: type_name }),
                    };
                    raw.push((type_name, BoundValue::Text(kind)));
                    raw.push((value_name, value));
                }
                self.decode_row(raw)
            })
            .collect()
    }
}
