//! Exact existing transport: decimal integer casts plus storage-class-tagged reads.
use crate::{ProbeError, TransactionCallback, TransactionScope, TransactionStore};
use futures_core::future::BoxFuture;
use sqlx_core::{
    column::ColumnIndex, database::Database, decode::Decode, encode::Encode, query::query,
    row::Row, types::Type,
};

#[derive(Debug, PartialEq)]
enum ExactCell {
    Integer(i64),
    Real(f64),
    Null,
    Text(String),
    Blob(Vec<u8>),
}

pub struct NumericRoundtrip;
impl<TStore> TransactionCallback<TStore, String> for NumericRoundtrip
where
    TStore: TransactionStore,
    <TStore::Database as Database>::Arguments:
        sqlx_core::arguments::IntoArguments<TStore::Database>,
    for<'value> String: Type<TStore::Database>
        + Decode<'value, TStore::Database>
        + Encode<'value, TStore::Database>,
    for<'value> f64: Type<TStore::Database>
        + Decode<'value, TStore::Database>
        + Encode<'value, TStore::Database>,
    for<'value> Vec<u8>: Type<TStore::Database>
        + Decode<'value, TStore::Database>
        + Encode<'value, TStore::Database>,
    for<'value> Option<String>: Encode<'value, TStore::Database>,
    for<'value> bool: Type<TStore::Database> + Decode<'value, TStore::Database>,
    for<'row> &'row str: ColumnIndex<<TStore::Database as Database>::Row>,
{
    fn call<'scope>(
        self,
        mut scope: TStore::Scope<'scope>,
    ) -> BoxFuture<'scope, Result<String, ProbeError>> {
        Box::pin(async move {
            let samples = [
                ("i64-min", ExactCell::Integer(i64::MIN)),
                ("i64-max", ExactCell::Integer(i64::MAX)),
                ("safe-max", ExactCell::Integer(9_007_199_254_740_991)),
                ("two53", ExactCell::Integer(9_007_199_254_740_992)),
                ("two53-plus-one", ExactCell::Integer(9_007_199_254_740_993)),
                ("real-one", ExactCell::Real(1.0)),
                ("positive-infinity", ExactCell::Real(f64::INFINITY)),
                ("negative-infinity", ExactCell::Real(f64::NEG_INFINITY)),
                ("null", ExactCell::Null),
                (
                    "text",
                    ExactCell::Text("snowman ☃ and 9007199254740993".into()),
                ),
                ("blob", ExactCell::Blob(vec![0, 255, 128, 1])),
            ];
            let sample_count = samples.len();
            for (name, expected) in &samples {
                let name = (*name).to_owned();
                match expected {
                    ExactCell::Integer(value) => {
                        query::<TStore::Database>(
                            "INSERT INTO numeric_samples VALUES (?, CAST(? AS INTEGER))",
                        )
                        .bind(name)
                        .bind(value.to_string())
                        .execute(scope.executor())
                        .await?;
                    }
                    ExactCell::Real(value) => {
                        query::<TStore::Database>(
                            "INSERT INTO numeric_samples VALUES (?, CAST(? AS REAL))",
                        )
                        .bind(name)
                        .bind(*value)
                        .execute(scope.executor())
                        .await?;
                    }
                    ExactCell::Null => {
                        query::<TStore::Database>("INSERT INTO numeric_samples VALUES (?, ?)")
                            .bind(name)
                            .bind(Option::<String>::None)
                            .execute(scope.executor())
                            .await?;
                    }
                    ExactCell::Text(value) => {
                        query::<TStore::Database>("INSERT INTO numeric_samples VALUES (?, ?)")
                            .bind(name)
                            .bind(value.clone())
                            .execute(scope.executor())
                            .await?;
                    }
                    ExactCell::Blob(value) => {
                        query::<TStore::Database>("INSERT INTO numeric_samples VALUES (?, ?)")
                            .bind(name)
                            .bind(value.clone())
                            .execute(scope.executor())
                            .await?;
                    }
                }
            }
            let mut observations = Vec::new();
            for (name, expected) in samples {
                let row = query::<TStore::Database>("SELECT typeof(sample_value) AS storage_class, CASE WHEN typeof(sample_value) = 'integer' THEN CAST(sample_value AS TEXT) ELSE sample_value END AS transport_value FROM numeric_samples WHERE sample_name = ?")
                    .bind(name.to_owned()).fetch_one(scope.executor()).await?;
                let storage_class: String = row.try_get("storage_class")?;
                let actual = match storage_class.as_str() {
                    "integer" => ExactCell::Integer(
                        row.try_get::<String, _>("transport_value")?
                            .parse()
                            .map_err(|_| ProbeError::Rejected)?,
                    ),
                    "real" => ExactCell::Real(row.try_get("transport_value")?),
                    "null" => {
                        if row
                            .try_get::<Option<String>, _>("transport_value")?
                            .is_some()
                        {
                            return Err(ProbeError::Rejected);
                        }
                        ExactCell::Null
                    }
                    "text" => ExactCell::Text(row.try_get("transport_value")?),
                    "blob" => ExactCell::Blob(row.try_get("transport_value")?),
                    _ => return Err(ProbeError::Rejected),
                };
                if actual != expected {
                    return Err(ProbeError::Rejected);
                }
                observations.push(format!("{name}:{storage_class}:{actual:?}"));
            }
            let boolean_row = query::<TStore::Database>("SELECT 2 AS stored_boolean")
                .fetch_one(scope.executor())
                .await?;
            let accepts_two: bool = boolean_row.try_get("stored_boolean")?;
            if !accepts_two {
                return Err(ProbeError::Rejected);
            }
            Ok(format!(
                "{sample_count} exact values/classes passed: {}; raw bool(2)={accepts_two} (domain must reject)",
                observations.join("; ")
            ))
        })
    }
}
