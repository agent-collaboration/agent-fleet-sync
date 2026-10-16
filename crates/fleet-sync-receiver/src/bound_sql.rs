use crate::{BoundValue, StoreError};
use turso_parser::{
    ast::fmt::{BlankContext, ToTokens, TokenStream, WriteTokenStream},
    parser::Parser,
    token::TokenType,
};
pub(crate) fn quoted_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

pub(crate) fn encode_bindings(sql: &str, parameters: &[BoundValue]) -> Result<String, StoreError> {
    if parameters.is_empty() {
        return Ok(sql.into());
    }
    let invalid = |error: turso_parser::error::Error| StoreError::Constraint {
        message: format!("core statement parse: {error}"),
    };
    let mut parser = Parser::new(sql.as_bytes());
    let command = parser
        .next_cmd()
        .map_err(invalid)?
        .ok_or_else(|| StoreError::Corrupt {
            field: "empty bound statement".into(),
        })?;
    if parser.next_cmd().map_err(invalid)?.is_some() {
        return Err(StoreError::Corrupt {
            field: "multiple bound statements".into(),
        });
    }
    let mut encoded = String::new();
    let mut writer = BindingWriter {
        inner: WriteTokenStream::new(&mut encoded),
        parameters,
    };
    command.to_tokens(&mut writer, &BlankContext)?;
    Ok(encoded.trim_end_matches(';').to_owned())
}
struct BindingWriter<'a> {
    inner: WriteTokenStream<'a, String>,
    parameters: &'a [BoundValue],
}
impl TokenStream for BindingWriter<'_> {
    type Error = StoreError;
    fn append(&mut self, kind: TokenType, value: Option<&str>) -> Result<(), Self::Error> {
        if kind == TokenType::TK_VARIABLE {
            let variable = value.ok_or_else(|| StoreError::Corrupt {
                field: "bound variable".into(),
            })?;
            let index = variable
                .strip_prefix('?')
                .and_then(|s| s.parse::<usize>().ok())
                .and_then(|n| n.checked_sub(1))
                .ok_or_else(|| StoreError::Corrupt {
                    field: "non-positional bound variable".into(),
                })?;
            let parameter = self
                .parameters
                .get(index)
                .ok_or_else(|| StoreError::Corrupt {
                    field: "bound parameter index".into(),
                })?;
            let cast = match parameter {
                BoundValue::IntegerText(_) => Some("INTEGER"),
                BoundValue::Real(_) => Some("REAL"),
                _ => None,
            };
            if let Some(kind) = cast {
                return self
                    .inner
                    .append(
                        TokenType::TK_ANY,
                        Some(&format!("CAST({variable} AS {kind})")),
                    )
                    .map_err(|_| StoreError::Corrupt {
                        field: "bound SQL rendering".into(),
                    });
            }
        }
        self.inner
            .append(kind, value)
            .map_err(|_| StoreError::Corrupt {
                field: "bound SQL rendering".into(),
            })
    }
}
pub(crate) fn project_transport_columns(sql: &str, columns: &[String]) -> String {
    let projections=columns.iter().enumerate().flat_map(|(index,name)|{
        let column=format!("\"_copy_source\".{}",quoted_identifier(name));
        [format!("typeof({column}) AS \"_copy_type_{index}\""),format!("CASE WHEN typeof({column})='integer' THEN CAST({column} AS TEXT) ELSE {column} END AS \"_copy_value_{index}\"")]
    }).collect::<Vec<_>>().join(", ");
    format!(
        "SELECT {projections} FROM ({}) AS \"_copy_source\"",
        sql.trim().trim_end_matches(';')
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;
    use crate::{BoundStatement, sql_transport::SqlRow};
    use fleet_sync_protocol::SqlValue;
    #[test]
    fn typed_casts_preserve_placeholders_and_do_not_touch_question_marks_in_literals() {
        let statement = BoundStatement::new(
            "INSERT INTO records VALUES(?, ?, ?, '?')",
            vec![
                SqlValue::Integer(i64::MAX),
                SqlValue::Real(1.0),
                SqlValue::Text("literal".into()),
            ],
        );
        let sql = statement.sql().expect("SQL");
        assert!(sql.contains("CAST(?1 AS INTEGER)"));
        assert!(sql.contains("CAST(?2 AS REAL)"));
        assert!(sql.contains("?3"));
        assert!(sql.contains("'?'"));
        assert_eq!(
            statement.parameters(),
            &[
                BoundValue::IntegerText(i64::MAX.to_string()),
                BoundValue::Real(1.0),
                BoundValue::Text("literal".into())
            ]
        );
    }
    #[test]
    fn typed_read_plan_decodes_classes_and_rejects_bad_or_duplicate_aliases() {
        let statement = BoundStatement::query(
            "SELECT record_value FROM records",
            Vec::new(),
            vec!["record_value".into()],
        );
        assert!(statement.sql().expect("query").contains("typeof("));
        assert!(statement.sql().expect("query").contains(" AS TEXT)"));
        let raw = vec![
            ("_copy_type_0".into(), BoundValue::Text("integer".into())),
            (
                "_copy_value_0".into(),
                BoundValue::Text(i64::MAX.to_string()),
            ),
        ];
        assert_eq!(
            statement
                .decode_row(raw)
                .expect("decode")
                .integer("record_value")
                .expect("integer"),
            i64::MAX
        );
        for raw in [
            vec![
                ("_copy_type_0".into(), BoundValue::Text("integer".into())),
                ("_copy_value_0".into(), BoundValue::Real(1.0)),
            ],
            vec![
                ("_copy_type_0".into(), BoundValue::Text("unknown".into())),
                ("_copy_value_0".into(), BoundValue::Null),
            ],
            vec![
                ("_copy_type_0".into(), BoundValue::Text("integer".into())),
                ("_copy_type_0".into(), BoundValue::Text("integer".into())),
            ],
        ] {
            assert!(statement.decode_row(raw).is_err());
        }
        assert!(
            SqlRow::from_transport(vec![("record_value".into(), BoundValue::Real(f64::NAN))])
                .is_err()
        );
    }
}
