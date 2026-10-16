use crate::{ColumnTypeSource, GenerateError};
use std::collections::BTreeMap;
use turso_parser::{
    ast::{Cmd, Expr, OneSelect, ResultColumn, Stmt},
    parser::Parser,
};

/// The exact expression families the design names. This is not affinity
/// inference: an unrecognized function, cast target, wrapper or compound has no hint.
pub(crate) fn expression_classes(
    sql: &str,
) -> Result<BTreeMap<String, (&'static str, ColumnTypeSource)>, GenerateError> {
    let mut parser = Parser::new(sql.as_bytes());
    let Some(Cmd::Stmt(Stmt::Select(select))) = parser
        .next_cmd()
        .map_err(|error| GenerateError::Sql(error.to_string()))?
    else {
        return Ok(BTreeMap::new());
    };
    if !select.body.compounds.is_empty() {
        return Ok(BTreeMap::new());
    }
    let OneSelect::Select { columns, .. } = select.body.select else {
        return Ok(BTreeMap::new());
    };
    let mut classes = BTreeMap::new();
    for column in columns {
        if let ResultColumn::Expr(expression, Some(alias)) = column
            && alias.is_explicit()
            && let Some(class) = classify_expression(&expression)
        {
            classes.insert(alias.name().as_str().into(), class);
        }
    }
    Ok(classes)
}

fn classify_expression(expression: &Expr) -> Option<(&'static str, ColumnTypeSource)> {
    match expression {
        Expr::FunctionCall {
            name,
            distinctness,
            args,
            order_by,
            within_group,
            filter_over,
        } if name.as_str().eq_ignore_ascii_case("typeof")
            && args.len() == 1
            && distinctness.is_none()
            && order_by.is_empty()
            && within_group.is_empty()
            && filter_over.filter_clause.is_none()
            && filter_over.over_clause.is_none() =>
        {
            Some(("TEXT", ColumnTypeSource::Typeof))
        }
        Expr::Cast {
            type_name: Some(target),
            ..
        } if target.size.is_none() && target.array_dimensions == 0 => {
            let class = match target.name.to_ascii_uppercase().as_str() {
                "INTEGER" => "INTEGER",
                "REAL" => "REAL",
                "TEXT" => "TEXT",
                "BLOB" => "BLOB",
                _ => return None,
            };
            Some((class, ColumnTypeSource::Cast))
        }
        _ => None,
    }
}
