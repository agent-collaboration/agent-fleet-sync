use turso_parser::ast::{
    Cmd, ColumnConstraint, CreateTableBody, Expr, InsertBody, Name, QualifiedName, Stmt,
    TableConstraint, UpsertDo,
};

/// Replicated identities use one spelling and one UUID version. Local keys are
/// outside this contract; SQL affinity alone never proves a replicated key.
pub use runtime_primitives::canonical_uuid_v7;

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("replicated key column {column:?} of {table} must be TEXT NOT NULL")]
pub struct KeyColumnError {
    pub table: String,
    pub column: Option<String>,
}
/// Every CREATE key column must be explicitly non-null TEXT. The error names
/// the offending column, or None for a missing key. Other statements return None.
pub fn replicated_key_columns(
    command: &Cmd,
) -> Result<Option<(&QualifiedName, Vec<&Name>)>, KeyColumnError> {
    let Cmd::Stmt(Stmt::CreateTable { tbl_name, body, .. }) = command else {
        return Ok(None);
    };
    let invalid = |column: Option<&Name>| KeyColumnError {
        table: tbl_name.name.as_str().into(),
        column: column.map(|name| name.as_str().into()),
    };
    let CreateTableBody::ColumnsAndConstraints {
        columns,
        constraints,
        ..
    } = body
    else {
        return Err(invalid(None));
    };
    let mut keys = Vec::new();
    for column in columns {
        for constraint in &column.constraints {
            if let ColumnConstraint::PrimaryKey { auto_increment, .. } = &constraint.constraint {
                if *auto_increment {
                    return Err(invalid(Some(&column.col_name)));
                }
                keys.push(&column.col_name);
            }
        }
    }
    for constraint in constraints {
        if let TableConstraint::PrimaryKey {
            columns,
            auto_increment,
            ..
        } = &constraint.constraint
        {
            for column in columns {
                let Some(name) = expression_name(&column.expr) else {
                    return Err(invalid(None));
                };
                if *auto_increment {
                    return Err(invalid(Some(name)));
                }
                keys.push(name);
            }
        }
    }
    if keys.is_empty() {
        return Err(invalid(None));
    }
    for key in &keys {
        let Some(column) = columns
            .iter()
            .find(|column| column.col_name.as_str().eq_ignore_ascii_case(key.as_str()))
        else {
            return Err(invalid(Some(key)));
        };
        if !column.col_type.as_ref().is_some_and(|kind| {
            kind.name.eq_ignore_ascii_case("TEXT")
                && kind.size.is_none()
                && kind.array_dimensions == 0
        }) || !column.constraints.iter().any(|constraint| {
            matches!(
                constraint.constraint,
                ColumnConstraint::NotNull {
                    nullable: false,
                    ..
                }
            )
        }) {
            return Err(invalid(Some(key)));
        }
    }
    Ok(Some((tbl_name, keys)))
}
fn expression_name(expression: &Expr) -> Option<&Name> {
    match expression {
        Expr::Id(name) | Expr::Name(name) => Some(name),
        Expr::Collate(inner, _) => expression_name(inner),
        _ => None,
    }
}

/// Any assignment is refused, including SET id=id. The assignment's expression
/// cannot turn a forbidden identity mutation into a supported statement.
pub fn assigns_replicated_key(command: &Cmd, key: &str) -> bool {
    let assigned = |sets: &[turso_parser::ast::Set]| {
        sets.iter()
            .flat_map(|set| &set.col_names)
            .any(|column| column.as_str().eq_ignore_ascii_case(key))
    };
    match command {
        Cmd::Stmt(Stmt::Update(update)) => assigned(&update.sets),
        Cmd::Stmt(Stmt::Insert {
            body: InsertBody::Select(_, upsert),
            ..
        }) => {
            let mut clause = upsert.as_deref();
            while let Some(upsert) = clause {
                if let UpsertDo::Set { sets, .. } = &upsert.do_clause
                    && assigned(sets)
                {
                    return true;
                }
                clause = upsert.next.as_deref();
            }
            false
        }
        _ => false,
    }
}
