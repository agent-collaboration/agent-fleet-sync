//! Inspect parsed expressions, so identifiers and quoted SQL text never become function calls.
use turso_parser::ast::*;

enum MigrationNode<'sql> {
    Statement(&'sql Stmt),
    Expression(&'sql Expr),
    Select(&'sql Select),
    From(&'sql FromClause),
    Table(&'sql SelectTable),
    Column(&'sql ColumnDefinition),
    Window(&'sql Window),
}

pub(crate) fn nondeterministic_function(command: &Cmd) -> Option<String> {
    let Cmd::Stmt(statement) = command else {
        return None;
    };
    let mut pending = vec![MigrationNode::Statement(statement)];
    while let Some(node) = pending.pop() {
        match node {
            MigrationNode::Statement(statement) => push_statement(statement, &mut pending),
            MigrationNode::Select(select) => {
                push_with(&select.with, &mut pending);
                push_select_core(&select.body.select, &mut pending);
                for compound in &select.body.compounds {
                    push_select_core(&compound.select, &mut pending);
                }
                push_sorted(&select.order_by, &mut pending);
                if let Some(limit) = &select.limit {
                    pending.push(MigrationNode::Expression(&limit.expr));
                    push_optional(&limit.offset, &mut pending);
                }
            }
            MigrationNode::From(from) => {
                pending.push(MigrationNode::Table(&from.select));
                for join in &from.joins {
                    pending.push(MigrationNode::Table(&join.table));
                    if let Some(JoinConstraint::On(expression)) = &join.constraint {
                        pending.push(MigrationNode::Expression(expression));
                    }
                }
            }
            MigrationNode::Table(table) => match table {
                SelectTable::Table(..) => {}
                SelectTable::Select(select, _) => pending.push(MigrationNode::Select(select)),
                SelectTable::Sub(from, _) => pending.push(MigrationNode::From(from)),
                SelectTable::TableCall(_, arguments, _) => {
                    push_expressions(arguments, &mut pending)
                }
            },
            MigrationNode::Column(column) => {
                for constraint in &column.constraints {
                    match &constraint.constraint {
                        ColumnConstraint::Check { expr, .. }
                        | ColumnConstraint::Default(expr)
                        | ColumnConstraint::Generated { expr, .. } => {
                            pending.push(MigrationNode::Expression(expr))
                        }
                        ColumnConstraint::PrimaryKey { .. }
                        | ColumnConstraint::NotNull { .. }
                        | ColumnConstraint::Unique(_)
                        | ColumnConstraint::Collate { .. }
                        | ColumnConstraint::ForeignKey { .. } => {}
                    }
                }
            }
            MigrationNode::Window(window) => {
                push_expressions(&window.partition_by, &mut pending);
                push_sorted(&window.order_by, &mut pending);
                if let Some(frame) = &window.frame_clause {
                    for bound in std::iter::once(&frame.start).chain(frame.end.iter()) {
                        if let FrameBound::Following(expression)
                        | FrameBound::Preceding(expression) = bound
                        {
                            pending.push(MigrationNode::Expression(expression));
                        }
                    }
                }
            }
            MigrationNode::Expression(expression) => match expression {
                Expr::FunctionCall {
                    name,
                    args,
                    order_by,
                    within_group,
                    filter_over,
                    ..
                } => {
                    let name = name.as_str().to_ascii_lowercase();
                    if matches!(
                        name.as_str(),
                        "random" | "randomblob" | "changes" | "last_insert_rowid"
                    ) {
                        return Some(name);
                    }
                    if matches!(name.as_str(),"date"|"time"|"datetime"|"julianday"|"unixepoch"|"strftime")
                        && (args.is_empty() || (name=="strftime" && args.len()==1) || args.iter().any(|argument|matches!(argument.as_ref(),Expr::Literal(Literal::String(value)) if is_now_literal(value))))
                    {return Some(name);}
                    push_expressions(args, &mut pending);
                    push_sorted(order_by, &mut pending);
                    push_sorted(within_group, &mut pending);
                    push_function_tail(filter_over, &mut pending);
                }
                Expr::FunctionCallStar { name, filter_over } => {
                    if matches!(
                        name.as_str().to_ascii_lowercase().as_str(),
                        "random" | "randomblob" | "changes" | "last_insert_rowid"
                    ) {
                        return Some(name.as_str().to_ascii_lowercase());
                    }
                    push_function_tail(filter_over, &mut pending);
                }
                Expr::Literal(Literal::CurrentDate) => return Some("current_date".into()),
                Expr::Literal(Literal::CurrentTime) => return Some("current_time".into()),
                Expr::Literal(Literal::CurrentTimestamp) => {
                    return Some("current_timestamp".into());
                }
                Expr::Between {
                    lhs, start, end, ..
                } => pending.extend([
                    MigrationNode::Expression(lhs),
                    MigrationNode::Expression(start),
                    MigrationNode::Expression(end),
                ]),
                Expr::Binary(left, _, right)
                | Expr::Subscript {
                    base: left,
                    index: right,
                } => pending.extend([
                    MigrationNode::Expression(left),
                    MigrationNode::Expression(right),
                ]),
                Expr::Case {
                    base,
                    when_then_pairs,
                    else_expr,
                } => {
                    push_optional(base, &mut pending);
                    push_optional(else_expr, &mut pending);
                    for (condition, value) in when_then_pairs {
                        pending.extend([
                            MigrationNode::Expression(condition),
                            MigrationNode::Expression(value),
                        ]);
                    }
                }
                Expr::Cast { expr, .. }
                | Expr::Collate(expr, _)
                | Expr::FieldAccess { base: expr, .. }
                | Expr::IsNull(expr)
                | Expr::NotNull(expr)
                | Expr::Unary(_, expr) => pending.push(MigrationNode::Expression(expr)),
                Expr::Exists(select) | Expr::Subquery(select) => {
                    pending.push(MigrationNode::Select(select))
                }
                Expr::InList { lhs, rhs, .. } => {
                    pending.push(MigrationNode::Expression(lhs));
                    push_expressions(rhs, &mut pending);
                }
                Expr::InSelect { lhs, rhs, .. } => {
                    pending.extend([MigrationNode::Expression(lhs), MigrationNode::Select(rhs)])
                }
                Expr::InTable { lhs, args, .. } => {
                    pending.push(MigrationNode::Expression(lhs));
                    push_expressions(args, &mut pending);
                }
                Expr::Like {
                    lhs, rhs, escape, ..
                } => {
                    pending.extend([
                        MigrationNode::Expression(lhs),
                        MigrationNode::Expression(rhs),
                    ]);
                    push_optional(escape, &mut pending);
                }
                Expr::Parenthesized(expressions)
                | Expr::Array {
                    elements: expressions,
                } => push_expressions(expressions, &mut pending),
                Expr::Raise(_, expression) => push_optional(expression, &mut pending),
                Expr::SubqueryResult { lhs, .. } => push_optional(lhs, &mut pending),
                Expr::Literal(_)
                | Expr::Id(_)
                | Expr::Name(_)
                | Expr::Qualified(..)
                | Expr::DoublyQualified(..)
                | Expr::Column { .. }
                | Expr::RowId { .. }
                | Expr::Register(_)
                | Expr::Variable(_)
                | Expr::Default => {}
            },
        }
    }
    None
}

fn is_now_literal(value: &str) -> bool {
    value
        .strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
        .unwrap_or(value)
        .replace("''", "'")
        .eq_ignore_ascii_case("now")
}

#[cfg(test)]
mod tests {
    use super::nondeterministic_function;
    fn inspect(sql: &str) -> Result<Option<String>, Box<dyn std::error::Error>> {
        let command = turso_parser::parser::Parser::new(sql.as_bytes())
            .next_cmd()?
            .ok_or("no command")?;
        Ok(nondeterministic_function(&command))
    }

    #[test]
    #[allow(clippy::panic_in_result_fn)]
    fn finds_calls_in_defaults_ctes_subqueries_and_upserts()
    -> Result<(), Box<dyn std::error::Error>> {
        for sql in [
            "CREATE TABLE data(id TEXT PRIMARY KEY DEFAULT(randomblob(16)))",
            "ALTER TABLE data ADD COLUMN created TEXT DEFAULT CURRENT_TIMESTAMP",
            "INSERT INTO data VALUES(random())",
            "UPDATE data SET id=lower(hex(randomblob(8)))",
            "WITH source AS(SELECT changes()) INSERT INTO data SELECT * FROM source",
            "INSERT INTO data SELECT (SELECT last_insert_rowid())",
            "INSERT INTO data VALUES('x') ON CONFLICT DO UPDATE SET id=random()",
            "CREATE VIEW recent AS SELECT datetime('now')",
            "INSERT INTO data VALUES(strftime('%Y','NOW'))",
            "INSERT INTO data VALUES(datetime())",
        ] {
            assert!(inspect(sql)?.is_some(), "{sql}");
        }
        Ok(())
    }

    #[test]
    #[allow(clippy::panic_in_result_fn)]
    fn identifiers_literals_and_fixed_times_are_not_calls() -> Result<(), Box<dyn std::error::Error>>
    {
        for sql in [
            "CREATE TABLE random(id TEXT PRIMARY KEY,payload TEXT DEFAULT 'random()')",
            "INSERT INTO random VALUES('randomblob()','CURRENT_TIMESTAMP')",
            "UPDATE data SET id=datetime('2020-01-01')",
            "CREATE VIEW named AS SELECT id AS random FROM random",
            "INSERT INTO data VALUES(date('now'''))",
        ] {
            assert_eq!(inspect(sql)?, None, "{sql}");
        }
        Ok(())
    }
}

fn push_expressions<'sql>(expressions: &'sql [Box<Expr>], pending: &mut Vec<MigrationNode<'sql>>) {
    pending.extend(
        expressions
            .iter()
            .map(|expression| MigrationNode::Expression(expression)),
    );
}
fn push_optional<'sql>(
    expression: &'sql Option<Box<Expr>>,
    pending: &mut Vec<MigrationNode<'sql>>,
) {
    if let Some(expression) = expression {
        pending.push(MigrationNode::Expression(expression));
    }
}
fn push_sorted<'sql>(columns: &'sql [SortedColumn], pending: &mut Vec<MigrationNode<'sql>>) {
    pending.extend(
        columns
            .iter()
            .map(|column| MigrationNode::Expression(&column.expr)),
    );
}
fn push_with<'sql>(with: &'sql Option<With>, pending: &mut Vec<MigrationNode<'sql>>) {
    if let Some(with) = with {
        pending.extend(
            with.ctes
                .iter()
                .map(|entry| MigrationNode::Select(&entry.select)),
        );
    }
}
fn push_result_columns<'sql>(
    columns: &'sql [ResultColumn],
    pending: &mut Vec<MigrationNode<'sql>>,
) {
    for column in columns {
        if let ResultColumn::Expr(expression, _) = column {
            pending.push(MigrationNode::Expression(expression));
        }
    }
}
fn push_function_tail<'sql>(tail: &'sql FunctionTail, pending: &mut Vec<MigrationNode<'sql>>) {
    push_optional(&tail.filter_clause, pending);
    if let Some(Over::Window(window)) = &tail.over_clause {
        pending.push(MigrationNode::Window(window));
    }
}
fn push_select_core<'sql>(core: &'sql OneSelect, pending: &mut Vec<MigrationNode<'sql>>) {
    match core {
        OneSelect::Values(rows) => {
            for row in rows {
                push_expressions(row, pending);
            }
        }
        OneSelect::Select {
            columns,
            from,
            where_clause,
            group_by,
            window_clause,
            ..
        } => {
            push_result_columns(columns, pending);
            push_optional(where_clause, pending);
            if let Some(from) = from {
                pending.push(MigrationNode::From(from));
            }
            if let Some(group) = group_by {
                push_expressions(&group.exprs, pending);
                push_optional(&group.having, pending);
            }
            pending.extend(
                window_clause
                    .iter()
                    .map(|definition| MigrationNode::Window(&definition.window)),
            );
        }
    }
}
fn push_upserts<'sql>(upsert: Option<&'sql Upsert>, pending: &mut Vec<MigrationNode<'sql>>) {
    let mut next = upsert;
    while let Some(upsert) = next {
        if let Some(index) = &upsert.index {
            push_sorted(&index.targets, pending);
            push_optional(&index.where_clause, pending);
        }
        if let UpsertDo::Set { sets, where_clause } = &upsert.do_clause {
            pending.extend(
                sets.iter()
                    .map(|assignment| MigrationNode::Expression(&assignment.expr)),
            );
            push_optional(where_clause, pending);
        }
        next = upsert.next.as_deref();
    }
}
fn push_statement<'sql>(statement: &'sql Stmt, pending: &mut Vec<MigrationNode<'sql>>) {
    match statement {
        Stmt::CreateTable { body, .. } => match body {
            CreateTableBody::AsSelect(select) => pending.push(MigrationNode::Select(select)),
            CreateTableBody::ColumnsAndConstraints {
                columns,
                constraints,
                ..
            } => {
                pending.extend(columns.iter().map(MigrationNode::Column));
                for constraint in constraints {
                    match &constraint.constraint {
                        TableConstraint::Check { expr, .. } => {
                            pending.push(MigrationNode::Expression(expr))
                        }
                        TableConstraint::PrimaryKey { columns, .. }
                        | TableConstraint::Unique { columns, .. } => push_sorted(columns, pending),
                        TableConstraint::ForeignKey { .. } => {}
                    }
                }
            }
        },
        Stmt::CreateIndex {
            columns,
            with_clause,
            where_clause,
            ..
        } => {
            push_sorted(columns, pending);
            push_optional(where_clause, pending);
            pending.extend(
                with_clause
                    .iter()
                    .map(|(_, expression)| MigrationNode::Expression(expression)),
            );
        }
        Stmt::CreateView { select, .. } | Stmt::Select(select) => {
            pending.push(MigrationNode::Select(select))
        }
        Stmt::AlterTable(alter) => {
            if let AlterTableBody::AddColumn(column)
            | AlterTableBody::AlterColumn { new: column, .. } = &alter.body
            {
                pending.push(MigrationNode::Column(column));
            }
        }
        Stmt::Insert {
            with,
            body,
            returning,
            ..
        } => {
            push_with(with, pending);
            push_result_columns(returning, pending);
            if let InsertBody::Select(select, upsert) = body {
                pending.push(MigrationNode::Select(select));
                push_upserts(upsert.as_deref(), pending);
            }
        }
        Stmt::Update(update) => {
            push_with(&update.with, pending);
            push_optional(&update.where_clause, pending);
            push_result_columns(&update.returning, pending);
            pending.extend(
                update
                    .sets
                    .iter()
                    .map(|assignment| MigrationNode::Expression(&assignment.expr)),
            );
            if let Some(from) = &update.from {
                pending.push(MigrationNode::From(from));
            }
        }
        Stmt::Delete {
            with,
            where_clause,
            returning,
            ..
        } => {
            push_with(with, pending);
            push_optional(where_clause, pending);
            push_result_columns(returning, pending);
        }
        // Admission rejects every other statement carrying executable expressions.
        _ => {}
    }
}
