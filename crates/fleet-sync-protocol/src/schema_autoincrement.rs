use turso_parser::ast::{
    AlterTableBody, Cmd, ColumnConstraint, ColumnDefinition, CreateTableBody, QualifiedName, Stmt,
    TableConstraint,
};

/// Inspect parser constraints, so an identifier or literal with this spelling
/// never turns an ordinary local/replicated table into an AUTOINCREMENT table.
pub fn schema_autoincrement_table(command: &Cmd) -> Option<&QualifiedName> {
    let Cmd::Stmt(statement) = command else {
        return None;
    };
    match statement {
        Stmt::CreateTable {
            tbl_name,
            body:
                CreateTableBody::ColumnsAndConstraints {
                    columns,
                    constraints,
                    ..
                },
            ..
        } if columns.iter().any(column_autoincrement)
            || constraints.iter().any(|constraint| {
                matches!(
                    constraint.constraint,
                    TableConstraint::PrimaryKey {
                        auto_increment: true,
                        ..
                    }
                )
            }) =>
        {
            Some(tbl_name)
        }
        Stmt::AlterTable(alter) => match &alter.body {
            AlterTableBody::AddColumn(column) | AlterTableBody::AlterColumn { new: column, .. }
                if column_autoincrement(column) =>
            {
                Some(&alter.name)
            }
            _ => None,
        },
        _ => None,
    }
}
fn column_autoincrement(column: &ColumnDefinition) -> bool {
    column.constraints.iter().any(|constraint| {
        matches!(
            constraint.constraint,
            ColumnConstraint::PrimaryKey {
                auto_increment: true,
                ..
            }
        )
    })
}
