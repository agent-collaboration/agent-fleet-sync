#![allow(clippy::unwrap_used)]
use fleet_sync_protocol::*;

#[test]
fn schema_admission_policy_preserves_allowed_forms_and_nested_namespace_refusals() {
    let protected = |form, name: &str| {
        Err(SchemaAdmissionError {
            form,
            reason: SchemaAdmissionReason::ProtectedObject { name: name.into() },
        })
    };
    let disallowed = |form| {
        Err(SchemaAdmissionError {
            form,
            reason: SchemaAdmissionReason::DisallowedForm,
        })
    };
    for (sql, expected) in [
        ("CREATE TABLE example(id TEXT PRIMARY KEY)", Ok(())),
        (
            "INSERT INTO example SELECT * FROM _fleet_sync_sessions",
            protected(SchemaStatementForm::Insert, "_fleet_sync_sessions"),
        ),
        (
            "CREATE TABLE \"_fleet_sync_private\"(id)",
            protected(SchemaStatementForm::CreateTable, "_fleet_sync_private"),
        ),
        (
            "CREATE TRIGGER changed AFTER INSERT ON example BEGIN DELETE FROM example; END",
            disallowed(SchemaStatementForm::CreateTrigger),
        ),
        (
            "PRAGMA foreign_keys = OFF",
            disallowed(SchemaStatementForm::Pragma),
        ),
        ("SELECT 1", disallowed(SchemaStatementForm::Select)),
        ("EXPLAIN SELECT 1", disallowed(SchemaStatementForm::Explain)),
    ] {
        assert_eq!(admit_schema_statement(sql), expected, "{sql}");
        let mut parser = turso_parser::parser::Parser::new(sql.as_bytes());
        let command = parser.next_cmd().unwrap().unwrap();
        assert!(parser.next_cmd().unwrap().is_none());
        assert_eq!(admit_schema_command(&command), expected, "{sql}");
        assert_eq!(
            admit_schema_command(&command),
            expected,
            "borrowed command reused: {sql}"
        );
    }
}
