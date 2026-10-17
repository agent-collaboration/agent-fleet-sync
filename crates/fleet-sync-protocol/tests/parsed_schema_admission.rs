#![allow(clippy::unwrap_used)]
use fleet_sync_protocol::*;

#[test]
fn replicated_keys_require_non_null_text_in_every_column_while_local_keys_stay_unrestricted() {
    for sql in [
        "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,payload TEXT)",
        "CREATE TABLE records(id INTEGER,payload TEXT,PRIMARY KEY(id AUTOINCREMENT))",
        "CREATE TABLE records(id INTEGER PRIMARY KEY,payload TEXT)",
        "CREATE TABLE records(id TEXT PRIMARY KEY,payload TEXT)",
        "CREATE TABLE records(first_id TEXT NOT NULL,second_id INTEGER NOT NULL,PRIMARY KEY(first_id,second_id))",
        "CREATE TABLE records(first_id TEXT NOT NULL,second_id TEXT,PRIMARY KEY(first_id,second_id))",
        "CREATE TABLE records(payload TEXT)",
    ] {
        admit_schema_statement(sql).unwrap();
        assert!(
            matches!(
                admit_replicated_schema_statement(sql),
                Err(SchemaAdmissionError {
                    reason: SchemaAdmissionReason::KeyNotUuidV7 { .. },
                    ..
                })
            ),
            "{sql}"
        );
    }
    for sql in [
        "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY,payload TEXT)",
        "CREATE TABLE records(first_id TEXT NOT NULL,second_id TEXT NOT NULL,PRIMARY KEY(first_id,second_id))",
        "CREATE TABLE records(first_id TEXT NOT NULL,second_id TEXT NOT NULL,PRIMARY KEY(second_id,first_id))",
        "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY,\"AUTOINCREMENT\" TEXT DEFAULT 'AUTOINCREMENT')",
    ] {
        admit_replicated_schema_statement(sql).unwrap();
    }
}

#[test]
fn uuid_keys_accept_only_canonical_lowercase_hyphenated_rfc_uuid_v7() {
    assert!(canonical_uuid_v7("019eef00-0000-7000-8000-000000000001"));
    assert!(canonical_uuid_v7("019eef00-0000-7000-bfff-ffffffffffff"));
    for invalid in [
        "019EEF00-0000-7000-8000-000000000001",
        "019eef00000070008000000000000001",
        "{019eef00-0000-7000-8000-000000000001}",
        "019eef00-0000-4000-8000-000000000001",
        "019eef00-0000-7000-c000-000000000001",
        "not-a-uuid",
    ] {
        assert!(!canonical_uuid_v7(invalid), "{invalid}");
    }
}

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
