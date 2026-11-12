use super::{
    HistoryFault, ReceiptField, ReceiptRepresentation, SourceFault, StoredReceipt,
    installed_on_text, is_plain_identifier, validate_receipt, validate_source,
};
use crate::SyncTime;
use runtime_primitives::Clock;
use sqlx_core::{
    migrate::{Migration, MigrationType, Migrator},
    sql_str::SqlSafeStr,
};
use std::borrow::Cow;

struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> SyncTime {
        SyncTime::from_unix_millis(0)
    }
}

fn source(versions: &[(i64, bool)]) -> Migrator {
    Migrator {
        migrations: Cow::Owned(
            versions
                .iter()
                .map(|(version, no_tx)| {
                    Migration::new(
                        *version,
                        Cow::Borrowed("case"),
                        MigrationType::Simple,
                        "SELECT 1".into_sql_str(),
                        *no_tx,
                    )
                })
                .collect(),
        ),
        ..Migrator::DEFAULT
    }
}

#[test]
fn source_must_be_strictly_ordered_transactional_and_nonempty() {
    assert_eq!(validate_source(&source(&[(1, false), (5, false)])), Ok(5));
    assert_eq!(
        validate_source(&source(&[(5, false), (1, false)])),
        Err(SourceFault::Unordered { version: 1 })
    );
    assert_eq!(
        validate_source(&source(&[(1, false), (1, false)])),
        Err(SourceFault::Unordered { version: 1 })
    );
    assert_eq!(
        validate_source(&source(&[(1, false), (2, true)])),
        Err(SourceFault::NoTransaction { version: 2 })
    );
    assert_eq!(validate_source(&source(&[])), Err(SourceFault::Empty));
}

#[test]
fn history_tables_are_plain_identifiers() {
    for valid in [
        "_sqlx_migrations_fleet_sync_lease",
        "_fleet_sync_migrations",
        "a1",
    ] {
        assert!(is_plain_identifier(valid), "{valid}");
    }
    for invalid in ["", "1table", "a-b", "a b", "a;DROP TABLE x", "\"quoted\""] {
        assert!(!is_plain_identifier(invalid), "{invalid}");
    }
}

#[test]
fn installed_on_matches_sqlite_current_timestamp_text() {
    assert_eq!(
        installed_on_text(1_762_819_200_999).as_deref(),
        Some("2025-11-11 00:00:00")
    );
    assert_eq!(installed_on_text(i64::MAX), None);
}

fn stored() -> StoredReceipt {
    StoredReceipt {
        version: Some(3),
        description: Some("three".into()),
        success: Some(1),
        checksum: Some(vec![1, 2]),
        execution_time: Some(4),
        installed_on: Some("2025-11-11 00:00:00".into()),
        version_class: Some("integer".into()),
        success_class: Some("integer".into()),
        checksum_class: Some("blob".into()),
        execution_class: Some("integer".into()),
        installed_class: Some("text".into()),
    }
}

/// One way to corrupt a valid stored receipt.
type Corruption = fn(&mut StoredReceipt);

fn field_of(result: Result<super::ValidReceipt, HistoryFault>) -> Option<ReceiptField> {
    match result {
        Err(HistoryFault::Record { field, .. }) => Some(field),
        _ => None,
    }
}

#[test]
fn clocked_receipts_validate_every_field() {
    let clocked = ReceiptRepresentation::Clocked(&FixedClock);
    let valid = validate_receipt(stored(), clocked);
    assert!(matches!(valid, Ok(ref receipt) if receipt.version == 3 && !receipt.dirty));
    let cases: [(Corruption, ReceiptField); 6] = [
        (
            |row| row.version_class = Some("text".into()),
            ReceiptField::Version,
        ),
        (|row| row.success = Some(2), ReceiptField::Success),
        (
            |row| row.checksum_class = Some("text".into()),
            ReceiptField::Checksum,
        ),
        (
            |row| row.description = Some(String::new()),
            ReceiptField::Description,
        ),
        (
            |row| row.installed_on = Some("2025-02-30 00:00:00".into()),
            ReceiptField::InstalledOn,
        ),
        (
            |row| row.execution_time = Some(-1),
            ReceiptField::ExecutionTime,
        ),
    ];
    for (corrupt, field) in cases {
        let mut row = stored();
        corrupt(&mut row);
        assert_eq!(field_of(validate_receipt(row, clocked)), Some(field));
    }
}

#[test]
fn journal_receipts_keep_the_unknown_duration_sentinel() {
    let mut row = stored();
    row.execution_time = Some(-1);
    row.installed_on = None;
    row.description = None;
    assert!(validate_receipt(row, ReceiptRepresentation::StorageDefault).is_ok());
    let mut row = stored();
    row.success_class = Some("text".into());
    assert_eq!(
        field_of(validate_receipt(row, ReceiptRepresentation::StorageDefault)),
        Some(ReceiptField::Success)
    );
}

#[test]
fn a_zero_success_flag_is_left_for_sqlx_to_refuse_as_dirty() {
    let mut row = stored();
    row.success = Some(0);
    assert!(matches!(
        validate_receipt(row, ReceiptRepresentation::StorageDefault),
        Ok(receipt) if receipt.dirty
    ));
}
