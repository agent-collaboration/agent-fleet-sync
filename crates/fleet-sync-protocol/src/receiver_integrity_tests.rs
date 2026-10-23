//! The receiver owns this wire refusal; it never carries row keys or data.
#![allow(clippy::panic_in_result_fn)]
use crate::{PieceRefusal, SeedAnswer, TableName};
type TestResult = Result<(), Box<dyn std::error::Error>>;
#[test]
fn integrity_refusals_round_trip_only_child_and_parent_table_names() -> TestResult {
    let seed = SeedAnswer::IntegrityViolation {
        table: TableName::new("a_child")?,
        references: TableName::new("z_parent")?,
    };
    let piece = PieceRefusal::IntegrityViolation {
        table: TableName::new("a_child")?,
        references: TableName::new("z_parent")?,
    };
    let wire =
        r#"{"kind":"integrityViolation","value":{"table":"a_child","references":"z_parent"}}"#;
    assert_eq!(serde_json::to_string(&seed)?, wire);
    assert_eq!(serde_json::to_string(&piece)?, wire);
    assert_eq!(serde_json::from_str::<SeedAnswer>(wire)?, seed);
    assert_eq!(serde_json::from_str::<PieceRefusal>(wire)?, piece);
    let leaking = r#"{"kind":"integrityViolation","value":{"table":"a_child","references":"z_parent","key":"row-data"}}"#;
    assert!(serde_json::from_str::<SeedAnswer>(leaking).is_err());
    assert!(serde_json::from_str::<PieceRefusal>(leaking).is_err());
    Ok(())
}
