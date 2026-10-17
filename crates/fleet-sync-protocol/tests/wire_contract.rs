#![allow(clippy::unwrap_used, clippy::expect_used)]

use fleet_sync_protocol::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

fn position(sequence: u64) -> ReplicationPosition {
    ReplicationPosition::new(Epoch::new(1).unwrap(), CommitSequence::new(sequence))
}
fn contents(annotation: Option<String>) -> PieceContents<String> {
    PieceContents {
        database: DatabaseId::new(Uuid::from_u128(1)).unwrap(),
        epoch: Epoch::new(1).unwrap(),
        continues_from: position(0),
        through: position(1),
        transactions: vec![ReplicatedTransaction::Rows {
            commit_id: CommitSequence::new(1),
            rows: vec![
                RowChange::upsert(
                    "example",
                    vec![SqlValue::Text("row".into())],
                    vec![SqlValue::Integer(i64::MAX)],
                )
                .unwrap(),
            ],
        }],
        part: None,
        annotation,
    }
}

#[test]
fn exact_sql_storage_classes_and_integer_boundaries_cross_json() {
    for (value, json) in [
        (
            SqlValue::Integer(i64::MAX),
            r#"{"kind":"integer","value":9223372036854775807}"#,
        ),
        (
            SqlValue::Integer(i64::MIN),
            r#"{"kind":"integer","value":-9223372036854775808}"#,
        ),
        (SqlValue::Null, r#"{"kind":"null"}"#),
        (
            SqlValue::Blob(vec![0, 255]),
            r#"{"kind":"blob","value":"AP8="}"#,
        ),
        (SqlValue::Real(1.5), r#"{"kind":"real","value":1.5}"#),
        (
            SqlValue::Text("literal".into()),
            r#"{"kind":"text","value":"literal"}"#,
        ),
    ] {
        assert_eq!(serde_json::to_string(&value).unwrap(), json);
        assert_eq!(serde_json::from_str::<SqlValue>(json).unwrap(), value);
    }
    assert!(serde_json::to_vec(&SqlValue::Real(f64::NAN)).is_err());
    assert!(serde_json::from_str::<SqlValue>(r#"{"kind":"blob","value":"??=="}"#).is_err());
}

#[test]
fn sql_real_infinities_and_negative_zero_preserve_their_exact_wire_values() {
    for (value, wire) in [
        (
            f64::INFINITY,
            r#"{"kind":"real","value":"positiveInfinity"}"#,
        ),
        (
            f64::NEG_INFINITY,
            r#"{"kind":"real","value":"negativeInfinity"}"#,
        ),
        (-0.0, r#"{"kind":"real","value":-0.0}"#),
    ] {
        let encoded = serde_json::to_string(&SqlValue::Real(value)).unwrap();
        let decoded: SqlValue = serde_json::from_str(wire).unwrap();

        assert_eq!(encoded, wire);
        assert!(
            matches!(decoded, SqlValue::Real(decoded) if decoded.to_bits() == value.to_bits()),
            "real storage class or bits changed: {wire}"
        );
    }

    for marker in ["positiveInfinity", "negativeInfinity"] {
        let text_wire = serde_json::json!({"kind":"text","value":marker});
        assert_eq!(
            serde_json::from_value::<SqlValue>(text_wire).unwrap(),
            SqlValue::Text(marker.into())
        );
    }
}

#[test]
fn sql_real_decode_rejects_nan_and_undeclared_special_values() {
    for wire in [
        r#"{"kind":"real","value":"NaN"}"#,
        r#"{"kind":"real","value":NaN}"#,
        r#"{"kind":"real","value":null}"#,
        r#"{"kind":"real","value":"Infinity"}"#,
        r#"{"kind":"real","value":"-Infinity"}"#,
        r#"{"kind":"real","value":1e400}"#,
    ] {
        assert!(serde_json::from_str::<SqlValue>(wire).is_err(), "{wire}");
    }
}

#[test]
fn annotation_is_optional_and_covered_by_the_piece_digest() {
    let unannotated = Piece::new(contents(None)).unwrap();
    let annotated = Piece::new(contents(Some("consumer".into()))).unwrap();
    assert_ne!(unannotated.digest, annotated.digest);
    let encoded = serde_json::to_vec(&annotated).unwrap();
    let decoded: Piece<String> = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded.annotation.as_deref(), Some("consumer"));
    assert_eq!(decoded.recomputed_digest().unwrap(), annotated.digest);
}

#[test]
fn piece_rejects_bad_continuation_and_nonfinal_annotation() {
    let mut invalid = contents(None);
    invalid.continues_from = position(2);
    assert!(matches!(
        Piece::new(invalid),
        Err(PieceError::InvalidPosition)
    ));
    let mut partial = contents(Some("premature".into()));
    partial.part = Some(
        TransactionPart::new(
            TransactionKey {
                epoch: Epoch::new(1).unwrap(),
                commit_id: CommitSequence::new(1),
            },
            0,
            2,
            PieceDigest::of_transaction(partial.transactions.first().unwrap()).unwrap(),
        )
        .unwrap(),
    );
    assert!(matches!(
        Piece::new(partial),
        Err(PieceError::InvalidAnnotation)
    ));
}

#[test]
fn identities_epochs_and_parts_validate_during_decode() {
    assert!(serde_json::from_str::<SessionId>("\"00000000-0000-0000-0000-000000000000\"").is_err());
    assert!(serde_json::from_str::<Epoch>("0").is_err());
    assert!(
        TransactionPart::new(
            TransactionKey {
                epoch: Epoch::new(1).unwrap(),
                commit_id: CommitSequence::new(1)
            },
            1,
            1,
            PieceDigest::of_transaction(&ReplicatedTransaction::Rows {
                commit_id: CommitSequence::new(1),
                rows: vec![]
            })
            .unwrap()
        )
        .is_err()
    );
}

struct ExampleProtocol;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConsumerRequest {
    request: RequestId,
    text: String,
}
impl ConsumerProtocol for ExampleProtocol {
    type Delivery = NoDelivery;
    type Request = ConsumerRequest;
    type Answer = u64;
    type ServerRequest = String;
    type ClientAnswer = bool;
    type Notice = String;
    type Annotation = String;
}

#[test]
fn consumer_payloads_are_typed_and_frames_never_supply_peer_identity() {
    let frame: ClientFrame<ExampleProtocol> = ClientFrame::Request {
        frame: FrameId::new(Uuid::from_u128(2)).unwrap(),
        body: OperationRequest::Consumer(ConsumerRequest {
            request: RequestId::new(Uuid::from_u128(3)).unwrap(),
            text: "payload".into(),
        }),
    };
    let decoded: ClientFrame<ExampleProtocol> =
        serde_json::from_slice(&serde_json::to_vec(&frame).unwrap()).unwrap();
    assert!(
        matches!(decoded, ClientFrame::Request { body: OperationRequest::Consumer(ConsumerRequest { text, .. }), .. } if text == "payload")
    );
    assert!(serde_json::from_str::<ClientFrame>(r#"{"kind":"hello","value":{"sessionId":"00000000-0000-0000-0000-000000000001","schemaSupport":{"maxVersion":1}}}"#).is_ok());
    assert!(serde_json::from_str::<ClientFrame>(r#"{"kind":"hello","value":{"sessionId":"00000000-0000-0000-0000-000000000001","schemaSupport":{"maxVersion":1},"peer":"forged"}}"#).is_err());
}

#[test]
fn schema_admission_checks_nested_protected_identifiers_and_statement_forms() {
    assert!(admit_schema_statement("CREATE TABLE example(id TEXT PRIMARY KEY)").is_ok());
    assert!(
        admit_schema_statement("INSERT INTO example SELECT * FROM _fleet_sync_sessions").is_err()
    );
    assert!(
        admit_schema_statement(
            "CREATE TRIGGER changed AFTER INSERT ON example BEGIN DELETE FROM example; END"
        )
        .is_err()
    );
    assert!(admit_schema_statement("CREATE TABLE one(id); CREATE TABLE two(id)").is_err());
    assert!(reserved_namespace("_FLEET_SYNC_sessions"));
    assert!(!reserved_namespace("consumer_rows"));
}

#[test]
fn schema_fingerprint_ignores_engine_tables_and_canonicalizes_quoting() {
    let plain = SchemaEntry {
        kind: SchemaObjectKind::Table,
        name: "example".into(),
        sql: "CREATE TABLE example(id TEXT PRIMARY KEY)".into(),
    };
    let quoted = SchemaEntry {
        sql: "CREATE TABLE \"example\" (\"id\" TEXT PRIMARY KEY)".into(),
        ..plain.clone()
    };
    let internal = SchemaEntry {
        name: "_fleet_sync_sessions".into(),
        sql: "CREATE TABLE _fleet_sync_sessions(id TEXT)".into(),
        ..plain
    };
    assert_eq!(
        schema_fingerprint(&[plain]).unwrap(),
        schema_fingerprint(&[quoted, internal]).unwrap()
    );
}

#[test]
fn hook_refusal_codes_validate_on_decode_and_preserve_valid_reasons() {
    let valid = serde_json::json!({"code":"consumerRejected","detail":"hook rejected"});
    let refusal: HookRefusal = serde_json::from_value(valid.clone()).unwrap();
    assert_eq!(serde_json::to_value(refusal).unwrap(), valid);
    let nested = serde_json::json!({"kind":"hook","value":valid});
    let refusal: PieceRefusal = serde_json::from_value(nested.clone()).unwrap();
    assert_eq!(serde_json::to_value(refusal).unwrap(), nested);
    assert!(
        serde_json::from_value::<HookRefusal>(
            serde_json::json!({"code":"","detail":"hook rejected"})
        )
        .is_err()
    );
    assert!(
        serde_json::from_value::<PieceRefusal>(
            serde_json::json!({"kind":"hook","value":{"code":"","detail":"hook rejected"}})
        )
        .is_err()
    );
}
