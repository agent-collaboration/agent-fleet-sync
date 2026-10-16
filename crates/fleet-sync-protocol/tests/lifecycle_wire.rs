#![allow(clippy::unwrap_used)]
use fleet_sync_protocol::*;

#[test]
fn lifecycle_refusals_keep_gate_details_and_validate_codes() {
    let answer: RegistrationAnswer = serde_json::from_str(r#"{"kind":"refused","value":{"kind":"databaseRejected","value":{"reason":{"code":"consumerRejected","detail":"not admitted"}}}}"#).unwrap();
    assert!(
        matches!(answer, RegistrationAnswer::Refused(RegistrationRefusal::DatabaseRejected { reason }) if reason.code.as_str() == "consumerRejected" && reason.detail == "not admitted")
    );
    assert!(serde_json::from_str::<GateReason>(r#"{"code":"","detail":"invalid"}"#).is_err());
    assert!(serde_json::from_str::<SeedId>(r#""00000000-0000-0000-0000-000000000000""#).is_err());
    assert!(serde_json::from_str::<PageDigest>(r#""not-a-digest""#).is_err());
}

#[test]
fn incomplete_seed_and_failed_holder_change_have_typed_outcomes() {
    let answer: SeedAnswer =
        serde_json::from_str(r#"{"kind":"incomplete","value":{"missing":[0,3,7]}}"#).unwrap();
    assert_eq!(
        answer,
        SeedAnswer::Incomplete {
            missing: vec![0, 3, 7]
        }
    );
    let failure: HolderChangeAnswer =
        serde_json::from_str(r#"{"kind":"failed","value":{"reason":"versionMismatch"}}"#).unwrap();
    assert!(
        matches!(failure, HolderChangeAnswer::Failed { reason } if reason.as_str() == "versionMismatch")
    );
    let unavailable: HolderChangeAnswer = serde_json::from_str(
        r#"{"kind":"refused","value":{"kind":"holderUnreachable","value":{"lastSeen":null}}}"#,
    )
    .unwrap();
    assert!(matches!(
        unavailable,
        HolderChangeAnswer::Refused(HolderChangeRefusal::HolderUnreachable { last_seen: None })
    ));
}

#[test]
fn independent_table_digests_match_and_column_or_row_reordering_changes_them() {
    let rows = [
        vec![SqlValue::Text("first".into()), SqlValue::Integer(i64::MAX)],
        vec![
            SqlValue::Text("second".into()),
            SqlValue::Blob(vec![0, 255]),
        ],
    ];
    let mut writer = TableDigestBuilder::new();
    let mut copy = TableDigestBuilder::new();
    for row in &rows {
        writer.push_row(row).unwrap();
        copy.push_row(row).unwrap();
    }
    let expected = writer.finish();
    assert_eq!(copy.finish(), expected);
    let mut columns_reordered = TableDigestBuilder::new();
    for row in &rows {
        columns_reordered
            .push_row(&row.iter().cloned().rev().collect::<Vec<_>>())
            .unwrap();
    }
    assert_ne!(columns_reordered.finish(), expected);
    let mut rows_reordered = TableDigestBuilder::new();
    for row in rows.iter().rev() {
        rows_reordered.push_row(row).unwrap();
    }
    assert_ne!(rows_reordered.finish(), expected);
    assert_ne!(TableDigestBuilder::new().finish(), expected);
}

#[test]
fn copy_origin_distinguishes_expected_registration_position_from_unpositioned_import() {
    let position = ReplicationPosition::new(Epoch::new(1).unwrap(), CommitSequence::new(8));
    for expected_position in [Some(position), None] {
        let origin = CopyOrigin::AwaitingSeed { expected_position };
        let wire = serde_json::to_value(&origin).unwrap();
        assert_eq!(wire["kind"], "awaitingSeed");
        assert!(wire["value"].get("expectedPosition").is_some());
        assert_eq!(serde_json::from_value::<CopyOrigin>(wire).unwrap(), origin);
    }
    let answer = SeedAnswer::PositionMismatch {
        expected: position,
        offered: ReplicationPosition::new(Epoch::new(1).unwrap(), CommitSequence::new(9)),
    };
    assert_eq!(
        serde_json::from_value::<SeedAnswer>(serde_json::to_value(&answer).unwrap()).unwrap(),
        answer
    );
}
