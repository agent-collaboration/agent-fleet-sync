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
    let unavailable: HolderChangeAnswer =
        serde_json::from_str(r#"{"kind":"holderUnreachable","value":{"lastSeen":null}}"#).unwrap();
    assert!(matches!(
        unavailable,
        HolderChangeAnswer::HolderUnreachable { last_seen: None }
    ));
}
