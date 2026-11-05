#![expect(
    clippy::panic_in_result_fn,
    reason = "Assertions verify the exact refusal/retry contract; setup errors remain Results."
)]
use super::*;
use fleet_sync_protocol::{
    GateReason, GateReasonCode, HookRefusal, HookRefusalCode, PieceRefusal, RegistrationRefusal,
};
type TestResult = Result<(), Box<dyn std::error::Error>>;
fn gate(code: &str) -> Result<RegistrationAnswer, Box<dyn std::error::Error>> {
    Ok(RegistrationAnswer::Refused(
        RegistrationRefusal::DatabaseRejected {
            reason: GateReason {
                code: GateReasonCode::new(code)?,
                detail: "must keep reason".into(),
            },
        },
    ))
}
#[test]
fn registration_enrollment_refusal_is_retryable_before_answer_storage() -> TestResult {
    assert!(
        matches!(registration(gate("callerNotEnrolled")?),Err(CallFailure::Retryable{reason}) if reason.code=="callerNotEnrolled")
    );
    Ok(())
}
#[test]
fn renewal_enrollment_refusal_is_retryable_before_answer_storage() -> TestResult {
    let answer = serde_json::from_value(
        serde_json::json!({"kind":"refused","value":{"kind":"holderNotEnrolled"}}),
    )?;
    assert!(
        matches!(renewal(answer),Err(CallFailure::Retryable{reason}) if reason.code=="callerNotEnrolled")
    );
    Ok(())
}
#[test]
fn replication_enrollment_refusal_is_retryable_without_closing_seed() -> TestResult {
    let answer = ReplicationAnswer::Refused(PieceRefusal::Hook(HookRefusal {
        code: HookRefusalCode::new("callerNotEnrolled")?,
        detail: "must keep generation".into(),
    }));
    assert!(
        matches!(replication(answer),Err(CallFailure::Retryable{reason}) if reason.code=="callerNotEnrolled")
    );
    Ok(())
}
#[test]
fn other_refusals_remain_terminal_typed_answers() -> TestResult {
    let original = gate("databaseDenied")?;
    let expected = serde_json::to_value(&original)?;
    assert_eq!(registration(original)?, expected);
    let original = ReplicationAnswer::Refused(PieceRefusal::Hook(HookRefusal {
        code: HookRefusalCode::new("schemaRefused")?,
        detail: "unaltered".into(),
    }));
    let expected = serde_json::to_value(&original)?;
    assert_eq!(serde_json::to_value(replication(original)?)?, expected);
    Ok(())
}
