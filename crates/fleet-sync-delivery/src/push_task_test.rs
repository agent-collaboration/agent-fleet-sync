#![allow(clippy::unwrap_used)]
use super::*;

#[test]
fn decoded_no_capacity_is_retryable_and_never_a_terminal_push_answer() {
    let answer: PushResult<()> =
        serde_json::from_str(r#"{"kind":"refused","value":{"reason":{"kind":"mailboxFull"}}}"#)
            .unwrap();
    assert!(
        matches!(PushCall::<NoDelivery>::validated_answer(answer),Err(CallFailure::Retryable {reason}) if reason.code=="noCapacity")
    );
    assert!(
        matches!(PushCall::<NoDelivery>::validated_answer(PushResult::<()>::Accepted {accepted_at:SyncTime::from_unix_millis(7)}),Ok(PushResult::Accepted {accepted_at}) if accepted_at.unix_millis()==7)
    );
}

#[test]
fn unavailable_bindings_are_retryable_answers_with_visible_constant_reason_codes() {
    for reason in [
        SenderBindingFailure::Unknown,
        SenderBindingFailure::Conflict,
        SenderBindingFailure::Unavailable,
    ] {
        assert!(
            matches!(PushCall::<NoDelivery>::validated_answer(PushResult::<()>::BindingUnavailable {reason}),Err(CallFailure::Retryable {reason:stored}) if stored.code==reason.code())
        );
    }
}

#[test]
fn unavailable_submit_binding_is_retryable_without_a_cached_submission_answer() {
    for reason in [
        SenderBindingFailure::Unknown,
        SenderBindingFailure::Conflict,
        SenderBindingFailure::Unavailable,
    ] {
        assert!(
            matches!(SubmitCall::<NoDelivery>::validated_answer(DispatchOutcome::Answered(SubmitResult::<()>::BindingUnavailable {reason})),Err(CallFailure::Retryable {reason:stored}) if stored.code==reason.code())
        );
    }
}
