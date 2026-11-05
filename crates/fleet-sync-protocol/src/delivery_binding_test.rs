#![allow(clippy::unwrap_used)]
use super::*;
use crate::{PushResult, SubmitResult};

#[test]
fn binding_unavailability_is_a_closed_non_refusal_wire_result_with_visible_codes() {
    for (reason, label, code) in [
        (
            SenderBindingFailure::Unknown,
            "unknown",
            "senderBindingUnknown",
        ),
        (
            SenderBindingFailure::Conflict,
            "conflict",
            "senderBindingConflict",
        ),
        (
            SenderBindingFailure::Unavailable,
            "unavailable",
            "senderBindingUnavailable",
        ),
    ] {
        assert_eq!(reason.code(), code);
        let expected = serde_json::json!({"kind":"bindingUnavailable","value":{"reason":label}});
        assert_eq!(
            serde_json::to_value(PushResult::<()>::BindingUnavailable { reason }).unwrap(),
            expected
        );
        assert_eq!(
            serde_json::to_value(SubmitResult::<()>::BindingUnavailable { reason }).unwrap(),
            expected
        );
        assert!(
            matches!(serde_json::from_value::<PushResult<()>>(expected).unwrap(),PushResult::BindingUnavailable {reason:stored} if stored==reason)
        );
    }
    assert!(serde_json::from_str::<SenderBindingFailure>("\"other\"").is_err());
}
