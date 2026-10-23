#![allow(clippy::unwrap_used)]
use fleet_sync_protocol::*;

#[test]
fn release_refusal_and_fence_results_keep_their_typed_details() {
    let refusal: ReleaseAnswer = serde_json::from_str(r#"{"kind":"releaseRefused","value":{"kind":"migrationPending","value":{"migration":9007199254740993,"state":"pending"}}}"#).unwrap();
    assert!(matches!(
        refusal,
        ReleaseAnswer::ReleaseRefused(ReleaseRefusal::MigrationPending {
            migration: 9_007_199_254_740_993,
            state: MigrationState::Pending
        })
    ));
    let fence: FenceResult = serde_json::from_str(
        r#"{"kind":"previewStale","value":{"acked":{"epoch":2,"sequence":9007199254740993}}}"#,
    )
    .unwrap();
    assert!(
        matches!(fence, FenceResult::PreviewStale { acked } if acked.epoch.value() == 2 && acked.sequence.value() == 9_007_199_254_740_993)
    );
    assert!(serde_json::from_str::<FenceResult>(r#"{"kind":"fenced","value":{"oldEpoch":0,"newEpoch":2,"start":{"epoch":1,"sequence":0}}}"#).is_err());
}

#[test]
fn takeover_preview_carries_content_fence_and_optional_last_seen_without_clock_expiry() {
    for last_seen in [serde_json::Value::Null, serde_json::json!(1234)] {
        let wire = serde_json::json!({
            "token": "019eef00-0000-7000-8000-000000000001", "fence": 2,
            "acked": {"epoch": 2, "sequence": 7},
            "holder": "019eef00-0000-7000-8000-000000000003",
            "lastSeen": last_seen
        });
        let preview: TakeoverPreview = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(preview).unwrap(), wire);
    }
}

#[test]
fn release_supersession_and_request_conflicts_are_typed_answers() {
    for wire in [
        serde_json::json!({"kind":"requestConflict"}),
        serde_json::json!({"kind":"releaseRefused","value":{"kind":"superseded","value":{"current":3}}}),
    ] {
        let answer: ReleaseAnswer = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(answer).unwrap(), wire);
    }
}

#[test]
fn holder_change_epoch_unprovable_round_trips_as_a_typed_refusal() {
    let refusal_wire = r#"{"kind":"epochUnprovable"}"#;
    let answer_wire = r#"{"kind":"refused","value":{"kind":"epochUnprovable"}}"#;

    let refusal: HolderChangeRefusal = serde_json::from_str(refusal_wire).unwrap();
    let answer: HolderChangeAnswer = serde_json::from_str(answer_wire).unwrap();

    assert_eq!(serde_json::to_string(&refusal).unwrap(), refusal_wire);
    assert_eq!(serde_json::to_string(&answer).unwrap(), answer_wire);
}

#[test]
fn cancellation_and_stale_holder_answers_preserve_their_wire_details() {
    for wire in [
        serde_json::json!({"kind":"cancelled","value":{"by":"admin-subject"}}),
        serde_json::json!({"kind":"refused","value":{"kind":"holderStale","value":{"lastRenewedAt":1234}}}),
    ] {
        let answer: HolderChangeAnswer = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(answer).unwrap(), wire);
    }
    let settlement = serde_json::json!({"kind":"cancelled"});
    let answer: SettleOutcome = serde_json::from_value(settlement.clone()).unwrap();
    assert_eq!(serde_json::to_value(answer).unwrap(), settlement);
    let refusal = serde_json::json!({"kind":"releaseRefused","value":{"kind":"withdrawn"}});
    let answer: ReleaseAnswer = serde_json::from_value(refusal.clone()).unwrap();
    assert_eq!(serde_json::to_value(answer).unwrap(), refusal);
}

#[test]
fn withdrawal_server_call_and_answers_keep_call_and_transfer_identity() {
    let request = serde_json::json!({"kind":"withdrawRelease","value":{
        "call":{"operation":"019f0000-0000-7000-8000-000000000001","step":"withdraw"},
        "request":{"database":"019f0000-0000-7000-8000-000000000002","epoch":3,
            "transfer":"019f0000-0000-7000-8000-000000000003"}
    }});
    let call: ServerCall = serde_json::from_value(request.clone()).unwrap();
    assert_eq!(serde_json::to_value(call).unwrap(), request);
    for kind in ["withdrawn", "notHolder"] {
        let wire = serde_json::json!({"kind":"withdrawRelease","value":{"kind":kind}});
        let answer: ServerCallAnswer = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(answer).unwrap(), wire);
    }
}

#[test]
fn cancelled_actor_is_bounded_in_utf8_bytes_at_wire_decode() {
    for actor in ["a".repeat(256), "é".repeat(128)] {
        let wire = serde_json::json!({"kind":"cancelled","value":{"by":actor}});
        let answer: HolderChangeAnswer = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(answer).unwrap(), wire);
    }
    for actor in [String::new(), "a".repeat(257), "é".repeat(129)] {
        let wire = serde_json::json!({"kind":"cancelled","value":{"by":actor}});
        assert!(serde_json::from_value::<HolderChangeAnswer>(wire).is_err());
    }
}
