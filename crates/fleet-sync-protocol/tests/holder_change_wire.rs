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
            "token": "preview-token", "fence": 2,
            "acked": {"epoch": 2, "sequence": 7},
            "holder": "00000000-0000-0000-0000-000000000003",
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
