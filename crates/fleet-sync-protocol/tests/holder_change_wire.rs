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
