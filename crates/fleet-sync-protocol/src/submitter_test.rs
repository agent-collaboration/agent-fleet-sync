#![allow(clippy::unwrap_used)]
use super::Submitter;

#[test]
fn fleet_submitter_json_is_canonical_and_round_trips() {
    let encoded = r#"{"kind":"fleet","value":"019eef00-0000-7000-8000-000000000001"}"#;
    let submitter: Submitter = serde_json::from_str(encoded).unwrap();
    assert_eq!(
        submitter.as_uuid().to_string(),
        "019eef00-0000-7000-8000-000000000001"
    );
    assert_eq!(serde_json::to_string(&submitter).unwrap(), encoded);
    for invalid in [
        r#"{"kind":"fleet","value":"019EEF00-0000-7000-8000-000000000001"}"#,
        r#"{"kind":"fleet","value":"019eef00-0000-4000-8000-000000000001"}"#,
        r#"{"kind":"fleet","value":"019eef00000070008000000000000001"}"#,
        r#"{"kind":"fleet","value":"019eef00-0000-7000-8000-000000000001","extra":true}"#,
    ] {
        assert!(
            serde_json::from_str::<Submitter>(invalid).is_err(),
            "accepted {invalid}"
        );
    }
}
