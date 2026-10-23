#![allow(clippy::unwrap_used)]
use fleet_sync_protocol::*;

#[test]
fn host_migration_version_round_trips_through_wire_and_the_single_retry_codec() {
    for version in [0, 7, u64::MAX] {
        let reason = CopyUnavailableReason::MigrationPending { version };
        let wire = serde_json::json!({"kind":"copyUnavailable","value":{"reason":{"kind":"migrationPending","value":{"version":version}}}});
        let answer = ReplicationAnswer::CopyUnavailable {
            reason: reason.clone(),
        };
        assert_eq!(serde_json::to_value(answer).unwrap(), wire);
        let decoded: ReplicationAnswer = serde_json::from_value(wire).unwrap();
        assert!(
            matches!(decoded, ReplicationAnswer::CopyUnavailable { reason:actual } if actual==reason)
        );
        let (code, detail) = reason.encode_retry();
        assert_eq!(code, "copy_migration_pending");
        assert_eq!(detail, version.to_string());
        assert_eq!(
            CopyUnavailableReason::decode_retry(&code, &detail).unwrap(),
            reason
        );
    }
}

#[test]
fn malformed_migration_versions_are_rejected_by_wire_and_retry_codec() {
    for detail in [
        "",
        "-1",
        "+7",
        "07",
        " 7",
        "7 ",
        "7.0",
        "\"7\"",
        "null",
        "18446744073709551616",
        "{}",
    ] {
        assert!(
            CopyUnavailableReason::decode_retry("copy_migration_pending", detail).is_err(),
            "accepted malformed version {detail}"
        );
    }
    for value in [
        serde_json::json!({}),
        serde_json::json!({"version":-1}),
        serde_json::json!({"version":"7"}),
        serde_json::json!({"version":7.5}),
        serde_json::json!({"version":7,"extra":true}),
    ] {
        assert!(
            serde_json::from_value::<CopyUnavailableReason>(
                serde_json::json!({"kind":"migrationPending","value":value})
            )
            .is_err()
        );
    }
}
