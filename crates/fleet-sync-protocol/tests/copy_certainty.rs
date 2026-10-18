#![allow(clippy::panic_in_result_fn)]
use fleet_sync_protocol::*;

#[test]
fn closed_retry_codec_keeps_the_entire_reason_and_rejects_unrelated_or_unknown_codes()
-> Result<(), Box<dyn std::error::Error>> {
    for (reason, expected_code, expected_detail) in [
        (
            CopyUnavailableReason::Uninitialized,
            "copy_uninitialized",
            "",
        ),
        (CopyUnavailableReason::StorageFull, "copy_storage_full", ""),
        (
            CopyUnavailableReason::Damaged {
                detail: "row codec failed".into(),
            },
            "copy_damaged",
            "row codec failed",
        ),
    ] {
        let (code, detail) = reason.encode_retry();
        assert_eq!((&*code, &*detail), (expected_code, expected_detail));
        assert_eq!(CopyUnavailableReason::decode_retry(&code, &detail)?, reason);
        for unknown in ["", "not_copy_unavailable", "fleet_sync_copy_unavailable"] {
            assert!(CopyUnavailableReason::decode_retry(unknown, &detail).is_err());
        }
    }
    for unit_code in ["copy_uninitialized", "copy_storage_full"] {
        assert!(CopyUnavailableReason::decode_retry(unit_code, "unexpected payload").is_err());
    }
    for diagnostic in ["{not-json", "copy_uninitialized", "", "row codec failed"] {
        assert_eq!(
            CopyUnavailableReason::decode_retry("copy_damaged", diagnostic)?,
            CopyUnavailableReason::Damaged {
                detail: diagnostic.into()
            }
        );
    }
    Ok(())
}

#[test]
fn outcome_unknown_is_closed_and_validates_its_bounded_diagnostic_at_decode()
-> Result<(), Box<dyn std::error::Error>> {
    let actual = ReplicationAnswer::OutcomeUnknown {
        reason: OutcomeUnknownReason::StoreBoundary {
            detail: DiagnosticText::new("commit reply lost")?,
        },
    };
    assert_eq!(
        serde_json::to_value(&actual)?,
        serde_json::json!({"kind":"outcomeUnknown","value":{"reason":{"kind":"storeBoundary","value":{"detail":"commit reply lost"}}}})
    );
    for detail in [String::new(), "界".repeat(1025)] {
        let wire = serde_json::json!({"kind":"outcomeUnknown","value":{"reason":{"kind":"storeBoundary","value":{"detail":detail}}}});
        assert!(serde_json::from_value::<ReplicationAnswer>(wire).is_err());
    }
    assert!(serde_json::from_value::<ReplicationAnswer>(serde_json::json!({"kind":"outcomeUnknown","value":{"reason":{"kind":"futureBoundary"}}})).is_err());
    assert_eq!(
        DiagnosticText::from_error("界".repeat(1200)).as_str(),
        "界".repeat(1024)
    );
    Ok(())
}
