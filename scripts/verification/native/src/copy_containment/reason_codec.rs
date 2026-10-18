//! Pure closed-codec controls; these do not establish a host's transaction certainty.
use super::*;

pub(super) fn observe(observations: &mut Observations) -> TestResult {
    for (index, (reason, expected_code, expected_detail)) in [
        (
            CopyUnavailableReason::Uninitialized,
            "copy_uninitialized",
            "",
        ),
        (CopyUnavailableReason::StorageFull, "copy_storage_full", ""),
        (
            CopyUnavailableReason::Damaged {
                detail: "complete damaged-copy context".into(),
            },
            "copy_damaged",
            "complete damaged-copy context",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let (code, detail) = reason.encode_retry();
        let decoded = CopyUnavailableReason::decode_retry(&code, &detail);
        let matches_reason = decoded.as_ref().is_ok_and(|actual| actual == &reason);
        let observed = match decoded {
            Ok(actual) => json!(actual),
            Err(error) => json!({"error":error.to_string()}),
        };
        observations.check(
            &format!("copy-reason-codec-roundtrip-{index}"),
            code == expected_code && detail == expected_detail && matches_reason,
            json!({"code":expected_code,"detail":expected_detail,"reason":reason}),
            json!({"code":code,"detail":detail,"reason":observed}),
        );
        let refused = ["", "unrelated_reason", "fleet_sync_copy_unavailable"]
            .into_iter()
            .all(|unknown| CopyUnavailableReason::decode_retry(unknown, &detail).is_err());
        observations.check(
            &format!("copy-reason-codec-rejects-unknown-{index}"),
            refused,
            json!({"unknownCode":"rejected"}),
            json!({"rejected":refused}),
        );
    }
    let units_reject_payload = ["copy_uninitialized", "copy_storage_full"]
        .into_iter()
        .all(|code| CopyUnavailableReason::decode_retry(code, "unexpected payload").is_err());
    observations.check(
        "unit-copy-reasons-reject-payload",
        units_reject_payload,
        json!({"unexpectedPayload":"rejected"}),
        json!({"rejected":units_reject_payload}),
    );
    for (index, diagnostic) in [
        "{not-json",
        "copy_uninitialized",
        "",
        "complete damaged-copy context",
    ]
    .into_iter()
    .enumerate()
    {
        let decoded = CopyUnavailableReason::decode_retry("copy_damaged", diagnostic);
        let keeps_diagnostic = decoded.as_ref().is_ok_and(|actual| {
            actual
                == &CopyUnavailableReason::Damaged {
                    detail: diagnostic.into(),
                }
        });
        let observed = match decoded {
            Ok(actual) => json!(actual),
            Err(error) => json!({"error":error.to_string()}),
        };
        observations.check(
            &format!("copy-detail-is-not-a-discriminator-{index}"),
            keeps_diagnostic,
            json!({"kind":"damaged","detail":diagnostic}),
            observed,
        );
    }
    let unknown = json!({"kind":"outcomeUnknown","value":{"reason":{"kind":"futureBoundary"}}});
    let rejected = serde_json::from_value::<ReplicationAnswer>(unknown.clone()).is_err();
    observations.check(
        "outcome-reason-codec-is-closed",
        rejected,
        json!({"unknownKind":"rejected"}),
        json!({"wire":unknown,"rejected":rejected}),
    );
    Ok(())
}
