#![allow(clippy::unwrap_used)]
use fleet_sync_protocol::{HolderChangeReason, HolderChangeReasonError, HolderChangeRequest};
use serde_json::{Value, json};

fn request(kind: &str) -> Value {
    let mut value = json!({
        "attempt": "019eef00-0000-7000-8000-000000000001",
        "database": "019eef00-0000-7000-8000-000000000002",
        "to": "019eef00-0000-7000-8000-000000000003",
        "reason": "  maintenance: déplacer 🐒\n"
    });
    let fields = value.as_object_mut().unwrap();
    if kind == "move" {
        fields.insert("fromEpoch".into(), json!(1));
    } else {
        fields.insert(
            "preview".into(),
            json!({
                "token": "019eef00-0000-7000-8000-000000000004",
                "fence": 1,
                "acked": {"epoch": 1, "sequence": 0},
                "holder": "019eef00-0000-7000-8000-000000000005",
                "lastSeen": null
            }),
        );
    }
    json!({"kind":kind, "value":value})
}

#[test]
fn holder_change_reason_preserves_utf8_bytes_and_enforces_the_byte_bound() {
    for value in [
        "  maintenance: déplacer 🐒\n".to_owned(),
        "x".repeat(16 * 1024),
        "é".repeat(8 * 1024),
    ] {
        let reason = HolderChangeReason::new(value.clone()).unwrap();
        assert_eq!(reason.as_str(), value);
        assert_eq!(serde_json::to_value(reason).unwrap(), json!(value));
    }
    for value in [String::new(), "x".repeat(16 * 1024 + 1), "é".repeat(8193)] {
        assert_eq!(HolderChangeReason::new(value), Err(HolderChangeReasonError));
    }
}

#[test]
fn move_and_takeover_require_a_valid_reason_and_preserve_its_wire_value() {
    for kind in ["move", "takeover"] {
        let wire = request(kind);
        let decoded: HolderChangeRequest = serde_json::from_value(wire.clone()).unwrap();
        let encoded = serde_json::to_value(decoded).unwrap();
        assert_eq!(encoded["value"]["reason"], "  maintenance: déplacer 🐒\n");
        assert_eq!(encoded, wire);
        for invalid in [json!(""), json!("é".repeat(8193)), json!(null), json!(23)] {
            let mut invalid_wire = wire.clone();
            invalid_wire["value"]["reason"] = invalid;
            assert!(serde_json::from_value::<HolderChangeRequest>(invalid_wire).is_err());
        }
        let mut missing = wire;
        missing["value"].as_object_mut().unwrap().remove("reason");
        assert!(serde_json::from_value::<HolderChangeRequest>(missing).is_err());
    }
}

#[test]
fn machine_claims_carry_no_reason() {
    let wire = json!({"kind":"claim", "value":{
        "attempt":"019eef00-0000-7000-8000-000000000001",
        "database":"019eef00-0000-7000-8000-000000000002",
        "claimant":"019eef00-0000-7000-8000-000000000003"
    }});
    let decoded: HolderChangeRequest = serde_json::from_value(wire.clone()).unwrap();
    assert!(matches!(decoded, HolderChangeRequest::Claim { .. }));
    assert_eq!(serde_json::to_value(decoded).unwrap(), wire);
    let mut with_reason = wire;
    with_reason["value"]["reason"] = json!("machine requests have none");
    assert!(serde_json::from_value::<HolderChangeRequest>(with_reason).is_err());
}
