#![allow(clippy::unwrap_used)]
use crate::*;
fn wire(claim: Option<serde_json::Value>) -> serde_json::Value {
    let mut value = serde_json::json!({"kind":"settle","value":{
        "call":{"operation":"019eef00-0000-7000-8000-000000000077","step":"settle"},
        "mailbox":"019eef00-0000-7000-8000-000000000002",
        "item":"019eef00-0000-7000-8000-000000000001",
        "digest":"a".repeat(64)
    }});
    if let Some(claim) = claim {
        value
            .pointer_mut("/value")
            .and_then(serde_json::Value::as_object_mut)
            .unwrap()
            .insert("submitter".into(), claim);
    }
    value
}
#[test]
fn settlement_preserves_machine_and_project_claims_on_the_wire() {
    for kind in ["machine", "project"] {
        let value = wire(Some(
            serde_json::json!({"kind":kind,"value":"019eef00-0000-7000-8000-000000000009"}),
        ));
        let decoded = serde_json::from_value::<DeliveryRequest<NoDelivery>>(value.clone());
        assert!(decoded.is_ok(), "Settle must carry its typed {kind} claim");
        assert_eq!(serde_json::to_value(decoded.unwrap()).unwrap(), value);
    }
}
#[test]
fn settlement_without_a_claim_is_rejected_in_the_hard_cutover() {
    assert!(
        serde_json::from_value::<DeliveryRequest<NoDelivery>>(wire(None)).is_err(),
        "a claim-less Settle must not infer Machine from its caller"
    );
}
