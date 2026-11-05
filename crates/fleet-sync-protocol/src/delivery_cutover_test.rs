#![allow(clippy::unwrap_used)]
use crate::*;
use serde_json::json;

#[test]
fn removed_mailbox_operations_are_rejected() {
    let call = json!({"operation":"019eef00-0000-7000-8000-000000000003","step":"delivery"});
    for value in [
        json!({"kind":"fetch","value":{"call":call,"stream":"items","after":0,"maxItems":1,"maxBytes":1000}}),
        json!({"kind":"received","value":{"call":call,"stream":"items","through":1}}),
        json!({"kind":"retire","value":{"call":call,"mailbox":"019eef00-0000-7000-8000-000000000002","items":[]}}),
    ] {
        assert!(
            serde_json::from_value::<DeliveryRequest<NoDelivery>>(value).is_err(),
            "a removed Mailbox fetch operation must not remain a wire contract"
        );
    }
}

#[test]
fn unavailable_answer_uses_delivery_name_and_rejects_old_mailbox_name() {
    let value = json!({"kind":"deliveryUnavailable","value":{"reason":{"kind":"mailboxFull"}}});
    let answer = serde_json::from_value::<DeliveryAnswer<NoDelivery>>(value.clone());
    assert!(
        answer.is_ok(),
        "delivery availability has a role-neutral wire name"
    );
    assert_eq!(serde_json::to_value(answer.unwrap()).unwrap(), value);
    let old = json!({"kind":"mailboxUnavailable","value":{"reason":{"kind":"mailboxFull"}}});
    assert!(
        serde_json::from_value::<DeliveryAnswer<NoDelivery>>(old).is_err(),
        "the hard cutover has no old-name alias"
    );
}
