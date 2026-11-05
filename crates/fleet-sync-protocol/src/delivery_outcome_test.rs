#![allow(clippy::unwrap_used)]
use super::*;

#[test]
fn sender_not_enrolled_round_trips_as_a_unit_refusal() {
    let wire = r#"{"kind":"senderNotEnrolled"}"#;
    let refusal: DeliveryRefusal = serde_json::from_str(wire).unwrap();
    assert_eq!(refusal, DeliveryRefusal::SenderNotEnrolled);
    assert_eq!(serde_json::to_string(&refusal).unwrap(), wire);
    assert!(DeliveryStatus::<()>::Refused { reason: refusal }.is_terminal());
}
