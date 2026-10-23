#![allow(clippy::unwrap_used)]
use fleet_sync_protocol::{
    DeliveryRefusal, DeliveryStatus, MAX_RECIPIENT_REFUSAL_CODE_BYTES, RecipientRefusalCode,
};
use serde_json::json;

#[test]
fn consumer_recipient_refusal_codes_round_trip_without_interpretation() {
    for code in [
        "unsupportedMode",
        "busy",
        "staleGuard",
        "permissionDenied",
        "ownedElsewhere",
        "rejectedUnclassified",
        &"R".repeat(64),
    ] {
        let wire = json!({"kind":"recipientRefused","value":{"code":code}});
        let refusal: DeliveryRefusal = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(&refusal).unwrap(), wire);
        let status = DeliveryStatus::<()>::Refused { reason: refusal };
        assert!(status.is_terminal());
        assert!(!status.may_advance_to(&DeliveryStatus::Delivered { acceptance: () }));
        assert_eq!(
            serde_json::to_value(status).unwrap(),
            json!({"kind":"refused","value":{"reason":wire}})
        );
    }
}

#[test]
fn recipient_refusal_decode_rejects_empty_missing_and_non_string_codes_and_detail() {
    for value in [
        json!({"code":""}),
        json!({}),
        json!({"code":null}),
        json!({"code":7}),
        json!({"code":{}}),
        json!({"code":"R".repeat(65)}),
        json!({"code":"not_valid"}),
        json!({"code":"non ascii é"}),
        json!({"code":"busy\n"}),
        json!({"code":"busy","detail":"not a protocol field"}),
    ] {
        assert!(
            serde_json::from_value::<DeliveryRefusal>(
                json!({"kind":"recipientRefused","value":value})
            )
            .is_err()
        );
    }
}

#[test]
fn recipient_refusal_constructor_and_decode_enforce_the_same_byte_boundary() {
    assert_eq!(MAX_RECIPIENT_REFUSAL_CODE_BYTES, 64);
    for code in ["a".to_owned(), "9".to_owned(), "Ab9".repeat(21) + "Z"] {
        let constructed = RecipientRefusalCode::new(code.clone()).unwrap();
        assert_eq!(constructed.as_str(), code);
        assert_eq!(
            serde_json::from_value::<RecipientRefusalCode>(json!(code)).unwrap(),
            constructed
        );
    }
    for code in [
        "".to_owned(),
        "a".repeat(65),
        "a_b".into(),
        "a-b".into(),
        "a b".into(),
        "a\n".into(),
        "é".into(),
    ] {
        assert!(
            RecipientRefusalCode::new(code.clone()).is_err(),
            "constructor accepted {code:?}"
        );
        assert!(
            serde_json::from_value::<RecipientRefusalCode>(json!(code)).is_err(),
            "decoder accepted {code:?}"
        );
    }
    let coded = DeliveryRefusal::RecipientRefused {
        code: RecipientRefusalCode::new("R".repeat(64)).unwrap(),
    };
    let conflict = DeliveryRefusal::ItemConflict {
        existing: fleet_sync_protocol::ItemDigest::try_from("f".repeat(64)).unwrap(),
    };
    assert_eq!(serde_json::to_vec(&coded).unwrap().len(), 111);
    assert_eq!(serde_json::to_vec(&conflict).unwrap().len(), 111);
}
