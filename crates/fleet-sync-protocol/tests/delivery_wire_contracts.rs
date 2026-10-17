#![allow(clippy::unwrap_used)]
use fleet_sync_protocol::{OperationAnswer, OperationRequest, ServerFrame};
use serde_json::{Value, json};

const ITEM: &str = "01900000-0000-7000-8000-000000000001";
const PEER: &str = "00000000-0000-0000-0000-000000000002";

#[test]
fn an_unavailable_mailbox_is_distinct_from_an_item_refusal() {
    let wire = json!({"kind":"delivery","value":{"kind":"mailboxUnavailable","value":{
        "reason":{"kind":"processingFailed","value":{"kind":"custodyCorrupt"}}
    }}});
    let decoded = serde_json::from_value::<OperationAnswer>(wire.clone());
    assert!(
        decoded.is_ok(),
        "decision83 unavailability must decode as a request answer"
    );
    assert_eq!(serde_json::to_value(decoded.unwrap()).unwrap(), wire);
    let mut obsolete = wire;
    obsolete["value"]["kind"] = json!("refused");
    assert!(serde_json::from_value::<OperationAnswer>(obsolete).is_err());
}

fn call() -> Value {
    json!({"operation":"00000000-0000-0000-0000-000000000003","step":"delivery"})
}

// The oracle is decision 47's eight request bodies, not a production encoder.
#[test]
fn all_eight_delivery_request_bodies_cross_the_operation_envelope() {
    let requests = [
        json!({"kind":"submit","value":{"call":call(),"item":{"kind":"item","value":{
            "id":ITEM,"sender":null,"recipient":{"peer":PEER,"address":null},
            "validUntil":null,"body":null,
            "digest":"9f53c2e6152e4beca49916373f25f8e55daccea42063cfde30b00688eae644ec"
        }}}}),
        json!({"kind":"fetch","value":{"call":call(),"stream":"items","after":0,"maxItems":4,"maxBytes":4096}}),
        json!({"kind":"received","value":{"call":call(),"stream":"controls","through":9}}),
        json!({"kind":"report","value":{"call":call(),"report":{"item":ITEM,"reportSeq":2,"outcome":{"kind":"delivered","value":{"acceptance":null}}}}}),
        json!({"kind":"statusFetch","value":{"call":call(),"after":7,"limit":4}}),
        json!({"kind":"query","value":{"call":call(),"mailbox":PEER,"items":[ITEM]}}),
        json!({"kind":"retire","value":{"call":call(),"mailbox":PEER,"items":[ITEM]}}),
        json!({"kind":"settle","value":{"call":call(),"mailbox":PEER,"item":ITEM,"digest":"9f53c2e6152e4beca49916373f25f8e55daccea42063cfde30b00688eae644ec"}}),
    ];
    let mut failures = Vec::new();
    for request in requests {
        let wire = json!({"kind":"delivery","value":request});
        match serde_json::from_value::<OperationRequest>(wire.clone()) {
            Ok(decoded) => assert_eq!(serde_json::to_value(decoded).unwrap(), wire),
            Err(error) => failures.push(format!("{}: {error}", wire["value"]["kind"])),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn all_eight_delivery_answers_preserve_their_typed_bodies() {
    let answers = [
        json!({"kind":"submit","value":{"kind":"accepted","value":{"seq":1,"status":{"kind":"sent","value":{"seq":1}},"version":1,"cancel":null}}}),
        json!({"kind":"fetch","value":{"stream":"controls","entries":[{"kind":"retired","value":{"seq":2,"items":[ITEM]}}],"through":2}}),
        json!({"kind":"received","value":{"stream":"controls","releasedThrough":2}}),
        json!({"kind":"report","value":{"kind":"applied","value":{"item":ITEM,"acknowledged":2,"status":{"kind":"delivered","value":{"acceptance":null}},"version":4}}}),
        json!({"kind":"statusFetch","value":{"events":[{"cursor":5,"item":ITEM,"status":{"kind":"held"},"version":3,"cancel":null}],"through":5}}),
        json!({"kind":"query","value":{"statuses":[{"item":ITEM,"state":{"kind":"known","value":{"digest":"9f53c2e6152e4beca49916373f25f8e55daccea42063cfde30b00688eae644ec","status":{"kind":"held"},"version":3,"cancel":null}}}]}}),
        json!({"kind":"retire","value":{"retired":[ITEM],"unknown":[]}}),
        json!({"kind":"settle","value":{"kind":"settled","value":{"reason":"custodyUnresolvable","version":1}}}),
        json!({"kind":"settle","value":{"kind":"replayed","value":{"status":{"kind":"held"},"version":3,"cancel":null}}}),
        json!({"kind":"settle","value":{"kind":"itemConflict","value":{"existing":"9f53c2e6152e4beca49916373f25f8e55daccea42063cfde30b00688eae644ec"}}}),
    ];
    let mut failures = Vec::new();
    for answer in answers {
        let wire = json!({"kind":"delivery","value":answer});
        match serde_json::from_value::<OperationAnswer>(wire.clone()) {
            Ok(decoded) => assert_eq!(serde_json::to_value(decoded).unwrap(), wire),
            Err(error) => failures.push(format!("{}: {error}", wire["value"]["kind"])),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn status_publication_crosses_the_server_call_with_its_version() {
    let wire = json!({"kind":"request","value":{
        "frame":"00000000-0000-0000-0000-000000000004",
        "body":{"kind":"crate","value":{"kind":"publishStatus","value":{
            "call":call(),"publication":{"item":ITEM,"status":{"kind":"delivered","value":{"acceptance":null}},"version":4,"cancel":null}
        }}}
    }});
    let decoded: ServerFrame = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), wire);
}

#[test]
fn submit_cannot_choose_another_peers_status_stream() {
    let wire = json!({"kind":"delivery","value":{"kind":"submit","value":{"call":call(),"item":{"kind":"item","value":{
        "id":ITEM,"sender":null,"recipient":{"peer":PEER,"address":null},"validUntil":null,"body":null,
        "digest":"9f53c2e6152e4beca49916373f25f8e55daccea42063cfde30b00688eae644ec"
    }},"statusTarget":PEER}}});
    assert!(serde_json::from_value::<OperationRequest>(wire).is_err());
}

#[test]
fn typed_refusals_cross_report_and_publication_answers() {
    let report = json!({"kind":"delivery","value":{"kind":"report","value":{"kind":"refused","value":{"reason":{"kind":"tooLarge","value":{"field":"acceptance"}}}}}});
    let decoded: OperationAnswer = serde_json::from_value(report.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), report);
    for value in [
        json!({"kind":"applied"}),
        json!({"kind":"refused","value":{"reason":{"kind":"tooLarge","value":{"field":"acceptance"}}}}),
    ] {
        let wire = json!({"kind":"crate","value":{"kind":"published","value":value}});
        let decoded: fleet_sync_protocol::ClientAnswer =
            serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), wire);
    }
}

#[test]
fn processing_failure_kinds_are_closed_on_the_wire() {
    for kind in [
        "custodyMissing",
        "custodyUnresolvable",
        "custodyCorrupt",
        "digestMismatch",
        "versionMismatch",
        "nondeterministic",
        "invalidPayload",
        "executionFailed",
    ] {
        let wire = json!({"kind":"processingFailed","value":{"kind":kind}});
        let decoded: fleet_sync_protocol::DeliveryRefusal =
            serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), wire);
    }
    assert!(
        serde_json::from_value::<fleet_sync_protocol::DeliveryRefusal>(
            json!({"kind":"processingFailed","value":{"kind":"unknown"}})
        )
        .is_err()
    );
}

#[test]
fn delivery_header_preserves_identity_and_rejects_a_body() {
    let header = json!({"id":ITEM,"sender":null,"recipient":{"peer":PEER,"address":null},"validUntil":null,"digest":"9f53c2e6152e4beca49916373f25f8e55daccea42063cfde30b00688eae644ec"});
    let decoded: fleet_sync_protocol::DeliveryHeader<fleet_sync_protocol::NoDelivery> =
        serde_json::from_value(header.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), header);
    let mut with_body = header;
    with_body["body"] = json!("forbidden");
    assert!(
        serde_json::from_value::<
            fleet_sync_protocol::DeliveryHeader<fleet_sync_protocol::NoDelivery>,
        >(with_body)
        .is_err()
    );
}

#[test]
fn accepted_and_replayed_submit_receipts_carry_the_canonical_expired_snapshot() {
    for kind in ["accepted", "replayed"] {
        let mut snapshot = json!({"status":{"kind":"expired"},"version":1,"cancel":null});
        if kind == "accepted" {
            snapshot["seq"] = json!(1);
        }
        let wire = json!({"kind":"delivery","value":{"kind":"submit","value":{"kind":kind,"value":snapshot}}});
        let decoded: OperationAnswer = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), wire);
    }
    let old_sparse = json!({"kind":"delivery","value":{"kind":"submit","value":{"kind":"accepted","value":{"seq":1,"version":1}}}});
    assert!(serde_json::from_value::<OperationAnswer>(old_sparse).is_err());
}
