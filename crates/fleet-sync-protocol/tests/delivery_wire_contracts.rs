#![allow(clippy::unwrap_used)]
use fleet_sync_protocol::{OperationAnswer, OperationRequest, ServerFrame};
use serde_json::{Value, json};

const ITEM: &str = "01900000-0000-7000-8000-000000000001";
const PEER: &str = "00000000-0000-0000-0000-000000000002";

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
        json!({"kind":"submit","value":{"kind":"accepted","value":{"seq":1,"version":1}}}),
        json!({"kind":"fetch","value":{"stream":"controls","entries":[{"kind":"retired","value":{"seq":2,"items":[ITEM]}}],"through":2}}),
        json!({"kind":"received","value":{"stream":"controls","releasedThrough":2}}),
        json!({"kind":"report","value":{"item":ITEM,"acknowledged":2,"status":{"kind":"delivered","value":{"acceptance":null}},"version":4}}),
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
