use fleet_sync_protocol::*;
use serde_json::json;

const ITEM: &str = "01900000-0000-7000-8000-000000000001";
const PEER: &str = "019eef00-0000-7000-8000-000000000002";
const FRAME: &str = "019eef00-0000-7000-8000-000000000004";
const DIGEST: &str = "bd00f74cc22f8538a42a06a5227faf5e6b323622380dc9c61a3ad79ac72f3a4b";

#[test]
fn push_server_frame_and_all_client_receipts_match_literal_contracts() {
    let wire = json!({"kind":"request","value":{"frame":FRAME,"body":{"kind":"crate","value":{"kind":"push","value":{
        "call":{"operation":"019eef00-0000-7000-8000-000000000003","step":"push"},
        "item":{"kind":"item","value":{"id":ITEM,"sender":null,"recipient":{"peer":PEER,"address":null},"validUntil":null,"body":null,"digest":DIGEST}}
    }}}}});
    let frame: ServerFrame = serde_json::from_value(wire.clone()).unwrap();
    assert!(
        matches!(&frame,ServerFrame::Request {body:ServerRequest::Crate(ServerCall::Push {item,..}),..} if item.id().as_uuid().to_string()==ITEM)
    );
    assert_eq!(serde_json::to_value(&frame).unwrap(), wire);
    for receipt in [
        json!({"kind":"accepted","value":{"acceptedAt":12345}}),
        json!({"kind":"replayed","value":{"status":{"kind":"held"},"cancel":null}}),
        json!({"kind":"conflict","value":{"existing":DIGEST}}),
        json!({"kind":"refused","value":{"reason":{"kind":"notThisMachine"}}}),
        json!({"kind":"refused","value":{"reason":{"kind":"mailboxFull"}}}),
        json!({"kind":"refused","value":{"reason":{"kind":"tooOld"}}}),
    ] {
        let wire = json!({"kind":"answer","value":{"frame":FRAME,"body":{"kind":"crate","value":{"kind":"pushed","value":receipt}}}});
        let answer: ClientFrame = serde_json::from_value(wire.clone()).unwrap();
        assert!(matches!(
            answer,
            ClientFrame::Answer {
                body: ClientAnswer::Crate(ServerCallAnswer::Pushed(_)),
                ..
            }
        ));
        assert_eq!(serde_json::to_value(answer).unwrap(), wire);
    }
}

#[test]
fn report_acknowledgements_and_sequence_free_submit_are_strict() {
    for result in [
        json!({"kind":"ack","value":{"reportSeq":2}}),
        json!({"kind":"replayed","value":{"reportSeq":1}}),
        json!({"kind":"refused","value":{"reason":{"kind":"notRecipient"}}}),
    ] {
        let wire = json!({"kind":"delivery","value":{"kind":"report","value":result}});
        let answer: OperationAnswer = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(answer).unwrap(), wire);
    }
    let accepted =
        json!({"kind":"accepted","value":{"status":{"kind":"sent"},"version":1,"cancel":null}});
    let result: SubmitResult<()> = serde_json::from_value(accepted.clone()).unwrap();
    assert_eq!(serde_json::to_value(result).unwrap(), accepted);
    let mut old = accepted;
    old["value"]["seq"] = json!(1);
    assert!(serde_json::from_value::<SubmitResult<()>>(old).is_err());
    assert!(
        serde_json::from_value::<DeliveryStatus<()>>(json!({"kind":"sent","value":{"seq":1}}))
            .is_err()
    );
    assert!(serde_json::from_value::<ReportResult>(json!({"kind":"applied","value":{"item":ITEM,"acknowledged":2,"status":{"kind":"held"},"version":3}})).is_err());
}
