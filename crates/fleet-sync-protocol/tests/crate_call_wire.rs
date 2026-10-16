#![allow(clippy::unwrap_used)]
use fleet_sync_protocol::*;
use uuid::Uuid;

#[test]
fn server_release_and_client_answer_use_the_canonical_call_identity() {
    let call: runtime_primitives::CallId = CallId {
        operation: OperationId(Uuid::from_u128(1)),
        step: StepName::new("release").unwrap(),
    };
    let frame_id = FrameId::new(Uuid::from_u128(2)).unwrap();
    let frame: ServerFrame = ServerFrame::Request {
        frame: frame_id,
        body: ServerRequest::Crate(ServerCall::Release {
            call: call.clone(),
            request: ReleaseRequest {
                database: DatabaseId::new(Uuid::from_u128(3)).unwrap(),
                epoch: Epoch::new(1).unwrap(),
                transfer: TransferId::new(Uuid::from_u128(4)).unwrap(),
            },
        }),
    };
    let wire = serde_json::to_value(&frame).unwrap();
    assert_eq!(wire["value"]["body"]["kind"], "crate");
    assert_eq!(wire["value"]["body"]["value"]["kind"], "release");
    let decoded: ServerFrame = serde_json::from_value(wire).unwrap();
    assert!(
        matches!(decoded, ServerFrame::Request { frame, body: ServerRequest::Crate(ServerCall::Release { call: decoded, .. }) } if frame == frame_id && decoded == call)
    );
    let answer: ClientFrame = ClientFrame::Answer {
        frame: frame_id,
        body: ClientAnswer::Crate(ServerCallAnswer::Release(ReleaseAnswer::Released {
            final_position: ReplicationPosition::new(
                Epoch::new(1).unwrap(),
                CommitSequence::new(7),
            ),
        })),
    };
    let decoded: ClientFrame =
        serde_json::from_slice(&serde_json::to_vec(&answer).unwrap()).unwrap();
    assert!(
        matches!(decoded, ClientFrame::Answer { frame, body: ClientAnswer::Crate(ServerCallAnswer::Release(ReleaseAnswer::Released { final_position })) } if frame == frame_id && final_position.sequence.value() == 7)
    );
}

#[test]
fn peer_session_lease_frames_exclude_administrative_operations() {
    for kind in [
        "move",
        "takeover",
        "previewTakeover",
        "import",
        "retire",
        "retirement",
    ] {
        let wire = serde_json::json!({"kind":"lease","value":{"kind":kind,"value":{}}});
        assert!(serde_json::from_value::<OperationRequest>(wire).is_err());
    }
    let shell: OperationRequest = OperationRequest::Delivery(DeliveryRequest::Fetch {
        stream: DeliveryStream::Controls,
        after: u64::MAX,
    });
    let decoded: OperationRequest =
        serde_json::from_slice(&serde_json::to_vec(&shell).unwrap()).unwrap();
    assert!(matches!(
        decoded,
        OperationRequest::Delivery(DeliveryRequest::Fetch {
            stream: DeliveryStream::Controls,
            after: u64::MAX
        })
    ));
}
