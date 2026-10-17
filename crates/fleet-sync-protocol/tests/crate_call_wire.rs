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
    let database = DatabaseId::new(Uuid::from_u128(1)).unwrap();
    let attempt = TransferId::new(Uuid::from_u128(2)).unwrap();
    let target = PeerId::new(Uuid::from_u128(3)).unwrap();
    let position = ReplicationPosition::new(Epoch::new(1).unwrap(), CommitSequence::new(7));
    let retirement = Retirement {
        attempt: RequestId::new(Uuid::from_u128(4)).unwrap(),
        database,
        expect_fence: Epoch::new(1).unwrap(),
        expect_acked: position,
    };
    let administrative = [
        serde_json::to_value(HolderChangeRequest::Move {
            attempt,
            database,
            from_epoch: Epoch::new(1).unwrap(),
            to: target,
        })
        .unwrap(),
        serde_json::to_value(HolderChangeRequest::Takeover {
            attempt,
            database,
            preview: TakeoverPreview {
                token: PreviewToken::new("preview-token").unwrap(),
                fence: Epoch::new(1).unwrap(),
                acked: position,
                holder: target,
                last_seen: None,
            },
            to: target,
        })
        .unwrap(),
        serde_json::json!({"kind":"previewTakeover","value": PreviewTakeover { database }}),
        serde_json::json!({"kind":"import","value": Import { attempt: RequestId::new(Uuid::from_u128(5)).unwrap(), database, digest: ImportDigest::new("a".repeat(64)).unwrap() }}),
        serde_json::json!({"kind":"retire","value":retirement}),
        serde_json::json!({"kind":"retirement","value":retirement}),
    ];
    for body in administrative {
        let kind = body["kind"].clone();
        let wire = serde_json::json!({"kind":"lease","value":body});
        assert!(
            serde_json::from_value::<OperationRequest>(wire).is_err(),
            "peer session admitted {kind}"
        );
    }
    let claim = serde_json::json!({"kind":"lease","value":{"kind":"claim","value":{"attempt":"00000000-0000-0000-0000-000000000006","database":"00000000-0000-0000-0000-000000000001"}}});
    assert!(matches!(
        serde_json::from_value::<OperationRequest>(claim).unwrap(),
        OperationRequest::Lease(LeaseRequest::Claim { .. })
    ));
    let shell: OperationRequest = OperationRequest::Delivery(DeliveryRequest::Fetch {
        call: CallId {
            operation: OperationId(Uuid::from_u128(7)),
            step: StepName::new("fetch").unwrap(),
        },
        stream: DeliveryStream::Controls,
        after: DeliverySeq::new(u64::MAX),
        max_items: 10,
        max_bytes: 4096,
    });
    let decoded: OperationRequest =
        serde_json::from_slice(&serde_json::to_vec(&shell).unwrap()).unwrap();
    assert!(matches!(
        decoded,
        OperationRequest::Delivery(DeliveryRequest::Fetch {
            stream: DeliveryStream::Controls,
            after, ..
        }) if after.value() == u64::MAX
    ));
}
