#![allow(clippy::unwrap_used)]
use fleet_sync_protocol::*;
use serde::{Serialize, de::DeserializeOwned};
use uuid::Uuid;

fn round_trip<T: Serialize + DeserializeOwned + Eq + std::fmt::Debug>(value: T) {
    assert_eq!(
        serde_json::from_value::<T>(serde_json::to_value(&value).unwrap()).unwrap(),
        value
    );
}

#[test]
fn copy_call_refusals_are_typed_answers() {
    assert_eq!(
        serde_json::to_value(CreateCopyAnswer::Retired).unwrap(),
        serde_json::json!({"kind":"retired"})
    );
    for answer in [AwaitAckedAnswer::Retired, AwaitAckedAnswer::NotInitialized] {
        round_trip(answer);
    }
    round_trip(AwaitAckedAnswer::Acked {
        through: ReplicationPosition::new(Epoch::new(1).unwrap(), CommitSequence::new(8)),
    });
    for answer in [CopyStatusAnswer::Retired, CopyStatusAnswer::NotInitialized] {
        let frame = ReplicationAnswer::CopyStatus(answer.clone());
        let decoded: ReplicationAnswer =
            serde_json::from_value(serde_json::to_value(frame).unwrap()).unwrap();
        assert!(matches!(decoded, ReplicationAnswer::CopyStatus(value) if value == answer));
    }
    round_trip(RetireCopyAnswer::NotInitialized);
}

#[test]
fn gate_requests_and_settlement_keep_the_consumers_typed_payloads() {
    let database = DatabaseId::new(Uuid::from_u128(0x019eef00000070008000000000000001)).unwrap();
    let writer = PeerId::new(Uuid::from_u128(0x019eef00000070008000000000000002)).unwrap();
    let change = HolderChangeRequest::Claim {
        attempt: TransferId::new(Uuid::from_u128(0x019eef00000070008000000000000003)).unwrap(),
        database,
        claimant: writer,
    };
    round_trip(RegistrationAdmission { database, writer });
    round_trip(RetirementAdmission { database });
    round_trip(ClaimReservation {
        change: change.clone(),
    });
    for outcome in [
        SettleOutcome::Granted {
            epoch: Epoch::new(2).unwrap(),
        },
        SettleOutcome::Refused {
            reason: HolderChangeRefusal::Retired,
        },
    ] {
        round_trip(ClaimSettlement {
            change: change.clone(),
            outcome,
        });
    }
    for answer in [
        GateAnswer::Admitted,
        GateAnswer::Reserved,
        GateAnswer::Refused {
            reason: GateReason {
                code: GateReasonCode::new("archived").unwrap(),
                detail: "database is archived".into(),
            },
        },
    ] {
        round_trip(answer);
    }
    assert_eq!(
        serde_json::to_value(SettlementRecorded).unwrap(),
        serde_json::Value::Null
    );
    round_trip(SettlementRecorded);
}

#[test]
fn copy_creation_conflict_returns_the_complete_existing_request() {
    let wire = serde_json::json!({"kind":"creationConflict","value":{"existing":{
        "database":"019eef00-0000-7000-8000-000000000001", "epoch":2,
        "holder":"019eef00-0000-7000-8000-000000000002",
        "origin":{"kind":"awaitingSeed","value":{"expectedPosition":{"epoch":2,"sequence":8}}}
    }}});
    let answer: CreateCopyAnswer = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(serde_json::to_value(answer).unwrap(), wire);
}

#[test]
fn snapshot_manifest_request_and_answers_keep_the_pinned_position_and_table_details() {
    let database = DatabaseId::new(Uuid::from_u128(0x019eef00000070008000000000000001)).unwrap();
    let position = ReplicationPosition::new(Epoch::new(2).unwrap(), CommitSequence::new(8));
    let request: ReplicationRequest = ReplicationRequest::SnapshotManifest { database, position };
    let wire = serde_json::to_value(request).unwrap();
    assert_eq!(wire["kind"], "snapshotManifest");
    let decoded: ReplicationRequest = serde_json::from_value(wire).unwrap();
    assert!(
        matches!(decoded, ReplicationRequest::SnapshotManifest { database: stored, position: pinned } if stored == database && pinned == position)
    );
    let manifest = SnapshotManifest {
        database,
        position,
        schema: SchemaIdentity::new(3, "schema-fingerprint".into()).unwrap(),
        tables: vec![TableManifest {
            table: TableName::new("example").unwrap(),
            row_count: u64::MAX,
            digest: TableDigest::new("b".repeat(64)).unwrap(),
        }],
    };
    for answer in [
        SnapshotManifestAnswer::Manifest(manifest),
        SnapshotManifestAnswer::Moved { position },
        SnapshotManifestAnswer::NotInitialized,
        SnapshotManifestAnswer::Retired,
    ] {
        round_trip(answer.clone());
        let frame = ReplicationAnswer::SnapshotManifest(answer.clone());
        let decoded: ReplicationAnswer =
            serde_json::from_value(serde_json::to_value(frame).unwrap()).unwrap();
        assert!(matches!(decoded, ReplicationAnswer::SnapshotManifest(value) if value == answer));
    }
}

#[test]
fn holder_gate_refusal_keeps_the_consumers_reason() {
    let wire = serde_json::json!({"kind":"gateRefused","value":{"reason":{"code":"archived","detail":"database is archived"}}});
    let refusal: HolderChangeRefusal = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(serde_json::to_value(refusal).unwrap(), wire);
}

#[test]
fn retirement_can_refuse_an_uninitialized_copy() {
    let wire = serde_json::json!({"kind":"copyNotInitialized"});
    let refusal: RetirementRefusal = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(serde_json::to_value(refusal).unwrap(), wire);
}
