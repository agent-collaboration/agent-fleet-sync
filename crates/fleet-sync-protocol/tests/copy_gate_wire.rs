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
    let database = DatabaseId::new(Uuid::from_u128(1)).unwrap();
    let writer = PeerId::new(Uuid::from_u128(2)).unwrap();
    let change = HolderChangeRequest::Claim {
        attempt: TransferId::new(Uuid::from_u128(3)).unwrap(),
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
