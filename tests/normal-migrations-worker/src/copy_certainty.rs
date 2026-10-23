//! The real DurableObjectStore and the production copy classifier.
use durable_steps::{
    AtomicStore, CallFailure, CallId, CallTarget, Host, StoreError, Uuid,
    durable_object_store::DurableObjectStore,
};
use fleet_sync_protocol::{CopyUnavailableReason, PeerId, Piece, ReplicationAnswer, SyncTime};
use fleet_sync_receiver::{
    ApplyDecision, apply_piece, apply_rollback, classify_copy_store_error, copy_rollback,
};
use runtime_primitives::{AlarmOwner, Clock, IdSource, WakeFailure, WakeScheduler};
use sqlx_durable_object::DoConnection;
use std::sync::Arc;
use worker::State;

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CopyProbeFault {
    None,
    RollbackAfterApply,
    LostCommitReply,
    LostRollbackReply,
    UnknownRollbackCode,
}

struct ProbeHost;
pub async fn migrate_journal(state: &State) -> worker::Result<()> {
    DurableObjectStore::new(
        DoConnection::new(state.storage()),
        Arc::new(ProbeHost),
        AlarmOwner::new(state.storage()),
    )
    .migrate()
    .await
    .map_err(crate::error)
}
impl Clock for ProbeHost {
    fn now(&self) -> SyncTime {
        SyncTime::from_unix_millis(0)
    }
}
impl IdSource for ProbeHost {
    fn new_id(&self) -> Uuid {
        Uuid::from_u128(0x019eef00000070008000000000000001)
    }
}
impl WakeScheduler for ProbeHost {
    async fn wake_at(&self, _: SyncTime) -> Result<(), WakeFailure> {
        Ok(())
    }
}
impl Host for ProbeHost {
    async fn dispatch<C: CallTarget>(
        &self,
        _: C,
        _: C::Request,
        _: CallId,
    ) -> Result<C::Answer, CallFailure> {
        Err(CallFailure::AnswerLost)
    }
}

pub async fn apply(
    state: &State,
    piece: Piece,
    sender: PeerId,
    fault: CopyProbeFault,
) -> worker::Result<ReplicationAnswer> {
    let store = DurableObjectStore::new(
        DoConnection::new(state.storage()),
        Arc::new(ProbeHost),
        AlarmOwner::new(state.storage()),
    );
    store.migrate().await.map_err(crate::error)?;
    let rollback = matches!(
        fault,
        CopyProbeFault::RollbackAfterApply | CopyProbeFault::LostRollbackReply
    );
    let corrupt_code = matches!(fault, CopyProbeFault::UnknownRollbackCode);
    let result = store
        .transaction(move |scope| {
            Box::pin(async move {
                let hook = crate::ProbeHook { refusal: None };
                let decision = apply_piece::<_, _>(
                    scope,
                    &piece,
                    &sender,
                    &hook,
                    SyncTime::from_unix_millis(0),
                )
                .await
                .map_err(apply_rollback)?;
                if corrupt_code {
                    return Err(StoreError::StepRollback {
                        failure: Box::new(durable_steps::OperationFailure::Refused {
                            code: "unrelated_refusal".into(),
                            detail: "not a copy reason".into(),
                        }),
                    });
                }
                if rollback {
                    return Err(copy_rollback(CopyUnavailableReason::Damaged {
                        detail: "fixture refusal after real apply before commit".into(),
                    }));
                }
                Ok(match decision {
                    ApplyDecision::Applied { through, .. } => {
                        ReplicationAnswer::Applied { through }
                    }
                    ApplyDecision::AlreadyApplied { through } => {
                        ReplicationAnswer::AlreadyApplied { through }
                    }
                    ApplyDecision::PartStaged {
                        transaction,
                        index,
                        acked,
                    } => ReplicationAnswer::PartStaged {
                        transaction,
                        index,
                        acked,
                    },
                })
            })
        })
        .await;
    // The SQL really committed. Simulate losing only its boundary reply.
    let result = match result {
        Ok(_) if matches!(fault, CopyProbeFault::LostCommitReply) => Err(StoreError::backend(
            "injected committed boundary reply loss",
        )),
        Err(_) if matches!(fault, CopyProbeFault::LostRollbackReply) => Err(StoreError::backend(
            "injected rolled back boundary reply loss",
        )),
        result => result,
    };
    Ok(match result {
        Ok(answer) => answer,
        Err(error) => classify_copy_store_error(error).answer(),
    })
}
