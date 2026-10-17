//! The registry host routes actual copy calls to receiver rules in independent transactions.
//! Holder release is a receipted stand-in; the real writer/drain journey belongs to D2.
use crate::{
    CopyAcknowledged, CopyRead,
    turso_registry_fixture::{CopyScope, TestHost, decode, encode, lost},
};
use durable_steps::{CallFailure, CallId, NotSentReason};
use fleet_sync_protocol::*;
use sqlx_core::{connection::Connection, row::Row};
use sqlx_turso::Turso;
use std::{
    collections::btree_map::Entry,
    sync::{Arc, atomic::Ordering},
};
use tokio::sync::Notify;

pub(super) struct FenceGate {
    pub started: Notify,
    pub proceed: Notify,
}
impl FenceGate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            started: Notify::new(),
            proceed: Notify::new(),
        })
    }
}
impl TestHost {
    pub async fn copy_dispatch(
        &self,
        kind: &str,
        target: serde_json::Value,
        request: serde_json::Value,
        call: CallId,
    ) -> Result<serde_json::Value, CallFailure> {
        if kind == "fleet-sync.copy-retire" && self.retire_unreachable.load(Ordering::SeqCst) {
            return Err(CallFailure::NotSent {
                reason: NotSentReason::Unreachable { last_seen: None },
            });
        }
        if kind == "fleet-sync.copy-status" && self.status_unreachable.load(Ordering::SeqCst) {
            return Err(CallFailure::NotSent {
                reason: NotSentReason::Unreachable { last_seen: None },
            });
        }
        let database = match kind {
            "fleet-sync.copy-create" => decode::<CreateCopy>(request.clone())?.database,
            "fleet-sync.copy-fence" => decode::<FenceRequest>(request.clone())?.database,
            "fleet-sync.copy-retire" => decode::<RetireCopy>(request.clone())?.database,
            "fleet-sync.copy-status" => decode::<CopyRead>(target.clone())?.database,
            "fleet-sync.copy-acknowledged" => decode::<CopyAcknowledged>(target)?.database,
            "fleet-sync.holder-release" => decode::<ReleaseRequest>(request.clone())?.database,
            _ => return Err(CallFailure::AnswerLost),
        };
        if kind == "fleet-sync.holder-release" {
            self.release_observed.notify_one();
            if self.hold_release.load(Ordering::SeqCst) {
                return std::future::pending().await;
            }
            if self.release_unreachable.load(Ordering::SeqCst) {
                return Err(CallFailure::NotSent {
                    reason: NotSentReason::Unreachable {
                        last_seen: Some(SyncTime::from_unix_millis(1000)),
                    },
                });
            }
        }
        if kind == "fleet-sync.copy-acknowledged" {
            self.acked_observed.notify_one();
            if self.hold_acked.load(Ordering::SeqCst) {
                return std::future::pending().await;
            }
        }
        if kind == "fleet-sync.copy-fence" {
            if self.fence_unreachable.load(Ordering::SeqCst) {
                return Err(CallFailure::NotSent {
                    reason: NotSentReason::Unreachable { last_seen: None },
                });
            }
            let fence = decode::<FenceRequest>(request.clone())?;
            let gate = self
                .fence_gates
                .lock()
                .await
                .get(&fence.transfer_id)
                .cloned();
            if let Some(gate) = gate {
                gate.started.notify_one();
                gate.proceed.notified().await;
            }
        }
        let mut copies = self.copies.lock().await;
        if let Entry::Vacant(entry) = copies.entry(database) {
            entry.insert(self.copy_connection(database).await.map_err(lost)?);
        }
        let connection = copies.get_mut(&database).ok_or(CallFailure::AnswerLost)?;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(lost)?;
        let mut scope = CopyScope(&mut transaction);
        let answer = match kind {
            "fleet-sync.copy-create" => encode(
                fleet_sync_receiver::create_copy(&mut scope, &decode(request)?)
                    .await
                    .map_err(lost)?,
            )?,
            "fleet-sync.copy-fence" => encode(
                fleet_sync_receiver::fence(&mut scope, &decode(request)?)
                    .await
                    .map_err(lost)?,
            )?,
            "fleet-sync.copy-retire" => encode(
                fleet_sync_receiver::retire_copy(
                    &mut scope,
                    &decode(request)?,
                    runtime_primitives::Clock::now(self),
                )
                .await
                .map_err(lost)?,
            )?,
            "fleet-sync.copy-status" => encode(
                fleet_sync_receiver::copy_status(&mut scope)
                    .await
                    .map_err(lost)?,
            )?,
            "fleet-sync.copy-acknowledged" => {
                let requested: AwaitAcked = decode(request)?;
                let answer = match fleet_sync_receiver::copy_status(&mut scope)
                    .await
                    .map_err(lost)?
                {
                    CopyStatusAnswer::Status(status) if status.acked >= requested.through => {
                        AwaitAckedAnswer::Acked {
                            through: requested.through,
                        }
                    }
                    CopyStatusAnswer::Status(_) => return Err(CallFailure::AnswerLost),
                    CopyStatusAnswer::NotInitialized => AwaitAckedAnswer::NotInitialized,
                    CopyStatusAnswer::Retired => AwaitAckedAnswer::Retired,
                };
                encode(answer)?
            }
            "fleet-sync.holder-release" => {
                let requested: ReleaseRequest = decode(request)?;
                let status = fleet_sync_receiver::copy_status(&mut scope)
                    .await
                    .map_err(lost)?;
                transaction.commit().await.map_err(lost)?;
                drop(copies);
                return self.released(requested, status, call).await;
            }
            _ => return Err(CallFailure::AnswerLost),
        };
        transaction.commit().await.map_err(lost)?;
        if kind == "fleet-sync.copy-create" && self.lose_create.swap(false, Ordering::SeqCst) {
            return Err(CallFailure::AnswerLost);
        }
        if kind == "fleet-sync.copy-fence" && self.lose_fence.swap(false, Ordering::SeqCst) {
            return Err(CallFailure::AnswerLost);
        }
        if kind == "fleet-sync.copy-retire" && self.lose_retire.swap(false, Ordering::SeqCst) {
            return Err(CallFailure::AnswerLost);
        }
        Ok(answer)
    }
    async fn released(
        &self,
        request: ReleaseRequest,
        status: CopyStatusAnswer,
        call: CallId,
    ) -> Result<serde_json::Value, CallFailure> {
        let call = serde_json::to_string(&call).map_err(lost)?;
        let offered = serde_json::to_string(&request).map_err(lost)?;
        let mut connection = self.gate.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(lost)?;
        let prior =
            sqlx_core::query::query::<Turso>("SELECT request,answer FROM releases WHERE call=?1")
                .bind(call.clone())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(lost)?;
        let answer = if let Some(prior) = prior {
            if prior.try_get::<String, _>("request").map_err(lost)? != offered {
                ReleaseAnswer::RequestConflict
            } else {
                serde_json::from_str(&prior.try_get::<String, _>("answer").map_err(lost)?)
                    .map_err(lost)?
            }
        } else {
            let answer = if let Some(answer) = self.release_answer.lock().await.clone() {
                answer
            } else {
                match status {
                    CopyStatusAnswer::Status(status) if status.fence == request.epoch => {
                        ReleaseAnswer::Released {
                            final_position: status.acked,
                        }
                    }
                    CopyStatusAnswer::Status(status) => {
                        ReleaseAnswer::ReleaseRefused(ReleaseRefusal::Superseded {
                            current: status.fence,
                        })
                    }
                    _ => return Err(CallFailure::AnswerLost),
                }
            };
            sqlx_core::query::query::<Turso>(
                "INSERT INTO releases(call,request,answer) VALUES(?1,?2,?3)",
            )
            .bind(call)
            .bind(offered)
            .bind(serde_json::to_string(&answer).map_err(lost)?)
            .execute(&mut *transaction)
            .await
            .map_err(lost)?;
            answer
        };
        transaction.commit().await.map_err(lost)?;
        if self.lose_release.swap(false, Ordering::SeqCst) {
            return Err(CallFailure::AnswerLost);
        }
        encode(answer)
    }
}
