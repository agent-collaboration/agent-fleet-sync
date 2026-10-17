use crate::{
    ServiceConsumer,
    call_router::{lost, unreachable},
    service_state::ServiceState,
};
use durable_steps::{AtomicStore, CallFailure, CallTarget, StoreError};
use fleet_sync_lease::*;
use fleet_sync_protocol::*;
use fleet_sync_session::PeerAuthenticator;
use fleet_sync_tokio_runner::CallEnvelope;
use runtime_primitives::Clock;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

fn decode<T: DeserializeOwned>(value: Value) -> Result<T, CallFailure> {
    serde_json::from_value(value).map_err(lost)
}
fn encode<T: Serialize>(value: T) -> Result<Value, CallFailure> {
    serde_json::to_value(value).map_err(lost)
}
impl<P: ConsumerProtocol, C: ServiceConsumer<P>, A: PeerAuthenticator<Material = String>>
    ServiceState<P, C, A>
{
    pub(crate) async fn route_call(&self, envelope: CallEnvelope) -> Result<Value, CallFailure> {
        let CallEnvelope {
            kind,
            target,
            request,
            call,
        } = envelope;
        match kind {
            fleet_sync_lease::RegistrationAdmission::KIND => {
                let request: fleet_sync_protocol::RegistrationAdmission = decode(request)?;
                encode(
                    self.consumer
                        .admit_registration(&request.database, &request.writer, call)
                        .await
                        .map_err(lost)?,
                )
            }
            fleet_sync_lease::RetirementAdmission::KIND => {
                let request: fleet_sync_protocol::RetirementAdmission = decode(request)?;
                encode(
                    self.consumer
                        .admit_retirement(&request.database, call)
                        .await
                        .map_err(lost)?,
                )
            }
            fleet_sync_lease::ClaimReservation::KIND => {
                let request: fleet_sync_protocol::ClaimReservation = decode(request)?;
                encode(
                    self.consumer
                        .reserve(&request.change, call)
                        .await
                        .map_err(lost)?,
                )
            }
            fleet_sync_lease::ClaimSettlement::KIND => {
                let request: fleet_sync_protocol::ClaimSettlement = decode(request)?;
                self.consumer
                    .settle(&request.change, request.outcome, call)
                    .await
                    .map_err(lost)?;
                encode(SettlementRecorded)
            }
            HolderRelease::KIND => {
                let target: HolderRelease = decode(target)?;
                let request: ReleaseRequest = decode(request)?;
                if request.database != target.database {
                    return Err(unreachable());
                }
                let answer = self
                    .sessions()
                    .map_err(lost)?
                    .request_peer(
                        target.holder,
                        ServerRequest::Crate(ServerCall::Release { call, request }),
                        self.options.call_timeout,
                    )
                    .await?;
                match answer {
                    ClientAnswer::Crate(ServerCallAnswer::Release(answer)) => encode(answer),
                    _ => Err(CallFailure::AnswerLost),
                }
            }
            CopyCreate::KIND => {
                let target: CopyCreate = decode(target)?;
                self.local_receiver(target.copy)?;
                let request: CreateCopy = decode(request)?;
                let copy = self.copy(request.database).await.map_err(lost)?;
                let answer = copy
                    .object
                    .transaction(move |scope| {
                        Box::pin(async move {
                            fleet_sync_receiver::create_copy(scope, &request)
                                .await
                                .map_err(StoreError::backend)
                        })
                    })
                    .await
                    .map_err(lost)?;
                copy.changed.send_replace(());
                encode(answer)
            }
            CopyFence::KIND => {
                let target: CopyFence = decode(target)?;
                self.local_receiver(target.copy)?;
                let request: FenceRequest = decode(request)?;
                let copy = self.copy(request.database).await.map_err(lost)?;
                let answer = copy
                    .object
                    .transaction(move |scope| {
                        Box::pin(async move {
                            fleet_sync_receiver::fence(scope, &request)
                                .await
                                .map_err(StoreError::backend)
                        })
                    })
                    .await
                    .map_err(lost)?;
                copy.changed.send_replace(());
                encode(answer)
            }
            CopyRetire::KIND => {
                let target: CopyRetire = decode(target)?;
                self.local_receiver(target.copy)?;
                let request: RetireCopy = decode(request)?;
                let copy = self.copy(request.database).await.map_err(lost)?;
                let now = copy.object.host().now();
                let answer = copy
                    .object
                    .transaction(move |scope| {
                        Box::pin(async move {
                            fleet_sync_receiver::retire_copy(scope, &request, now)
                                .await
                                .map_err(StoreError::backend)
                        })
                    })
                    .await
                    .map_err(lost)?;
                copy.changed.send_replace(());
                encode(answer)
            }
            CopyRead::KIND => {
                let target: CopyRead = decode(target)?;
                self.local_receiver(target.copy)?;
                let _: () = decode(request)?;
                encode(self.copy_status(target.database).await.map_err(lost)?)
            }
            CopyAcknowledged::KIND => {
                let target: CopyAcknowledged = decode(target)?;
                self.local_receiver(target.copy)?;
                let request: AwaitAcked = decode(request)?;
                encode(
                    self.await_acked(target.database, request.through)
                        .await
                        .map_err(lost)?,
                )
            }
            _ => Err(unreachable()),
        }
    }
    fn local_receiver(&self, copy: ReceiverId) -> Result<(), CallFailure> {
        if copy == self.options.receiver {
            Ok(())
        } else {
            Err(unreachable())
        }
    }
}
