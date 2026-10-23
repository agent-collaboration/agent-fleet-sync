use crate::{
    ServiceConsumer, ServiceObject,
    delivery_owner::{DeliveryObject, DeliveryOwner},
    service_state::ServiceState,
};
use fleet_sync_delivery::{
    DeliveryError, DeliveryLimits, Mailbox, PublishStatus, admit_publications,
};
use fleet_sync_protocol::{ConsumerProtocol, DeliveryTypes, PeerId};
use fleet_sync_session::PeerAuthenticator;
use runtime_primitives::{Backoff, Clock, WakeScheduler};
use sqlx_turso::Turso;
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinSet, time::Instant};

// Bound pending deadline/drive passes; Engine separately caps active operations.
const MAX_PENDING_MAILBOX_PASSES: usize = 8;

const RETRY: Backoff = Backoff {
    initial: Duration::from_millis(100),
    max: Duration::from_secs(30),
    factor: 2,
};

enum PassFailure {
    Corrupt { field: &'static str },
    Operational { phase: &'static str, cause: String },
}
impl PassFailure {
    fn operational(phase: &'static str, cause: impl std::fmt::Display) -> Self {
        Self::Operational {
            phase,
            cause: cause.to_string(),
        }
    }
}

async fn mailbox_pass<D: DeliveryTypes>(
    owner: &DeliveryOwner,
    mailbox: PeerId,
    limits: DeliveryLimits,
) -> Result<(), PassFailure> {
    let host = owner.object.host();
    owner
        .ensure_schema()
        .await
        .map_err(|error| PassFailure::operational("migration", error))?;
    let now = host.now();
    let next = owner
        .object
        .rule_transaction(move |scope| {
            Box::pin(async move {
                let next = Mailbox::<_, D>::new(scope, mailbox, limits)
                    .expire_due(now)
                    .await?;
                admit_publications::<_, D, PublishStatus<ServiceObject, Turso, D>>(scope).await?;
                Ok::<_, DeliveryError>(next)
            })
        })
        .await
        .map_err(|error| match error {
            DeliveryError::Corrupt { field } => PassFailure::Corrupt { field },
            error => PassFailure::operational("scan", error),
        })?;
    if let Some(next) = next {
        // Keep the earlier of the custody deadline and the engine's retry.
        host.wake_at(next)
            .await
            .map_err(|error| PassFailure::operational("deadlineWake", error))?;
    }
    let report = owner
        .engine
        .drive_due()
        .await
        .map_err(|error| PassFailure::operational("engine", error))?;
    for (operation, failure) in report.failed {
        tracing::warn!(operation=%operation.as_uuid(),?failure,"durable delivery operation failed");
    }
    if let Some(error) = report.wake_failure {
        return Err(PassFailure::operational("engineWake", error));
    }
    Ok(())
}

pub(crate) async fn run_mailbox<P, C, A>(
    state: Arc<ServiceState<P, C, A>>,
    mailbox: PeerId,
    limits: DeliveryLimits,
    mut shutdown: watch::Receiver<bool>,
) where
    P: ConsumerProtocol,
    C: ServiceConsumer<P>,
    A: PeerAuthenticator<
            Material = String,
            Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute,
        >,
{
    let mut attempt = 0_u32;
    loop {
        if *shutdown.borrow() {
            return;
        }
        let result = tokio::select! {
            result = state.delivery_owner(DeliveryObject::Mailbox(mailbox)) => result,
            _ = shutdown.changed() => return,
        };
        match result {
            Ok(owner) => {
                run_initialized_mailbox::<P::Delivery>(owner, mailbox, limits, shutdown).await;
                return;
            }
            Err(cause) => {
                let delay = RETRY.delay(attempt);
                attempt = attempt.saturating_add(1);
                tracing::warn!(?mailbox, phase="initialization", %cause, ?delay, "delivery mailbox operation retry");
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {},
                    _ = shutdown.changed() => return,
                }
            }
        }
    }
}

async fn owned_mailbox_pass<D: DeliveryTypes>(
    owner: Arc<DeliveryOwner>,
    mailbox: PeerId,
    limits: DeliveryLimits,
    retry: bool,
) -> (bool, Result<(), PassFailure>) {
    (retry, mailbox_pass::<D>(&owner, mailbox, limits).await)
}

pub(crate) async fn run_initialized_mailbox<D: DeliveryTypes>(
    owner: Arc<DeliveryOwner>,
    mailbox: PeerId,
    limits: DeliveryLimits,
    mut shutdown: watch::Receiver<bool>,
) {
    let host = owner.object.host();
    let mut health = owner.health.subscribe();
    let mut attempt = 0_u32;
    let mut retry_at = None;
    let mut retry_in_flight = false;
    let mut stopping = *shutdown.borrow() || health.borrow().is_some();
    let mut passes = JoinSet::new();
    if !stopping {
        passes.spawn(owned_mailbox_pass::<D>(
            owner.clone(),
            mailbox,
            limits,
            false,
        ));
    }
    loop {
        if *shutdown.borrow() || health.borrow().is_some() {
            stopping = true;
        }
        if stopping && passes.is_empty() {
            return;
        }
        tokio::select! {
            observed = passes.join_next(), if !passes.is_empty() => {
                let Some(observed) = observed else {continue;};
                let (was_retry, result) = match observed {
                    Ok(result) => result,
                    Err(error) => {
                        tracing::error!(?mailbox, %error, "delivery mailbox pass panicked");
                        stopping = true;
                        continue;
                    }
                };
                {
                    if was_retry { retry_in_flight = false; }
                    match result {
                        Ok(()) => {
                            // A sibling success cannot cancel an outstanding
                            // operational backoff or its retry attempt.
                            if retry_at.is_none() && !retry_in_flight {attempt = 0;}
                        }
                        Err(PassFailure::Corrupt {field}) => {
                            owner.mark_corrupt(mailbox, field);
                            stopping = true;
                        }
                        Err(PassFailure::Operational {phase, cause}) => {
                            let delay = RETRY.delay(attempt);
                            attempt = attempt.saturating_add(1);
                            retry_at.get_or_insert_with(|| Instant::now() + delay);
                            tracing::warn!(?mailbox, phase, %cause, ?delay, "delivery mailbox operation retry");
                        }
                    }
                }
            },
            _ = async {
                match retry_at {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => {host.wait().await;},
                }
            }, if !stopping && !retry_in_flight && passes.len() < MAX_PENDING_MAILBOX_PASSES => {
                // The single timer owner handles both sources. Kicks never
                // bypass operational backoff, including while its retry runs.
                let retry = retry_at.take().is_some();
                retry_in_flight = retry;
                passes.spawn(owned_mailbox_pass::<D>(owner.clone(), mailbox, limits, retry));
            },
            changed = health.changed(), if !stopping => {
                if changed.is_err() || health.borrow().is_some() {stopping = true;}
            },
            changed = shutdown.changed(), if !stopping => {
                if changed.is_err() || *shutdown.borrow() {stopping = true;}
            }
        }
    }
}
