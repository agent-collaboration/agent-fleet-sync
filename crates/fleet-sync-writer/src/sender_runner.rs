use crate::{
    CaptureBookkeeping, PieceAnnotation, ReplicatedSchema, ReplicationEvents, ReplicationSender,
    ReplicationTransport, SenderError,
};
use fleet_sync_protocol::ReceiverId;
use std::{collections::BTreeSet, sync::Arc};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};

#[derive(Debug, thiserror::Error)]
#[error("replication to {copy:?} delayed: {error}")]
pub struct CopyFailure {
    pub copy: ReceiverId,
    pub error: SenderError,
}

impl<S, A, B, T> ReplicationSender<S, A, B, T>
where
    S: ReplicatedSchema,
    A: PieceAnnotation,
    B: CaptureBookkeeping,
    T: ReplicationTransport<A::Protocol>,
{
    /// Discover durable copy registrations and run each independently. Error
    /// reports carry the copy identity; a refusing copy does not stop its peers.
    pub async fn run(
        self: Arc<Self>,
        mut shutdown: watch::Receiver<bool>,
        events: mpsc::Sender<Result<ReplicationEvents, CopyFailure>>,
    ) -> Result<(), SenderError> {
        if self.options.retry_after.is_zero() {
            return Err(SenderError::Invalid {
                field: "sender.retry_after",
            });
        }
        let mut authority_changes = self.database.authority_changes.subscribe();
        let mut seen_tails = BTreeSet::new();
        for tail in self.rejected_tails().await? {
            seen_tails.insert(tail.epoch);
            tokio::select! { sent=events.send(Ok(ReplicationEvents::RejectedTail(tail)))=>{if sent.is_err(){return Ok(());}}, _=wait_for_shutdown(&mut shutdown)=>return Ok(()) }
        }
        let mut started = BTreeSet::new();
        let mut workers = JoinSet::new();
        let mut discovery = tokio::time::interval(self.options.retry_after);
        loop {
            if *shutdown.borrow() || events.is_closed() {
                workers.abort_all();
                while workers.join_next().await.is_some() {}
                return Ok(());
            }
            for copy in self.copies().await? {
                if started.insert(copy.copy) {
                    let sender = self.clone();
                    let stopped = shutdown.clone();
                    let output = events.clone();
                    workers.spawn(async move { sender.run_copy(copy.copy, stopped, output).await });
                }
            }
            tokio::select! {
                result=shutdown.changed()=>{if result.is_err(){workers.abort_all();while workers.join_next().await.is_some(){}return Ok(());}},
                _=discovery.tick()=>{},
                changed=authority_changes.changed()=>{
                    if changed.is_ok() {
                        for tail in self.rejected_tails().await? {
                            if seen_tails.insert(tail.epoch) {
                                tokio::select! {
                                    sent=events.send(Ok(ReplicationEvents::RejectedTail(tail)))=>{if sent.is_err(){break;}},
                                    _=wait_for_shutdown(&mut shutdown)=>break,
                                }
                            }
                        }
                    }
                },
                result=workers.join_next(),if !workers.is_empty()=>{
                    match result {
                        Some(Ok(()))=>{},
                        Some(Err(_))=>return Err(SenderError::Invalid{field:"sender.copy_task"}),
                        None=>{},
                    }
                }
            }
        }
    }

    async fn run_copy(
        &self,
        copy: ReceiverId,
        mut shutdown: watch::Receiver<bool>,
        events: mpsc::Sender<Result<ReplicationEvents, CopyFailure>>,
    ) {
        let mut needs_snapshot_reported = false;
        loop {
            if *shutdown.borrow() {
                return;
            }
            let result = tokio::select! {
                result=self.replicate_once(copy)=>result,
                _=wait_for_shutdown(&mut shutdown)=>return,
            };
            let output = match result {
                Ok(reported) => reported
                    .into_iter()
                    .filter(|event| match event {
                        ReplicationEvents::CopyNeedsSnapshot { copy: changed }
                            if *changed == copy =>
                        {
                            let emit = !needs_snapshot_reported;
                            needs_snapshot_reported = true;
                            emit
                        }
                        ReplicationEvents::Acknowledged { copy: changed, .. }
                            if *changed == copy =>
                        {
                            needs_snapshot_reported = false;
                            true
                        }
                        _ => true,
                    })
                    .map(Ok)
                    .collect::<Vec<_>>(),
                Err(error) => vec![Err(CopyFailure { copy, error })],
            };
            for event in output {
                tokio::select! {
                    sent=events.send(event)=>{if sent.is_err(){return;}},
                    _=wait_for_shutdown(&mut shutdown)=>return,
                }
            }
            tokio::select! {
                _=tokio::time::sleep(self.options.retry_after)=>{},
                _=wait_for_shutdown(&mut shutdown)=>return,
            }
        }
    }
}

async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow() || shutdown.changed().await.is_err() {
            return;
        }
    }
}
