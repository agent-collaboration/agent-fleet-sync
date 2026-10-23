#![allow(clippy::panic_in_result_fn)]
use super::*;
use crate::{ServiceCalls, call_router::RoutedCall};
use fleet_sync_delivery::{Mailbox, admit_publications, minimum_settlement_allowance};
use fleet_sync_protocol::*;
use runtime_primitives::{Clock, WakeScheduler};
use sqlx_core::{query::query, row::Row};
use std::time::Duration;
use tokio::sync::mpsc;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
struct Mail;
impl DeliveryTypes for Mail {
    const MAX_SENDER_BYTES: u32 = 128;
    const MAX_ADDRESS_BYTES: u32 = 128;
    const MAX_ACCEPTANCE_BYTES: u32 = 128;
    const MAX_STATUS_ROUTE_BYTES: u32 = 128;
    const MAX_LOCAL_OBSERVATION_BYTES: u32 = 4;
    type Sender = String;
    type Address = String;
    type Body = String;
    type Acceptance = String;
    type LocalObservation = ();
    type StatusRoute = String;
}
fn peer(index: u64) -> TestResult<PeerId> {
    Ok(PeerId::new(
        format!("019eef00-0000-7000-8000-{index:012x}").parse()?,
    )?)
}
fn item(index: u64) -> TestResult<ItemId> {
    Ok(ItemId::new(
        format!("019eef00-0000-7000-8000-{index:012x}").parse()?,
    )?)
}
async fn submit(owner: &DeliveryOwner, limits: DeliveryLimits, id: ItemId) -> TestResult {
    let mailbox = peer(4)?;
    let origin = peer(2)?;
    let now = owner.object.host().now();
    let envelope = DeliveryEnvelope::Item(DeliveryItem::<Mail>::new(
        id,
        "sender".into(),
        Recipient {
            peer: mailbox,
            address: "inbox".into(),
        },
        None,
        "body".into(),
    )?);
    owner
        .object
        .rule_transaction(move |scope| {
            Box::pin(async move {
                Mailbox::<_, Mail>::new(scope, mailbox, limits)
                    .submit(origin, envelope, now, "route".into(), None)
                    .await?;
                admit_publications::<_, Mail, PublishStatus<ServiceObject, Turso, Mail>>(scope)
                    .await?;
                Ok::<_, fleet_sync_delivery::DeliveryError>(())
            })
        })
        .await?;
    Ok(())
}
async fn next_call(calls: &mut mpsc::Receiver<RoutedCall>) -> TestResult<RoutedCall> {
    Ok(tokio::time::timeout(Duration::from_secs(2), calls.recv())
        .await?
        .ok_or("call carrier closed")?)
}

#[tokio::test]
async fn mailbox_later_wake_publishes_while_an_earlier_publication_is_held() -> TestResult {
    let directory = tempfile::tempdir()?;
    let (sender, mut calls) = mpsc::channel(32);
    let host = Arc::new(ServiceHost::new(
        Arc::new(ServiceCalls {
            sender,
            answer_timeout: Duration::from_secs(10),
        }),
        SystemClock,
    ));
    let objects = fleet_sync_tokio_runner::ObjectStore::open(directory.path()).await?;
    let object = objects
        .object(
            ObjectKey::new("mailbox-independent-publications")?,
            ObjectOptions::default(),
            host.clone(),
        )
        .await?;
    let mut engine = Engine::new(object.clone(), host.clone());
    engine.register(PublishStatus::<ServiceObject, Turso, Mail>::new())?;
    let owner = Arc::new(DeliveryOwner {
        object,
        engine: Arc::new(engine),
        health: watch::channel(None).0,
        schema: OnceCell::new(),
    });
    let limits = DeliveryLimits::new::<Mail>(
        10_000_000,
        200,
        10_000_000,
        200,
        10_000_000,
        minimum_settlement_allowance::<Mail>()?,
    )?;
    owner.ensure_schema().await?;
    submit(&owner, limits, item(1)?).await?;
    let (shutdown, stopped) = watch::channel(false);
    let running = {
        let owner = owner.clone();
        let mailbox = peer(4)?;
        tokio::spawn(async move {
            crate::delivery_deadlines::run_initialized_mailbox::<Mail>(
                owner, mailbox, limits, stopped,
            )
            .await
        })
    };
    let first = next_call(&mut calls).await?;
    let publication: StatusPublication<String> = serde_json::from_value(first.envelope.request)?;
    assert_eq!(publication.item, item(1)?);
    // Native storage commit and its actual owner wake happen after call1 has
    // entered the real carrier. Only call2's arrival can satisfy the assertion.
    submit(&owner, limits, item(2)?).await?;
    host.wake_at(host.now()).await?;
    let independent = next_call(&mut calls).await;
    first
        .answer
        .send(Ok(serde_json::to_value(StatusPublicationResult::Applied)?))
        .map_err(|_| "first answer receiver dropped")?;
    shutdown.send(true)?;
    if let Ok(second) = &independent {
        let publication: StatusPublication<String> =
            serde_json::from_value(second.envelope.request.clone())?;
        assert_eq!(publication.item, item(2)?);
        assert_ne!(first.envelope.call, second.envelope.call);
    }
    let independent = match independent {
        Ok(second) => {
            second
                .answer
                .send(Ok(serde_json::to_value(StatusPublicationResult::Applied)?))
                .map_err(|_| "second answer receiver dropped")?;
            true
        }
        Err(_) => false,
    };
    tokio::time::timeout(Duration::from_secs(3), running).await??;
    // On the wrong build shutdown exits before the second wake is consumed.
    // Its queued call is settled only after recording the failed observation.
    let owed: i64 = owner
        .object
        .transaction(|scope| {
            Box::pin(async move {
                query::<Turso>("SELECT count(*) AS count FROM _fleet_sync_status_owed")
                    .fetch_one(scope.executor())
                    .await?
                    .try_get("count")
                    .map_err(StoreError::backend)
            })
        })
        .await?;
    objects.close().await?;
    assert!(
        independent,
        "mailbox later wake stalled behind held publication call"
    );
    assert_eq!(
        owed, 0,
        "both real publication tasks must retire their committed debt"
    );
    Ok(())
}
