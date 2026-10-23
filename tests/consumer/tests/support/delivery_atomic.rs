use durable_steps::{
    AtomicStore, CallFailure, NotSentReason, StoreError, storage::TransactionScope,
    turso_store::TursoStore,
};
use fleet_sync_delivery::{
    DELIVERY_MIGRATOR, Mailbox, MailboxFailureDisposition, MailboxOperation,
    classify_delivery_store_error, delivery_rollback, mailbox_failure_disposition,
};
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::{SampleDelivery, TestResult, delivery_limits};
use fleet_sync_tokio_runner::{CallEnvelope, CallRouter, TokioHost};
use runtime_primitives::SystemClock;
use sqlx_turso::Turso;
use std::sync::Arc;

struct NoCalls;
impl CallRouter for NoCalls {
    async fn dispatch(&self, _: CallEnvelope) -> Result<serde_json::Value, CallFailure> {
        Err(CallFailure::NotSent {
            reason: NotSentReason::Overloaded,
        })
    }
}

pub async fn failed_answer(
    operation: MailboxOperation,
    mailbox: PeerId,
    origin: PeerId,
    identity: ItemId,
) -> TestResult<serde_json::Value> {
    let directory = tempfile::tempdir()?;
    let host = Arc::new(TokioHost::new(Arc::new(NoCalls), SystemClock));
    let store = TursoStore::open_and_migrate(directory.path().join("atomic.db"), host).await?;
    store
        .transaction(|scope| {
            Box::pin(async move {
                let mut migrator = sqlx_core::migrate::Migrator::with_migrations(
                    DELIVERY_MIGRATOR.iter().cloned().collect::<Vec<_>>(),
                );
                migrator.dangerous_set_table_name("_fleet_sync_delivery_migrations");
                migrator
                    .run_direct(None, scope.executor(), false)
                    .await
                    .map_err(StoreError::backend)
            })
        })
        .await?;
    let failure = store.transaction::<(), _>(move |scope| Box::pin(async move {
        let mut owner = Mailbox::<_, SampleDelivery>::new(scope, mailbox, delivery_limits().map_err(StoreError::backend)?);
        let sender = if operation == MailboxOperation::Submit { "x".repeat(256) } else { "sample sender".into() };
        let envelope = DeliveryEnvelope::Item(DeliveryItem::<SampleDelivery>::new(identity, sender, Recipient {
            peer: mailbox, address: "inbox".into(),
        }, None, "body".into()).map_err(StoreError::backend)?);
        owner.submit(origin, envelope, SyncTime::from_unix_millis(0), "route".into(), None).await.map_err(delivery_rollback)?;
        match operation {
            MailboxOperation::Report => {
                owner.report(mailbox, RecipientReport { item: identity, report_seq: ReportSeq::new(1), outcome: RecipientOutcome::Delivered { acceptance: "x".repeat(256) } })
                    .await.map_err(delivery_rollback)?;
            }
            MailboxOperation::Query => {
                sqlx_core::query::query::<Turso>("UPDATE _fleet_sync_mail_items SET record=json_set(record,'$.value.payload',NULL),payload_bytes=0")
                    .execute(scope.executor()).await?;
                Mailbox::<_, SampleDelivery>::new(scope, mailbox, delivery_limits().map_err(StoreError::backend)?)
                    .query(&[identity]).await.map_err(delivery_rollback)?;
            }
            _ => {}
        }
        Ok(())
    })).await;
    let error = failure.err().ok_or("expected delivery rollback")?;
    match mailbox_failure_disposition::<SampleDelivery>(
        mailbox,
        operation,
        classify_delivery_store_error(error),
    ) {
        MailboxFailureDisposition::Answer { answer, corruption } => {
            if operation == MailboxOperation::Query {
                assert_eq!(
                    corruption.ok_or("missing mailbox health signal")?.mailbox,
                    mailbox
                );
            } else {
                assert!(corruption.is_none());
            }
            Ok(serde_json::to_value(answer)?)
        }
        MailboxFailureDisposition::Unhandled(error) => Err(error.into()),
    }
}
