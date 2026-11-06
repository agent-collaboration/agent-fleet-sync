#![allow(clippy::unwrap_used)]
use super::*;
use crate::custody_fixture::{
    TestTypes, item_id,
    legacy_submitters::{legacy_store, startup},
    limits,
    outbox_projection::{OUTCOME_BINDINGS, prepare, projected},
    storage_error,
};
use durable_steps::AtomicStore;
use sqlx_core::query::query;
use sqlx_turso::Turso;

#[tokio::test]
async fn retained_terminal_legacy_guard_does_not_project() {
    let fixture = legacy_store(2).await;
    startup(&fixture.store).await.unwrap();
    prepare(&fixture).await;
    fixture.store.transaction(|scope|Box::pin(async move {
        // A schema-valid retained Legacy terminal row. The private guard is
        // tested directly because public writers deliberately skip Legacy.
        query::<Turso>("UPDATE _fleet_sync_outbox_items SET status_kind='expired',live=0,settlement_charge_bytes=0 WHERE item_id=?1")
            .bind(item_id(101).as_uuid().to_string()).execute(scope.executor()).await?;
        let mut outbox=Outbox::<_,TestTypes,_>::new(scope,limits(),&OUTCOME_BINDINGS);
        assert!(matches!(outbox.recovery_header(item_id(101)).await.map_err(storage_error)?,Some(OutboxHeaderState::Legacy {status:DeliveryStatus::Expired,..})));
        outbox.project_terminal_outcome(item_id(101),&DeliveryStatus::Expired).await.map_err(storage_error)?;
        assert!(matches!(outbox.status(item_id(101)).await.map_err(storage_error)?.state,crate::OutboxStatusState::Known {status:DeliveryStatus::Expired,..}));
        Ok(())
    })).await.unwrap();
    assert!(projected(&fixture).await.is_empty());
}
