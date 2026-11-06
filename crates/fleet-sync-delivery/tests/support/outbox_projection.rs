//! Consumer projection written through the real custody transaction.
use super::*;
use durable_steps::{AtomicStore, storage::TransactionScope};
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;

pub struct OutcomeBindings {
    pub refuse: bool,
}
pub const OUTCOME_BINDINGS: OutcomeBindings = OutcomeBindings { refuse: false };

impl<S: TransactionScope<Database = Turso>> SenderBinding<S, TestTypes> for OutcomeBindings {
    async fn resolve_submitter<'a>(
        &'a self,
        scope: &'a mut S,
        sender: &'a String,
    ) -> Result<Submitter, SenderBindingFailure> {
        <FixtureBindings as SenderBinding<S, TestTypes>>::resolve_submitter(
            &FixtureBindings,
            scope,
            sender,
        )
        .await
    }

    async fn on_outbox_outcome(
        &self,
        scope: &mut S,
        item: ItemId,
        status: &DeliveryStatus<Acceptance>,
    ) -> Result<(), DeliveryError> {
        query::<Turso>("INSERT INTO outbox_projection(item,status) VALUES(?1,?2)")
            .bind(item.as_uuid().to_string())
            .bind(serde_json::to_string(status).unwrap())
            .execute(scope.executor())
            .await
            .map_err(|error| DeliveryError::Storage(error.to_string()))?;
        if self.refuse {
            return Err(DeliveryError::Storage("outbox projection refused".into()));
        }
        Ok(())
    }
}

pub async fn prepare(fixture: &Fixture) {
    fixture.store.transaction(|scope| Box::pin(async move {
        query::<Turso>("CREATE TABLE outbox_projection(event INTEGER PRIMARY KEY,item TEXT NOT NULL,status TEXT NOT NULL)")
            .execute(scope.executor()).await?;
        Ok(())
    })).await.unwrap();
}

pub async fn projected(fixture: &Fixture) -> Vec<(String, serde_json::Value)> {
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                query::<Turso>("SELECT item,status FROM outbox_projection ORDER BY event")
                    .fetch_all(scope.executor())
                    .await?
                    .into_iter()
                    .map(|row| {
                        Ok((
                            row.try_get("item")?,
                            serde_json::from_str(&row.try_get::<String, _>("status")?).unwrap(),
                        ))
                    })
                    .collect::<Result<Vec<_>, sqlx_core::Error>>()
                    .map_err(durable_steps::StoreError::backend)
            })
        })
        .await
        .unwrap()
}

pub async fn enqueue(fixture: &Fixture) {
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let DeliveryEnvelope::Item(original) = item(1, "outbox body") else {
                    return Err(durable_steps::StoreError::backend("fixture original"));
                };
                Outbox::<_, TestTypes, _>::new(scope, limits(), &OUTCOME_BINDINGS)
                    .enqueue(
                        original,
                        peer(3),
                        Submitter::Machine(peer(1)),
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
}

pub fn submit_input() -> SubmitInput {
    SubmitInput {
        item: item_id(1),
        digest: item(1, "outbox body").digest().clone(),
        relay: peer(3),
        mailbox: peer(2),
    }
}
