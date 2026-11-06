//! Explicit provider for this private test consumer's source-address scheme.
use super::*;
use durable_steps::StoreError;
use durable_steps::storage::TransactionScope;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::future::Future;

pub struct FixtureBindings;
impl<S, D> SenderBinding<S, D> for FixtureBindings
where
    S: TransactionScope<Database = Turso>,
    D: DeliveryTypes,
{
    async fn resolve_submitter<'a>(
        &'a self,
        scope: &'a mut S,
        sender: &'a D::Sender,
    ) -> Result<Submitter, SenderBindingFailure> {
        let value = serde_json::to_value(sender).map_err(|_| SenderBindingFailure::Unavailable)?;
        if let Some(source) = value.as_str() {
            if let Some(service) = source.strip_prefix("svc:") {
                let service = service
                    .split('|')
                    .next()
                    .ok_or(SenderBindingFailure::Unknown)?;
                return lookup_service(scope, service).await;
            }
            if let Some(project) = source.strip_prefix("p:") {
                let identity = project
                    .split('|')
                    .next()
                    .ok_or(SenderBindingFailure::Unknown)?;
                return DatabaseId::try_from(identity.to_owned())
                    .map(Submitter::Project)
                    .map_err(|_| SenderBindingFailure::Unknown);
            }
            if let Some(machine) = source.strip_prefix("m:") {
                let identity = machine
                    .split('|')
                    .next()
                    .ok_or(SenderBindingFailure::Unknown)?;
                return PeerId::try_from(identity.to_owned())
                    .map(Submitter::Machine)
                    .map_err(|_| SenderBindingFailure::Unknown);
            }
            if let Some(fleet) = source.strip_prefix("f:") {
                let identity = fleet
                    .split('|')
                    .next()
                    .ok_or(SenderBindingFailure::Unknown)?;
                return FleetId::try_from(identity.to_owned())
                    .map(Submitter::Fleet)
                    .map_err(|_| SenderBindingFailure::Unknown);
            }
            // Unqualified session addresses in this fixture belong to its
            // explicitly known producer. They are not a public SDK fallback.
            return Ok(Submitter::Machine(peer(1)));
        }
        serde_json::from_value(value).map_err(|_| SenderBindingFailure::Unknown)
    }
}

async fn lookup_service<S: TransactionScope<Database = Turso>>(
    scope: &mut S,
    service: &str,
) -> Result<Submitter, SenderBindingFailure> {
    let rows = query::<Turso>("SELECT peer FROM test_sender_bindings WHERE service=?1")
        .bind(service.to_owned())
        .fetch_all(scope.executor())
        .await
        .map_err(|_| SenderBindingFailure::Unavailable)?;
    if rows.is_empty() {
        return Err(SenderBindingFailure::Unknown);
    }
    if rows.len() != 1 {
        return Err(SenderBindingFailure::Conflict);
    }
    let raw: String = rows
        .first()
        .ok_or(SenderBindingFailure::Conflict)?
        .try_get("peer")
        .map_err(|_| SenderBindingFailure::Conflict)?;
    let peer = PeerId::try_from(raw.clone()).map_err(|_| SenderBindingFailure::Conflict)?;
    let count = query::<Turso>(
        "SELECT COUNT(DISTINCT service) AS count FROM test_sender_bindings WHERE peer=?1",
    )
    .bind(raw)
    .fetch_one(scope.executor())
    .await
    .map_err(|_| SenderBindingFailure::Unavailable)?
    .try_get::<i64, _>("count")
    .map_err(|_| SenderBindingFailure::Conflict)?;
    if count != 1 {
        return Err(SenderBindingFailure::Conflict);
    }
    Ok(Submitter::Machine(peer))
}

#[expect(
    clippy::manual_async_fn,
    reason = "explicit Send for borrowed GAT transaction scope"
)]
pub fn create_binding_directory<S: TransactionScope<Database = Turso>>(
    scope: &mut S,
) -> impl Future<Output = Result<(), StoreError>> + Send + '_ {
    async move {
        query::<Turso>("CREATE TABLE test_sender_bindings(id TEXT PRIMARY KEY NOT NULL,service TEXT NOT NULL,peer TEXT NOT NULL,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL)").execute(scope.executor()).await?;
        Ok(())
    }
}
#[expect(
    clippy::manual_async_fn,
    reason = "explicit Send for borrowed GAT transaction scope"
)]
pub fn insert_binding<'a, S: TransactionScope<Database = Turso>>(
    scope: &'a mut S,
    identity: u64,
    service: &'a str,
    owner: PeerId,
) -> impl Future<Output = Result<(), StoreError>> + Send + 'a {
    async move {
        query::<Turso>("INSERT INTO test_sender_bindings(id,service,peer,created_at,updated_at) VALUES(?1,?2,?3,0,0)").bind(item_id(identity).as_uuid().to_string()).bind(service.to_owned()).bind(owner.as_uuid().to_string()).execute(scope.executor()).await?;
        Ok(())
    }
}
