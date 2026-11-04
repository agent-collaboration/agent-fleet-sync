//! Delivery shares the reference service's existing registry store and engine.
use crate::{RelayDeliveryConfig, ServiceConsumer, ServiceError, ServiceHost, ServiceObject};
use durable_steps::{AtomicStore, Engine, StoreError, storage::TransactionScope};
use fleet_sync_delivery::{DELIVERY_MIGRATOR, PublishStatus, PushItem, RemoveDeliveryRecord};
use fleet_sync_protocol::ConsumerProtocol;
use runtime_primitives::Clock;
use sqlx_turso::Turso;
use std::sync::Arc;

pub(crate) type RelayPublication<D> = PublishStatus<ServiceObject, Turso, D>;
pub(crate) type RelayPush<D, C> = PushItem<ServiceObject, Turso, D, RelayPublication<D>, C>;
pub(crate) type RelayRemoval = RemoveDeliveryRecord<ServiceObject, Turso>;

pub(crate) async fn install<P: ConsumerProtocol, C: ServiceConsumer<P>>(
    object: &Arc<ServiceObject>,
    engine: &mut Engine<ServiceObject, ServiceHost>,
    config: &RelayDeliveryConfig,
    binding: Arc<C>,
) -> Result<(), ServiceError> {
    let now = object.host().now();
    let bindings = binding.clone();
    object
        .transaction(move |scope| {
            Box::pin(async move {
                let mut migrations = sqlx_core::migrate::Migrator::with_migrations(
                    DELIVERY_MIGRATOR.iter().cloned().collect::<Vec<_>>(),
                );
                migrations.dangerous_set_table_name("_fleet_sync_delivery_migrations");
                migrations
                    .run_direct(None, scope.executor(), false)
                    .await
                    .map_err(StoreError::backend)?;
                fleet_sync_delivery::retire_relay_host_tables(scope)
                    .await
                    .map_err(fleet_sync_delivery::delivery_rollback)?;
                fleet_sync_delivery::backfill_submitters::<P::Delivery, _, _>(
                    scope,
                    bindings.as_ref(),
                    now,
                )
                .await
                .map_err(fleet_sync_delivery::delivery_rollback)?;
                fleet_sync_delivery::admit_existing_record_removals::<_, RelayRemoval>(scope, now)
                    .await
                    .map_err(StoreError::backend)?;
                Ok(())
            })
        })
        .await?;
    engine.register(RelayPublication::<P::Delivery>::new())?;
    engine.register(RelayPush::<P::Delivery, C>::new(
        config.limits,
        object.host(),
        binding,
    ))?;
    engine.register(RelayRemoval::new())?;
    Ok(())
}
