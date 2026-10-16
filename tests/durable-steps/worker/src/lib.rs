#[path = "../../../../crates/durable-steps/tests/conformance/mod.rs"]
mod conformance;
#[path = "../../journey_task.rs"]
mod journey_task;
mod journey_worker;
#[path = "../../../../crates/durable-steps/tests/support/mod.rs"]
mod support;
pub use journey_worker::{JourneyObject, ReceiverObject};

use conformance::BusinessScope;
use durable_steps::{
    AtomicStore, Host, JournalScope, StoreError,
    durable_object_store::{DurableObjectScope, DurableObjectStore},
    storage::TransactionScope,
};
use runtime_primitives::{AlarmOwner, Clock};
use sqlx_core::{executor::Executor, query::query, query_as::query_as};
use sqlx_durable_object::{Do, DoConnection};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use support::TestHost;
use worker::{Context, DurableObject, Env, Request, Response, State, durable_object, event};

#[derive(sqlx::FromRow)]
struct CounterRow {
    #[sqlx(rename = "value!: i64")]
    value: i64,
}
impl<H: Host> BusinessScope for DurableObjectScope<'_, H> {
    async fn read_counter(&mut self, name: &str) -> Result<Option<u64>, StoreError> {
        query_as::<Do, CounterRow>(include_str!(
            "../../../../crates/durable-steps/tests/conformance/queries/read_counter.sql"
        ))
        .bind(name)
        .fetch_optional(self.executor())
        .await?
        .map(|row| u64::try_from(row.value).map_err(StoreError::backend))
        .transpose()
    }
    async fn write_counter(&mut self, name: &str, value: u64) -> Result<(), StoreError> {
        query::<Do>(include_str!(
            "../../../../crates/durable-steps/tests/conformance/queries/write_counter.sql"
        ))
        .bind(name)
        .bind(i64::try_from(value).map_err(StoreError::backend)?)
        .execute(self.executor())
        .await?;
        Ok(())
    }
}

#[durable_object]
pub struct StepsObject {
    state: State,
    store: Arc<DurableObjectStore<TestHost>>,
    host: Arc<TestHost>,
}
impl DurableObject for StepsObject {
    fn new(state: State, _env: Env) -> Self {
        let host = TestHost::new();
        // Deliberately controlled conformance time, safely ahead of the real
        // scheduler. The separate alarm journey uses the real platform clock.
        let now = i64::try_from(worker::Date::now().as_millis()).unwrap_or(i64::MAX);
        host.clock
            .store(now.saturating_add(3_600_000), Ordering::SeqCst);
        let alarm = AlarmOwner::new(state.storage());
        let store = Arc::new(DurableObjectStore::new(
            DoConnection::new(state.storage()),
            Arc::clone(&host),
            alarm,
        ));
        Self { state, store, host }
    }
    async fn fetch(&self, request: Request) -> worker::Result<Response> {
        self.store.migrate().await.map_err(worker_error)?;
        self.store.transaction(|scope| Box::pin(async move {
            scope.executor().execute(include_str!("../../../../crates/durable-steps/tests/conformance/migrations/1000_business.sql")).await?;
            Ok(())
        })).await.map_err(worker_error)?;
        match request.path().as_str() {
            "/suite" => {
                let passed = conformance::run(Arc::clone(&self.store), Arc::clone(&self.host))
                    .await
                    .map_err(worker_error)?;
                self.state.storage().sync().await?;
                Response::from_json(&passed)
            }
            "/alarm-atomic" => {
                let later = self.host.now().after(Duration::from_secs(100));
                let earlier = self.host.now().after(Duration::from_secs(50));
                let aborted = self
                    .store
                    .transaction(move |scope| {
                        Box::pin(async move {
                            scope.write_counter("alarm-rollback", 1).await?;
                            scope.ensure_wake(earlier).await?;
                            Err::<(), _>(StoreError::Aborted {
                                reason: "alarm rollback".into(),
                            })
                        })
                    })
                    .await;
                conformance::check(
                    matches!(aborted, Err(StoreError::Aborted { .. }))
                        && self.state.storage().get_alarm().await?.is_none(),
                    "alarm and business writes roll back together",
                )
                .map_err(worker_error)?;
                self.store
                    .transaction(move |scope| {
                        Box::pin(async move {
                            conformance::check(
                                scope.read_counter("alarm-rollback").await?.is_none(),
                                "alarm business write rolled back",
                            )?;
                            scope.ensure_wake(later).await?;
                            scope.ensure_wake(earlier).await?;
                            scope.ensure_wake(later).await?;
                            Ok(())
                        })
                    })
                    .await
                    .map_err(worker_error)?;
                let actual = self.state.storage().get_alarm().await?;
                conformance::check(
                    actual == Some(earlier.unix_millis()),
                    "absolute earliest alarm survives later requests",
                )
                .map_err(worker_error)?;
                Response::from_json(&actual)
            }
            _ => Response::error("unknown fixture path", 404),
        }
    }
}
fn worker_error(error: impl std::fmt::Display) -> worker::Error {
    worker::Error::RustError(error.to_string())
}

#[event(fetch)]
async fn fetch(request: Request, env: Env, _context: Context) -> worker::Result<Response> {
    let journey = request.path().starts_with("/journey/");
    let namespace = env.durable_object(if journey { "JOURNEY" } else { "STEPS" })?;
    let object = env
        .var(if journey {
            "JOURNEY_OBJECT_NAME"
        } else {
            "STEPS_OBJECT_NAME"
        })?
        .to_string();
    namespace
        .id_from_name(&object)?
        .get_stub()?
        .fetch_with_request(request)
        .await
}
