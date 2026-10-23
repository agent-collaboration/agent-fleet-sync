//! A durable cross-object report is the barrier between Hello and Welcome.
use crate::session_storage::load_session;
use crate::{ConnectionRegistration, SessionError, SessionKey, SessionScope, SessionState};
use durable_steps::{
    AtomicStore, CallTarget, OperationContext, OperationFailure, RequestKey, StepName, StoreError,
    Task,
};
use fleet_sync_protocol::*;
use serde::{Deserialize, Serialize};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::marker::PhantomData;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PublishSchemaSupportInput {
    pub connection: ConnectionId,
    pub expires_at: SyncTime,
    pub report: ReportSchemaSupport,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct SchemaSupportRegistry;
impl CallTarget for SchemaSupportRegistry {
    const KIND: &'static str = "fleet-sync.report-schema-support";
    type Request = ReportSchemaSupport;
    type Answer = SchemaSupportRecorded;
}

pub fn schema_support_request_key(report: &ReportSchemaSupport) -> Result<RequestKey, StoreError> {
    RequestKey::new(format!("{}:{}", report.peer, report.attach_seq.value()))
        .map_err(StoreError::backend)
}

pub struct PublishSchemaSupport<S, D>(PhantomData<fn() -> (S, D)>);
impl<S, D> Default for PublishSchemaSupport<S, D> {
    fn default() -> Self {
        Self(PhantomData)
    }
}
impl<S, D> Task for PublishSchemaSupport<S, D>
where
    S: AtomicStore,
    D: Database,
    for<'t> S::Scope<'t>: SessionScope<Database = D>,
    D::Arguments: IntoArguments<D>,
    for<'v> String: Type<D> + Encode<'v, D> + Decode<'v, D>,
    for<'v> Option<String>: Type<D> + Encode<'v, D> + Decode<'v, D>,
    for<'r> &'r str: ColumnIndex<D::Row>,
{
    const KIND: &'static str = "fleet-sync.publish-schema-support";
    const VERSION: u32 = 1;
    type Store = S;
    type Input = PublishSchemaSupportInput;
    type Output = ();
    async fn run<C: OperationContext<Store = S>>(
        &self,
        ctx: &mut C,
        input: &Self::Input,
    ) -> Result<(), OperationFailure> {
        ctx.call(
            StepName::new("report").map_err(|error| OperationFailure::InvalidPayload {
                message: error.to_string(),
            })?,
            SchemaSupportRegistry,
            input.report.clone(),
        )
        .await?;
        let input = input.clone();
        let now = ctx.now();
        ctx.local(
            StepName::new("attach").map_err(|error| OperationFailure::InvalidPayload {
                message: error.to_string(),
            })?,
            move |scope| {
                Box::pin(async move {
                    complete_schema_publication(scope, &input, now)
                        .await
                        .map_err(|error| StoreError::backend(error).into())
                })
            },
        )
        .await?;
        Ok(())
    }
}

/// Complete only the still-live attach that originated the accepted report.
pub async fn complete_schema_publication<S>(
    scope: &mut S,
    input: &PublishSchemaSupportInput,
    now: SyncTime,
) -> Result<(), SessionError>
where
    S: SessionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    let report = &input.report;
    if now >= input.expires_at
        || scope.registration(&input.connection)
            != Some(ConnectionRegistration::Attaching {
                session_id: report.session_id,
                schema_support: report.support,
                attach_seq: report.attach_seq,
            })
    {
        return Ok(());
    }
    let key = SessionKey {
        peer: report.peer,
        session_id: report.session_id,
    };
    if load_session(scope, &key).await?.is_none_or(|record| {
        record.state
            != (SessionState::Active {
                connection: input.connection,
            })
    }) {
        return Ok(());
    }
    scope.set_registration(
        &input.connection,
        ConnectionRegistration::Attached {
            session_id: report.session_id,
            schema_support: report.support,
        },
    );
    scope.welcome_after_commit(input.connection, report.peer, report.session_id);
    Ok(())
}
