//! One Router, one commit/effect order, one HTTP error policy.
use crate::{
    CallbackEnding, ProbeError, ReadDecideInsert, TransactionStore,
    numeric_cases::NumericRoundtrip, transaction_cases::ReadNames,
};
use axum::{Router, extract::State, http::StatusCode, routing::get};
use sqlx_core::{
    column::ColumnIndex, database::Database, decode::Decode, encode::Encode, types::Type,
};
use std::future::Future;

/// The probe emits one platform trace line after observing durable commit.
#[derive(Debug)]
pub struct CommittedEffect {
    pub request_name: String,
}

pub trait PlatformCapabilities: TransactionStore + Clone + Sync {
    fn run_effect(
        &self,
        effect: CommittedEffect,
    ) -> impl Future<Output = Result<String, ProbeError>> + Send;
}

pub fn shared_router<TPlatform>(platform: TPlatform) -> Router
where
    TPlatform: PlatformCapabilities,
    <TPlatform::Database as Database>::Arguments:
        sqlx_core::arguments::IntoArguments<TPlatform::Database>,
    for<'value> String: Type<TPlatform::Database>
        + Decode<'value, TPlatform::Database>
        + Encode<'value, TPlatform::Database>,
    for<'value> f64: Type<TPlatform::Database>
        + Decode<'value, TPlatform::Database>
        + Encode<'value, TPlatform::Database>,
    for<'value> Vec<u8>: Type<TPlatform::Database>
        + Decode<'value, TPlatform::Database>
        + Encode<'value, TPlatform::Database>,
    for<'value> Option<String>: Encode<'value, TPlatform::Database>,
    for<'value> bool: Type<TPlatform::Database> + Decode<'value, TPlatform::Database>,
    for<'row> &'row str: ColumnIndex<<TPlatform::Database as Database>::Row>,
{
    Router::new()
        .route("/effect", get(run_http::<TPlatform>))
        .route("/suite", get(run_suite::<TPlatform>))
        .route("/inspect", get(inspect::<TPlatform>))
        .with_state(platform)
}

async fn run_http<TPlatform>(
    State(platform): State<TPlatform>,
) -> Result<String, (StatusCode, &'static str)>
where
    TPlatform: PlatformCapabilities,
    <TPlatform::Database as Database>::Arguments:
        sqlx_core::arguments::IntoArguments<TPlatform::Database>,
    for<'value> String: Type<TPlatform::Database>
        + Decode<'value, TPlatform::Database>
        + Encode<'value, TPlatform::Database>,
    for<'row> &'row str: ColumnIndex<<TPlatform::Database as Database>::Row>,
{
    let request_name = platform
        .transaction(ReadDecideInsert {
            request_name: "http-committed".into(),
            ending: CallbackEnding::Commit,
        })
        .await
        .map_err(http_error)?;
    // Returning from transaction means the platform commit finished successfully.
    platform
        .run_effect(CommittedEffect { request_name })
        .await
        .map_err(http_error)
}

async fn run_suite<TPlatform>(
    State(platform): State<TPlatform>,
) -> Result<String, (StatusCode, &'static str)>
where
    TPlatform: PlatformCapabilities,
    <TPlatform::Database as Database>::Arguments:
        sqlx_core::arguments::IntoArguments<TPlatform::Database>,
    for<'value> String: Type<TPlatform::Database>
        + Decode<'value, TPlatform::Database>
        + Encode<'value, TPlatform::Database>,
    for<'value> f64: Type<TPlatform::Database>
        + Decode<'value, TPlatform::Database>
        + Encode<'value, TPlatform::Database>,
    for<'value> Vec<u8>: Type<TPlatform::Database>
        + Decode<'value, TPlatform::Database>
        + Encode<'value, TPlatform::Database>,
    for<'value> Option<String>: Encode<'value, TPlatform::Database>,
    for<'value> bool: Type<TPlatform::Database> + Decode<'value, TPlatform::Database>,
    for<'row> &'row str: ColumnIndex<<TPlatform::Database as Database>::Row>,
{
    let transactions = crate::transaction_cases::run_cases(&platform)
        .await
        .map_err(http_error)?;
    let numbers = platform
        .transaction(NumericRoundtrip)
        .await
        .map_err(http_error)?;
    let interruption = crate::transaction_cases::observe_interruption(&platform)
        .await
        .map_err(http_error)?;
    Ok(format!("{transactions}; {numbers}; {interruption}"))
}

async fn inspect<TPlatform>(
    State(platform): State<TPlatform>,
) -> Result<String, (StatusCode, &'static str)>
where
    TPlatform: PlatformCapabilities,
    <TPlatform::Database as Database>::Arguments:
        sqlx_core::arguments::IntoArguments<TPlatform::Database>,
    for<'value> String: Type<TPlatform::Database> + Decode<'value, TPlatform::Database>,
    for<'row> &'row str: ColumnIndex<<TPlatform::Database as Database>::Row>,
{
    platform
        .transaction(ReadNames)
        .await
        .map(|rows| format!("committed rows={rows:?}"))
        .map_err(http_error)
}

pub fn validate_effect_visibility(
    rows: &[String],
    effect: &CommittedEffect,
) -> Result<String, ProbeError> {
    if !rows.contains(&effect.request_name) {
        return Err(ProbeError::Rejected);
    }
    Ok(format!(
        "effect observed after durable commit: {}",
        effect.request_name
    ))
}

fn http_error(error: ProbeError) -> (StatusCode, &'static str) {
    match error {
        ProbeError::Rejected => (StatusCode::CONFLICT, "domain rejected"),
        ProbeError::Storage(_) => (StatusCode::SERVICE_UNAVAILABLE, "storage failed"),
    }
}
