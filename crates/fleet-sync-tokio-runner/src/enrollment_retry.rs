//! Enrollment admission refusals stay pending before a durable call caches an answer.
use durable_steps::{CallFailure, CallRetryReason};
use fleet_sync_protocol::{
    LeaseRefusal, PieceRefusal, RegistrationAnswer, RegistrationRefusal, RenewAnswer,
    ReplicationAnswer,
};

fn retry() -> CallFailure {
    CallFailure::Retryable {
        reason: CallRetryReason {
            code: "callerNotEnrolled".into(),
            detail: "Machine is not enrolled or its enrollment has expired.".into(),
        },
    }
}
pub(crate) fn registration(answer: RegistrationAnswer) -> Result<serde_json::Value, CallFailure> {
    if matches!(&answer,RegistrationAnswer::Refused(RegistrationRefusal::DatabaseRejected{reason}) if reason.code.as_str()=="callerNotEnrolled")
    {
        return Err(retry());
    }
    serde_json::to_value(answer).map_err(|_| CallFailure::AnswerLost)
}
pub(crate) fn renewal(answer: RenewAnswer) -> Result<serde_json::Value, CallFailure> {
    if matches!(
        &answer,
        RenewAnswer::Refused(LeaseRefusal::HolderNotEnrolled)
    ) {
        return Err(retry());
    }
    serde_json::to_value(answer).map_err(|_| CallFailure::AnswerLost)
}
pub(crate) fn replication(answer: ReplicationAnswer) -> Result<ReplicationAnswer, CallFailure> {
    if matches!(&answer,ReplicationAnswer::Refused(PieceRefusal::Hook(reason)) if reason.code.as_str()=="callerNotEnrolled")
    {
        return Err(retry());
    }
    Ok(answer)
}
#[cfg(test)]
#[path = "enrollment_retry_test.rs"]
mod tests;
