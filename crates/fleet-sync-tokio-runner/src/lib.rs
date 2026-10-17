//! Tokio owns files, locks, calls and timers; rules own domain decisions.

mod call_host;
mod client_pump;
#[cfg(feature = "test-support")]
mod seed_answer_probe;
mod writer_installer;
mod writer_runtime;
#[cfg(feature = "test-support")]
pub use seed_answer_probe::SeedPageObservation;
mod writer_session_calls;
pub use client_pump::{SessionPump, SessionRequests};
pub use writer_runtime::{NativeWriterRuntime, NativeWriterStore, WriterHost};
pub use writer_session_calls::WriterSessionCalls;
mod checked_native_queries;
mod install_calls;
mod install_contract;
mod install_coordinator;
#[cfg(test)]
mod install_coordinator_tests;
#[cfg(test)]
mod install_failure_tests;
mod install_records;
mod install_staging_calls;
mod install_task;
pub use install_calls::{
    InstallFile, InstallFileRequest, InstallFileResult, InstallRefusal, InstallSource,
};
pub use install_contract::{
    InstallFileAnswer, InstallProgress, InstallRecord, InstallRequest, InstallStatus,
};
pub use install_coordinator::{InstallCoordinator, InstallHandle};
pub use install_task::InstallSnapshot;
mod native_carry;
mod native_carry_rows;
mod native_carry_schema;
pub use native_carry::{
    LocalStateCarried, LocalStateCarryError, LocalStateSelection, carry_local_state,
};
mod install_target;
#[cfg(test)]
mod native_carry_tests;
mod native_object;
mod native_publish;
mod object_key;
mod object_runner;
#[cfg(test)]
mod object_store_tests;
mod session_host;
mod session_peer_call;
mod session_socket;
mod staging_publish;
pub use install_target::{SnapshotTarget, WriterInstallTarget};
#[cfg(feature = "test-support")]
mod writer_transport_probe;
#[cfg(feature = "test-support")]
pub use writer_transport_probe::WriterTransportObservation;
mod staging_store;
pub use staging_store::{StagedPage, StagedVerification, StagingError, StagingStore};
#[cfg(test)]
mod staging_store_tests;
#[cfg(test)]
mod writer_bootstrap_fixture;
#[cfg(test)]
mod writer_bootstrap_tests;

pub use call_host::{CallEnvelope, CallRouter, TokioHost};
pub use native_object::{ObjectOptions, ObjectStore, TokioObject};
pub use native_publish::PublishError;
pub use object_key::{InvalidObjectKey, ObjectKey};
pub use object_runner::TokioRunner;
pub use session_host::{SessionAdmission, SessionHandler, SessionHost, SessionHostError};
#[cfg(test)]
mod native_publish_tests;
