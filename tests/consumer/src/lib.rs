mod checked_queries;
mod consumer;
mod consumer_claim_gate;
mod consumer_store;
mod control;
mod copy_faults;
pub use copy_faults::CopyFaultRequest;
mod schema_control;
pub use schema_control::{SchemaControlAnswer, SchemaControlRequest};
mod fixture_authenticator;
mod protocol;
mod sample_writes;
pub use sample_writes::{insert_reading, insert_sample, insert_source};
#[cfg(feature = "test-support")]
mod delivery_faults;
mod server;
#[cfg(feature = "test-support")]
mod service_events;
#[cfg(feature = "test-support")]
pub use service_events::ServiceEvent;
mod writer_actor;
mod writer_bootstrap;
mod writer_control;
mod writer_move;
pub use consumer::Consumer;
pub use consumer_store::ConsumerStore;
pub use control::{ControlAnswer, ControlRequest};
pub use fixture_authenticator::FixtureAuthenticator;
pub use protocol::{
    ClaimGateReceipt, ClaimSettlementReceipt, EffectReceipt, HookEffect, SampleAnswer,
    SampleEmptyRequest, SampleProtocol, SampleRequest, SampleSchema, SavedEffect,
};
pub use server::run_server;
pub use server::run_server_with_policy;
pub use server::run_server_with_receiver;
pub use writer_actor::run_writer;
pub use writer_control::{WriterCommand, WriterReply};
pub use writer_move::run_move;

pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

mod delivery_protocol;
pub use delivery_protocol::{SampleDelivery, delivery_limits};

#[cfg(feature = "test-support")]
pub mod delivery_node;
