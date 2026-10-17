mod checked_queries;
mod consumer;
mod consumer_store;
mod control;
mod protocol;
mod sample_writes;
pub use sample_writes::{insert_reading, insert_sample, insert_source};
mod server;
pub use consumer::{Consumer, FixtureAuthenticator};
pub use consumer_store::ConsumerStore;
pub use control::{ControlAnswer, ControlRequest};
pub use protocol::{
    EffectReceipt, HookEffect, SampleAnswer, SampleProtocol, SampleRequest, SampleSchema,
    SavedEffect,
};
pub use server::run_server;

pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
