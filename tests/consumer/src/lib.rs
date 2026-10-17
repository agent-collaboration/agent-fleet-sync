mod checked_queries;
mod consumer;
mod consumer_store;
mod control;
mod protocol;
mod sample_writes;
pub use sample_writes::{insert_reading, insert_sample, insert_source};
mod server;
mod writer_actor;
mod writer_control;
pub use consumer::{Consumer, FixtureAuthenticator};
pub use consumer_store::ConsumerStore;
pub use control::{ControlAnswer, ControlRequest};
pub use protocol::{
    EffectReceipt, HookEffect, SampleAnswer, SampleProtocol, SampleRequest, SampleSchema,
    SavedEffect,
};
pub use server::run_server;
pub use server::run_server_with_receiver;
pub use writer_actor::run_writer;
pub use writer_control::{WriterCommand, WriterReply};

pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
