use fleet_sync_protocol::TableName;
pub use sqlx_core::migrate::Migration;

pub trait ReplicatedSchema: Send + Sync + 'static {
    fn migrations(&self) -> &'static [Migration];
    fn local_migrations(&self) -> &'static [Migration] {
        &[]
    }
    fn replicated_tables(&self) -> &'static [TableName];
}

#[derive(Clone, Copy, Debug, Default)]
pub struct WriterOptions {
    pub allow_provisional_writes: bool,
}
