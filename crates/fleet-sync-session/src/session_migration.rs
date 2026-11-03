//! Validate the old wire records before applying the typed storage corpus.
use crate::{checked_legacy_queries as legacy, *};
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments,
    column::ColumnIndex,
    connection::Connection,
    database::Database,
    decode::Decode,
    encode::Encode,
    executor::Executor,
    migrate::{Migrate, MigrateError, Migration, Migrator},
    types::Type,
};

#[derive(Debug, thiserror::Error)]
pub enum SessionMigrationError {
    #[error("invalid legacy {table} row {key}: {field}")]
    InvalidLegacyRow {
        table: &'static str,
        key: String,
        field: &'static str,
    },
    #[error(transparent)]
    Sql(#[from] sqlx_core::Error),
    #[error(transparent)]
    Migration(#[from] MigrateError),
}
fn invalid(table: &'static str, key: &str, field: &'static str) -> SessionMigrationError {
    SessionMigrationError::InvalidLegacyRow {
        table,
        key: key.into(),
        field,
    }
}
fn required(
    value: Option<String>,
    table: &'static str,
    key: &str,
    field: &'static str,
) -> Result<String, SessionMigrationError> {
    value.ok_or_else(|| invalid(table, key, field))
}

pub struct SessionMigrator {
    sql: Migrator,
}
pub static SESSION_MIGRATOR: SessionMigrator = SessionMigrator {
    sql: sqlx::migrate!("./migrations"),
};
impl SessionMigrator {
    /// SQL fragments for hosts that run `validate_legacy_rows` in their corpus transaction.
    pub fn sql_migrations(&self) -> impl DoubleEndedIterator<Item = &Migration> {
        self.sql.iter()
    }
    pub fn with_history_table(&self, name: &str) -> Self {
        let mut sql = Migrator::with_migrations(self.sql.iter().cloned().collect::<Vec<_>>());
        sql.dangerous_set_table_name(name.to_owned());
        Self { sql }
    }
    #[expect(
        clippy::manual_async_fn,
        reason = "explicit Send keeps the connection-backed migration usable in host transactions"
    )]
    pub fn run_direct<'a, C>(
        &'a self,
        target: Option<i64>,
        connection: &'a mut C,
        skip: bool,
    ) -> impl std::future::Future<Output = Result<(), SessionMigrationError>> + Send + 'a
    where
        C: Connection + Migrate,
        for<'q> &'q mut C: Executor<'q, Database = C::Database>,
        <C::Database as Database>::Arguments: IntoArguments<C::Database>,
        for<'v> String: Type<C::Database> + Encode<'v, C::Database>,
        for<'v> Option<String>: Type<C::Database> + Decode<'v, C::Database>,
        for<'r> &'r str: ColumnIndex<<C::Database as Database>::Row>,
    {
        async move {
            let mut transaction = if connection.is_in_transaction() {
                connection.begin().await?
            } else {
                connection.begin_with("BEGIN IMMEDIATE").await?
            };
            let result = async {
                if target.is_none_or(|version| version >= 202611030001) {
                    self.sql
                        .run_direct(Some(202610180001), &mut *transaction, skip)
                        .await?;
                    validate_legacy_rows(&mut ConnectionScope(&mut *transaction)).await?;
                }
                self.sql.run_direct(target, &mut *transaction, skip).await?;
                Ok(())
            }
            .await;
            match result {
                Ok(()) => {
                    transaction.commit().await?;
                    Ok(())
                }
                Err(error) => {
                    transaction.rollback().await?;
                    Err(error)
                }
            }
        }
    }
}
struct ConnectionScope<'a, C>(&'a mut C);
impl<C> TransactionScope for ConnectionScope<'_, C>
where
    C: Connection,
    for<'q> &'q mut C: Executor<'q, Database = C::Database>,
{
    type Database = C::Database;
    type Executor<'q>
        = &'q mut C
    where
        Self: 'q;
    fn executor(&mut self) -> Self::Executor<'_> {
        self.0
    }
}

// The consumer arm is intentionally opaque. Its fields never become library columns.
struct LegacyConsumer;
impl ConsumerProtocol for LegacyConsumer {
    type Delivery = NoDelivery;
    type Request = ();
    type Answer = ();
    type ServerRequest = ();
    type ClientAnswer = ();
    type Notice = serde_json::Value;
    type Annotation = ();
}

/// Call in the same transaction that applies `sql_migrations`, after checking its prefix.
#[expect(
    clippy::manual_async_fn,
    reason = "explicit Send preserves the borrowed-scope/GAT migration boundary"
)]
pub fn validate_legacy_rows<'a, S>(
    scope: &'a mut S,
) -> impl std::future::Future<Output = Result<(), SessionMigrationError>> + Send + 'a
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    async move {
        let sessions = legacy::legacy_columns(scope, "_fleet_sync_sessions".into()).await?;
        let sessions_old = sessions
            .iter()
            .any(|column| column.column_name.as_deref() == Some("record"));
        if !sessions.is_empty()
            && !sessions_old
            && !sessions
                .iter()
                .any(|column| column.column_name.as_deref() == Some("state_kind"))
        {
            return Err(invalid("_fleet_sync_sessions", "<schema>", "layout"));
        }
        if sessions_old {
            for row in legacy::legacy_sessions(scope).await? {
                let key = format!(
                    "{}/{} [rowid {}]",
                    row.peer.as_deref().unwrap_or("<missing>"),
                    row.session_id.as_deref().unwrap_or("<missing>"),
                    row.legacy_rowid.as_deref().unwrap_or("<missing>")
                );
                let table = "_fleet_sync_sessions";
                let peer = PeerId::try_from(required(row.peer, table, &key, "peer")?)
                    .map_err(|_| invalid(table, &key, "peer"))?;
                let session =
                    SessionId::try_from(required(row.session_id, table, &key, "session_id")?)
                        .map_err(|_| invalid(table, &key, "session_id"))?;
                // This is the same domain wire decoder used by session frame and record reads.
                let record: SessionRecord =
                    serde_json::from_str(&required(row.record, table, &key, "record")?)
                        .map_err(|_| invalid(table, &key, "record"))?;
                if record.peer != peer || record.session_id != session {
                    return Err(invalid(table, &key, "identity"));
                }
            }
        }
        let notices = legacy::legacy_columns(scope, "_fleet_sync_notices".into()).await?;
        let notices_old = notices
            .iter()
            .any(|column| column.column_name.as_deref() == Some("body"));
        if !notices.is_empty()
            && !notices_old
            && !notices
                .iter()
                .any(|column| column.column_name.as_deref() == Some("notice_kind"))
        {
            return Err(invalid("_fleet_sync_notices", "<schema>", "layout"));
        }
        if notices_old {
            for row in legacy::legacy_notices(scope).await? {
                let table = "_fleet_sync_notices";
                let key = format!(
                    "{}/{}/{} [rowid {}]",
                    row.peer.as_deref().unwrap_or("<missing>"),
                    row.session_id.as_deref().unwrap_or("<missing>"),
                    row.position.as_deref().unwrap_or("<missing>"),
                    row.legacy_rowid.as_deref().unwrap_or("<missing>")
                );
                PeerId::try_from(required(row.peer, table, &key, "peer")?)
                    .map_err(|_| invalid(table, &key, "peer"))?;
                SessionId::try_from(required(row.session_id, table, &key, "session_id")?)
                    .map_err(|_| invalid(table, &key, "session_id"))?;
                let position = crate::session_storage::canonical_counter(
                    required(row.position, table, &key, "position")?,
                    "position",
                )
                .map_err(|_| invalid(table, &key, "position"))?;
                if position == 0 {
                    return Err(invalid(table, &key, "position"));
                }
                let _: Notice<LegacyConsumer> =
                    serde_json::from_str(&required(row.body, table, &key, "body")?)
                        .map_err(|_| invalid(table, &key, "body"))?;
            }
        }
        if sessions_old || notices_old {
            let columns =
                legacy::legacy_columns(scope, "_fleet_sync_attach_sequences".into()).await?;
            if !columns.is_empty() {
                if !columns
                    .iter()
                    .any(|column| column.column_name.as_deref() == Some("sequence"))
                {
                    return Err(invalid(
                        "_fleet_sync_attach_sequences",
                        "<schema>",
                        "layout",
                    ));
                }
                for row in legacy::legacy_attach_sequences(scope).await? {
                    let table = "_fleet_sync_attach_sequences";
                    let key = format!(
                        "{} [rowid {}]",
                        row.peer.as_deref().unwrap_or("<missing>"),
                        row.legacy_rowid.as_deref().unwrap_or("<missing>")
                    );
                    PeerId::try_from(required(row.peer, table, &key, "peer")?)
                        .map_err(|_| invalid(table, &key, "peer"))?;
                    let value = crate::session_storage::canonical_counter(
                        required(row.sequence, table, &key, "sequence")?,
                        "sequence",
                    )
                    .map_err(|_| invalid(table, &key, "sequence"))?;
                    AttachSequence::new(value).map_err(|_| invalid(table, &key, "sequence"))?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "session_migration_test.rs"]
mod tests;
