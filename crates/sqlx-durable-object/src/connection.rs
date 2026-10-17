use std::fmt::{self, Debug, Formatter};
use std::future::{self, Future};
use std::str::FromStr;
use std::time::Duration;

use log::LevelFilter;
use sqlx_core::Url;
use sqlx_core::connection::{ConnectOptions, Connection};
use sqlx_core::error::Error;
use sqlx_core::transaction::Transaction;

use crate::Do;

/// A connection to a Durable Object's SQL storage.
///
/// There is no socket behind it and nothing to pool: the database lives in
/// the Durable Object itself. Build one from the object's storage with
/// [`DoConnection::new`], in its constructor or per request.
///
/// Both `&mut DoConnection` and `&DoConnection` are sqlx executors, so a
/// connection kept in the object's struct serves every request through
/// `&self`.
pub struct DoConnection {
    /// For [`execute_batch`](Self::execute_batch) and
    /// [`fetch_batch`](Self::fetch_batch), which run through
    /// `Storage::transaction`.
    pub(crate) storage: worker::send::SendWrapper<worker::Storage>,
    /// `storage.sql()`, kept rather than fetched for every query.
    pub(crate) sql: worker::SqlStorage,
}

impl DoConnection {
    /// Wraps a Durable Object's storage: `state.storage()`.
    #[must_use]
    pub fn new(storage: worker::Storage) -> Self {
        let sql = storage.sql();
        Self {
            storage: worker::send::SendWrapper::new(storage),
            sql,
        }
    }

    /// The storage this connection wraps, for anything this crate does not
    /// cover -- the key-value API, alarms -- while keeping the connection.
    #[must_use]
    pub fn storage(&self) -> &worker::Storage {
        &self.storage
    }

    /// The SQL API this connection runs queries through.
    #[must_use]
    pub fn sql(&self) -> &worker::SqlStorage {
        &self.sql
    }

    /// The storage this connection wraps, giving up the connection. See
    /// [`storage`](Self::storage) to borrow it instead.
    #[must_use]
    pub fn into_storage(self) -> worker::Storage {
        self.storage.0
    }
}

impl From<worker::Storage> for DoConnection {
    fn from(storage: worker::Storage) -> Self {
        Self::new(storage)
    }
}

impl Debug for DoConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("DoConnection").finish_non_exhaustive()
    }
}

impl Connection for DoConnection {
    type Database = Do;

    type Options = DoConnectOptions;

    // Nothing to close: the storage belongs to the Durable Object, not to us.
    fn close(self) -> impl Future<Output = Result<(), Error>> + Send + 'static {
        future::ready(Ok(()))
    }

    fn close_hard(self) -> impl Future<Output = Result<(), Error>> + Send + 'static {
        future::ready(Ok(()))
    }

    // The database is in-process: there is nothing to go stale.
    fn ping(&mut self) -> impl Future<Output = Result<(), Error>> + Send + '_ {
        future::ready(Ok(()))
    }

    /// Always fails -- see [`DoTransactionManager`](crate::DoTransactionManager).
    fn begin(&mut self) -> impl Future<Output = Result<Transaction<'_, Do>, Error>> + Send + '_ {
        Transaction::begin(self, None)
    }

    fn shrink_buffers(&mut self) {}

    fn flush(&mut self) -> impl Future<Output = Result<(), Error>> + Send + '_ {
        future::ready(Ok(()))
    }

    fn should_flush(&self) -> bool {
        false
    }
}

/// The options type sqlx requires of every connection.
///
/// It parses `do://` so the type is constructible, but
/// [`connect`](ConnectOptions::connect) always fails: a connection comes from
/// a Durable Object's storage, which only the object itself holds. Use
/// [`DoConnection::new`].
///
/// Non-exhaustive, so parsing is the only way to get one -- as sqlx itself
/// does -- and a field can be added without breaking anyone.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct DoConnectOptions {
    url: Url,
}

impl FromStr for DoConnectOptions {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        let url = Url::parse(s).map_err(|error| Error::Configuration(error.into()))?;
        Self::from_url(&url)
    }
}

impl ConnectOptions for DoConnectOptions {
    type Connection = DoConnection;

    fn from_url(url: &Url) -> Result<Self, Error> {
        if url.scheme() == "do" {
            Ok(Self { url: url.clone() })
        } else {
            Err(Error::Configuration(
                format!("expected a `do://` URL, got scheme `{}`", url.scheme()).into(),
            ))
        }
    }

    fn to_url_lossy(&self) -> Url {
        self.url.clone()
    }

    fn connect(&self) -> impl Future<Output = Result<DoConnection, Error>> + Send + '_ {
        future::ready(Err(Error::Configuration(
            "a Durable Object connection cannot be opened from a URL; build it from the object's \
             storage with `DoConnection::new(state.storage())`"
                .into(),
        )))
    }

    // No statement logging yet, so there is nothing for these to configure.
    fn log_statements(self, _level: LevelFilter) -> Self {
        self
    }

    fn log_slow_statements(self, _level: LevelFilter, _duration: Duration) -> Self {
        self
    }
}

#[cfg(test)]
mod tests {
    use sqlx_core::connection::ConnectOptions;

    use super::DoConnectOptions;

    #[test]
    fn parses_a_do_url() {
        let options: DoConnectOptions = "do://".parse().unwrap();
        assert_eq!(options.to_url_lossy().as_str(), "do://");
    }

    #[test]
    fn rejects_other_schemes() {
        assert!("sqlite://db".parse::<DoConnectOptions>().is_err());
        assert!("d1://DB".parse::<DoConnectOptions>().is_err());
    }
}
