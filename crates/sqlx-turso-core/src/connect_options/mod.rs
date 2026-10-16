//! How to open one Turso store: target, open mode and engine settings

mod connection_url;
mod database_target;

use std::{path::Path, sync::Arc, time::Duration};

use sqlx_core::{
    connection::{ConnectOptions, LogSettings},
    error::Error,
};
use url::Url;

pub(crate) use database_target::OpenMode;
pub use database_target::TursoDatabaseTarget;

use crate::{connection::TursoConnection, engine_connection::EngineConnection};

const DEFAULT_STATEMENT_CACHE_CAPACITY: usize = 100;
const DEFAULT_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Typed connection options for a Turso database
///
/// Defaults match SQLx's SQLite driver where they overlap: a 5 second busy timeout, a
/// 100-statement cache and foreign keys enforced.
#[derive(Clone, Debug)]
pub struct TursoConnectOptions(Arc<ConnectOptionsInner>);

#[derive(Clone, Debug)]
struct ConnectOptionsInner {
    target: TursoDatabaseTarget,
    open_mode: OpenMode,
    busy_timeout: Duration,
    statement_cache_capacity: usize,
    foreign_keys: bool,
    log_settings: LogSettings,
}

impl TursoConnectOptions {
    /// Creates options for a private in-memory database with the default settings
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns where the database lives
    pub fn target(&self) -> &TursoDatabaseTarget {
        &self.0.target
    }

    /// Returns the file path for file-backed databases
    pub fn get_filename(&self) -> Option<&Path> {
        match &self.0.target {
            TursoDatabaseTarget::Memory => None,
            TursoDatabaseTarget::File(path) => Some(path),
        }
    }

    /// Returns whether the target is an in-memory database
    pub fn is_in_memory(&self) -> bool {
        matches!(self.0.target, TursoDatabaseTarget::Memory)
    }

    /// Returns whether a missing database file is created on open
    pub fn get_create_if_missing(&self) -> bool {
        self.0.open_mode == OpenMode::CreateIfMissing
    }

    /// Returns the driver's maximum lock-wait budget for a statement
    pub fn get_busy_timeout(&self) -> Duration {
        self.0.busy_timeout
    }

    /// Returns the prepared-statement cache capacity
    pub fn get_statement_cache_capacity(&self) -> usize {
        self.0.statement_cache_capacity
    }

    /// Returns whether connections enforce foreign keys
    pub fn get_foreign_keys(&self) -> bool {
        self.0.foreign_keys
    }

    /// Returns SQLx statement logging settings
    pub fn log_settings(&self) -> &LogSettings {
        &self.0.log_settings
    }

    pub(crate) fn open_mode(&self) -> OpenMode {
        self.0.open_mode
    }

    /// Targets the database file at `filename`
    pub fn filename(mut self, filename: impl AsRef<Path>) -> Self {
        Arc::make_mut(&mut self.0).target = TursoDatabaseTarget::File(filename.as_ref().into());
        self
    }

    /// Targets a private in-memory database
    pub fn in_memory(mut self) -> Self {
        Arc::make_mut(&mut self.0).target = TursoDatabaseTarget::Memory;
        self
    }

    /// Sets whether a missing database file is created on open
    pub fn create_if_missing(mut self, create_if_missing: bool) -> Self {
        Arc::make_mut(&mut self.0).open_mode = if create_if_missing {
            OpenMode::CreateIfMissing
        } else {
            OpenMode::ReadWrite
        };
        self
    }

    /// Sets the driver's maximum lock-wait budget for a statement
    pub fn busy_timeout(mut self, busy_timeout: Duration) -> Self {
        Arc::make_mut(&mut self.0).busy_timeout = busy_timeout;
        self
    }

    /// Sets the prepared-statement cache capacity; zero disables the cache
    pub fn statement_cache_capacity(mut self, statement_cache_capacity: usize) -> Self {
        Arc::make_mut(&mut self.0).statement_cache_capacity = statement_cache_capacity;
        self
    }

    /// Sets whether connections enforce foreign keys
    ///
    /// A migration that rebuilds a table with incoming foreign keys opens with `false`, checks
    /// `PRAGMA foreign_key_check` before commit, then enables enforcement before use.
    pub fn foreign_keys(mut self, foreign_keys: bool) -> Self {
        Arc::make_mut(&mut self.0).foreign_keys = foreign_keys;
        self
    }
}

impl Default for TursoConnectOptions {
    fn default() -> Self {
        Self(Arc::new(ConnectOptionsInner {
            target: TursoDatabaseTarget::Memory,
            open_mode: OpenMode::ReadWrite,
            busy_timeout: DEFAULT_BUSY_TIMEOUT,
            statement_cache_capacity: DEFAULT_STATEMENT_CACHE_CAPACITY,
            foreign_keys: true,
            log_settings: LogSettings::default(),
        }))
    }
}

impl ConnectOptions for TursoConnectOptions {
    type Connection = TursoConnection;

    fn from_url(url: &Url) -> Result<Self, Error> {
        url.as_str().parse()
    }

    fn to_url_lossy(&self) -> Url {
        connection_url::render_connection_url(self)
    }

    async fn connect(&self) -> Result<Self::Connection, Error>
    where
        Self::Connection: Sized,
    {
        let engine = EngineConnection::open(self).await?;
        Ok(TursoConnection::new(self.clone(), engine))
    }

    fn log_statements(mut self, level: log::LevelFilter) -> Self {
        Arc::make_mut(&mut self.0)
            .log_settings
            .log_statements(level);
        self
    }

    fn log_slow_statements(mut self, level: log::LevelFilter, duration: Duration) -> Self {
        Arc::make_mut(&mut self.0)
            .log_settings
            .log_slow_statements(level, duration);
        self
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, time::Duration};

    use sqlx_core::{
        connection::{ConnectOptions, Connection},
        error::Error,
        executor::Executor,
        query_scalar::query_scalar,
    };

    use super::TursoConnectOptions;
    use crate::TursoAdapterError;

    #[test]
    fn defaults_match_sqlx_sqlite() {
        // Arrange and Act
        let options = TursoConnectOptions::new();

        // Assert
        assert!(options.is_in_memory());
        assert!(!options.get_create_if_missing());
        assert_eq!(options.get_statement_cache_capacity(), 100);
        assert_eq!(options.get_busy_timeout(), Duration::from_secs(5));
        assert!(options.get_foreign_keys());
    }

    #[tokio::test]
    async fn connect_opens_memory_database() -> sqlx_core::Result<()> {
        // Arrange
        let options = TursoConnectOptions::new();

        // Act
        let mut connection = options.connect().await?;

        // Assert
        connection.ping().await?;
        assert!(connection.options().is_in_memory());
        Ok(())
    }

    #[tokio::test]
    async fn memory_databases_are_private_to_their_connection() -> sqlx_core::Result<()> {
        // Arrange
        let options = TursoConnectOptions::new();
        let mut first = options.connect().await?;
        let mut second = options.connect().await?;
        first
            .execute("CREATE TABLE private_memory (id INTEGER PRIMARY KEY)")
            .await?;

        // Act
        let error = second
            .execute("INSERT INTO private_memory (id) VALUES (1)")
            .await
            .expect_err("the second connection has its own empty database");

        // Assert
        assert!(error.to_string().contains("private_memory"));
        Ok(())
    }

    #[tokio::test]
    async fn create_if_missing_creates_the_database_file() -> sqlx_core::Result<()> {
        // Arrange
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("created.db");
        let options = TursoConnectOptions::new()
            .filename(&path)
            .create_if_missing(true);

        // Act
        let mut connection = options.connect().await?;

        // Assert
        connection.ping().await?;
        assert_eq!(connection.options().get_filename(), Some(path.as_path()));
        assert!(path.exists());
        Ok(())
    }

    #[tokio::test]
    async fn read_write_mode_rejects_a_missing_file() -> sqlx_core::Result<()> {
        // Arrange
        let directory = tempfile::tempdir()?;
        let options = TursoConnectOptions::new().filename(directory.path().join("missing.db"));

        // Act
        let error = options.connect().await.expect_err("file does not exist");

        // Assert
        assert!(matches!(
            error,
            Error::Io(ref io_error) if io_error.kind() == std::io::ErrorKind::NotFound
        ));
        Ok(())
    }

    #[tokio::test]
    async fn foreign_keys_setting_is_applied_to_the_connection() -> sqlx_core::Result<()> {
        // Arrange
        let enforcing = TursoConnectOptions::new();
        let relaxed = TursoConnectOptions::new().foreign_keys(false);

        // Act
        let mut enforcing = enforcing.connect().await?;
        let mut relaxed = relaxed.connect().await?;
        let enforcing_flag: i64 = query_scalar("PRAGMA foreign_keys")
            .fetch_one(&mut enforcing)
            .await?;
        let relaxed_flag: i64 = query_scalar("PRAGMA foreign_keys")
            .fetch_one(&mut relaxed)
            .await?;

        // Assert
        assert_eq!(enforcing_flag, 1);
        assert_eq!(relaxed_flag, 0);
        Ok(())
    }

    #[test]
    fn filename_targets_a_file() {
        let options = TursoConnectOptions::new().filename("data.db");
        assert_eq!(options.get_filename(), Some(Path::new("data.db")));
        assert!(!options.is_in_memory());
    }

    #[test]
    fn adapter_errors_surface_as_configuration_errors() {
        let error: Error = TursoAdapterError::ReadOnlyUnsupported.into();
        assert!(matches!(error, Error::Configuration(_)));
    }
}
