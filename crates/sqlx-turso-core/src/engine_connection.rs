//! Opening the Turso engine connection that a [`TursoConnection`](crate::TursoConnection) wraps
//!
//! Opens `turso::Database` directly and disables native busy spinning, then applies foreign keys before
//! returning the connection.

use std::{io, path::Path};

use sqlx_core::error::Error;

use crate::{
    TursoConnectOptions, TursoDatabaseTarget, connect_options::OpenMode, error::map_turso_error,
};

/// The raw engine connection
pub(crate) struct EngineConnection {
    pub(crate) raw: turso::Connection,
}

impl EngineConnection {
    pub(crate) async fn open(options: &TursoConnectOptions) -> Result<Self, Error> {
        let path = engine_path(options).await?;

        let engine = open_local(&path).await?;
        apply_connection_settings(options, &engine.raw).await?;
        Ok(engine)
    }
}

/// The path string the engine opens: `:memory:` or a file that exists or may be created
async fn engine_path(options: &TursoConnectOptions) -> Result<String, Error> {
    match options.target() {
        TursoDatabaseTarget::Memory => Ok(":memory:".to_owned()),
        TursoDatabaseTarget::File(path) => {
            ensure_file_may_open(options.open_mode(), path).await?;
            Ok(path.to_string_lossy().into_owned())
        }
    }
}

async fn ensure_file_may_open(open_mode: OpenMode, path: &Path) -> Result<(), Error> {
    match open_mode {
        OpenMode::CreateIfMissing => Ok(()),
        OpenMode::ReadWrite if tokio::fs::try_exists(path).await? => Ok(()),
        OpenMode::ReadWrite => Err(Error::Io(io::Error::new(
            io::ErrorKind::NotFound,
            format!("database file {} does not exist", path.display()),
        ))),
    }
}

async fn open_local(path: &str) -> Result<EngineConnection, Error> {
    let database = turso::Builder::new_local(path)
        .build()
        .await
        .map_err(map_turso_error)?;
    let raw = database.connect().map_err(map_turso_error)?;
    Ok(EngineConnection { raw })
}

async fn apply_connection_settings(
    options: &TursoConnectOptions,
    connection: &turso::Connection,
) -> Result<(), Error> {
    connection
        .pragma_update("busy_timeout", 0)
        .await
        .map_err(map_turso_error)?;
    let foreign_keys = if options.get_foreign_keys() {
        "ON"
    } else {
        "OFF"
    };
    connection
        .pragma_update("foreign_keys", foreign_keys)
        .await
        .map_err(map_turso_error)?;
    Ok(())
}
