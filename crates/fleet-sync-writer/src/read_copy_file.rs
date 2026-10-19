//! A disposable file is released only after its owner and final read pin drop.
use crate::file_admission::FileAdmission;
use sqlx_core::pool::Pool;
use sqlx_turso::Turso;
use std::{path::PathBuf, sync::Arc};
use tokio::{runtime::Handle, sync::watch};

pub(crate) type ReleaseResult = Option<Result<(), String>>;

#[derive(Debug)]
pub(crate) struct ReadCopyFile {
    cleanup: Option<Cleanup>,
    runtime: Handle,
}

#[derive(Debug)]
struct Cleanup {
    path: PathBuf,
    pool: Pool<Turso>,
    _admission: Arc<FileAdmission>,
    released: watch::Sender<ReleaseResult>,
}

impl ReadCopyFile {
    pub fn new(
        path: PathBuf,
        pool: Pool<Turso>,
        admission: Arc<FileAdmission>,
    ) -> (Arc<Self>, watch::Receiver<ReleaseResult>) {
        let (released, receiver) = watch::channel(None);
        (
            Arc::new(Self {
                cleanup: Some(Cleanup {
                    path,
                    pool,
                    _admission: admission,
                    released,
                }),
                runtime: Handle::current(),
            }),
            receiver,
        )
    }
}

impl Drop for ReadCopyFile {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            self.runtime.spawn(async move {
                // SQLx's asynchronous rollback/connection return finishes before unlinking.
                cleanup.pool.close().await;
                let result = remove_owned_files(&cleanup.path).await;
                cleanup.released.send_replace(Some(result));
            });
        }
    }
}

async fn remove_owned_files(path: &std::path::Path) -> Result<(), String> {
    for suffix in ["-wal", "-shm", ""] {
        let mut file = path.as_os_str().to_owned();
        file.push(suffix);
        match tokio::fs::remove_file(file).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(())
}
