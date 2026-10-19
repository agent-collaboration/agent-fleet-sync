use crate::{
    TokioObject,
    native_object::{ObjectConnection, open_existing_connection},
};
use durable_steps::{BoxFuture, Host, StoreError};
pub use fleet_sync_writer::native_file::{PublishError, checkpoint_for_publish};
use sqlx_turso::sqlx::Connection;
use std::path::Path;

impl<H: Host> TokioObject<H> {
    /// Permanently close one disposable generation after checkpointing its writes.
    pub(crate) async fn close_for_read_copy(&self) -> Result<(), PublishError> {
        let _engine = self.engine_work.lock().await;
        let mut owner = self.connection.lock().await;
        match &mut *owner {
            ObjectConnection::Closed => {}
            ObjectConnection::Publishing => return Err(PublishError::Closed),
            ObjectConnection::Open(connection) => checkpoint_for_publish(connection).await?,
        }
        let connection = std::mem::replace(&mut *owner, ObjectConnection::Closed);
        if let ObjectConnection::Open(connection) = connection {
            (*connection).close().await.map_err(StoreError::backend)?;
        }
        let mut wal = self.path().as_os_str().to_owned();
        wal.push("-wal");
        match tokio::fs::metadata(wal).await {
            Ok(metadata) if metadata.len() != 0 => Err(PublishError::WalRemains {
                bytes: metadata.len(),
            }),
            Ok(_) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(PublishError::io(error)),
        }
    }

    /// Hold the object's engine and transaction owners while publication carries
    /// local state and renames a verified file. The callback runs only after all
    /// native work has drained and the checkpointed connection has closed.
    pub async fn with_drained_file<T, F>(&self, publish: F) -> Result<T, PublishError>
    where
        T: Send + 'static,
        F: for<'path> FnOnce(&'path Path) -> BoxFuture<'path, Result<T, PublishError>> + Send,
    {
        let _engine = self.engine_work.lock().await;
        let mut owner = self.connection.lock().await;
        if matches!(*owner, ObjectConnection::Publishing) {
            *owner = ObjectConnection::Open(Box::new(
                open_existing_connection(self.path(), self.options).await?,
            ));
        }
        let ObjectConnection::Open(connection) = &mut *owner else {
            return Err(StoreError::Aborted {
                reason: "object is closed".into(),
            }
            .into());
        };
        checkpoint_for_publish(connection).await?;
        if let ObjectConnection::Open(connection) =
            std::mem::replace(&mut *owner, ObjectConnection::Publishing)
        {
            (*connection).close().await.map_err(StoreError::backend)?;
        }
        let wal = self.path().as_os_str().to_owned();
        let mut wal = wal;
        wal.push("-wal");
        let result = async {
            match tokio::fs::metadata(&wal).await {
                Ok(metadata) if metadata.len() != 0 => {
                    return Err(PublishError::WalRemains {
                        bytes: metadata.len(),
                    });
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(PublishError::io(error)),
            }
            publish(self.path()).await
        }
        .await;
        // A callback can fail after a rename; reopening the live path lets its
        // install-ID check settle that outcome on the next receipted call.
        *owner = ObjectConnection::Open(Box::new(
            open_existing_connection(self.path(), self.options).await?,
        ));
        result
    }
}
