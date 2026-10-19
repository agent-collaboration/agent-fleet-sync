//! Same-process opposed access to a physical file, before opening the SDK.
use crate::OpenError;
#[cfg(unix)]
use std::{
    collections::{HashMap, hash_map::Entry},
    num::NonZeroUsize,
    sync::{LazyLock, Mutex},
};
use std::{path::Path, sync::Arc};

#[cfg(unix)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}
#[derive(Clone, Copy, Debug)]
enum AccessMode {
    Writer,
    Retained,
    ReadCopy,
}
#[cfg(unix)]
enum FileAccess {
    Writers(NonZeroUsize),
    RetainedReaders(NonZeroUsize),
    ReadCopy,
}
#[cfg(unix)]
impl FileAccess {
    fn new(mode: AccessMode) -> Self {
        match mode {
            AccessMode::Writer => Self::Writers(NonZeroUsize::MIN),
            AccessMode::Retained => Self::RetainedReaders(NonZeroUsize::MIN),
            AccessMode::ReadCopy => Self::ReadCopy,
        }
    }
    fn claims(&mut self, mode: AccessMode) -> Result<&mut NonZeroUsize, OpenError> {
        match (self, mode) {
            (Self::Writers(claims), AccessMode::Writer)
            | (Self::RetainedReaders(claims), AccessMode::Retained) => Ok(claims),
            _ => Err(OpenError::FileInUse),
        }
    }
}
#[cfg(unix)]
static FILE_ADMISSIONS: LazyLock<Mutex<HashMap<FileIdentity, FileAccess>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Arc clones extend a single admission through pools and their transactions.
#[derive(Debug)]
pub(crate) struct FileAdmission {
    #[cfg(unix)]
    identity: FileIdentity,
    #[cfg(unix)]
    mode: AccessMode,
}
impl FileAdmission {
    pub async fn writer(path: &Path, create_if_missing: bool) -> Result<Arc<Self>, OpenError> {
        validate_native_path(path)?;
        Self::acquire(path, AccessMode::Writer, create_if_missing).await
    }
    pub async fn retained(path: &Path) -> Result<Arc<Self>, OpenError> {
        validate_native_path(path)?;
        Self::acquire(path, AccessMode::Retained, false).await
    }
    pub async fn read_copy(path: &Path) -> Result<Arc<Self>, OpenError> {
        validate_native_path(path)?;
        Self::acquire(path, AccessMode::ReadCopy, false).await
    }
    #[cfg(unix)]
    async fn acquire(
        path: &Path,
        mode: AccessMode,
        create_if_missing: bool,
    ) -> Result<Arc<Self>, OpenError> {
        use std::os::unix::fs::MetadataExt;
        let metadata = match tokio::fs::metadata(path).await {
            Ok(metadata) => metadata,
            Err(error) if create_if_missing && error.kind() == std::io::ErrorKind::NotFound => {
                // The ordinary writer already permits creation. Create only its
                // empty file so its physical identity is known before SDK open.
                match tokio::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(path)
                    .await
                {
                    Ok(file) => file.metadata().await.map_err(storage)?,
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        tokio::fs::metadata(path).await.map_err(storage)?
                    }
                    Err(error) => return Err(storage(error)),
                }
            }
            Err(error) => return Err(storage(error)),
        };
        if !metadata.is_file() {
            return Err(storage("database target is not a regular file"));
        }
        let identity = FileIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        let mut files = FILE_ADMISSIONS
            .lock()
            .map_err(|_| storage("file admission is poisoned"))?;
        match files.entry(identity) {
            Entry::Vacant(entry) => {
                entry.insert(FileAccess::new(mode));
            }
            Entry::Occupied(entry) => {
                let claims = entry.into_mut().claims(mode)?;
                *claims = claims
                    .get()
                    .checked_add(1)
                    .and_then(NonZeroUsize::new)
                    .ok_or_else(|| storage("file admission exhausted"))?;
            }
        }
        Ok(Arc::new(Self { identity, mode }))
    }
    #[cfg(not(unix))]
    async fn acquire(
        _path: &Path,
        mode: AccessMode,
        _create_if_missing: bool,
    ) -> Result<Arc<Self>, OpenError> {
        match mode {
            AccessMode::Retained | AccessMode::ReadCopy => Err(OpenError::FileIdentityUnsupported),
            // Existing writer behavior is unchanged on unsupported platforms;
            // no retained reader can be admitted there.
            AccessMode::Writer => Ok(Arc::new(Self {})),
        }
    }
}
#[cfg(unix)]
impl Drop for FileAdmission {
    fn drop(&mut self) {
        if let Ok(mut files) = FILE_ADMISSIONS.lock() {
            if matches!(self.mode, AccessMode::ReadCopy) {
                if matches!(files.get(&self.identity), Some(FileAccess::ReadCopy)) {
                    files.remove(&self.identity);
                }
                return;
            }
            let remove = match files
                .get_mut(&self.identity)
                .and_then(|access| access.claims(self.mode).ok())
            {
                Some(claims) => match NonZeroUsize::new(claims.get() - 1) {
                    Some(remaining) => {
                        *claims = remaining;
                        false
                    }
                    None => true,
                },
                None => false,
            };
            if remove {
                files.remove(&self.identity);
            }
        }
    }
}
pub(crate) fn validate_native_path(path: &Path) -> Result<(), OpenError> {
    path.to_str().ok_or(OpenError::NativePathNotUtf8)?;
    Ok(())
}
#[cfg(unix)]
fn storage(error: impl std::fmt::Display) -> OpenError {
    OpenError::Storage {
        message: error.to_string(),
    }
}
