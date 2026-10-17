//! Where a connection's database lives and whether opening may create it

use std::path::PathBuf;

/// Where a Turso connection's database lives
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TursoDatabaseTarget {
    /// A private in-memory database owned by one connection
    Memory,
    /// A database file at this path
    File(PathBuf),
}

/// Whether opening a file target may create the file
///
/// Read-only opens need the lower Turso SDK, so there is no read-only mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OpenMode {
    /// The file must already exist
    ReadWrite,
    /// A missing file is created
    CreateIfMissing,
}

impl OpenMode {
    /// The `mode` URL parameter value for this target and mode
    pub(crate) fn url_value(self, target: &TursoDatabaseTarget) -> &'static str {
        match (target, self) {
            (TursoDatabaseTarget::Memory, _) => "memory",
            (TursoDatabaseTarget::File(_), Self::ReadWrite) => "rw",
            (TursoDatabaseTarget::File(_), Self::CreateIfMissing) => "rwc",
        }
    }
}
