use crate::{
    OpenError, ReadPool, ReplicatedSchema, WriterRegistrationOutcome, native_pool::NativePool,
    writer_file_record::read_file_record,
};
use fleet_sync_protocol::{Epoch, GateReason, ReplicationPosition};
use sqlx_core::pool::PoolOptions;
use sqlx_turso::{Turso, TursoConnectOptions};
use std::path::Path;

/// Persisted loss of writer authority, independent of the original registration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetainedAuthority {
    Superseded {
        current: Option<Epoch>,
        ended_at: Option<ReplicationPosition>,
    },
    Retired,
    RegistrationRejected {
        reason: GateReason,
    },
}

/// Read access to a retained native file through the library's guarded surface.
pub struct RetainedFile {
    reader: ReadPool,
    registration_outcome: WriterRegistrationOutcome,
    authority: Option<RetainedAuthority>,
}
impl RetainedFile {
    /// Open an existing file without a writer, migration, capture or recovery.
    ///
    /// The caller must ensure no other process writes this file, and must not
    /// open writable Turso handles outside the library for its lifetime.
    /// The consumer also keeps at most one live `ReplicatedDatabase` owner per
    /// physical file; independent owners racing replacement are unsupported.
    pub async fn open_read_only(
        path: &Path,
        schema: impl ReplicatedSchema,
    ) -> Result<Self, OpenError> {
        let admission = crate::file_admission::FileAdmission::retained(path).await?;
        let pool = PoolOptions::<Turso>::new()
            .min_connections(1)
            .max_connections(4)
            .after_connect(move |_, _| {
                let admission = admission.clone();
                Box::pin(async move {
                    let _admission = admission;
                    Ok(())
                })
            })
            .connect_with(TursoConnectOptions::new().filename(path).read_only(true))
            .await?;
        let (registration_outcome, authority) = {
            let mut connection = pool.acquire().await?;
            crate::retained_file_format::validate(&mut connection, &schema).await?;
            let record = read_file_record(&mut connection)
                .await?
                .ok_or_else(|| OpenError::corrupt("writer.record"))?;
            (
                crate::registration_bookkeeping::outcome(&record),
                record
                    .authority
                    .as_ref()
                    .and_then(|authority| authority.retained_authority(record.epoch, record.peer)),
            )
        };
        Ok(Self {
            reader: ReadPool::from_native_pool(NativePool::passive(pool)),
            registration_outcome,
            authority,
        })
    }

    pub fn reader(&self) -> &ReadPool {
        &self.reader
    }

    /// Rejected capture ranges read without opening a writable owner.
    pub async fn rejected_tails(&self) -> Result<Vec<crate::RejectedTail>, OpenError> {
        let (pool, _guard) = self.reader.pin().await?;
        let mut connection = pool.acquire().await?;
        crate::sender_storage::rejected_tails(&mut connection)
            .await
            .map_err(|error| match error {
                crate::SenderError::Storage(error) => error,
                error => OpenError::Storage {
                    message: error.to_string(),
                },
            })
    }

    /// Latest persisted authority observed when this file opened.
    /// Absence of a loss observation does not establish a current write lease.
    pub fn authority(&self) -> Option<RetainedAuthority> {
        self.authority.clone()
    }

    /// The original registration's persisted result, not current authority.
    pub fn registration_outcome(&self) -> &WriterRegistrationOutcome {
        &self.registration_outcome
    }
}
