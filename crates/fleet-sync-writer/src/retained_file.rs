use crate::{
    OpenError, ReadPool, ReplicatedSchema, WriterRegistrationOutcome, native_pool::NativePool,
    writer_file_record::read_file_record,
};
use sqlx_core::pool::PoolOptions;
use sqlx_turso::{Turso, TursoConnectOptions};
use std::path::Path;

/// Read access to a retained native file through the library's guarded surface.
pub struct RetainedFile {
    reader: ReadPool,
    registration_outcome: WriterRegistrationOutcome,
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
        let registration_outcome = {
            let mut connection = pool.acquire().await?;
            crate::retained_file_format::validate(&mut connection, &schema).await?;
            let record = read_file_record(&mut connection)
                .await?
                .ok_or_else(|| OpenError::corrupt("writer.record"))?;
            crate::registration_bookkeeping::outcome(&record)
        };
        Ok(Self {
            reader: ReadPool::from_native_pool(NativePool::passive(pool)),
            registration_outcome,
        })
    }

    pub fn reader(&self) -> &ReadPool {
        &self.reader
    }

    /// The persisted registration result observed once when this file opened.
    pub fn registration_outcome(&self) -> &WriterRegistrationOutcome {
        &self.registration_outcome
    }
}
