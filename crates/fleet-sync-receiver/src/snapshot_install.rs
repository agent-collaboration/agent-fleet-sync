use crate::{
    CopyStatus, InstallPageReceipt, ReceiverError, ReceiverHook, SnapshotInstall,
    SnapshotInstallError, SnapshotInstallState, SnapshotReady, StoreError, checked_queries,
    copy_contract::{CopyRecord, validate_copy_status},
    copy_storage::{CopyStorage, encode_record},
    schema_reads::SchemaReads,
    snapshot_install_storage::{InstallMarker, InstallStorage},
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::{
    CopyRole, PageDigest, SnapshotCursor, SnapshotManifest, SnapshotManifestAnswer, SnapshotPage,
};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

/// Every error requires the host to roll back. Native file publication belongs
/// to the host; these rules own the staged rows, marker and readiness transition.
pub struct SnapshotInstallRules<'scope, S>(pub &'scope mut S);
impl<S> SnapshotInstallRules<'_, S>
where
    S: TransactionScope + Send,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'a> String: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'a> Option<String>: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'a> f64: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'a> Vec<u8>: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    pub async fn prepare(
        &mut self,
        install: &SnapshotInstall,
        source: &CopyStatus,
    ) -> Result<(), SnapshotInstallError> {
        validate_copy_status(source)?;
        if install.database != source.database
            || install.position != source.acked
            || install.schema != source.schema
        {
            return Err(SnapshotInstallError::IdentityConflict);
        }
        if let Some(marker) = InstallStorage(self.0).optional().await? {
            return if marker.install == *install && marker.source == *source {
                Ok(())
            } else {
                Err(SnapshotInstallError::IdentityConflict)
            };
        }
        if let Some(record) = CopyStorage(self.0).load().await? {
            return Err(if matches!(record, CopyRecord::Retired { .. }) {
                SnapshotInstallError::Retired
            } else {
                SnapshotInstallError::IdentityConflict
            });
        }
        let mut status = source.clone();
        status.role = CopyRole::Follower;
        CopyStorage(self.0)
            .save(&CopyRecord::Starting {
                status,
                install_id: install.install_id,
            })
            .await?;
        InstallStorage(self.0)
            .save(&InstallMarker {
                install: install.clone(),
                source: source.clone(),
                schema_steps: None,
                verified: None,
                ready: false,
            })
            .await
    }
    pub async fn state(&mut self) -> Result<Option<SnapshotInstallState>, SnapshotInstallError> {
        Ok(InstallStorage(self.0)
            .optional()
            .await?
            .map(|marker| SnapshotInstallState {
                install: marker.install,
                verified: marker.verified.is_some(),
                ready: marker.ready,
            }))
    }
    /// Membership includes replicated views as well as tables. Native publish
    /// uses the rules' recorded ownership instead of inferring it from names.
    pub async fn replicated_objects(
        &mut self,
    ) -> Result<std::collections::BTreeSet<String>, SnapshotInstallError> {
        Ok(crate::schema_membership::SchemaMembership(self.0)
            .names()
            .await?)
    }
    pub async fn page_receipt(
        &mut self,
        install: &SnapshotInstall,
        page: u32,
    ) -> Result<Option<InstallPageReceipt>, SnapshotInstallError> {
        let marker = InstallStorage(self.0).load().await?;
        check_identity(&marker, install)?;
        Ok(InstallStorage(self.0)
            .pages(install.install_id)
            .await?
            .into_iter()
            .find(|receipt| receipt.page == page))
    }
    // Explicit Send is required by borrowed-scope native host dispatch.
    #[allow(clippy::manual_async_fn)]
    pub fn stage_page<'call>(
        &'call mut self,
        install: &'call SnapshotInstall,
        index: u32,
        cursor: Option<&'call SnapshotCursor>,
        page: &'call SnapshotPage,
    ) -> impl std::future::Future<Output = Result<InstallPageReceipt, SnapshotInstallError>> + Send + 'call
    {
        async move {
            let mut marker = InstallStorage(self.0).load().await?;
            check_identity(&marker, install)?;
            if page.database != install.database
                || page.position != install.position
                || page.schema != install.schema
                || page.rows.len() > crate::snapshot_reads::PAGE_SIZE
            {
                return Err(SnapshotInstallError::IdentityConflict);
            }
            let digest = PageDigest::of(page)
                .map_err(|_| SnapshotInstallError::PageConflict { page: index })?;
            let receipts = InstallStorage(self.0).pages(install.install_id).await?;
            if let Some(receipt) = receipts.iter().find(|receipt| receipt.page == index) {
                return if receipt.digest == digest {
                    Ok(receipt.clone())
                } else {
                    Err(SnapshotInstallError::PageConflict { page: index })
                };
            }
            if marker.verified.is_some()
                || usize::try_from(index).ok() != Some(receipts.len())
                || (index == 0 && cursor.is_some())
                || (index > 0
                    && (receipts
                        .last()
                        .and_then(|receipt| receipt.next.as_ref())
                        .is_none()
                        || receipts.last().and_then(|receipt| receipt.next.as_ref()) != cursor))
            {
                return Err(SnapshotInstallError::PageConflict { page: index });
            }
            if let Some(cursor) = cursor
                && (cursor.position != install.position || cursor.table != page.table)
            {
                return Err(SnapshotInstallError::PageConflict { page: index });
            }
            if let Some(statements) = &marker.schema_steps {
                if statements != &page.schema_steps {
                    return Err(SnapshotInstallError::SchemaMismatch);
                }
            } else {
                crate::schema_membership::SchemaMembership(self.0)
                    .apply(&page.schema_steps)
                    .await?;
                crate::snapshot_rows::clear_snapshot_rows(self.0).await?;
                if SchemaReads(self.0).fingerprint().await? != install.schema.fingerprint {
                    return Err(SnapshotInstallError::SchemaMismatch);
                }
                let record = crate::row_transactions::SchemaStepRecord {
                    version: install.schema.version,
                    statements: page.schema_steps.clone(),
                };
                checked_queries::write_schema_step(
                    self.0,
                    install.schema.version.to_string(),
                    encode_record(&record)?,
                )
                .await?;
                marker.schema_steps = Some(page.schema_steps.clone());
            }
            crate::snapshot_rows::insert_snapshot_rows(self.0, page).await?;
            let receipt = InstallPageReceipt {
                install_id: install.install_id,
                page: index,
                digest,
                rows: u64::try_from(page.rows.len())
                    .map_err(|_| SnapshotInstallError::PageConflict { page: index })?,
                next: page.next.clone(),
            };
            InstallStorage(self.0).save_page(&receipt).await?;
            InstallStorage(self.0).save(&marker).await?;
            Ok(receipt)
        }
    }
    // Explicit Send is required by borrowed-scope native host dispatch.
    #[allow(clippy::manual_async_fn)]
    pub fn verify<'call>(
        &'call mut self,
        install: &'call SnapshotInstall,
        manifest: &'call SnapshotManifest,
    ) -> impl std::future::Future<Output = Result<(), SnapshotInstallError>> + Send + 'call {
        async move {
            let mut marker = InstallStorage(self.0).load().await?;
            check_identity(&marker, install)?;
            if manifest.database != install.database
                || manifest.position != install.position
                || manifest.schema != install.schema
            {
                return Err(SnapshotInstallError::IdentityConflict);
            }
            if let Some(saved) = marker.verified {
                return if saved == *manifest {
                    Ok(())
                } else {
                    Err(SnapshotInstallError::ManifestMismatch)
                };
            }
            let pages = InstallStorage(self.0).pages(install.install_id).await?;
            if marker.schema_steps.is_none()
                || pages.is_empty()
                || pages.last().is_some_and(|page| page.next.is_some())
            {
                return Err(SnapshotInstallError::Incomplete);
            }
            let status = starting_status(self.0, install).await?;
            let actual =
                crate::snapshot_manifest::compute_manifest(self.0, install.position, status)
                    .await?;
            if actual != SnapshotManifestAnswer::Manifest(manifest.clone())
                || SchemaReads(self.0).fingerprint().await? != install.schema.fingerprint
            {
                return Err(SnapshotInstallError::ManifestMismatch);
            }
            marker.verified = Some(manifest.clone());
            InstallStorage(self.0).save(&marker).await
        }
    }
    pub async fn mark_ready<H: ReceiverHook<S>>(
        &mut self,
        install: &SnapshotInstall,
        hook: &H,
    ) -> Result<SnapshotReady<H::FollowUp>, SnapshotInstallError> {
        let mut marker = InstallStorage(self.0).load().await?;
        check_identity(&marker, install)?;
        if marker.verified.is_none() {
            return Err(SnapshotInstallError::NotVerified);
        }
        let installed = crate::InstalledSnapshot {
            database: install.database,
            position: install.position,
            schema: install.schema.clone(),
        };
        if marker.ready {
            if !matches!(CopyStorage(self.0).load().await?,Some(CopyRecord::Following(status)) if status.database==install.database)
            {
                return Err(SnapshotInstallError::IdentityConflict);
            }
            return Ok(SnapshotReady::AlreadyReady { installed });
        }
        let status = starting_status(self.0, install).await?;
        CopyStorage(self.0)
            .save(&CopyRecord::Following(status))
            .await?;
        let follow_ups = crate::run_snapshot_hook(self.0, hook, &installed)
            .await
            .map_err(|refusal| SnapshotInstallError::Hook { refusal })?;
        marker.ready = true;
        InstallStorage(self.0).save(&marker).await?;
        Ok(SnapshotReady::Ready {
            installed,
            follow_ups,
        })
    }
}
pub(crate) fn check_identity(
    marker: &InstallMarker,
    install: &SnapshotInstall,
) -> Result<(), SnapshotInstallError> {
    if marker.install != *install {
        Err(SnapshotInstallError::IdentityConflict)
    } else {
        Ok(())
    }
}
pub(crate) async fn starting_status<S>(
    scope: &mut S,
    install: &SnapshotInstall,
) -> Result<CopyStatus, SnapshotInstallError>
where
    S: TransactionScope + Send,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'a> String: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'a> Option<String>: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'a> f64: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'a> Vec<u8>: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    match CopyStorage(scope).load().await? {
        Some(CopyRecord::Starting { status, install_id })
            if install_id == install.install_id
                && status.database == install.database
                && status.acked == install.position
                && status.schema == install.schema =>
        {
            Ok(status)
        }
        Some(CopyRecord::Retired { .. }) => Err(SnapshotInstallError::Retired),
        _ => Err(SnapshotInstallError::IdentityConflict),
    }
}
pub(crate) async fn readable_status<S>(scope: &mut S) -> Result<CopyStatus, ReceiverError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'a> String: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'a> Option<String>: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    match CopyStorage(scope).load().await? {
        Some(CopyRecord::Following(status)) => Ok(status),
        Some(CopyRecord::Starting { status, install_id }) => {
            let marker = InstallStorage(scope)
                .optional()
                .await
                .map_err(|error| match error {
                    SnapshotInstallError::Storage(error) => ReceiverError::Storage(error),
                    SnapshotInstallError::Rules(error) => error,
                    _ => ReceiverError::Storage(StoreError::Corrupt {
                        field: "snapshot install read marker".into(),
                    }),
                })?
                .ok_or(ReceiverError::NotInitialized)?;
            if marker.install.install_id != install_id
                || marker.install.database != status.database
                || marker.install.position != status.acked
                || marker.install.schema != status.schema
            {
                return Err(StoreError::Corrupt {
                    field: "snapshot readable marker".into(),
                }
                .into());
            }
            if marker.verified.is_none() {
                return Err(ReceiverError::NotInitialized);
            }
            Ok(status)
        }
        Some(CopyRecord::Retired { .. }) => Err(ReceiverError::Retired),
        _ => Err(ReceiverError::NotInitialized),
    }
}
