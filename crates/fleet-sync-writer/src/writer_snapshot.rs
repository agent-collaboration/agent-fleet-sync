use crate::{
    OpenError, ReplicatedDatabase, ReplicatedSchema, TableLayout,
    capture_stream::capture_head,
    native_rows::{read_rows, row_key},
    schema_inspection::inspect_replicated_tables,
    snapshot_schema::snapshot_schema,
    writer_file_record::read_file_record,
};
use fleet_sync_protocol::{
    DatabaseId, ReplicationPosition, SchemaIdentity, SnapshotCursor, SnapshotManifest,
    SnapshotPage, TableDigestBuilder, TableManifest, TableName,
};
use sqlx_core::transaction::Transaction;
use sqlx_turso::Turso;

const PAGE_ROWS: usize = 256;

/// One read transaction pins rows, schema and capture position for every page.
pub struct WriterSnapshot {
    transaction: Transaction<'static, Turso>,
    _reader: tokio::sync::OwnedRwLockReadGuard<()>,
    database: DatabaseId,
    position: ReplicationPosition,
    schema: SchemaIdentity,
    statements: Vec<String>,
    tables: Vec<TableLayout>,
}

impl<S: ReplicatedSchema> ReplicatedDatabase<S> {
    pub async fn snapshot(&self) -> Result<WriterSnapshot, OpenError> {
        let _control = self.control.lock().await;
        let (pool, reader) = self.readers.pin().await?;
        let mut transaction = pool.begin().await?;
        let record = read_file_record(&mut transaction)
            .await?
            .ok_or_else(|| OpenError::corrupt("writer.record"))?;
        let position = capture_head(&mut transaction, &record).await?;
        let bootstrapped = record.origin == crate::writer_file_record::WriterFileOrigin::Bootstrap;
        let mut tables = if bootstrapped {
            crate::migration_execution::inspect_present_tables(
                &mut transaction,
                self.schema.replicated_tables(),
            )
            .await?
        } else {
            inspect_replicated_tables(&mut transaction, self.schema.replicated_tables()).await?
        };
        tables.sort_by(|left, right| left.name().cmp(right.name()));
        let (schema, statements) =
            snapshot_schema(&mut transaction, self.schema.replicated_tables(), &tables).await?;
        if bootstrapped {
            let latest = crate::migration_execution::offered_migrations(&self.schema)?
                .last()
                .map_or(0, |migration| migration.version);
            if u64::try_from(latest).is_ok_and(|latest| schema.version >= latest) {
                // Future declarations are due only after their migrations run.
                inspect_replicated_tables(&mut transaction, self.schema.replicated_tables())
                    .await?;
            }
        }
        Ok(WriterSnapshot {
            transaction,
            _reader: reader,
            database: record.database,
            position,
            schema,
            statements,
            tables,
        })
    }
}

impl WriterSnapshot {
    pub const fn position(&self) -> ReplicationPosition {
        self.position
    }
    pub fn schema(&self) -> &SchemaIdentity {
        &self.schema
    }

    pub async fn page(
        &mut self,
        cursor: Option<SnapshotCursor>,
    ) -> Result<SnapshotPage, OpenError> {
        if cursor
            .as_ref()
            .is_some_and(|cursor| cursor.position != self.position)
        {
            return Err(OpenError::corrupt("snapshot.cursor_position"));
        }
        let index = if let Some(cursor) = &cursor {
            self.tables
                .iter()
                .position(|table| table.name() == cursor.table)
                .ok_or_else(|| OpenError::corrupt("snapshot.cursor_table"))?
        } else {
            0
        };
        let Some(table) = self.tables.get(index) else {
            return Ok(SnapshotPage {
                database: self.database,
                position: self.position,
                schema: self.schema.clone(),
                schema_steps: self.statements.clone(),
                table: String::new(),
                rows: Vec::new(),
                next: None,
            });
        };
        let after = cursor
            .as_ref()
            .map_or(&[][..], |cursor| cursor.after_key.as_slice());
        let mut rows = read_rows(&mut self.transaction, table, after, PAGE_ROWS + 1).await?;
        let next = if rows.len() > PAGE_ROWS {
            rows.truncate(PAGE_ROWS);
            Some(SnapshotCursor {
                position: self.position,
                table: table.name().into(),
                after_key: row_key(
                    table,
                    rows.last()
                        .ok_or_else(|| OpenError::corrupt("snapshot.page"))?,
                )?,
            })
        } else {
            self.tables.get(index + 1).map(|table| SnapshotCursor {
                position: self.position,
                table: table.name().into(),
                after_key: Vec::new(),
            })
        };
        Ok(SnapshotPage {
            database: self.database,
            position: self.position,
            schema: self.schema.clone(),
            schema_steps: self.statements.clone(),
            table: table.name().into(),
            rows,
            next,
        })
    }

    pub async fn manifest(&mut self) -> Result<SnapshotManifest, OpenError> {
        let mut tables = Vec::new();
        for table in &self.tables {
            let mut after = Vec::new();
            let mut digest = TableDigestBuilder::new();
            let mut row_count = 0_u64;
            loop {
                let rows = read_rows(&mut self.transaction, table, &after, PAGE_ROWS).await?;
                for row in &rows {
                    digest
                        .push_row(row)
                        .map_err(|_| OpenError::corrupt("snapshot.table_digest"))?;
                    row_count = row_count
                        .checked_add(1)
                        .ok_or_else(|| OpenError::corrupt("snapshot.row_count"))?;
                }
                if rows.len() < PAGE_ROWS {
                    break;
                }
                after = row_key(
                    table,
                    rows.last()
                        .ok_or_else(|| OpenError::corrupt("snapshot.page"))?,
                )?;
            }
            tables.push(TableManifest {
                table: TableName::new(table.name())
                    .map_err(|_| OpenError::corrupt("snapshot.table"))?,
                row_count,
                digest: digest.finish(),
            });
        }
        Ok(SnapshotManifest {
            database: self.database,
            position: self.position,
            schema: self.schema.clone(),
            tables,
        })
    }

    pub async fn close(self) -> Result<(), OpenError> {
        self.transaction.rollback().await?;
        Ok(())
    }
}
