use crate::{
    OpenError, ReplicatedDatabase, ReplicatedSchema, TableLayout,
    capture_stream::capture_head,
    native_rows::{read_rows, row_key},
    schema_inspection::inspect_replicated_tables,
    snapshot_schema::snapshot_schema,
    writer_file_record::read_file_record,
};
use fleet_sync_protocol::unit_budget::{
    MAX_UNIT_BYTES, TARGET_UNIT_BYTES, UnitTooLarge, measure_snapshot_unit, snapshot_size_after_row,
};
use fleet_sync_protocol::{
    DatabaseId, ReplicationPosition, SchemaIdentity, SnapshotCursor, SnapshotManifest,
    SnapshotPage, TableDigestBuilder, TableManifest, TableName,
};
use sqlx_core::transaction::Transaction;
use sqlx_turso::Turso;

const PAGE_ROWS: usize = 256;
// Eight consumed rows plus one lookahead: the native 20k-row probe took 10.96s
// versus 9.23s at sixteen, keeping half the prefetch for that modest extra gain.
// Admission's separate 256-row scan dominates whole-writer RSS; see W-C41 proof.
const SNAPSHOT_READ_BATCH_ROWS: usize = 8;

/// One read transaction pins rows, schema and capture position for every page.
pub struct WriterSnapshot {
    transaction: Transaction<'static, Turso>,
    _reader: crate::read_pool::ReadPin,
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
        let bootstrapped =
            record.origin == crate::writer_file_record::RetainedFileOriginKind::Bootstrap;
        let committed = crate::capture_stream::committed_layout(&mut transaction).await?;
        let names = committed
            .iter()
            .map(|layout| {
                TableName::new(layout.name())
                    .map_err(|_| OpenError::corrupt("migration.membership"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut tables = if bootstrapped {
            crate::migration_execution::inspect_present_tables(&mut transaction, &names).await?
        } else {
            inspect_replicated_tables(&mut transaction, self.schema.replicated_tables()).await?
        };
        tables.sort_by(|left, right| left.name().cmp(right.name()));
        let (schema, statements) = snapshot_schema(&mut transaction, &names).await?;
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
        let table = self.tables.get(index);
        let next_table = self.tables.get(index + 1).map(|table| SnapshotCursor {
            position: self.position,
            table: table.name().into(),
            after_key: Vec::new(),
        });
        let page = SnapshotPage {
            database: self.database,
            position: self.position,
            schema: self.schema.clone(),
            schema_steps: self.statements.clone(),
            table: table.map_or_else(String::new, |table| table.name().into()),
            rows: Vec::new(),
            next: next_table.clone(),
        };
        let measured = measure_page(page).map_err(|_| OpenError::corrupt("snapshot.encoding"))?;
        if measured.encoded_size > MAX_UNIT_BYTES {
            return Err(UnitTooLarge::SnapshotSchema {
                encoded_size: measured.encoded_size,
            }
            .into());
        }
        let mut encoded_size = measured.encoded_size;
        let mut page = measured.unit;
        let Some(table) = table else {
            return checked_page(page, encoded_size);
        };
        let after = cursor
            .as_ref()
            .map_or(&[][..], |cursor| cursor.after_key.as_slice());
        let mut batch = read_snapshot_batch(&mut self.transaction, table, after)
            .await?
            .into_iter();
        let mut batch_rows = 0;
        let mut after = after.to_vec();
        loop {
            if batch_rows == SNAPSHOT_READ_BATCH_ROWS {
                if batch.len() == 0 {
                    break;
                }
                drop(batch);
                batch = read_snapshot_batch(&mut self.transaction, table, &after)
                    .await?
                    .into_iter();
                batch_rows = 0;
            }
            let Some(row) = batch.next() else {
                break;
            };
            let key = row_key(table, &row)?;
            let next = if batch.len() > 0 {
                Some(SnapshotCursor {
                    position: self.position,
                    table: table.name().into(),
                    after_key: key.clone(),
                })
            } else {
                next_table.clone()
            };
            let candidate_size = snapshot_size_after_row(
                encoded_size,
                !page.rows.is_empty(),
                &row,
                &page.next,
                &next,
            )
            .map_err(|_| OpenError::corrupt("snapshot.encoding"))?;
            let previous_next = page.next.take();
            page.rows.push(row);
            page.next = next;

            if page.rows.len() > 1 && candidate_size > TARGET_UNIT_BYTES {
                page.rows.pop();
                page.next = previous_next;
                break;
            }
            if candidate_size > MAX_UNIT_BYTES {
                let key = key
                    .into_iter()
                    .map(|value| match value {
                        fleet_sync_protocol::SqlValue::Text(value) => Ok(value),
                        _ => Err(OpenError::corrupt("snapshot.row_key")),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                return Err(UnitTooLarge::Row {
                    table: table.name().into(),
                    key,
                    encoded_size: candidate_size,
                }
                .into());
            }
            encoded_size = candidate_size;
            if candidate_size >= TARGET_UNIT_BYTES || page.rows.len() == PAGE_ROWS {
                break;
            }
            after = key;
            batch_rows += 1;
        }
        checked_page(page, encoded_size)
    }

    pub async fn manifest(&mut self) -> Result<SnapshotManifest, OpenError> {
        let mut tables = Vec::new();
        let mut cursor = None;
        let mut digest = TableDigestBuilder::new();
        let mut row_count = 0_u64;
        loop {
            let page = self.page(cursor).await?;
            if page.table.is_empty() {
                break;
            }
            for row in &page.rows {
                digest
                    .push_row(row)
                    .map_err(|_| OpenError::corrupt("snapshot.table_digest"))?;
                row_count = row_count
                    .checked_add(1)
                    .ok_or_else(|| OpenError::corrupt("snapshot.row_count"))?;
            }
            if page
                .next
                .as_ref()
                .is_none_or(|next| next.table != page.table)
            {
                tables.push(TableManifest {
                    table: TableName::new(&page.table)
                        .map_err(|_| OpenError::corrupt("snapshot.table"))?,
                    row_count,
                    digest: digest.finish(),
                });
                digest = TableDigestBuilder::new();
                row_count = 0;
            }
            cursor = page.next;
            if cursor.is_none() {
                break;
            }
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

fn measure_page(
    page: SnapshotPage,
) -> Result<
    fleet_sync_protocol::unit_budget::MeasuredUnit<SnapshotPage>,
    fleet_sync_protocol::unit_budget::UnitEncodingError,
> {
    #[cfg(test)]
    FULL_MEASURES.with(|count| count.set(count.get() + 1));
    measure_snapshot_unit(page)
}

fn checked_page(page: SnapshotPage, expected: usize) -> Result<SnapshotPage, OpenError> {
    let measured = measure_page(page).map_err(|_| OpenError::corrupt("snapshot.encoding"))?;
    if measured.encoded_size != expected {
        return Err(OpenError::corrupt("snapshot.size_changed"));
    }
    Ok(measured.unit)
}

#[cfg(test)]
std::thread_local! { static FULL_MEASURES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }
#[cfg(test)]
#[path = "snapshot_sizing_oracle.rs"]
mod sizing_oracle;
#[cfg(test)]
#[path = "snapshot_sizing_tests.rs"]
mod sizing_tests;

async fn read_snapshot_batch(
    connection: &mut sqlx_turso::TursoConnection,
    table: &TableLayout,
    after: &[fleet_sync_protocol::SqlValue],
) -> Result<Vec<Vec<fleet_sync_protocol::SqlValue>>, OpenError> {
    #[cfg(test)]
    READ_QUERIES.with(|count| count.set(count.get() + 1));
    read_rows(connection, table, after, SNAPSHOT_READ_BATCH_ROWS + 1).await
}
#[cfg(test)]
std::thread_local! { static READ_QUERIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }
