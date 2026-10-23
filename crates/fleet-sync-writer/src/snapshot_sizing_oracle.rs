// Pre-W-C40 pager retained only as a differential oracle.
use super::*;
impl WriterSnapshot {
    pub(super) async fn page_oracle(
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
        let measured =
            measure_snapshot_unit(page).map_err(|_| OpenError::corrupt("snapshot.encoding"))?;
        if measured.encoded_size > MAX_UNIT_BYTES {
            return Err(UnitTooLarge::SnapshotSchema {
                encoded_size: measured.encoded_size,
            }
            .into());
        }
        let mut page = measured.unit;
        let Some(table) = table else {
            return Ok(page);
        };
        let after = cursor
            .as_ref()
            .map_or(&[][..], |cursor| cursor.after_key.as_slice());
        // One bounded query per row also works for SQLx backends which collect
        // a query's entire result before yielding its first Rust stream item.
        let mut pending = read_rows(&mut self.transaction, table, after, 1)
            .await?
            .pop();
        while let Some(row) = pending {
            let key = row_key(table, &row)?;
            let following = read_rows(&mut self.transaction, table, &key, 1)
                .await?
                .pop();
            let next = if following.is_some() {
                Some(SnapshotCursor {
                    position: self.position,
                    table: table.name().into(),
                    after_key: key.clone(),
                })
            } else {
                next_table.clone()
            };
            let previous_next = page.next.take();
            page.rows.push(row);
            page.next = next;
            let measured =
                measure_snapshot_unit(page).map_err(|_| OpenError::corrupt("snapshot.encoding"))?;
            page = measured.unit;
            if page.rows.len() > 1 && measured.encoded_size > TARGET_UNIT_BYTES {
                page.rows.pop();
                page.next = previous_next;
                break;
            }
            if measured.encoded_size > MAX_UNIT_BYTES {
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
                    encoded_size: measured.encoded_size,
                }
                .into());
            }
            if measured.encoded_size >= TARGET_UNIT_BYTES || page.rows.len() == PAGE_ROWS {
                break;
            }
            pending = following;
        }
        Ok(page)
    }
}
