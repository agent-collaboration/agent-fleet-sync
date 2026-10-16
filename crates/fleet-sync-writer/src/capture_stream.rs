use crate::{
    CaptureChange, OpenError, TableLayout, capture_normalization::decode_image, checked_queries,
    normalize_capture_change, schema_inspection::text, writer_file_record::WriterFileRecord,
    writer_scope::WriterScope,
};
use fleet_sync_protocol::{
    CommitSequence, ReplicatedTransaction, ReplicationPosition, SqlValue, SyncTime,
};
use sqlx_turso::TursoConnection;
use std::collections::BTreeMap;

pub(crate) struct CapturedCommit {
    pub position: ReplicationPosition,
    pub changed_at: SyncTime,
    pub transaction: ReplicatedTransaction,
    pub layout_after: Vec<TableLayout>,
}

struct CaptureRow {
    change_id: u64,
    transaction: u64,
    changed_at: SyncTime,
    kind: i64,
    table: Option<String>,
    before: Option<Vec<u8>>,
    after: Option<Vec<u8>>,
}

fn integer(value: Option<String>, class: Option<String>, field: &str) -> Result<i64, OpenError> {
    if class.as_deref() != Some("integer") {
        return Err(OpenError::corrupt(field));
    }
    value
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| OpenError::corrupt(field))
}

impl TryFrom<checked_queries::StoredCaptureRow> for CaptureRow {
    type Error = OpenError;
    fn try_from(row: checked_queries::StoredCaptureRow) -> Result<Self, OpenError> {
        let kind = integer(row.change_kind, row.kind_class, "capture.kind")?;
        let table = if kind == 2 {
            if row.table_name.is_some()
                || row.table_class.as_deref() != Some("null")
                || row.before_image.is_some()
                || row.after_image.is_some()
            {
                return Err(OpenError::corrupt("capture.commit"));
            }
            None
        } else {
            Some(text(row.table_name, row.table_class, "capture.table")?)
        };
        let seconds = integer(row.changed_at, row.time_class, "capture.time")?;
        Ok(Self {
            change_id: u64::try_from(integer(row.change_id, row.id_class, "capture.id")?)
                .map_err(|_| OpenError::corrupt("capture.id"))?,
            transaction: u64::try_from(integer(
                row.transaction_id,
                row.transaction_class,
                "capture.transaction",
            )?)
            .map_err(|_| OpenError::corrupt("capture.transaction"))?,
            changed_at: SyncTime::from_unix_millis(
                seconds
                    .checked_mul(1000)
                    .ok_or_else(|| OpenError::corrupt("capture.time"))?,
            ),
            kind,
            table,
            before: row.before_image,
            after: row.after_image,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
struct SchemaStep {
    statements: Vec<String>,
    layout: Vec<TableLayout>,
}

async fn stored_schema_steps(
    connection: &mut TursoConnection,
) -> Result<BTreeMap<u64, SchemaStep>, OpenError> {
    let mut steps = BTreeMap::new();
    for row in checked_queries::read_schema_steps(&mut WriterScope(connection)).await? {
        let version = u64::try_from(integer(
            row.version,
            row.version_class,
            "schema_step.version",
        )?)
        .map_err(|_| OpenError::corrupt("schema_step.version"))?;
        let statements = serde_json::from_str(&text(
            row.statements,
            row.statements_class,
            "schema_step.statements",
        )?)
        .map_err(|_| OpenError::corrupt("schema_step.statements"))?;
        let layout = serde_json::from_str(&text(
            row.schema_layout,
            row.layout_class,
            "schema_step.layout",
        )?)
        .map_err(|_| OpenError::corrupt("schema_step.layout"))?;
        if version == 0
            || steps
                .insert(version, SchemaStep { statements, layout })
                .is_some()
        {
            return Err(OpenError::corrupt("schema_step.version"));
        }
    }
    Ok(steps)
}

fn captured_schema_step(row: &CaptureRow) -> Result<(u64, SchemaStep), OpenError> {
    if row.kind != 1 || row.before.is_some() {
        return Err(OpenError::corrupt("schema_step.capture_kind"));
    }
    let values = decode_image(
        row.after
            .as_deref()
            .ok_or_else(|| OpenError::corrupt("schema_step.after"))?,
    )
    .map_err(|error| OpenError::Capture { error })?;
    let [
        SqlValue::Integer(version),
        SqlValue::Text(statements),
        SqlValue::Text(layout),
    ] = values.as_slice()
    else {
        return Err(OpenError::corrupt("schema_step.image"));
    };
    let version = u64::try_from(*version).map_err(|_| OpenError::corrupt("schema_step.version"))?;
    let statements = serde_json::from_str(statements)
        .map_err(|_| OpenError::corrupt("schema_step.statements"))?;
    let layout =
        serde_json::from_str(layout).map_err(|_| OpenError::corrupt("schema_step.layout"))?;
    Ok((version, SchemaStep { statements, layout }))
}

fn normalize_commit(
    rows: Vec<CaptureRow>,
    marker: CaptureRow,
    record: &WriterFileRecord,
    layout: &mut Vec<TableLayout>,
    steps: &BTreeMap<u64, SchemaStep>,
) -> Result<CapturedCommit, OpenError> {
    let commit_id = CommitSequence::new(marker.change_id);
    let mut migration = None;
    for row in &rows {
        if row
            .table
            .as_ref()
            .is_some_and(|name| name.eq_ignore_ascii_case("_fleet_sync_schema_steps"))
        {
            let (version, step) = captured_schema_step(row)?;
            if migration.is_some() || steps.get(&version) != Some(&step) {
                return Err(OpenError::corrupt("schema_step.capture"));
            }
            migration = Some((version, step));
        }
    }
    let transaction = if let Some((migration_version, step)) = migration {
        *layout = step.layout;
        // These statements already contain this commit's DML. Sending its
        // captured row images too would apply those effects a second time.
        ReplicatedTransaction::SchemaStep {
            commit_id,
            migration_version,
            statements: step.statements,
        }
    } else {
        let mut changes = Vec::new();
        for row in rows {
            let table = row
                .table
                .ok_or_else(|| OpenError::corrupt("capture.table"))?;
            let Some(table) = layout
                .iter()
                .find(|candidate| candidate.name().eq_ignore_ascii_case(&table))
            else {
                continue;
            };
            let change = match row.kind {
                -1 => CaptureChange::Delete {
                    before: row
                        .before
                        .ok_or_else(|| OpenError::corrupt("capture.before"))?,
                },
                0 => CaptureChange::Update {
                    before: row
                        .before
                        .ok_or_else(|| OpenError::corrupt("capture.before"))?,
                    after: row
                        .after
                        .ok_or_else(|| OpenError::corrupt("capture.after"))?,
                },
                1 => CaptureChange::Insert {
                    before: row.before,
                    after: row.after,
                },
                _ => return Err(OpenError::corrupt("capture.kind")),
            };
            changes.extend(
                normalize_capture_change(table, change)
                    .map_err(|error| OpenError::Capture { error })?,
            );
        }
        ReplicatedTransaction::Rows {
            commit_id,
            rows: changes,
        }
    };
    Ok(CapturedCommit {
        position: ReplicationPosition::new(record.epoch, commit_id),
        changed_at: marker.changed_at,
        transaction,
        layout_after: layout.clone(),
    })
}

/// Call inside one read or maintenance transaction. Reading in bounded pages
/// does not expose an incomplete writer commit, and historical images use the
/// layout at their position, including after older capture has been pruned.
pub(crate) async fn read_window(
    connection: &mut TursoConnection,
    record: &WriterFileRecord,
    after: ReplicationPosition,
    limit: usize,
) -> Result<Vec<CapturedCommit>, OpenError> {
    if after < record.retained_after || after.epoch > record.epoch || limit == 0 {
        return Err(OpenError::corrupt("capture.continuation"));
    }
    let steps = stored_schema_steps(connection).await?;
    let mut layout = record.retained_layout.clone();
    let mut cursor = record.retained_change_id;
    let mut group = Vec::<CaptureRow>::new();
    let mut commits = Vec::new();
    loop {
        let rows = checked_queries::read_capture(
            &mut WriterScope(connection),
            i64::try_from(cursor).map_err(|_| OpenError::corrupt("capture.cursor"))?,
            256,
        )
        .await?;
        if rows.is_empty() {
            if !group.is_empty() {
                return Err(OpenError::corrupt("capture.missing_commit"));
            }
            return Ok(commits);
        }
        for stored in rows {
            let row = CaptureRow::try_from(stored)?;
            if row.change_id <= cursor
                || row.transaction == 0
                || group
                    .first()
                    .is_some_and(|first| first.transaction != row.transaction)
            {
                return Err(OpenError::corrupt("capture.order"));
            }
            cursor = row.change_id;
            if row.kind == 2 {
                let commit =
                    normalize_commit(std::mem::take(&mut group), row, record, &mut layout, &steps)?;
                if commit.position > after {
                    commits.push(commit);
                }
                if commits.len() >= limit {
                    return Ok(commits);
                }
            } else {
                group.push(row);
            }
        }
    }
}

pub(crate) async fn capture_head(
    connection: &mut TursoConnection,
    record: &WriterFileRecord,
) -> Result<ReplicationPosition, OpenError> {
    let head = checked_queries::read_capture_head(&mut WriterScope(connection)).await?;
    let marker = match (head.marker, head.marker_class.as_deref()) {
        (None, Some("null")) => 0,
        (Some(value), Some("integer")) => value
            .parse::<u64>()
            .map_err(|_| OpenError::corrupt("capture.head"))?,
        _ => return Err(OpenError::corrupt("capture.head")),
    };
    if marker < record.retained_change_id {
        return Err(OpenError::corrupt("capture.head"));
    }
    Ok(if marker == 0 {
        record.start
    } else {
        ReplicationPosition::new(
            record.epoch,
            fleet_sync_protocol::CommitSequence::new(marker),
        )
    })
}
