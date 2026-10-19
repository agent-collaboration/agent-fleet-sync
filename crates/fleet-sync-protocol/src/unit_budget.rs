//! Encoded library carriers and staged-row bounds, independent of any host I/O.
pub use crate::encoded_size::encoded_json_size;
use crate::*;

/// These constants determine page and part identities. A release changing them
/// must drain or restart unfinished seeds and assemblies before taking over.
pub const TARGET_UNIT_BYTES: usize = 1024 * 1024;
pub const MAX_UNIT_BYTES: usize = 1536 * 1024;

// Both existing staged tables have four columns. A record header has one
// size varint and one per column (each <=9 bytes); 128 also covers the table
// cell's length/rowid varints and initial overflow pointer. This bounds record
// framing, not physical database pages. sqlite.org/fileformat2.html#record_format
const STAGED_ROW_FRAMING_BYTES: usize = 128;
const SQLITE_INTEGER_BYTES: usize = 8;
const STAGED_TIME_TEXT_BYTES: usize = 20; // i64::MIN formatted as decimal TEXT

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum UnitTooLarge {
    #[error("row of {table} with key {key:?} makes a {encoded_size}-byte unit")]
    Row {
        table: String,
        key: Vec<String>,
        encoded_size: usize,
    },
    #[error(
        "schema commit {commit_id:?}, migration {migration_version}, statement {statement_index} makes a {encoded_size}-byte unit"
    )]
    SchemaStatement {
        commit_id: CommitSequence,
        migration_version: u64,
        statement_index: usize,
        encoded_size: usize,
    },
    #[error("snapshot schema makes a {encoded_size}-byte unit")]
    SnapshotSchema { encoded_size: usize },
    #[error("annotation of commit {commit_id:?} makes a {encoded_size}-byte unit")]
    Annotation {
        commit_id: CommitSequence,
        encoded_size: usize,
    },
}

pub struct MeasuredUnit<T> {
    pub unit: T,
    /// Maximum library carrier size, including conservative staged-row metadata.
    pub encoded_size: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("cannot encode library unit: {message}")]
pub struct UnitEncodingError {
    pub message: String,
}

fn encoding_error(error: impl std::fmt::Display) -> UnitEncodingError {
    UnitEncodingError {
        message: error.to_string(),
    }
}
fn carrier_error() -> UnitEncodingError {
    UnitEncodingError {
        message: "invalid sizing carrier".into(),
    }
}
fn sizing_frame() -> Result<FrameId, UnitEncodingError> {
    // Every legal frame correlation has the same UUID text width. This carrier
    // is counted and taken apart locally; its identity is never emitted.
    FrameId::new(uuid::Uuid::from_u128(1)).map_err(encoding_error)
}

/// Count actual typed seed/session encodings with maximum-width future seed
/// counters, then return the original page. No wire shape or page is copied.
pub fn measure_snapshot_unit(
    page: SnapshotPage,
) -> Result<MeasuredUnit<SnapshotPage>, UnitEncodingError> {
    let seed = SeedPage {
        seed: SeedId {
            generation: u64::MAX,
        },
        page: u32::MAX,
        digest: PageDigest::of(&page).map_err(encoding_error)?,
        contents: page,
    };
    let staged = encoded_json_size(&seed)
        .map_err(encoding_error)?
        .saturating_add(u64::MAX.to_string().len())
        .saturating_add(SQLITE_INTEGER_BYTES)
        .saturating_add(seed.digest.as_str().len())
        .saturating_add(STAGED_ROW_FRAMING_BYTES);
    let request = ClientFrame::<NoConsumer>::Request {
        frame: sizing_frame()?,
        body: OperationRequest::Replication(ReplicationRequest::SeedPage(Box::new(seed))),
    };
    let mut encoded_size = staged.max(encoded_json_size(&request).map_err(encoding_error)?);
    let ClientFrame::Request {
        body: OperationRequest::Replication(ReplicationRequest::SeedPage(seed)),
        ..
    } = request
    else {
        return Err(carrier_error());
    };
    let answer = ServerFrame::<NoConsumer>::Answer {
        frame: sizing_frame()?,
        body: OperationAnswer::Replication(ReplicationAnswer::SnapshotPage(SnapshotRead::Page(
            Box::new(seed.contents),
        ))),
    };
    encoded_size = encoded_size.max(encoded_json_size(&answer).map_err(encoding_error)?);
    let ServerFrame::Answer {
        body:
            OperationAnswer::Replication(ReplicationAnswer::SnapshotPage(SnapshotRead::Page(page))),
        ..
    } = answer
    else {
        return Err(carrier_error());
    };
    Ok(MeasuredUnit {
        unit: *page,
        encoded_size,
    })
}

/// Move the actual annotation through the real typed frame without requiring
/// Clone. Staging counts its existing key, integer index, decimal TEXT timestamp
/// and record framing.
pub fn measure_piece_unit<P: ConsumerProtocol>(
    piece: Piece<P::Annotation>,
) -> Result<MeasuredUnit<Piece<P::Annotation>>, UnitEncodingError> {
    let key = piece
        .part
        .as_ref()
        .map(|part| part.transaction)
        .unwrap_or(TransactionKey {
            epoch: piece.epoch,
            commit_id: piece.through.sequence,
        });
    let staged = encoded_json_size(&piece)
        .map_err(encoding_error)?
        .saturating_add(encoded_json_size(&key).map_err(encoding_error)?)
        .saturating_add(SQLITE_INTEGER_BYTES + STAGED_TIME_TEXT_BYTES)
        .saturating_add(STAGED_ROW_FRAMING_BYTES);
    let frame = ClientFrame::<P>::Request {
        frame: sizing_frame()?,
        body: OperationRequest::Replication(ReplicationRequest::Piece(piece)),
    };
    let encoded_size = staged.max(encoded_json_size(&frame).map_err(encoding_error)?);
    let ClientFrame::Request {
        body: OperationRequest::Replication(ReplicationRequest::Piece(piece)),
        ..
    } = frame
    else {
        return Err(carrier_error());
    };
    Ok(MeasuredUnit {
        unit: piece,
        encoded_size,
    })
}

/// Every snapshot carrier contains the page exactly once and a fixed-width
/// digest. Appending a row therefore changes all carrier sizes by these same
/// JSON bytes, including the array comma and replacement of the next cursor.
pub fn snapshot_size_after_row(
    encoded_size: usize,
    has_rows: bool,
    row: &[SqlValue],
    previous_next: &Option<SnapshotCursor>,
    next: &Option<SnapshotCursor>,
) -> Result<usize, UnitEncodingError> {
    let previous_next_bytes = encoded_json_size(previous_next).map_err(encoding_error)?;
    let row_bytes = encoded_json_size(row).map_err(encoding_error)?;
    let next_bytes = encoded_json_size(next).map_err(encoding_error)?;
    encoded_size
        .checked_sub(previous_next_bytes)
        .and_then(|size| size.checked_add(usize::from(has_rows)))
        .and_then(|size| size.checked_add(row_bytes))
        .and_then(|size| size.checked_add(next_bytes))
        .ok_or_else(carrier_error)
}
