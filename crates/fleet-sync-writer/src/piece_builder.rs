use crate::{CapturedTransaction, PieceAnnotation, SenderError, capture_stream::CapturedCommit};
use fleet_sync_protocol::{
    ConsumerProtocol, DatabaseId, Epoch, Piece, PieceContents, PieceDigest, ReplicatedTransaction,
    ReplicationPosition, RowChange, SqlValue, TransactionKey, TransactionPart,
    unit_budget::{
        MAX_UNIT_BYTES, MeasuredUnit, TARGET_UNIT_BYTES, UnitTooLarge, encoded_json_size,
        measure_piece_unit,
    },
};
use std::{num::NonZeroUsize, ops::Range};

struct PartBuilder<'a> {
    database: DatabaseId,
    epoch: Epoch,
    after: ReplicationPosition,
    captured: &'a CapturedCommit,
    whole_digest: PieceDigest,
}

struct Fragment<T> {
    range: Range<usize>,
    annotation: Option<T>,
}

pub(crate) fn cut_transaction<A: PieceAnnotation>(
    database: DatabaseId,
    epoch: Epoch,
    after: ReplicationPosition,
    captured: &CapturedCommit,
    max_items: NonZeroUsize,
    annotator: &A,
) -> Result<Vec<Piece<<A::Protocol as ConsumerProtocol>::Annotation>>, SenderError> {
    let count = match &captured.transaction {
        ReplicatedTransaction::Rows { rows, .. } => rows.len(),
        ReplicatedTransaction::SchemaStep { statements, .. } => statements.len(),
    };
    let builder = PartBuilder {
        database,
        epoch,
        after,
        captured,
        whole_digest: PieceDigest::of_transaction(&captured.transaction).map_err(|_| {
            SenderError::Invalid {
                field: "transaction.digest",
            }
        })?,
    };
    let mut annotation = annotator.annotate(&[CapturedTransaction {
        position: captured.position,
        transaction: &captured.transaction,
    }]);
    if count <= max_items.get() {
        let measured = builder.measure::<A::Protocol>(0..count, annotation, None)?;
        if measured.encoded_size <= TARGET_UNIT_BYTES
            || (count <= 1 && measured.encoded_size <= MAX_UNIT_BYTES)
        {
            return Ok(vec![measured.unit]);
        }
        annotation = measured.unit.annotation;
    }
    let base = builder
        .probe::<A::Protocol>(count..count, None)?
        .encoded_size;
    let mut annotated_base = base;
    // Measure the whole annotation once without requiring Clone. Every carrier
    // contains the same piece JSON, so item bytes add equally to either base.
    if annotation.is_some() {
        let measured = builder.probe::<A::Protocol>(count..count, annotation)?;
        if measured.encoded_size > MAX_UNIT_BYTES {
            return Err(UnitTooLarge::Annotation {
                commit_id: captured.position.sequence,
                encoded_size: measured.encoded_size,
            }
            .into());
        }
        annotated_base = measured.encoded_size;
        annotation = measured.unit.annotation;
    }
    let sizes = PartSizes::new(&captured.transaction, base, annotated_base)?;
    if count == 0 {
        return Err(SenderError::Invalid {
            field: "part.empty",
        });
    }
    let mut fragments = Vec::new();
    let mut start = 0;
    while start < count {
        let mut end = start;
        while end < count && end - start < max_items.get() {
            let candidate_end = end + 1;
            let candidate_size = sizes.size(
                start..candidate_end,
                candidate_end == count && annotation.is_some(),
            )?;
            if candidate_size > TARGET_UNIT_BYTES && end > start {
                break;
            }
            if candidate_size > MAX_UNIT_BYTES {
                if candidate_end == count && annotation.is_some() {
                    let alone = sizes.size(start..candidate_end, false)?;
                    if alone > MAX_UNIT_BYTES {
                        return Err(builder.item_error(start, alone)?);
                    }
                    // The existing assembler accepts an empty final fragment.
                    // Use it only when these individually fitting items exceed
                    // the hard cap together; the annotation remains whole.
                    fragments.push(Fragment {
                        range: start..candidate_end,
                        annotation: None,
                    });
                    fragments.push(Fragment {
                        range: count..count,
                        annotation,
                    });
                    return builder.finish::<A::Protocol>(fragments, &sizes);
                }
                return Err(builder.item_error(start, candidate_size)?);
            }
            end = candidate_end;
            if candidate_size >= TARGET_UNIT_BYTES {
                break;
            }
        }
        fragments.push(Fragment {
            range: start..end,
            annotation: if end == count {
                annotation.take()
            } else {
                None
            },
        });
        start = end;
    }
    builder.finish::<A::Protocol>(fragments, &sizes)
}

impl PartBuilder<'_> {
    fn transaction(&self, range: Range<usize>) -> Result<ReplicatedTransaction, SenderError> {
        match &self.captured.transaction {
            ReplicatedTransaction::Rows { commit_id, rows } => Ok(ReplicatedTransaction::Rows {
                commit_id: *commit_id,
                rows: rows
                    .get(range)
                    .ok_or(SenderError::Invalid { field: "part.rows" })?
                    .to_vec(),
            }),
            ReplicatedTransaction::SchemaStep {
                commit_id,
                migration_version,
                statements,
            } => Ok(ReplicatedTransaction::SchemaStep {
                commit_id: *commit_id,
                migration_version: *migration_version,
                statements: statements
                    .get(range)
                    .ok_or(SenderError::Invalid {
                        field: "part.statements",
                    })?
                    .to_vec(),
            }),
        }
    }

    fn descriptor(&self, index: u32, count: u32) -> Result<TransactionPart, SenderError> {
        TransactionPart::new(
            TransactionKey {
                epoch: self.epoch,
                commit_id: self.captured.position.sequence,
            },
            index,
            count,
            self.whole_digest.clone(),
        )
        .map_err(|_| SenderError::Invalid {
            field: "part.descriptor",
        })
    }

    fn measure<P: ConsumerProtocol>(
        &self,
        range: Range<usize>,
        annotation: Option<P::Annotation>,
        part: Option<TransactionPart>,
    ) -> Result<MeasuredUnit<Piece<P::Annotation>>, SenderError> {
        #[cfg(test)]
        FULL_MEASURES.with(|count| count.set(count.get() + 1));
        let piece = Piece::new(PieceContents {
            database: self.database,
            epoch: self.epoch,
            continues_from: self.after,
            through: self.captured.position,
            transactions: vec![self.transaction(range)?],
            part,
            annotation,
        })?;
        measure_piece_unit::<P>(piece).map_err(|_| SenderError::Invalid {
            field: "part.encoding",
        })
    }

    fn probe<P: ConsumerProtocol>(
        &self,
        range: Range<usize>,
        annotation: Option<P::Annotation>,
    ) -> Result<MeasuredUnit<Piece<P::Annotation>>, SenderError> {
        // Future count/index widths are bounded before the number of fragments
        // is known. This valid final descriptor is measured, never emitted.
        self.measure::<P>(
            range,
            annotation,
            Some(self.descriptor(u32::MAX - 1, u32::MAX)?),
        )
    }

    fn item_error(&self, index: usize, encoded_size: usize) -> Result<SenderError, SenderError> {
        let reason = match &self.captured.transaction {
            ReplicatedTransaction::Rows { rows, .. } => {
                let row = rows
                    .get(index)
                    .ok_or(SenderError::Invalid { field: "part.row" })?;
                let (RowChange::Upsert { table, key, .. } | RowChange::Delete { table, key }) = row;
                let key = key
                    .iter()
                    .map(|value| match value {
                        SqlValue::Text(value) => Ok(value.clone()),
                        _ => Err(SenderError::Invalid {
                            field: "part.row_key",
                        }),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                UnitTooLarge::Row {
                    table: table.clone(),
                    key,
                    encoded_size,
                }
            }
            ReplicatedTransaction::SchemaStep {
                commit_id,
                migration_version,
                ..
            } => UnitTooLarge::SchemaStatement {
                commit_id: *commit_id,
                migration_version: *migration_version,
                statement_index: index,
                encoded_size,
            },
        };
        Ok(reason.into())
    }

    fn finish<P: ConsumerProtocol>(
        &self,
        fragments: Vec<Fragment<P::Annotation>>,
        sizes: &PartSizes,
    ) -> Result<Vec<Piece<P::Annotation>>, SenderError> {
        let count = u32::try_from(fragments.len()).map_err(|_| SenderError::Invalid {
            field: "part.count",
        })?;
        fragments
            .into_iter()
            .enumerate()
            .map(|(index, fragment)| {
                let part = if count > 1 {
                    Some(self.descriptor(
                        u32::try_from(index).map_err(|_| SenderError::Invalid {
                            field: "part.index",
                        })?,
                        count,
                    )?)
                } else {
                    None
                };
                let expected = sizes
                    .size(fragment.range.clone(), fragment.annotation.is_some())?
                    .checked_sub(
                        encoded_json_size(&Some(self.descriptor(u32::MAX - 1, u32::MAX)?))
                            .map_err(part_encoding)?,
                    )
                    .and_then(|size| size.checked_add(encoded_json_size(&part).ok()?))
                    .ok_or(SenderError::Invalid {
                        field: "part.size_changed",
                    })?;
                let measured = self.measure::<P>(fragment.range, fragment.annotation, part)?;
                if measured.encoded_size != expected {
                    return Err(SenderError::Invalid {
                        field: "part.size_changed",
                    });
                }
                if measured.encoded_size > MAX_UNIT_BYTES {
                    return Err(SenderError::Invalid {
                        field: "part.size_changed",
                    });
                }
                Ok(measured.unit)
            })
            .collect()
    }
}

fn part_encoding(_: serde_json::Error) -> SenderError {
    SenderError::Invalid {
        field: "part.encoding",
    }
}

// Prefix sums count each captured row/statement once. A fragment changes only
// this JSON array, its fixed-width digest, and (at emission) its part descriptor.
struct PartSizes {
    prefix: Vec<usize>,
    base: usize,
    annotated_base: usize,
}
impl PartSizes {
    fn new(
        transaction: &ReplicatedTransaction,
        base: usize,
        annotated_base: usize,
    ) -> Result<Self, SenderError> {
        let items = match transaction {
            ReplicatedTransaction::Rows { rows, .. } => rows
                .iter()
                .map(encoded_json_size)
                .collect::<Result<Vec<_>, _>>(),
            ReplicatedTransaction::SchemaStep { statements, .. } => statements
                .iter()
                .map(encoded_json_size)
                .collect::<Result<Vec<_>, _>>(),
        }
        .map_err(part_encoding)?;
        let mut prefix = vec![0_usize];
        for bytes in items {
            let total = prefix
                .last()
                .copied()
                .unwrap_or_default()
                .checked_add(bytes)
                .and_then(|size| size.checked_add(1))
                .ok_or(SenderError::Invalid {
                    field: "part.encoding",
                })?;
            prefix.push(total);
        }
        Ok(Self {
            prefix,
            base,
            annotated_base,
        })
    }
    fn size(&self, range: Range<usize>, annotated: bool) -> Result<usize, SenderError> {
        let bytes = self
            .prefix
            .get(range.end)
            .zip(self.prefix.get(range.start))
            .and_then(|(end, start)| end.checked_sub(*start))
            .and_then(|size| size.checked_sub(usize::from(!range.is_empty())))
            .ok_or(SenderError::Invalid {
                field: "part.encoding",
            })?;
        (if annotated {
            self.annotated_base
        } else {
            self.base
        })
        .checked_add(bytes)
        .ok_or(SenderError::Invalid {
            field: "part.encoding",
        })
    }
}

#[cfg(test)]
std::thread_local! { static FULL_MEASURES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }
#[cfg(test)]
#[path = "piece_sizing_oracle.rs"]
mod sizing_oracle;
#[cfg(test)]
#[path = "piece_sizing_tests.rs"]
mod sizing_tests;
