// Pre-W-C40 cutter retained only as a differential oracle.
use super::*;
pub(crate) fn cut_transaction_oracle<A: PieceAnnotation>(
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
    // Check the whole annotation independently before attributing an excessive
    // combined unit to its final row or statement. Ownership moves through each
    // real carrier; consumer annotations need not implement Clone.
    if annotation.is_some() {
        let measured = builder.probe::<A::Protocol>(count..count, annotation)?;
        if measured.encoded_size > MAX_UNIT_BYTES {
            return Err(UnitTooLarge::Annotation {
                commit_id: captured.position.sequence,
                encoded_size: measured.encoded_size,
            }
            .into());
        }
        annotation = measured.unit.annotation;
    }
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
            let candidate_annotation = if candidate_end == count {
                annotation.take()
            } else {
                None
            };
            let measured =
                builder.probe::<A::Protocol>(start..candidate_end, candidate_annotation)?;
            if candidate_end == count {
                annotation = measured.unit.annotation;
            }
            if measured.encoded_size > TARGET_UNIT_BYTES && end > start {
                break;
            }
            if measured.encoded_size > MAX_UNIT_BYTES {
                if candidate_end == count && annotation.is_some() {
                    let alone = builder.probe::<A::Protocol>(start..candidate_end, None)?;
                    if alone.encoded_size > MAX_UNIT_BYTES {
                        return Err(builder.item_error(start, alone.encoded_size)?);
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
                    return builder.finish_oracle::<A::Protocol>(fragments);
                }
                return Err(builder.item_error(start, measured.encoded_size)?);
            }
            end = candidate_end;
            if measured.encoded_size >= TARGET_UNIT_BYTES {
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
    builder.finish_oracle::<A::Protocol>(fragments)
}

impl PartBuilder<'_> {
    fn finish_oracle<P: ConsumerProtocol>(
        &self,
        fragments: Vec<Fragment<P::Annotation>>,
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
                let measured = self.measure::<P>(fragment.range, fragment.annotation, part)?;
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
