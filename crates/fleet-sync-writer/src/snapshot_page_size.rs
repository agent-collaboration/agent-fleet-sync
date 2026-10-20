use crate::OpenError;
use fleet_sync_protocol::{SnapshotCursor, SnapshotPage, SqlValue, unit_budget::encoded_json_size};

/// Retain each candidate's byte count for a final check without encoding its
/// body again. The initial size comes from the actual empty-page carriers.
pub(super) struct SnapshotPageSize {
    empty_size: usize,
    initial_next_bytes: usize,
    text_framing_bytes: usize,
    encoded_rows: Vec<usize>,
}

impl SnapshotPageSize {
    pub(super) fn new(page: &SnapshotPage, empty_size: usize) -> Result<Self, OpenError> {
        if !page.rows.is_empty() {
            return Err(size_changed());
        }
        Ok(Self {
            empty_size,
            initial_next_bytes: cursor_bytes(&page.next)?,
            // Let SqlValue's real serializer own its tag, field names and quotes.
            text_framing_bytes: encoded_json_size(&SqlValue::Text(String::new()))
                .map_err(|_| encoding_error())?,
            encoded_rows: Vec::new(),
        })
    }

    pub(super) fn row_bytes(&self, row: &[SqlValue]) -> Result<usize, OpenError> {
        row_bytes(row, self.text_framing_bytes)
    }

    pub(super) fn after_row(
        &self,
        previous_size: usize,
        row_bytes: usize,
        previous_next: &Option<SnapshotCursor>,
        next: &Option<SnapshotCursor>,
    ) -> Result<usize, OpenError> {
        let previous_next_bytes = cursor_bytes(previous_next)?;
        let next_bytes = cursor_bytes(next)?;
        previous_size
            .checked_sub(previous_next_bytes)
            .and_then(|size| size.checked_add(usize::from(!self.encoded_rows.is_empty())))
            .and_then(|size| size.checked_add(row_bytes))
            .and_then(|size| size.checked_add(next_bytes))
            .ok_or_else(encoding_error)
    }

    pub(super) fn record_row(&mut self, row_bytes: usize) {
        self.encoded_rows.push(row_bytes);
    }

    pub(super) fn final_size(&self, page: &SnapshotPage) -> Result<usize, OpenError> {
        if page.rows.len() != self.encoded_rows.len() {
            return Err(size_changed());
        }
        // Rebuild from the original carrier, all retained row byte counts, one
        // comma per additional row, and the actual final cursor. This does not
        // trust the candidate-size accumulator used to choose the page boundary.
        let row_bytes = self.encoded_rows.iter().try_fold(0_usize, |sum, bytes| {
            sum.checked_add(*bytes).ok_or_else(encoding_error)
        })?;
        let next_bytes = cursor_bytes(&page.next)?;
        self.empty_size
            .checked_sub(self.initial_next_bytes)
            .and_then(|size| size.checked_add(row_bytes))
            .and_then(|size| size.checked_add(page.rows.len().saturating_sub(1)))
            .and_then(|size| size.checked_add(next_bytes))
            .ok_or_else(encoding_error)
    }
}

fn cursor_bytes(cursor: &Option<SnapshotCursor>) -> Result<usize, OpenError> {
    encoded_json_size(cursor).map_err(|_| encoding_error())
}

fn row_bytes(row: &[SqlValue], text_framing_bytes: usize) -> Result<usize, OpenError> {
    // JSON array brackets and commas; the value framing remains serializer-owned.
    let initial = 2_usize
        .checked_add(row.len().saturating_sub(1))
        .ok_or_else(encoding_error)?;
    row.iter().try_fold(initial, |size, value| {
        let value_bytes = match value {
            SqlValue::Text(text) => text_framing_bytes
                .checked_add(text_bytes(text)?)
                .ok_or_else(encoding_error)?,
            SqlValue::Null | SqlValue::Integer(_) | SqlValue::Real(_) | SqlValue::Blob(_) => {
                encoded_json_size(value).map_err(|_| encoding_error())?
            }
        };
        size.checked_add(value_bytes).ok_or_else(encoding_error)
    })
}

fn text_bytes(text: &str) -> Result<usize, OpenError> {
    // Compact JSON emits UTF-8 unchanged. A short escape adds one byte; the
    // remaining ASCII controls use six-byte \u00XX escapes, adding five. ASCII
    // searches avoid Serde's formatter/write stack for every escaped fragment.
    let mut bytes = text.len();
    for character in ['"', '\\', '\u{8}', '\t', '\n', '\u{c}', '\r'] {
        bytes = bytes
            .checked_add(text.matches(character).count())
            .ok_or_else(encoding_error)?;
    }
    for byte in 0_u8..32 {
        if !matches!(byte, 8 | 9 | 10 | 12 | 13) {
            bytes = bytes
                .checked_add(
                    text.matches(char::from(byte))
                        .count()
                        .checked_mul(5)
                        .ok_or_else(encoding_error)?,
                )
                .ok_or_else(encoding_error)?;
        }
    }
    Ok(bytes)
}

fn encoding_error() -> OpenError {
    OpenError::corrupt("snapshot.encoding")
}
fn size_changed() -> OpenError {
    OpenError::corrupt("snapshot.size_changed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sender_test_fixture::TestResult;

    #[test]
    fn generated_row_sizes_match_real_json_for_every_sql_value_class() -> TestResult {
        let framing = encoded_json_size(&SqlValue::Text(String::new()))?;
        let controls: String = (0_u8..=127).map(char::from).collect();
        let mut pattern = controls;
        pattern.push_str("雪🦀\u{2028}\u{2029}/\\\"");
        for repetitions in [0, 1, 2, 7, 31, 63, 127, 255] {
            let row = vec![
                SqlValue::Null,
                SqlValue::Integer(i64::MIN),
                SqlValue::Integer(i64::MAX),
                SqlValue::Real(-0.0),
                SqlValue::Real(1.0e-300),
                SqlValue::Real(f64::INFINITY),
                SqlValue::Real(f64::NEG_INFINITY),
                SqlValue::Text(pattern.repeat(repetitions)),
                SqlValue::Blob((0_u8..=255).cycle().take(repetitions * 17).collect()),
            ];
            let actual = row_bytes(&row, framing)?;
            let expected = serde_json::to_vec(&row)?.len();
            if actual != expected {
                return Err(
                    format!("row length differs at {repetitions}: {actual}/{expected}").into(),
                );
            }
        }
        if row_bytes(&[], framing)? != serde_json::to_vec(&Vec::<SqlValue>::new())?.len() {
            return Err("empty row length differs".into());
        }
        Ok(())
    }

    #[test]
    fn sizing_preserves_the_real_serializers_nan_refusal() -> TestResult {
        let framing = encoded_json_size(&SqlValue::Text(String::new()))?;
        let row = [SqlValue::Real(f64::NAN)];
        if row_bytes(&row, framing).is_ok() || serde_json::to_vec(&row).is_ok() {
            return Err("NaN was accepted by sizing or serialization".into());
        }
        Ok(())
    }
}
