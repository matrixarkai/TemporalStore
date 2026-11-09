// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The stored form of one Sequence row, and the one place that reads it.
//!
//! A row is the value of one point in a feature series, and it is ONE shape: the protobuf wire form
//! `SequenceFeatureRow::encode_feature_proto_value` produces (fields 1-4: `gid`, `action_type`,
//! `duration`, `author_id`). The row's instant is the timestamp of the point it is stored under, so
//! the value does not repeat it. `SequenceAdd` and `FeatureAppend` both store this shape and both
//! read paths (`SequenceQuery` and the filtered feature range) read it through
//! [`decode_sequence_row_value`].
//!
//! There is no JSON arm and no first-byte sniff. `SequenceAdd` used to store a JSON object, which
//! the filtered feature range could not read, while `FeatureAppend` rows could not be read by
//! `SequenceQuery`; the first release carries no installed base, so the second shape is deleted
//! rather than kept readable.
//!
//! # A ROW THAT CANNOT BE READ IS COUNTED, NEVER SILENTLY DROPPED
//!
//! The reader used to be `.ok()`, so a value that was not a row vanished from a query result and said
//! nothing. [`note_unreadable_sequence_row`] counts it. The row is still left out of the result:
//! inventing one would be a plausible wrong answer.
use crate::types::SequenceFeatureRow;

static SEQUENCE_ROWS_UNREADABLE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// How many stored values a Sequence read has skipped since this process started because they
/// were not a row. A feature key that holds values which are not rows counts every time it is read
/// as a sequence.
pub fn sequence_rows_unreadable() -> u64 {
    SEQUENCE_ROWS_UNREADABLE.load(std::sync::atomic::Ordering::Relaxed)
}

/// Count a value that could not be read. Warns once per process, on the first, so a key full of
/// non-rows does not write a line per row per read.
pub(super) fn note_unreadable_sequence_row(timestamp_ms: u64, reason: &str) {
    let before = SEQUENCE_ROWS_UNREADABLE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if before == 0 {
        tracing::warn!(
            timestamp_ms,
            reason,
            "a stored value is not a readable sequence row and is left out of the result; later \
             ones are counted by `sequence_rows_unreadable()` without a line each"
        );
    }
}

/// A stored value as a row. THE ONLY READER of one.
///
/// `timestamp_ms` is the instant the value is stored under: the value does not carry it.
pub(super) fn decode_sequence_row_value(
    timestamp_ms: u64,
    bytes: &[u8],
) -> Result<SequenceFeatureRow, String> {
    if bytes.is_empty() {
        return Err("the value is empty".to_string());
    }
    SequenceFeatureRow::decode_feature_proto_value(timestamp_ms, bytes)
        .ok_or_else(|| "the value is not a protobuf sequence row".to_string())
}

/// [`decode_sequence_row_value`] for a read path: an unreadable value is counted and left out.
pub(super) fn decode_or_count_sequence_row(timestamp_ms: u64, bytes: &[u8]) -> Option<SequenceFeatureRow> {
    match decode_sequence_row_value(timestamp_ms, bytes) {
        Ok(row) => Some(row),
        Err(reason) => {
            note_unreadable_sequence_row(timestamp_ms, &reason);
            None
        }
    }
}
