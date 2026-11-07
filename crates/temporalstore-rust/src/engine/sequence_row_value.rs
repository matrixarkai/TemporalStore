// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The stored form of one Sequence row, and the one place that reads it.
//!
//! A row is the value of one point in a feature series. Two shapes are in use, decided from the
//! FIRST BYTE of the value and never from a setting:
//!
//!   * `{` (or JSON whitespace) -- the JSON object `SequenceAdd` has always written, carrying its
//!     own `timestamp_ms`. PERMANENT: the hold is on what is on disk, and a store written by any
//!     shipped build holds these.
//!   * anything else -- the protobuf wire form `SequenceFeatureRow::encode_feature_proto_value`
//!     produces (fields 1-4: `gid`, `action_type`, `duration`, `author_id`), with the row's
//!     timestamp taken from the point it is stored under. This is what `FeatureAppend` rows hold
//!     today and what the feature range filter already decodes.
//!
//! The two were not interchangeable before this reader: a row stored by `SequenceAdd` was invisible
//! to the feature range filter (a `{` is not a valid protobuf tag), and a row stored by
//! `FeatureAppend` was invisible to `SequenceQuery` (it is not JSON). Both read paths now go through
//! [`decode_sequence_row_value`].
//!
//! # A ROW THAT CANNOT BE READ IS COUNTED, NEVER SILENTLY DROPPED
//!
//! The reader used to be `serde_json::from_slice(..).ok()`, so a value that was not a row vanished
//! from a query result and said nothing. [`note_unreadable_sequence_row`] counts it. The row is still
//! left out of the result: inventing one would be a plausible wrong answer.
use crate::types::SequenceFeatureRow;

static SEQUENCE_ROWS_UNREADABLE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// How many stored values a Sequence read has skipped since this process started because they
/// were neither shape. A feature key that holds values which are not rows counts every time it is
/// read as a sequence.
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

/// A stored value in whichever shape it holds. THE ONLY READER of one.
///
/// `timestamp_ms` is the instant the value is stored under: the protobuf shape does not carry it.
pub(super) fn decode_sequence_row_value(
    timestamp_ms: u64,
    bytes: &[u8],
) -> Result<SequenceFeatureRow, String> {
    let Some(&first) = bytes.first() else {
        return Err("the value is empty".to_string());
    };
    if first == b'{' || first.is_ascii_whitespace() {
        return serde_json::from_slice(bytes)
            .map_err(|error| format!("the JSON value is not a row: {error}"));
    }
    SequenceFeatureRow::decode_feature_proto_value(timestamp_ms, bytes)
        .ok_or_else(|| "the value is not a row in either shape".to_string())
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
