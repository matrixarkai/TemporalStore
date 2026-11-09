// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The stored form of a Control State counter series block.
//!
//! A counter series is a `BTreeMap<u64, i64>` of points, and the block that holds it is the WHOLE
//! series rewritten on every write (bounded by the series cap, not by this module). A block is ONE
//! shape: a protobuf `DurableControlStateSeries` holding two parallel packed arrays, the timestamps
//! and the values, in strictly ascending timestamp order. There is no container byte, no JSON
//! arm and no first-byte sniff: the first release carries no installed base, so there is no older
//! shape to tell apart from this one.
//!
//! # A BLOCK THAT CANNOT BE READ IS COUNTED, NEVER AN EMPTY SERIES
//!
//! The load path used to be `if let Ok(series) = serde_json::from_slice(..)`, so a block that did not
//! parse left the series ABSENT and said nothing: a counter that read as never having been written.
//! [`note_unreadable_control_state_block`] counts the refusal and warns, so the loss is visible. It
//! still does not invent a series: the key stays absent, as it did, because the alternative -- a
//! zero series -- would be a plausible wrong answer.
//!
//! The same rule is why a block holding NO points is refused as well. The writer never stores one (a
//! series with no points removes its block), and an empty protobuf message is also what damage that
//! decodes to nothing looks like, so reading it as an empty series would turn corruption into a
//! counter that was never written.
use super::*;
use crate::sdk::v1;
use prost::Message as _;

static CONTROL_STATE_BLOCKS_UNREADABLE: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// How many series blocks this process has refused to read since it started.
pub fn control_state_blocks_unreadable() -> u64 {
    CONTROL_STATE_BLOCKS_UNREADABLE.load(std::sync::atomic::Ordering::Relaxed)
}

/// Count and report a block that could not be read. The key is left absent by the caller.
pub(super) fn note_unreadable_control_state_block(key: &str, reason: &str) {
    CONTROL_STATE_BLOCKS_UNREADABLE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    tracing::warn!(
        key,
        reason,
        "a control-state series block could not be read; the series is absent after this load"
    );
}

/// A series block. THE ONLY READER of one.
pub(super) fn decode_control_state_block(bytes: &[u8]) -> Result<BTreeMap<u64, i64>, String> {
    if bytes.is_empty() {
        return Err("the block is empty".to_string());
    }
    let proto = v1::DurableControlStateSeries::decode(bytes)
        .map_err(|error| format!("the block did not decode: {error}"))?;
    if proto.timestamps_ms.len() != proto.values.len() {
        return Err(format!(
            "the block has {} timestamps and {} values",
            proto.timestamps_ms.len(),
            proto.values.len()
        ));
    }
    if proto.timestamps_ms.is_empty() {
        return Err("the block holds no points".to_string());
    }
    // Strictly ascending: the writer iterates a `BTreeMap`, so anything else is damage, and
    // accepting a repeat would silently keep only one of two values for a timestamp.
    if proto.timestamps_ms.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err("the block's timestamps are not strictly ascending".to_string());
    }
    Ok(proto.timestamps_ms.into_iter().zip(proto.values).collect())
}

/// A series as a block. THE ONLY ENCODER: `persist_control_state_block` calls it.
pub(crate) fn encode_control_state_block(series: &BTreeMap<u64, i64>) -> Vec<u8> {
    v1::DurableControlStateSeries {
        timestamps_ms: series.keys().copied().collect(),
        values: series.values().copied().collect(),
    }
    .encode_to_vec()
}
