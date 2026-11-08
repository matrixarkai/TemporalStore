// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The stored form of a Control State counter series block.
//!
//! A counter series is a `BTreeMap<u64, i64>` of points, and the block that holds it is the WHOLE
//! series rewritten on every write (bounded by the series cap, not by this module). Three shapes
//! can be in a block, decided from the FIRST BYTE and never from a setting:
//!
//!   * `{` (or JSON whitespace) -- the JSON object every block written so far is. PERMANENT: the
//!     hold is on what is on disk.
//!   * `0x01` -- PACKED: this container byte, then a protobuf `DurableControlStateSeries` holding
//!     two parallel packed arrays, the timestamps and the values, in ascending timestamp order.
//!   * `0x02` -- RESERVED for a delta-coded shape (timestamps as gaps, which is what makes a point a
//!     couple of bytes instead of several). No build writes it and this one does not read it; it is
//!     refused by name, NOT read as the packed shape and NOT treated as an empty series. Reserving
//!     the byte now is what lets a later build add the shape without this build mis-reading it.
//!
//! ANY OTHER CONTROL BYTE is an unknown container and is refused the same way.
//!
//! # A BLOCK THAT CANNOT BE READ IS COUNTED, NEVER AN EMPTY SERIES
//!
//! The load path used to be `if let Ok(series) = serde_json::from_slice(..)`, so a block that did not
//! parse left the series ABSENT and said nothing: a counter that read as never having been written.
//! [`note_unreadable_control_state_block`] counts the refusal and warns, so the loss is visible. It
//! still does not invent a series: the key stays absent, as it did, because the alternative -- a
//! zero series -- would be a plausible wrong answer.
use super::*;
use crate::sdk::v1;
use prost::Message as _;

/// First byte of a packed series block. See the module note.
pub(crate) const CONTROL_STATE_BLOCK_PACKED: u8 = 0x01;
/// First byte reserved for a delta-coded series block. Refused, never read. See the module note.
pub(crate) const CONTROL_STATE_BLOCK_DELTA_RESERVED: u8 = 0x02;

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

/// A series block in whichever shape it holds. THE ONLY READER of one.
pub(super) fn decode_control_state_block(bytes: &[u8]) -> Result<BTreeMap<u64, i64>, String> {
    let Some((&first, body)) = bytes.split_first() else {
        return Err("the block is empty".to_string());
    };
    match first {
        CONTROL_STATE_BLOCK_PACKED => {
            let proto = v1::DurableControlStateSeries::decode(body)
                .map_err(|error| format!("the packed block did not decode: {error}"))?;
            if proto.timestamps_ms.len() != proto.values.len() {
                return Err(format!(
                    "the packed block has {} timestamps and {} values",
                    proto.timestamps_ms.len(),
                    proto.values.len()
                ));
            }
            // Strictly ascending: the writer iterates a `BTreeMap`, so anything else is damage, and
            // accepting a repeat would silently keep only one of two values for a timestamp.
            if proto.timestamps_ms.windows(2).any(|pair| pair[0] >= pair[1]) {
                return Err("the packed block's timestamps are not strictly ascending".to_string());
            }
            Ok(proto
                .timestamps_ms
                .into_iter()
                .zip(proto.values)
                .collect())
        }
        CONTROL_STATE_BLOCK_DELTA_RESERVED => Err(
            "the block uses container byte 0x02, reserved for a delta-coded series this build \
             does not read"
                .to_string(),
        ),
        // Whitespace JSON may start with, and `{`; every other control byte is a container this
        // build has never heard of.
        other if other < 0x20 && !matches!(other, b'\t' | b'\n' | b'\r') => Err(format!(
            "the block starts with unknown container byte {other:#04x}"
        )),
        _ => serde_json::from_slice::<BTreeMap<u64, i64>>(bytes)
            .map_err(|error| format!("the block is not a JSON series: {error}")),
    }
}

/// A series as the packed block. THE ONLY ENCODER: `persist_control_state_block` calls it and
/// nothing writes the JSON object any more. See the module note for why the decoder keeps its JSON
/// arm while this one has none.
pub(crate) fn encode_control_state_block(series: &BTreeMap<u64, i64>) -> Vec<u8> {
    let proto = v1::DurableControlStateSeries {
        timestamps_ms: series.keys().copied().collect(),
        values: series.values().copied().collect(),
    };
    let mut out = vec![CONTROL_STATE_BLOCK_PACKED];
    out.extend(proto.encode_to_vec());
    out
}
