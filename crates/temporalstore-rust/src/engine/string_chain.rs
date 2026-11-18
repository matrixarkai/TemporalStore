// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE ARITHMETIC OF A STRING THAT IS A BASE BLOCK PLUS A CHAIN OF SMALL TAIL SEGMENTS.
//!
//! An APPEND used to read the whole value, join the new bytes on, and write the whole value back,
//! so its cost grew with the value. A string can instead be stored as one base block and a chain of
//! small segment blocks, each holding only the bytes one append added. The append then writes the
//! segment alone, and a compaction folds the chain into a new base now and then.
//!
//! This file holds only the decisions that do not depend on the storage layout, so they can be
//! pinned by plain unit tests: how long the whole value is, whether an append would pass the length
//! a block can state, and when the chain has grown enough to fold. The layout (which blocks, where
//! they are listed, how they are filed and replayed) is in the callers.
//!
//! # LENGTHS
//!
//! A block states its own length as a `u32`, so no single block, and therefore no base, may be
//! longer than `u32::MAX` bytes. The chain keeps the base's LOGICAL length in a length card (the
//! entry at ordinal 0 of the slot's owned array), because the stored length of a compressed base is
//! not what a reader of STRLEN wants. Segments are stored uncompressed, so their stored length is
//! their logical length. The whole value is the card plus the segments.
//!
//! THE WHOLE VALUE IS CAPPED AT `u32::MAX` TOO. Not because a reader could not assemble more, but
//! because a compaction writes the whole value back as ONE block, and that block cannot state more.
//! An append that would pass the cap is refused, and nothing is written.

// The writer that uses these lands in the next change of the series; until then the decisions are
// exercised by their own tests only.
#![allow(dead_code)]

use crate::block_store::BlockStore;
use crate::types::ShardId;
use matrixcache::MultiLayerCache;

use super::hashing::BlockIdentity;
use super::state::ShardState;

/// The longest a base, and so the longest whole value, may be: what one block can state.
pub(crate) const MAX_VALUE_LEN: u64 = u32::MAX as u64;

/// The chain length at which an append compacts inline instead of adding a segment. The ordinal a
/// segment carries is sixteen bits, so 65 535 is the format's own limit; this is far below it so a
/// read, which pays one block read per segment, stays cheap.
pub(crate) const MAX_SEGMENTS_INLINE: usize = 256;

/// The segment count at which the storage round, not the write path, folds the chain.
pub(crate) const COMPACT_AT_SEGMENTS: usize = 32;

/// The tail a chain may hold before it is folded, whatever its segment count: the larger of this and
/// a quarter of the base.
pub(crate) const COMPACT_AT_TAIL_FLOOR: u64 = 64 * 1024;

/// What the chain holds, as the lengths alone say it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChainShape {
    /// The base's logical length, from the length card.
    pub base_len: u64,
    /// Each segment's length, in ordinal order.
    pub segment_lens: Vec<u64>,
}

impl ChainShape {
    /// A value with no segments.
    pub(crate) fn plain(base_len: u64) -> Self {
        Self {
            base_len,
            segment_lens: Vec::new(),
        }
    }

    /// The bytes the segments hold between them.
    pub(crate) fn tail_len(&self) -> u64 {
        self.segment_lens.iter().sum()
    }

    /// The whole value's length: what STRLEN answers and what an APPEND returns.
    pub(crate) fn total_len(&self) -> u64 {
        self.base_len + self.tail_len()
    }
}

/// Why an append was not planned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppendRefused {
    /// The whole value would be longer than one block can state.
    TooLong,
}

/// What an append is to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppendPlan {
    /// Write one segment holding the new bytes, numbered `ordinal`, and the new length card total.
    Segment { ordinal: u16, new_total: u64 },
    /// The chain is at its inline ceiling: fold it with the new bytes into a new base.
    Compact { new_total: u64 },
}

/// Decide an append of `added` bytes onto `shape`, touching nothing.
///
/// THE LENGTH CHECK COMES FIRST AND DOES NOT DEPEND ON THE ANSWER BELOW: an append that would pass
/// the cap is refused whether it would have been a segment or a compaction, so the caller never has
/// to undo a half-written segment.
pub(crate) fn plan_append(shape: &ChainShape, added: u64) -> Result<AppendPlan, AppendRefused> {
    let new_total = shape
        .total_len()
        .checked_add(added)
        .filter(|total| *total <= MAX_VALUE_LEN)
        .ok_or(AppendRefused::TooLong)?;
    if shape.segment_lens.len() >= MAX_SEGMENTS_INLINE {
        return Ok(AppendPlan::Compact { new_total });
    }
    // Segment ordinals start at 1; 0 is the length card.
    let ordinal = shape.segment_lens.len() + 1;
    Ok(AppendPlan::Segment {
        ordinal: ordinal as u16,
        new_total,
    })
}

/// Whether the storage round should fold this chain now. A chain with no segments never needs it.
pub(crate) fn should_compact(shape: &ChainShape) -> bool {
    if shape.segment_lens.is_empty() {
        return false;
    }
    shape.segment_lens.len() >= COMPACT_AT_SEGMENTS
        || shape.tail_len() > COMPACT_AT_TAIL_FLOOR.max(shape.base_len / 4)
}

/// Why a chain could not be read back.
///
/// NONE OF THESE IS A SHORT VALUE. A chain the reader cannot complete is an error the caller must
/// surface: answering with the prefix it did find would hand back an acknowledged value that is
/// missing its tail, which reads as success.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StringChainError {
    /// The slot lists segments but the base block cannot be read.
    BaseMissing,
    /// The slot lists this segment ordinal and its block cannot be read.
    SegmentMissing { ordinal: u64 },
    /// The base read back a length the card does not state.
    LengthMismatch { card: u64, read: u64 },
}

/// Join a base and its segments, in the order given, into the whole value.
///
/// The one place the chain's bytes are put together, so the reader and the tests cannot disagree
/// about the order.
pub(crate) fn assemble(base: Vec<u8>, segments: impl IntoIterator<Item = Vec<u8>>) -> Vec<u8> {
    let mut value = base;
    for segment in segments {
        value.extend_from_slice(&segment);
    }
    value
}

/// Read a string's whole value, whether it is one block or a base and a chain.
///
/// `Ok(None)` is a key with no value. A key with no listed segments reads exactly as it always has
/// (one block). A key whose slot lists segments is read as the base followed by every segment in
/// ordinal order, and any block that cannot be read is an error, never a shorter value.
///
/// THE LENGTH CARD IS CHECKED against what the base actually read back, because it is the one
/// statement of the base's logical length that was written separately from the bytes: a base
/// replaced under a stale card would otherwise be served with the wrong tail attached.
pub(super) fn read_string_assembled(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    key: &str,
) -> Result<Option<Vec<u8>>, StringChainError> {
    let base = super::bucket_store::read_bucket_index_value(
        cache, block_store, shard_id, shard, "string", key, None,
    );
    let owned = shard
        .strings()
        .get(key)
        .and_then(|slot| slot.owned_elements());
    let Some(owned) = owned.filter(|entries| !entries.is_empty()) else {
        return Ok(base);
    };
    let Some(base) = base else {
        return Err(StringChainError::BaseMissing);
    };
    // Ordinal 0 is the length card; the segments follow it, already in ordinal order.
    if let Some((_, card)) = owned.iter().find(|(ordinal, _)| *ordinal == 0) {
        if card.length() != base.len() as u64 {
            return Err(StringChainError::LengthMismatch {
                card: card.length(),
                read: base.len() as u64,
            });
        }
    }
    let (start, end) = shard.routing_range();
    let bucket = Some(crate::engine::hashing::block_routing_bucket(key, start, end));
    let mut parts = Vec::with_capacity(owned.len());
    for (ordinal, entry) in owned.iter().filter(|(ordinal, _)| *ordinal != 0) {
        // The identity is the string's own, with NO element name: a segment is a plain block of the
        // object, not a container frame, and an element name here would select into it.
        let bytes = super::read_block_bytes(
            cache,
            block_store,
            shard_id,
            entry,
            BlockIdentity::of(shard_id, "string", key, None),
            bucket,
        )
        .ok_or(StringChainError::SegmentMissing { ordinal: *ordinal })?;
        parts.push(bytes);
    }
    Ok(Some(assemble(base, parts)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_base_and_its_segments_are_joined_in_the_order_given() {
        let value = assemble(b"abc".to_vec(), vec![b"12".to_vec(), Vec::new(), b"xyz".to_vec()]);
        assert_eq!(value, b"abc12xyz");
        assert_eq!(assemble(Vec::new(), Vec::new()), Vec::<u8>::new());
    }

    fn chain(base: u64, segments: &[u64]) -> ChainShape {
        ChainShape {
            base_len: base,
            segment_lens: segments.to_vec(),
        }
    }

    #[test]
    fn the_whole_value_is_the_card_plus_every_segment() {
        let shape = chain(1_000, &[16, 16, 4]);
        assert_eq!(shape.tail_len(), 36);
        assert_eq!(shape.total_len(), 1_036);
        assert_eq!(ChainShape::plain(7).total_len(), 7);
    }

    #[test]
    fn an_append_adds_one_segment_numbered_after_the_last() {
        let plan = plan_append(&chain(144, &[]), 16).unwrap();
        assert_eq!(
            plan,
            AppendPlan::Segment {
                ordinal: 1,
                new_total: 160
            }
        );
        let plan = plan_append(&chain(144, &[16, 16]), 16).unwrap();
        assert_eq!(
            plan,
            AppendPlan::Segment {
                ordinal: 3,
                new_total: 192
            }
        );
    }

    #[test]
    fn an_append_past_the_length_a_block_can_state_is_refused_and_writes_nothing() {
        // The check is on the arithmetic, so no 4 GiB buffer is needed.
        let at_the_cap = chain(MAX_VALUE_LEN, &[]);
        assert_eq!(plan_append(&at_the_cap, 1), Err(AppendRefused::TooLong));
        assert_eq!(
            plan_append(&chain(MAX_VALUE_LEN - 10, &[4, 4]), 3),
            Err(AppendRefused::TooLong)
        );
        // Exactly the cap is allowed; one more is not.
        assert!(plan_append(&chain(MAX_VALUE_LEN - 16, &[]), 16).is_ok());
        assert_eq!(
            plan_append(&chain(MAX_VALUE_LEN - 16, &[]), 17),
            Err(AppendRefused::TooLong)
        );
        // A refusal beats a compaction: a chain at its inline ceiling past the cap is still refused.
        let full = chain(MAX_VALUE_LEN - 100, &vec![1; MAX_SEGMENTS_INLINE]);
        assert_eq!(plan_append(&full, 1_000), Err(AppendRefused::TooLong));
        // The sum cannot wrap.
        assert_eq!(
            plan_append(&chain(u64::MAX - 1, &[]), 5),
            Err(AppendRefused::TooLong)
        );
    }

    #[test]
    fn a_chain_at_its_inline_ceiling_compacts_instead_of_growing() {
        let full = chain(1_000, &vec![8; MAX_SEGMENTS_INLINE]);
        let expected = 1_000 + 8 * MAX_SEGMENTS_INLINE as u64 + 8;
        assert_eq!(
            plan_append(&full, 8),
            Ok(AppendPlan::Compact {
                new_total: expected
            })
        );
        let one_short = chain(1_000, &vec![8; MAX_SEGMENTS_INLINE - 1]);
        assert!(matches!(
            plan_append(&one_short, 8),
            Ok(AppendPlan::Segment { ordinal: 256, .. })
        ));
    }

    #[test]
    fn the_storage_round_folds_by_count_or_by_tail_size() {
        assert!(!should_compact(&ChainShape::plain(1 << 20)));
        // Few small segments on a large base: leave them.
        assert!(!should_compact(&chain(1 << 20, &[16; 31])));
        // The count threshold.
        assert!(should_compact(&chain(1 << 20, &[16; COMPACT_AT_SEGMENTS])));
        // The tail threshold: the larger of 64 KiB and a quarter of the base.
        assert!(!should_compact(&chain(1 << 20, &[200 * 1024])));
        assert!(should_compact(&chain(1 << 20, &[300 * 1024])));
        // A small base uses the floor, not a quarter of itself.
        assert!(!should_compact(&chain(1_000, &[60 * 1024])));
        assert!(should_compact(&chain(1_000, &[70 * 1024])));
    }
}
