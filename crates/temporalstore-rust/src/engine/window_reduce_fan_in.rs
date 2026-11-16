// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The ranked read over cell samples of SEVERAL keys that share one spec, as a pure function of
//! decoded samples.
//!
//! # WHAT IT ANSWERS
//!
//! A deployment that puts the dimensions it always queries by into the key (an entity, a channel, a
//! kind) stores one series per combination. A question that spans combinations ("the best members
//! of this entity over every channel") used to cost one ranked read per key and a merge by the
//! caller. The caller's merge cannot be exact: the top `k` of each key's answer does not contain
//! every member that is in the top `k` of the union (a member that is second in each of two keys
//! can lead their sum). This read merges BEFORE it scores, so the answer is the one a single key
//! holding every key's samples would give.
//!
//! ```text
//! per key: window -> samples
//! all keys: order samples by (stamp, key position) -> the single-key read over that order
//! ```
//!
//! # SEMANTICS, EXACTLY
//!
//! * The window applies to EACH key, with the same meaning it has for one key. `LastN { n }` is
//!   the newest `n` samples of each key, not of the union.
//!   A `Last` window that is anchored on the newest sample anchors on each key's own newest one.
//! * The samples of all keys are ordered by stamp, and a tie between keys is broken by the
//!   key's position in the request. That order is the order cells combine in, so `Sum`, `Max`
//!   and `Min` lanes are independent of the key order, while a `First` lane takes the earliest
//!   stamp's value (the lowest key position at a tied stamp) and a `Last` lane the latest (the
//!   highest key position at a tied stamp). Labels are the last cell's, by the same order.
//! * Every other stage (the predicates, the score, the top-k, the projection) is the single-key
//!   read's, run by the same code ([`Plan::run_ordered`]), so the two cannot drift apart.
//! * A key listed twice contributes twice. The command refuses duplicates by name; this function
//!   sees positions, not names.
//!
//! # BOUNDS
//!
//! [`MAX_FAN_IN_KEYS`] keys, [`MAX_FAN_IN_SAMPLES`] chosen samples and [`MAX_FAN_IN_CELLS`]
//! cells in those samples, per request. Each is checked BEFORE any cell is visited and a request
//! over one is refused whole, never truncated: an answer computed from a prefix of the keys would
//! look complete.
//!
//! # COST
//!
//! The allocations are one vector of sample views sized by the sample count, plus what the
//! single-key read allocates (sized by the sample count, the lane count and `k`). NOTHING IS
//! SIZED BY THE NUMBER OF CELLS OR MEMBERS. The module is pure: it reads decoded samples and a
//! clock value and touches no engine state.

use super::window_reduce_rank::{Plan, SampleRead};
#[cfg(test)]
use super::window_reduce_rank::SampleView;
use super::cell_set::{within, within_nonzero};
use crate::types::RankedCells;

/// Keys one fan-in read may merge.
pub(crate) const MAX_FAN_IN_KEYS: usize = 64;
/// Samples, summed over the keys, one fan-in read may merge.
pub(crate) const MAX_FAN_IN_SAMPLES: usize = 1024;
/// Cells, summed over the chosen samples, one fan-in read may visit.
pub(crate) const MAX_FAN_IN_CELLS: usize = 262_144;

/// The checks that need no sample: how many keys.
pub(crate) fn validate_key_count(keys: usize) -> Result<(), String> {
    within_nonzero("keys in a fan-in read", keys, MAX_FAN_IN_KEYS)
}

/// The checks on the key list a command carries: how many, and none twice. A key named twice
/// would contribute twice, so the request is refused naming it.
pub(crate) fn validate_keys(keys: &[String]) -> Result<(), String> {
    validate_key_count(keys.len())?;
    for (position, key) in keys.iter().enumerate() {
        if let Some(first) = keys[..position].iter().position(|earlier| earlier == key) {
            return Err(format!("key {position} repeats key {first}: a key is read once per request"));
        }
    }
    Ok(())
}

/// Reads one batch of independent ranked reads may carry.
pub(crate) const MAX_BATCH_READS: usize = 64;

/// The checks on a batch of independent reads: how many, and each read's own spec.
pub(crate) fn validate_batch(queries: &[crate::types::CellRankBatchItem]) -> Result<(), String> {
    within_nonzero("reads in a batch", queries.len(), MAX_BATCH_READS)?;
    for (position, query) in queries.iter().enumerate() {
        super::window_reduce_rank::validate_spec(&query.spec).map_err(|why| format!("read {position}: {why}"))?;
    }
    Ok(())
}

/// The checks on what the window chose, summed over the keys.
pub(crate) fn validate_chosen(snapshots: usize, cells: usize) -> Result<(), String> {
    within("samples in a fan-in read", snapshots, MAX_FAN_IN_SAMPLES)?;
    within("cells in a fan-in read", cells, MAX_FAN_IN_CELLS)
}

impl Plan {
    /// Runs the plan over the samples of every key in `keys`, each key's slice ascending by
    /// stamp. `now_ms` is the instant a `Last` window ends at.
    pub(crate) fn run_fan_in<'a, V: SampleRead<'a>>(
        &self,
        now_ms: u64,
        keys: &[&[V]],
    ) -> Result<RankedCells, String> {
        validate_key_count(keys.len())?;

        // Every key's window first: a window over one key's bound is refused here, and the sums
        // that follow are of what was chosen, not of what the keys hold.
        let (mut snapshots, mut cells, mut keys_read) = (0usize, 0usize, 0usize);
        for (position, key) in keys.iter().enumerate() {
            let chosen = self.select_window(now_ms, key).map_err(|why| format!("key {position}: {why}"))?;
            keys_read += usize::from(!chosen.is_empty());
            snapshots += chosen.len();
            cells += chosen.iter().map(|snapshot| snapshot.cell_count()).sum::<usize>();
            // Refused as soon as it is over, so a request of many large keys stops early.
            validate_chosen(snapshots, cells)?;
        }

        // The allocation sized by the sample count. The views are appended in key position order
        // and the sort is STABLE, so a tie between keys at one stamp stays in position order.
        let mut ordered: Vec<V> = Vec::with_capacity(snapshots);
        for key in keys {
            ordered.extend_from_slice(self.select_window(now_ms, key)?);
        }
        ordered.sort_by_key(|snapshot| snapshot.stamp());
        let mut ranked = self.run_ordered(&ordered)?;
        ranked.keys = keys_read;
        Ok(ranked)
    }
}

#[cfg(test)]
#[path = "window_reduce_fan_in_tests.rs"]
mod tests;
