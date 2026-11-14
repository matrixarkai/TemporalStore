// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Rolling old cell snapshots up into coarser time buckets, and capping a label group, as a pure
//! function of decoded snapshots.
//!
//! A key written every minute for a month holds 43,200 snapshots, each carrying every member.
//! Retention by trimming can only drop the oldest of them; it cannot make them cheaper. A rollup
//! replaces many old snapshots by one per bucket, and a cap bounds how many members of a label
//! group survive in it.
//!
//! # ROLLUP
//!
//! `older_than_ms` and `bucket_ms`. A snapshot whose stamp is STRICTLY below the cutoff is rolled:
//! its bucket is `floor(stamp / bucket_ms) * bucket_ms`, and every snapshot of one bucket becomes
//! one snapshot stored AT THE BUCKET START. A snapshot at exactly the cutoff, and every later one,
//! is returned as it was. Inside a bucket the cells merge by member, lane by lane, oldest snapshot
//! first, under [`combine_aggregated`]. Labels are the newest cell's, as they are for a write.
//!
//! # COMBINING IS NOT MERGING A WRITE
//!
//! The write-time merge ([`LaneReducer::combine`]) folds ONE more write into a cell, so a `Count`
//! lane there is "one more write": the value the write carries is ignored. A rollup combines cells
//! that ALREADY hold aggregates, so a `Count` lane holds a count, and counts ADD. Reusing the
//! write-time merge would make a lane that counted 3, 4 and 5 writes read 5 (the number of merge
//! events), not 12. The two are separate functions for that reason.
//!
//! | reducer | combine(older, newer)             |
//! |---------|-----------------------------------|
//! | Sum     | older + newer, SATURATING         |
//! | Count   | older + newer, SATURATING         |
//! | Max/Min | the larger / the smaller          |
//! | First   | older (the oldest snapshot's)     |
//! | Last    | newer (the newest snapshot's)     |
//!
//! SUMS SATURATE at `i64::MAX` / `i64::MIN` and never wrap, as the write-time merge and the window
//! reductions already do. A saturated value is sticky: it is a statement that the true total is at
//! least that large, not an exact total, and a later combine does not pull it back.
//!
//! A snapshot that is already a rolled bucket sits at its own bucket start, so rolling it again
//! with the same arguments changes nothing: same cells, same bytes. A rolled snapshot is treated
//! as occurring at its bucket start. A late write that lands inside a bucket that was already
//! rolled folds into it as the NEWEST contribution there.
//!
//! # CAP
//!
//! An optional per-group cap, applied to each rolled snapshot. Cells are grouped by the value of a
//! named label (every cell is one group when none is named), scored by the same [`CellScore`]
//! vocabulary the ranked read uses (resolved by the same [`Scorer`]), and the best `keep` of each
//! group survive. Higher scores are better; ties break by member ascending; a cell whose score is
//! undefined (a zero denominator) ranks after every scored cell. The count of cells dropped is
//! reported. [`cap_cells`] applies a cap to one snapshot on its own.
//!
//! The cap runs after the buckets are folded, never before: a member dropped early would lose the
//! contributions it makes later in the bucket. For the same reason a bucket whose union of
//! members is over the snapshot limit is REFUSED, not trimmed: use a smaller bucket.
//!
//! # BOUNDS
//!
//! `bucket_ms >= 1`; `1 <= keep <= `[`MAX_KEEP`]; at most [`MAX_ROLLUP_SNAPSHOTS`] snapshots below
//! the cutoff in one request, so one request cannot stall a shard. A request over the bound is
//! refused whole, naming the limit: roll a key up in steps by moving the cutoff forward.
//!
//! # PURITY AND COST
//!
//! No clock: the cutoff is in the request, so a leader, a follower and a replay reach the same
//! bytes. The snapshots are taken BY VALUE and the cells are MOVED into the result: the
//! allocations do not depend on the number of snapshots in a bucket or the number of cells (the
//! buffers of the fold are sized once and reused), and are bounded by a small constant per bucket.

use std::cmp::Ordering;

use super::cell_rank::{validate_score, Key, Scorer};
use super::cell_set::{
    is_strictly_ascending, validate_cells_in_snapshot, within, within_nonzero, Cell, CellHeader, LaneReducer,
    MAX_CELLS_PER_SNAPSHOT,
};
use crate::types::CellScore;

/// Snapshots below the cutoff one request may roll up.
pub(crate) const MAX_ROLLUP_SNAPSHOTS: usize = 1024;
/// Cells one group may keep.
pub(crate) const MAX_KEEP: usize = MAX_CELLS_PER_SNAPSHOT;

/// The request, as the command carries it.
pub(crate) use crate::types::{CellCapSpec as CapSpec, CellRollupSpec as RollupSpec};

/// One decoded snapshot: its stamp and its cells, strictly ascending by member.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Snapshot {
    pub(crate) timestamp_ms: u64,
    pub(crate) cells: Vec<Cell>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RollupStats {
    pub(crate) snapshots_before: u64,
    pub(crate) snapshots_after: u64,
    pub(crate) cells_before: u64,
    pub(crate) cells_after: u64,
    /// Cells folded into a cell of the same member in an earlier snapshot of their bucket.
    pub(crate) cells_combined: u64,
    /// Cells the cap removed.
    pub(crate) cells_dropped: u64,
}

#[derive(Debug, PartialEq)]
pub(crate) struct RollupResult {
    /// Ascending by stamp, strictly.
    pub(crate) snapshots: Vec<Snapshot>,
    pub(crate) stats: RollupStats,
}

/// The lane value after combining a cell that already aggregates earlier snapshots (`older`) with
/// one that aggregates later ones (`newer`). See the module notes for why this is not
/// [`LaneReducer::combine`].
pub(crate) fn combine_aggregated(reducer: LaneReducer, older: i64, newer: i64) -> i64 {
    match reducer {
        LaneReducer::Sum | LaneReducer::Count => older.saturating_add(newer),
        LaneReducer::Max => older.max(newer),
        LaneReducer::Min => older.min(newer),
        LaneReducer::First => older,
        LaneReducer::Last => newer,
    }
}

// ------------------------------------------------------------------------------------------
// Admission and compiling
// ------------------------------------------------------------------------------------------

/// The checks that need no header and no snapshot: a command runs this at admission.
pub(crate) fn validate_spec(spec: &RollupSpec) -> Result<(), String> {
    if spec.bucket_ms == 0 {
        return Err("bucket_ms must be at least 1".to_string());
    }
    if let Some(cap) = &spec.cap {
        within_nonzero("cap keep", cap.keep, MAX_KEEP)?;
        validate_score(&cap.score)?;
        if matches!(cap.score, CellScore::Time) {
            return Err("a cap cannot score by time: every cell of a snapshot shares its stamp".to_string());
        }
        if matches!(cap.score, CellScore::Decay { .. }) {
            return Err("a cap cannot score by decay: it scores one folded cell, and a decay reads a member's snapshots".to_string());
        }
    }
    Ok(())
}

/// A cap checked against a header.
struct Cap {
    group: Option<usize>,
    scorer: Scorer,
    keep: usize,
}

fn compile_cap(header: &CellHeader, cap: &CapSpec) -> Result<Cap, String> {
    let group = match &cap.group_by {
        None => None,
        Some(name) => Some(header.labels.iter().position(|label| label == name).ok_or_else(|| {
            format!("cap group `{name}` is not a label this key declares (labels {:?})", header.labels)
        })?),
    };
    Ok(Cap { group, scorer: Scorer::compile(header, &cap.score)?, keep: cap.keep })
}

/// The buffers a cap reuses from one snapshot to the next.
#[derive(Default)]
struct CapScratch {
    order: Vec<(u64, Option<Key>, usize)>,
    keep: Vec<bool>,
}

/// Whether a cell of `a` ranks before one of `b`: better score first, undefined last.
fn better_first(a: &Option<Key>, b: &Option<Key>) -> Ordering {
    match (a, b) {
        (Some(x), Some(y)) => y.cmp(x),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

/// Removes from `cells` (strictly ascending by member) all but the best `cap.keep` of each group.
/// Returns how many were removed.
fn apply_cap(cap: &Cap, cells: &mut Vec<Cell>, scratch: &mut CapScratch) -> Result<u64, String> {
    if cap.group.is_none() && cells.len() <= cap.keep {
        return Ok(0);
    }
    scratch.order.clear();
    scratch.order.reserve(cells.len());
    for (i, cell) in cells.iter().enumerate() {
        let group = cap.group.map_or(0, |g| cell.labels[g]);
        scratch.order.push((group, cap.scorer.score(&cell.member, 0, &cell.lanes)?, i));
    }
    // The index is the last key and cells ascend by member: ties break by member, smaller first.
    scratch
        .order
        .sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| better_first(&a.1, &b.1)).then_with(|| a.2.cmp(&b.2)));
    scratch.keep.clear();
    scratch.keep.resize(cells.len(), false);
    let (mut group, mut in_group) = (0u64, 0usize);
    for (n, &(g, _, i)) in scratch.order.iter().enumerate() {
        if n == 0 || g != group {
            group = g;
            in_group = 0;
        }
        if in_group < cap.keep {
            scratch.keep[i] = true;
        }
        in_group += 1;
    }
    let before = cells.len();
    let mut at = 0;
    cells.retain(|_| {
        let kept = scratch.keep[at];
        at += 1;
        kept
    });
    Ok((before - cells.len()) as u64)
}

/// Applies `cap` to one snapshot's cells on its own. Returns how many cells were removed.
#[allow(dead_code)] // the command applies a cap through `rollup`; this is the one-snapshot form
pub(crate) fn cap_cells(header: &CellHeader, cap: &CapSpec, cells: &mut Vec<Cell>) -> Result<u64, String> {
    let spec = RollupSpec { older_than_ms: 0, bucket_ms: 1, cap: Some(cap.clone()) };
    validate_spec(&spec)?;
    check_cells(header, cells)?;
    apply_cap(&compile_cap(header, cap)?, cells, &mut CapScratch::default())
}

/// Every cell has the header's shape and the cells ascend strictly by member.
fn check_cells(header: &CellHeader, cells: &[Cell]) -> Result<(), String> {
    validate_cells_in_snapshot(cells.len())?;
    if let Some(bad) = cells
        .iter()
        .find(|cell| cell.labels.len() != header.labels.len() || cell.lanes.len() != header.lanes.len())
    {
        return Err(format!(
            "a cell has {} labels and {} lanes, the header declares {} and {}",
            bad.labels.len(),
            bad.lanes.len(),
            header.labels.len(),
            header.lanes.len()
        ));
    }
    if !is_strictly_ascending(cells) {
        return Err("a snapshot is not strictly ascending by member".to_string());
    }
    Ok(())
}

// ------------------------------------------------------------------------------------------
// The fold
// ------------------------------------------------------------------------------------------

/// Merges `newer` into `acc` (both ascending by member), `acc` holding the older contribution.
/// `tmp` is the reused output buffer: it ends up in `acc` and `acc`'s old buffer stays in `tmp`.
fn fold_into(
    header: &CellHeader,
    acc: &mut Vec<Cell>,
    newer: Vec<Cell>,
    tmp: &mut Vec<Cell>,
    combined: &mut u64,
) -> Result<(), String> {
    tmp.clear();
    tmp.reserve(acc.len() + newer.len());
    {
        let mut old = acc.drain(..).peekable();
        let mut new = newer.into_iter().peekable();
        loop {
            let order = match (old.peek(), new.peek()) {
                (None, None) => break,
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (Some(o), Some(n)) => o.member.cmp(&n.member),
            };
            match order {
                Ordering::Less => tmp.extend(old.next()),
                Ordering::Greater => tmp.extend(new.next()),
                Ordering::Equal => {
                    let (Some(mut o), Some(n)) = (old.next(), new.next()) else { break };
                    for ((lane, spec), &incoming) in o.lanes.iter_mut().zip(&header.lanes).zip(&n.lanes) {
                        *lane = combine_aggregated(spec.reducer, *lane, incoming);
                    }
                    o.labels = n.labels;
                    tmp.push(o);
                    *combined += 1;
                }
            }
        }
    }
    std::mem::swap(acc, tmp);
    within("cells in a rolled snapshot", acc.len(), MAX_CELLS_PER_SNAPSHOT)
}

/// Rolls the snapshots below the cutoff up into buckets and caps each rolled snapshot.
///
/// `snapshots` must ascend strictly by stamp and each must be a valid snapshot of `header`
/// (checked for the ones below the cutoff; the others are returned as given). Nothing is partly
/// done: an `Err` returns no snapshots.
pub(crate) fn rollup(header: &CellHeader, mut snapshots: Vec<Snapshot>, spec: &RollupSpec) -> Result<RollupResult, String> {
    validate_spec(spec)?;
    let cap = spec.cap.as_ref().map(|cap| compile_cap(header, cap)).transpose()?;
    if snapshots.windows(2).any(|pair| pair[0].timestamp_ms >= pair[1].timestamp_ms) {
        return Err("snapshots are not in strictly ascending stamp order".to_string());
    }
    let below = snapshots.partition_point(|snapshot| snapshot.timestamp_ms < spec.older_than_ms);
    within("snapshots below the cutoff", below, MAX_ROLLUP_SNAPSHOTS)?;

    let mut stats = RollupStats { snapshots_before: snapshots.len() as u64, ..RollupStats::default() };
    let rest = snapshots.split_off(below);
    let mut out: Vec<Snapshot> = Vec::with_capacity(below + rest.len());
    let mut scratch = CapScratch::default();
    let mut tmp: Vec<Cell> = Vec::new();
    let mut current: Option<Snapshot> = None;

    // A finished bucket: capped, then filed.
    let mut finish = |bucket: Snapshot, out: &mut Vec<Snapshot>, stats: &mut RollupStats| -> Result<(), String> {
        let mut bucket = bucket;
        if let Some(cap) = &cap {
            stats.cells_dropped += apply_cap(cap, &mut bucket.cells, &mut scratch)?;
        }
        out.push(bucket);
        Ok(())
    };

    for snapshot in snapshots {
        check_cells(header, &snapshot.cells)?;
        stats.cells_before += snapshot.cells.len() as u64;
        let bucket_start = snapshot.timestamp_ms - snapshot.timestamp_ms % spec.bucket_ms;
        match current.as_mut() {
            Some(open) if open.timestamp_ms == bucket_start => {
                fold_into(header, &mut open.cells, snapshot.cells, &mut tmp, &mut stats.cells_combined)?;
            }
            _ => {
                if let Some(done) = current.take() {
                    finish(done, &mut out, &mut stats)?;
                }
                current = Some(Snapshot { timestamp_ms: bucket_start, cells: snapshot.cells });
            }
        }
    }
    if let Some(done) = current.take() {
        finish(done, &mut out, &mut stats)?;
    }
    for snapshot in rest {
        stats.cells_before += snapshot.cells.len() as u64;
        out.push(snapshot);
    }
    stats.snapshots_after = out.len() as u64;
    stats.cells_after = out.iter().map(|snapshot| snapshot.cells.len() as u64).sum();
    Ok(RollupResult { snapshots: out, stats })
}

#[cfg(test)]
#[path = "cell_rollup_tests.rs"]
mod tests;
