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
//! # TIERS, THE ANCHOR, TRUNCATION AND CAP CONTROLS
//!
//! A request may carry more than the one `(older_than, bucket_ms)` pair ([`PlanSpec`]):
//!
//! * TIERS: an ordered list, finest first. A snapshot takes the bucket width of the COARSEST tier
//!   whose cutoff is above its stamp, so recent history stays fine and old history gets coarser
//!   in one request. Each coarser width is a multiple of the one before, which keeps the output
//!   ascending. A final tier of width [`CATCH_ALL_BUCKET_MS`] is the catch-all: everything older
//!   than its cutoff folds into ONE snapshot stored at stamp 0. A coarser tier's cutoff is aligned
//!   down to a multiple of the finer width, so a bucket never straddles a boundary and rolling the
//!   result again changes nothing.
//! * ANCHOR: [`Anchor::Request`] reads each `older_than` as a stamp; [`Anchor::NewestMinusAge`]
//!   reads it as an age and resolves it against the newest stamp the series holds (saturating at
//!   0). There is no clock: the newest snapshot is part of the stored state, so a follower and a
//!   replay resolve the same cutoffs.
//! * TRUNCATION: [`truncation_drop`] says how many of the oldest snapshots to remove from a series
//!   of a given length: none unless it holds more than `trigger` (default `keep_last`), then down
//!   to `keep_last`. It is separate from the fold because it removes snapshots the rollup never
//!   read.
//! * CAP CONTROLS: `protect_newest_ppm` reserves that share of `keep` (rounded down) for the
//!   group's most recently seen members, which survive whatever their score; a member is as recent
//!   as the newest snapshot that wrote it. `trigger_over` leaves a group alone until it holds more
//!   than `keep + trigger_over` members, then cuts it to `keep`.
//!
//! A request that names none of these is the single-tier request, byte for byte.
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
// The extension: tiers, an anchor, truncation and cap controls
// ------------------------------------------------------------------------------------------

/// Coarser tiers a request may add to the first.
pub(crate) const MAX_COARSER_TIERS: usize = 8;
/// Snapshots a truncation may keep.
pub(crate) const MAX_TRUNCATE_KEEP: usize = 1_000_000;
/// Snapshots a truncation trigger may name.
pub(crate) const MAX_TRUNCATE_TRIGGER: usize = 10_000_000;
/// Parts per million in a whole.
pub(crate) const PPM: u32 = 1_000_000;
/// Members a group may exceed its `keep` by before the cap acts.
pub(crate) const MAX_TRIGGER_OVER: usize = MAX_KEEP;
/// A tier this wide is the catch-all: one snapshot, stored at stamp 0, holds everything older than
/// the tier's cutoff. Only the last tier may be it.
pub(crate) const CATCH_ALL_BUCKET_MS: u64 = u64::MAX;

/// What a tier's `older_than` means.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum Anchor {
    /// An absolute stamp: snapshots stamped strictly below it are in the tier.
    #[default]
    Request,
    /// An age: the cutoff is the NEWEST stored snapshot's stamp minus the age (saturating at 0).
    /// The newest snapshot is part of the stored series, not of a clock, so replay agrees.
    NewestMinusAge,
}

/// Snapshots below the tier's cutoff are folded into buckets of `bucket_ms`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TierSpec {
    pub(crate) older_than: u64,
    pub(crate) bucket_ms: u64,
}

/// Keep the newest `keep_last` snapshots, acting only when the series holds more than `trigger`
/// (when named; otherwise more than `keep_last`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TruncateSpec {
    pub(crate) keep_last: usize,
    pub(crate) trigger: Option<usize>,
}

/// A cap with its controls.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CapPlanSpec {
    pub(crate) group_by: Option<String>,
    pub(crate) score: CellScore,
    pub(crate) keep: usize,
    /// Parts per million of `keep` (rounded down) reserved for the group's most recently seen
    /// members: they survive whatever their score. 0 reserves none.
    pub(crate) protect_newest_ppm: u32,
    /// A group is cut only when it holds more than `keep + trigger_over` members; then to `keep`.
    pub(crate) trigger_over: usize,
}

/// A rollup request in full. `tiers` is ordered finest first; tier 0 is the single pair of the
/// original request.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PlanSpec {
    pub(crate) anchor: Anchor,
    pub(crate) tiers: Vec<TierSpec>,
    pub(crate) truncate: Option<TruncateSpec>,
    pub(crate) cap: Option<CapPlanSpec>,
}

impl PlanSpec {
    /// The single-tier request, with every new control at its default.
    pub(crate) fn from_legacy(spec: &RollupSpec) -> PlanSpec {
        PlanSpec {
            anchor: Anchor::Request,
            tiers: vec![TierSpec { older_than: spec.older_than_ms, bucket_ms: spec.bucket_ms }],
            truncate: None,
            cap: spec.cap.as_ref().map(|cap| CapPlanSpec {
                group_by: cap.group_by.clone(),
                score: cap.score.clone(),
                keep: cap.keep,
                protect_newest_ppm: 0,
                trigger_over: 0,
            }),
        }
    }
}

/// A cap, borrowed.
#[derive(Clone, Copy)]
struct CapView<'a> {
    group_by: Option<&'a str>,
    score: &'a CellScore,
    keep: usize,
    protect_newest_ppm: u32,
    trigger_over: usize,
}

/// A request, borrowed. The single-tier request is viewed straight off its spec, so the work it
/// does allocates nothing for the extension.
#[derive(Clone, Copy)]
struct PlanView<'a> {
    anchor: Anchor,
    tiers: &'a [TierSpec],
    truncate: Option<TruncateSpec>,
    cap: Option<CapView<'a>>,
}

impl PlanSpec {
    fn view(&self) -> PlanView<'_> {
        PlanView {
            anchor: self.anchor,
            tiers: &self.tiers,
            truncate: self.truncate,
            cap: self.cap.as_ref().map(|cap| CapView {
                group_by: cap.group_by.as_deref(),
                score: &cap.score,
                keep: cap.keep,
                protect_newest_ppm: cap.protect_newest_ppm,
                trigger_over: cap.trigger_over,
            }),
        }
    }
}

/// Runs `with` over the single-tier request viewed as a full one.
fn with_legacy_view<R>(spec: &RollupSpec, with: impl FnOnce(&PlanView<'_>) -> R) -> R {
    let tier = [TierSpec { older_than: spec.older_than_ms, bucket_ms: spec.bucket_ms }];
    let view = PlanView {
        anchor: Anchor::Request,
        tiers: &tier,
        truncate: None,
        cap: spec.cap.as_ref().map(|cap| CapView {
            group_by: cap.group_by.as_deref(),
            score: &cap.score,
            keep: cap.keep,
            protect_newest_ppm: 0,
            trigger_over: 0,
        }),
    };
    with(&view)
}

// ------------------------------------------------------------------------------------------
// Admission and compiling
// ------------------------------------------------------------------------------------------

/// The checks that need no header and no snapshot: a command runs this at admission.
pub(crate) fn validate_spec(spec: &RollupSpec) -> Result<(), String> {
    with_legacy_view(spec, validate_view)
}

/// [`validate_spec`] for a full request. Everything that needs only the request is checked here;
/// what needs the newest stamp (the resolved cutoffs) is not an error, a tier may cover nothing.
pub(crate) fn validate_plan(spec: &PlanSpec) -> Result<(), String> {
    validate_view(&spec.view())
}

fn validate_view(spec: &PlanView<'_>) -> Result<(), String> {
    if spec.tiers.is_empty() {
        return Err("a rollup needs at least one tier".to_string());
    }
    within("coarser tiers", spec.tiers.len() - 1, MAX_COARSER_TIERS)?;
    for (i, tier) in spec.tiers.iter().enumerate() {
        if tier.bucket_ms == 0 {
            return Err(if i == 0 {
                "bucket_ms must be at least 1".to_string()
            } else {
                format!("tier {i} bucket_ms must be at least 1")
            });
        }
    }
    for (i, pair) in spec.tiers.windows(2).enumerate() {
        let (finer, coarser) = (pair[0], pair[1]);
        let ordered = match spec.anchor {
            Anchor::Request => coarser.older_than < finer.older_than,
            Anchor::NewestMinusAge => coarser.older_than > finer.older_than,
        };
        if !ordered {
            return Err(format!(
                "tier {} older_than {} must reach further back than tier {i} older_than {} \
                 (cutoffs fall, ages grow, down the list)",
                i + 1,
                coarser.older_than,
                finer.older_than
            ));
        }
        if coarser.bucket_ms == CATCH_ALL_BUCKET_MS {
            if i + 2 != spec.tiers.len() {
                return Err("only the last tier may be the catch-all".to_string());
            }
        } else if coarser.bucket_ms % finer.bucket_ms != 0 {
            return Err(format!(
                "tier {} bucket_ms {} is not a multiple of tier {i} bucket_ms {}",
                i + 1,
                coarser.bucket_ms,
                finer.bucket_ms
            ));
        }
    }
    if let Some(truncate) = &spec.truncate {
        within_nonzero("truncate keep_last", truncate.keep_last, MAX_TRUNCATE_KEEP)?;
        if let Some(trigger) = truncate.trigger {
            within("truncate trigger", trigger, MAX_TRUNCATE_TRIGGER)?;
            if trigger < truncate.keep_last {
                return Err(format!(
                    "truncate trigger {trigger} is below keep_last {}: it would never leave a series it acts on",
                    truncate.keep_last
                ));
            }
        }
    }
    if let Some(cap) = &spec.cap {
        within_nonzero("cap keep", cap.keep, MAX_KEEP)?;
        validate_score(cap.score)?;
        if matches!(cap.score, CellScore::Time) {
            return Err("a cap cannot score by time: every cell of a snapshot shares its stamp".to_string());
        }
        if matches!(cap.score, CellScore::Decay { .. }) {
            return Err("a cap cannot score by decay: it scores one folded cell, and a decay reads a member's snapshots".to_string());
        }
        if cap.protect_newest_ppm > PPM {
            return Err(format!("protect_newest_ppm {} is over the limit of {PPM}", cap.protect_newest_ppm));
        }
        within("cap trigger_over", cap.trigger_over, MAX_TRIGGER_OVER)?;
    }
    Ok(())
}

/// The resolved tiers, in a fixed array: a request has at most `1 + MAX_COARSER_TIERS`.
struct Resolved {
    cutoffs: [(u64, u64); 1 + MAX_COARSER_TIERS],
    len: usize,
}

impl Resolved {
    fn as_slice(&self) -> &[(u64, u64)] {
        &self.cutoffs[..self.len]
    }
}

/// Each tier as `(cutoff, bucket_ms)`, finest first, the cutoff an absolute stamp. `newest_ms` is
/// the newest stamp the series holds; it is read only under [`Anchor::NewestMinusAge`]. The spec
/// must have passed validation (at most `1 + MAX_COARSER_TIERS` tiers).
///
/// A coarser tier's cutoff is ALIGNED DOWN to a multiple of the finer tier's bucket width. A bucket
/// of the finer tier then starts at or after the boundary, so a rolled snapshot is always in the
/// tier that rolled it and rolling again with the same arguments changes nothing. The first tier's
/// cutoff is as given.
fn resolve(spec: &PlanView<'_>, newest_ms: u64) -> Resolved {
    let mut resolved = Resolved { cutoffs: [(0, 0); 1 + MAX_COARSER_TIERS], len: 0 };
    for tier in spec.tiers {
        let mut cutoff = match spec.anchor {
            Anchor::Request => tier.older_than,
            Anchor::NewestMinusAge => newest_ms.saturating_sub(tier.older_than),
        };
        if resolved.len > 0 {
            cutoff -= cutoff % resolved.cutoffs[resolved.len - 1].1;
        }
        resolved.cutoffs[resolved.len] = (cutoff, tier.bucket_ms);
        resolved.len += 1;
    }
    resolved
}

/// [`resolve`] as a list, for a request that has passed [`validate_plan`].
pub(crate) fn resolve_tiers(spec: &PlanSpec, newest_ms: u64) -> Vec<(u64, u64)> {
    resolve(&spec.view(), newest_ms).as_slice().to_vec()
}

/// The bucket width of the snapshot stamped `ts`: that of the coarsest tier whose cutoff is above
/// it. `None` when it is at or past every cutoff (not rolled).
fn bucket_width(tiers: &[(u64, u64)], ts: u64) -> Option<u64> {
    tiers.iter().rev().find(|(cutoff, _)| ts < *cutoff).map(|(_, width)| *width)
}

/// How many of the oldest snapshots a truncation removes from a series holding `total`.
pub(crate) fn truncation_drop(total: usize, truncate: &TruncateSpec) -> usize {
    if total > truncate.trigger.unwrap_or(truncate.keep_last) {
        total - truncate.keep_last
    } else {
        0
    }
}

/// A cap checked against a header.
struct Cap {
    group: Option<usize>,
    scorer: Scorer,
    keep: usize,
    protect: usize,
    trigger_over: usize,
}

fn compile_cap(header: &CellHeader, cap: &CapView<'_>) -> Result<Cap, String> {
    let group = match cap.group_by {
        None => None,
        Some(name) => Some(header.labels.iter().position(|label| label == name).ok_or_else(|| {
            format!("cap group `{name}` is not a label this key declares (labels {:?})", header.labels)
        })?),
    };
    Ok(Cap {
        group,
        scorer: Scorer::compile(header, cap.score)?,
        keep: cap.keep,
        protect: (cap.keep as u128 * u128::from(cap.protect_newest_ppm) / u128::from(PPM)) as usize,
        trigger_over: cap.trigger_over,
    })
}

/// The buffers a cap reuses from one snapshot to the next.
#[derive(Default)]
struct CapScratch {
    order: Vec<(u64, Option<Key>, usize)>,
    keep: Vec<bool>,
    recent: Vec<(u64, usize)>,
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

/// Removes from `cells` (strictly ascending by member) the members a cap does not keep, group by
/// group. A group of at most `keep + trigger_over` is left alone; a larger one keeps its `protect`
/// most recently seen members (`seen[i]` is the newest stamp that contributed cell `i`; ties
/// to the smaller member), then the best of the rest by score until it holds `keep`. `seen` is
/// empty exactly when nothing is protected. Returns how many cells were removed.
fn apply_cap(cap: &Cap, cells: &mut Vec<Cell>, seen: &[u64], scratch: &mut CapScratch) -> Result<u64, String> {
    let slack = cap.keep + cap.trigger_over;
    if cap.group.is_none() && cells.len() <= slack {
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
    let mut from = 0;
    while from < scratch.order.len() {
        let group = scratch.order[from].0;
        let mut to = from;
        while to < scratch.order.len() && scratch.order[to].0 == group {
            to += 1;
        }
        if to - from <= slack {
            for &(_, _, i) in &scratch.order[from..to] {
                scratch.keep[i] = true;
            }
        } else {
            let mut room = cap.keep;
            if cap.protect > 0 {
                scratch.recent.clear();
                scratch.recent.extend(scratch.order[from..to].iter().map(|&(_, _, i)| (seen[i], i)));
                scratch.recent.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
                for &(_, i) in scratch.recent.iter().take(cap.protect) {
                    scratch.keep[i] = true;
                    room -= 1;
                }
            }
            for &(_, _, i) in &scratch.order[from..to] {
                if room == 0 {
                    break;
                }
                if !scratch.keep[i] {
                    scratch.keep[i] = true;
                    room -= 1;
                }
            }
        }
        from = to;
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
    with_legacy_view(&spec, |view| {
        let cap = view.cap.as_ref().expect("the spec carries the cap");
        apply_cap(&compile_cap(header, cap)?, cells, &[], &mut CapScratch::default())
    })
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

/// The two buffers that follow the cells of the open bucket when members are protected: the
/// newest stamp that contributed each cell.
#[derive(Default)]
struct Seen {
    track: bool,
    acc: Vec<u64>,
    tmp: Vec<u64>,
}

/// Merges `newer` (stamped `newer_ts`) into `acc` (both ascending by member), `acc` holding the
/// older contribution. `tmp` is the reused output buffer: it ends up in `acc` and `acc`'s old
/// buffer stays in `tmp`.
fn fold_into(
    header: &CellHeader,
    acc: &mut Vec<Cell>,
    newer: Vec<Cell>,
    newer_ts: u64,
    tmp: &mut Vec<Cell>,
    seen: &mut Seen,
    combined: &mut u64,
) -> Result<(), String> {
    tmp.clear();
    tmp.reserve(acc.len() + newer.len());
    if seen.track {
        seen.tmp.clear();
        seen.tmp.reserve(acc.len() + newer.len());
    }
    {
        let mut at = 0usize;
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
                Ordering::Less => {
                    tmp.extend(old.next());
                    if seen.track {
                        seen.tmp.push(seen.acc[at]);
                    }
                    at += 1;
                }
                Ordering::Greater => {
                    tmp.extend(new.next());
                    if seen.track {
                        seen.tmp.push(newer_ts);
                    }
                }
                Ordering::Equal => {
                    let (Some(mut o), Some(n)) = (old.next(), new.next()) else { break };
                    for ((lane, spec), &incoming) in o.lanes.iter_mut().zip(&header.lanes).zip(&n.lanes) {
                        *lane = combine_aggregated(spec.reducer, *lane, incoming);
                    }
                    o.labels = n.labels;
                    tmp.push(o);
                    if seen.track {
                        seen.tmp.push(newer_ts);
                    }
                    at += 1;
                    *combined += 1;
                }
            }
        }
    }
    std::mem::swap(acc, tmp);
    if seen.track {
        std::mem::swap(&mut seen.acc, &mut seen.tmp);
    }
    within("cells in a rolled snapshot", acc.len(), MAX_CELLS_PER_SNAPSHOT)
}

/// A finished bucket: capped, then filed.
fn finish(
    cap: &Option<Cap>,
    scratch: &mut CapScratch,
    mut bucket: Snapshot,
    seen: &Seen,
    out: &mut Vec<Snapshot>,
    stats: &mut RollupStats,
) -> Result<(), String> {
    if let Some(cap) = cap {
        stats.cells_dropped += apply_cap(cap, &mut bucket.cells, &seen.acc, scratch)?;
    }
    out.push(bucket);
    Ok(())
}

/// Rolls the snapshots below the cutoff up into buckets and caps each rolled snapshot: the
/// single-tier request, as it has always been.
///
/// `snapshots` must ascend strictly by stamp and each must be a valid snapshot of `header`
/// (checked for the ones below the cutoff; the others are returned as given). Nothing is partly
/// done: an `Err` returns no snapshots.
pub(crate) fn rollup(header: &CellHeader, snapshots: Vec<Snapshot>, spec: &RollupSpec) -> Result<RollupResult, String> {
    with_legacy_view(spec, |view| rollup_view(header, snapshots, view, 0))
}

/// [`rollup`] for a full request. `newest_ms` is the newest stamp of the whole series (not only of
/// `snapshots`); it matters only under [`Anchor::NewestMinusAge`]. Truncation is not applied here:
/// it removes snapshots the rollup never read, so the caller composes [`truncation_drop`] over the
/// series it holds.
pub(crate) fn rollup_tiered(
    header: &CellHeader,
    snapshots: Vec<Snapshot>,
    spec: &PlanSpec,
    newest_ms: u64,
) -> Result<RollupResult, String> {
    rollup_view(header, snapshots, &spec.view(), newest_ms)
}

fn rollup_view(
    header: &CellHeader,
    mut snapshots: Vec<Snapshot>,
    spec: &PlanView<'_>,
    newest_ms: u64,
) -> Result<RollupResult, String> {
    validate_view(spec)?;
    let cap = spec.cap.as_ref().map(|cap| compile_cap(header, cap)).transpose()?;
    if snapshots.windows(2).any(|pair| pair[0].timestamp_ms >= pair[1].timestamp_ms) {
        return Err("snapshots are not in strictly ascending stamp order".to_string());
    }
    let resolved = resolve(spec, newest_ms);
    let tiers = resolved.as_slice();
    let below = snapshots.partition_point(|snapshot| snapshot.timestamp_ms < tiers[0].0);
    within("snapshots below the cutoff", below, MAX_ROLLUP_SNAPSHOTS)?;

    let mut stats = RollupStats { snapshots_before: snapshots.len() as u64, ..RollupStats::default() };
    let rest = snapshots.split_off(below);
    let mut out: Vec<Snapshot> = Vec::with_capacity(below + rest.len());
    let mut scratch = CapScratch::default();
    let mut tmp: Vec<Cell> = Vec::new();
    let mut seen = Seen { track: cap.as_ref().is_some_and(|cap| cap.protect > 0), ..Seen::default() };
    let mut current: Option<Snapshot> = None;

    for snapshot in snapshots {
        check_cells(header, &snapshot.cells)?;
        stats.cells_before += snapshot.cells.len() as u64;
        let width = bucket_width(tiers, snapshot.timestamp_ms).expect("a snapshot below the first cutoff has a tier");
        let bucket_start = snapshot.timestamp_ms - snapshot.timestamp_ms % width;
        match current.as_mut() {
            Some(open) if open.timestamp_ms == bucket_start => {
                fold_into(
                    header,
                    &mut open.cells,
                    snapshot.cells,
                    snapshot.timestamp_ms,
                    &mut tmp,
                    &mut seen,
                    &mut stats.cells_combined,
                )?;
            }
            _ => {
                if let Some(done) = current.take() {
                    finish(&cap, &mut scratch, done, &seen, &mut out, &mut stats)?;
                }
                if seen.track {
                    seen.acc.clear();
                    seen.acc.resize(snapshot.cells.len(), snapshot.timestamp_ms);
                }
                current = Some(Snapshot { timestamp_ms: bucket_start, cells: snapshot.cells });
            }
        }
    }
    if let Some(done) = current.take() {
        finish(&cap, &mut scratch, done, &seen, &mut out, &mut stats)?;
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

#[cfg(test)]
#[path = "cell_rollup_tier_tests.rs"]
mod tier_tests;
