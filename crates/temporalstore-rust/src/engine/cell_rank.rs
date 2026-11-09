// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The ranked windowed read over merged cell snapshots, as a pure function of decoded snapshots.
//!
//! ```text
//! window -> select (named predicates) -> reduce per member -> score -> top-k -> project
//! ```
//!
//! # WHAT EACH STAGE MEANS
//!
//! * WINDOW picks snapshots: [`CellWindow`]. A window that would hold more than
//!   [`MAX_WINDOW_SNAPSHOTS`] snapshots is refused, not truncated.
//! * SELECT tests every cell of every chosen snapshot against the named predicates, which reuse
//!   the six `FeatureFilter` operators. A name is a label, a lane or `member`; any other name is
//!   refused when the plan is compiled, before a snapshot is read. A cell is tested as the snapshot
//!   stores it, so the predicates cut BEFORE the member's snapshots are reduced together.
//! * REDUCE folds one member's surviving cells lane by lane with the reduction the header declares
//!   for that lane ([`read_reducer`]: the write-side merge verb, as the read-side window reduction) through the shared window
//!   reducers, in snapshot order. Labels are the newest surviving cell's.
//! * SCORE turns the reduced lanes into the ordering value: [`CellScore`]. A member whose score is
//!   undefined (a zero denominator, no trials) is counted in `unscored` and not ranked.
//! * TOP-K keeps the `k` best in a bounded heap. Equal scores order by member, smaller first
//!   (`u64` members before bytes members), so the answer is a function of the data alone.
//! * PROJECT returns the named labels and lanes, or all of them.
//!
//! # COST
//!
//! Snapshots are stored sorted by member, so the members of all chosen snapshots are visited in
//! order by a k-way merge: a member's cells meet each other without any per-member table. Work is
//! `O(cells * log snapshots)` for the merge plus `O(snapshots * lanes)` per member for the
//! reduction, and the heap is `O(k)`. THE ALLOCATIONS DO NOT DEPEND ON THE NUMBER OF CELLS OR
//! MEMBERS: the cursors, the merge heap, the per-member scratch and the result slots are sized by
//! the snapshot count, the lane count and `k`, once, before the first cell is read. A snapshot
//! whose cells are not strictly ascending by member is refused (it would be merged wrongly).
//!
//! The module is pure: it reads decoded [`Cell`]s and a clock value and touches no engine state.

#![allow(dead_code)]

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use super::cell_set::{validate_top_k, within, Cell, CellHeader, LaneReducer, Member};
use super::product_model::filter_op_holds;
use super::window_aggregate::{ratio, weighted_sum, wilson_lower_bound, Reduced, Reducer};
use crate::types::{
    CellMemberId, CellRankSpec, CellScore, CellWindow, FeatureFilter, FeatureFilterOp, RankedCellRow,
    RankedCells,
};

/// Snapshots one ranked read may merge.
pub(crate) const MAX_WINDOW_SNAPSHOTS: usize = 512;
/// Predicates one ranked read may carry.
pub(crate) const MAX_PREDICATES: usize = 8;
/// Lanes in one weighted score.
pub(crate) const MAX_WEIGHTED_LANES: usize = 8;

/// One decoded snapshot: its stamp and its cells, sorted strictly ascending by member.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SnapshotView<'a> {
    pub(crate) timestamp_ms: u64,
    pub(crate) cells: &'a [Cell],
}

// ------------------------------------------------------------------------------------------
// Compiling a query against a header
// ------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Target {
    Member,
    Label(usize),
    Lane(usize),
}

struct Predicate {
    target: Target,
    op: FeatureFilterOp,
    value: u64,
}

impl Predicate {
    fn holds(&self, cell: &Cell) -> bool {
        // Compared as i128 so a `u64` label or value and an `i64` lane meet without a wrap.
        let lhs = match self.target {
            Target::Member => match &cell.member {
                Member::U64(v) => i128::from(*v),
                // A bytes member has no number to compare.
                Member::Bytes(_) => return false,
            },
            Target::Label(i) => i128::from(cell.labels[i]),
            Target::Lane(i) => i128::from(cell.lanes[i]),
        };
        filter_op_holds(&self.op, lhs, i128::from(self.value))
    }
}

enum Scorer {
    Lane(usize),
    Ratio(usize, usize),
    Wilson(usize, usize, f64),
    Member,
    Time,
    Weighted(Vec<usize>, Vec<f64>),
}

/// A query checked against a header: every name resolved to a position, every bound met.
pub(crate) struct Plan {
    window: CellWindow,
    predicates: Vec<Predicate>,
    scorer: Scorer,
    top_k: usize,
    lane_count: usize,
    label_count: usize,
    reducers: Vec<Reducer>,
    labels_out: Vec<usize>,
    lanes_out: Vec<usize>,
    label_names: Vec<String>,
    lane_names: Vec<String>,
}

/// The window reduction that combines a lane's STORED values across snapshots. The lane's verb
/// merged writes into one snapshot; across snapshots a counted lane holds counts, and counts add.
fn read_reducer(lane: LaneReducer) -> Reducer {
    match lane {
        LaneReducer::Sum | LaneReducer::Count => Reducer::Sum,
        LaneReducer::Max => Reducer::Max,
        LaneReducer::Min => Reducer::Min,
        LaneReducer::First => Reducer::First,
        LaneReducer::Last => Reducer::Last,
    }
}

fn resolve(header: &CellHeader, name: &str) -> Result<Target, String> {
    let label = header.labels.iter().position(|label| label == name);
    let lane = header.lanes.iter().position(|lane| lane.name == name);
    match (name == "member", label, lane) {
        (true, None, None) => Ok(Target::Member),
        (false, Some(i), None) => Ok(Target::Label(i)),
        (false, None, Some(i)) => Ok(Target::Lane(i)),
        (false, None, None) => Err(format!(
            "unknown name `{name}`: this key declares labels {:?}, lanes {:?} and `member`",
            header.labels,
            header.lanes.iter().map(|lane| lane.name.as_str()).collect::<Vec<_>>()
        )),
        _ => Err(format!("name `{name}` is ambiguous between a label, a lane and `member`")),
    }
}

fn lane_named(header: &CellHeader, name: &str) -> Result<usize, String> {
    match resolve(header, name)? {
        Target::Lane(i) => Ok(i),
        _ => Err(format!("`{name}` is not a lane, and a score reads lanes")),
    }
}

impl Plan {
    /// Checks `spec` against `header`. Every refusal names what was wrong; nothing is read.
    pub(crate) fn compile(header: &CellHeader, spec: &CellRankSpec) -> Result<Plan, String> {
        validate_top_k(spec.top_k)?;
        within("predicates per query", spec.filters.len(), MAX_PREDICATES)?;
        match &spec.window {
            CellWindow::Between { start_ms, end_ms } if start_ms > end_ms => {
                return Err(format!("window starts at {start_ms}, after it ends at {end_ms}"))
            }
            CellWindow::LastN { n } => {
                if *n == 0 {
                    return Err("window of the last 0 snapshots is empty".to_string());
                }
                within("snapshots in the window", *n, MAX_WINDOW_SNAPSHOTS)?;
            }
            _ => {}
        }

        let mut predicates = Vec::with_capacity(spec.filters.len());
        for FeatureFilter { field, op, value } in &spec.filters {
            predicates.push(Predicate { target: resolve(header, field)?, op: op.clone(), value: *value });
        }

        let scorer = match &spec.score {
            CellScore::Lane { lane } => Scorer::Lane(lane_named(header, lane)?),
            CellScore::Ratio { numerator, denominator } => {
                Scorer::Ratio(lane_named(header, numerator)?, lane_named(header, denominator)?)
            }
            CellScore::Wilson { successes, trials, z } => {
                if !z.is_finite() || *z < 0.0 {
                    return Err(format!("wilson z must be finite and not negative, got {z}"));
                }
                Scorer::Wilson(lane_named(header, successes)?, lane_named(header, trials)?, *z)
            }
            CellScore::Member => Scorer::Member,
            CellScore::Time => Scorer::Time,
            CellScore::Weighted { lanes, weights } => {
                if lanes.is_empty() {
                    return Err("a weighted score needs at least one lane".to_string());
                }
                within("lanes in a weighted score", lanes.len(), MAX_WEIGHTED_LANES)?;
                if lanes.len() != weights.len() {
                    return Err(format!(
                        "a weighted score has {} lanes and {} weights",
                        lanes.len(),
                        weights.len()
                    ));
                }
                if let Some(bad) = weights.iter().find(|w| !w.is_finite()) {
                    return Err(format!("weight {bad} is not finite"));
                }
                let indexes = lanes.iter().map(|name| lane_named(header, name)).collect::<Result<Vec<_>, _>>()?;
                Scorer::Weighted(indexes, weights.clone())
            }
        };

        let (mut labels_out, mut lanes_out) = (Vec::new(), Vec::new());
        if spec.project.is_empty() {
            labels_out.extend(0..header.labels.len());
            lanes_out.extend(0..header.lanes.len());
        } else {
            for (at, name) in spec.project.iter().enumerate() {
                if spec.project[..at].contains(name) {
                    return Err(format!("`{name}` is projected twice"));
                }
                match resolve(header, name)? {
                    Target::Label(i) => labels_out.push(i),
                    Target::Lane(i) => lanes_out.push(i),
                    Target::Member => return Err("`member` is returned with every row".to_string()),
                }
            }
        }

        Ok(Plan {
            window: spec.window.clone(),
            predicates,
            scorer,
            top_k: spec.top_k,
            lane_count: header.lanes.len(),
            label_count: header.labels.len(),
            reducers: header.lanes.iter().map(|lane| read_reducer(lane.reducer)).collect(),
            label_names: labels_out.iter().map(|&i| header.labels[i].clone()).collect(),
            lane_names: lanes_out.iter().map(|&i| header.lanes[i].name.clone()).collect(),
            labels_out,
            lanes_out,
        })
    }

    /// The window this plan reads, for a caller that fetches snapshots itself.
    pub(crate) fn window(&self) -> &CellWindow {
        &self.window
    }
}

// ------------------------------------------------------------------------------------------
// Scores
// ------------------------------------------------------------------------------------------

/// An ordering value. Within one query every key is the same variant.
#[derive(Clone, Copy, Debug)]
enum Key {
    Int(i128),
    Float(f64),
}

impl Key {
    fn cmp(&self, other: &Key) -> Ordering {
        match (self, other) {
            (Key::Int(a), Key::Int(b)) => a.cmp(b),
            (Key::Float(a), Key::Float(b)) => a.total_cmp(b),
            (Key::Int(_), Key::Float(_)) => Ordering::Less,
            (Key::Float(_), Key::Int(_)) => Ordering::Greater,
        }
    }

    fn as_f64(self) -> f64 {
        match self {
            Key::Int(v) => v as f64,
            Key::Float(v) => v,
        }
    }
}

impl Plan {
    /// `None`: the score is undefined for this member (counted, not ranked).
    fn score(&self, member: &Member, last_ms: u64, lanes: &[i64]) -> Result<Option<Key>, String> {
        Ok(match &self.scorer {
            Scorer::Lane(i) => Some(Key::Int(i128::from(lanes[*i]))),
            Scorer::Ratio(n, d) => ratio(lanes[*n], lanes[*d]).map(Key::Float),
            Scorer::Wilson(s, t, z) => {
                let (successes, trials) = (lanes[*s], lanes[*t]);
                if successes < 0 || trials < 0 {
                    None
                } else {
                    wilson_lower_bound(successes as u64, trials as u64, *z).map(Key::Float)
                }
            }
            Scorer::Member => match member {
                Member::U64(v) => Some(Key::Int(i128::from(*v))),
                Member::Bytes(_) => {
                    return Err("the `member` score needs u64 members, and a bytes member was selected".to_string())
                }
            },
            Scorer::Time => Some(Key::Int(i128::from(last_ms))),
            Scorer::Weighted(indexes, weights) => {
                let mut picked = [0i64; MAX_WEIGHTED_LANES];
                for (slot, &lane) in picked.iter_mut().zip(indexes) {
                    *slot = lanes[lane];
                }
                weighted_sum(&picked[..indexes.len()], weights).map(Key::Float)
            }
        })
    }
}

// ------------------------------------------------------------------------------------------
// The bounded top-k
// ------------------------------------------------------------------------------------------

/// A kept member. The heap's greatest entry is the WORST one kept, so a better candidate replaces
/// it in place.
struct Entry<'a> {
    key: Key,
    member: &'a Member,
    slot: usize,
}

impl Ord for Entry<'_> {
    /// Greater means worse: lower key, then larger member.
    fn cmp(&self, other: &Self) -> Ordering {
        other.key.cmp(&self.key).then_with(|| self.member.cmp(other.member))
    }
}
impl PartialOrd for Entry<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for Entry<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Entry<'_> {}

/// The kept entries and the flat slots their values live in. All of it is allocated for `k`
/// entries up front; keeping a better member overwrites the worst one's slot.
struct TopK<'a> {
    k: usize,
    heap: BinaryHeap<Entry<'a>>,
    lanes: Vec<i64>,
    labels: Vec<u64>,
    last: Vec<u64>,
    lane_count: usize,
    label_count: usize,
}

impl<'a> TopK<'a> {
    fn new(k: usize, lane_count: usize, label_count: usize) -> Self {
        TopK {
            k,
            heap: BinaryHeap::with_capacity(k),
            lanes: vec![0; k * lane_count],
            labels: vec![0; k * label_count],
            last: vec![0; k],
            lane_count,
            label_count,
        }
    }

    fn write(&mut self, slot: usize, last_ms: u64, labels: &[u64], lanes: &[i64]) {
        self.last[slot] = last_ms;
        self.labels[slot * self.label_count..(slot + 1) * self.label_count].copy_from_slice(labels);
        self.lanes[slot * self.lane_count..(slot + 1) * self.lane_count].copy_from_slice(lanes);
    }

    fn offer(&mut self, key: Key, member: &'a Member, last_ms: u64, labels: &[u64], lanes: &[i64]) {
        if self.heap.len() < self.k {
            let slot = self.heap.len();
            self.write(slot, last_ms, labels, lanes);
            self.heap.push(Entry { key, member, slot });
            return;
        }
        let candidate = Entry { key, member, slot: 0 };
        let Some(mut worst) = self.heap.peek_mut() else { return };
        if candidate.cmp(&worst) == Ordering::Less {
            let slot = worst.slot;
            *worst = Entry { key, member, slot };
            drop(worst);
            self.write(slot, last_ms, labels, lanes);
        }
    }
}

fn member_id(member: &Member) -> CellMemberId {
    match member {
        Member::U64(v) => CellMemberId::U64(*v),
        Member::Bytes(b) => CellMemberId::Bytes(b.clone()),
    }
}

// ------------------------------------------------------------------------------------------
// Running a plan
// ------------------------------------------------------------------------------------------

fn snapshots_between<'s, 'a>(
    snapshots: &'s [SnapshotView<'a>],
    from_ms: u64,
    to_ms: u64,
) -> &'s [SnapshotView<'a>] {
    let first = snapshots.partition_point(|s| s.timestamp_ms < from_ms);
    let end = snapshots.partition_point(|s| s.timestamp_ms <= to_ms);
    &snapshots[first..end.max(first)]
}

impl Plan {
    /// The snapshots the window covers, in ascending stamp order.
    fn select_window<'s, 'a>(
        &self,
        now_ms: u64,
        snapshots: &'s [SnapshotView<'a>],
    ) -> Result<&'s [SnapshotView<'a>], String> {
        if snapshots.windows(2).any(|pair| pair[0].timestamp_ms >= pair[1].timestamp_ms) {
            return Err("snapshots are not in strictly ascending stamp order".to_string());
        }
        let chosen = match &self.window {
            CellWindow::Between { start_ms, end_ms } => snapshots_between(snapshots, *start_ms, *end_ms),
            CellWindow::Last { duration_ms } => snapshots_between(snapshots, now_ms.saturating_sub(*duration_ms), now_ms),
            CellWindow::Since { start_ms } => snapshots_between(snapshots, *start_ms, u64::MAX),
            CellWindow::LastN { n } => &snapshots[snapshots.len().saturating_sub(*n)..],
        };
        within("snapshots in the window", chosen.len(), MAX_WINDOW_SNAPSHOTS)?;
        Ok(chosen)
    }

    /// The first cell at or after `*at` that passes every predicate, as its member. Each cell is
    /// visited once, so this is also where the shape and the order of a snapshot are checked.
    fn settle<'a>(&self, cells: &'a [Cell], at: &mut usize) -> Result<Option<&'a Member>, String> {
        while *at < cells.len() {
            let cell = &cells[*at];
            if cell.lanes.len() != self.lane_count || cell.labels.len() != self.label_count {
                return Err(format!(
                    "a cell has {} labels and {} lanes, the header declares {} and {}",
                    cell.labels.len(),
                    cell.lanes.len(),
                    self.label_count,
                    self.lane_count
                ));
            }
            if *at > 0 && cells[*at - 1].member >= cell.member {
                return Err("a snapshot is not strictly ascending by member".to_string());
            }
            if self.predicates.iter().all(|predicate| predicate.holds(cell)) {
                return Ok(Some(&cell.member));
            }
            *at += 1;
        }
        Ok(None)
    }

    /// Moves cursor `i` past its current cell, recording that cell as an occurrence of the
    /// member being gathered, and re-queues the cursor at its next surviving cell.
    fn consume<'a>(
        &self,
        chosen: &[SnapshotView<'a>],
        at: &mut [usize],
        queue: &mut BinaryHeap<Reverse<(&'a Member, usize)>>,
        occurrences: &mut Vec<(u64, &'a Cell)>,
        i: usize,
    ) -> Result<(), String> {
        let cells: &'a [Cell] = chosen[i].cells;
        occurrences.push((chosen[i].timestamp_ms, &cells[at[i]]));
        at[i] += 1;
        if let Some(next) = self.settle(cells, &mut at[i])? {
            queue.push(Reverse((next, i)));
        }
        Ok(())
    }

    /// Runs the plan over `snapshots` (ascending by stamp; the window is applied here). `now_ms`
    /// is the instant a `Last` window ends at.
    pub(crate) fn run<'a>(&self, now_ms: u64, snapshots: &[SnapshotView<'a>]) -> Result<RankedCells, String> {
        let chosen = self.select_window(now_ms, snapshots)?;

        // Everything sized by the snapshot count, the lane count or k is allocated here, once.
        let mut at = vec![0usize; chosen.len()];
        let mut queue: BinaryHeap<Reverse<(&'a Member, usize)>> = BinaryHeap::with_capacity(chosen.len());
        let mut occurrences: Vec<(u64, &'a Cell)> = Vec::with_capacity(chosen.len());
        let mut reduced = vec![0i64; self.lane_count];
        let mut top = TopK::new(self.top_k, self.lane_count, self.label_count);
        let mut unscored = 0u64;

        for (i, snapshot) in chosen.iter().enumerate() {
            if let Some(member) = self.settle(snapshot.cells, &mut at[i])? {
                queue.push(Reverse((member, i)));
            }
        }

        while let Some(Reverse((member, first))) = queue.pop() {
            occurrences.clear();
            self.consume(chosen, &mut at, &mut queue, &mut occurrences, first)?;
            while queue.peek().map_or(false, |Reverse((next, _))| *next == member) {
                let Some(Reverse((_, i))) = queue.pop() else { break };
                self.consume(chosen, &mut at, &mut queue, &mut occurrences, i)?;
            }

            // Occurrences arrive in ascending snapshot order: the queue breaks ties by index.
            for (lane, reducer) in self.reducers.iter().enumerate() {
                let points = occurrences.iter().map(|(ts, cell)| (*ts, cell.lanes[lane]));
                match reducer.reduce(points).map_err(|e| e.0)? {
                    Some(Reduced::Int(v)) => reduced[lane] = v,
                    other => return Err(format!("lane {lane} did not reduce to an integer: {other:?}")),
                }
            }
            let &(last_ms, newest) = occurrences.last().ok_or("a member has no occurrence")?;
            match self.score(member, last_ms, &reduced)? {
                Some(key) => top.offer(key, member, last_ms, &newest.labels, &reduced),
                None => unscored += 1,
            }
        }

        let TopK { heap, lanes, labels, last, lane_count, label_count, .. } = top;
        let rows = heap
            .into_sorted_vec()
            .into_iter()
            .map(|entry| {
                let (lane_base, label_base) = (entry.slot * lane_count, entry.slot * label_count);
                RankedCellRow {
                    member: member_id(entry.member),
                    score: entry.key.as_f64(),
                    last_ms: last[entry.slot],
                    labels: self.labels_out.iter().map(|&i| labels[label_base + i]).collect(),
                    lanes: self.lanes_out.iter().map(|&i| lanes[lane_base + i]).collect(),
                }
            })
            .collect();
        Ok(RankedCells {
            label_columns: self.label_names.clone(),
            lane_columns: self.lane_names.clone(),
            snapshots: chosen.len(),
            unscored,
            rows,
        })
    }
}

#[cfg(test)]
#[path = "cell_rank_tests.rs"]
mod tests;
