// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The ranked windowed read over merged cell samples, as a pure function of decoded samples.
//!
//! ```text
//! window -> select (named predicates) -> reduce per member -> score -> top-k -> project
//! ```
//!
//! # WHAT EACH STAGE MEANS
//!
//! * WINDOW picks samples: [`WindowReduceWindow`]. A window that would hold more than
//!   [`MAX_WINDOW_SAMPLES`] samples is refused, not truncated.
//! * SELECT tests every cell of every chosen sample against the named predicates, which reuse
//!   the six `FeatureFilter` operators. A name is a label, a lane or `member`; any other name is
//!   refused when the plan is compiled, before a sample is read. A cell is tested as the sample
//!   stores it, so the predicates cut BEFORE the member's samples are reduced together.
//! * REDUCE folds one member's surviving cells lane by lane with the reduction the header declares
//!   for that lane ([`read_reducer`]: the write-side merge verb, as the read-side window reduction) through the shared window
//!   reducers, in sample order. Labels are the newest surviving cell's.
//! * SCORE turns the reduced lanes into the ordering value: [`WindowReduceScore`]. Every member is
//!   ranked: a ratio whose denominator is 0 scores 0, and a Wilson score of lanes that are both 0
//!   scores 0 (`unscored` stays 0 for those; only a score that cannot be formed at all, such as a
//!   non-finite weighted sum, leaves a member out and is counted there).
//!   The one score that is not a function of the reduced lanes is `Decay`: it reads the value each of
//!   the member's samples stores for a lane and halves it for every half-life between that sample
//!   and the NEWEST sample of the window (so it does not read the clock, and a key read later or
//!   replayed scores the same). Every member's score is scaled by the same factor when that instant
//!   moves, so the order never depends on it. Its half-life is bounded [`MIN_HALF_LIFE_MS`]..=
//!   [`MAX_HALF_LIFE_MS`]; the total is a float and cannot wrap.
//! * TOP-K keeps the `k` best in a bounded heap. Equal scores order by member, smaller first
//!   (`u64` members before bytes members), so the answer is a function of the data alone.
//! * PROJECT returns the named labels and lanes, or all of them.
//!
//! # COST
//!
//! Samples are stored sorted by member, so the members of all chosen samples are visited in
//! order by a k-way merge: a member's cells meet each other without any per-member table. Work is
//! `O(cells * log samples)` for the merge plus `O(samples * lanes)` per member for the
//! reduction, and the heap is `O(k)`. THE ALLOCATIONS DO NOT DEPEND ON THE NUMBER OF CELLS OR
//! MEMBERS: the cursors, the merge heap, the per-member scratch and the result slots are sized by
//! the sample count, the lane count and `k`, once, before the first cell is read. A sample
//! whose cells are not strictly ascending by member is refused (it would be merged wrongly).
//!
//! The module is pure: it reads decoded [`Cell`]s and a clock value and touches no engine state.

use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeSet, BinaryHeap};

use super::window_reduce_rank_options::{
    last_bounds, validate_options, Anchor, CompiledMembers, CompiledQuota, RankOptions,
};
use super::cell_set::{validate_top_k, within, Cell, CellHeader, CellRef, FlatCells, LaneReducer, MemberRef};
#[cfg(test)]
use super::cell_set::Member;
use super::product_model::filter_op_holds;
use super::window_aggregate::{weighted_sum, DecayedSumAcc, Reduced, Reducer};
use crate::types::{
    CellMemberId, WindowReduceRankSpec, WindowReduceSampleInfo, WindowReduceScore, WindowReduceWindow, FeatureFilter,
    FeatureFilterOp, RankedCellRow, RankedCells,
};

/// Samples one ranked read may merge.
pub(crate) const MAX_WINDOW_SAMPLES: usize = 512;
/// Rows a [`WindowReduceWindow::NewestRows`] window may ask for: no more than a merged read of
/// several keys gathers ([`super::window_reduce_fan_in::MAX_FAN_IN_CELLS`]).
pub(crate) const MAX_WINDOW_ROWS: usize = 262_144;
/// Predicates one ranked read may carry.
pub(crate) const MAX_PREDICATES: usize = 8;
/// Lanes in one weighted score.
pub(crate) const MAX_WEIGHTED_LANES: usize = 8;
/// The shortest half-life of a decay score: one millisecond.
pub(crate) const MIN_HALF_LIFE_MS: u64 = 1;
/// The longest half-life of a decay score: ten years of 365 days.
pub(crate) const MAX_HALF_LIFE_MS: u64 = 10 * 365 * 24 * 60 * 60 * 1000;

/// One decoded sample: its stamp and its cells, sorted strictly ascending by member.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SampleView<'a> {
    pub(crate) timestamp_ms: u64,
    pub(crate) cells: &'a [Cell],
}

/// One decoded sample held in the flat form: the same stamp and cells, with no allocation per cell.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FlatView<'a> {
    pub(crate) timestamp_ms: u64,
    pub(crate) cells: &'a FlatCells,
}

/// One decoded sample in the flat form, read without its first `skip` cells: the sample at the
/// edge of a [`WindowReduceWindow::NewestRows`] window, which holds more cells than the window
/// has room for. The cells it keeps are its largest members, in order, so everything the ranking
/// checks of a sample (strictly ascending members) still holds. `skip` of 0 is the whole sample.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CutView<'a> {
    pub(crate) timestamp_ms: u64,
    pub(crate) cells: &'a FlatCells,
    pub(crate) skip: usize,
}

/// What the plan reads of a decoded sample: its stamp, and each cell in place. The plain
/// `Vec<Cell>` form and the flat form both read through it, so the ranking is one piece of code
/// and the two forms cannot disagree about an answer.
pub(crate) trait SampleRead<'a>: Copy {
    fn stamp(&self) -> u64;
    fn cell_count(&self) -> usize;
    /// Cell `i`, in place. `i` is below `cell_count()`.
    fn cell(&self, i: usize) -> CellRef<'a>;
}

impl<'a> SampleRead<'a> for SampleView<'a> {
    fn stamp(&self) -> u64 {
        self.timestamp_ms
    }
    fn cell_count(&self) -> usize {
        self.cells.len()
    }
    fn cell(&self, i: usize) -> CellRef<'a> {
        let cell: &'a Cell = &self.cells[i];
        CellRef { member: cell.member.view(), labels: &cell.labels, lanes: &cell.lanes }
    }
}

impl<'a> SampleRead<'a> for FlatView<'a> {
    fn stamp(&self) -> u64 {
        self.timestamp_ms
    }
    fn cell_count(&self) -> usize {
        self.cells.len()
    }
    fn cell(&self, i: usize) -> CellRef<'a> {
        let cells: &'a FlatCells = self.cells;
        cells.cell(i)
    }
}

impl<'a> SampleRead<'a> for CutView<'a> {
    fn stamp(&self) -> u64 {
        self.timestamp_ms
    }
    fn cell_count(&self) -> usize {
        self.cells.len().saturating_sub(self.skip)
    }
    fn cell(&self, i: usize) -> CellRef<'a> {
        let cells: &'a FlatCells = self.cells;
        cells.cell(self.skip + i)
    }
}

// ------------------------------------------------------------------------------------------
// Compiling a query against a header
// ------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Target {
    Member,
    Label(usize),
    Lane(usize),
}

pub(crate) struct Predicate {
    target: Target,
    op: FeatureFilterOp,
    value: u64,
}

impl Predicate {
    pub(crate) fn holds(&self, cell: &CellRef<'_>) -> bool {
        // Compared as i128 so a `u64` label or value and an `i64` lane meet without a wrap.
        let lhs = match self.target {
            Target::Member => match cell.member {
                MemberRef::U64(v) => i128::from(v),
                // A bytes member has no number to compare.
                MemberRef::Bytes(_) => return false,
            },
            Target::Label(i) => i128::from(cell.labels[i]),
            Target::Lane(i) => i128::from(cell.lanes[i]),
        };
        // A lane is signed, so the 64 bits a predicate carries are read as signed for it; a label
        // and a member are unsigned.
        let rhs = match self.target {
            Target::Lane(_) => i128::from(self.value as i64),
            _ => i128::from(self.value),
        };
        filter_op_holds(&self.op, lhs, rhs)
    }
}

/// A [`WindowReduceScore`] with every name resolved to a lane position. The ranked read and the capped
/// group of a rollup score a cell by this one type.
pub(crate) enum Scorer {
    Lane(usize),
    Ratio(usize, usize),
    Wilson(usize, usize, f64),
    Member,
    Time,
    Weighted(Vec<usize>, Vec<f64>),
    Decay(usize, u64),
}

/// A query checked against a header: every name resolved to a position, every bound met.
pub(crate) struct Plan {
    window: WindowReduceWindow,
    members: CompiledMembers,
    anchor: Anchor,
    quota: Option<CompiledQuota>,
    predicates: Vec<Predicate>,
    scorer: Scorer,
    /// The read answers stored cells, newest sample first ([`WindowReduceScore::Recent`]).
    recent: bool,
    /// The answer says which samples were used and how many cells each held.
    sample_info: bool,
    top_k: usize,
    lane_count: usize,
    label_count: usize,
    reducers: Vec<Reducer>,
    labels_out: Vec<usize>,
    lanes_out: Vec<usize>,
    label_names: Vec<String>,
    lane_names: Vec<String>,
}

/// The window reduction that combines a lane's STORED values across samples. The lane's verb
/// merged writes into one sample; across samples a counted lane holds counts, and counts add.
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

/// The named predicates of a query resolved against `header`: a label, a lane or `member` each.
/// The ranked read and a removal select cells by this one code.
pub(crate) fn compile_predicates(header: &CellHeader, filters: &[FeatureFilter]) -> Result<Vec<Predicate>, String> {
    filters
        .iter()
        .map(|FeatureFilter { field, op, value }| {
            Ok(Predicate { target: resolve(header, field)?, op: op.clone(), value: *value })
        })
        .collect()
}

fn lane_named(header: &CellHeader, name: &str) -> Result<usize, String> {
    match resolve(header, name)? {
        Target::Lane(i) => Ok(i),
        _ => Err(format!("`{name}` is not a lane, and a score reads lanes")),
    }
}

/// The checks that need no header: bounds, ranges and parameters. A command runs this at
/// admission, before any sample is read; [`Plan::compile`] runs it again so a plan can never be
/// built from a spec that skipped it.
pub(crate) fn validate_spec(spec: &WindowReduceRankSpec) -> Result<(), String> {
    validate_top_k(spec.top_k)?;
    within("predicates per query", spec.filters.len(), MAX_PREDICATES)?;
    match &spec.window {
        WindowReduceWindow::Between { start_ms, end_ms } if start_ms > end_ms => {
            return Err(format!("window starts at {start_ms}, after it ends at {end_ms}"))
        }
        WindowReduceWindow::LastN { n } => {
            if *n == 0 {
                return Err("window of the last 0 samples is empty".to_string());
            }
            within("samples in the window", *n, MAX_WINDOW_SAMPLES)?;
        }
        WindowReduceWindow::NewestRows { n } => {
            if *n == 0 {
                return Err("window of the newest 0 rows is empty".to_string());
            }
            within("rows in the window", *n, MAX_WINDOW_ROWS)?;
        }
        _ => {}
    }
    validate_options(&spec.window, &RankOptions::of(spec))?;
    check_recent(&spec.score, &spec.quota)?;
    validate_score(&spec.score)
}

/// A `Recent` read answers stored cells, and a quota on it walks those cells (see
/// [`Plan::run_recent_quota`]): it needs a lane to count, because a quota of `others` alone would
/// end the walk at the first cell.
fn check_recent(score: &WindowReduceScore, quota: &crate::types::WindowReduceQuota) -> Result<(), String> {
    if matches!(score, WindowReduceScore::Recent) && !quota.is_none() && quota.lanes.is_empty() {
        return Err("a quota on a stored-cell read needs at least one lane to count".to_string());
    }
    Ok(())
}

/// The checks on a score that need no header.
pub(crate) fn validate_score(score: &WindowReduceScore) -> Result<(), String> {
    match score {
        WindowReduceScore::Wilson { z, .. } if !z.is_finite() || *z < 0.0 => {
            return Err(format!("wilson z must be finite and not negative, got {z}"))
        }
        WindowReduceScore::Weighted { lanes, weights } => {
            if lanes.is_empty() {
                return Err("a weighted score needs at least one lane".to_string());
            }
            within("lanes in a weighted score", lanes.len(), MAX_WEIGHTED_LANES)?;
            if lanes.len() != weights.len() {
                return Err(format!("a weighted score has {} lanes and {} weights", lanes.len(), weights.len()));
            }
            if let Some(bad) = weights.iter().find(|w| !w.is_finite()) {
                return Err(format!("weight {bad} is not finite"));
            }
        }
        WindowReduceScore::Decay { half_life_ms, .. } => {
            if !(MIN_HALF_LIFE_MS..=MAX_HALF_LIFE_MS).contains(half_life_ms) {
                return Err(format!(
                    "half-life {half_life_ms} ms is outside {MIN_HALF_LIFE_MS}..={MAX_HALF_LIFE_MS}"
                ));
            }
        }
        _ => {}
    }
    Ok(())
}

impl Scorer {
    /// Resolves the names of `score` against `header`.
    pub(crate) fn compile(header: &CellHeader, score: &WindowReduceScore) -> Result<Scorer, String> {
        Ok(match score {
            WindowReduceScore::Lane { lane } => Scorer::Lane(lane_named(header, lane)?),
            WindowReduceScore::Ratio { numerator, denominator } => {
                Scorer::Ratio(lane_named(header, numerator)?, lane_named(header, denominator)?)
            }
            WindowReduceScore::Wilson { successes, trials, z } => {
                Scorer::Wilson(lane_named(header, successes)?, lane_named(header, trials)?, *z)
            }
            WindowReduceScore::Member => Scorer::Member,
            WindowReduceScore::Time => Scorer::Time,
            WindowReduceScore::Weighted { lanes, weights } => {
                let indexes = lanes.iter().map(|name| lane_named(header, name)).collect::<Result<Vec<_>, _>>()?;
                Scorer::Weighted(indexes, weights.clone())
            }
            WindowReduceScore::Decay { lane, half_life_ms } => Scorer::Decay(lane_named(header, lane)?, *half_life_ms),
            WindowReduceScore::Recent => {
                return Err("`recent` returns stored cells, not members, and is not a score a cap can use".to_string())
            }
        })
    }
}

impl Plan {
    /// Checks `spec` against `header`. Every refusal names what was wrong; nothing is read.
    pub(crate) fn compile(header: &CellHeader, spec: &WindowReduceRankSpec) -> Result<Plan, String> {
        Plan::compile_with(header, spec, &RankOptions::of(spec))
    }

    /// [`Plan::compile`] with the controls given instead of the ones the spec carries; `compile` is
    /// this with `RankOptions::of(spec)`.
    pub(crate) fn compile_with(header: &CellHeader, spec: &WindowReduceRankSpec, options: &RankOptions) -> Result<Plan, String> {
        validate_spec(spec)?;
        validate_options(&spec.window, options)?;
        check_recent(&spec.score, &options.quota)?;
        let recent = matches!(spec.score, WindowReduceScore::Recent);
        let members = CompiledMembers::compile(&options.members)?;
        let quota = if options.quota.is_none() {
            None
        } else {
            let lanes = options
                .quota
                .lanes
                .iter()
                .map(|entry| lane_named(header, &entry.lane).map(|lane| (lane, entry.count)))
                .collect::<Result<Vec<_>, _>>()?;
            Some(CompiledQuota::new(lanes, options.quota.others))
        };

        let predicates = compile_predicates(header, &spec.filters)?;

        // A recent read has no ordering value: its rows come newest first, which `run_recent` walks.
        let scorer = if recent { Scorer::Time } else { Scorer::compile(header, &spec.score)? };

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
            members,
            anchor: options.anchor,
            quota,
            predicates,
            scorer,
            recent,
            sample_info: spec.sample_info,
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
}

// ------------------------------------------------------------------------------------------
// Scores
// ------------------------------------------------------------------------------------------

/// An ordering value. Within one query every key is the same variant.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Key {
    Int(i128),
    Float(f64),
}

impl Key {
    pub(crate) fn cmp(&self, other: &Key) -> Ordering {
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
    /// `points` are the member's occurrences in ascending sample order and `as_of_ms` the newest
    /// stamp of the window: the two things a decay score reads that a reduced lane does not hold.
    fn score(
        &self,
        member: MemberRef<'_>,
        last_ms: u64,
        lanes: &[i64],
        points: &[(u64, CellRef<'_>)],
        as_of_ms: u64,
    ) -> Result<Option<Key>, String> {
        match &self.scorer {
            Scorer::Decay(lane, half_life_ms) => {
                let mut acc = DecayedSumAcc::new(*half_life_ms, as_of_ms).map_err(|e| e.0)?;
                for (timestamp_ms, cell) in points {
                    acc.push(*timestamp_ms, cell.lanes[*lane]);
                }
                Ok(Some(Key::Float(acc.finish_f64())))
            }
            scorer => scorer.score(member, last_ms, lanes),
        }
    }
}

/// The ratio score: the quotient of the two lanes as a whole number (the fraction is dropped, so
/// every ratio below 1 scores 0 and members tie on it), and 0 when the denominator is 0. A member
/// whose denominator is 0 is ranked at 0, not dropped.
pub(crate) fn whole_ratio(numerator: i64, denominator: i64) -> i128 {
    if denominator == 0 {
        0
    } else {
        i128::from(numerator) / i128::from(denominator)
    }
}

/// The Wilson score: the lower bound of the interval for the smaller of the two lanes out of the
/// larger (so which of them holds the successes does not matter), at `z` deviations, scaled by
/// 10,000 and cut to a whole number. 0 when the larger lane is 0 or the arithmetic leaves the
/// finite numbers; a member is always ranked.
pub(crate) fn wilson_score(first: i64, second: i64, z: f64) -> i128 {
    let (low, high) = (first.min(second), first.max(second));
    if high == 0 {
        return 0;
    }
    let high = high as f64;
    let ratio = low as f64 / high;
    let z2 = z * z;
    let score = (ratio + z2 / (2.0 * high) - (z / (2.0 * high)) * (4.0 * high * (1.0 - ratio) * ratio + z2).sqrt())
        / (1.0 + z2 / high)
        * 10_000.0;
    if !score.is_finite() {
        0
    } else if score >= i64::MAX as f64 {
        i128::from(i64::MAX)
    } else {
        i128::from(score as i64)
    }
}

impl Scorer {
    /// The ordering value of one member's `lanes`. `None`: the score is undefined for it.
    pub(crate) fn score(&self, member: MemberRef<'_>, last_ms: u64, lanes: &[i64]) -> Result<Option<Key>, String> {
        Ok(match self {
            Scorer::Lane(i) => Some(Key::Int(i128::from(lanes[*i]))),
            Scorer::Ratio(n, d) => Some(Key::Int(whole_ratio(lanes[*n], lanes[*d]))),
            Scorer::Wilson(s, t, z) => Some(Key::Int(wilson_score(lanes[*s], lanes[*t], *z))),
            Scorer::Member => match member {
                MemberRef::U64(v) => Some(Key::Int(i128::from(v))),
                MemberRef::Bytes(_) => {
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
            Scorer::Decay(..) => {
                return Err("a decay score reads a member's samples, not one folded cell".to_string())
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
    member: MemberRef<'a>,
    slot: usize,
}

impl Ord for Entry<'_> {
    /// Greater means worse: lower key, then larger member.
    fn cmp(&self, other: &Self) -> Ordering {
        other.key.cmp(&self.key).then_with(|| self.member.cmp(&other.member))
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

    fn offer(&mut self, key: Key, member: MemberRef<'a>, last_ms: u64, labels: &[u64], lanes: &[i64]) {
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

fn member_id(member: MemberRef<'_>) -> CellMemberId {
    match member {
        MemberRef::U64(v) => CellMemberId::U64(v),
        MemberRef::Bytes(b) => CellMemberId::Bytes(b.to_vec()),
    }
}

// ------------------------------------------------------------------------------------------
// Running a plan
// ------------------------------------------------------------------------------------------

fn samples_between<'s, 'a, V: SampleRead<'a>>(samples: &'s [V], from_ms: u64, to_ms: u64) -> &'s [V] {
    let first = samples.partition_point(|s| s.stamp() < from_ms);
    let end = samples.partition_point(|s| s.stamp() <= to_ms);
    &samples[first..end.max(first)]
}

impl Plan {
    /// The samples the window covers, in ascending stamp order.
    pub(crate) fn select_window<'s, 'a, V: SampleRead<'a>>(
        &self,
        now_ms: u64,
        samples: &'s [V],
    ) -> Result<&'s [V], String> {
        if samples.windows(2).any(|pair| pair[0].stamp() >= pair[1].stamp()) {
            return Err("samples are not in strictly ascending stamp order".to_string());
        }
        let chosen = match &self.window {
            WindowReduceWindow::Between { start_ms, end_ms } => samples_between(samples, *start_ms, *end_ms),
            WindowReduceWindow::Last { duration_ms } => {
                let newest = samples.last().map(|s| s.stamp());
                match last_bounds(self.anchor, *duration_ms, now_ms, newest) {
                    Some((from_ms, to_ms)) => samples_between(samples, from_ms, to_ms),
                    None => &samples[..0],
                }
            }
            WindowReduceWindow::Since { start_ms } => samples_between(samples, *start_ms, u64::MAX),
            WindowReduceWindow::LastN { n } => &samples[samples.len().saturating_sub(*n)..],
            // The rows of a rows window are counted where the samples are decoded, because the
            // sample at its edge is read without its first cells and a slice of samples cannot say
            // that. The reader hands over exactly the samples the rows reach, so every one of
            // them is in the window.
            WindowReduceWindow::NewestRows { .. } => samples,
        };
        within("samples in the window", chosen.len(), MAX_WINDOW_SAMPLES)?;
        Ok(chosen)
    }

    /// The first cell at or after `*at` that passes every predicate, as its member. Each cell is
    /// visited once, so this is also where the shape and the order of a sample are checked.
    fn settle<'a, V: SampleRead<'a>>(&self, sample: &V, at: &mut usize) -> Result<Option<MemberRef<'a>>, String> {
        while *at < sample.cell_count() {
            let cell = sample.cell(*at);
            if cell.lanes.len() != self.lane_count || cell.labels.len() != self.label_count {
                return Err(format!(
                    "a cell has {} labels and {} lanes, the header declares {} and {}",
                    cell.labels.len(),
                    cell.lanes.len(),
                    self.label_count,
                    self.lane_count
                ));
            }
            if *at > 0 && sample.cell(*at - 1).member >= cell.member {
                return Err("a sample is not strictly ascending by member".to_string());
            }
            if self.members.allows(cell.member) && self.predicates.iter().all(|predicate| predicate.holds(&cell)) {
                return Ok(Some(cell.member));
            }
            *at += 1;
        }
        Ok(None)
    }

    /// Moves cursor `i` past its current cell, recording that cell as an occurrence of the
    /// member being gathered, and re-queues the cursor at its next surviving cell.
    fn consume<'a, V: SampleRead<'a>>(
        &self,
        chosen: &[V],
        at: &mut [usize],
        queue: &mut BinaryHeap<Reverse<(MemberRef<'a>, usize)>>,
        occurrences: &mut Vec<(u64, CellRef<'a>)>,
        i: usize,
    ) -> Result<(), String> {
        occurrences.push((chosen[i].stamp(), chosen[i].cell(at[i])));
        at[i] += 1;
        if let Some(next) = self.settle(&chosen[i], &mut at[i])? {
            queue.push(Reverse((next, i)));
        }
        Ok(())
    }

    /// What the answer says of the samples it used, oldest first: nothing unless the read asked.
    /// A sample's cells are counted as the window holds them, before the member set and the
    /// predicates apply.
    fn sample_summary<'a, V: SampleRead<'a>>(&self, chosen: &[V]) -> Vec<WindowReduceSampleInfo> {
        if !self.sample_info {
            return Vec::new();
        }
        chosen
            .iter()
            .map(|sample| WindowReduceSampleInfo { timestamp_ms: sample.stamp(), cells: sample.cell_count() })
            .collect()
    }

    /// The rows of a [`WindowReduceScore::Recent`] read: the stored cells of `chosen` that pass the member
    /// set and every predicate, the newest sample first, each sample's cells by member, until
    /// `top_k` rows are held. No cell is reduced with another, so a member written in three
    /// samples is three rows. Where two samples share a stamp the later one in `chosen` goes
    /// first (it is the one a reduction would combine last). The walk stops at `top_k`, so the work
    /// is bounded by the window and by the rows asked for.
    fn run_recent<'a, V: SampleRead<'a>>(&self, chosen: &[V]) -> Result<RankedCells, String> {
        if let Some(quota) = &self.quota {
            return self.run_recent_quota(chosen, quota);
        }
        let mut rows = Vec::with_capacity(self.top_k.min(chosen.iter().map(|s| s.cell_count()).sum::<usize>()));
        'newest: for sample in chosen.iter().rev() {
            let mut at = 0usize;
            while self.settle(sample, &mut at)?.is_some() {
                let cell = sample.cell(at);
                rows.push(RankedCellRow {
                    member: member_id(cell.member),
                    score: sample.stamp() as f64,
                    last_ms: sample.stamp(),
                    labels: self.labels_out.iter().map(|&i| cell.labels[i]).collect(),
                    lanes: self.lanes_out.iter().map(|&i| cell.lanes[i]).collect(),
                });
                if rows.len() == self.top_k {
                    break 'newest;
                }
                at += 1;
            }
        }
        Ok(RankedCells {
            label_columns: self.label_names.clone(),
            lane_columns: self.lane_names.clone(),
            samples: chosen.len(),
            window_first_ms: chosen.first().map_or(0, |s| s.stamp()),
            window_last_ms: chosen.last().map_or(0, |s| s.stamp()),
            matched_first_ms: rows.iter().map(|r| r.last_ms).min().unwrap_or(0),
            matched_last_ms: rows.iter().map(|r| r.last_ms).max().unwrap_or(0),
            keys: 0,
            unscored: 0,
            rows,
            refused: String::new(),
            sample_info: self.sample_summary(chosen),
        })
    }

    /// The stored-cell read with a quota: the cells of `chosen` that pass the member set and every
    /// predicate, newest sample first, none reduced, walked until every lane of the quota is full or
    /// `top_k` cells have been walked.
    ///
    /// A lane is SHORT until it holds its count of distinct members. A walked cell that has a
    /// non-zero value in a short lane is returned when its member is new to that lane (it then counts
    /// toward every short lane it fits), and a cell that fits no short lane is returned while fewer
    /// than `others` of them have been. The walk ends when no lane is short, even when `others` is not
    /// met, and a cell that is walked and not returned still counts toward `top_k`. So `top_k` bounds
    /// the cells looked at, not the rows returned, and the same member can come back in several rows
    /// when it fits no lane.
    fn run_recent_quota<'a, V: SampleRead<'a>>(&self, chosen: &[V], quota: &CompiledQuota) -> Result<RankedCells, String> {
        struct Short<'m> {
            lane: usize,
            count: usize,
            seen: BTreeSet<MemberRef<'m>>,
        }
        let mut short: Vec<Short<'a>> =
            quota.lanes().iter().map(|&(lane, count)| Short { lane, count, seen: BTreeSet::new() }).collect();
        let mut others_left = quota.others();
        let (mut walked, mut rows) = (0usize, Vec::new());
        'newest: for sample in chosen.iter().rev() {
            let mut at = 0usize;
            while self.settle(sample, &mut at)?.is_some() {
                let cell = sample.cell(at);
                walked += 1;
                let (mut fits, mut new_member) = (false, false);
                let mut i = 0;
                while i < short.len() {
                    if cell.lanes[short[i].lane] != 0 {
                        fits = true;
                        if short[i].seen.insert(cell.member) {
                            new_member = true;
                            if short[i].seen.len() >= short[i].count {
                                short.swap_remove(i);
                                continue;
                            }
                        }
                    }
                    i += 1;
                }
                let others = !new_member && !fits && others_left > 0;
                if others {
                    others_left -= 1;
                }
                if new_member || others {
                    rows.push(RankedCellRow {
                        member: member_id(cell.member),
                        score: sample.stamp() as f64,
                        last_ms: sample.stamp(),
                        labels: self.labels_out.iter().map(|&i| cell.labels[i]).collect(),
                        lanes: self.lanes_out.iter().map(|&i| cell.lanes[i]).collect(),
                    });
                }
                if short.is_empty() || walked >= self.top_k {
                    break 'newest;
                }
                at += 1;
            }
        }
        Ok(RankedCells {
            label_columns: self.label_names.clone(),
            lane_columns: self.lane_names.clone(),
            samples: chosen.len(),
            window_first_ms: chosen.first().map_or(0, |s| s.stamp()),
            window_last_ms: chosen.last().map_or(0, |s| s.stamp()),
            sample_info: self.sample_summary(chosen),
            rows,
            ..RankedCells::default()
        })
    }

    /// Runs the plan over `samples` (ascending by stamp; the window is applied here). `now_ms`
    /// is the instant a `Last` window ends at.
    pub(crate) fn run<'a, V: SampleRead<'a>>(&self, now_ms: u64, samples: &[V]) -> Result<RankedCells, String> {
        let chosen = self.select_window(now_ms, samples)?;
        let mut ranked = self.run_ordered(chosen)?;
        ranked.keys = usize::from(!chosen.is_empty());
        Ok(ranked)
    }

    /// Ranks the members of `chosen`, samples already picked and in the order their cells are
    /// to be combined: ascending by stamp, and where two share a stamp the earlier one in the
    /// slice is combined first (`First` and `Last` lanes resolve a tie by that order). The window
    /// is not applied again. A caller that merges samples of more than one series builds the
    /// slice itself; [`Plan::run`] hands it one series' window.
    pub(crate) fn run_ordered<'a, V: SampleRead<'a>>(&self, chosen: &[V]) -> Result<RankedCells, String> {
        if self.recent {
            return self.run_recent(chosen);
        }
        // Everything sized by the sample count, the lane count or k is allocated here, once.
        let mut at = vec![0usize; chosen.len()];
        let mut queue: BinaryHeap<Reverse<(MemberRef<'a>, usize)>> = BinaryHeap::with_capacity(chosen.len());
        let mut occurrences: Vec<(u64, CellRef<'a>)> = Vec::with_capacity(chosen.len());
        let mut reduced = vec![0i64; self.lane_count];
        let mut top = TopK::new(self.top_k, self.lane_count, self.label_count);
        let mut unscored = 0u64;
        let (mut matched_first, mut matched_last) = (u64::MAX, 0u64);
        let as_of_ms = chosen.last().map_or(0, |sample| sample.stamp());

        for (i, sample) in chosen.iter().enumerate() {
            if let Some(member) = self.settle(sample, &mut at[i])? {
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

            // Occurrences arrive in ascending sample order: the queue breaks ties by index.
            for (lane, reducer) in self.reducers.iter().enumerate() {
                let points = occurrences.iter().map(|(ts, cell)| (*ts, cell.lanes[lane]));
                match reducer.reduce(points).map_err(|e| e.0)? {
                    Some(Reduced::Int(v)) => reduced[lane] = v,
                    other => return Err(format!("lane {lane} did not reduce to an integer: {other:?}")),
                }
            }
            let &(last_ms, newest) = occurrences.last().ok_or("a member has no occurrence")?;
            matched_first = matched_first.min(occurrences[0].0);
            matched_last = matched_last.max(last_ms);
            match self.score(member, last_ms, &reduced, &occurrences, as_of_ms)? {
                Some(key) => top.offer(key, member, last_ms, &newest.labels, &reduced),
                None => unscored += 1,
            }
        }

        let TopK { heap, lanes, labels, last, lane_count, label_count, .. } = top;
        let mut ranked = heap.into_sorted_vec();
        if let Some(quota) = &self.quota {
            quota.retain(&mut ranked, |entry| &lanes[entry.slot * lane_count..(entry.slot + 1) * lane_count]);
        }
        let rows = ranked
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
            samples: chosen.len(),
            window_first_ms: chosen.first().map_or(0, |s| s.stamp()),
            window_last_ms: chosen.last().map_or(0, |s| s.stamp()),
            matched_first_ms: if matched_first == u64::MAX { 0 } else { matched_first },
            matched_last_ms: matched_last,
            // The caller knows how many series `chosen` came from.
            keys: 0,
            unscored,
            rows,
            refused: String::new(),
            sample_info: self.sample_summary(chosen),
        })
    }
}

#[cfg(test)]
mod signed_lane_predicate_tests {
    use super::*;
    use super::super::cell_set::{DimSpec, DimType, EntityType, KeySpec, LaneReducer, LaneSpec};

    fn header() -> CellHeader {
        CellHeader {
            version: super::super::cell_set::HEADER_VERSION,
            key: KeySpec {
                namespace: "ns".to_string(),
                entity: EntityType::U64,
                dimensions: vec![DimSpec { name: "channel".to_string(), ty: DimType::U16, required: true }],
            },
            labels: vec!["slot".to_string()],
            lanes: vec![LaneSpec { name: "v".to_string(), reducer: LaneReducer::Sum }],
        }
    }

    fn holds(filter: FeatureFilter, lane: i64, slot: u64) -> bool {
        let predicates = compile_predicates(&header(), &[filter]).expect("the names are declared");
        let (labels, lanes) = ([slot], [lane]);
        predicates[0].holds(&CellRef { member: MemberRef::U64(1), labels: &labels, lanes: &lanes })
    }

    fn on(field: &str, op: FeatureFilterOp, value: u64) -> FeatureFilter {
        FeatureFilter { field: field.to_string(), op, value }
    }

    #[test]
    fn a_lane_predicate_reads_its_value_as_signed_and_a_label_predicate_as_unsigned() {
        // A lane is signed: the 64 bits of -1 equal a lane holding -1, and -1 is below 0 and 5.
        assert!(holds(on("v", FeatureFilterOp::Equal, (-1i64) as u64), -1, 0));
        assert!(!holds(on("v", FeatureFilterOp::Equal, (-1i64) as u64), 1, 0));
        assert!(holds(on("v", FeatureFilterOp::LessThan, 0), -1, 0));
        assert!(holds(on("v", FeatureFilterOp::GreaterThan, (-5i64) as u64), -1, 0));
        assert!(!holds(on("v", FeatureFilterOp::GreaterThan, (-5i64) as u64), -9, 0));
        // A label is unsigned: the largest label equals the largest value, and is above every other.
        assert!(holds(on("slot", FeatureFilterOp::Equal, u64::MAX), 0, u64::MAX));
        assert!(holds(on("slot", FeatureFilterOp::GreaterThan, 5), 0, u64::MAX));
        // The test no cell passes: a member is never below 0.
        assert!(!holds(on("member", FeatureFilterOp::LessThan, 0), 0, 0));
    }
}

#[cfg(test)]
#[path = "window_reduce_rank_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "window_reduce_rank_flat_tests.rs"]
mod flat_tests;
