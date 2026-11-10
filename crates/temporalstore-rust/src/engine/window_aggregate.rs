// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The one home of the reductions a windowed read applies to a series of numbers.
//!
//! # WHAT USED TO BE SPREAD OUT
//!
//! Three read paths each carried a private copy of "sum / min / max / first / last / count" over a
//! window: the feature aggregate query (`aggregate_feature_values`), the counter-family query
//! (`aggregate_control_state_values`), and the plain-scan arm of the windowed counter sum. They
//! agree on the verbs and disagree on the fallbacks, and those disagreements are answers a caller
//! may depend on, so this module keeps them exactly:
//!
//! | name                      | feature query              | counter query              |
//! |---------------------------|----------------------------|----------------------------|
//! | `sum`                     | sum of the parseable values| sum                        |
//! | `avg` / `average`         | truncating mean            | (unknown) sum              |
//! | `min` / `max`             | over the parseable values  | over the values            |
//! | `first` / `last`          | the first / last VALUE     | the first / last value     |
//! | `count`                   | number of values           | sum                        |
//! | `events` / `len`          | number of values           | number of values           |
//! | empty name                | number of values           | sum                        |
//! | any other name            | number of values           | sum                        |
//!
//! The lenient fallbacks belong to those two query doors and are NOT widened here: a door decides
//! what an unknown name means, [`AggName::parse`] only says whether the name is one this module
//! knows. A new read that wants unknown names refused calls [`AggName::parse`] and refuses `None`.
//!
//! # OVERFLOW
//!
//! The legacy reductions ([`sum_legacy`], [`mean_truncated`]) keep `Iterator::sum`'s behaviour
//! (a panic in a debug build, a wrap in a release build) because changing it would change an
//! answer. Nothing new should call them.
//!
//! # THE TYPED REDUCTIONS
//!
//! The accumulators from [`Mergeability`] down serve the ranked read and the typed aggregate
//! query. They are used by those reads as they land, so the module allows unused items until then.
#![allow(dead_code)]

/// A reduction name this module knows. Matched ASCII case-insensitively.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AggName {
    Sum,
    Avg,
    Min,
    Max,
    First,
    Last,
    /// `count`, and the empty name.
    Count,
    Events,
    Len,
}

impl AggName {
    /// `Some` for a name this module knows, `None` for anything else. Does not allocate.
    pub(crate) fn parse(name: &str) -> Option<AggName> {
        const TABLE: [(&str, AggName); 10] = [
            ("sum", AggName::Sum),
            ("avg", AggName::Avg),
            ("average", AggName::Avg),
            ("min", AggName::Min),
            ("max", AggName::Max),
            ("first", AggName::First),
            ("last", AggName::Last),
            ("count", AggName::Count),
            ("events", AggName::Events),
            ("len", AggName::Len),
        ];
        if name.is_empty() {
            return Some(AggName::Count);
        }
        TABLE
            .iter()
            .find(|(known, _)| known.eq_ignore_ascii_case(name))
            .map(|(_, parsed)| *parsed)
    }
}

/// Sum with `Iterator::sum`'s overflow behaviour (see the module note).
pub(crate) fn sum_legacy(values: impl Iterator<Item = i64>) -> i64 {
    values.sum()
}

/// Truncating integer mean; `0` for no values. The sum has `Iterator::sum`'s overflow behaviour.
pub(crate) fn mean_truncated(values: impl Iterator<Item = i64>) -> i64 {
    let (sum, count) = values.fold((0i64, 0i64), |(sum, count), value| (sum + value, count + 1));
    if count == 0 {
        0
    } else {
        sum / count
    }
}

/// Smallest value, `0` for no values.
pub(crate) fn min_or_zero(values: impl Iterator<Item = i64>) -> i64 {
    values.min().unwrap_or_default()
}

/// Largest value, `0` for no values.
pub(crate) fn max_or_zero(values: impl Iterator<Item = i64>) -> i64 {
    values.max().unwrap_or_default()
}

/// First value, `0` for no values.
pub(crate) fn first_or_zero(mut values: impl Iterator<Item = i64>) -> i64 {
    values.next().unwrap_or_default()
}

/// Last value, `0` for no values.
pub(crate) fn last_or_zero(values: impl Iterator<Item = i64>) -> i64 {
    values.last().unwrap_or_default()
}

/// The feature aggregate query's reduction over `values`, each already parsed to a number or
/// `None` when it is not one. `total` is the number of values INCLUDING the unparseable ones,
/// which is what the count verbs report.
pub(crate) fn feature_legacy(
    name: &str,
    values: impl Iterator<Item = Option<i64>>,
    total: usize,
) -> i64 {
    match AggName::parse(name) {
        Some(AggName::Sum) => sum_legacy(values.flatten()),
        Some(AggName::Avg) => mean_truncated(values.flatten()),
        Some(AggName::Min) => min_or_zero(values.flatten()),
        Some(AggName::Max) => max_or_zero(values.flatten()),
        // First and last name the first and last VALUE, parseable or not.
        Some(AggName::First) => first_or_zero(values.take(1).flatten()),
        Some(AggName::Last) => last_or_zero(values.last().into_iter().flatten()),
        _ => total as i64,
    }
}

/// Reduce a feature window as its values are read, without retaining the window in memory.
pub(crate) fn feature_stream(name: &str, values: impl Iterator<Item = Option<i64>>) -> i64 {
    match AggName::parse(name) {
        Some(AggName::Sum) => sum_legacy(values.flatten()),
        Some(AggName::Avg) => mean_truncated(values.flatten()),
        Some(AggName::Min) => min_or_zero(values.flatten()),
        Some(AggName::Max) => max_or_zero(values.flatten()),
        Some(AggName::First) => first_or_zero(values.take(1).flatten()),
        Some(AggName::Last) => last_or_zero(values.last().into_iter().flatten()),
        _ => values.count() as i64,
    }
}

/// The counter-family query's reduction over `values`. `Avg` is not a counter verb, so it falls
/// to the sum like every unknown name.
pub(crate) fn counter_legacy(name: &str, values: &[i64]) -> i64 {
    match AggName::parse(name) {
        Some(AggName::Events) | Some(AggName::Len) => values.len() as i64,
        Some(AggName::Min) => min_or_zero(values.iter().copied()),
        Some(AggName::Max) => max_or_zero(values.iter().copied()),
        Some(AggName::First) => first_or_zero(values.iter().copied()),
        Some(AggName::Last) => last_or_zero(values.iter().copied()),
        _ => sum_legacy(values.iter().copied()),
    }
}

// ---------------------------------------------------------------------------------------------
// Typed reductions for the ranked and aggregate reads.
//
// Every accumulator below is a small `Copy` value: `push` one point at a time, optionally `merge`
// a partial result computed elsewhere, then `finish`. None of them allocates, so a window of any
// length is reduced in constant space. Each states what an empty window answers, what overflow
// does, and whether two partial results can be merged (see [`Mergeability`]).
// ---------------------------------------------------------------------------------------------

/// Whether two partial results of a reduction can be combined into the result of the
/// concatenated window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mergeability {
    /// `merge` gives the reduction of the concatenation whatever the order of the two parts.
    Commutative,
    /// `merge(earlier, later)` gives the reduction of the concatenation, but only when `self` holds
    /// the earlier part of the stream and `other` the later: the answer depends on arrival order.
    OrderedOnly,
    /// No `merge` exists: the state is a running fold whose result depends on the whole order.
    No,
}

/// How a fractional result becomes an integer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Rounding {
    Floor,
    Ceil,
    /// Halves round away from zero (`f64::round`).
    Nearest,
}

/// `value` as an `i64` under `mode`. NaN answers `0`; a value outside the range saturates.
pub(crate) fn round_to_i64(value: f64, mode: Rounding) -> i64 {
    if value.is_nan() {
        return 0;
    }
    let rounded = match mode {
        Rounding::Floor => value.floor(),
        Rounding::Ceil => value.ceil(),
        Rounding::Nearest => value.round(),
    };
    // `as` saturates for floats and maps NaN to 0, which is the documented behaviour.
    rounded as i64
}

fn clamp_i128(value: i128) -> i64 {
    value.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

/// Sum in a 128-bit accumulator: exact for any window shorter than 2^63 points, so merging two
/// partial sums is exact too. `finish` saturates to the `i64` range. Empty window: `0`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SumAcc {
    total: i128,
}

impl SumAcc {
    pub(crate) const MERGE: Mergeability = Mergeability::Commutative;
    pub(crate) fn push(&mut self, value: i64) {
        self.total += value as i128;
    }
    pub(crate) fn merge(&mut self, other: &SumAcc) {
        self.total += other.total;
    }
    pub(crate) fn finish(&self) -> i64 {
        clamp_i128(self.total)
    }
    pub(crate) fn exact(&self) -> i128 {
        self.total
    }
}

/// Number of points. `finish` saturates at `i64::MAX`. Empty window: `0`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct CountAcc {
    n: u64,
}

impl CountAcc {
    pub(crate) const MERGE: Mergeability = Mergeability::Commutative;
    pub(crate) fn push(&mut self, _value: i64) {
        self.n = self.n.saturating_add(1);
    }
    pub(crate) fn merge(&mut self, other: &CountAcc) {
        self.n = self.n.saturating_add(other.n);
    }
    pub(crate) fn finish(&self) -> i64 {
        self.n.min(i64::MAX as u64) as i64
    }
}

/// Smallest value. Empty window: `None`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MinAcc {
    best: Option<i64>,
}

impl MinAcc {
    pub(crate) const MERGE: Mergeability = Mergeability::Commutative;
    pub(crate) fn push(&mut self, value: i64) {
        self.best = Some(self.best.map_or(value, |b| b.min(value)));
    }
    pub(crate) fn merge(&mut self, other: &MinAcc) {
        if let Some(value) = other.best {
            self.push(value);
        }
    }
    pub(crate) fn finish(&self) -> Option<i64> {
        self.best
    }
}

/// Largest value. Empty window: `None`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MaxAcc {
    best: Option<i64>,
}

impl MaxAcc {
    pub(crate) const MERGE: Mergeability = Mergeability::Commutative;
    pub(crate) fn push(&mut self, value: i64) {
        self.best = Some(self.best.map_or(value, |b| b.max(value)));
    }
    pub(crate) fn merge(&mut self, other: &MaxAcc) {
        if let Some(value) = other.best {
            self.push(value);
        }
    }
    pub(crate) fn finish(&self) -> Option<i64> {
        self.best
    }
}

/// The value with the smallest timestamp; among equal timestamps, the one pushed first.
/// Empty window: `None`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct FirstAcc {
    at: Option<(u64, i64)>,
}

impl FirstAcc {
    pub(crate) const MERGE: Mergeability = Mergeability::OrderedOnly;
    pub(crate) fn push(&mut self, timestamp_ms: u64, value: i64) {
        if self.at.map_or(true, |(t, _)| timestamp_ms < t) {
            self.at = Some((timestamp_ms, value));
        }
    }
    /// `other` must hold the points that arrived after `self`'s.
    pub(crate) fn merge(&mut self, other: &FirstAcc) {
        if let Some((t, v)) = other.at {
            self.push(t, v);
        }
    }
    pub(crate) fn finish(&self) -> Option<i64> {
        self.at.map(|(_, v)| v)
    }
}

/// The value with the largest timestamp; among equal timestamps, the one pushed last.
/// Empty window: `None`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct LastAcc {
    at: Option<(u64, i64)>,
}

impl LastAcc {
    pub(crate) const MERGE: Mergeability = Mergeability::OrderedOnly;
    pub(crate) fn push(&mut self, timestamp_ms: u64, value: i64) {
        if self.at.map_or(true, |(t, _)| timestamp_ms >= t) {
            self.at = Some((timestamp_ms, value));
        }
    }
    /// `other` must hold the points that arrived after `self`'s.
    pub(crate) fn merge(&mut self, other: &LastAcc) {
        if let Some((t, v)) = other.at {
            self.push(t, v);
        }
    }
    pub(crate) fn finish(&self) -> Option<i64> {
        self.at.map(|(_, v)| v)
    }
}

/// Arithmetic mean from an exact 128-bit sum and a count, so partial means merge exactly (the
/// division happens once, at the end). Empty window: `None`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MeanAcc {
    sum: SumAcc,
    n: u64,
}

impl MeanAcc {
    pub(crate) const MERGE: Mergeability = Mergeability::Commutative;
    pub(crate) fn push(&mut self, value: i64) {
        self.sum.push(value);
        self.n = self.n.saturating_add(1);
    }
    pub(crate) fn merge(&mut self, other: &MeanAcc) {
        self.sum.merge(&other.sum);
        self.n = self.n.saturating_add(other.n);
    }
    pub(crate) fn finish(&self) -> Option<f64> {
        (self.n > 0).then(|| self.sum.exact() as f64 / self.n as f64)
    }
}

/// Count, mean and sum of squared deviations by Welford's update; two partial states combine by
/// the parallel form (Chan et al.), so a variance over a window equals the variance of its parts
/// merged. Values are taken as `f64`, so integers beyond 2^53 lose their low bits.
///
/// Empty window: `None` for every output. A single point has population variance `0` and no
/// sample variance (`None`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct MomentsAcc {
    n: u64,
    mean: f64,
    m2: f64,
}

impl MomentsAcc {
    pub(crate) const MERGE: Mergeability = Mergeability::Commutative;
    pub(crate) fn push(&mut self, value: i64) {
        let x = value as f64;
        self.n += 1;
        let delta = x - self.mean;
        self.mean += delta / self.n as f64;
        self.m2 += delta * (x - self.mean);
    }
    pub(crate) fn merge(&mut self, other: &MomentsAcc) {
        if other.n == 0 {
            return;
        }
        if self.n == 0 {
            *self = *other;
            return;
        }
        let (na, nb) = (self.n as f64, other.n as f64);
        let n = na + nb;
        let delta = other.mean - self.mean;
        self.mean += delta * nb / n;
        self.m2 += other.m2 + delta * delta * na * nb / n;
        self.n += other.n;
    }
    pub(crate) fn mean(&self) -> Option<f64> {
        (self.n > 0).then_some(self.mean)
    }
    pub(crate) fn variance(&self, sample: bool) -> Option<f64> {
        let divisor = if sample { self.n.checked_sub(1)? } else { self.n };
        (divisor > 0).then(|| (self.m2 / divisor as f64).max(0.0))
    }
    pub(crate) fn std_dev(&self, sample: bool) -> Option<f64> {
        self.variance(sample).map(f64::sqrt)
    }
}

/// Why a parameter was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ParamError(pub(crate) String);

/// Sum of `value * 0.5^(age / half_life)` where `age = as_of - timestamp`, evaluated at an
/// explicit `as_of_ms` so the result does not depend on when the read runs. A point newer than
/// `as_of_ms` has age `0` (weight 1). Because `as_of_ms` is fixed, partial results merge in any
/// order (up to floating-point association; exact when the weights are dyadic). Empty window: `0`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct DecayedSumAcc {
    half_life_ms: u64,
    as_of_ms: u64,
    total: f64,
}

impl DecayedSumAcc {
    pub(crate) const MERGE: Mergeability = Mergeability::Commutative;
    /// Refuses a zero half-life.
    pub(crate) fn new(half_life_ms: u64, as_of_ms: u64) -> Result<Self, ParamError> {
        if half_life_ms == 0 {
            return Err(ParamError("half_life_ms must be greater than 0".to_string()));
        }
        Ok(Self { half_life_ms, as_of_ms, total: 0.0 })
    }
    pub(crate) fn push(&mut self, timestamp_ms: u64, value: i64) {
        let age = self.as_of_ms.saturating_sub(timestamp_ms) as f64;
        let weight = (-age / self.half_life_ms as f64).exp2();
        self.total += value as f64 * weight;
    }
    /// Both sides must share one half-life and one `as_of_ms`; a mismatch is refused.
    pub(crate) fn merge(&mut self, other: &DecayedSumAcc) -> Result<(), ParamError> {
        if self.half_life_ms != other.half_life_ms || self.as_of_ms != other.as_of_ms {
            return Err(ParamError(
                "decayed sums with a different half_life_ms or as_of_ms cannot be merged".to_string(),
            ));
        }
        self.total += other.total;
        Ok(())
    }
    pub(crate) fn finish_f64(&self) -> f64 {
        self.total
    }
    pub(crate) fn finish(&self, mode: Rounding) -> i64 {
        round_to_i64(self.total, mode)
    }
}

/// Exponentially weighted moving average: the first value seeds the state, then
/// `state = alpha * value + (1 - alpha) * state`. The result depends on the order points are
/// pushed (ascending timestamp is the caller's job) and there is no merge. Empty window: `None`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct EwmaAcc {
    alpha: f64,
    state: Option<f64>,
}

impl EwmaAcc {
    pub(crate) const MERGE: Mergeability = Mergeability::No;
    /// Refuses an `alpha` outside `(0, 1]`, NaN included.
    pub(crate) fn new(alpha: f64) -> Result<Self, ParamError> {
        if !(alpha > 0.0 && alpha <= 1.0) {
            return Err(ParamError(format!("alpha must be in (0, 1], got {alpha}")));
        }
        Ok(Self { alpha, state: None })
    }
    pub(crate) fn push(&mut self, value: i64) {
        let x = value as f64;
        self.state = Some(match self.state {
            None => x,
            Some(s) => self.alpha * x + (1.0 - self.alpha) * s,
        });
    }
    pub(crate) fn finish(&self) -> Option<f64> {
        self.state
    }
}

/// `total` per second over the inclusive window `[start_ms, end_ms]` (the engine's windows include
/// both ends, so the window is `end_ms - start_ms + 1` milliseconds long). `None` when
/// `start_ms > end_ms`.
pub(crate) fn rate_per_second(total: f64, start_ms: u64, end_ms: u64) -> Option<f64> {
    if start_ms > end_ms {
        return None;
    }
    let span_ms = (end_ms - start_ms) as f64 + 1.0;
    Some(total / (span_ms / 1000.0))
}

/// `numerator / denominator`; `None` when the denominator is `0`.
pub(crate) fn ratio(numerator: i64, denominator: i64) -> Option<f64> {
    (denominator != 0).then(|| numerator as f64 / denominator as f64)
}

/// Lower bound of the Wilson score interval for `successes` out of `trials` at `z` standard
/// deviations (1.96 is 95%). The result is in `[0, 1]`.
///
/// `None` for zero trials, for more successes than trials, and for a `z` that is negative or not
/// finite. `z == 0` returns the plain proportion.
pub(crate) fn wilson_lower_bound(successes: u64, trials: u64, z: f64) -> Option<f64> {
    if trials == 0 || successes > trials || !z.is_finite() || z < 0.0 {
        return None;
    }
    let n = trials as f64;
    let p = successes as f64 / n;
    let z2 = z * z;
    let centre = p + z2 / (2.0 * n);
    let margin = z * (p * (1.0 - p) / n + z2 / (4.0 * n * n)).sqrt();
    let lower = (centre - margin) / (1.0 + z2 / n);
    Some(lower.clamp(0.0, 1.0))
}

/// `sum(lane[i] * weight[i])`. `None` when the lengths differ, a weight is not finite, or the
/// result is not finite. Both empty: `Some(0.0)`.
pub(crate) fn weighted_sum(lanes: &[i64], weights: &[f64]) -> Option<f64> {
    if lanes.len() != weights.len() || weights.iter().any(|w| !w.is_finite()) {
        return None;
    }
    let total: f64 = lanes.iter().zip(weights).map(|(l, w)| *l as f64 * w).sum();
    total.is_finite().then_some(total)
}

/// A reduction chosen by name or by typed parameters. [`Reducer::parse`] covers the names that
/// need no parameters; the parameterised ones are built by the caller.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Reducer {
    Sum,
    Max,
    Min,
    First,
    Last,
    Count,
    Mean,
    Variance { sample: bool },
    StdDev { sample: bool },
    DecayedSum { half_life_ms: u64, as_of_ms: u64, rounding: Rounding },
    Ewma { alpha: f64 },
}

/// What a reduction produced.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Reduced {
    Int(i64),
    Float(f64),
}

impl Reduced {
    pub(crate) fn as_f64(self) -> f64 {
        match self {
            Reduced::Int(v) => v as f64,
            Reduced::Float(v) => v,
        }
    }
}

impl Reducer {
    /// The parameterless reducers by name, ASCII case-insensitive. An unknown name is refused,
    /// naming it.
    pub(crate) fn parse(name: &str) -> Result<Reducer, ParamError> {
        const TABLE: [(&str, Reducer); 12] = [
            ("sum", Reducer::Sum),
            ("max", Reducer::Max),
            ("min", Reducer::Min),
            ("first", Reducer::First),
            ("last", Reducer::Last),
            ("count", Reducer::Count),
            ("mean", Reducer::Mean),
            ("avg", Reducer::Mean),
            ("variance", Reducer::Variance { sample: false }),
            ("var_samp", Reducer::Variance { sample: true }),
            ("stddev", Reducer::StdDev { sample: false }),
            ("stddev_samp", Reducer::StdDev { sample: true }),
        ];
        TABLE
            .iter()
            .find(|(known, _)| known.eq_ignore_ascii_case(name))
            .map(|(_, reducer)| *reducer)
            .ok_or_else(|| ParamError(format!("unknown reducer `{name}`")))
    }

    /// Whether partial results of this reducer can be merged.
    pub(crate) fn mergeability(&self) -> Mergeability {
        match self {
            Reducer::Sum | Reducer::Max | Reducer::Min | Reducer::Count | Reducer::Mean => {
                Mergeability::Commutative
            }
            Reducer::Variance { .. } | Reducer::StdDev { .. } | Reducer::DecayedSum { .. } => {
                Mergeability::Commutative
            }
            Reducer::First | Reducer::Last => Mergeability::OrderedOnly,
            Reducer::Ewma { .. } => Mergeability::No,
        }
    }

    /// Reduce `(timestamp_ms, value)` points in the order given (ascending timestamp for the
    /// order-dependent reducers). `None` for an empty window where the reducer has no answer
    /// (`Sum` and `Count` answer `0`, `DecayedSum` answers `0`). Does not allocate.
    pub(crate) fn reduce(
        &self,
        points: impl Iterator<Item = (u64, i64)>,
    ) -> Result<Option<Reduced>, ParamError> {
        Ok(match *self {
            Reducer::Sum => {
                let mut acc = SumAcc::default();
                points.for_each(|(_, v)| acc.push(v));
                Some(Reduced::Int(acc.finish()))
            }
            Reducer::Count => {
                let mut acc = CountAcc::default();
                points.for_each(|(_, v)| acc.push(v));
                Some(Reduced::Int(acc.finish()))
            }
            Reducer::Max => {
                let mut acc = MaxAcc::default();
                points.for_each(|(_, v)| acc.push(v));
                acc.finish().map(Reduced::Int)
            }
            Reducer::Min => {
                let mut acc = MinAcc::default();
                points.for_each(|(_, v)| acc.push(v));
                acc.finish().map(Reduced::Int)
            }
            Reducer::First => {
                let mut acc = FirstAcc::default();
                points.for_each(|(t, v)| acc.push(t, v));
                acc.finish().map(Reduced::Int)
            }
            Reducer::Last => {
                let mut acc = LastAcc::default();
                points.for_each(|(t, v)| acc.push(t, v));
                acc.finish().map(Reduced::Int)
            }
            Reducer::Mean => {
                let mut acc = MeanAcc::default();
                points.for_each(|(_, v)| acc.push(v));
                acc.finish().map(Reduced::Float)
            }
            Reducer::Variance { sample } => {
                let mut acc = MomentsAcc::default();
                points.for_each(|(_, v)| acc.push(v));
                acc.variance(sample).map(Reduced::Float)
            }
            Reducer::StdDev { sample } => {
                let mut acc = MomentsAcc::default();
                points.for_each(|(_, v)| acc.push(v));
                acc.std_dev(sample).map(Reduced::Float)
            }
            Reducer::DecayedSum { half_life_ms, as_of_ms, rounding } => {
                let mut acc = DecayedSumAcc::new(half_life_ms, as_of_ms)?;
                points.for_each(|(t, v)| acc.push(t, v));
                Some(Reduced::Int(acc.finish(rounding)))
            }
            Reducer::Ewma { alpha } => {
                let mut acc = EwmaAcc::new(alpha)?;
                points.for_each(|(_, v)| acc.push(v));
                acc.finish().map(Reduced::Float)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The two functions below are the bodies this module replaced, copied verbatim and kept as the
    // oracle: every answer of the shared door must equal them on every input.
    fn legacy_parse(bytes: &Vec<u8>) -> Option<i64> {
        std::str::from_utf8(bytes)
            .ok()?
            .trim_start_matches(|c: char| c.is_ascii_whitespace())
            .parse()
            .ok()
    }

    fn oracle_feature(values: &[Vec<u8>], aggregator: &str) -> i64 {
        match aggregator.to_ascii_lowercase().as_str() {
            "sum" => values.iter().filter_map(legacy_parse).sum(),
            "avg" | "average" => {
                let numeric = values.iter().filter_map(legacy_parse).collect::<Vec<_>>();
                if numeric.is_empty() {
                    0
                } else {
                    numeric.iter().sum::<i64>() / numeric.len() as i64
                }
            }
            "min" => values.iter().filter_map(legacy_parse).min().unwrap_or_default(),
            "max" => values.iter().filter_map(legacy_parse).max().unwrap_or_default(),
            "first" => values.first().and_then(legacy_parse).unwrap_or_default(),
            "last" => values.last().and_then(legacy_parse).unwrap_or_default(),
            "count" | "events" | "" => values.len() as i64,
            _ => values.len() as i64,
        }
    }

    fn oracle_counter(values: &[i64], aggregator: &str) -> i64 {
        match aggregator.to_ascii_lowercase().as_str() {
            "sum" | "count" | "" => values.iter().sum(),
            "events" | "len" => values.len() as i64,
            "min" => values.iter().copied().min().unwrap_or_default(),
            "max" => values.iter().copied().max().unwrap_or_default(),
            "first" => values.first().copied().unwrap_or_default(),
            "last" => values.last().copied().unwrap_or_default(),
            _ => values.iter().sum(),
        }
    }

    fn door_feature(values: &[Vec<u8>], aggregator: &str) -> i64 {
        feature_legacy(
            aggregator,
            values.iter().map(legacy_parse),
            values.len(),
        )
    }

    const NAMES: [&str; 16] = [
        "sum", "SUM", "Avg", "average", "min", "MAX", "first", "Last", "count", "events", "len",
        "", "p99", "median", "sum ", "rate",
    ];

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    fn sample_bytes(rng: &mut Rng) -> Vec<u8> {
        match rng.next() % 6 {
            0 => b"not a number".to_vec(),
            1 => vec![0xff, 0xfe],
            2 => format!("  {}", (rng.next() % 1000) as i64 - 500).into_bytes(),
            3 => Vec::new(),
            _ => ((rng.next() % 2000) as i64 - 1000).to_string().into_bytes(),
        }
    }

    #[test]
    fn the_shared_doors_answer_what_the_bodies_they_replaced_answered() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut compared = 0usize;
        for round in 0..400 {
            let n = (rng.next() % 12) as usize; // includes the empty window
            let raw: Vec<Vec<u8>> = (0..n).map(|_| sample_bytes(&mut rng)).collect();
            let numbers: Vec<i64> = (0..n).map(|_| (rng.next() % 2000) as i64 - 1000).collect();
            for name in NAMES {
                assert_eq!(
                    door_feature(&raw, name),
                    oracle_feature(&raw, name),
                    "feature `{name}` round {round} over {raw:?}"
                );
                assert_eq!(
                    feature_stream(name, raw.iter().map(legacy_parse)),
                    door_feature(&raw, name),
                    "streaming feature `{name}` round {round} over {raw:?}"
                );
                assert_eq!(
                    counter_legacy(name, &numbers),
                    oracle_counter(&numbers, name),
                    "counter `{name}` round {round} over {numbers:?}"
                );
                compared += 2;
            }
        }
        // A control that must come out right: the loop really compared, and the two doors really
        // disagree on `count`, so a door that collapsed them would fail above.
        assert_eq!(compared, 400 * NAMES.len() * 2);
        assert_eq!(door_feature(&[b"4".to_vec(), b"x".to_vec(), b"5".to_vec()], "count"), 3);
        assert_eq!(counter_legacy("count", &[4, 5]), 9);
    }

    #[test]
    fn a_name_is_known_or_refused_and_the_table_is_case_insensitive() {
        for known in ["sum", "Sum", "AVG", "average", "min", "max", "first", "last", "count", "events", "len", ""] {
            assert!(AggName::parse(known).is_some(), "`{known}` should be known");
        }
        for unknown in ["p99", "median", " sum", "sum ", "rate", "avgg"] {
            assert_eq!(AggName::parse(unknown), None, "`{unknown}` should be refused");
        }
    }

    #[test]
    fn edge_answers_are_the_documented_ones() {
        assert_eq!(mean_truncated(std::iter::empty()), 0);
        assert_eq!(mean_truncated([7, 8].into_iter()), 7); // truncates toward zero
        assert_eq!(mean_truncated([-7, -8].into_iter()), -7);
        assert_eq!(min_or_zero(std::iter::empty()), 0);
        assert_eq!(first_or_zero([3, 4].into_iter()), 3);
        assert_eq!(last_or_zero([3, 4].into_iter()), 4);
        // `first` of the feature query is the first VALUE: an unparseable one answers 0.
        assert_eq!(door_feature(&[b"x".to_vec(), b"9".to_vec()], "first"), 0);
    }
}

#[cfg(test)]
mod algorithm_tests {
    use super::*;

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn value(&mut self) -> i64 {
            (self.next() % 2001) as i64 - 1000
        }
    }

    fn close(a: f64, b: f64, tolerance: f64) -> bool {
        (a - b).abs() <= tolerance * (1.0 + a.abs().max(b.abs()))
    }

    /// The classic eight-point sample: mean 5, population variance 4, population stddev 2, sample
    /// variance 32/7. A value only a variance can produce.
    const SAMPLE: [i64; 8] = [2, 4, 4, 4, 5, 5, 7, 9];

    fn points(values: &[i64]) -> impl Iterator<Item = (u64, i64)> + '_ {
        values.iter().enumerate().map(|(i, v)| (i as u64 * 1000, *v))
    }

    fn int(reduced: Option<Reduced>) -> i64 {
        match reduced {
            Some(Reduced::Int(v)) => v,
            other => panic!("expected an integer answer, got {other:?}"),
        }
    }

    fn float(reduced: Option<Reduced>) -> f64 {
        match reduced {
            Some(Reduced::Float(v)) => v,
            other => panic!("expected a float answer, got {other:?}"),
        }
    }

    #[test]
    fn each_reducer_answers_the_value_only_that_reducer_produces() {
        let run = |r: Reducer| r.reduce(points(&SAMPLE)).expect("valid parameters");
        assert_eq!(int(run(Reducer::Sum)), 40);
        assert_eq!(int(run(Reducer::Count)), 8);
        assert_eq!(int(run(Reducer::Max)), 9);
        assert_eq!(int(run(Reducer::Min)), 2);
        assert_eq!(int(run(Reducer::First)), 2);
        assert_eq!(int(run(Reducer::Last)), 9);
        assert_eq!(float(run(Reducer::Mean)), 5.0);
        assert!(close(float(run(Reducer::Variance { sample: false })), 4.0, 1e-12));
        assert!(close(float(run(Reducer::StdDev { sample: false })), 2.0, 1e-12));
        assert!(close(float(run(Reducer::Variance { sample: true })), 32.0 / 7.0, 1e-12));
        // Decay: four 8s one half-life apart, evaluated at the newest: 8 * (1 + 1/2 + 1/4 + 1/8).
        let eights = [8i64; 4];
        let decayed = Reducer::DecayedSum { half_life_ms: 1000, as_of_ms: 3000, rounding: Rounding::Nearest };
        assert_eq!(int(decayed.reduce(points(&eights)).unwrap()), 15);
        // EWMA at alpha 0.5 over 8, 0, 0 is 2 (the mean is 2.67, the last value is 0).
        let ewma = Reducer::Ewma { alpha: 0.5 };
        assert_eq!(float(ewma.reduce(points(&[8, 0, 0])).unwrap()), 2.0);
    }

    #[test]
    fn the_three_roundings_differ_on_a_fraction() {
        // 8 * 2^-0.5 = 5.656..., and its negation.
        let at = |mode, v| {
            let mut acc = DecayedSumAcc::new(2000, 1000).unwrap();
            acc.push(0, v);
            acc.finish(mode)
        };
        assert_eq!((at(Rounding::Floor, 8), at(Rounding::Ceil, 8), at(Rounding::Nearest, 8)), (5, 6, 6));
        assert_eq!((at(Rounding::Floor, -8), at(Rounding::Ceil, -8), at(Rounding::Nearest, -8)), (-6, -5, -6));
        assert_eq!(round_to_i64(2.5, Rounding::Nearest), 3);
        assert_eq!(round_to_i64(f64::NAN, Rounding::Floor), 0);
        assert_eq!(round_to_i64(1e30, Rounding::Floor), i64::MAX);
        assert_eq!(round_to_i64(-1e30, Rounding::Ceil), i64::MIN);
    }

    #[test]
    fn an_empty_window_answers_what_the_notes_say() {
        let none = std::iter::empty::<(u64, i64)>;
        assert_eq!(Reducer::Sum.reduce(none()).unwrap(), Some(Reduced::Int(0)));
        assert_eq!(Reducer::Count.reduce(none()).unwrap(), Some(Reduced::Int(0)));
        for reducer in [
            Reducer::Max,
            Reducer::Min,
            Reducer::First,
            Reducer::Last,
            Reducer::Mean,
            Reducer::Variance { sample: false },
            Reducer::StdDev { sample: true },
            Reducer::Ewma { alpha: 0.5 },
        ] {
            assert_eq!(reducer.reduce(none()).unwrap(), None, "{reducer:?}");
        }
        let decayed = Reducer::DecayedSum { half_life_ms: 10, as_of_ms: 0, rounding: Rounding::Floor };
        assert_eq!(decayed.reduce(none()).unwrap(), Some(Reduced::Int(0)));
        // One point: population variance 0, sample variance has no answer.
        let one = [(0u64, 7i64)];
        assert_eq!(Reducer::Variance { sample: false }.reduce(one.into_iter()).unwrap(), Some(Reduced::Float(0.0)));
        assert_eq!(Reducer::Variance { sample: true }.reduce(one.into_iter()).unwrap(), None);
    }

    #[test]
    fn overflow_saturates_and_a_sum_is_exact_until_it_is_read() {
        let mut acc = SumAcc::default();
        acc.push(i64::MAX);
        acc.push(i64::MAX);
        assert_eq!(acc.finish(), i64::MAX);
        let mut low = SumAcc::default();
        low.push(i64::MIN);
        low.push(i64::MIN);
        assert_eq!(low.finish(), i64::MIN);
        // MAX + MAX - MAX is MAX in a wide accumulator. A saturating running sum would answer 0
        // (MAX + MAX saturates to MAX, and MAX - MAX is 0): only the wide path can give MAX here.
        let mut wide = SumAcc::default();
        for v in [i64::MAX, i64::MAX, -i64::MAX] {
            wide.push(v);
        }
        assert_eq!(wide.finish(), i64::MAX);
        let mut count = CountAcc { n: u64::MAX };
        count.push(0);
        assert_eq!(count.finish(), i64::MAX);
    }

    #[test]
    fn ratio_wilson_rate_and_weights_edge_cases() {
        assert_eq!(ratio(3, 4), Some(0.75));
        assert_eq!(ratio(3, 0), None);
        // 10 of 10 at 95%: 0.72246 (n / (n + z^2)) * (1 + z^2/2n - z * sqrt(z^2/4n^2)).
        let w = wilson_lower_bound(10, 10, 1.96).unwrap();
        assert!((w - 0.72246).abs() < 1e-5, "{w}");
        // 0 of 10 cannot go below 0; z = 0 is the plain proportion.
        assert!(wilson_lower_bound(0, 10, 1.96).unwrap() < 1e-12);
        assert_eq!(wilson_lower_bound(3, 10, 0.0), Some(0.3));
        // More successes than trials, zero trials, a negative or non-finite z: refused.
        assert_eq!(wilson_lower_bound(11, 10, 1.96), None);
        assert_eq!(wilson_lower_bound(0, 0, 1.96), None);
        assert_eq!(wilson_lower_bound(1, 2, -1.0), None);
        assert_eq!(wilson_lower_bound(1, 2, f64::NAN), None);
        // More evidence at the same proportion ranks higher.
        assert!(wilson_lower_bound(90, 100, 1.96).unwrap() > wilson_lower_bound(9, 10, 1.96).unwrap());
        // 600 events over the inclusive window [0, 59_999] is a 60 s window: 10 per second.
        assert_eq!(rate_per_second(600.0, 0, 59_999), Some(10.0));
        assert_eq!(rate_per_second(1.0, 5, 4), None);
        assert!(close(rate_per_second(1.0, 7, 7).unwrap(), 1000.0, 1e-9)); // a one-millisecond window
        assert_eq!(weighted_sum(&[2, 3], &[0.5, 2.0]), Some(7.0));
        assert_eq!(weighted_sum(&[], &[]), Some(0.0));
        assert_eq!(weighted_sum(&[1, 2], &[1.0]), None);
        assert_eq!(weighted_sum(&[1], &[f64::NAN]), None);
        assert_eq!(weighted_sum(&[i64::MAX], &[f64::MAX]), None);
    }

    #[test]
    fn bad_parameters_and_names_are_refused_naming_the_field() {
        for alpha in [0.0, -0.1, 1.5, f64::NAN, f64::INFINITY] {
            assert!(EwmaAcc::new(alpha).unwrap_err().0.contains("alpha"), "{alpha}");
        }
        assert!(EwmaAcc::new(1.0).is_ok());
        assert!(DecayedSumAcc::new(0, 5).unwrap_err().0.contains("half_life_ms"));
        assert!(DecayedSumAcc::new(1, 5).is_ok());
        let err = Reducer::parse("p99").unwrap_err();
        assert!(err.0.contains("p99"), "{err:?}");
        assert!(Reducer::parse("MEAN").is_ok());
        let mut a = DecayedSumAcc::new(10, 100).unwrap();
        assert!(a.merge(&DecayedSumAcc::new(11, 100).unwrap()).unwrap_err().0.contains("half_life_ms"));
        assert!(a.merge(&DecayedSumAcc::new(10, 101).unwrap()).is_err());
        let bad = Reducer::Ewma { alpha: 2.0 };
        assert!(bad.reduce(std::iter::empty()).is_err());
    }

    #[test]
    fn mergeability_is_declared_and_matches_what_merge_can_do() {
        use Mergeability::*;
        assert_eq!(Reducer::Sum.mergeability(), Commutative);
        assert_eq!(Reducer::Mean.mergeability(), Commutative);
        assert_eq!(Reducer::Variance { sample: true }.mergeability(), Commutative);
        assert_eq!(Reducer::First.mergeability(), OrderedOnly);
        assert_eq!(Reducer::Last.mergeability(), OrderedOnly);
        assert_eq!(Reducer::Ewma { alpha: 0.3 }.mergeability(), No);
        assert_eq!(EwmaAcc::MERGE, No);
        assert_eq!(FirstAcc::MERGE, OrderedOnly);
        assert_eq!(SumAcc::MERGE, Commutative);
    }

    /// Merging two partial results equals reducing the concatenation, for every split of every
    /// random window, for each mergeable reducer.
    #[test]
    fn merging_partial_results_equals_reducing_the_concatenation() {
        let mut rng = Rng(0xD1B5_4A32_D192_ED03);
        let mut splits = 0usize;
        for _ in 0..300 {
            let n = (rng.next() % 14) as usize; // includes the empty window
            // Non-decreasing timestamps with deliberate ties.
            let mut t = 0u64;
            let pts: Vec<(u64, i64)> = (0..n)
                .map(|_| {
                    t += rng.next() % 3 * 500;
                    (t, rng.value())
                })
                .collect();
            for cut in 0..=n {
                splits += 1;
                let (left, right) = pts.split_at(cut);

                let (mut sa, mut sb, mut sw) = (SumAcc::default(), SumAcc::default(), SumAcc::default());
                let (mut ca, mut cb, mut cw) = (CountAcc::default(), CountAcc::default(), CountAcc::default());
                let (mut xa, mut xb, mut xw) = (MaxAcc::default(), MaxAcc::default(), MaxAcc::default());
                let (mut na, mut nb, mut nw) = (MinAcc::default(), MinAcc::default(), MinAcc::default());
                let (mut ma, mut mb, mut mw) = (MeanAcc::default(), MeanAcc::default(), MeanAcc::default());
                let (mut va, mut vb, mut vw) = (MomentsAcc::default(), MomentsAcc::default(), MomentsAcc::default());
                let (mut fa, mut fb, mut fw) = (FirstAcc::default(), FirstAcc::default(), FirstAcc::default());
                let (mut la, mut lb, mut lw) = (LastAcc::default(), LastAcc::default(), LastAcc::default());
                let mk = || DecayedSumAcc::new(1000, 20_000).unwrap();
                let (mut da, mut db, mut dw) = (mk(), mk(), mk());
                for (side, slice) in [(0, left), (1, right)] {
                    for &(ts, v) in slice {
                        if side == 0 {
                            sa.push(v); ca.push(v); xa.push(v); na.push(v); ma.push(v); va.push(v);
                            fa.push(ts, v); la.push(ts, v); da.push(ts, v);
                        } else {
                            sb.push(v); cb.push(v); xb.push(v); nb.push(v); mb.push(v); vb.push(v);
                            fb.push(ts, v); lb.push(ts, v); db.push(ts, v);
                        }
                    }
                }
                for &(ts, v) in &pts {
                    sw.push(v); cw.push(v); xw.push(v); nw.push(v); mw.push(v); vw.push(v);
                    fw.push(ts, v); lw.push(ts, v); dw.push(ts, v);
                }
                sa.merge(&sb);
                ca.merge(&cb);
                xa.merge(&xb);
                na.merge(&nb);
                ma.merge(&mb);
                va.merge(&vb);
                fa.merge(&fb);
                la.merge(&lb);
                da.merge(&db).unwrap();
                assert_eq!(sa, sw, "sum cut {cut} of {pts:?}");
                assert_eq!(ca, cw, "count");
                assert_eq!(xa, xw, "max");
                assert_eq!(na, nw, "min");
                assert_eq!(ma, mw, "mean");
                assert_eq!(fa, fw, "first, cut {cut} of {pts:?}");
                assert_eq!(la, lw, "last, cut {cut} of {pts:?}");
                assert!(close(da.finish_f64(), dw.finish_f64(), 1e-9), "decayed sum");
                match (va.variance(false), vw.variance(false)) {
                    (Some(a), Some(w)) => assert!(close(a, w, 1e-9), "variance {a} vs {w}"),
                    (a, w) => assert_eq!(a, w),
                }
                assert_eq!(va.variance(true).is_some(), vw.variance(true).is_some());
            }
        }
        // The loop really ran, and the control: a merge that ignored one side must be caught.
        assert!(splits > 1000, "{splits}");
        let mut lone = SumAcc::default();
        lone.push(5);
        let mut other = SumAcc::default();
        other.push(7);
        assert_ne!(lone, SumAcc::default(), "control");
        lone.merge(&other);
        assert_eq!(lone.finish(), 12);
    }

    /// `First` and `Last` need order: merged in the WRONG order they disagree with the
    /// concatenation, which is what `OrderedOnly` documents.
    #[test]
    fn first_and_last_merge_only_in_stream_order() {
        let (mut early, mut late) = (LastAcc::default(), LastAcc::default());
        early.push(1, 10);
        late.push(2, 20);
        let mut right_way = early;
        right_way.merge(&late);
        assert_eq!(right_way.finish(), Some(20));
        // Equal timestamps: the later-pushed value wins for Last and the earlier-pushed for First.
        let (mut a, mut b) = (LastAcc::default(), LastAcc::default());
        a.push(5, 1);
        b.push(5, 2);
        a.merge(&b);
        assert_eq!(a.finish(), Some(2));
        let (mut c, mut d) = (FirstAcc::default(), FirstAcc::default());
        c.push(5, 1);
        d.push(5, 2);
        c.merge(&d);
        assert_eq!(c.finish(), Some(1));
        // Merged backwards, the tie goes the other way.
        let (mut e, mut f) = (LastAcc::default(), LastAcc::default());
        e.push(5, 1);
        f.push(5, 2);
        f.merge(&e);
        assert_eq!(f.finish(), Some(1));
    }

    #[test]
    fn parsed_reducer_names_map_to_the_declared_reducers() {
        assert_eq!(Reducer::parse("Sum").unwrap(), Reducer::Sum);
        assert_eq!(Reducer::parse("avg").unwrap(), Reducer::Mean);
        assert_eq!(Reducer::parse("var_samp").unwrap(), Reducer::Variance { sample: true });
        assert_eq!(Reducer::parse("stddev_samp").unwrap(), Reducer::StdDev { sample: true });
        assert!(Reducer::parse("").is_err());
        assert!(Reducer::parse("sum ").is_err());
    }
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    fn series(n: usize) -> Vec<(u64, i64)> {
        (0..n).map(|i| (i as u64 * 10, (i as i64 % 97) - 40)).collect()
    }

    fn allocs_of(points: &[(u64, i64)], reducer: Reducer) -> u64 {
        let probe = Probe::start();
        let answer = reducer.reduce(points.iter().copied()).expect("valid parameters");
        let spent = probe.stop().allocs;
        assert!(answer.is_some() || points.is_empty());
        spent as u64
    }

    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn the_reducers_do_not_allocate_per_point() {
        let small = series(100);
        let large = series(20_000);

        // THE CONTROL, FIRST: collecting a window grows with its length, so a probe that reads
        // zero for the reducers is reading the reducers and not a blind counter.
        let collect = |points: &[(u64, i64)]| {
            let probe = Probe::start();
            let copy: Vec<(u64, i64)> = points.iter().copied().collect();
            let spent = probe.stop().allocs;
            assert_eq!(copy.len(), points.len());
            spent as u64
        };
        assert!(collect(&large) >= 1, "the control did not see its own allocation");

        for reducer in [
            Reducer::Sum,
            Reducer::Count,
            Reducer::Max,
            Reducer::Min,
            Reducer::First,
            Reducer::Last,
            Reducer::Mean,
            Reducer::Variance { sample: true },
            Reducer::StdDev { sample: false },
            Reducer::DecayedSum { half_life_ms: 500, as_of_ms: 200_000, rounding: Rounding::Nearest },
            Reducer::Ewma { alpha: 0.25 },
        ] {
            let at_small = allocs_of(&small, reducer);
            let at_large = allocs_of(&large, reducer);
            assert_eq!(at_small, 0, "{reducer:?} allocated over 100 points");
            assert_eq!(at_large, 0, "{reducer:?} allocated over 20000 points");
        }
    }
}
