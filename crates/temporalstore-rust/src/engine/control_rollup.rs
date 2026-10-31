// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Multi-resolution rollup ladder for Control State counters.
//!
//! Long-window Control State aggregates (frequency caps / counts over hours→weeks)
//! otherwise scan every raw bucket in the window. This module maintains, per key, a
//! ladder of exact per-bucket sums at 1s → 1m → 1h → 1d, so a wide-window sum-family
//! query reads O(levels) coarse cells plus the sub-second edges instead of O(range).
//!
//! Correctness: a cell stores the exact sum of the increments folded into its span, so
//! a fully-covered window returns a value bit-identical to the raw range scan; partial
//! sub-second edges fall back to the authoritative `shard.control_state` series. The
//! index is a derived, `#[serde(skip)]` view: kept incrementally on counter writes,
//! rebuilt from the authoritative series whenever its recorded `built_len` no longer
//! matches the series length (a safety net against any un-instrumented growth), dropped
//! on delete/expire, and cleared on reconcile. Everything here is gated by
//! `Config.control_rollup_enabled()`; with the gate off nothing in this module runs and
//! the default path is byte-for-byte unchanged.

use std::collections::BTreeMap;

use super::state::ShardState;

/// Rollup bucket widths in milliseconds, finest first. Each is an integer multiple of
/// the previous (1s→1m→1h→1d) so the levels tile cleanly.
pub(crate) const LEVEL_WIDTHS_MS: [u64; 4] = [1_000, 60_000, 3_600_000, 86_400_000];
const FINEST_WIDTH_MS: u64 = LEVEL_WIDTHS_MS[0];

use super::product_model::{
    control_state_pack_bucket, control_state_bucket_start_ms, CONTROL_STATE_PRECISION_CODES,
    CONTROL_STATE_PRECISION_SHIFT,
};

type CellKey = (u64, u8, u64); // (precision code, level index, bucket_start_ms)

/// The codes a Control State read has to cover when it names no precision.
pub(crate) const ALL_PRECISIONS: &[u64] = &CONTROL_STATE_PRECISION_CODES;

/// Feature aggregates share this ladder but their series keys are plain timestamps, never
/// precision-tagged, so they live entirely in the untagged space and cover one code.
pub(crate) const UNTAGGED_ONLY: &[u64] = &[0];

/// Per-key rollup index: exact per-cell sums plus the control_state series length the
/// index was last built/folded against (staleness guard).
#[derive(Debug, Default, Clone)]
pub(crate) struct RollupEntry {
    cells: BTreeMap<CellKey, i64>,
    built_len: usize,
}

fn bucket_start(ts_ms: u64, width_ms: u64) -> u64 {
    ts_ms - ts_ms % width_ms
}

/// A cell is folded under the precision its bucket was written at.
///
/// Without the code a cell would sum buckets from DIFFERENT precisions -- the 1h cell covering
/// 09:00 would hold both the per-minute writes in that hour and the per-hour write filed at 09:00
/// -- and a precision-scoped read off that cell would return the other precision's traffic too.
/// The arithmetic also has to be done on the bucket's START: the key's high bits are its code, and
/// `packed % width` is not a time at all.
fn fold_cells(cells: &mut BTreeMap<CellKey, i64>, bucket_key: u64, amount: i64) {
    let code = bucket_key >> CONTROL_STATE_PRECISION_SHIFT;
    let start_ms = control_state_bucket_start_ms(bucket_key);
    for (level, width) in LEVEL_WIDTHS_MS.iter().enumerate() {
        *cells
            .entry((code, level as u8, bucket_start(start_ms, *width)))
            .or_default() += amount;
    }
}

fn build(series: &BTreeMap<u64, i64>) -> BTreeMap<CellKey, i64> {
    let mut cells = BTreeMap::new();
    for (bucket_key, amount) in series {
        fold_cells(&mut cells, *bucket_key, *amount);
    }
    cells
}

fn coarsest_fit(cursor: u64, hi: u64) -> (u8, u64) {
    let mut chosen = (0u8, FINEST_WIDTH_MS);
    for (level, width) in LEVEL_WIDTHS_MS.iter().enumerate() {
        if cursor % width == 0 && cursor + width <= hi {
            chosen = (level as u8, *width);
        }
    }
    chosen
}

/// The window's sum at ONE precision: coarse cells over the middle, raw buckets at the
/// sub-second edges, all of it confined to `code`'s own key space.
fn range_sum_at(
    cells: &BTreeMap<CellKey, i64>,
    series: &BTreeMap<u64, i64>,
    code: u64,
    start_ms: u64,
    end_ms: u64,
) -> i64 {
    if start_ms > end_ms {
        return 0;
    }
    let e = end_ms.saturating_add(1); // exclusive upper bound
    let w0 = FINEST_WIDTH_MS;
    let mid_lo = start_ms.div_ceil(w0) * w0; // first whole-second boundary >= start
    let mid_hi = (e / w0) * w0; // last whole-second boundary <= end+1
    // `[lo, hi)` of bucket STARTS, read inside this precision's keys.
    let raw = |lo: u64, hi: u64| -> i64 {
        if lo >= hi {
            return 0;
        }
        series
            .range(control_state_pack_bucket(code, lo)..control_state_pack_bucket(code, hi))
            .map(|(_, v)| *v)
            .sum()
    };
    if mid_lo >= mid_hi {
        // window smaller than one finest bucket: raw scan
        return raw(start_ms, e);
    }
    let mut sum: i64 = raw(start_ms, mid_lo) + raw(mid_hi, e); // sub-second edges
    let mut cursor = mid_lo; // middle covered by coarsest cells
    while cursor < mid_hi {
        let (level, width) = coarsest_fit(cursor, mid_hi);
        if let Some(cell) = cells.get(&(code, level, cursor)) {
            sum += *cell;
        }
        cursor += width;
    }
    sum
}

/// The window's sum over every precision in `codes`.
///
/// A read that names no precision passes `ALL_PRECISIONS`, which is nine range reads rather than a
/// walk of the series; a key counting at a single precision finds nothing under the other eight and
/// pays `O(log n)` for each.
fn range_sum(
    cells: &BTreeMap<CellKey, i64>,
    series: &BTreeMap<u64, i64>,
    codes: &[u64],
    start_ms: u64,
    end_ms: u64,
) -> i64 {
    codes
        .iter()
        .map(|code| range_sum_at(cells, series, *code, start_ms, end_ms))
        .sum()
}

/// Aggregators the rollup ladder can serve exactly (the sum family). "count"/"" map to
/// a windowed sum of the stored counter values in `aggregate_control_state_values`.
pub(crate) fn is_sum_family(aggregator: &str) -> bool {
    matches!(aggregator.trim().to_ascii_lowercase().as_str(), "" | "sum" | "count")
}

/// Called AFTER `shard.control_state[key]` has been updated with `amount` at `bucket_ms`.
pub(crate) fn record_increment(shard: &mut ShardState, key: &str, bucket_ms: u64, amount: i64) {
    let series_len = shard.control_state().get(key).map(|s| s.len()).unwrap_or(0);
    match shard.control_state_rollups_mut().get_mut(key) {
        Some(entry) => {
            fold_cells(&mut entry.cells, bucket_ms, amount);
            entry.built_len = series_len;
        }
        None => {
            // First touch this session: build from the full (already-updated) series.
            let cells = shard.control_state().get(key).map(build).unwrap_or_default();
            shard
                .control_state_rollups_mut()
                .insert(key.to_string(), RollupEntry { cells, built_len: series_len });
        }
    }
}

/// Generic sum-family windowed aggregate over any (rollup index map, i64 series) pair.
/// Rebuilds the key's index from `series` when its recorded length no longer matches the
/// series (staleness guard), then covers the window with coarse cells + raw edges. This is
/// the one primitive shared by the control-state counter path and the feature-aggregate path.
pub(crate) fn windowed_sum_series(
    rollups: &mut std::collections::HashMap<String, RollupEntry>,
    series: &BTreeMap<u64, i64>,
    key: &str,
    codes: &[u64],
    start_ms: u64,
    end_ms: u64,
) -> i64 {
    let series_len = series.len();
    let stale = rollups
        .get(key)
        .map(|entry| entry.built_len != series_len)
        .unwrap_or(true);
    if stale {
        rollups.insert(
            key.to_string(),
            RollupEntry {
                cells: build(series),
                built_len: series_len,
            },
        );
    }
    let empty_cells = BTreeMap::new();
    let cells = rollups.get(key).map(|e| &e.cells).unwrap_or(&empty_cells);
    range_sum(cells, series, codes, start_ms, end_ms)
}

/// Sum-family windowed aggregate over the control-state counter series (O(levels)).
pub(crate) fn windowed_sum(shard: &mut ShardState, key: &str, start_ms: u64, end_ms: u64) -> i64 {
    let empty_series = BTreeMap::new();
    // TWO DISJOINT FIELD BORROWS, NOT TWO DOOR CALLS. A door takes `&mut self` and borrows the
    // whole shard, which conflicts with the `&shard.control_state` this series came from;
    // `rollups` and `control_state` are separate fields, so reaching them directly keeps the
    // disjointness the old pair of fields had. The alternative was cloning the series on a
    // read path, which is the wrong trade for a borrow problem.
    // BOTH SIDES AS FIELDS NOW. The note above applied to the rollup table; the series is
    // behind a door too since the counter maps merged, so the same disjointness argument has to
    // cover it -- `counter_series` and `rollups` are separate fields, and a door call on either
    // would borrow the whole shard and conflict with the other.
    let series = super::state::counters_ref(
        &shard.side_tables,
        super::state::SideTable::ControlStateCounters,
    )
    .and_then(|table| table.get(key))
    .unwrap_or(&empty_series);
    let table = shard
        .rollups
        .entry(super::state::RollupDomain::ControlState)
        .or_default();
    windowed_sum_series(table, series, key, ALL_PRECISIONS, start_ms, end_ms)
}

/// Sum-family windowed aggregate over the feature numeric view (`feature_values`), served by
/// the same rollup primitive. Callers must have populated `feature_values[key]` first.
pub(crate) fn feature_windowed_sum(shard: &mut ShardState, key: &str, start_ms: u64, end_ms: u64) -> i64 {
    let empty_series = BTreeMap::new();
    // Disjoint field borrows, as in `windowed_sum` above and for the same reason.
    let series = super::state::counters_ref(
        &shard.side_tables,
        super::state::SideTable::FeatureCounters,
    )
    .and_then(|table| table.get(key))
    .unwrap_or(&empty_series);
    let table = shard
        .rollups
        .entry(super::state::RollupDomain::Feature)
        .or_default();
    windowed_sum_series(table, series, key, UNTAGGED_ONLY, start_ms, end_ms)
}

/// Drop a feature key's numeric view + rollup (on delete/expire).
pub(crate) fn feature_forget(shard: &mut ShardState, key: &str) {
    shard.feature_rollups_mut().remove(key);
    shard.feature_values_mut().remove(key);
}

/// Clear all feature numeric views + rollups (on reconcile) so they rebuild lazily.
pub(crate) fn feature_clear_all(shard: &mut ShardState) {
    shard.feature_rollups_mut().clear();
    shard.feature_values_mut().clear();
}

/// Drop a key's rollup index (on delete/expire) so a later re-creation rebuilds cleanly.
pub(crate) fn forget(shard: &mut ShardState, key: &str) {
    shard.control_state_rollups_mut().remove(key);
}

/// Clear all rollup indexes (on reconcile) so they lazily rebuild from the authoritative
/// control_state series that reconcile just materialized.
pub(crate) fn clear_all(shard: &mut ShardState) {
    shard.control_state_rollups_mut().clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn brute(series: &BTreeMap<u64, i64>, start: u64, end: u64) -> i64 {
        series.range(start..=end).map(|(_, v)| *v).sum()
    }

    #[test]
    fn range_sum_matches_raw_scan_across_windows() {
        // deterministic long series across ~3 days at mixed granularities
        let mut series = BTreeMap::new();
        let mut state: u64 = 0x2545_F491_4F6C_DD1D;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            state
        };
        for _ in 0..4000 {
            let ts = next() % (3 * 86_400_000);
            let amount = (next() % 21) as i64 - 10; // -10..=10
            *series.entry(ts).or_default() += amount;
        }
        let cells = build(&series);
        let windows = [
            (0u64, 999u64),
            (0, 60_000),
            (500, 3_600_000),
            (0, 86_400_000),
            (1_000, 7 * 86_400_000),
            (37_000_000, 91_000_000),
            (0, 3 * 86_400_000),
        ];
        for (s, e) in windows {
            assert_eq!(
                range_sum(&cells, &series, ALL_PRECISIONS, s, e),
                brute(&series, s, e),
                "window {s}..={e}"
            );
        }
    }

    /// A cell belongs to ONE precision, and that is what lets a scoped read be exact.
    ///
    /// Without the code in the cell key, the 1h cell covering an hour would hold both the
    /// per-minute writes inside it and the per-hour write filed at its start, and a read scoped to
    /// either precision would be answered with the other one's traffic folded in. The ladder is
    /// where that would happen silently: the engine would be asking for one precision and the cell
    /// it got back would already have the sum of two.
    ///
    /// Two claims, because the first alone is satisfied by a ladder that simply ignores the codes:
    /// each precision's scoped sum matches a brute-force scan restricted to that precision, AND
    /// their union matches a brute-force scan over everything.
    #[test]
    fn a_cell_sums_one_precision_and_their_union_is_the_whole_series() {
        // Minute-coded and hour-coded buckets sharing instants, which is exactly the overlap that
        // used to collapse: at the top of an hour both precisions truncate to the same start.
        const MINUTE_CODE: u64 = 30;
        const HOUR_CODE: u64 = 50;
        let starts = [0u64, 3_600_000, 7_200_000, 86_400_000];

        let mut series = BTreeMap::new();
        for (index, start) in starts.iter().enumerate() {
            series.insert(
                control_state_pack_bucket(MINUTE_CODE, *start),
                (index as i64) + 1,
            );
            series.insert(
                control_state_pack_bucket(HOUR_CODE, *start),
                ((index as i64) + 1) * 100,
            );
        }
        let cells = build(&series);

        // Brute force over one precision: the buckets carrying that code whose START is in range.
        let brute_at = |code: u64, start_ms: u64, end_ms: u64| -> i64 {
            series
                .iter()
                .filter(|(key, _)| {
                    **key >> CONTROL_STATE_PRECISION_SHIFT == code
                        && (start_ms..=end_ms).contains(&control_state_bucket_start_ms(**key))
                })
                .map(|(_, value)| *value)
                .sum()
        };

        for (s, e) in [
            (0u64, 86_400_000u64),
            (0, 3_599_999),
            (3_600_000, 7_200_000),
            (1, 86_400_000),
        ] {
            for code in [MINUTE_CODE, HOUR_CODE] {
                assert_eq!(
                    brute_at(code, s, e),
                    range_sum_at(&cells, &series, code, s, e),
                    "scoped sum at code {code} over {s}..={e} must hold only that precision"
                );
            }
            // DENOMINATOR for the pair above: the two precisions carry different totals, so a
            // ladder that mixed them would have to disagree with at least one scoped answer.
            assert_ne!(
                brute_at(MINUTE_CODE, s, e),
                brute_at(HOUR_CODE, s, e),
                "the fixture has to put DIFFERENT traffic at each precision to be able to tell"
            );
            assert_eq!(
                brute_at(MINUTE_CODE, s, e) + brute_at(HOUR_CODE, s, e),
                range_sum(&cells, &series, ALL_PRECISIONS, s, e),
                "a read naming no precision answers across all of them over {s}..={e}"
            );
        }
    }

    #[test]
    fn incremental_fold_equals_full_rebuild() {
        let mut series = BTreeMap::new();
        let mut cells = BTreeMap::new();
        let increments = [(10u64, 3i64), (10, 4), (1_500, 2), (65_000, 9), (10, -1), (90_000_000, 5)];
        for (ts, amt) in increments {
            *series.entry(ts).or_default() += amt;
            fold_cells(&mut cells, ts, amt);
        }
        assert_eq!(cells, build(&series));
    }
}
