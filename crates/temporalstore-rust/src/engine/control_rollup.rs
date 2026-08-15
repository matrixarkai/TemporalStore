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

type CellKey = (u8, u64); // (level index, bucket_start_ms)

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

fn fold_cells(cells: &mut BTreeMap<CellKey, i64>, bucket_ms: u64, amount: i64) {
    for (level, width) in LEVEL_WIDTHS_MS.iter().enumerate() {
        *cells
            .entry((level as u8, bucket_start(bucket_ms, *width)))
            .or_default() += amount;
    }
}

fn build(series: &BTreeMap<u64, i64>) -> BTreeMap<CellKey, i64> {
    let mut cells = BTreeMap::new();
    for (bucket_ms, amount) in series {
        fold_cells(&mut cells, *bucket_ms, *amount);
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

fn range_sum(cells: &BTreeMap<CellKey, i64>, series: &BTreeMap<u64, i64>, start_ms: u64, end_ms: u64) -> i64 {
    if start_ms > end_ms {
        return 0;
    }
    let e = end_ms.saturating_add(1); // exclusive upper bound
    let w0 = FINEST_WIDTH_MS;
    let mid_lo = start_ms.div_ceil(w0) * w0; // first whole-second boundary >= start
    let mid_hi = (e / w0) * w0; // last whole-second boundary <= end+1
    if mid_lo >= mid_hi {
        // window smaller than one finest bucket: raw scan
        return series.range(start_ms..e).map(|(_, v)| *v).sum();
    }
    let mut sum: i64 = 0;
    sum += series.range(start_ms..mid_lo).map(|(_, v)| *v).sum::<i64>(); // left sub-second edge
    sum += series.range(mid_hi..e).map(|(_, v)| *v).sum::<i64>(); // right sub-second edge
    let mut cursor = mid_lo; // middle covered by coarsest cells
    while cursor < mid_hi {
        let (level, width) = coarsest_fit(cursor, mid_hi);
        if let Some(cell) = cells.get(&(level, cursor)) {
            sum += *cell;
        }
        cursor += width;
    }
    sum
}

/// Aggregators the rollup ladder can serve exactly (the sum family). "count"/"" map to
/// a windowed sum of the stored counter values in `aggregate_control_state_values`.
pub(crate) fn is_sum_family(aggregator: &str) -> bool {
    matches!(aggregator.trim().to_ascii_lowercase().as_str(), "" | "sum" | "count")
}

/// Called AFTER `shard.control_state[key]` has been updated with `amount` at `bucket_ms`.
pub(crate) fn record_increment(shard: &mut ShardState, key: &str, bucket_ms: u64, amount: i64) {
    let series_len = shard.control_state.get(key).map(|s| s.len()).unwrap_or(0);
    match shard.control_state_rollups.get_mut(key) {
        Some(entry) => {
            fold_cells(&mut entry.cells, bucket_ms, amount);
            entry.built_len = series_len;
        }
        None => {
            // First touch this session: build from the full (already-updated) series.
            let cells = shard.control_state.get(key).map(build).unwrap_or_default();
            shard
                .control_state_rollups
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
    range_sum(cells, series, start_ms, end_ms)
}

/// Sum-family windowed aggregate over the control-state counter series (O(levels)).
pub(crate) fn windowed_sum(shard: &mut ShardState, key: &str, start_ms: u64, end_ms: u64) -> i64 {
    let empty_series = BTreeMap::new();
    let series = shard.control_state.get(key).unwrap_or(&empty_series);
    windowed_sum_series(&mut shard.control_state_rollups, series, key, start_ms, end_ms)
}

/// Sum-family windowed aggregate over the feature numeric view (`feature_values`), served by
/// the same rollup primitive. Callers must have populated `feature_values[key]` first.
pub(crate) fn feature_windowed_sum(shard: &mut ShardState, key: &str, start_ms: u64, end_ms: u64) -> i64 {
    let empty_series = BTreeMap::new();
    let series = shard.feature_values.get(key).unwrap_or(&empty_series);
    windowed_sum_series(&mut shard.feature_rollups, series, key, start_ms, end_ms)
}

/// Drop a feature key's numeric view + rollup (on delete/expire).
pub(crate) fn feature_forget(shard: &mut ShardState, key: &str) {
    shard.feature_rollups.remove(key);
    shard.feature_values.remove(key);
}

/// Clear all feature numeric views + rollups (on reconcile) so they rebuild lazily.
pub(crate) fn feature_clear_all(shard: &mut ShardState) {
    shard.feature_rollups.clear();
    shard.feature_values.clear();
}

/// Drop a key's rollup index (on delete/expire) so a later re-creation rebuilds cleanly.
pub(crate) fn forget(shard: &mut ShardState, key: &str) {
    shard.control_state_rollups.remove(key);
}

/// Clear all rollup indexes (on reconcile) so they lazily rebuild from the authoritative
/// control_state series that reconcile just materialized.
pub(crate) fn clear_all(shard: &mut ShardState) {
    shard.control_state_rollups.clear();
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
            assert_eq!(range_sum(&cells, &series, s, e), brute(&series, s, e), "window {s}..={e}");
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
