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

/// How many steps the middle loop below would take over `[mid_lo, mid_hi)`.
///
/// The loop climbs to the coarsest aligned level and descends again, so the count is the span in
/// coarsest buckets plus the bounded climb at each end: at most 59 one-second steps to reach a
/// minute boundary, 59 one-minute steps to reach an hour, and 23 one-hour steps to reach a day,
/// twice. The exact figure does not matter -- this only has to be an UPPER BOUND, because it is
/// compared against the cost of the alternative and both answers are exact.
fn tiling_steps(mid_lo: u64, mid_hi: u64) -> u64 {
    let coarsest = LEVEL_WIDTHS_MS[LEVEL_WIDTHS_MS.len() - 1];
    (mid_hi - mid_lo) / coarsest + 2 * (59 + 59 + 23)
}

fn range_sum(cells: &BTreeMap<CellKey, i64>, series: &BTreeMap<u64, i64>, start_ms: u64, end_ms: u64) -> i64 {
    if start_ms > end_ms {
        return 0;
    }
    // CLAMP TO THE DATA BEFORE TILING. THIS IS NOT AN OPTIMISATION.
    //
    // The middle loop below walks TILING POSITIONS, not cells: it steps `cursor` by at most one
    // day from one end of the window to the other and asks the cell map at every stop. Positions
    // outside the series' own extent cannot change the ANSWER -- `cells.get` returns `None` there
    // and adds nothing -- but they do change the WALK. An unbounded window, which a client may
    // simply ask for by passing `u64::MAX` as the end, puts `mid_hi` at about 1.8e19, so the loop
    // takes on the order of 2e11 steps with a map lookup each: one query occupies the shard for
    // hours. This primitive is shared, so the control-state counter path carried it too.
    //
    // Narrowing the window to the first and last instant actually present cannot change the sum,
    // because every instant outside that span is absent from the series AND from every cell.
    let (Some((&first, _)), Some((&last, _))) = (series.iter().next(), series.iter().next_back())
    else {
        return 0;
    };
    if start_ms > last || end_ms < first {
        return 0;
    }
    let start_ms = start_ms.max(first);
    let end_ms = end_ms.min(last);
    let w0 = FINEST_WIDTH_MS;
    let mid_lo = start_ms.div_ceil(w0) * w0; // first whole-second boundary >= start
    // `end_ms + 1` is the exclusive upper bound the cells tile against. It saturates only when the
    // series really does hold a point at `u64::MAX`, and the right edge below is INCLUSIVE of
    // `end_ms` so that point is still counted -- the exclusive `..e` spelling it replaced dropped
    // it, which the clamp above makes reachable where before it needed the caller to ask for it.
    let mid_hi = (end_ms.saturating_add(1) / w0) * w0; // last whole-second boundary <= end+1
    // A RAW SCAN IS SOMETIMES THE CHEAPER EXACT ANSWER, and the clamp alone is not a bound.
    //
    // A series holding two points a century apart has a span no clamp shrinks: the tiling then
    // costs the span while a scan costs the two points. Both are exact, so take whichever is
    // smaller. This is also the only remaining defence if a single out-of-range instant ever
    // reaches the series, which would otherwise restore the whole walk above.
    if mid_lo >= mid_hi || tiling_steps(mid_lo, mid_hi) > series.len() as u64 {
        return series.range(start_ms..=end_ms).map(|(_, v)| *v).sum();
    }
    let mut sum: i64 = 0;
    sum += series.range(start_ms..mid_lo).map(|(_, v)| *v).sum::<i64>(); // left sub-second edge
    // RIGHT SUB-SECOND EDGE, INCLUSIVE AND THEREFORE GUARDED. `mid_hi` is `end_ms + 1` whenever
    // the window ends on a whole second, and `BTreeMap::range` PANICS on an inclusive range whose
    // start exceeds its end -- so the emptiness that the old exclusive `mid_hi..e` expressed by
    // construction has to be asked for here.
    if mid_hi <= end_ms {
        sum += series.range(mid_hi..=end_ms).map(|(_, v)| *v).sum::<i64>();
    }
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
    windowed_sum_series(table, series, key, start_ms, end_ms)
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
    windowed_sum_series(table, series, key, start_ms, end_ms)
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
