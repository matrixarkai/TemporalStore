// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Counts durability barriers by the call site that takes them.
//!
//! A write's latency is roughly `barriers x fsync`, so a total barrier count per write says how
//! much there is to win but not where to go looking. Attributing them by reasoning about the code
//! has produced confident wrong answers here more than once -- two changes aimed at a suspected
//! site passed their tests and then moved nothing. Each site reports itself instead, so the split
//! is measured rather than argued.
//!
//! The counters are process-wide and monotonic. A mutex per barrier is free next to an fsync
//! (microseconds against milliseconds), so this stays on rather than hiding behind a build flag
//! that would be off exactly when someone needs the numbers.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

fn counters() -> &'static Mutex<BTreeMap<&'static str, u64>> {
    static COUNTERS: OnceLock<Mutex<BTreeMap<&'static str, u64>>> = OnceLock::new();
    COUNTERS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// Record one durability barrier taken at `site`.
///
/// Call this immediately before the `sync_data`/`sync_all` itself, not after: a barrier that
/// fails still cost the wait, and a site that reports only its successes understates itself.
pub fn record_barrier(site: &'static str) {
    if let Ok(mut counts) = counters().lock() {
        *counts.entry(site).or_insert(0) += 1;
    }
}

/// Add `count` to a named tally of work done.
///
/// Separate from `record_barrier` only in what it counts: barriers are events, this is a
/// quantity. Both share the map so a harness reads one place. Used where a cost must be asserted
/// exactly rather than timed -- on a shared machine a stopwatch measures the other tenants.
pub fn record_scan(name: &'static str, count: u64) {
    if let Ok(mut counts) = counters().lock() {
        *counts.entry(name).or_insert(0) += count;
    }
}

/// Every site's total so far, ordered by site name.
pub fn snapshot() -> BTreeMap<&'static str, u64> {
    counters()
        .lock()
        .map(|counts| counts.clone())
        .unwrap_or_default()
}

/// Drop every count. Used by a harness to bracket a measured span.
pub fn reset() {
    if let Ok(mut counts) = counters().lock() {
        counts.clear();
    }
}

/// Total barriers across all sites.
pub fn total() -> u64 {
    snapshot().values().sum()
}

/// The counts as `site=count` pairs, busiest first, for a one-line log or report.
pub fn report_line() -> String {
    let counts = snapshot();
    let mut pairs: Vec<(&'static str, u64)> = counts.into_iter().collect();
    pairs.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(right.0)));
    pairs
        .iter()
        .map(|(site, count)| format!("{site}={count}"))
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_are_attributed_per_site_and_reset_clears_them() {
        reset();
        record_barrier("alpha");
        record_barrier("alpha");
        record_barrier("beta");
        let counts = snapshot();
        assert_eq!(counts.get("alpha"), Some(&2));
        assert_eq!(counts.get("beta"), Some(&1));
        assert_eq!(total(), 3);
        // Busiest site leads the report so the dominant cost is the first thing read.
        assert!(report_line().starts_with("alpha=2"));
        reset();
        assert_eq!(total(), 0);
    }
}
