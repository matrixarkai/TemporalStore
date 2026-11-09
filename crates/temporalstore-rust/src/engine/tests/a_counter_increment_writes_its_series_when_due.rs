// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! AN INCREMENT REWRITES ITS COUNTER'S SERIES BLOCK WHEN IT IS DUE, NOT ON EVERY WRITE.
//!
//! `ControlStateIncrement` stages a `control_counter` outcome (the resulting value at one bucket,
//! idempotent when replayed) and then persisted the WHOLE series as a block, into the log record
//! and the block store, on every increment. A recovery keeps the series from the served index in
//! preference to the block-derived one and replays the outcomes, so the block bought nothing it
//! used; it cost the series' length every time. Per increment, 200 / 1,000 / 2,000 entries:
//!
//! ```text
//!   log bytes          299      1,721      3,515
//!   block-store bytes  222      1,637      3,392
//!   instructions    1.87M (200) 4.4M (800) 13.9M (3,200)       -> ~120M at the 30,000-point default cap
//! ```
//!
//! The block is now written on the first increment of a key and on a deterministic share of the
//! rest, one in `len / 16` (at least 16), so an increment costs about sixteen entries' worth
//! whatever the series holds. The other four counter arms stage no outcome and keep writing it
//! every time.
//!
//! WHAT MOVES ON DISK: an increment's log record carries the outcome and no series block, except
//! on the first increment of a key and on the due ones. Nothing is added to any format.
//!
//! THREE GUARDS, BECAUSE THEY FAIL ON DISJOINT MUTATIONS.
//!   * `a_counter_increment_writes_its_series_only_now_and_then` counts the log and block bytes an
//!     increment writes at three series lengths, against an arm with the block forced on every
//!     increment: the per-increment bytes must be flat across the lengths and a multiple below the
//!     forced arm, which must itself grow with the length, or the counters are blind.
//!   * `a_counter_series_recovers_whether_or_not_its_block_was_written` runs a history of
//!     increments (replacing repeated timestamps, then a `ControlStateSet` in the middle, which has
//!     no outcome and so must still write its block) and requires the series a recovery serves --
//!     after dropping the engine with no unload, after a compaction, a dump and a log reclaim, and
//!     after a graceful reload -- to equal an oracle, with the forced arm as the differential.
//!   * `a_counter_series_trimmed_at_its_bound_recovers_as_it_did_before` does the same with a bound
//!     small enough that every increment trims, against the forced arm, because the oracle for a
//!     trim across a recovery is what the old behaviour served.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::types::CommandResponse;
use std::collections::BTreeMap;

fn open(dir: &std::path::Path, bound: Option<usize>) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        1 << 22,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    if let Some(bound) = bound {
        assert!(
            engine
                .set_config(crate::control::SetConfigRequest {
                    shard_id: 1,
                    config: crate::control::Config {
                        version: 2,
                        feature_max_size: bound,
                        ..Default::default()
                    },
                })
                .ok,
            "the bound has to be the one under test"
        );
    }
    engine
}

fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "{:?}", response.status);
    response.response
}

fn increment(engine: &TemporalEngine, at: u64, amount: i64) {
    run(
        engine,
        Command::ControlStateIncrement {
            key: "c".to_string(),
            timestamp_ms: at,
            amount,
        },
    );
}

fn series(engine: &TemporalEngine) -> BTreeMap<u64, i64> {
    match run(
        engine,
        Command::ControlStateDetail {
            key: "c".to_string(),
            start_ms: 0,
            end_ms: u64::MAX,
            count: None,
        },
    ) {
        CommandResponse::FeaturePoints { points } => points
            .into_iter()
            .map(|point| {
                (
                    point.timestamp_ms,
                    String::from_utf8(point.value).unwrap().parse::<i64>().unwrap(),
                )
            })
            .collect(),
        other => panic!("expected FeaturePoints, got {other:?}"),
    }
}

fn with_every_block<T>(forced: bool, body: impl FnOnce() -> T) -> T {
    crate::engine::ALWAYS_WRITE_COUNTER_BLOCK.with(|flag| flag.set(forced));
    let out = body();
    crate::engine::ALWAYS_WRITE_COUNTER_BLOCK.with(|flag| flag.set(false));
    out
}

struct Rng(u64);

impl Rng {
    fn next(&mut self, bound: u64) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 33) % bound
    }
}

/// Log bytes and block-store bytes written per increment, over `writes` increments to a series that
/// already holds `entries`.
fn bytes_per_increment(entries: usize, writes: usize, forced: bool) -> (f64, f64) {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), None);
    with_every_block(forced, || {
        for index in 0..entries as u64 {
            increment(&engine, 1_000 + index, 1);
        }
        let wal = engine.wal_store.raw_stats(1).bytes_written;
        let blocks = engine.block_store().stats().bytes_written;
        for index in 0..writes as u64 {
            increment(&engine, 1_000_000 + index, 1);
        }
        (
            (engine.wal_store.raw_stats(1).bytes_written - wal) as f64 / writes as f64,
            (engine.block_store().stats().bytes_written - blocks) as f64 / writes as f64,
        )
    })
}

#[test]
fn a_counter_increment_writes_its_series_only_now_and_then() {
    const WRITES: usize = 800;
    let lengths = [200usize, 800, 3200];
    let skipped = lengths.map(|entries| bytes_per_increment(entries, WRITES, false));
    let forced = lengths.map(|entries| bytes_per_increment(entries, WRITES, true));
    for (index, entries) in lengths.iter().enumerate() {
        eprintln!(
            "  {entries:>5} entries: log+blocks per increment {:>9.1} B due-only, {:>9.1} B every time",
            skipped[index].0 + skipped[index].1,
            forced[index].0 + forced[index].1
        );
    }
    let total = |pair: (f64, f64)| pair.0 + pair.1;
    // THE CONTROL: written every time, the cost must grow with the series, or the counters are blind.
    assert!(
        total(forced[2]) > 4.0 * total(forced[0]) && total(forced[2]) - total(forced[0]) > 8_000.0,
        "the forced arm did not grow with the series ({:.0} B at 200, {:.0} B at 3,200)",
        total(forced[0]),
        total(forced[2])
    );
    // FLAT: sixteen entries' worth whatever the length, with room for which increments were due.
    assert!(
        total(skipped[2]) < 3.0 * total(skipped[0]),
        "an increment still costs the series' length: {:.0} B at 200 entries, {:.0} B at 3,200",
        total(skipped[0]),
        total(skipped[2])
    );
    // AND A MULTIPLE BELOW WHAT WAS WRITTEN: not just flat, flat and low.
    assert!(
        total(forced[2]) > 10.0 * total(skipped[2]),
        "due-only wrote {:.0} B per increment at 3,200 entries against {:.0} B every time",
        total(skipped[2]),
        total(forced[2])
    );
}

/// A history over one counter: increments that add to existing buckets and make new ones, then a
/// `ControlStateSet` (no outcome: its block is its only record) in the middle.
fn history(engine: &TemporalEngine, seed: u64, steps: usize) -> BTreeMap<u64, i64> {
    let mut rng = Rng(seed);
    let mut oracle = BTreeMap::new();
    for step in 0..steps {
        let at = 1_000 + rng.next(220);
        if step == steps / 2 {
            run(
                engine,
                Command::ControlStateSet {
                    family: ControlStateFamily::Counter,
                    key: "c".to_string(),
                    timestamp_ms: 1_000 + rng.next(220),
                    amount: 777,
                },
            );
            // The set's own value is read back below rather than modelled: it overwrites a bucket.
            oracle = series(engine);
            continue;
        }
        let amount = 1 + rng.next(5) as i64;
        increment(engine, at, amount);
        *oracle.entry(at).or_insert(0) += amount;
    }
    oracle
}

fn recovered(dir: &std::path::Path, bound: Option<usize>) -> BTreeMap<u64, i64> {
    let engine = open(dir, bound);
    series(&engine)
}

#[test]
fn a_counter_series_recovers_whether_or_not_its_block_was_written() {
    for forced in [false, true] {
        // No unload: recovery from the served index and the log alone.
        let dir = tempfile::tempdir().unwrap();
        let oracle = with_every_block(forced, || {
            let engine = open(dir.path(), None);
            let oracle = history(&engine, 11, 700);
            assert_eq!(series(&engine), oracle, "forced={forced}: the live series is not the oracle");
            oracle
        });
        assert!(oracle.len() > 50, "the history left too few buckets to mean anything");
        assert_eq!(recovered(dir.path(), None), oracle, "forced={forced}: recovery without an unload served another series");

        // A compaction, a dump and a log reclaim in between, then no unload.
        let dir = tempfile::tempdir().unwrap();
        let oracle = with_every_block(forced, || {
            let engine = open(dir.path(), None);
            let first = history(&engine, 29, 400);
            assert!(engine.compact_shard_blocks(1).is_ok());
            engine.apply_storage_lifecycle(crate::engine::reports::StorageLifecycleRequest {
                shard_id: 1,
                min_undumped_wal_records: 0,
                min_undumped_wal_bytes: 0,
                ..crate::engine::reports::StorageLifecycleRequest::default()
            });
            let plan = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());
            let reclaim = engine.apply_storage_wal_reclaim(plan);
            assert!(reclaim.wal_records_removed > 0, "forced={forced}: no log was reclaimed");
            // More increments after the reclaim, on top of the dumped series.
            let mut oracle = first;
            let mut rng = Rng(5);
            for _ in 0..250 {
                let at = 1_000 + rng.next(220);
                increment(&engine, at, 2);
                *oracle.entry(at).or_insert(0) += 2;
            }
            oracle
        });
        assert_eq!(recovered(dir.path(), None), oracle, "forced={forced}: recovery after compaction, dump and reclaim served another series");

        // A graceful reload.
        let dir = tempfile::tempdir().unwrap();
        let oracle = with_every_block(forced, || {
            let engine = open(dir.path(), None);
            let oracle = history(&engine, 77, 500);
            engine.unload_shard(1);
            oracle
        });
        assert_eq!(recovered(dir.path(), None), oracle, "forced={forced}: a graceful reload served another series");
    }
}

#[test]
fn a_counter_series_trimmed_at_its_bound_recovers_as_it_did_before() {
    const BOUND: usize = 40;
    let serve = |forced: bool| {
        let dir = tempfile::tempdir().unwrap();
        let live = with_every_block(forced, || {
            let engine = open(dir.path(), Some(BOUND));
            for step in 0..600u64 {
                increment(&engine, 1_000 + step * 3, 1);
            }
            series(&engine)
        });
        (live, recovered(dir.path(), Some(BOUND)))
    };
    let (live_skipped, recovered_skipped) = serve(false);
    let (live_forced, recovered_forced) = serve(true);
    assert_eq!(live_skipped.len(), BOUND, "the bound did not hold a live series");
    assert_eq!(live_skipped, live_forced, "the live series differs between the arms");
    assert_eq!(
        recovered_skipped, recovered_forced,
        "a trimmed series recovers differently when its block is not written every time"
    );
}

/// Profiler driver, not a gate: `CTR_E=<entries> CTR_K=<increments> [CTR_FORCE=1]` under
/// `valgrind --tool=callgrind --toggle-collect='*counter_span*'`.
#[inline(never)]
fn counter_span(engine: &TemporalEngine, base: u64, writes: u64) {
    for step in 0..writes {
        increment(engine, base + step, 1);
    }
}

#[test]
#[ignore]
fn a_counter_increment_for_the_profiler() {
    let entries = std::env::var("CTR_E").ok().and_then(|v| v.parse().ok()).unwrap_or(800u64);
    let writes = std::env::var("CTR_K").ok().and_then(|v| v.parse().ok()).unwrap_or(400u64);
    let forced = std::env::var_os("CTR_FORCE").is_some();
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), None);
    with_every_block(forced, || {
        for index in 0..entries {
            increment(&engine, 1_000 + index, 1);
        }
        counter_span(&engine, 1_000_000, writes);
    });
}
