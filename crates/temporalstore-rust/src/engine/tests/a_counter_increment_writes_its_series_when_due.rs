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
//!   instructions    1.87M (200) 4.4M (800) 13.9M (3,200)       -> 3.16M at the 10,000-point default cap (release)
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

/// Series after each of the first `steps` increments of one counter (index `j` is the series after
/// `j` increments, so index 0 is empty), and whether each increment wrote a series block. The
/// engine is dropped with no unload and its directory left in `dir`.
fn drive_and_drop(dir: &std::path::Path, steps: usize) -> (Vec<BTreeMap<u64, i64>>, Vec<bool>) {
    let engine = open(dir, None);
    let mut states = vec![BTreeMap::new()];
    let mut wrote_block = Vec::new();
    let mut rng = Rng(3);
    for _ in 0..steps {
        let at = 1_000 + rng.next(400);
        let before = engine.block_store().stats().bytes_written;
        increment(&engine, at, 1 + rng.next(4) as i64);
        wrote_block.push(engine.block_store().stats().bytes_written > before);
        states.push(series(&engine));
    }
    (states, wrote_block)
}

/// Copy `from` to `to`, then cut the last `cut` bytes of the written log in `to`.
fn copy_dir_cut_log(from: &std::path::Path, to: &std::path::Path, cut: usize) {
    fn copy(from: &std::path::Path, to: &std::path::Path) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let target = to.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), target).unwrap();
            }
        }
    }
    copy(from, to);
    let path = to.join("indexes").join("wals").join("shard-1.wal.bin");
    let mut bytes = std::fs::read(&path).expect("the log exists");
    while bytes.last() == Some(&0) {
        bytes.pop();
    }
    assert!(bytes.len() > cut, "the log is shorter than the cut");
    bytes.truncate(bytes.len() - cut);
    std::fs::write(&path, &bytes).expect("rewrite the log");
}

/// A TORN LOG TAIL DURING COUNTER RECOVERY serves the series of some whole prefix of the increments,
/// never another series: cut in the middle of the last record after a first increment, after an
/// increment that wrote no series block, and after one that did.
#[test]
fn a_counter_log_cut_mid_record_recovers_a_prefix_of_its_increments() {
    // Find, once, a step that wrote no block and one that did, past the first sixteen.
    let probe = tempfile::tempdir().unwrap();
    let (_, wrote) = drive_and_drop(probe.path(), 120);
    assert!(wrote[0], "the first increment of a key must write its block");
    let quiet = (20..wrote.len()).find(|&step| !wrote[step]).expect("no increment skipped its block");
    let due = (20..wrote.len()).find(|&step| wrote[step]).expect("no later increment wrote its block");
    drop(probe);

    for (label, step) in [("first increment", 0usize), ("increment without a block", quiet), ("increment with a block", due)] {
        let steps = step + 1;
        let dir = tempfile::tempdir().unwrap();
        let (states, _) = drive_and_drop(dir.path(), steps);
        let full = states.len() - 1;
        let mut previous = full + 1;
        for cut in [1usize, 7, 20, 60, 150, 400] {
            let cutdir = tempfile::tempdir().unwrap();
            let size = {
                let log = dir.path().join("indexes").join("wals").join("shard-1.wal.bin");
                let mut bytes = std::fs::read(log).unwrap();
                while bytes.last() == Some(&0) {
                    bytes.pop();
                }
                bytes.len()
            };
            if size <= cut {
                continue;
            }
            copy_dir_cut_log(dir.path(), cutdir.path(), cut);
            let got = recovered(cutdir.path(), None);
            let prefix = states.iter().position(|state| *state == got);
            eprintln!("  {label}: cut {cut:>3} B of {size} B -> prefix {prefix:?} of {full}");
            let prefix = prefix.unwrap_or_else(|| {
                panic!("{label}: a log cut by {cut} B recovered a series that is no prefix of the increments: {got:?}")
            });
            assert!(prefix < full, "{label}: a log cut by {cut} B still served the whole series");
            assert!(prefix <= previous, "{label}: a larger cut recovered a later prefix ({prefix} after {previous})");
            previous = prefix;
        }
    }
}

// ---- THE OTHER FOUR ARMS --------------------------------------------------------------------
//
// `ControlStateIncrementWithOptions`, `ControlStateSet`, `ControlStateSetAndGet` and
// `ControlStateSetAndGetWithOptions` add into one bucket of one series exactly as an increment does
// (`ControlStateSet` ADDS; it does not overwrite), so each can state the total it left in that
// bucket. Replay installs that total with `insert`, which is the same value however many times it
// runs, so the whole-series block is a copy for them too. The family arms write the series under
// `control_state:h:<key>`, not under `<key>`.

const FAMILY_KEY: &str = "control_state:h:c";

fn series_of(engine: &TemporalEngine, key: &str) -> BTreeMap<u64, i64> {
    match run(
        engine,
        Command::ControlStateDetail {
            key: key.to_string(),
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

/// The series an arm writes: arm 0 is `IncrementWithOptions` on `c`, the rest are family arms.
fn arm_key(arm: usize) -> &'static str {
    if arm == 0 {
        "c"
    } else {
        FAMILY_KEY
    }
}

/// One write of `amount` at `at` through arm `arm` (0 `IncrementWithOptions`, 1 `Set`, 2 `SetAndGet`,
/// 3 `SetAndGetWithOptions`), rounded to `precision_ms` where the arm takes one.
fn arm_write(engine: &TemporalEngine, arm: usize, at: u64, amount: i64, precision_ms: Option<u64>, uuid: Option<String>) {
    let key = "c".to_string();
    let family = ControlStateFamily::Counter;
    let command = match arm {
        0 => Command::ControlStateIncrementWithOptions {
            key,
            timestamp_ms: at,
            amount,
            precision_ms,
            ttl_ms: None,
        },
        1 => Command::ControlStateSet {
            family,
            key,
            timestamp_ms: at,
            amount,
        },
        2 => Command::ControlStateSetAndGet {
            family,
            key,
            timestamp_ms: at,
            amount,
            start_ms: 0,
            end_ms: u64::MAX,
            aggregator: "sum".to_string(),
        },
        3 => Command::ControlStateSetAndGetWithOptions {
            family,
            key,
            timestamp_ms: at,
            amount,
            start_ms: 0,
            end_ms: u64::MAX,
            aggregator: "sum".to_string(),
            precision_ms,
            ttl_ms: None,
            uuid,
        },
        _ => unreachable!(),
    };
    run(engine, command);
}

fn bytes_per_arm_write(arm: usize, entries: usize, writes: usize, forced: bool) -> f64 {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), None);
    with_every_block(forced, || {
        for index in 0..entries as u64 {
            arm_write(&engine, arm, 1_000 + index, 1, None, None);
        }
        let wal = engine.wal_store.raw_stats(1).bytes_written;
        let blocks = engine.block_store().stats().bytes_written;
        for index in 0..writes as u64 {
            arm_write(&engine, arm, 1_000_000 + index, 1, None, None);
        }
        ((engine.wal_store.raw_stats(1).bytes_written - wal) + (engine.block_store().stats().bytes_written - blocks)) as f64
            / writes as f64
    })
}

#[test]
fn every_counter_arm_writes_its_series_only_now_and_then() {
    const WRITES: usize = 400;
    for arm in 0..4usize {
        let skipped = [200usize, 3200].map(|entries| bytes_per_arm_write(arm, entries, WRITES, false));
        let forced = [200usize, 3200].map(|entries| bytes_per_arm_write(arm, entries, WRITES, true));
        eprintln!(
            "  arm {arm}: due-only {:>7.1} B at 200, {:>7.1} B at 3,200; every time {:>8.1} B at 200, {:>9.1} B at 3,200",
            skipped[0], skipped[1], forced[0], forced[1]
        );
        assert!(
            forced[1] > 4.0 * forced[0] && forced[1] - forced[0] > 8_000.0,
            "arm {arm}: the forced arm did not grow with the series ({:.0} B, {:.0} B)",
            forced[0],
            forced[1]
        );
        assert!(
            skipped[1] < 3.0 * skipped[0],
            "arm {arm}: a write still costs the series' length: {:.0} B at 200, {:.0} B at 3,200",
            skipped[0],
            skipped[1]
        );
        assert!(
            forced[1] > 10.0 * skipped[1],
            "arm {arm}: due-only wrote {:.0} B at 3,200 entries against {:.0} B every time",
            skipped[1],
            forced[1]
        );
    }
}

/// A seeded history over every arm, `Increment` included, on the two series the arms write. Some
/// writes repeat an earlier `uuid`, which add nothing and still write their block.
fn history_every_arm(engine: &TemporalEngine, seed: u64, steps: usize) {
    let mut rng = Rng(seed);
    for step in 0..steps {
        let at = 1_000 + rng.next(220);
        let amount = 1 + rng.next(5) as i64;
        let precision = if rng.next(2) == 0 { Some(10) } else { None };
        match rng.next(5) {
            4 => increment(engine, at, amount),
            arm => {
                let arm = arm as usize;
                let uuid = if arm == 3 && rng.next(3) == 0 {
                    // The same uuid twice in a row: the second adds nothing.
                    let uuid = format!("u{step}");
                    arm_write(engine, 3, at, amount, precision, Some(uuid.clone()));
                    Some(uuid)
                } else {
                    None
                };
                arm_write(engine, arm, at, amount, precision, uuid);
            }
        }
    }
}

fn both_series(engine: &TemporalEngine) -> (BTreeMap<u64, i64>, BTreeMap<u64, i64>) {
    (series_of(engine, "c"), series_of(engine, FAMILY_KEY))
}

fn recovered_both(dir: &std::path::Path, bound: Option<usize>) -> (BTreeMap<u64, i64>, BTreeMap<u64, i64>) {
    let engine = open(dir, bound);
    both_series(&engine)
}

#[test]
fn a_series_every_counter_arm_wrote_recovers_whether_or_not_its_block_was_written() {
    for forced in [false, true] {
        // No unload.
        let dir = tempfile::tempdir().unwrap();
        let live = with_every_block(forced, || {
            let engine = open(dir.path(), None);
            history_every_arm(&engine, 13, 800);
            both_series(&engine)
        });
        assert!(live.0.len() > 30 && live.1.len() > 30, "the history left too few buckets to mean anything");
        assert_eq!(recovered_both(dir.path(), None), live, "forced={forced}: recovery without an unload served another series");

        // A compaction, a dump and a log reclaim, then more writes, then no unload.
        let dir = tempfile::tempdir().unwrap();
        let live = with_every_block(forced, || {
            let engine = open(dir.path(), None);
            history_every_arm(&engine, 31, 500);
            assert!(engine.compact_shard_blocks(1).is_ok());
            engine.apply_storage_lifecycle(crate::engine::reports::StorageLifecycleRequest {
                shard_id: 1,
                min_undumped_wal_records: 0,
                min_undumped_wal_bytes: 0,
                ..crate::engine::reports::StorageLifecycleRequest::default()
            });
            let plan = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());
            assert!(engine.apply_storage_wal_reclaim(plan).wal_records_removed > 0, "forced={forced}: no log was reclaimed");
            history_every_arm(&engine, 47, 300);
            both_series(&engine)
        });
        assert_eq!(recovered_both(dir.path(), None), live, "forced={forced}: recovery after compaction, dump and reclaim served another series");

        // A graceful reload.
        let dir = tempfile::tempdir().unwrap();
        let live = with_every_block(forced, || {
            let engine = open(dir.path(), None);
            history_every_arm(&engine, 71, 600);
            let live = both_series(&engine);
            engine.unload_shard(1);
            live
        });
        assert_eq!(recovered_both(dir.path(), None), live, "forced={forced}: a graceful reload served another series");
    }
}

#[test]
fn a_series_every_counter_arm_trimmed_at_its_bound_recovers_as_it_did_before() {
    const BOUND: usize = 40;
    let serve = |forced: bool| {
        let dir = tempfile::tempdir().unwrap();
        let live = with_every_block(forced, || {
            let engine = open(dir.path(), Some(BOUND));
            for step in 0..600u64 {
                for arm in 0..4 {
                    arm_write(&engine, arm, 1_000 + step * 3 + arm as u64 % 2, 1, None, None);
                }
            }
            both_series(&engine)
        });
        (live, recovered_both(dir.path(), Some(BOUND)))
    };
    let (live_skipped, recovered_skipped) = serve(false);
    let (live_forced, recovered_forced) = serve(true);
    assert_eq!(live_skipped.0.len(), BOUND, "the bound did not hold the live series of the options arm");
    assert_eq!(live_skipped.1.len(), BOUND, "the bound did not hold the live series of the family arms");
    assert_eq!(live_skipped, live_forced, "the live series differ between the arms");
    assert_eq!(
        recovered_skipped, recovered_forced,
        "a trimmed series recovers differently when its block is not written every time"
    );
}

/// A torn log tail during recovery of a series any of the four arms wrote serves a whole prefix of
/// that arm's writes: cut after the first write of the key, after a write without a block and
/// after one with a block.
#[test]
fn a_log_cut_mid_record_recovers_a_prefix_of_the_writes_of_every_counter_arm() {
    fn drive(dir: &std::path::Path, arm: usize, steps: usize) -> (Vec<BTreeMap<u64, i64>>, Vec<bool>) {
        let engine = open(dir, None);
        let mut states = vec![BTreeMap::new()];
        let mut wrote_block = Vec::new();
        let mut rng = Rng(5 + arm as u64);
        for _ in 0..steps {
            let at = 1_000 + rng.next(400);
            let before = engine.block_store().stats().bytes_written;
            arm_write(&engine, arm, at, 1 + rng.next(4) as i64, None, None);
            wrote_block.push(engine.block_store().stats().bytes_written > before);
            states.push(series_of(&engine, arm_key(arm)));
        }
        (states, wrote_block)
    }
    fn written_log_len(dir: &std::path::Path) -> usize {
        let mut bytes = std::fs::read(dir.join("indexes").join("wals").join("shard-1.wal.bin")).unwrap();
        while bytes.last() == Some(&0) {
            bytes.pop();
        }
        bytes.len()
    }
    for arm in 0..4usize {
        let probe = tempfile::tempdir().unwrap();
        let (_, wrote) = drive(probe.path(), arm, 120);
        assert!(wrote[0], "arm {arm}: the first write of a key must write its block");
        let quiet = (20..wrote.len()).find(|&step| !wrote[step]).unwrap_or_else(|| panic!("arm {arm}: no write skipped its block"));
        let due = (20..wrote.len()).find(|&step| wrote[step]).unwrap_or_else(|| panic!("arm {arm}: no later write wrote its block"));
        drop(probe);
        for (label, step) in [("first write", 0usize), ("write without a block", quiet), ("write with a block", due)] {
            let dir = tempfile::tempdir().unwrap();
            let (states, _) = drive(dir.path(), arm, step + 1);
            let full = states.len() - 1;
            let size = written_log_len(dir.path());
            let mut previous = full + 1;
            for cut in [1usize, 60, 400] {
                if size <= cut {
                    continue;
                }
                let cutdir = tempfile::tempdir().unwrap();
                copy_dir_cut_log(dir.path(), cutdir.path(), cut);
                let engine = open(cutdir.path(), None);
                let got = series_of(&engine, arm_key(arm));
                let prefix = states.iter().position(|state| *state == got).unwrap_or_else(|| {
                    panic!("arm {arm}, {label}: a log cut by {cut} B recovered no prefix of the writes: {got:?}")
                });
                eprintln!("  arm {arm}, {label}: cut {cut:>3} B of {size} B -> prefix {prefix} of {full}");
                assert!(prefix < full, "arm {arm}, {label}: a log cut by {cut} B still served the whole series");
                assert!(prefix <= previous, "arm {arm}, {label}: a larger cut recovered a later prefix");
                previous = prefix;
            }
        }
    }
}

/// ONE REAL RUN AT THE DEFAULT CAP (ignored: it fills a series of `control_state_max_size` points,
/// which is `feature_max_size`, 10,000 by default).
///
/// The same writes in both arms (the block written every time, which is how every increment wrote
/// before #2798, and due-only): fill a counter to the cap, write 200 more increments (each trims a
/// point), measure the log and block bytes, drop the engine with no unload, recover. PRINTS how the
/// recovered series differs from the live one and requires them equal.
#[test]
#[ignore]
fn a_counter_at_the_default_cap_recovers_held_at_the_cap() {
    let cap = crate::control::Config::default().control_state_max_size();
    let mut failures = Vec::new();
    for forced in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (live, log, block) = with_every_block(forced, || {
            let engine = open(dir.path(), None);
            for index in 0..cap as u64 + 100 {
                increment(&engine, 1_000 + index, 1);
            }
            assert_eq!(series(&engine).len(), cap, "the series did not hold at the default cap");
            let wal = engine.wal_store.raw_stats(1).bytes_written;
            let blocks = engine.block_store().stats().bytes_written;
            for index in 0..200u64 {
                increment(&engine, 10_000_000 + index, 1);
            }
            let log = (engine.wal_store.raw_stats(1).bytes_written - wal) as f64 / 200.0;
            let block = (engine.block_store().stats().bytes_written - blocks) as f64 / 200.0;
            (series(&engine), log, block)
        });
        let recovered = recovered(dir.path(), None);
        let resurrected = recovered.keys().filter(|at| !live.contains_key(at)).count();
        let lost = live.keys().filter(|at| !recovered.contains_key(at)).count();
        let changed = live.iter().filter(|(at, value)| recovered.get(at).is_some_and(|other| other != *value)).count();
        eprintln!(
            "  forced={forced:<5} cap {cap}, 10,300 increments: log {log:.0} B + blocks {block:.0} B per increment; live {} points, recovered {} points ({resurrected} the live series had trimmed, {lost} missing, {changed} with another value)",
            live.len(),
            recovered.len()
        );
        if recovered != live {
            failures.push(forced);
        }
    }
    assert!(failures.is_empty(), "a series at the default cap recovered as another series (forced arms: {failures:?})");
}

// ---- A RECOVERED COUNTER SERIES IS HELD AT ITS BOUND ----------------------------------------------
//
// A series is trimmed to its bound after each write and the trim records no removal. A replayed
// `control_counter` outcome installed its point and, until it trimmed as the write did, brought back
// every point the trim had dropped. Measured at the default cap of 10,000 points after 10,300
// increments, recovered with no unload: the live series held 10,000 points and the recovered one
// 10,300 in each arm (the block written every time, which is how every increment wrote before
// #2798, and due-only), nothing missing and no value changed. The tests below hold for all five
// counter arms and for both series (`<key>` and `control_state:h:<key>`).

/// One write through arm 0..=3 (see `arm_write`) or arm 4, `ControlStateIncrement`.
fn any_arm_write(engine: &TemporalEngine, arm: usize, at: u64, amount: i64) {
    if arm == 4 {
        increment(engine, at, amount);
    } else {
        arm_write(engine, arm, at, amount, None, None);
    }
}

/// The series arm `arm` writes: arms 0 and 4 write `c`, the family arms the family series.
fn any_arm_key(arm: usize) -> &'static str {
    if arm == 0 || arm == 4 {
        "c"
    } else {
        FAMILY_KEY
    }
}

fn set_bound(engine: &TemporalEngine, bound: usize, version: u64) {
    assert!(
        engine
            .set_config(crate::control::SetConfigRequest {
                shard_id: 1,
                config: crate::control::Config {
                    version,
                    feature_max_size: bound,
                    ..Default::default()
                },
            })
            .ok,
        "the bound has to be the one under test"
    );
}

#[test]
fn a_counter_series_recovers_held_at_its_bound() {
    const BOUND: usize = 40;
    for arm in 0..5usize {
        for forced in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let live = with_every_block(forced, || {
                let engine = open(dir.path(), Some(BOUND));
                for step in 0..600u64 {
                    any_arm_write(&engine, arm, 1_000 + step * 3, 1);
                }
                series_of(&engine, any_arm_key(arm))
            });
            assert_eq!(live.len(), BOUND, "arm {arm}, forced={forced}: the bound did not hold a live series");
            let engine = open(dir.path(), Some(BOUND));
            let got = series_of(&engine, any_arm_key(arm));
            assert_eq!(
                got.len(),
                BOUND,
                "arm {arm}, forced={forced}: a recovery with no unload held {} points against a bound of {BOUND}",
                got.len()
            );
            assert_eq!(got, live, "arm {arm}, forced={forced}: the recovered series is not the live one");
        }
    }
}

#[test]
fn a_counter_series_written_out_of_order_recovers_held_at_its_bound() {
    const BOUND: usize = 40;
    for arm in 0..5usize {
        for forced in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut rng = Rng(101 + arm as u64);
            let live = with_every_block(forced, || {
                let engine = open(dir.path(), Some(BOUND));
                for _ in 0..800 {
                    // Buckets arrive in any order, over a range wider than the bound, so a point
                    // older than everything held is written into a full series.
                    any_arm_write(&engine, arm, 1_000 + rng.next(300), 1 + rng.next(4) as i64);
                }
                series_of(&engine, any_arm_key(arm))
            });
            assert_eq!(live.len(), BOUND, "arm {arm}, forced={forced}: the bound did not hold a live series");
            let got = recovered_series(dir.path(), Some(BOUND), any_arm_key(arm));
            assert_eq!(got, live, "arm {arm}, forced={forced}: an out-of-order history recovered as another series");
            assert_eq!(got.keys().next_back(), live.keys().next_back(), "arm {arm}, forced={forced}: the newest bucket was dropped");
        }
    }
}

fn recovered_series(dir: &std::path::Path, bound: Option<usize>, key: &str) -> BTreeMap<u64, i64> {
    let engine = open(dir, bound);
    series_of(&engine, key)
}

#[test]
fn replaying_a_counter_log_twice_or_after_a_graceful_reload_gives_one_series() {
    const BOUND: usize = 40;
    for arm in [4usize, 1] {
        let dir = tempfile::tempdir().unwrap();
        let live = {
            let engine = open(dir.path(), Some(BOUND));
            for step in 0..600u64 {
                any_arm_write(&engine, arm, 1_000 + step * 3, 1);
            }
            series_of(&engine, any_arm_key(arm))
        };
        // The same log replayed twice: each recovery reads its own copy of what the crash left.
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        copy_dir_cut_log(dir.path(), first.path(), 0);
        copy_dir_cut_log(dir.path(), second.path(), 0);
        let a = recovered_series(first.path(), Some(BOUND), any_arm_key(arm));
        let b = recovered_series(second.path(), Some(BOUND), any_arm_key(arm));
        assert_eq!(a, live, "arm {arm}: a first replay is not the live series");
        assert_eq!(a, b, "arm {arm}: two replays of one log are two series");
        // A replay of a log a recovery has already replayed and written back.
        {
            let engine = open(first.path(), Some(BOUND));
            engine.unload_shard(1);
        }
        assert_eq!(
            recovered_series(first.path(), Some(BOUND), any_arm_key(arm)),
            live,
            "arm {arm}: a replay after a graceful reload is not the live series"
        );
    }
}

#[test]
fn a_counter_bound_changed_between_runs_recovers_as_the_write_path_held_it() {
    // SHRUNK WHILE RUNNING, then written: the records after the change replay under the new bound.
    for arm in [4usize, 1] {
        let dir = tempfile::tempdir().unwrap();
        let live = {
            let engine = open(dir.path(), Some(40));
            for step in 0..300u64 {
                any_arm_write(&engine, arm, 1_000 + step * 3, 1);
            }
            set_bound(&engine, 25, 3);
            for step in 300..600u64 {
                any_arm_write(&engine, arm, 1_000 + step * 3, 1);
            }
            series_of(&engine, any_arm_key(arm))
        };
        assert_eq!(live.len(), 25, "arm {arm}: the shrunk bound did not hold a live series");
        assert_eq!(
            recovered_series(dir.path(), None, any_arm_key(arm)),
            live,
            "arm {arm}: a bound shrunk while running recovered as another series"
        );
    }
    // GROWN WHILE RUNNING: nothing a smaller bound dropped may come back, and the larger bound fills.
    for arm in [4usize, 1] {
        let dir = tempfile::tempdir().unwrap();
        let live = {
            let engine = open(dir.path(), Some(25));
            for step in 0..300u64 {
                any_arm_write(&engine, arm, 1_000 + step * 3, 1);
            }
            set_bound(&engine, 40, 3);
            for step in 300..600u64 {
                any_arm_write(&engine, arm, 1_000 + step * 3, 1);
            }
            series_of(&engine, any_arm_key(arm))
        };
        assert_eq!(live.len(), 40, "arm {arm}: the grown bound did not hold a live series");
        assert_eq!(
            recovered_series(dir.path(), None, any_arm_key(arm)),
            live,
            "arm {arm}: a bound grown while running recovered as another series"
        );
    }
    // SHRUNK BETWEEN RUNS: the log was written under 40; the new run is configured with 25. A load
    // trims nothing (a write bound, not a resident invariant), the next write trims to the new bound,
    // and the series is then the one a process that never restarted holds.
    for arm in [4usize, 1] {
        let dir = tempfile::tempdir().unwrap();
        let reference_dir = tempfile::tempdir().unwrap();
        let history = |engine: &TemporalEngine| {
            for step in 0..600u64 {
                any_arm_write(engine, arm, 1_000 + step * 3, 1);
            }
        };
        let live_at_40 = {
            let engine = open(dir.path(), Some(40));
            history(&engine);
            series_of(&engine, any_arm_key(arm))
        };
        let reference = {
            let engine = open(reference_dir.path(), Some(40));
            history(&engine);
            set_bound(&engine, 25, 3);
            any_arm_write(&engine, arm, 1_000_000, 1);
            series_of(&engine, any_arm_key(arm))
        };
        let engine = open(dir.path(), None);
        assert_eq!(
            series_of(&engine, any_arm_key(arm)),
            live_at_40,
            "arm {arm}: a load trimmed durable points"
        );
        set_bound(&engine, 25, 3);
        any_arm_write(&engine, arm, 1_000_000, 1);
        assert_eq!(series_of(&engine, any_arm_key(arm)), reference, "arm {arm}: the next write did not trim to the new bound");
    }
}
