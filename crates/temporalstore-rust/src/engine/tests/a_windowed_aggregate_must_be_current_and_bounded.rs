// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! TWO DEFECTS IN THE GATED ROLLUP PATH, FOUND BY ONE PROBE. Both answer a query wrongly or not
//! at all, and neither is visible with the gate off, so the raw path is the oracle throughout:
//! every test below drives the same commands twice and differs only in `control_rollup`.
//!
//! # 1. THE NUMERIC VIEW WAS NEVER INVALIDATED ON A WRITE
//!
//! `feature_values` is a derived numeric view of a feature series, folded once and then served by
//! the rollup ladder. Its staleness test was a LENGTH COMPARISON:
//!
//! ```ignore
//! let stale = shard.feature_values().get(&key[..])
//!     .map(|values| values.len() != feature_len)
//!     .unwrap_or(feature_len > 0);
//! ```
//!
//! Nothing on the write path invalidated it -- the only other sites were `feature_forget`
//! (expiry) and `feature_clear_all` (reconcile), and the suite's own note in
//! `string_keyed_holders` says these maps are "populated by the AGGREGATE QUERY, not the append".
//! A length test cannot see three changes that matter:
//!
//!   * an append that REPLACES the value at an instant already present -- same length;
//!   * an append of P points to a series AT ITS BOUND, which trims P -- same length, and **that
//!     is the steady state of every bounded series**, which is what a bound is for;
//!   * a delete followed by a re-creation with the same number of points.
//!
//! So a long sequence at its bound served the sum it held the first time it was ever queried, for
//! as long as it stayed at its bound.
//!
//! # 2. AN UNBOUNDED WINDOW WALKED THE WHOLE 64-BIT TIME DOMAIN
//!
//! `range_sum` covered the middle of a window by stepping a cursor across TILING POSITIONS, at
//! most one day at a time, and asking the cell map at each stop. Positions with no cell add
//! nothing to the answer, so the sum was right -- but a window ending at `u64::MAX`, which a
//! client may simply ask for, puts the far end at about 1.8e19 and the loop at about 2e11 steps.
//! That is one query occupying the shard for hours. It was found by this file's own staleness
//! test hanging, and it is in the primitive SHARED with the control-state counter path, so the
//! last test here drives that path instead of the feature one.
//!
//! The two wide-window tests run their scenario on a thread and wait with a deadline, so that a
//! revert of the clamp FAILS rather than hanging the suite. The deadline is 60s against a correct
//! path that answers in microseconds -- about seven orders of magnitude of headroom, so a loaded
//! machine cannot flake it. A revert leaves that thread spinning until the binary exits, which is
//! the price of reporting the defect as a failure instead of a timeout.

use super::*;

const BOUND: usize = 4;
/// Bounded end for the staleness tests, so they can only fail for the staleness reason.
const END_MS: u64 = 1_000_000;
/// See the module note: headroom, not a measurement.
const DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);

fn engine_with(gate_on: bool) -> (TemporalEngine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let mut extend_config = std::collections::BTreeMap::new();
    if gate_on {
        extend_config.insert("control_rollup".to_string(), "on".to_string());
    }
    assert!(
        engine
            .set_config(crate::control::SetConfigRequest {
                shard_id: 1,
                config: crate::control::Config {
                    version: 2,
                    feature_max_size: BOUND,
                    extend_config,
                    ..Default::default()
                },
            })
            .ok,
        "the bound and the gate have to be the ones under test"
    );
    (engine, dir)
}

/// Append `count` points from `base`, each holding the decimal of `value`, so an expected sum is
/// arithmetic rather than a fixture.
fn append_valued(engine: &TemporalEngine, base: u64, count: usize, value: i64) {
    let points = (0..count as u64)
        .map(|offset| FeaturePoint {
            timestamp_ms: base + offset,
            value: value.to_string().into_bytes(),
        })
        .collect();
    let outcome = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "seq".to_string(),
            points,
        },
    });
    assert!(
        outcome.status.ok,
        "append at {base} failed: {:?}",
        outcome.response
    );
}

fn sum_to(engine: &TemporalEngine, end_ms: u64) -> i64 {
    match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAggQuery {
                key: "seq".to_string(),
                start_ms: 0,
                end_ms,
                aggregator: "sum".to_string(),
                count: None,
            },
        })
        .response
    {
        CommandResponse::Aggregate { value } => value,
        other => panic!("expected Aggregate, got {other:?}"),
    }
}

fn sum(engine: &TemporalEngine) -> i64 {
    sum_to(engine, END_MS)
}

/// Fill to the bound, query (which is also what POPULATES the view, so it is the state the view
/// can get stuck in), then append `BOUND` more. The series is AT its bound, so this appends
/// `BOUND` and trims `BOUND`: the length does not move and every retained point is now a 10.
fn at_the_bound(gate_on: bool) -> (i64, i64) {
    let (engine, _dir) = engine_with(gate_on);
    append_valued(&engine, 1_000, BOUND, 1);
    let first = sum(&engine);
    append_valued(&engine, 2_000, BOUND, 10);
    (first, sum(&engine))
}

/// Fill, query, then write the SAME instants again with a different value. The length does not
/// move because every instant is replaced rather than added.
fn overwritten_in_place(gate_on: bool) -> (i64, i64) {
    let (engine, _dir) = engine_with(gate_on);
    append_valued(&engine, 1_000, BOUND, 1);
    let first = sum(&engine);
    append_valued(&engine, 1_000, BOUND, 10);
    (first, sum(&engine))
}

/// Fill, query, delete the whole series, then re-create it with the SAME number of points. A
/// length test reads the new series as the same size as the view it left behind.
fn deleted_and_recreated(gate_on: bool) -> (i64, i64) {
    let (engine, _dir) = engine_with(gate_on);
    append_valued(&engine, 1_000, BOUND, 1);
    let first = sum(&engine);
    let outcome = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureDelete {
            key: "seq".to_string(),
        },
    });
    assert!(outcome.status.ok, "delete failed: {:?}", outcome.response);
    append_valued(&engine, 5_000, BOUND, 10);
    (first, sum(&engine))
}

/// The raw path, which is the oracle: it folds the retained series on every query and has no view
/// to go stale. If this ever fails the scenarios themselves are wrong, not the rollup.
#[test]
fn the_raw_sums_follow_the_series_through_every_change_that_keeps_its_length() {
    assert_eq!(
        at_the_bound(false),
        (BOUND as i64, BOUND as i64 * 10),
        "at the bound: {BOUND} points of 1, then {BOUND} points of 10 with the oldest {BOUND} trimmed"
    );
    assert_eq!(
        overwritten_in_place(false),
        (BOUND as i64, BOUND as i64 * 10),
        "overwritten in place: the same {BOUND} instants now hold 10"
    );
    assert_eq!(
        deleted_and_recreated(false),
        (BOUND as i64, BOUND as i64 * 10),
        "deleted and re-created: {BOUND} fresh points of 10"
    );
}

#[test]
fn the_rollup_sum_must_not_be_stale_after_a_trim_that_keeps_the_length() {
    let (raw_first, raw_second) = at_the_bound(false);
    let (first, second) = at_the_bound(true);
    assert_eq!(first, raw_first, "the two paths must agree before the trim");
    assert_eq!(
        second, raw_second,
        "THE ROLLUP IS STALE: the series holds {BOUND} points of 10 and the rollup answers \
         {second} where the raw path answers {raw_second}. The numeric view was rebuilt only when \
         the series LENGTH changed, and a command that appends {BOUND} and trims {BOUND} leaves it \
         unchanged -- which is the steady state of every bounded series"
    );
}

#[test]
fn the_rollup_sum_must_not_be_stale_after_an_overwrite_at_an_instant_already_present() {
    let (raw_first, raw_second) = overwritten_in_place(false);
    let (first, second) = overwritten_in_place(true);
    assert_eq!(first, raw_first, "the two paths must agree before the overwrite");
    assert_eq!(
        second, raw_second,
        "THE ROLLUP IS STALE: every instant was rewritten with a 10 and the rollup answers \
         {second} where the raw path answers {raw_second}. Replacing a value at an instant already \
         present does not change the series length"
    );
}

#[test]
fn the_rollup_sum_must_not_be_stale_after_a_delete_and_a_recreate_of_the_same_length() {
    let (raw_first, raw_second) = deleted_and_recreated(false);
    let (first, second) = deleted_and_recreated(true);
    assert_eq!(first, raw_first, "the two paths must agree before the delete");
    assert_eq!(
        second, raw_second,
        "THE ROLLUP IS STALE: the key was deleted and re-created with {BOUND} points of 10, and \
         the rollup answers {second} where the raw path answers {raw_second}. The delete arm \
         removed the series and left the view behind, so a re-creation of the same size reads as \
         fresh"
    );
}

/// Run `scenario` on its own thread and refuse to wait forever. See the module note on why the
/// claim is made this way rather than with a bare call.
fn within_deadline<T: Send + 'static>(what: &str, scenario: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(scenario());
    });
    rx.recv_timeout(DEADLINE).unwrap_or_else(|_| {
        panic!(
            "{what} DID NOT FINISH IN {DEADLINE:?}. A windowed aggregate covered its middle by \
             stepping across tiling POSITIONS rather than over the cells that exist, so a window \
             ending at u64::MAX walked about 1.8e19 milliseconds in steps of at most one day -- \
             on the order of 2e11 map lookups for a series of a handful of points"
        )
    })
}

#[test]
fn an_unbounded_feature_window_must_not_walk_the_whole_time_domain() {
    let raw = within_deadline("the raw path on an unbounded window", || {
        let (engine, _dir) = engine_with(false);
        append_valued(&engine, 1_000, BOUND, 7);
        sum_to(&engine, u64::MAX)
    });
    assert_eq!(
        raw,
        BOUND as i64 * 7,
        "the oracle: {BOUND} points of 7 over an unbounded window"
    );
    let rollup = within_deadline("the rollup path on an unbounded window", || {
        let (engine, _dir) = engine_with(true);
        append_valued(&engine, 1_000, BOUND, 7);
        sum_to(&engine, u64::MAX)
    });
    assert_eq!(
        rollup, raw,
        "an unbounded window must give the rollup path the same answer as the raw path"
    );
}

/// THE DEFECT IS IN THE SHARED PRIMITIVE, so it has to be pinned on the other tenant too. This
/// drives control-state counters, whose series is authoritative rather than derived -- the
/// staleness half above cannot apply to it, and the walk half did.
#[test]
fn an_unbounded_control_state_window_must_not_walk_the_whole_time_domain() {
    /// THE TWO ARMS DO NOT SPELL THE KEY THE SAME WAY, and the oracle below is what caught it.
    /// `ControlStateSet` carries a family and files the series under
    /// `control_state_family_key(family, key)`; `ControlStateCount` carries no family and reads the
    /// key exactly as given. So a counter written as `cs` in the `Counter` family is read back as
    /// `control_state:h:cs`, and a test that spells both the same way measures an absent series --
    /// which answers 0 for any window and would have called the walk "bounded".
    const WRITTEN_AS: &str = "cs";
    const FILED_UNDER: &str = "control_state:h:cs";

    fn count_to_max(gate_on: bool) -> i64 {
        let (engine, _dir) = engine_with(gate_on);
        for offset in 0..BOUND as u64 {
            let outcome = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ControlStateSet {
                    family: ControlStateFamily::Counter,
                    key: WRITTEN_AS.to_string(),
                    timestamp_ms: 1_000 + offset,
                    amount: 3,
                },
            });
            assert!(outcome.status.ok, "set failed: {:?}", outcome.response);
        }
        match engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ControlStateCount {
                    key: FILED_UNDER.to_string(),
                    start_ms: 0,
                    end_ms: u64::MAX,
                },
            })
            .response
        {
            CommandResponse::Integer { value } => value,
            other => panic!("expected Integer, got {other:?}"),
        }
    }

    let raw = within_deadline("the raw counter path on an unbounded window", || {
        count_to_max(false)
    });
    assert_eq!(
        raw,
        BOUND as i64 * 3,
        "the oracle: {BOUND} increments of 3 over an unbounded window"
    );
    let rollup = within_deadline("the counter rollup path on an unbounded window", || {
        count_to_max(true)
    });
    assert_eq!(
        rollup, raw,
        "an unbounded window must give the counter rollup path the same answer as the raw path"
    );
}
