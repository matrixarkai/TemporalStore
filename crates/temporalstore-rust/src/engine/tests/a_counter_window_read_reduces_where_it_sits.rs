// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A COUNTER WINDOW IS REDUCED WHERE IT SITS, NOT COPIED OUT TO BE REDUCED.
//!
//! The counter queries (`ControlStateQuery`, `ControlStateFamilyQuery`, the window a
//! `ControlStateSetAndGet` answers with) collected the buckets of the window into a `Vec<i64>` and
//! handed the slice to the reduction: eight bytes a bucket, allocated and dropped on every read, so
//! a read over a series at the default cap of 10,000 points allocated 80 KB to answer one number.
//! Every verb reads the window once and keeps constant state, so the reduction now takes the
//! iterator over the stored range.
//!
//! WHAT MOVES: the bytes a window read allocates stop growing with the window. WHAT DOES NOT: any
//! answer -- the verbs, their fallbacks (`avg` and every unknown name sum, `events` and `len` count,
//! an empty window answers 0) and the stored series are exactly what they were.
//!
//! TWO GUARDS.
//!   * `a_window_read_answers_what_an_independent_reduction_answers` drives each reading arm over
//!     many windows -- empty, one bucket, several, a window past the series, a key with no series,
//!     negative and repeated values -- under every verb name and case, against a reduction written
//!     here over the same buckets.
//!   * `probe::a_window_read_does_not_allocate_per_bucket` (needs `alloc-probe`, `--ignored`) holds
//!     the bytes a read allocates at a 100-bucket and a 1,000-bucket window within a few
//!     bytes of each other, and carries a control: the detail read of the same series must grow
//!     with it, or the probe cannot see that dimension.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::types::{CommandResponse, ControlStateFamily};
use std::collections::BTreeMap;

fn open(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        1 << 22,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
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

fn integer(response: CommandResponse) -> i64 {
    match response {
        CommandResponse::Integer { value } => value,
        other => panic!("expected an integer, got {other:?}"),
    }
}

/// The reduction a counter window has always answered, over the buckets the window holds.
fn oracle(window: &[i64], verb: &str) -> i64 {
    match verb.to_ascii_lowercase().as_str() {
        "events" | "len" => window.len() as i64,
        "min" => window.iter().copied().min().unwrap_or(0),
        "max" => window.iter().copied().max().unwrap_or(0),
        "first" => window.first().copied().unwrap_or(0),
        "last" => window.last().copied().unwrap_or(0),
        // `avg`, `count`, the empty name and every unknown verb are the sum.
        _ => window.iter().sum(),
    }
}

const VERBS: [&str; 14] = [
    "sum", "SUM", "min", "Min", "max", "MAX", "first", "last", "events", "LEN", "avg", "count", "", "bogus",
];

#[test]
fn a_window_read_answers_what_an_independent_reduction_answers() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let mut series: BTreeMap<u64, i64> = BTreeMap::new();
    for (at, amount) in [
        (1_000u64, 5i64),
        (1_010, -3),
        (1_020, 5),
        (1_020, 2),
        (1_040, -40),
        (1_100, 7),
        (1_150, 0),
        (2_000, 11),
    ] {
        run(
            &engine,
            Command::ControlStateIncrement {
                key: "c".to_string(),
                timestamp_ms: at,
                amount,
            },
        );
        *series.entry(at).or_default() += amount;
    }

    let windows = [
        (0u64, u64::MAX),
        (1_000, 1_000),
        (1_005, 1_015),
        (1_011, 1_019),
        (1_000, 1_100),
        (1_101, 1_999),
        (3_000, 4_000),
        (2_000, 2_000),
    ];
    let mut compared = 0usize;
    let mut nonzero = 0usize;
    for (start_ms, end_ms) in windows {
        let held: Vec<i64> = series
            .range(start_ms..=end_ms)
            .map(|(_, value)| *value)
            .collect();
        for verb in VERBS {
            let expected = oracle(&held, verb);
            let answered = integer(run(
                &engine,
                Command::ControlStateQuery {
                    key: "c".to_string(),
                    start_ms,
                    end_ms,
                    aggregator: verb.to_string(),
                },
            ));
            assert_eq!(answered, expected, "query `{verb}` over [{start_ms}, {end_ms}]");
            // A key with no series reduces the empty window.
            let absent = integer(run(
                &engine,
                Command::ControlStateQuery {
                    key: "absent".to_string(),
                    start_ms,
                    end_ms,
                    aggregator: verb.to_string(),
                },
            ));
            assert_eq!(absent, oracle(&[], verb), "absent key, verb `{verb}`");
            compared += 2;
            nonzero += usize::from(expected != 0);
        }
    }
    // THE CONTROL: the windows really held buckets, so the equal answers are not equal zeros.
    assert_eq!(compared, windows.len() * VERBS.len() * 2);
    assert!(nonzero > windows.len(), "only {nonzero} of the answers were non-zero");

    // The family arm reduces the family-prefixed series, which is empty here: the empty window.
    for verb in VERBS {
        let family = integer(run(
            &engine,
            Command::ControlStateFamilyQuery {
                family: ControlStateFamily::Counter,
                key: "f".to_string(),
                start_ms: 1_000,
                end_ms: 1_150,
                aggregator: verb.to_string(),
            },
        ));
        assert_eq!(family, oracle(&[], verb), "family query on an empty series, `{verb}`");
    }

    // The arm that writes and reads in one command answers over the window AFTER its own bucket
    // landed. One key per verb, so each verb sees the same history.
    for verb in ["sum", "min", "MAX", "first", "last", "events", "bogus"] {
        let key = format!("sg-{verb}");
        let mut held: BTreeMap<u64, i64> = BTreeMap::new();
        for (at, amount) in [(10u64, 4i64), (20, -9), (20, 1), (30, 6), (5, 2)] {
            *held.entry(at).or_default() += amount;
            let answered = integer(run(
                &engine,
                Command::ControlStateSetAndGet {
                    family: ControlStateFamily::Counter,
                    key: key.clone(),
                    timestamp_ms: at,
                    amount,
                    start_ms: 0,
                    end_ms: u64::MAX,
                    aggregator: verb.to_string(),
                },
            ));
            let window: Vec<i64> = held.values().copied().collect();
            assert_eq!(answered, oracle(&window, verb), "set-and-get `{verb}` after writing {at}");
        }
    }
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    const OPS: usize = 20;

    fn bytes_per_op(buckets: usize, command: impl Fn() -> Command) -> f64 {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = open(dir.path());
        for at in 0..buckets as u64 {
            run(
                &engine,
                Command::ControlStateIncrement {
                    key: "c".to_string(),
                    timestamp_ms: 1_000 + at,
                    amount: 1,
                },
            );
        }
        let commands: Vec<Command> = (0..OPS).map(|_| command()).collect();
        let probe = Probe::start();
        for command in commands {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            assert!(response.status.ok);
        }
        probe.stop().alloc_bytes as f64 / OPS as f64
    }

    fn query(aggregator: &'static str) -> impl Fn() -> Command {
        move || Command::ControlStateQuery {
            key: "c".to_string(),
            start_ms: 0,
            end_ms: u64::MAX,
            aggregator: aggregator.to_string(),
        }
    }

    fn detail() -> Command {
        Command::ControlStateDetail {
            key: "c".to_string(),
            start_ms: 0,
            end_ms: u64::MAX,
            count: None,
        }
    }

    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_window_read_does_not_allocate_per_bucket() {
        let canary = crate::alloc_probe::Probe::start();
        let sink: Vec<u8> = Vec::with_capacity(8192);
        assert!(
            canary.stop().allocs > 0,
            "the counting allocator is not installed -- rerun with `--features alloc-probe`"
        );
        drop(sink);

        let control_short = bytes_per_op(100, detail);
        let control_long = bytes_per_op(1_000, detail);
        let control_slope = (control_long - control_short) / 900.0;
        assert!(
            control_slope >= 8.0,
            "the control (the detail read) allocated {control_slope:.1} B per bucket, so the probe \
             cannot see work that grows with the window"
        );
        for verb in ["sum", "min", "events", "first"] {
            let short = bytes_per_op(100, query(verb));
            let long = bytes_per_op(1_000, query(verb));
            let slope = (long - short) / 900.0;
            eprintln!(
                "  [{verb}] {short:.0} B @100, {long:.0} B @1000, {slope:.2} B/bucket \
                 (control {control_slope:.1} B/bucket)"
            );
            assert!(
                slope < 1.0,
                "a `{verb}` read allocated {slope:.2} B per bucket of its window; the window is being \
                 copied out to be reduced"
            );
        }
    }
}
