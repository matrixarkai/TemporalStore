// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A COUNTER SERIES THAT SITS AT ITS CAP TRIMS ITS OLDEST POINT WITHOUT BUILDING A LIST OF IT.
//!
//! A counter write trims the series to `control_state_max_size` before it decides whether to write
//! the block, and again inside the block write when the block is due. The trim collected the keys
//! of the points to drop into a `Vec<u64>` and removed them one by one, so a series that sits at
//! its cap -- every write to a new bucket pushes one point out -- allocated a vector on every
//! such write, twice when the block was due. It now takes the oldest point off the front one at a
//! time (`BTreeMap::pop_first`), which is the same set of points in the same order.
//!
//! WHAT MOVES: one allocation per write at the cap (two when the block is due). WHAT DOES NOT: the
//! points kept, the answer, the staged outcome or any byte written.
//!
//! TWO GUARDS.
//!   * `a_bounded_series_keeps_its_newest_points_whatever_the_write_order` drives a series at a small
//!     bound with timestamps that arrive ascending, descending, repeated and shuffled, and requires
//!     the series after EVERY write to equal a model that keeps the newest `bound` points.
//!   * `probe::a_write_at_the_cap_allocates_what_a_write_below_it_allocates` (needs `alloc-probe`,
//!     `--ignored`) holds a write that trims one point to the allocations of a write that trims
//!     none, over the same new-bucket writes.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::types::CommandResponse;
use std::collections::BTreeMap;

fn open(dir: &std::path::Path, bound: usize) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        1 << 22,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
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

struct Rng(u64);

impl Rng {
    fn next(&mut self, bound: u64) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 33) % bound
    }
}

/// What a bound has always meant: add the write, then drop the OLDEST points until `bound` remain.
fn model_write(model: &mut BTreeMap<u64, i64>, bound: usize, at: u64, amount: i64) {
    *model.entry(at).or_default() += amount;
    while model.len() > bound {
        let oldest = *model.keys().next().unwrap();
        model.remove(&oldest);
    }
}

#[test]
fn a_bounded_series_keeps_its_newest_points_whatever_the_write_order() {
    const BOUND: usize = 6;
    let orders: [(&str, Box<dyn Fn(usize, &mut Rng) -> u64>); 4] = [
        ("ascending", Box::new(|step, _| 1_000 + step as u64 * 10)),
        ("descending", Box::new(|step, _| 100_000 - step as u64 * 10)),
        ("repeating", Box::new(|step, _| 1_000 + (step as u64 % 8) * 10)),
        ("shuffled", Box::new(|_, rng| 1_000 + rng.next(40) * 10)),
    ];
    let mut trimmed_writes = 0usize;
    for (name, timestamp) in orders {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), BOUND);
        let mut model = BTreeMap::new();
        let mut rng = Rng(7);
        for step in 0..120usize {
            let at = timestamp(step, &mut rng);
            let amount = rng.next(5) as i64 + 1;
            let before = model.len();
            model_write(&mut model, BOUND, at, amount);
            trimmed_writes += usize::from(before == BOUND && model.len() == BOUND && !model.contains_key(&at));
            run(
                &engine,
                Command::ControlStateIncrement {
                    key: "c".to_string(),
                    timestamp_ms: at,
                    amount,
                },
            );
            assert_eq!(series(&engine), model, "{name} order, after write {step} at {at}");
        }
        assert_eq!(model.len(), BOUND, "{name}: the series never reached its cap");
    }
    // THE CONTROL: the writes really reached the trim, so equal series are not equal because
    // nothing was ever dropped. A write older than the whole series is dropped at once, which is
    // the descending order's every write after the sixth.
    assert!(trimmed_writes > 100, "only {trimmed_writes} writes were trimmed away");
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    const OPS: usize = 300;

    /// Allocations per write of `OPS` writes, each to a NEW bucket, to a series that holds
    /// `already` points under a bound of `bound`.
    fn per_write(already: usize, bound: usize) -> f64 {
        // THE BLOCK IS FORCED ON EVERY WRITE, so both arms write the same blocks. A due-only block
        // is chosen by a hash of the timestamp and the result, and its cost dwarfs the one
        // allocation under test: the two arms would differ by which writes the hash picked.
        crate::engine::ALWAYS_WRITE_COUNTER_BLOCK.with(|flag| flag.set(true));
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = open(dir.path(), bound);
        for at in 0..already as u64 {
            run(
                &engine,
                Command::ControlStateIncrement {
                    key: "c".to_string(),
                    timestamp_ms: 1_000 + at,
                    amount: 1,
                },
            );
        }
        let commands: Vec<Command> = (0..OPS as u64)
            .map(|at| Command::ControlStateIncrement {
                key: "c".to_string(),
                timestamp_ms: 1_000 + already as u64 + at,
                amount: 1,
            })
            .collect();
        let probe = Probe::start();
        for command in commands {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            assert!(response.status.ok);
        }
        let allocs = probe.stop().allocs as f64 / OPS as f64;
        crate::engine::ALWAYS_WRITE_COUNTER_BLOCK.with(|flag| flag.set(false));
        allocs
    }

    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_write_at_the_cap_allocates_what_a_write_below_it_allocates() {
        let canary = crate::alloc_probe::Probe::start();
        let sink: Vec<u8> = Vec::with_capacity(8192);
        assert!(
            canary.stop().allocs > 0,
            "the counting allocator is not installed -- rerun with `--features alloc-probe`"
        );
        drop(sink);

        // Below the cap: a bound no write reaches. At the cap: a bound the series is already at.
        let below = per_write(8, 100_000);
        let at_cap = per_write(8, 8);
        eprintln!("  [trim] {below:.2} allocs/write below the cap, {at_cap:.2} at it");
        assert!(
            at_cap - below < 0.3,
            "a write that trims one point allocated {:.2} blocks more than a write that trims none; \
             the trim is collecting its victims into a list",
            at_cap - below
        );
    }
}
