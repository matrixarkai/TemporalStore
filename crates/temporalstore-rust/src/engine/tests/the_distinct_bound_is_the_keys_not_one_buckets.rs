// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The distinct bound counts the KEY's exact members, not one bucket's.
//!
//! WHAT WAS WRONG. The conversion from an exact member set to a fixed-size sketch compared ONE
//! bucket's set against `DISTINCT_SKETCH_THRESHOLD`. That bounds a key which keeps its members in
//! a few buckets and does nothing whatever for a key that spreads them over many: a thousand
//! buckets holding fifty members each is fifty thousand exact members with not one bucket within
//! two orders of magnitude of the limit, so nothing ever converted and the memory had no ceiling.
//!
//! A time-bucketed distinct counter spreads its members over many buckets BY CONSTRUCTION -- that
//! is what the bucketing is for -- so the unbounded shape is the ordinary one, not the exotic one.
//!
//! WHAT THE MODEL THIS FOLLOWS DOES. It measures its promotion against the whole key's member
//! total and promotes the KEY, and it amortises the check rather than paying it per write. This
//! does the same, checking on a bucket's creation: the total is a walk of the key's buckets, so
//! per-write would charge every write for the key's width, and a bucket appearing is the only way
//! the total grows that the per-bucket arm cannot already see.
//!
//! WHAT THIS PINS, AND HOW EACH CLAIM FAILS.
//!
//!   * `a_key_spread_over_many_small_buckets_is_still_bounded` -- the claim. Every bucket here
//!     holds 25 members, 1/163rd of the threshold, so the per-bucket rule can NEVER fire; only a
//!     per-key total can. Remove the per-key check and nothing converts and this dies.
//!   * the same test's FIRST assertion is the denominator in the other direction: while the key's
//!     total is still under the threshold, nothing is sketched. Without it, a change that sketched
//!     everything unconditionally would satisfy the main claim.
//!   * `the_bound_does_not_fire_without_the_gate` -- with the gate off, nothing converts however
//!     many members arrive, so the bound cannot be reached by a deployment that asked for exact
//!     distinct counts.
#![allow(clippy::all)]
use super::*;

use crate::engine::hll::{record_change, DistinctBucket, DISTINCT_SKETCH_THRESHOLD};

const SHARD: ShardId = 1;
const KEY: &str = "distinct:spread";
/// Far under `DISTINCT_SKETCH_THRESHOLD` (4096), so no bucket can trip the per-bucket rule.
const PER_BUCKET: usize = 25;
/// Enough buckets that the KEY's total crosses the threshold: 200 * 25 = 5,000 > 4,096.
const BUCKETS: u64 = 200;

fn engine_with_gate(on: bool) -> TemporalEngine {
    let engine = TemporalEngine::default();
    engine.load_shard(SHARD);
    let mut config = Config {
        version: 2,
        ..Config::default()
    };
    if on {
        config
            .extend_config
            .insert("control_distinct_sketch".to_string(), "on".to_string());
    }
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: SHARD,
                config
            })
            .ok,
        "setting the distinct-sketch gate failed"
    );
    // One execute, so the per-execute assignment puts the gate on the shard.
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: SHARD,
                command: Command::ControlStateChangeAdd {
                    key: KEY.to_string(),
                    timestamp_ms: 0,
                    value: b"seed".to_vec(),
                    precision_ms: None,
                    ttl_ms: None,
                },
            })
            .status
            .ok
    );
    engine
}

/// (exact buckets, sketched buckets) for the key.
fn shape(engine: &TemporalEngine) -> (usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&SHARD).expect("the shard is loaded");
    let series = match shard.control_state_changes.get(KEY) {
        Some(series) => series,
        None => return (0, 0),
    };
    let sketched = series
        .values()
        .filter(|bucket| matches!(bucket, DistinctBucket::Sketch(_)))
        .count();
    (series.len() - sketched, sketched)
}

/// Fill buckets `[from, to)` with `PER_BUCKET` distinct members each.
fn fill(engine: &TemporalEngine, from: u64, to: u64) {
    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&SHARD).expect("the shard is loaded");
    for bucket in from..to {
        for member in 0..PER_BUCKET {
            record_change(
                shard,
                KEY,
                // 1-based so bucket 0 stays the seed's own bucket.
                (bucket + 1) * 1_000,
                format!("b{bucket}-m{member}").into_bytes(),
            );
        }
    }
}

#[test]
fn a_key_spread_over_many_small_buckets_is_still_bounded() {
    let engine = engine_with_gate(true);
    {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&SHARD).expect("the shard is loaded");
        assert!(
            shard.control_distinct_sketch,
            "DENOMINATOR: the gate has to be on the shard or nothing can convert"
        );
    }

    // UNDER the threshold: 40 * 25 = 1,000 members. Nothing may convert yet -- otherwise the claim
    // below is satisfied by a change that sketches everything regardless.
    fill(&engine, 0, 40);
    let (exact_before, sketched_before) = shape(&engine);
    assert_eq!(
        0, sketched_before,
        "DENOMINATOR: under the key's threshold nothing converts (exact buckets: {exact_before})"
    );
    assert!(
        exact_before >= 40,
        "DENOMINATOR: the fill has to have created buckets, got {exact_before}"
    );

    // PAST the threshold, still 25 members per bucket. No bucket is anywhere near 4,096, so only a
    // bound that counts the KEY can fire here.
    fill(&engine, 40, BUCKETS);
    let (exact_after, sketched_after) = shape(&engine);
    println!("  after {BUCKETS} buckets x {PER_BUCKET}: exact={exact_after} sketched={sketched_after}");
    assert!(
        sketched_after > 0,
        "a key over the threshold across many small buckets has to be bounded; \
         exact={exact_after} sketched={sketched_after}, and no single bucket ever exceeded \
         {PER_BUCKET} of {DISTINCT_SKETCH_THRESHOLD}"
    );
}

#[test]
fn the_bound_does_not_fire_without_the_gate() {
    let engine = engine_with_gate(false);
    fill(&engine, 0, BUCKETS);
    let (exact, sketched) = shape(&engine);
    println!("  gate off: exact={exact} sketched={sketched}");
    assert_eq!(
        0, sketched,
        "with the gate off a deployment asked for exact distinct counts and must keep them"
    );
    assert!(
        exact > 0,
        "DENOMINATOR: the fill has to have created buckets, got {exact}"
    );
}
