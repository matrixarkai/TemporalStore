// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A HIGH-CARDINALITY DISTINCT ACTUALLY HOLDS, COUNTED RATHER THAN SAMPLED.
//!
//! # WHY THIS EXISTS: THE FIRST INSTRUMENT WAS INVALID
//!
//! The footprint of this path was first measured as a resident-set delta around the writes, in a
//! separate process per arm. That instrument does not work, and its own sweep proved it: the exact
//! arm read 31.13, then 138.61, then 46.12 bytes per value at 12,500 / 25,000 / 50,000 — not
//! monotonic — the sketched arm read HIGHER than the exact one at 12,500, and at 50,000 it
//! reported **zero growth**. Resident size follows allocator high-water marks and page reuse, so a
//! single sample of it was read as a 65% saving that the next two samples contradicted.
//!
//! So this counts the bytes the shard is holding, from the shard. `DistinctBucket` is either
//! `Exact(BTreeSet<Vec<u8>>)` — every member, kept — or `Sketch(Hll)`, whose `registers` is
//! exactly `1 << 12` = 4,096 `u8`. Both are countable, the count is the same in debug and
//! release, and no allocator is involved in the answer.
//!
//! # THE CLAIM UNDER TEST
//!
//! Unbounded, one key's exact set grows with its cardinality and has no ceiling. Bounded — which
//! is the native design, converting at 4,096 per bucket to a fixed-size sketch — the members are
//! dropped and the held bytes stop moving. Two cardinalities above the threshold are the minimum
//! that can tell "flat" from "smaller", which the invalid instrument could not.
//!
//! # WHAT IT COSTS, MEASURED IN THE SAME RUN
//!
//! Exactness past the threshold. The accuracy column is reported beside the footprint so the
//! trade is visible in one table rather than argued in prose.

use super::*;

/// Both above `DISTINCT_SKETCH_THRESHOLD` (4,096), so the bounded arm has converted at each and a
/// flat line means a ceiling rather than an unreached threshold.
const CARDINALITIES: [usize; 2] = [8_192, 16_384];
const BUCKET_MS: u64 = 1_000;
const KEY: &str = "high-cardinality";
/// `1 << P` with `P = 12`, the register count the sketch is defined with.
const SKETCH_REGISTERS: usize = 4_096;

struct Census {
    buckets: usize,
    exact_buckets: usize,
    sketched_buckets: usize,
    members_held: usize,
    member_payload_bytes: usize,
    sketch_bytes: usize,
    answer: i64,
}

fn drive(gate_on: bool, distinct: usize) -> Census {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let mut extend_config = std::collections::BTreeMap::new();
    // Spelled in BOTH arms, so the figures never depend on what the default happens to be —
    // which is the thing this measurement exists to inform and would otherwise silently become
    // the control.
    extend_config.insert(
        "control_distinct_sketch".to_string(),
        if gate_on { "on" } else { "off" }.to_string(),
    );
    assert!(
        engine
            .set_config(crate::control::SetConfigRequest {
                shard_id: 1,
                config: crate::control::Config {
                    version: 2,
                    extend_config,
                    ..Default::default()
                },
            })
            .ok,
        "the gate under test has to be the one that is set"
    );

    for index in 0..distinct {
        let outcome = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateChangeAdd {
                key: KEY.to_string(),
                timestamp_ms: BUCKET_MS,
                // Distinct by construction, and fixed width so the payload total is N * len and
                // a reader can check the arithmetic by hand.
                value: format!("{index:012}").into_bytes(),
                precision_ms: None,
                ttl_ms: None,
            },
        });
        assert!(outcome.status.ok, "change add {index} failed: {:?}", outcome.response);
    }

    let answer = match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateQuery {
                key: KEY.to_string(),
                start_ms: 0,
                end_ms: BUCKET_MS * 2,
                aggregator: "change".to_string(),
            },
        })
        .response
    {
        CommandResponse::Integer { value } => value,
        other => panic!("expected Integer, got {other:?}"),
    };

    // THE CENSUS, FROM THE SHARD. Not a resident-set delta: these are the containers themselves.
    // `shards` is a std `RwLock`, so `read()` yields a `Result` -- unwrapped here because a
    // poisoned lock means another test panicked and this measurement is void anyway.
    let shards = engine.shards.read().unwrap();
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let series = shard
        .control_state_changes
        .get(KEY)
        .expect("the key must be present; if it is not, the writes did not land");
    let mut census = Census {
        buckets: 0,
        exact_buckets: 0,
        sketched_buckets: 0,
        members_held: 0,
        member_payload_bytes: 0,
        sketch_bytes: 0,
        answer,
    };
    for (_bucket_ms, bucket) in series.iter() {
        census.buckets += 1;
        match bucket {
            crate::engine::hll::DistinctBucket::Exact(members) => {
                census.exact_buckets += 1;
                census.members_held += members.len();
                census.member_payload_bytes += members.iter().map(|m| m.len()).sum::<usize>();
            }
            crate::engine::hll::DistinctBucket::Sketch(_) => {
                census.sketched_buckets += 1;
                // The register array is the whole of a sketch's footprint and its width is fixed
                // by `P`, so this is arithmetic rather than a measurement.
                census.sketch_bytes += SKETCH_REGISTERS;
            }
        }
    }
    census
}

#[test]
fn an_unbounded_distinct_holds_every_member_and_the_bound_holds_a_fixed_sketch() {
    println!(
        "{:<10} {:<8} {:<9} {:<9} {:<14} {:<14} {:<12} {:<10} {}",
        "arm", "N", "buckets", "sketched", "members held", "payload B", "sketch B", "answer", "err %"
    );
    let mut unbounded_held = Vec::new();
    let mut bounded_held = Vec::new();
    for distinct in CARDINALITIES {
        for gate_on in [false, true] {
            let c = drive(gate_on, distinct);
            let err = (c.answer - distinct as i64) as f64 * 100.0 / distinct as f64;
            println!(
                "{:<10} {:<8} {:<9} {:<9} {:<14} {:<14} {:<12} {:<10} {:+.3}",
                if gate_on { "bounded" } else { "unbounded" },
                distinct,
                c.buckets,
                c.sketched_buckets,
                c.members_held,
                c.member_payload_bytes,
                c.sketch_bytes,
                c.answer,
                err
            );
            // Every value went to ONE instant, so there is one bucket and the sweep varies
            // cardinality alone. Asserted, because a fixture that silently spread them over many
            // buckets would make both arms look bounded.
            assert_eq!(c.buckets, 1, "the fixture must drive exactly one bucket");
            if gate_on {
                assert_eq!(c.sketched_buckets, 1, "at {distinct} the bucket must have converted");
                assert_eq!(
                    c.members_held, 0,
                    "THE CONVERSION IS NOT A CACHE BESIDE THE MEMBERS: a sketched bucket still \
                     holding {} members means the exact set was not dropped, and the footprint \
                     claim is void",
                    c.members_held
                );
                bounded_held.push(c.sketch_bytes + c.member_payload_bytes);
            } else {
                assert_eq!(
                    c.members_held, distinct,
                    "unbounded, the bucket must hold every member it was shown"
                );
                assert_eq!(c.sketched_buckets, 0, "nothing may convert with the gate off");
                unbounded_held.push(c.sketch_bytes + c.member_payload_bytes);
            }
        }
    }

    // THE TWO CLAIMS, AS ARITHMETIC OVER THE TWO CARDINALITIES.
    assert!(
        unbounded_held[1] > unbounded_held[0],
        "unbounded must GROW with cardinality: {} at {} then {} at {}",
        unbounded_held[0], CARDINALITIES[0], unbounded_held[1], CARDINALITIES[1]
    );
    assert_eq!(
        bounded_held[0], bounded_held[1],
        "BOUNDED MUST BE FLAT, NOT MERELY SMALLER. Held bytes moved from {} to {} when \
         cardinality doubled from {} to {}, so this is a reduction and not a ceiling, and the \
         word 'bounded' may not be used for it",
        bounded_held[0], bounded_held[1], CARDINALITIES[0], CARDINALITIES[1]
    );
    assert_eq!(
        bounded_held[0], SKETCH_REGISTERS,
        "a bounded bucket's whole footprint is its register array"
    );
    println!(
        "\nunbounded held {} B at {} and {} B at {}; bounded held {} B at both",
        unbounded_held[0], CARDINALITIES[0], unbounded_held[1], CARDINALITIES[1], bounded_held[0]
    );
    println!(
        "ratio at {}: {:.1}x",
        CARDINALITIES[1],
        unbounded_held[1] as f64 / bounded_held[1] as f64
    );
}
