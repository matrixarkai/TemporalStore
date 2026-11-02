// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! TWO FEATURE MODELS SHIPPED WITHOUT THE BOUND THEIR MECHANISM ALREADY HAD.
//!
//! # HIGH-CARDINALITY DISTINCT HAD NO CEILING
//!
//! A distinct ("change") count keeps the exact members of a bucket so the answer is exact. The
//! fixed-size sketch that replaces an oversized member set, its 4,096-register width and the
//! `DistinctBucket::{Exact,Sketch}` stored form all already existed and are already paid for by
//! the index format stamp -- only `control_distinct_sketch` was off. With it off the resident cost
//! of ONE key is O(its cardinality) with no ceiling, which is unshippable on the model whose whole
//! purpose is high cardinality.
//!
//! The assertions below do NOT rest on an estimate. An HLL estimate could coincidentally equal the
//! truth, so a test that asserted "the default answer is wrong" would be a coin flip. They pin the
//! stored REPRESENTATION instead, which `index_shape_for_test` names per bucket: `members=` for an
//! exact set, `sketched=` for a sketch. That distinction exists in the dump precisely because "a
//! dump that silently rounds is worse than one that says which kind of number it is holding".
//!
//! # `SequenceAdd` WAS THE ONE FEATURE WRITE WITH NO HARD MAX
//!
//! `FeatureAppend`, `FeatureAppendWithPolicy` and `FeatureReplace` all refuse a write that would
//! take the series past `FEATURE_ADD_HARD_MAX_SIZE`. `SequenceAdd` lands in the same
//! `shard.features()` series through the same append and the same trim, and had no such arm -- so
//! the one command named for long sequences was the one a caller could hand an unbounded batch to.
//! The trim does not cover it, because the absence costs WORK rather than footprint: every row was
//! appended and a block written for each, and the trim then dropped all but the bound.

use super::*;

/// Comfortably past `DISTINCT_SKETCH_THRESHOLD` (4,096) so the conversion is reached, and small
/// enough that the exact arm is still cheap to build.
const DISTINCT: usize = 5_000;
const BUCKET_MS: u64 = 1_000;
const KEY: &str = "high-cardinality";

fn engine_with(extend: &[(&str, &str)]) -> (TemporalEngine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let mut extend_config = std::collections::BTreeMap::new();
    for (name, value) in extend {
        extend_config.insert((*name).to_string(), (*value).to_string());
    }
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
    (engine, dir)
}

/// Feed `DISTINCT` values that are distinct BY CONSTRUCTION into one bucket of one key.
fn fill_distinct(engine: &TemporalEngine) {
    for index in 0..DISTINCT {
        let outcome = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateChangeAdd {
                key: KEY.to_string(),
                timestamp_ms: BUCKET_MS,
                value: format!("v{index}").into_bytes(),
                precision_ms: None,
                ttl_ms: None,
            },
        });
        assert!(outcome.status.ok, "change add {index} failed: {:?}", outcome.response);
    }
}

/// The dump's line for our key, so the assertion names the representation rather than a count.
fn change_line(engine: &TemporalEngine) -> String {
    let shape = engine.index_shape_for_test(1);
    let line = shape
        .lines()
        .find(|line| line.starts_with("control_change ") && line.contains(KEY))
        .unwrap_or_else(|| {
            panic!("no control_change line for {KEY}; the fill did not land. dump was:\n{shape}")
        });
    line.to_string()
}

#[test]
fn a_high_cardinality_distinct_is_bounded_by_default() {
    let (engine, _dir) = engine_with(&[]);
    fill_distinct(&engine);
    let line = change_line(&engine);
    assert!(
        line.contains("sketched="),
        "A HIGH-CARDINALITY DISTINCT IS UNBOUNDED AS SHIPPED. {DISTINCT} distinct values went into \
         one bucket and it is still holding them exactly, so this key's resident cost is O(its \
         cardinality) with no ceiling. The sketch, its width and its stored form all already \
         exist; only the default was off. Line was: {line}"
    );
    assert!(
        !line.contains("members="),
        "a bucket cannot be both representations at once: {line}"
    );
}

#[test]
fn an_explicit_opt_out_restores_exact_members_at_any_cardinality() {
    let (engine, _dir) = engine_with(&[("control_distinct_sketch", "off")]);
    fill_distinct(&engine);
    let line = change_line(&engine);
    assert!(
        line.contains("members="),
        "THE OPT-OUT IS WHAT MAKES THE DEFAULT SAFE TO FLIP. An operator who needs exact distinct \
         counts at any cardinality must be able to say so, and `flag_or(.., true)` reads an \
         explicitly false value exactly as `flag` did. If this is red the opt-out does not work \
         and the default must not ship. Line was: {line}"
    );
    assert!(
        !line.contains("sketched="),
        "the opt-out arm must not have converted: {line}"
    );
}
