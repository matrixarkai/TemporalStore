// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A SUM OVER A FEATURE SERIES KEEPS A NUMBER PER POINT, SO IT SHOULD NOT BUILD A POINT PER POINT.
//!
//! The raw sum read every point through the reader a range query uses: copy the cached block out,
//! decode it into a `Vec<FeaturePoint>`, pick one point, copy its value into a `Vec<u8>`, and
//! collect every value of the window into a `Vec<Vec<u8>>` before adding them. Six allocations per
//! point (608 for a hundred-point sum), none of which the answer -- one integer -- needs. The
//! sum now borrows each value from the cached block and adds it where it sits.
//!
//! THREE ARMS.
//!   * `with_feature_point_value_answers_what_the_decoder_answers` -- the in-place reader against
//!     the strict decoder it replaces, over well-formed blocks, a repeated timestamp, every
//!     truncation of a block, bytes after the last point, a forged count, the JSON shape and an
//!     unframed value. A damaged block must be refused by both or by neither.
//!   * `a_sum_answers_what_the_collected_values_summed_to` -- through the command surface: the
//!     sum against the same points read by `FeatureQuery` (a path this change does not touch) and
//!     summed with the retained `aggregate_feature_values`, over an empty series, one point, many,
//!     negatives, values that are not integers (NaN, inf, text, whitespace), a window, a count cap,
//!     with the rollup ladder on and off, after a trim and after a reload.
//!   * `page_keys_equal_the_constructors_keys` -- the keys the run builds are the keys
//!     `CacheKey::page_with_slot` builds, field for field. A changed byte is a silent cache miss.
//!   * `probe::a_feature_sum_does_not_allocate_per_point` needs `alloc-probe` and `--ignored`.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::engine::packed_blocks::{
    decode_feature_block_strict, encode_feature_block, with_feature_point_value,
};
use crate::engine::state::{PackedFeatureBlock, PackedFeatureBlockDecode};
use crate::types::FeaturePoint;

fn point(timestamp_ms: u64, value: &[u8]) -> FeaturePoint {
    FeaturePoint {
        timestamp_ms,
        value: value.to_vec(),
    }
}

/// What the replaced reader answered about the same bytes.
fn decoded_answer(bytes: &[u8], timestamp_ms: u64) -> Option<Vec<u8>> {
    match decode_feature_block_strict(bytes) {
        PackedFeatureBlockDecode::Packed(points) => points
            .into_iter()
            .find(|point| point.timestamp_ms == timestamp_ms)
            .map(|point| point.value),
        PackedFeatureBlockDecode::Legacy => Some(bytes.to_vec()),
        PackedFeatureBlockDecode::Corrupt(_) => None,
    }
}

fn in_place_answer(bytes: &[u8], timestamp_ms: u64) -> Option<Vec<u8>> {
    with_feature_point_value(bytes, timestamp_ms, |value| value.to_vec())
}

#[test]
fn with_feature_point_value_answers_what_the_decoder_answers() {
    let many: Vec<FeaturePoint> = (0..20u64)
        .map(|index| point(1_000 + index, format!("{}", index as i64 - 7).as_bytes()))
        .collect();
    let blocks: Vec<(&str, Vec<u8>)> = vec![
        ("empty", encode_feature_block(&[])),
        ("one", encode_feature_block(&[point(5, b"42")])),
        ("many", encode_feature_block(&many)),
        (
            "repeated timestamp keeps the first",
            encode_feature_block(&[point(9, b"1"), point(9, b"2"), point(10, b"3")]),
        ),
        (
            "empty values",
            encode_feature_block(&[point(1, b""), point(2, b"7")]),
        ),
    ];

    let mut compared = 0usize;
    let mut present = 0usize;
    for (name, block) in &blocks {
        // Every timestamp the block holds, one before it, one after it, and a stranger.
        for timestamp in (0..1_030u64).chain([u64::MAX]) {
            let expected = decoded_answer(block, timestamp);
            assert_eq!(
                expected,
                in_place_answer(block, timestamp),
                "{name}: timestamp {timestamp}"
            );
            compared += 1;
            present += expected.is_some() as usize;
        }
    }
    // DENOMINATORS: a run that never found a point proves nothing about the found arm.
    assert!(present >= 20 + 1 + 2 + 2, "only {present} reads found a point");
    assert!(compared > 5_000);

    // EVERY TRUNCATION of a well-formed block is torn, and both readers must refuse it.
    let block = encode_feature_block(&many);
    let mut refused = 0usize;
    for cut in 0..block.len() {
        let torn = &block[..cut];
        for timestamp in [1_000u64, 1_019, 5_000] {
            assert_eq!(
                decoded_answer(torn, timestamp),
                in_place_answer(torn, timestamp),
                "cut at {cut}, timestamp {timestamp}"
            );
        }
        refused += decoded_answer(torn, 1_000).is_none() as usize;
    }
    assert!(
        refused >= block.len() - 8,
        "only {refused} of {} truncations were refused, so the torn arm was not exercised",
        block.len()
    );

    // BYTES AFTER THE LAST POINT, and a count that claims more than the block holds.
    let mut trailing = block.clone();
    trailing.push(0);
    assert_eq!(None, decoded_answer(&trailing, 1_000));
    assert_eq!(None, in_place_answer(&trailing, 1_000));
    let mut forged = block.clone();
    let magic = crate::engine::constants::FEATURE_BLOCK_BINARY_MAGIC.len();
    forged[magic..magic + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(None, decoded_answer(&forged, 1_000));
    assert_eq!(None, in_place_answer(&forged, 1_000));

    // THE JSON SHAPE and an UNFRAMED value, which keep the allocating reader.
    let json_block = {
        let mut bytes = crate::engine::constants::FEATURE_BLOCK_MAGIC.to_vec();
        bytes.extend_from_slice(
            &serde_json::to_vec(&PackedFeatureBlock {
                version: 1,
                points: many.clone(),
            })
            .expect("the block serialises"),
        );
        bytes
    };
    assert_eq!(Some(b"-7".to_vec()), decoded_answer(&json_block, 1_000));
    assert_eq!(Some(b"-7".to_vec()), in_place_answer(&json_block, 1_000));
    assert_eq!(None, in_place_answer(&json_block, 4));
    assert_eq!(Some(b"123".to_vec()), in_place_answer(b"123", 77));
    assert_eq!(decoded_answer(b"123", 77), in_place_answer(b"123", 77));
}

fn engine_on(dir: &std::path::Path, rollup: bool, bound: usize) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    let mut extend_config = std::collections::BTreeMap::new();
    if rollup {
        extend_config.insert("control_rollup".to_string(), "on".to_string());
    }
    assert!(
        engine
            .set_config(crate::control::SetConfigRequest {
                shard_id: 1,
                config: crate::control::Config {
                    version: 2,
                    feature_max_size: bound,
                    extend_config,
                    ..Default::default()
                },
            })
            .ok
    );
    engine
}

fn append(engine: &TemporalEngine, key: &str, points: Vec<FeaturePoint>) {
    let outcome = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: key.to_string(),
            points,
        },
    });
    assert!(outcome.status.ok, "{:?}", outcome.response);
}

fn sum(engine: &TemporalEngine, key: &str, start_ms: u64, end_ms: u64, count: Option<usize>) -> i64 {
    match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAggQuery {
                key: key.to_string(),
                start_ms,
                end_ms,
                aggregator: "sum".to_string(),
                count,
            },
        })
        .response
    {
        CommandResponse::Aggregate { value } => value,
        other => panic!("not an aggregate: {other:?}"),
    }
}

/// The sum as the collected values summed to: the points `FeatureQuery` reads (a path this change
/// does not touch), through the retained aggregate.
fn collected_sum(
    engine: &TemporalEngine,
    key: &str,
    start_ms: u64,
    end_ms: u64,
    count: Option<usize>,
) -> (i64, usize) {
    match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery {
                key: key.to_string(),
                start_ms,
                end_ms,
                count,
            },
        })
        .response
    {
        CommandResponse::FeaturePoints { points } => {
            let values: Vec<Vec<u8>> = points.into_iter().map(|point| point.value).collect();
            (aggregate_feature_values(&values, "sum"), values.len())
        }
        other => panic!("not points: {other:?}"),
    }
}

#[test]
fn a_sum_answers_what_the_collected_values_summed_to() {
    let mixed: Vec<FeaturePoint> = {
        let texts: [&[u8]; 12] = [
            b"12345678", b"-9", b"0", b"NaN", b"inf", b"-inf", b"  5", b"7 ", b"abc", b"",
            b"9000000000000000000", b"1.5",
        ];
        texts
            .iter()
            .enumerate()
            .map(|(index, text)| point(10_000 + index as u64 * 10, text))
            .collect()
    };
    let many: Vec<FeaturePoint> = (0..150u64)
        .map(|index| point(50_000 + index, format!("{}", index as i64 - 60).as_bytes()))
        .collect();

    let mut compared = 0usize;
    let mut nonzero = 0usize;
    for rollup in [false, true] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path(), rollup, 100_000);
        append(&engine, "one", vec![point(100, b"-41")]);
        append(&engine, "mixed", mixed.clone());
        append(&engine, "many", many.clone());

        let mut check = |engine: &TemporalEngine, label: &str| {
            let mut local = 0usize;
            for (key, start, end, count) in [
                ("absent", 0u64, u64::MAX, None),
                ("one", 0, u64::MAX, None),
                ("one", 101, u64::MAX, None),
                ("mixed", 0, u64::MAX, None),
                ("mixed", 10_020, 10_070, None),
                ("many", 0, u64::MAX, None),
                ("many", 50_010, 50_090, None),
            ] {
                let (expected, _) = collected_sum(engine, key, start, end, count);
                assert_eq!(
                    expected,
                    sum(engine, key, start, end, count),
                    "rollup={rollup} {label}: {key} [{start}, {end}]"
                );
                local += 1;
                nonzero += (expected != 0) as usize;
            }
            // A count cap is the raw path's alone: the rollup serves only an uncapped window.
            for (key, count) in [("many", 1usize), ("many", 7), ("mixed", 5)] {
                let (expected, taken) = collected_sum(engine, key, 0, u64::MAX, Some(count));
                assert_eq!(count, taken, "{key}: the cap took {taken}");
                assert_eq!(
                    expected,
                    sum(engine, key, 0, u64::MAX, Some(count)),
                    "rollup={rollup} {label}: {key} capped at {count}"
                );
                local += 1;
            }
            local
        };
        compared += check(&engine, "live");

        // Known arithmetic, so the comparison above cannot be two readers wrong the same way.
        assert_eq!(-41, sum(&engine, "one", 0, u64::MAX, None));
        assert_eq!(
            (0..150i64).map(|index| index - 60).sum::<i64>(),
            sum(&engine, "many", 0, u64::MAX, None)
        );
        assert_eq!(0, sum(&engine, "absent", 0, u64::MAX, None));

        // AFTER A RELOAD: a fresh engine over the same directories reads every block cold.
        drop(engine);
        let reloaded = engine_on(dir.path(), rollup, 100_000);
        compared += check(&reloaded, "reloaded");
        assert_eq!(-41, sum(&reloaded, "one", 0, u64::MAX, None));
    }

    // AFTER A TRIM: a bound far below the series drops the oldest points; the sum is of the rest.
    for rollup in [false, true] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path(), rollup, 20);
        append(&engine, "many", many.clone());
        let (expected, taken) = collected_sum(&engine, "many", 0, u64::MAX, None);
        assert!(taken <= 20 && taken > 0, "the bound kept {taken} points");
        assert_eq!(expected, sum(&engine, "many", 0, u64::MAX, None), "trimmed rollup={rollup}");
        compared += 1;
        drop(engine);
        let reloaded = engine_on(dir.path(), rollup, 20);
        assert_eq!(
            expected,
            sum(&reloaded, "many", 0, u64::MAX, None),
            "trimmed and reloaded rollup={rollup}"
        );
        compared += 1;
    }

    // DENOMINATORS: a run that compared few windows, or only zeros, proves nothing.
    assert!(compared >= 2 * 2 * 10 + 4, "only {compared} comparisons ran");
    assert!(nonzero >= 12, "only {nonzero} windows summed to something other than 0");
}

#[test]
fn page_keys_equal_the_constructors_keys() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path(), false, 100_000);
    // Values of growing width so offsets and lengths take one, two and many digits and the blocks
    // spill across more than one slab position.
    for field in 0..120usize {
        let outcome = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: "h".to_string(),
                field: format!("f{field:03}"),
                value: vec![b'v'; 1 + field * 37],
            },
        });
        assert!(outcome.status.ok);
    }
    let addresses: Vec<crate::ElementEntry> = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        shard
            .bucket_index.hashes
            .get("h")
            .expect("the hash exists")
            .iter()
            .map(|(_, address)| address.clone())
            .collect()
    };
    assert_eq!(120, addresses.len());

    let mut keys = crate::engine::PageKeys::new();
    let mut slabs = std::collections::BTreeSet::new();
    let mut shared = 0usize;
    for bucket in [None, Some(0u32), Some(7), Some(65_535), Some(u32::MAX)] {
        for address in &addresses {
            let built = keys.key(1, address, bucket);
            let expected = matrixcache::CacheKey::page_with_slot(
                1,
                address.block_slab_id(),
                address.offset(),
                address.length(),
                bucket,
            );
            assert_eq!(expected, built, "bucket {bucket:?}");
            assert_eq!(expected.record_key, built.record_key);
            assert_eq!(expected.selector, built.selector);
            assert_eq!(expected.namespace, built.namespace);
            assert_eq!(
                built.selector.len(),
                match &built.selector {
                    std::borrow::Cow::Owned(selector) => selector.capacity(),
                    std::borrow::Cow::Borrowed(selector) => selector.len(),
                },
                "the selector holds slack: it is resident as long as its entry is"
            );
            slabs.insert(address.block_slab_id());
        }
    }
    // THE SHARING HAPPENED: consecutive keys of one slab hold ONE record-key allocation.
    let first = keys.key(1, &addresses[0], Some(1));
    let second = keys.key(1, &addresses[1], Some(1));
    if addresses[0].block_slab_id() == addresses[1].block_slab_id() {
        assert!(
            std::sync::Arc::ptr_eq(&first.record_key, &second.record_key),
            "two blocks of one slab built two record keys"
        );
        shared += 1;
    }
    assert_eq!(1, shared, "the fixture never put two blocks in one slab, so sharing was not tested");
    // A different slab must NOT reuse the held record key.
    if let Some(other) = addresses
        .iter()
        .find(|address| address.block_slab_id() != addresses[0].block_slab_id())
    {
        let third = keys.key(1, other, Some(1));
        assert!(!std::sync::Arc::ptr_eq(&first.record_key, &third.record_key));
        assert_ne!(first.record_key, third.record_key);
    }
    assert!(!slabs.is_empty());
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    const OPS: usize = 20;

    fn per_op(points: usize, command: impl Fn() -> Command) -> f64 {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path(), false, 100_000);
        for chunk in 0..points {
            append(&engine, "s", vec![point(1_000 + chunk as u64, b"12345678")]);
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
        probe.stop().allocs as f64 / OPS as f64
    }

    fn sum_command() -> Command {
        Command::FeatureAggQuery {
            key: "s".to_string(),
            start_ms: 0,
            end_ms: u64::MAX,
            aggregator: "sum".to_string(),
            count: None,
        }
    }

    fn query_command() -> Command {
        Command::FeatureQuery {
            key: "s".to_string(),
            start_ms: 0,
            end_ms: u64::MAX,
            count: None,
        }
    }

    /// Allocations a sum spends PER POINT, as the slope between two series lengths.
    ///
    /// The slope and not the quotient: the fixed cost of the command (the key, the response, the
    /// window bounds) divides out of a difference, and the claim is about what grows with the
    /// series. The CONTROL is the range query over the same points, which still builds a point per
    /// point: it must show a per-point cost, or the probe is blind to this dimension.
    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_feature_sum_does_not_allocate_per_point() {
        let sum_short = per_op(10, sum_command);
        let sum_long = per_op(100, sum_command);
        let sum_slope = (sum_long - sum_short) / 90.0;
        let query_short = per_op(10, query_command);
        let query_long = per_op(100, query_command);
        let query_slope = (query_long - query_short) / 90.0;
        println!(
            "sum: {sum_short:.1} @10, {sum_long:.1} @100, {sum_slope:.2}/point; \
             query (control): {query_short:.1} @10, {query_long:.1} @100, {query_slope:.2}/point"
        );
        assert!(
            query_slope >= 2.0,
            "the control spent {query_slope:.2} allocations per point, so the probe cannot see \
             work that grows with the series"
        );
        assert!(
            sum_slope <= 1.5,
            "a sum spent {sum_slope:.2} allocations per point; the page key's selector is the one \
             that may remain"
        );
    }
}
