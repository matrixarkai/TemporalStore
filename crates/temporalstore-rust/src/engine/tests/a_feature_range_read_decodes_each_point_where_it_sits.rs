// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A FEATURE RANGE READ KEEPS ONE VALUE PER POINT IT RETURNS, SO IT SHOULD NOT BUILD A POINT PER POINT
//! OF EVERY BLOCK IT TOUCHES.
//!
//! `FeatureQuery` and `FeatureQueryFiltered` copied each block out of the cache, decoded the whole of
//! it into a `Vec<FeaturePoint>` (a value `Vec` per point of the block, wanted or not), kept that in a
//! table keyed by the block's address, and cloned the wanted point out of it. The filtered read did
//! all of that for every point in the window and then dropped the ones the filters refused. Each
//! point is now read where it sits in the cached block's bytes, a block is walked once however many
//! of its points are asked for, and a value is copied only into a point the answer keeps.
//!
//! WHAT MUST NOT MOVE: the points a read answers, for a window, a count cap and filters; the window
//! is still `count` points BEFORE the filters are applied.
//!
//! THREE ARMS.
//!   * `the_cursor_answers_what_the_decoder_answers` -- the walker over block bytes against the
//!     strict decoder it replaces, for ascending, sparse, descending, repeated and random instants,
//!     and the shapes it must hand to the point-by-point reader: a torn block at every cut, trailing
//!     bytes, a forged count, a repeated or descending instant, the JSON shape, an unframed value.
//!   * `a_range_read_answers_what_was_appended` -- through the command surface, against a model of
//!     the points appended: one block of many points, one-point blocks, replaced instants, windows,
//!     count caps, filters, a trimmed series and a reload.
//!   * `probe::a_feature_range_read_does_not_allocate_per_point_of_the_block` needs `alloc-probe`
//!     and `--ignored`.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::engine::packed_blocks::{
    ascending_cursor, decode_feature_block_strict, encode_feature_block,
};
use crate::engine::state::PackedFeatureBlockDecode;
use crate::types::{FeatureFilter, FeatureFilterOp, FeaturePoint, SequenceFeatureRow};
use std::collections::BTreeMap;

const BASE: u64 = 1_787_270_070_000;

fn row_at(index: u64, gid: u64) -> FeaturePoint {
    let row = SequenceFeatureRow {
        timestamp_ms: BASE + index * 1_000,
        gid,
        action_type: (gid % 5) as u32 + 1,
        duration: 3 + (gid % 11) as u32,
        author_id: 9_000 + gid,
    };
    FeaturePoint {
        timestamp_ms: row.timestamp_ms,
        value: row.encode_feature_proto_value(),
    }
}

/// What the strict decoder answers about one instant of a block.
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

fn plain(timestamp_ms: u64, value: &[u8]) -> FeaturePoint {
    FeaturePoint {
        timestamp_ms,
        value: value.to_vec(),
    }
}

#[test]
fn the_cursor_answers_what_the_decoder_answers() {
    let many: Vec<FeaturePoint> = (0..20u64)
        .map(|index| plain(1_000 + index, format!("value-{index}").as_bytes()))
        .collect();
    let ascending: Vec<(&str, Vec<u8>)> = vec![
        ("many", encode_feature_block(&many)),
        ("one", encode_feature_block(&[plain(5, b"42")])),
        ("empty block", encode_feature_block(&[])),
        (
            "empty values",
            encode_feature_block(&[plain(1_001, b""), plain(1_002, b""), plain(1_007, b"x")]),
        ),
        (
            "a hole",
            encode_feature_block(&[plain(1_000, b"a"), plain(1_010, b"b"), plain(1_019, b"c")]),
        ),
    ];
    // The instants a run of reads can ask for, in the orders a range read, a retry and a damaged
    // index could produce: every instant, strangers around them, backwards, twice each, shuffled.
    let mut orders: Vec<(&str, Vec<u64>)> = vec![
        ("ascending", (1_000..1_020).collect()),
        ("with strangers", (990..1_030).collect()),
        ("descending", (990..1_030).rev().collect()),
        ("twice each", (990..1_030).flat_map(|t| [t, t]).collect()),
    ];
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let shuffled: Vec<u64> = (0..300)
        .map(|_| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            990 + (state >> 33) % 40
        })
        .collect();
    orders.push(("shuffled", shuffled));
    orders.push(("small", vec![5, 5, 4, 6, 1, 5]));

    let mut compared = 0usize;
    let mut present = 0usize;
    let mut walkers = 0usize;
    for (name, block) in &ascending {
        let mut cursor_made = false;
        for (order, instants) in &orders {
            let Some(mut cursor) = ascending_cursor(block) else {
                panic!("{name}: a sound ascending block must get a cursor");
            };
            cursor_made = true;
            for instant in instants {
                let expected = decoded_answer(block, *instant);
                assert_eq!(
                    expected,
                    cursor.value_at(block, *instant).map(|value| value.to_vec()),
                    "{name}, {order}: instant {instant}"
                );
                compared += 1;
                present += expected.is_some() as usize;
            }
        }
        walkers += cursor_made as usize;
    }
    // DENOMINATORS: the found arm and the absent arm were both exercised, on every block.
    assert_eq!(ascending.len(), walkers);
    assert!(present >= 20 * 3 + 6, "only {present} reads found a point");
    assert!(compared - present >= 100, "only {} reads missed", compared - present);

    // EVERY TRUNCATION of a sound block is torn, and none may get a cursor.
    let block = encode_feature_block(&many);
    let mut refused = 0usize;
    for cut in 0..block.len() {
        if ascending_cursor(&block[..cut]).is_none() {
            refused += 1;
        }
    }
    assert_eq!(block.len(), refused, "a torn block was handed a cursor");

    // BYTES AFTER THE LAST POINT, and a count that claims more than the block holds.
    let mut trailing = block.clone();
    trailing.push(0);
    assert!(ascending_cursor(&trailing).is_none());
    let mut forged = block.clone();
    let magic = crate::engine::constants::FEATURE_BLOCK_BINARY_MAGIC.len();
    forged[magic..magic + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(ascending_cursor(&forged).is_none());

    // A REPEATED OR DESCENDING INSTANT is read point by point: first of a repeated instant wins.
    assert!(ascending_cursor(&encode_feature_block(&[plain(9, b"1"), plain(9, b"2"), plain(10, b"3")])).is_none());
    assert!(ascending_cursor(&encode_feature_block(&[plain(9, b"1"), plain(3, b"2")])).is_none());

    // THE JSON SHAPE and an UNFRAMED value.
    let json_block = {
        let mut bytes = crate::engine::constants::FEATURE_BLOCK_MAGIC.to_vec();
        bytes.extend_from_slice(
            &serde_json::to_vec(&crate::engine::state::PackedFeatureBlock {
                version: 1,
                points: many.clone(),
            })
            .expect("the block serialises"),
        );
        bytes
    };
    assert!(ascending_cursor(&json_block).is_none());
    assert!(ascending_cursor(b"123").is_none());
    assert!(ascending_cursor(b"").is_none());
}

fn engine_on(dir: &std::path::Path, bound: usize) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
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

fn query(engine: &TemporalEngine, command: Command) -> Vec<FeaturePoint> {
    let outcome = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(outcome.status.ok, "{:?}", outcome.response);
    match outcome.response {
        CommandResponse::FeaturePoints { points } => points,
        other => panic!("not points: {other:?}"),
    }
}

/// What the model says a window holds: `count` points of it, in order.
fn window(
    model: &BTreeMap<u64, FeaturePoint>,
    start_ms: u64,
    end_ms: u64,
    count: usize,
) -> Vec<FeaturePoint> {
    // BOTH ENDS ARE INCLUDED, and a start past the end is the empty window.
    if start_ms > end_ms {
        return Vec::new();
    }
    model
        .range(start_ms..=end_ms)
        .take(count)
        .map(|(_, point)| point.clone())
        .collect()
}

/// Every window, cap and filter the reads are checked over; returns how many answers it compared and
/// how many of them held a point.
fn check(
    engine: &TemporalEngine,
    model: &BTreeMap<u64, FeaturePoint>,
    bound: usize,
    label: &str,
) -> (usize, usize) {
    let at = |index: u64| BASE + index * 1_000;
    let mut compared = 0usize;
    let mut nonempty = 0usize;
    for (start_ms, end_ms, count) in [
        (0u64, u64::MAX, None),
        (at(50), at(80), None),
        (at(245), at(265), Some(8usize)),
        (at(9), at(13), None),
        (0, u64::MAX, Some(1)),
        (0, u64::MAX, Some(0)),
        (at(999), u64::MAX, None),
        (at(10), at(10), None),
        (at(299), at(300), None),
    ] {
        let expected = window(model, start_ms, end_ms, count.unwrap_or(bound).min(bound));
        let got = query(
            engine,
            Command::FeatureQuery {
                key: "s".to_string(),
                start_ms,
                end_ms,
                count,
            },
        );
        assert_eq!(expected, got, "{label}: FeatureQuery [{start_ms}, {end_ms}] count {count:?}");
        compared += 1;
        nonempty += !got.is_empty() as usize;

        // The same windows through the filtered read. THE COUNT CAPS THE WINDOW BEFORE THE
        // FILTERS, so a cap that cuts through refused points answers fewer than `count`.
        let filters = [
            (vec![], Box::new(|_: &SequenceFeatureRow| true) as Box<dyn Fn(&SequenceFeatureRow) -> bool>),
            (
                vec![FeatureFilter {
                    field: "action_type".to_string(),
                    op: FeatureFilterOp::Equal,
                    value: 1,
                }],
                Box::new(|row: &SequenceFeatureRow| row.action_type == 1),
            ),
            (
                vec![
                    FeatureFilter {
                        field: "gid".to_string(),
                        op: FeatureFilterOp::GreaterOrEqual,
                        value: 100,
                    },
                    FeatureFilter {
                        field: "duration".to_string(),
                        op: FeatureFilterOp::LessThan,
                        value: 9,
                    },
                ],
                Box::new(|row: &SequenceFeatureRow| row.gid >= 100 && (row.duration as u64) < 9),
            ),
            (
                vec![FeatureFilter {
                    field: "gid".to_string(),
                    op: FeatureFilterOp::GreaterThan,
                    value: u64::MAX / 2,
                }],
                Box::new(|row: &SequenceFeatureRow| row.gid > u64::MAX / 2),
            ),
        ];
        for (filters, keeps) in filters {
            let expected: Vec<FeaturePoint> = window(model, start_ms, end_ms, count.unwrap_or(bound).min(bound))
                .into_iter()
                .filter(|point| {
                    let row = SequenceFeatureRow::decode_feature_proto_value(point.timestamp_ms, &point.value)
                        .expect("the model holds rows");
                    keeps(&row)
                })
                .collect();
            let got = query(
                engine,
                Command::FeatureQueryFiltered {
                    key: "s".to_string(),
                    start_ms,
                    end_ms,
                    count,
                    filters,
                },
            );
            assert_eq!(
                expected, got,
                "{label}: FeatureQueryFiltered [{start_ms}, {end_ms}] count {count:?}"
            );
            compared += 1;
            nonempty += !got.is_empty() as usize;
        }
    }
    (compared, nonempty)
}

fn build(engine: &TemporalEngine, model: &mut BTreeMap<u64, FeaturePoint>) {
    let mut put = |points: Vec<FeaturePoint>, model: &mut BTreeMap<u64, FeaturePoint>| {
        for point in &points {
            model.insert(point.timestamp_ms, point.clone());
        }
        append(engine, "s", points);
    };
    // One command of many points, so one block holds many.
    put((0..250).map(|index| row_at(index, index)).collect(), model);
    // One point a command, so each is a block of its own.
    for index in 250..260 {
        put(vec![row_at(index, index)], model);
    }
    put((260..300).map(|index| row_at(index, index)).collect(), model);
    // REPLACED INSTANTS, in the middle of the first block: the newer value must be the one read.
    put(vec![row_at(10, 1_010), row_at(11, 1_011), row_at(200, 1_200)], model);
}

#[test]
fn a_range_read_answers_what_was_appended() {
    let mut compared = 0usize;
    let mut nonempty = 0usize;
    for bound in [100_000usize, 120] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path(), bound);
        let mut model = BTreeMap::new();
        build(&engine, &mut model);
        // A bound below the series drops the OLDEST points; the model drops them the same way.
        while model.len() > bound {
            let oldest = *model.keys().next().expect("a point");
            model.remove(&oldest);
        }
        let (c, n) = check(&engine, &model, bound, &format!("bound {bound}, live"));
        compared += c;
        nonempty += n;
        // AFTER A RELOAD: a fresh engine over the same directories reads every block cold.
        drop(engine);
        let reloaded = engine_on(dir.path(), bound);
        let (c, n) = check(&reloaded, &model, bound, &format!("bound {bound}, reloaded"));
        compared += c;
        nonempty += n;
    }
    // DENOMINATORS: windows that returned nothing prove nothing, and neither do too few of them.
    assert_eq!(2 * 2 * 9 * 5, compared);
    assert!(nonempty >= 40, "only {nonempty} answers held a point");
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    const OPS: usize = 12;

    /// Allocations per operation of `command(j)` over a series of `points` points in blocks of 100.
    fn per_op(points: usize, command: impl Fn(usize, usize) -> Command) -> f64 {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path(), 100_000);
        let mut index = 0usize;
        while index < points {
            let top = (index + 100).min(points);
            append(
                &engine,
                "s",
                (index..top).map(|i| row_at(i as u64, i as u64)).collect(),
            );
            index = top;
        }
        let commands: Vec<Command> = (0..OPS).map(|j| command(points, j)).collect();
        let probe = Probe::start();
        for command in commands {
            let outcome = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            assert!(outcome.status.ok);
        }
        probe.stop().allocs as f64 / OPS as f64
    }

    /// A different count each time, so the response cache never answers a repeat.
    fn all(points: usize, j: usize) -> Command {
        Command::FeatureQuery {
            key: "s".to_string(),
            start_ms: 0,
            end_ms: u64::MAX,
            count: Some(points + 1 + j),
        }
    }

    fn filtered(passing: &'static str) -> impl Fn(usize, usize) -> Command {
        move |points, j| Command::FeatureQueryFiltered {
            key: "s".to_string(),
            start_ms: 0,
            end_ms: u64::MAX,
            count: Some(points + 1 + j),
            filters: match passing {
                // one row in a thousand
                "few" => vec![FeatureFilter {
                    field: "gid".to_string(),
                    op: FeatureFilterOp::Equal,
                    value: 3,
                }],
                // one row in five
                "fifth" => vec![FeatureFilter {
                    field: "action_type".to_string(),
                    op: FeatureFilterOp::Equal,
                    value: 1,
                }],
                _ => vec![],
            },
        }
    }

    /// Allocations a range read spends PER POINT IN THE WINDOW, as the slope between two series
    /// lengths: the fixed cost of the command divides out of a difference.
    ///
    /// THE CONTROL is the unfiltered read, which must still allocate once per point it returns (the
    /// value the answer keeps): if its slope were far below one the probe would not be counting
    /// what a point costs. A filtered read that returns about nothing must spend a small fraction
    /// of that, and a read that returns a fifth of the window must spend about a fifth.
    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_feature_range_read_does_not_allocate_per_point_of_the_block() {
        let slope = |command: &dyn Fn(usize, usize) -> Command| {
            (per_op(1_000, command) - per_op(200, command)) / 800.0
        };
        let unfiltered = slope(&all);
        let few = slope(&filtered("few"));
        let fifth = slope(&filtered("fifth"));
        println!(
            "allocations per point of the window: unfiltered {unfiltered:.2}, filtered keeping \
             one in a thousand {few:.2}, keeping one in five {fifth:.2}"
        );
        assert!(
            unfiltered >= 0.9,
            "the unfiltered read spent {unfiltered:.2} allocations a point, so the probe cannot see \
             the value each returned point owns"
        );
        assert!(
            unfiltered <= 1.3,
            "an unfiltered read spent {unfiltered:.2} allocations a point; the value the point \
             keeps is the one that may remain"
        );
        assert!(
            few <= 0.3,
            "a filtered read that keeps almost nothing spent {few:.2} allocations a point of the \
             window: it built the points the filters refused"
        );
        assert!(
            fifth <= 0.6,
            "a filtered read that keeps a fifth spent {fifth:.2} allocations a point of the window"
        );
    }
}
