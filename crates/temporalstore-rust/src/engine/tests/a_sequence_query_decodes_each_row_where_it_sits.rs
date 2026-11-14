// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A SEQUENCE QUERY KEEPS FIVE NUMBERS PER ROW, SO IT SHOULD NOT BUILD A BLOCK PER ROW.
//!
//! The range read used to decode each block it met into a `Vec<FeaturePoint>`, copy the value of
//! the point it wanted into a `Vec<u8>`, keep both in a table keyed by the block's address, and only
//! then decode the row from the copy. Several allocations per row, none of which the answer -- one
//! `SequenceFeatureRow` -- needs. The row is now decoded straight from the value's slice inside the
//! cached block.
//!
//! WHAT MUST NOT MOVE: the rows a query answers, and the count of unreadable values and damaged
//! blocks. A damaged block is counted once per row asked of it; a sound block that merely holds no
//! point at an instant is not damaged and counts nothing.
//!
//! THREE ARMS.
//!   * `an_in_place_read_answers_what_the_decoder_answered` -- the in-place reader against the strict
//!     decoder it replaces, block by block: a sound block, a repeated instant, an unreadable value, an
//!     empty value, every truncation, trailing bytes, a forged count, the JSON shape, an unframed value.
//!   * `a_sequence_query_answers_the_rows_that_were_added` -- through the command surface, with known
//!     rows, a window, a count cap, filters, replaced instants, many rows in one block, and a reload.
//!   * `probe::a_sequence_query_does_not_allocate_per_row` needs `alloc-probe` and `--ignored`.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::engine::packed_blocks::{
    decode_feature_block_strict, encode_feature_block, feature_point_in_place, PointRead,
};
use crate::engine::sequence_row_value::decode_sequence_row_value;
use crate::engine::state::PackedFeatureBlockDecode;
use crate::types::{FeatureFilter, FeatureFilterOp, FeaturePoint};

const SHARD: ShardId = 1;

fn row(timestamp_ms: u64, gid: u64) -> SequenceFeatureRow {
    SequenceFeatureRow {
        timestamp_ms,
        gid,
        action_type: (gid % 5) as u32 + 1,
        duration: 3 + (gid % 11) as u32,
        author_id: 9_000 + gid,
    }
}

fn point_of(row: &SequenceFeatureRow) -> FeaturePoint {
    FeaturePoint {
        timestamp_ms: row.timestamp_ms,
        value: row.encode_feature_proto_value(),
    }
}

/// What a read of one instant in one block came to, spelled so two readers can be compared.
#[derive(Debug, PartialEq)]
enum Answer {
    Row(SequenceFeatureRow),
    UnreadableValue,
    NoPoint,
    DamagedBlock,
}

/// The reader this change replaced: decode the whole block, pick the point, decode its value.
fn decoded_answer(bytes: &[u8], timestamp_ms: u64) -> Answer {
    let value_answer = |value: &[u8]| match decode_sequence_row_value(timestamp_ms, value) {
        Ok(row) => Answer::Row(row),
        Err(_) => Answer::UnreadableValue,
    };
    match decode_feature_block_strict(bytes) {
        PackedFeatureBlockDecode::Packed(points) => points
            .into_iter()
            .find(|point| point.timestamp_ms == timestamp_ms)
            .map(|point| value_answer(&point.value))
            .unwrap_or(Answer::NoPoint),
        PackedFeatureBlockDecode::Legacy => value_answer(bytes),
        PackedFeatureBlockDecode::Corrupt(_) => Answer::DamagedBlock,
    }
}

fn in_place_answer(bytes: &[u8], timestamp_ms: u64) -> Answer {
    match feature_point_in_place(bytes, timestamp_ms, |value| {
        match decode_sequence_row_value(timestamp_ms, value) {
            Ok(row) => Answer::Row(row),
            Err(_) => Answer::UnreadableValue,
        }
    }) {
        PointRead::Found(answer) => answer,
        PointRead::Absent => Answer::NoPoint,
        PointRead::Corrupt => Answer::DamagedBlock,
    }
}

#[test]
fn an_in_place_read_answers_what_the_decoder_answered() {
    let many: Vec<FeaturePoint> = (0..20u64)
        .map(|index| point_of(&row(1_000 + index, index + 1)))
        .collect();
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
    let blocks: Vec<(&str, Vec<u8>)> = vec![
        ("one", encode_feature_block(&[point_of(&row(5, 1))])),
        ("many", encode_feature_block(&many)),
        (
            "a repeated instant keeps the first",
            encode_feature_block(&[
                point_of(&row(9, 1)),
                point_of(&row(9, 2)),
                point_of(&row(10, 3)),
            ]),
        ),
        (
            "an unreadable value among sound ones",
            encode_feature_block(&[
                point_of(&row(1, 1)),
                FeaturePoint { timestamp_ms: 2, value: b"{\"gid\":1}".to_vec() },
                point_of(&row(3, 3)),
            ]),
        ),
        (
            "an empty value",
            encode_feature_block(&[
                FeaturePoint { timestamp_ms: 1, value: Vec::new() },
                point_of(&row(2, 2)),
            ]),
        ),
        ("the JSON shape", json_block),
        ("an unframed value", row(77, 4).encode_feature_proto_value()),
    ];

    let mut compared = 0usize;
    let mut rows = 0usize;
    let mut unreadable = 0usize;
    let mut none = 0usize;
    for (name, block) in &blocks {
        for timestamp in (0..1_030u64).chain([u64::MAX]) {
            let expected = decoded_answer(block, timestamp);
            let got = in_place_answer(block, timestamp);
            assert_eq!(expected, got, "{name}: timestamp {timestamp}");
            compared += 1;
            match got {
                Answer::Row(_) => rows += 1,
                Answer::UnreadableValue => unreadable += 1,
                Answer::NoPoint => none += 1,
                Answer::DamagedBlock => {}
            }
        }
    }
    // DENOMINATORS: every arm of the answer was met, or the comparison proves nothing about it.
    assert!(compared > 7_000);
    assert!(rows >= 20 + 1 + 3 + 2 + 20, "only {rows} reads decoded a row");
    assert!(unreadable >= 2, "only {unreadable} reads met an unreadable value");
    assert!(none > 1_000, "only {none} reads asked a sound block for a point it lacks");

    // EVERY TRUNCATION of a sound block is damaged, and both readers must say so.
    let block = encode_feature_block(&many);
    let mut damaged = 0usize;
    for cut in 0..block.len() {
        let torn = &block[..cut];
        for timestamp in [1_000u64, 1_019, 5_000] {
            assert_eq!(
                decoded_answer(torn, timestamp),
                in_place_answer(torn, timestamp),
                "cut at {cut}, timestamp {timestamp}"
            );
        }
        damaged += (in_place_answer(torn, 1_000) == Answer::DamagedBlock) as usize;
    }
    assert!(
        damaged >= block.len() - 8,
        "only {damaged} of {} truncations were damaged, so the torn arm was not exercised",
        block.len()
    );

    // BYTES AFTER THE LAST POINT, and a count that claims more than the block holds.
    let mut trailing = block.clone();
    trailing.push(0);
    assert_eq!(Answer::DamagedBlock, decoded_answer(&trailing, 1_000));
    assert_eq!(Answer::DamagedBlock, in_place_answer(&trailing, 1_000));
    let mut forged = block.clone();
    let magic = crate::engine::constants::FEATURE_BLOCK_BINARY_MAGIC.len();
    forged[magic..magic + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(Answer::DamagedBlock, decoded_answer(&forged, 1_000));
    assert_eq!(Answer::DamagedBlock, in_place_answer(&forged, 1_000));

    // KNOWN ANSWERS, so the comparison above cannot be two readers wrong the same way.
    assert_eq!(Answer::Row(row(1_003, 4)), in_place_answer(&block, 1_003));
    assert_eq!(Answer::NoPoint, in_place_answer(&block, 1_020));
}

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(SHARD);
    engine
}

fn add(engine: &TemporalEngine, key: &str, rows: Vec<SequenceFeatureRow>) {
    let outcome = engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::SequenceAdd {
            key: key.to_string(),
            rows,
        },
    });
    assert!(outcome.status.ok, "{:?}", outcome.response);
}

fn query(
    engine: &TemporalEngine,
    key: &str,
    start_ms: u64,
    end_ms: u64,
    count: usize,
    filters: Vec<FeatureFilter>,
) -> Vec<SequenceFeatureRow> {
    match engine
        .execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::SequenceQuery {
                key: key.to_string(),
                start_ms,
                end_ms,
                count,
                filters,
            },
        })
        .response
    {
        CommandResponse::SequenceRows { rows } => rows,
        other => panic!("SequenceQuery did not answer with rows: {other:?}"),
    }
}

#[test]
fn a_sequence_query_answers_the_rows_that_were_added() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());

    // One row per write, then forty in ONE write (one block holding many points), then an
    // instant written twice: the second write replaces the first.
    let singles: Vec<SequenceFeatureRow> = (0..30u64).map(|i| row(10_000 + i * 10, i + 1)).collect();
    for single in &singles {
        add(&engine, "s", vec![single.clone()]);
    }
    let batch: Vec<SequenceFeatureRow> = (0..40u64).map(|i| row(20_000 + i, 100 + i)).collect();
    add(&engine, "s", batch.clone());
    add(&engine, "s", vec![row(10_100, 999)]);

    let mut expected: Vec<SequenceFeatureRow> = singles.clone();
    expected.extend(batch.clone());
    for entry in expected.iter_mut() {
        if entry.timestamp_ms == 10_100 {
            *entry = row(10_100, 999);
        }
    }
    expected.sort_by_key(|entry| entry.timestamp_ms);

    let mut compared = 0usize;
    let mut check = |engine: &TemporalEngine, label: &str| {
        // The whole series, a window across both shapes of block, a cap, and a window of nothing.
        assert_eq!(expected, query(engine, "s", 0, u64::MAX, 1_000, vec![]), "{label}: all");
        let window: Vec<_> = expected
            .iter()
            .filter(|entry| entry.timestamp_ms >= 10_250 && entry.timestamp_ms <= 20_010)
            .cloned()
            .collect();
        assert_eq!(window, query(engine, "s", 10_250, 20_010, 1_000, vec![]), "{label}: window");
        assert_eq!(
            expected[..7].to_vec(),
            query(engine, "s", 0, u64::MAX, 7, vec![]),
            "{label}: cap"
        );
        assert!(query(engine, "s", 30_000, 40_000, 10, vec![]).is_empty(), "{label}: empty window");
        assert!(query(engine, "absent", 0, u64::MAX, 10, vec![]).is_empty(), "{label}: absent key");
        // A filter is applied after the cap, as it always was.
        let filter = FeatureFilter {
            field: "action_type".to_string(),
            op: FeatureFilterOp::Equal,
            value: 3,
        };
        let filtered: Vec<_> = expected
            .iter()
            .take(40)
            .filter(|entry| entry.action_type == 3)
            .cloned()
            .collect();
        assert!(!filtered.is_empty());
        assert_eq!(
            filtered,
            query(engine, "s", 0, u64::MAX, 40, vec![filter]),
            "{label}: filter"
        );
        // The batch of several keys answers each key as the single query does.
        let groups = match engine
            .execute(ExecuteRequest {
                shard_id: SHARD,
                command: Command::SequenceBatchQuery {
                    queries: vec![
                        crate::types::SequenceQuerySpec {
                            key: "s".to_string(),
                            start_ms: 0,
                            end_ms: u64::MAX,
                            count: 1_000,
                            filters: vec![],
                        },
                        crate::types::SequenceQuerySpec {
                            key: "absent".to_string(),
                            start_ms: 0,
                            end_ms: u64::MAX,
                            count: 1_000,
                            filters: vec![],
                        },
                    ],
                },
            })
            .response
        {
            CommandResponse::SequenceRowGroups { groups } => groups,
            other => panic!("not groups: {other:?}"),
        };
        assert_eq!(2, groups.len());
        assert_eq!(expected, groups[0].1, "{label}: batch");
        assert!(groups[1].1.is_empty());
        compared += 6;
    };
    check(&engine, "live");
    assert_eq!(70, expected.len());
    assert_eq!(row(10_100, 999), expected[10]);

    // AFTER A RELOAD: a fresh engine over the same directories reads every block cold.
    drop(engine);
    let reloaded = engine_on(dir.path());
    check(&reloaded, "reloaded");
    assert!(compared >= 12, "only {compared} comparisons ran");
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    const OPS: usize = 20;

    /// Allocations per query over `rows` rows written `batch` at a time (one block per write).
    fn per_op(rows: usize, batch: usize, command: impl Fn() -> Command) -> f64 {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        let all: Vec<SequenceFeatureRow> =
            (0..rows as u64).map(|index| row(1_000 + index, index + 1)).collect();
        for chunk in all.chunks(batch) {
            add(&engine, "s", chunk.to_vec());
        }
        let commands: Vec<Command> = (0..OPS).map(|_| command()).collect();
        let probe = Probe::start();
        for command in commands {
            let response = engine.execute(ExecuteRequest {
                shard_id: SHARD,
                command,
            });
            assert!(response.status.ok);
        }
        probe.stop().allocs as f64 / OPS as f64
    }

    fn sequence_command() -> Command {
        Command::SequenceQuery {
            key: "s".to_string(),
            start_ms: 0,
            end_ms: u64::MAX,
            count: usize::MAX,
            filters: Vec::new(),
        }
    }

    /// The control is the range query over the same points, which still builds a point per point.
    fn feature_command() -> Command {
        Command::FeatureQuery {
            key: "s".to_string(),
            start_ms: 0,
            end_ms: u64::MAX,
            count: None,
        }
    }

    /// Allocations a sequence read spends PER ROW, as the slope between two series lengths.
    ///
    /// The slope and not the quotient: the fixed cost of the command divides out of a difference, and
    /// the claim is about what grows with the rows read. The CONTROL must show a per-row cost, or the
    /// probe cannot see work that grows with the series.
    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_sequence_query_does_not_allocate_per_row() {
        let short = per_op(10, 1, sequence_command);
        let long = per_op(100, 1, sequence_command);
        let slope = (long - short) / 90.0;
        let batched_short = per_op(10, 10, sequence_command);
        let batched_long = per_op(100, 10, sequence_command);
        let batched_slope = (batched_long - batched_short) / 90.0;
        let control_short = per_op(10, 1, feature_command);
        let control_long = per_op(100, 1, feature_command);
        let control_slope = (control_long - control_short) / 90.0;
        println!(
            "sequence: {short:.1} @10, {long:.1} @100, {slope:.2}/row; \
             batched by 10: {batched_short:.1} @10, {batched_long:.1} @100, {batched_slope:.2}/row; \
             feature range (control): {control_short:.1} @10, {control_long:.1} @100, \
             {control_slope:.2}/row"
        );
        assert!(
            control_slope >= 2.0,
            "the control spent {control_slope:.2} allocations per row, so the probe cannot see \
             work that grows with the series"
        );
        assert!(
            slope <= control_slope / 2.0,
            "a sequence read spent {slope:.2} allocations per row against {control_slope:.2} for \
             the range read that builds a point per point; it must cost less than half"
        );
        assert!(
            batched_slope <= slope / 2.0,
            "rows sharing a block spent {batched_slope:.2} allocations each against {slope:.2} for \
             rows with a block apiece, so a held block is not being reused"
        );
    }
}
