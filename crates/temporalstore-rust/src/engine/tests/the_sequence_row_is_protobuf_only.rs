// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A SEQUENCE ROW IS PROTOBUF ONLY, AND AN UNREADABLE VALUE IS COUNTED.
//!
//! `SequenceAdd` stores the protobuf wire form `FeatureAppend` rows already held and there is no JSON
//! arm left in the reader. The first release has no installed base, so no test here plants an old
//! shape to prove it still loads: the JSON object is the thing that must now be REFUSED.
#![allow(clippy::all)]
use super::*;
use crate::engine::sequence_row_value::decode_sequence_row_value as decode;
use crate::engine::sequence_rows_unreadable;

const SHARD: ShardId = 1;

fn row(timestamp_ms: u64, gid: u64) -> SequenceFeatureRow {
    SequenceFeatureRow {
        timestamp_ms,
        gid,
        action_type: 7,
        duration: 33,
        author_id: 9_001,
    }
}

fn sequence_query(engine: &TemporalEngine, key: &str) -> Vec<SequenceFeatureRow> {
    match engine
        .execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::SequenceQuery {
                key: key.to_string(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: 1000,
                filters: Vec::new(),
            },
        })
        .response
    {
        CommandResponse::SequenceRows { rows } => rows,
        other => panic!("SequenceQuery did not answer with rows: {other:?}"),
    }
}

/// The filtered feature range: the second read path.
fn filtered_points(engine: &TemporalEngine, key: &str, gid: u64) -> Vec<u64> {
    match engine
        .execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::FeatureQueryFiltered {
                key: key.to_string(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: None,
                filters: vec![crate::types::FeatureFilter {
                    field: "gid".to_string(),
                    op: crate::types::FeatureFilterOp::Equal,
                    value: gid,
                }],
            },
        })
        .response
    {
        CommandResponse::FeaturePoints { points } => points.iter().map(|p| p.timestamp_ms).collect(),
        other => panic!("FeatureQueryFiltered did not answer with points: {other:?}"),
    }
}

/// The stored bytes of every point under a key, exactly as the engine hands them back.
fn stored_values(engine: &TemporalEngine, key: &str) -> Vec<(u64, Vec<u8>)> {
    match engine
        .execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::FeatureQuery {
                key: key.to_string(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: None,
            },
        })
        .response
    {
        CommandResponse::FeaturePoints { points } => {
            points.into_iter().map(|p| (p.timestamp_ms, p.value)).collect()
        }
        other => panic!("FeatureQuery did not answer with points: {other:?}"),
    }
}

fn append(engine: &TemporalEngine, key: &str, timestamp_ms: u64, value: Vec<u8>) {
    let response = engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::FeatureAppend {
            key: key.to_string(),
            points: vec![crate::types::FeaturePoint { timestamp_ms, value }],
        },
    });
    assert!(response.status.ok, "{response:?}");
}

fn sequence_add(engine: &TemporalEngine, key: &str, rows: Vec<SequenceFeatureRow>) {
    let response = engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::SequenceAdd { key: key.to_string(), rows },
    });
    assert!(response.status.ok, "{response:?}");
}

#[test]
fn a_sequence_row_codec_reads_protobuf_and_refuses_everything_else() {
    // The extremes of every field.
    let original = SequenceFeatureRow {
        timestamp_ms: 1_700_000_000_123,
        gid: u64::MAX,
        action_type: u32::MAX,
        duration: 0,
        author_id: 9,
    };
    let proto = original.encode_feature_proto_value();
    assert!(serde_json::from_slice::<serde_json::Value>(&proto).is_err());
    assert_eq!(decode(original.timestamp_ms, &proto).unwrap(), original);
    // The instant is the one the value is stored under: the bytes do not carry it.
    assert_eq!(decode(5, &proto).unwrap().timestamp_ms, 5);

    // ---- everything below is a REFUSAL.
    let refused = |bytes: &[u8], what: &str| {
        let error = decode(1, bytes).expect_err(&format!("{what} was read instead of refused"));
        assert!(!error.is_empty(), "{what}: the refusal says nothing");
    };
    // THE JSON OBJECT THE WRITER USED TO STORE IS NOT A ROW ANY MORE.
    refused(&serde_json::to_vec(&original).unwrap(), "the old JSON object");
    refused(b"", "an empty value");
    refused(b"[1,2]", "a JSON array");
    refused(b"not a row", "plain text");
    refused(&proto[..proto.len() - 1], "a truncated protobuf row");
    refused(&proto[..proto.len() - 2], "a protobuf row missing its last field");
    // action_type (field 2) past u32: the decoder narrows it, so it must refuse rather than wrap.
    let mut wide = Vec::new();
    wide.extend([0x08, 0x01, 0x10]);
    wide.extend([0x80, 0x80, 0x80, 0x80, 0x10]); // 1 << 32
    wide.extend([0x18, 0x01, 0x20, 0x01]);
    refused(&wide, "an action_type past u32");
}

#[test]
fn a_sequence_add_row_is_stored_as_protobuf_and_both_read_paths_read_it_across_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let blocks = dir.path().join("blocks");
    let indexes = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-a"), &blocks, &indexes);
    engine.load_shard(SHARD);

    // Rows from the real `SequenceAdd` writer, at a current-era timestamp.
    let first_ms = 1_784_890_000_000u64;
    let written = vec![row(first_ms, 42), row(first_ms + 1, 43), row(first_ms + 2, 42)];
    sequence_add(&engine, "added", written.clone());
    // The same rows from the other producer, `FeatureAppend`.
    for r in &written {
        append(&engine, "appended", r.timestamp_ms, r.encode_feature_proto_value());
    }
    // Values that are not rows.
    append(&engine, "opaque", 3_000, b"not a row".to_vec());
    append(&engine, "opaque", 3_001, vec![0xff, 0xff, 0xff]);

    // THE STORED BYTES, not what a query decodes them to: only the writer can have chosen them.
    let stored = stored_values(&engine, "added");
    assert_eq!(stored.len(), written.len());
    let mut proto_bytes = 0usize;
    let mut json_bytes = 0usize;
    for ((timestamp_ms, value), r) in stored.iter().zip(&written) {
        assert_eq!(*timestamp_ms, r.timestamp_ms);
        assert_eq!(value, &r.encode_feature_proto_value(), "SequenceAdd did not store the protobuf row");
        assert!(serde_json::from_slice::<serde_json::Value>(value).is_err(), "the stored row parses as JSON");
        proto_bytes += value.len();
        json_bytes += serde_json::to_vec(r).unwrap().len();
    }
    println!(
        "SEQUENCE ROW VALUE over {} rows: JSON {json_bytes} B ({:.1} B/row), protobuf {proto_bytes} B ({:.1} B/row), \
         protobuf is {:.1}% of JSON",
        written.len(),
        json_bytes as f64 / written.len() as f64,
        proto_bytes as f64 / written.len() as f64,
        100.0 * proto_bytes as f64 / json_bytes as f64,
    );
    assert!(proto_bytes * 2 < json_bytes, "the protobuf rows are not under half the JSON ones");

    let check = |engine: &TemporalEngine, label: &str| {
        for key in ["added", "appended"] {
            assert_eq!(sequence_query(engine, key), written, "{label}: SequenceQuery over {key}");
            assert_eq!(
                filtered_points(engine, key, 42),
                vec![first_ms, first_ms + 2],
                "{label}: the filtered range over {key}"
            );
        }
    };
    check(&engine, "resident");

    // The unreadable: two values, read once as a sequence. The count moves by exactly two and the
    // result is empty, never a made-up row.
    let before = sequence_rows_unreadable();
    assert!(sequence_query(&engine, "opaque").is_empty());
    assert_eq!(sequence_rows_unreadable() - before, 2, "the unreadable values were not counted");

    engine.flush_shard_index(SHARD);
    drop(engine);
    let restarted = TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-b"), &blocks, &indexes);
    restarted.load_shard(SHARD);
    check(&restarted, "after a restart");
}

/// NO JSON ENCODE OR DECODE PATH IS LEFT IN THE SEQUENCE ROW CODE.
///
/// The test above reads the bytes the real writer stored; this sees an encoder nobody calls YET. It
/// reads the `SequenceAdd` arm and the whole reader module with comment lines removed and asserts
/// neither names `serde_json`. The CONTROL runs the same scan over a string that does, so a scan that
/// could not see the token would fail here instead of passing over a writer.
#[test]
fn no_json_path_is_left_in_the_sequence_row_code() {
    fn uncommented(text: &str) -> String {
        text.lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
    }
    fn arm(source: &str, from: &str, to: &str) -> String {
        let start = source.find(from).unwrap_or_else(|| panic!("no declaration {from}"));
        let rest = &source[start..];
        let end = rest[from.len()..]
            .find(to)
            .unwrap_or_else(|| panic!("no declaration {to} after {from}"))
            + from.len();
        uncommented(&rest[..end])
    }
    // control
    assert!(
        uncommented("// serde_json in a comment\nlet v = serde_json::to_vec(&row);").contains("serde_json"),
        "the scan cannot see a serde_json call"
    );
    assert!(!uncommented("// serde_json in a comment only").contains("serde_json"));

    let writer = arm(
        include_str!("../execute_on_shard.rs"),
        "Command::SequenceAdd { key, rows } => {",
        "Command::SequenceQuery {",
    );
    assert!(writer.len() > 500, "the scan found almost nothing");
    assert!(
        writer.contains("encode_feature_proto_value()"),
        "the SequenceAdd arm does not call the protobuf encoder"
    );
    assert!(
        !writer.contains("serde_json"),
        "the SequenceAdd arm still reaches for serde_json: a JSON encode path survives"
    );
    let reader = uncommented(include_str!("../sequence_row_value.rs"));
    assert!(reader.contains("decode_feature_proto_value"), "the reader does not decode protobuf");
    assert!(!reader.contains("serde_json"), "the sequence row reader still names serde_json");
}
