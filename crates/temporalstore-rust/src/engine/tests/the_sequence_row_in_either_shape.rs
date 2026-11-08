// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A SEQUENCE ROW IS READ IN EITHER SHAPE, AND AN UNREADABLE VALUE IS COUNTED.
//!
//! The reader half. `SequenceAdd` still writes the JSON object, so a JSON row below comes from the
//! real writer; a protobuf row comes from `FeatureAppend`, the other producer of feature values,
//! carrying `encode_feature_proto_value`. Before this reader the two were invisible to each
//! other's read paths, and the tests below can only pass if both read paths decode both shapes.
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

/// The feature range read that applies a row filter. It decodes the same values as the sequence
/// query and used to decode only the protobuf shape.
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
fn a_sequence_row_codec_reads_both_shapes_and_refuses_what_it_cannot_read() {
    // The extremes of every field, and a timestamp past 2^53 (which a double cannot hold).
    let original = SequenceFeatureRow {
        timestamp_ms: 1_700_000_000_123,
        gid: u64::MAX,
        action_type: u32::MAX,
        duration: 0,
        author_id: 9,
    };

    let json = serde_json::to_vec(&original).unwrap();
    assert_eq!(json[0], b'{');
    assert_eq!(decode(original.timestamp_ms, &json).unwrap(), original, "the JSON arm changed the row");
    let padded = [b" \n".as_slice(), &json].concat();
    assert_eq!(decode(original.timestamp_ms, &padded).unwrap(), original, "leading whitespace is JSON");

    let proto = original.encode_feature_proto_value();
    assert!(proto[0] != b'{' && !proto[0].is_ascii_whitespace());
    assert!(serde_json::from_slice::<serde_json::Value>(&proto).is_err());
    assert_eq!(decode(original.timestamp_ms, &proto).unwrap(), original, "the protobuf arm changed the row");

    // The two arms take the instant from different places: JSON carries its own, protobuf is stored
    // under one. A reader that confused them would return the wrong instant here.
    assert_eq!(decode(5, &json).unwrap().timestamp_ms, original.timestamp_ms);
    assert_eq!(decode(5, &proto).unwrap().timestamp_ms, 5);

    // ---- everything below is a REFUSAL.
    let refused = |bytes: &[u8], what: &str| {
        let error = decode(1, bytes).expect_err(&format!("{what} was read instead of refused"));
        assert!(!error.is_empty(), "{what}: the refusal says nothing");
    };
    refused(b"", "an empty value");
    refused(b"{\"gid\":1", "truncated JSON");
    refused(b"{\"gid\":1}", "JSON missing four fields");
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
fn both_read_paths_decode_both_shapes_across_a_restart_and_count_the_unreadable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let blocks = dir.path().join("blocks");
    let indexes = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-a"), &blocks, &indexes);
    engine.load_shard(SHARD);

    // JSON rows, from the real `SequenceAdd` writer.
    sequence_add(&engine, "json", vec![row(1_000, 42), row(1_001, 43), row(1_002, 42)]);
    // Protobuf rows, from the `FeatureAppend` producer. Only a reader that decodes this shape can
    // answer a sequence query for this key at all: it returned nothing before.
    for (timestamp_ms, gid) in [(2_000u64, 100u64), (2_001, 101), (2_002, 100)] {
        append(&engine, "proto", timestamp_ms, row(timestamp_ms, gid).encode_feature_proto_value());
    }
    // Values that are neither shape.
    append(&engine, "opaque", 3_000, b"not a row".to_vec());
    append(&engine, "opaque", 3_001, vec![0xff, 0xff, 0xff]);

    let check = |engine: &TemporalEngine, label: &str| {
        let json_rows = sequence_query(engine, "json");
        assert_eq!(
            json_rows.iter().map(|r| (r.timestamp_ms, r.gid)).collect::<Vec<_>>(),
            vec![(1_000, 42), (1_001, 43), (1_002, 42)],
            "{label}: the JSON rows"
        );
        let proto_rows = sequence_query(engine, "proto");
        assert_eq!(
            proto_rows.iter().map(|r| (r.timestamp_ms, r.gid, r.author_id)).collect::<Vec<_>>(),
            vec![(2_000, 100, 9_001), (2_001, 101, 9_001), (2_002, 100, 9_001)],
            "{label}: the protobuf rows, with the instant taken from where each is stored"
        );
        // The filtered feature range is the second read path.
        assert_eq!(filtered_points(engine, "json", 42), vec![1_000, 1_002], "{label}: filter over JSON rows");
        assert_eq!(filtered_points(engine, "proto", 100), vec![2_000, 2_002], "{label}: filter over protobuf rows");
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
