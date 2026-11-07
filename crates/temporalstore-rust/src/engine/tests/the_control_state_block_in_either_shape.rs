// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A CONTROL STATE SERIES BLOCK IS READ IN EITHER SHAPE, AND AN UNREADABLE ONE IS COUNTED.
//!
//! The reader half: the writer still stores JSON, so the packed blocks below are built by the
//! test-only encoder and put into the block store through the same append the real writer uses.
//! `append_control_state_block` is that shared tail, split out of `persist_control_state_block` so
//! this test and the writer cannot disagree about how a block reaches the index.
#![allow(clippy::all)]
use super::*;
use crate::engine::control_state_block::{
    control_state_blocks_unreadable, encode_control_state_block, CONTROL_STATE_BLOCK_PACKED,
};
use std::collections::BTreeMap;

const SHARD: ShardId = 1;

fn series(points: &[(u64, i64)]) -> BTreeMap<u64, i64> {
    points.iter().copied().collect()
}

fn count(engine: &TemporalEngine, key: &str) -> CommandResponse {
    engine
        .execute(crate::types::ExecuteRequest {
            shard_id: SHARD,
            command: crate::types::Command::ControlStateCount {
                key: key.to_string(),
                start_ms: 0,
                end_ms: u64::MAX,
            },
        })
        .response
}

#[test]
fn a_series_block_codec_reads_both_shapes_and_refuses_what_it_cannot_read() {
    use crate::engine::control_state_block::decode_control_state_block as decode;
    // Negative values, a timestamp past 2^53 (which a double cannot hold) and the extremes.
    let original = series(&[(0, -1), (10, 7), (1_700_000_000_123, i64::MIN), (u64::MAX, i64::MAX)]);

    let json = serde_json::to_vec(&original).unwrap();
    assert_eq!(json[0], b'{');
    assert_eq!(decode(&json).unwrap(), original, "the JSON arm changed the series");

    let packed = encode_control_state_block(&original);
    assert_eq!(packed[0], CONTROL_STATE_BLOCK_PACKED);
    assert!(serde_json::from_slice::<serde_json::Value>(&packed).is_err());
    assert_eq!(decode(&packed).unwrap(), original, "the packed arm changed the series");

    // An empty series is a series (`{}` and the bare container byte), not an error.
    assert_eq!(decode(b"{}").unwrap(), BTreeMap::new());
    assert_eq!(decode(&encode_control_state_block(&BTreeMap::new())).unwrap(), BTreeMap::new());

    // ---- everything below is a REFUSAL, and none may come back as an empty series.
    let refused = |bytes: &[u8], what: &str| {
        let error = decode(bytes).expect_err(&format!("{what} was read instead of refused"));
        assert!(!error.is_empty(), "{what}: the refusal says nothing");
        error
    };
    refused(b"", "an empty block");
    assert!(refused(&[0x02, 0x0A, 0x01, 0x01], "the reserved delta byte").contains("0x02"));
    assert!(refused(&[0x03, 0x00], "an unknown container byte").contains("0x03"));
    refused(b"{not json", "unparseable JSON");
    // Truncated inside a field: the length prefix runs past the end.
    refused(&packed[..packed.len() - 3], "a truncated packed block");
    // Timestamps and values of different lengths: built by hand, since the encoder cannot.
    use prost::Message as _;
    let mismatched = crate::sdk::v1::DurableControlStateSeries {
        timestamps_ms: vec![1, 2],
        values: vec![5],
    };
    let mut bytes = vec![CONTROL_STATE_BLOCK_PACKED];
    bytes.extend(mismatched.encode_to_vec());
    assert!(refused(&bytes, "mismatched arrays").contains("2 timestamps and 1 values"));
    // A repeated or descending timestamp would silently keep only one of two values.
    for bad in [vec![5u64, 5], vec![9, 3]] {
        let unordered = crate::sdk::v1::DurableControlStateSeries {
            timestamps_ms: bad,
            values: vec![1, 2],
        };
        let mut bytes = vec![CONTROL_STATE_BLOCK_PACKED];
        bytes.extend(unordered.encode_to_vec());
        refused(&bytes, "out-of-order timestamps");
    }
}

/// THE LOAD PATH READS A JSON BLOCK AND A PACKED ONE, AND COUNTS THE ONES IT CANNOT READ.
///
/// Five keys, each stored ONLY as a block, through the same append the real writer uses. They are
/// deliberately not written through the counter path: the serialized series in the index is
/// authoritative on load and a block is consulted only for a key the persisted map does not hold
/// (`saw_control_state`'s merge), so a key that was also written live would be answered from the
/// index and the block would never be read. After a flush and a restart the counts are read back
/// through the real query. The packed series carries a total (107) no other arm can produce; the
/// unreadable ones read as absent (0) AND move the counter, which is the half the old
/// `if let Ok(..)` could not show.
#[test]
fn the_load_path_reads_a_json_block_and_a_packed_one_and_counts_the_unreadable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let blocks = dir.path().join("blocks");
    let indexes = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-a"), &blocks, &indexes);
    engine.load_shard(SHARD);
    {
        let (start, end) = engine.shard_routing_range(SHARD);
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&SHARD).expect("the shard is loaded");
        let mut store = |key: &str, bytes: Vec<u8>| {
            assert!(
                append_control_state_block(
                    &engine.cache,
                    &engine.block_store,
                    SHARD,
                    shard,
                    key,
                    &bytes,
                    start,
                    end,
                    false,
                ),
                "{key}: the block was not stored"
            );
        };
        // The installed base: the JSON every block written so far is.
        store("ctr-json", serde_json::to_vec(&series(&[(10, 1)])).unwrap());
        store("ctr-packed", encode_control_state_block(&series(&[(10, 100), (20, 7)])));
        store("ctr-reserved", vec![0x02, 0x0A, 0x01, 0x01]);
        store("ctr-garbage", b"{not json".to_vec());
        store("ctr-unknown", vec![0x03, 0x00]);
    }
    engine.flush_shard_index(SHARD);
    drop(engine);

    let before = control_state_blocks_unreadable();
    let restarted = TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-b"), &blocks, &indexes);
    restarted.load_shard(SHARD);

    assert_eq!(
        count(&restarted, "ctr-json"),
        CommandResponse::Integer { value: 1 },
        "the JSON block was not read"
    );
    assert_eq!(
        count(&restarted, "ctr-packed"),
        CommandResponse::Integer { value: 107 },
        "the packed block was not read"
    );
    for key in ["ctr-reserved", "ctr-garbage", "ctr-unknown"] {
        assert_eq!(
            count(&restarted, key),
            CommandResponse::Integer { value: 0 },
            "{key}: an unreadable block was read as a series"
        );
    }
    assert_eq!(
        control_state_blocks_unreadable() - before,
        3,
        "the three unreadable blocks were not each counted exactly once"
    );
}
