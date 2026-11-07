// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A CONTROL STATE SERIES BLOCK IS READ IN EITHER SHAPE, AND AN UNREADABLE ONE IS COUNTED.
//!
//! The writer stores packed blocks and has no JSON encode path, so every JSON block below is the
//! installed base and is PLANTED, put into the block store through the same append the real writer uses.
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

/// A REAL COUNTER WRITE STORES A PACKED BLOCK, A FRACTION OF THE SIZE, AND A RESTART READS IT.
///
/// Through the real counter path in the default (synchronous) mode, which rewrites the whole series
/// into a block on every write. After 1,000 points at one-second spacing from a current-era
/// millisecond timestamp, with small signed amounts, the newest block is read back out of the block
/// store: it starts with the container byte, decodes to exactly the in-memory series, and is
/// compared with what the JSON object for the same series would have been.
///
/// The size comparison is asserted loosely (under half) and PRINTED exactly, because the claim is a
/// measurement and the exact figure belongs in the record, not in an assertion that would then pin
/// a coincidence of the fixture.
#[test]
fn a_counter_write_stores_a_packed_block_a_fraction_of_the_size_and_a_restart_reads_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let blocks = dir.path().join("blocks");
    let indexes = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-a"), &blocks, &indexes);
    engine.load_shard(SHARD);
    let first_ms = 1_700_000_000_000u64;
    let points = 1_000u64;
    let mut expected_total = 0i64;
    for i in 0..points {
        let amount = (i % 7) as i64 - 3;
        expected_total += amount;
        let response = engine.execute(crate::types::ExecuteRequest {
            shard_id: SHARD,
            command: crate::types::Command::ControlStateIncrement {
                key: "ctr".to_string(),
                timestamp_ms: first_ms + i * 1_000,
                amount,
            },
        });
        assert!(response.status.ok, "{response:?}");
    }

    let (in_memory, stored) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&SHARD).expect("the shard is loaded");
        let in_memory = shard.control_state().get("ctr").expect("the series").clone();
        let slot = shard.control_state_blocks().get("ctr").expect("the block");
        let stored = engine.block_store.read(&slot.address()).expect("the block reads");
        (in_memory, stored)
    };
    assert_eq!(in_memory.len() as u64, points);
    assert_eq!(
        stored[0], CONTROL_STATE_BLOCK_PACKED,
        "the counter writer did not store the packed shape"
    );
    assert!(serde_json::from_slice::<serde_json::Value>(&stored).is_err());
    assert_eq!(
        crate::engine::control_state_block::decode_control_state_block(&stored).unwrap(),
        in_memory,
        "the stored block is not the in-memory series"
    );
    let json_len = serde_json::to_vec(&in_memory).unwrap().len();
    println!(
        "CONTROL STATE BLOCK at {points} points: JSON {json_len} B ({:.2} B/point), packed {} B \
         ({:.2} B/point), packed is {:.1}% of JSON",
        json_len as f64 / points as f64,
        stored.len(),
        stored.len() as f64 / points as f64,
        100.0 * stored.len() as f64 / json_len as f64,
    );
    assert!(
        stored.len() * 2 < json_len,
        "the packed block ({} B) is not under half the JSON one ({json_len} B)",
        stored.len()
    );

    // ---- a restart reads it. The index's own series is authoritative on load, so this proves the
    // ---- stored block does not break the load; the block-only read is the reader test above.
    engine.flush_shard_index(SHARD);
    drop(engine);
    let restarted = TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-b"), &blocks, &indexes);
    restarted.load_shard(SHARD);
    assert_eq!(
        count(&restarted, "ctr"),
        CommandResponse::Integer { value: expected_total },
        "the series did not survive a restart"
    );
}

/// NO JSON ENCODE PATH IS LEFT IN THE CONTROL STATE WRITER.
///
/// The test above reads the first byte of what the real writer stored; this sees an encoder nobody
/// calls YET. It reads `persist_control_state_block` with comment lines removed and asserts it names
/// no `serde_json` and calls the packed encoder. The POSITIVE CONTROL is the decoder, which still
/// calls `serde_json::from_slice` (the permanent JSON arm): a scan that could not see that call
/// would pass over any writer.
#[test]
fn no_json_encode_path_is_left_in_the_control_state_writer() {
    fn body(source: &str, from: &str, to: &str) -> String {
        let start = source
            .find(from)
            .unwrap_or_else(|| panic!("no declaration {from}"));
        let rest = &source[start..];
        let end = rest[from.len()..]
            .find(to)
            .unwrap_or_else(|| panic!("no declaration {to} after {from}"))
            + from.len();
        rest[..end]
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
    }
    let engine_source = include_str!("../../engine.rs");
    let codec_source = include_str!("../control_state_block.rs");

    let writer = body(
        engine_source,
        "fn persist_control_state_block(",
        "fn append_control_state_block(",
    );
    assert!(writer.len() > 300, "the scan found almost nothing");
    assert!(
        writer.contains("encode_control_state_block("),
        "the writer does not call the packed encoder"
    );
    assert!(
        !writer.contains("serde_json"),
        "the control-state writer still reaches for serde_json: a JSON encode path survives. Delete \
         it; the decoder keeps its JSON arm for the blocks already on disk."
    );
    let encoder = body(
        codec_source,
        "pub(crate) fn encode_control_state_block(",
        "\n}\n",
    );
    assert!(!encoder.contains("serde_json"), "the packed encoder names serde_json");
    let decoder = body(
        codec_source,
        "pub(super) fn decode_control_state_block(",
        "pub(crate) fn encode_control_state_block(",
    );
    assert!(
        decoder.contains("serde_json::from_slice"),
        "the control no longer sees the permanent JSON arm, so this scan could pass over a writer it cannot see"
    );
}
