// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A CONTROL STATE SERIES BLOCK IS PROTOBUF ONLY, AND AN UNREADABLE ONE IS COUNTED.
//!
//! The block is a bare `DurableControlStateSeries`: no container byte, no JSON arm, no first-byte
//! sniff. The first release carries no installed base, so no test here plants an older shape to prove
//! it still loads. The JSON object the writer used to store is the thing that must now be REFUSED.
//! Blocks of other shapes are put into the block store through `append_control_state_block`, the same
//! append the real writer uses.
#![allow(clippy::all)]
use super::*;
use crate::engine::control_state_block::{control_state_blocks_unreadable, encode_control_state_block};
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
fn a_series_block_codec_reads_protobuf_and_refuses_everything_else() {
    use crate::engine::control_state_block::decode_control_state_block as decode;
    use prost::Message as _;
    // Negative values, a timestamp past 2^53 (which a double cannot hold) and the extremes.
    let original = series(&[(0, -1), (10, 7), (1_700_000_000_123, i64::MIN), (u64::MAX, i64::MAX)]);
    let block = encode_control_state_block(&original);
    assert!(serde_json::from_slice::<serde_json::Value>(&block).is_err());
    assert_eq!(decode(&block).unwrap(), original, "the block changed the series");

    // ---- everything below is a REFUSAL, and none may come back as an empty series.
    let refused = |bytes: &[u8], what: &str| {
        let error = decode(bytes).expect_err(&format!("{what} was read instead of refused"));
        assert!(!error.is_empty(), "{what}: the refusal says nothing");
        error
    };
    refused(b"", "an empty block");
    // THE JSON OBJECT THE WRITER USED TO STORE IS NOT A BLOCK ANY MORE.
    refused(&serde_json::to_vec(&original).unwrap(), "the old JSON object");
    refused(b"{}", "an empty JSON object");
    refused(b"{not json", "unparseable text");
    // Truncated inside a field: the length prefix runs past the end.
    refused(&block[..block.len() - 3], "a truncated block");
    // A message that decodes to nothing (one unknown field) is damage, not an empty series.
    assert!(refused(&[0x78, 0x01], "a block of only an unknown field").contains("no points"));
    // Timestamps and values of different lengths: built by hand, since the encoder cannot.
    let mismatched = crate::sdk::v1::DurableControlStateSeries {
        timestamps_ms: vec![1, 2],
        values: vec![5],
    };
    assert!(refused(&mismatched.encode_to_vec(), "mismatched arrays").contains("2 timestamps and 1 values"));
    // A repeated or descending timestamp would silently keep only one of two values.
    for bad in [vec![5u64, 5], vec![9, 3]] {
        let unordered = crate::sdk::v1::DurableControlStateSeries {
            timestamps_ms: bad,
            values: vec![1, 2],
        };
        refused(&unordered.encode_to_vec(), "out-of-order timestamps");
    }
}

/// THE LOAD PATH READS A PROTOBUF BLOCK AND COUNTS THE ONES IT CANNOT READ.
///
/// Four keys, each stored ONLY as a block, through the same append the real writer uses. They are
/// deliberately not written through the counter path: the serialized series in the index is
/// authoritative on load and a block is consulted only for a key the persisted map does not hold
/// (`saw_control_state`'s merge), so a key that was also written live would be answered from the
/// index and the block would never be read. After a flush and a restart the counts are read back
/// through the real query. The block carries a total (107) no other arm can produce; the unreadable
/// ones read as absent (0) AND move the counter, which is the half the old `if let Ok(..)` could not
/// show.
#[test]
fn the_load_path_reads_a_protobuf_block_and_counts_the_unreadable() {
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
        store("ctr-block", encode_control_state_block(&series(&[(10, 100), (20, 7)])));
        // The JSON object the writer used to store: refused now, so a series stored that way is absent.
        store("ctr-json", serde_json::to_vec(&series(&[(10, 1)])).unwrap());
        store("ctr-garbage", b"{not json".to_vec());
        store("ctr-nothing", vec![0x78, 0x01]);
    }
    engine.flush_shard_index(SHARD);
    drop(engine);

    let before = control_state_blocks_unreadable();
    let restarted = TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-b"), &blocks, &indexes);
    restarted.load_shard(SHARD);

    assert_eq!(
        count(&restarted, "ctr-block"),
        CommandResponse::Integer { value: 107 },
        "the block was not read"
    );
    for key in ["ctr-json", "ctr-garbage", "ctr-nothing"] {
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

/// A REAL COUNTER WRITE STORES A BLOCK A FRACTION OF THE JSON SIZE, AND A RESTART READS IT.
///
/// Through the real counter path in the default (synchronous) mode, with the series block forced on
/// every increment (an increment otherwise writes it only when due, see
/// `a_counter_increment_writes_its_series_when_due`, so the newest block is then a stale copy and
/// this test could not say it is the encoding of the whole series). After 1,000 points at one-second spacing from a current-era
/// millisecond timestamp, with small signed amounts, the newest block is read back out of the block
/// store: it is exactly the encoding of the in-memory series, is not JSON, and is compared with what
/// the JSON object for the same series would have been.
///
/// The size comparison is asserted loosely (under half) and PRINTED exactly, because the claim is a
/// measurement and the exact figure belongs in the record, not in an assertion that would then pin
/// a coincidence of the fixture.
#[test]
fn a_counter_write_stores_a_block_a_fraction_of_the_json_size_and_a_restart_reads_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let blocks = dir.path().join("blocks");
    let indexes = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-a"), &blocks, &indexes);
    engine.load_shard(SHARD);
    let first_ms = 1_700_000_000_000u64;
    let points = 1_000u64;
    let mut expected_total = 0i64;
    crate::engine::ALWAYS_WRITE_COUNTER_BLOCK.with(|flag| flag.set(true));
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
    crate::engine::ALWAYS_WRITE_COUNTER_BLOCK.with(|flag| flag.set(false));

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
        stored,
        encode_control_state_block(&in_memory),
        "the counter writer did not store the block encoding of the series"
    );
    assert!(serde_json::from_slice::<serde_json::Value>(&stored).is_err());
    assert_eq!(
        crate::engine::control_state_block::decode_control_state_block(&stored).unwrap(),
        in_memory,
        "the stored block is not the in-memory series"
    );
    let json_len = serde_json::to_vec(&in_memory).unwrap().len();
    println!(
        "CONTROL STATE BLOCK at {points} points: JSON {json_len} B ({:.2} B/point), block {} B \
         ({:.2} B/point), block is {:.1}% of JSON",
        json_len as f64 / points as f64,
        stored.len(),
        stored.len() as f64 / points as f64,
        100.0 * stored.len() as f64 / json_len as f64,
    );
    assert!(
        stored.len() * 2 < json_len,
        "the block ({} B) is not under half the JSON one ({json_len} B)",
        stored.len()
    );

    // ---- a restart reads it. The index's own series is authoritative on load, so this proves the
    // ---- stored block does not break the load; the block-only read is the load-path test above.
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

/// NO JSON PATH IS LEFT IN THE CONTROL STATE BLOCK CODE.
///
/// The test above reads the bytes the real writer stored; this sees an encoder nobody calls YET. It
/// reads `persist_control_state_block` and the whole codec module with comment lines removed and
/// asserts neither names `serde_json`, and that the writer calls the encoder. The CONTROL runs the
/// same scan over a string that does name it, so a scan that could not see the token would fail here
/// instead of passing over a writer.
#[test]
fn no_json_path_is_left_in_the_control_state_block_code() {
    fn uncommented(text: &str) -> String {
        text.lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
    }
    let engine_source = include_str!("../../engine.rs");
    let start = engine_source
        .find("fn persist_control_state_block(")
        .expect("no declaration persist_control_state_block");
    let rest = &engine_source[start..];
    let end = rest["fn persist_control_state_block(".len()..]
        .find("fn append_control_state_block(")
        .expect("no declaration append_control_state_block after it")
        + "fn persist_control_state_block(".len();
    let writer = uncommented(&rest[..end]);

    // control
    assert!(
        uncommented("// serde_json in a comment\nlet v = serde_json::to_vec(series);").contains("serde_json"),
        "the scan cannot see a serde_json call"
    );
    assert!(!uncommented("// serde_json in a comment only").contains("serde_json"));

    assert!(writer.len() > 300, "the scan found almost nothing");
    assert!(
        writer.contains("encode_control_state_block("),
        "the writer does not call the block encoder"
    );
    assert!(
        !writer.contains("serde_json"),
        "the control-state writer still reaches for serde_json: a JSON encode path survives"
    );
    let codec = uncommented(include_str!("../control_state_block.rs"));
    assert!(codec.contains("DurableControlStateSeries::decode"), "the codec does not decode the block");
    assert!(
        !codec.contains("serde_json"),
        "the control-state block codec still names serde_json: a JSON arm survives"
    );
}
