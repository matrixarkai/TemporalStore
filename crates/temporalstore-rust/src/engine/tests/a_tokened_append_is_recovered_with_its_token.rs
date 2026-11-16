// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A TOKENED APPEND IS RECOVERED WITH ITS TOKEN AFTER A CRASH.
//!
//! The ring of retry tokens lives in the index image, which is flushed now and then. An append
//! written after the last flush reached only the log, and recovery installs the outcomes a record
//! carries instead of running the command again, so the token it filed was lost: a retry of exactly
//! that request after the crash was applied a second time. A tokened append now logs the entry it
//! filed as one more item (`cell_retry`), and replay files it (`a_logged_retry_token_is_installed_
//! on_replay` is the reader).
//!
//! WHAT EACH GUARD CAN ONLY PASS ON THE NEW PATH.
//!   * after the crash the ring holds the token filed AFTER the flush, and a retry of that request
//!     answers its first receipt `(1, 0, 1)` as a replay with the lane unmoved. Before this change
//!     the same retry was applied: `(1, 0, 1, false)` and the lane doubled. The DENOMINATOR is the
//!     token filed BEFORE the flush, which came back from the image both before and after.
//!   * a key deleted after the token was filed is not given its ring back by replay.
//!   * an append without a token logs no item and records the bytes it always recorded; a tokened
//!     one logs exactly one item, whose size is measured here.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::engine::cell_retry_ring::{decode_logged_entry, RetryReceipt};
use crate::engine::cell_set::{
    self, Cell, CellHeader, CellKey, DimSpec, DimType, DimValue, EntityType, EntityValue, KeySpec, LaneReducer,
    LaneSpec, Member, HEADER_VERSION,
};

fn spec() -> KeySpec {
    KeySpec {
        namespace: "ns".to_string(),
        entity: EntityType::U64,
        dimensions: vec![DimSpec { name: "channel".to_string(), ty: DimType::U16, required: true }],
    }
}

fn header() -> CellHeader {
    CellHeader {
        version: HEADER_VERSION,
        key: spec(),
        labels: vec!["slot".to_string()],
        lanes: vec![
            LaneSpec { name: "hits".to_string(), reducer: LaneReducer::Sum },
            LaneSpec { name: "writes".to_string(), reducer: LaneReducer::Count },
        ],
    }
}

fn key_for(entity: u64) -> String {
    cell_set::encode_key(&spec(), &CellKey { entity: EntityValue::U64(entity), dims: vec![Some(DimValue::U16(3))] })
        .expect("the key fits the spec")
}

fn adds(hits: i64) -> FeaturePoint {
    let cell = Cell { member: Member::U64(7), labels: vec![0], lanes: vec![hits, 0] };
    FeaturePoint { timestamp_ms: 100, value: cell_set::encode_value(&header(), &[cell]).expect("encodes") }
}

fn engine_in(dir: &std::path::Path, cache: &str, async_storage: bool) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.join(cache),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    if async_storage {
        assert!(
            engine
                .set_config(crate::control::SetConfigRequest {
                    shard_id: 1,
                    config: crate::control::Config {
                        version: 2,
                        async_storage: true,
                        feature_max_size: 5_000,
                        ..Default::default()
                    },
                })
                .ok,
            "the log has to be what holds the last write"
        );
    }
    engine
}

fn send(engine: &TemporalEngine, key: &str, points: Vec<FeaturePoint>, token: Option<u64>) -> ExecuteResponse {
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppendWithPolicy {
            key: key.to_string(),
            points,
            policy: FeatureWritePolicy::MergeReduce,
            retry_token: token,
        },
    })
}

/// (snapshots written, cells created, cells merged, replayed)
fn receipt(response: &ExecuteResponse) -> (u64, u64, u64, bool) {
    assert!(response.status.ok, "the request was refused: {:?}", response.status);
    match &response.response {
        CommandResponse::CellMergeReceipt { snapshots_written, cells_created, cells_merged, replayed } => {
            (*snapshots_written, *cells_created, *cells_merged, *replayed)
        }
        other => panic!("a merge-reduce append answers a receipt, got {other:?}"),
    }
}

fn raw_series(engine: &TemporalEngine, key: &str) -> Vec<FeaturePoint> {
    match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery { key: key.to_string(), start_ms: 0, end_ms: u64::MAX, count: Some(1_000_000) },
        })
        .response
    {
        CommandResponse::FeaturePoints { points } => points,
        other => panic!("expected FeaturePoints, got {other:?}"),
    }
}

/// The two lanes of member 7 at timestamp 100.
fn lanes(engine: &TemporalEngine, key: &str) -> Vec<i64> {
    cell_set::decode_value(&raw_series(engine, key)[0].value).expect("a cell snapshot").1[0].lanes.clone()
}

fn ring_bytes(engine: &TemporalEngine, key: &str) -> Option<Vec<u8>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    shards.get(&1).expect("shard 1 is loaded").retry_ring(key).map(|ring| ring.encode())
}

fn ring_tokens(engine: &TemporalEngine, key: &str) -> Option<Vec<u64>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    shards.get(&1).expect("shard 1 is loaded").retry_ring(key).map(|ring| ring.tokens())
}

/// The last log record of the shard: its framed bytes and its decoded form.
fn last_record(engine: &TemporalEngine) -> (Vec<u8>, crate::wal::WriteAheadLogRecord) {
    let records = engine.write_ahead_log_store().scan(1, 0, u64::MAX, u64::MAX).unwrap();
    let line = records.last().expect("the log holds a record").1.clone();
    let record = crate::wal::decode_wal_line(&line).unwrap();
    (line, record)
}

fn recovered_after_a_crash(async_storage: bool) {
    let dir = tempfile::tempdir().unwrap();
    let live = engine_in(dir.path(), "cache", async_storage);
    let key = key_for(1);
    // Token 1 is applied and then FLUSHED: it is in the index image. Token 2 merges into the same
    // cell afterwards and lives only in the log.
    assert_eq!((1, 1, 0, false), receipt(&send(&live, &key, vec![adds(3)], Some(1))));
    live.flush_shard_index(1);
    assert_eq!((1, 0, 1, false), receipt(&send(&live, &key, vec![adds(4)], Some(2))));
    let (series, ring, summed) = (raw_series(&live, &key), ring_bytes(&live, &key), lanes(&live, &key));
    assert_eq!(vec![7, 2], summed, "DENOMINATOR: both appends landed");
    assert_eq!(Some(vec![1, 2]), ring_tokens(&live, &key));
    drop(live);

    let restarted = engine_in(dir.path(), "cache-b", async_storage);
    assert_eq!(series, raw_series(&restarted, &key), "the replay reached other bytes");
    assert_eq!(summed, lanes(&restarted, &key));
    // The token filed after the flush came back with the log, and so did the one in the image.
    assert_eq!(ring, ring_bytes(&restarted, &key), "the ring after recovery is not the ring before the crash");
    assert_eq!(Some(vec![1, 2]), ring_tokens(&restarted, &key));
    // Retries of both requests answer their first receipt, as replays, and write nothing. Without
    // the logged entry the retry of token 2 would read (1, 0, 1, false) and the lane would be 11.
    assert_eq!((1, 1, 0, true), receipt(&send(&restarted, &key, vec![adds(3)], Some(1))));
    assert_eq!((1, 0, 1, true), receipt(&send(&restarted, &key, vec![adds(4)], Some(2))));
    assert_eq!(summed, lanes(&restarted, &key), "a retry after the crash was summed again");
    assert_eq!(series, raw_series(&restarted, &key));
    // It is a live ring: a new token is applied and filed after the two.
    assert_eq!((1, 0, 1, false), receipt(&send(&restarted, &key, vec![adds(1)], Some(3))));
    assert_eq!(Some(vec![1, 2, 3]), ring_tokens(&restarted, &key));
}

#[test]
fn a_tokened_append_written_after_the_last_flush_is_replayed_with_its_token() {
    recovered_after_a_crash(true);
}

#[test]
fn a_synchronous_engine_recovers_the_token_of_an_append_after_its_last_flush_the_same_way() {
    recovered_after_a_crash(false);
}

#[test]
fn a_key_deleted_after_its_token_was_filed_is_not_given_the_ring_back_by_replay() {
    let dir = tempfile::tempdir().unwrap();
    let live = engine_in(dir.path(), "cache", true);
    let key = key_for(2);
    live.flush_shard_index(1);
    assert!(!receipt(&send(&live, &key, vec![adds(3)], Some(9))).3);
    assert_eq!(Some(vec![9]), ring_tokens(&live, &key));
    assert!(live.execute(ExecuteRequest { shard_id: 1, command: Command::FeatureDelete { key: key.clone() } }).status.ok);
    assert_eq!(None, ring_tokens(&live, &key));
    drop(live);

    let restarted = engine_in(dir.path(), "cache-b", true);
    assert_eq!(None, ring_tokens(&restarted, &key), "replay resurrected the ring of a deleted key");
    // The same token on the key written again is a new request.
    assert_eq!((1, 1, 0, false), receipt(&send(&restarted, &key, vec![adds(3)], Some(9))));
}

#[test]
fn a_tokenless_append_logs_no_item_and_a_tokened_one_logs_exactly_one_whose_size_is_measured() {
    let key = key_for(3);

    // THE SAME APPEND, WITHOUT A TOKEN, on its own engine.
    let dir = tempfile::tempdir().unwrap();
    let plain = engine_in(dir.path(), "cache", true);
    assert!(!receipt(&send(&plain, &key, vec![adds(3)], None)).3);
    let (plain_line, plain_record) = last_record(&plain);
    assert!(plain_record.outcomes.iter().all(|item| item.kind != "cell_retry"), "a tokenless append logged a token");
    let encoded = |record: &crate::wal::WriteAheadLogRecord| crate::wal::encode_wal_line_for_test(record).unwrap().len();
    let plain_encoded = encoded(&plain_record);

    // THE SAME APPEND WITH A TOKEN, on another.
    let dir = tempfile::tempdir().unwrap();
    let tokened = engine_in(dir.path(), "cache", true);
    assert!(!receipt(&send(&tokened, &key, vec![adds(3)], Some(7001))).3);
    let (tokened_line, tokened_record) = last_record(&tokened);
    let items: Vec<_> = tokened_record.outcomes.iter().filter(|item| item.kind == "cell_retry").collect();
    assert_eq!(items.len(), 1, "a tokened append logs exactly one entry");
    assert_eq!(items[0].object_key, key);
    assert_eq!(
        decode_logged_entry(items[0].value.as_deref().unwrap()),
        Ok((7001, RetryReceipt { snapshots_written: 1, cells_created: 1, cells_merged: 0 }))
    );

    // Without that item the tokened record is as long as the tokenless record: the token costs
    // exactly what the item costs and the rest of the record is unchanged.
    let mut stripped = tokened_record.clone();
    stripped.outcomes.retain(|item| item.kind != "cell_retry");
    let stripped_encoded = encoded(&stripped);
    assert_eq!(stripped_encoded, plain_encoded, "a token changed something besides its own item");
    let cost = encoded(&tokened_record) - plain_encoded;
    println!("MEASURED: a tokenless append is {} bytes in the log, a tokened one {} (+{cost}); the entry is {} bytes",
        plain_encoded, plain_encoded + cost, items[0].value.as_ref().unwrap().len());
    let _ = (plain_line, tokened_line);
    // A guard on the size, with room: the item names its kind, its key, a routing bucket and the
    // entry, and nothing else.
    assert!(cost > items[0].value.as_ref().unwrap().len() && cost <= key.len() + 64, "+{cost} bytes");
}
