// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A RETRIED MERGE APPEND ANSWERS ITS FIRST RECEIPT, DRIVEN THROUGH THE REAL `execute` PATH.
//!
//! A merge-reduce append that is sent twice counts every `Sum` and `Count` lane twice. A request may
//! carry a retry token; the key remembers the last 16 tokens it applied, each with the receipt it
//! landed, and the same token again answers that receipt, marked as a replay, and writes nothing.
//!
//! WHAT EACH GUARD CAN ONLY PASS ON THE NEW PATH.
//!   * the retry answers `(1 snapshot, 1 cell created, 0 merged)` with `replayed` set. A merge that
//!     ran again would say `(1, 0, 1)`: the cell exists by then. Only the stored first receipt says
//!     "created", and only the replay path sets the flag.
//!   * the stored series is byte-identical before and after the retry, and the `Sum` lane holds the
//!     request once. THE CONTROL is the same request without a token, which doubles the lane: the
//!     token is what held it.
//!   * the 17th token evicts the first, so the first, retried, is applied again and the lane moves:
//!     the window is a count of appends, stated and measured.
//!   * a key that was deleted, expired or trimmed to nothing forgets its ring: the same token on the
//!     key written again is a new request.
//!   * the ring comes back from the shard's index image after a flush and a crash, and after a
//!     storage cycle has dumped it and reclaimed the log.

#![allow(clippy::all)]
use super::*;

use crate::engine::cell_retry_ring::RETRY_RING_CAPACITY;
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

/// Two lanes: `hits` is summed and `writes` counts the writes that reached the cell.
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
    cell_set::encode_key(
        &spec(),
        &CellKey { entity: EntityValue::U64(entity), dims: vec![Some(DimValue::U16(3))] },
    )
    .expect("the key fits the spec")
}

fn adds(hits: i64) -> FeaturePoint {
    let cell = Cell { member: Member::U64(7), labels: vec![0], lanes: vec![hits, 0] };
    FeaturePoint { timestamp_ms: 100, value: cell_set::encode_value(&header(), &[cell]).expect("encodes") }
}

fn engine_on(dir: &std::path::Path, bound: Option<usize>) -> TemporalEngine {
    let engine =
        TemporalEngine::with_local_dirs(1 << 20, dir.join("cache"), dir.join("blocks"), dir.join("indexes"));
    engine.load_shard(1);
    if let Some(bound) = bound {
        assert!(
            engine
                .set_config(crate::control::SetConfigRequest {
                    shard_id: 1,
                    config: crate::control::Config { version: 2, feature_max_size: bound, ..Default::default() },
                })
                .ok,
            "the bound has to be the one under test"
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
            command: Command::FeatureQuery {
                key: key.to_string(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: Some(1_000_000),
            },
        })
        .response
    {
        CommandResponse::FeaturePoints { points } => points,
        other => panic!("expected FeaturePoints, got {other:?}"),
    }
}

/// The two lanes of member 7 at timestamp 100.
fn lanes(engine: &TemporalEngine, key: &str) -> Vec<i64> {
    raw_series(engine, key)
        .into_iter()
        .find(|p| p.timestamp_ms == 100)
        .map(|p| cell_set::decode_value(&p.value).expect("a stored value is a cell snapshot").1)
        .and_then(|cells| cells.into_iter().find(|c| c.member == Member::U64(7)))
        .map(|c| c.lanes)
        .unwrap_or_else(|| panic!("member 7 is not stored at 100"))
}

/// The tokens the SHARD holds for a key, oldest first. Read directly, not through a command.
fn ring_tokens(engine: &TemporalEngine, key: &str) -> Option<Vec<u64>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    shards.get(&1).expect("shard 1 is loaded").retry_ring(key).map(|ring| ring.tokens())
}

fn ring_bytes(engine: &TemporalEngine, key: &str) -> Option<Vec<u8>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    shards.get(&1).expect("shard 1 is loaded").retry_ring(key).map(|ring| ring.encode())
}

#[test]
fn a_repeated_token_answers_the_first_receipt_and_writes_nothing() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let key = key_for(1);
    assert_eq!((1, 1, 0, false), receipt(&send(&engine, &key, vec![adds(5)], Some(9))));
    let stored = raw_series(&engine, &key);
    assert_eq!(vec![5, 1], lanes(&engine, &key));

    // The retry: the first receipt, as a replay. A merge that ran again would say created 0,
    // merged 1, and the lane would read 10.
    assert_eq!((1, 1, 0, true), receipt(&send(&engine, &key, vec![adds(5)], Some(9))));
    assert_eq!(stored, raw_series(&engine, &key), "a replay changed the stored snapshot");
    assert_eq!(vec![5, 1], lanes(&engine, &key));
    assert_eq!(Some(vec![9]), ring_tokens(&engine, &key), "a replay filed the token again");

    // CONTROL: the same request without a token is applied, which is what the token prevented.
    assert_eq!((1, 0, 1, false), receipt(&send(&engine, &key, vec![adds(5)], None)));
    assert_eq!(vec![10, 2], lanes(&engine, &key));
    assert_eq!(Some(vec![9]), ring_tokens(&engine, &key), "a request without a token touched the ring");
}

#[test]
fn two_tokens_interleaved_and_one_token_twice_apart_apply_once_each() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let key = key_for(2);
    let a1 = receipt(&send(&engine, &key, vec![adds(1)], Some(10)));
    let b1 = receipt(&send(&engine, &key, vec![adds(100)], Some(20)));
    let a2 = receipt(&send(&engine, &key, vec![adds(1)], Some(10)));
    let b2 = receipt(&send(&engine, &key, vec![adds(100)], Some(20)));
    let c = receipt(&send(&engine, &key, vec![adds(10_000)], Some(30)));
    let a3 = receipt(&send(&engine, &key, vec![adds(1)], Some(10)));
    assert_eq!(vec![10_101, 3], lanes(&engine, &key), "a repeated token was applied again");
    assert_eq!((1, 1, 0, false), a1);
    assert_eq!((1, 0, 1, false), b1);
    assert_eq!((1, 1, 0, true), a2, "the retry did not answer a's own receipt");
    assert_eq!((1, 0, 1, true), b2, "the retry did not answer b's own receipt");
    assert_eq!((1, 0, 1, false), c);
    assert_eq!((1, 1, 0, true), a3);
    assert_eq!(Some(vec![10, 20, 30]), ring_tokens(&engine, &key), "a replay reordered the ring");
}

#[test]
fn the_ring_holds_sixteen_through_the_engine_and_the_seventeenth_token_evicts_the_first() {
    assert_eq!(16, RETRY_RING_CAPACITY);
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let key = key_for(3);
    for token in 1..=16u64 {
        assert!(!receipt(&send(&engine, &key, vec![adds(1)], Some(token))).3);
    }
    assert_eq!(vec![16, 16], lanes(&engine, &key));
    // At exactly the limit the oldest is still protected.
    assert!(receipt(&send(&engine, &key, vec![adds(1)], Some(1))).3);
    assert_eq!(vec![16, 16], lanes(&engine, &key));

    // One past it: token 17 is applied and evicts token 1 ...
    assert!(!receipt(&send(&engine, &key, vec![adds(1)], Some(17))).3);
    assert_eq!(Some((2..=17).collect::<Vec<u64>>()), ring_tokens(&engine, &key));
    assert_eq!(vec![17, 17], lanes(&engine, &key));
    // ... so token 1, retried now, is NOT protected: it is applied again, and takes token 2's slot.
    let late = receipt(&send(&engine, &key, vec![adds(1)], Some(1)));
    assert!(!late.3, "a retry older than the window was answered as a replay");
    assert_eq!(vec![18, 18], lanes(&engine, &key));
    assert_eq!(Some((3..=17).chain(std::iter::once(1)).collect::<Vec<u64>>()), ring_tokens(&engine, &key));
    // Token 2 fell out with it.
    assert!(!receipt(&send(&engine, &key, vec![adds(1)], Some(2))).3);
    assert_eq!(vec![19, 19], lanes(&engine, &key));
    // A token still in the window is still protected after all of that.
    assert!(receipt(&send(&engine, &key, vec![adds(1)], Some(17))).3);
    assert_eq!(vec![19, 19], lanes(&engine, &key));
}

#[test]
fn a_token_belongs_to_its_key() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let (one, two) = (key_for(4), key_for(5));
    assert!(!receipt(&send(&engine, &one, vec![adds(1)], Some(77))).3);
    // The same token on another key is a different request.
    assert!(!receipt(&send(&engine, &two, vec![adds(1)], Some(77))).3);
    assert!(receipt(&send(&engine, &one, vec![adds(1)], Some(77))).3);
    assert!(receipt(&send(&engine, &two, vec![adds(1)], Some(77))).3);
    assert_eq!(vec![1, 1], lanes(&engine, &one));
    assert_eq!(vec![1, 1], lanes(&engine, &two));
}

#[test]
fn a_refused_request_does_not_consume_its_token_and_the_wrong_policy_is_refused() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let key = key_for(6);
    assert!(!receipt(&send(&engine, &key, vec![adds(1)], Some(1))).3);
    let before = raw_series(&engine, &key);

    // A header that disagrees with the stored one is refused, token or not, and files nothing.
    let mut other = header();
    other.lanes[0].reducer = LaneReducer::Max;
    let cell = Cell { member: Member::U64(7), labels: vec![0], lanes: vec![1, 0] };
    let bad = FeaturePoint { timestamp_ms: 100, value: cell_set::encode_value(&other, &[cell]).unwrap() };
    let refused = send(&engine, &key, vec![bad], Some(2));
    assert!(!refused.status.ok, "the request was accepted");
    assert_eq!(Some(vec![1]), ring_tokens(&engine, &key), "a refused request filed its token");
    assert_eq!(before, raw_series(&engine, &key));
    // Its token is still unused: the corrected request applies.
    assert!(!receipt(&send(&engine, &key, vec![adds(1)], Some(2))).3);

    // A token under a policy that cannot honour it is refused, not ignored.
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppendWithPolicy {
            key: key.clone(),
            points: vec![FeaturePoint { timestamp_ms: 5, value: b"v".to_vec() }],
            policy: FeatureWritePolicy::Upsert,
            retry_token: Some(3),
        },
    });
    assert!(!response.status.ok);
    assert_eq!("invalid_argument", response.status.code);
    assert!(response.status.message.contains("merge-reduce"), "{:?}", response.status.message);
    assert_eq!(
        vec![100],
        raw_series(&engine, &key).iter().map(|p| p.timestamp_ms).collect::<Vec<u64>>(),
        "the refused request wrote a point"
    );
}

#[test]
fn a_delete_an_expiry_and_a_trim_to_nothing_each_forget_the_ring() {
    // DELETE
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let key = key_for(7);
    assert!(!receipt(&send(&engine, &key, vec![adds(1)], Some(5))).3);
    assert_eq!(Some(vec![5]), ring_tokens(&engine, &key));
    let deleted = engine.execute(ExecuteRequest { shard_id: 1, command: Command::FeatureDelete { key: key.clone() } });
    assert!(deleted.status.ok, "{:?}", deleted.status);
    assert_eq!(None, ring_tokens(&engine, &key), "the delete left the ring behind");
    // Written again, the same token is a new request, not a replay.
    assert_eq!((1, 1, 0, false), receipt(&send(&engine, &key, vec![adds(1)], Some(5))));
    assert_eq!(vec![1, 1], lanes(&engine, &key));

    // EXPIRY: a deadline in the past, which the next command removes the key for.
    let expiring = key_for(8);
    assert!(!receipt(&send(&engine, &expiring, vec![adds(1)], Some(5))).3);
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 is loaded");
        crate::engine::set_expiry(shard, expiring.clone(), 1);
    }
    assert_eq!((1, 1, 0, false), receipt(&send(&engine, &expiring, vec![adds(1)], Some(5))));
    assert_eq!(vec![1, 1], lanes(&engine, &expiring), "the expired key's value came back");
    assert_eq!(Some(vec![5]), ring_tokens(&engine, &expiring));

    // A TRIM TO NOTHING: the series bound is zero, so what the append wrote is trimmed away and the
    // key holds no series for a ring to belong to.
    let dir = tempfile::tempdir().unwrap();
    let trimming = engine_on(dir.path(), Some(0));
    let key = key_for(9);
    let first = receipt(&send(&trimming, &key, vec![adds(1)], Some(5)));
    assert!(!first.3);
    assert_eq!(None, ring_tokens(&trimming, &key), "a ring was kept for a key with no series");
    assert!(!receipt(&send(&trimming, &key, vec![adds(1)], Some(5))).3, "a token was remembered for a key that holds nothing");
}

#[test]
fn a_token_survives_a_crash_and_a_storage_cycle_that_reclaimed_the_log() {
    // NAME KEPT: it is pinned in CI. The crash here is a crash after a flush.
    let dir = tempfile::tempdir().unwrap();
    let key = key_for(10);
    let engine = engine_on(dir.path(), None);
    for token in 1..=3u64 {
        assert!(!receipt(&send(&engine, &key, vec![adds(1)], Some(token))).3);
    }
    assert_eq!(vec![3, 3], lanes(&engine, &key));
    let ring = ring_bytes(&engine, &key).expect("the ring is held");

    // A CRASH after the index was flushed: the engine is dropped without a clean unload, and the
    // ring comes back from the flushed image. (A ring is held in the image like a deadline or a
    // seen set, so what was filed after the last flush is rebuilt only where the log is replayed,
    // which this single-node harness has not got.)
    engine.flush_shard_index(1);
    drop(engine);
    let replayed = engine_on(dir.path(), None);
    assert_eq!(Some(ring.clone()), ring_bytes(&replayed, &key), "the flushed ring was not read back");
    assert!(receipt(&send(&replayed, &key, vec![adds(1)], Some(2))).3, "a token was forgotten across a crash");
    assert_eq!(vec![3, 3], lanes(&replayed, &key));

    // A STORAGE CYCLE that dumps the shard and reclaims the log, then a clean restart: the ring can
    // come back only from the index image.
    let cycle = replayed.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: 1,
        max_dump_buckets_per_round: 16,
        min_undumped_wal_records: 0,
        warm_cache: true,
        ..StorageManagerCycleRequest::default()
    });
    assert!(cycle.completed, "{cycle:?}");
    assert!(
        cycle.stages.iter().any(|stage| stage.stage == "reclaim_wal" && stage.dumped_bucket_count > 0),
        "the cycle did not reclaim the log: {cycle:?}"
    );
    replayed.unload_shard(1);
    drop(replayed);
    let reloaded = engine_on(dir.path(), None);
    assert_eq!(Some(ring), ring_bytes(&reloaded, &key), "the ring did not survive the dump and the reclaim");
    assert!(receipt(&send(&reloaded, &key, vec![adds(1)], Some(3))).3);
    assert_eq!(vec![3, 3], lanes(&reloaded, &key));
    // And it is a live ring: a new token is applied and filed.
    assert!(!receipt(&send(&reloaded, &key, vec![adds(1)], Some(4))).3);
    assert_eq!(Some(vec![1, 2, 3, 4]), ring_tokens(&reloaded, &key));
}

fn script(engine: &TemporalEngine, key: &str) {
    for (token, hits) in [(1u64, 3i64), (2, 4), (1, 3), (3, 5), (2, 4), (4, 6)] {
        send(engine, key, vec![adds(hits)], Some(token));
    }
    send(engine, key, vec![adds(2)], None);
}

#[test]
fn two_engines_given_the_same_requests_hold_the_same_bytes() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (a, b) = (engine_on(dir_a.path(), None), engine_on(dir_b.path(), None));
    let key = key_for(11);
    script(&a, &key);
    script(&b, &key);
    assert_eq!(raw_series(&a, &key), raw_series(&b, &key));
    assert_eq!(ring_bytes(&a, &key), ring_bytes(&b, &key));
    // DENOMINATOR: the script retried three times and wrote the untokened request once.
    assert_eq!(vec![3 + 4 + 5 + 6 + 2, 5], lanes(&a, &key));
    assert_eq!(Some(vec![1, 2, 3, 4]), ring_tokens(&a, &key));
}

#[test]
fn an_append_without_a_token_stores_what_it_always_stored() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (plain, tokened) = (engine_on(dir_a.path(), None), engine_on(dir_b.path(), None));
    let key = key_for(12);
    for hits in [3i64, 4, 5] {
        send(&plain, &key, vec![adds(hits)], None);
    }
    for (token, hits) in [(1u64, 3i64), (2, 4), (3, 5)] {
        send(&tokened, &key, vec![adds(hits)], Some(token));
    }
    // A token changes no byte of any stored snapshot.
    assert_eq!(raw_series(&plain, &key), raw_series(&tokened, &key));

    // The tokenless shard's image names no ring table and the tokened one does: the table costs a
    // key that never uses a token nothing.
    let image = |engine: &TemporalEngine| {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1 is loaded");
        let plain_json = String::from_utf8(crate::engine::encode_index_bytes_as_plain_json(shard)).expect("utf-8");
        let decoded = crate::engine::decode_index_bytes(&crate::engine::encode_index_bytes(shard)).expect("decodes");
        (plain_json, decoded.retry_ring(&key).map(|ring| ring.encode()))
    };
    let (plain_image, plain_ring) = image(&plain);
    let (tokened_image, tokened_ring) = image(&tokened);
    assert!(!plain_image.contains("retry_rings"), "a shard that never used a token wrote a ring table");
    assert_eq!(None, plain_ring);
    assert!(tokened_image.contains("retry_rings"), "the tokened shard's image has no ring table");
    // And the image round trip carries the ring as it was.
    assert_eq!(ring_bytes(&tokened, &key), tokened_ring);
}

#[test]
fn the_token_rides_the_command_bytes_and_a_tokenless_command_is_unchanged_on_every_wire() {
    use prost::Message as _;
    let tokenless = Command::FeatureAppendWithPolicy {
        key: "k".to_string(),
        points: Vec::new(),
        policy: FeatureWritePolicy::MergeReduce,
        retry_token: None,
    };
    let tokened = |token: u64| Command::FeatureAppendWithPolicy {
        key: "k".to_string(),
        points: Vec::new(),
        policy: FeatureWritePolicy::MergeReduce,
        retry_token: Some(token),
    };

    // THE SCHEMA, byte for byte: key (1), policy (3) and nothing else for a tokenless command, and
    // field 16 as a varint (tag 0x80 0x01) when there is a token -- zero included, which is a token.
    let message = |token: Option<u64>| {
        crate::sdk::v1::FeatureAppendWithPolicy {
            key: "k".to_string(),
            points: Vec::new(),
            policy: crate::sdk::v1::FeatureWritePolicy::MergeReduce as i32,
            retry_token: token,
        }
        .encode_to_vec()
    };
    assert_eq!(vec![0x0a, 0x01, b'k', 0x18, 0x05], message(None));
    assert_eq!(vec![0x0a, 0x01, b'k', 0x18, 0x05, 0x80, 0x01, 0x00], message(Some(0)));
    assert_eq!(vec![0x0a, 0x01, b'k', 0x18, 0x05, 0x80, 0x01, 0xac, 0x02], message(Some(300)));

    // THE LOG'S ENCODING of the command carries exactly those bytes more, and reads back the same.
    let bare = crate::shared_store::command_protobuf_bytes(&tokenless).unwrap();
    let with = crate::shared_store::command_protobuf_bytes(&tokened(300)).unwrap();
    assert_eq!(bare.len() + 4, with.len());
    assert_eq!(tokenless, crate::shared_store::command_from_protobuf_bytes(&bare).unwrap());
    assert_eq!(tokened(300), crate::shared_store::command_from_protobuf_bytes(&with).unwrap());
    assert_eq!(tokened(0), crate::shared_store::command_from_protobuf_bytes(
        &crate::shared_store::command_protobuf_bytes(&tokened(0)).unwrap()
    ).unwrap());

    // THE JSON of the proxy: a tokenless command names no token and a plain receipt no replay.
    assert!(!serde_json::to_string(&tokenless).unwrap().contains("retry_token"));
    assert!(serde_json::to_string(&tokened(300)).unwrap().contains("\"retry_token\":300"));
    let receipt_of = |replayed: bool| CommandResponse::CellMergeReceipt {
        snapshots_written: 1,
        cells_created: 2,
        cells_merged: 3,
        replayed,
    };
    assert!(!serde_json::to_string(&receipt_of(false)).unwrap().contains("replayed"));
    assert!(serde_json::to_string(&receipt_of(true)).unwrap().contains("\"replayed\":true"));
    // The response proto: a plain receipt writes no `replayed` field, a replay writes field 15.
    let wire = |replayed: bool| crate::sdk::types_command_response_to_sdk(receipt_of(replayed)).encode_to_vec();
    assert_eq!(wire(false).len() + 2, wire(true).len());
    assert!(wire(true).ends_with(&[0x78, 0x01]), "{:?}", wire(true));
}

#[test]
fn a_replay_is_admitted_before_the_request_is_checked_against_the_key() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let key = key_for(13);
    assert_eq!((1, 1, 0, false), receipt(&send(&engine, &key, vec![adds(5)], Some(40))));

    // The key moves on: a later request upgrades the stored header with a trailing lane.
    let mut wider = header();
    wider.lanes.push(LaneSpec { name: "extra".to_string(), reducer: LaneReducer::Sum });
    let cell = Cell { member: Member::U64(7), labels: vec![0], lanes: vec![1, 0, 1] };
    let upgrade = FeaturePoint { timestamp_ms: 100, value: cell_set::encode_value(&wider, &[cell]).unwrap() };
    assert!(send(&engine, &key, vec![upgrade], None).status.ok, "the upgrade was refused");

    // DENOMINATOR: the original request, sent again WITHOUT its token, no longer fits the key.
    let stale = send(&engine, &key, vec![adds(5)], None);
    assert!(!stale.status.ok, "the original request still fits the upgraded key, so this proves nothing");
    // With its token it is a replay, and a replay is answered, not refused for what the key became.
    assert_eq!((1, 1, 0, true), receipt(&send(&engine, &key, vec![adds(5)], Some(40))));
}

#[test]
fn an_image_that_holds_rings_cannot_be_read_by_a_binary_that_does_not_know_them() {
    // The stamp is what refuses it: an older binary skips a side-table domain it does not declare,
    // so without a step it would load the image, drop every ring and write it back.
    assert!(
        crate::engine::SHARD_INDEX_FORMAT_VERSION > 31,
        "the retry ring table is in the image, so the stamp has to be past the last one that had none"
    );
}
