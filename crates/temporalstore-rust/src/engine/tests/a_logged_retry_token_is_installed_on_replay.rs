// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A LOGGED RETRY TOKEN IS INSTALLED WHEN A RECORD IS REPLAYED.
//!
//! A merge-reduce append that carried a retry token files the token and the receipt it landed in
//! its key's ring. The ring lives in the index image, so an append that reached only the log is
//! replayed from the outcomes the record carries, and nothing in them said a token was filed.
//! A record can now carry one more item, of the kind `cell_retry`, whose value is the entry the
//! write filed. This file is about the READER of that item: what replay does with it. The writer
//! is a separate change, so every item here is built by hand.
//!
//! WHAT EACH GUARD CAN ONLY PASS ON THE NEW ARM.
//!   * a retry of the logged token answers the stored first receipt, marked as a replay, and the
//!     lane does not move. THE CONTROL is a token that was not logged: it is applied and the lane
//!     doubles, so the item is what held the first one.
//!   * installing the same item twice leaves the ring byte for byte as it was, and the ring holds
//!     sixteen: the seventeenth logged token evicts the first.
//!   * a value that is not one entry in its one spelling is refused (the item is not installed and
//!     the ring is untouched), which fails a load rather than serving a ring that is not the
//!     writer's.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::engine::cell_retry_ring::{encode_entry, RetryReceipt, RETRY_RING_CAPACITY};
use crate::engine::cell_set::{
    self, Cell, CellHeader, CellKey, DimSpec, DimType, DimValue, EntityType, EntityValue, KeySpec, LaneReducer,
    LaneSpec, Member, HEADER_VERSION,
};
use crate::wal::WalOutcomeItem;

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

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(1 << 20, dir.join("cache"), dir.join("blocks"), dir.join("indexes"));
    engine.load_shard(1);
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

/// (samples written, cells created, cells merged, replayed)
fn receipt(response: &ExecuteResponse) -> (u64, u64, u64, bool) {
    assert!(response.status.ok, "the request was refused: {:?}", response.status);
    match &response.response {
        CommandResponse::CellMergeReceipt { samples_written, cells_created, cells_merged, replayed } => {
            (*samples_written, *cells_created, *cells_merged, *replayed)
        }
        other => panic!("a merge-reduce append answers a receipt, got {other:?}"),
    }
}

/// The two lanes of member 7 at timestamp 100.
fn lanes(engine: &TemporalEngine, key: &str) -> Vec<i64> {
    let points = match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery { key: key.to_string(), start_ms: 0, end_ms: u64::MAX, count: Some(10) },
        })
        .response
    {
        CommandResponse::FeaturePoints { points } => points,
        other => panic!("expected FeaturePoints, got {other:?}"),
    };
    cell_set::decode_value(&points[0].value).expect("a cell sample").1[0].lanes.clone()
}

fn ring_tokens(engine: &TemporalEngine, key: &str) -> Option<Vec<u64>> {
    engine.shards.with_state(1, |owner| {
        owner.expect("shard 1 is loaded").retry_ring(key).map(|ring| ring.tokens())
    })
}

fn ring_bytes(engine: &TemporalEngine, key: &str) -> Option<Vec<u8>> {
    engine.shards.with_state(1, |owner| {
        owner.expect("shard 1 is loaded").retry_ring(key).map(|ring| ring.encode())
    })
}

fn landed(n: u64) -> RetryReceipt {
    RetryReceipt { samples_written: 1, cells_created: n, cells_merged: n + 1 }
}

/// The item a write that filed `token` would log.
fn logged(key: &str, value: Option<Vec<u8>>) -> WalOutcomeItem {
    WalOutcomeItem {
        kind: "cell_retry".into(),
        object_key: key.to_string(),
        element_name: None,
        object_id: 0,
        routing_bucket: 0,
        address: None,
        value,
        ttl: None,
        deleted: false,
        meta: true,
    }
}

fn install(engine: &TemporalEngine, item: &WalOutcomeItem) -> bool {
    engine.apply_outcome_item(1, item, &[])
}

#[test]
fn a_logged_token_is_filed_and_the_retry_after_it_answers_the_first_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    let key = key_for(1);
    // The series exists (one tokenless append) and holds no token.
    assert!(!receipt(&send(&engine, &key, vec![adds(3)], None)).3);
    assert_eq!(None, ring_tokens(&engine, &key));

    // The record says a write filed token 77 with a receipt that says "created".
    assert!(install(&engine, &logged(&key, Some(encode_entry(77, &landed(1))))));
    assert_eq!(Some(vec![77]), ring_tokens(&engine, &key));
    // The retry answers that receipt, as a replay, and the lane does not move.
    assert_eq!((1, 1, 2, true), receipt(&send(&engine, &key, vec![adds(3)], Some(77))));
    assert_eq!(vec![3, 1], lanes(&engine, &key));
    // THE CONTROL: a token no record logged is applied, and the lane moves.
    assert_eq!((1, 0, 1, false), receipt(&send(&engine, &key, vec![adds(3)], Some(78))));
    assert_eq!(vec![6, 2], lanes(&engine, &key));
    assert_eq!(Some(vec![77, 78]), ring_tokens(&engine, &key));
}

#[test]
fn installing_an_entry_twice_changes_nothing_and_the_ring_holds_sixteen() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    let key = key_for(2);
    assert!(send(&engine, &key, vec![adds(1)], None).status.ok);

    let item = logged(&key, Some(encode_entry(5, &landed(5))));
    assert!(install(&engine, &item));
    let once = ring_bytes(&engine, &key);
    assert!(install(&engine, &item), "a record replayed again installs as it did the first time");
    assert_eq!(once, ring_bytes(&engine, &key), "a second install changed the ring");

    // Sixteen are held: the first sixteen entries are all there, and the seventeenth evicts the
    // oldest, which is the token that was installed first (5).
    for token in 100..100 + (RETRY_RING_CAPACITY as u64 - 1) {
        assert!(install(&engine, &logged(&key, Some(encode_entry(token, &landed(token))))));
    }
    let held = ring_tokens(&engine, &key).unwrap();
    assert_eq!(held.len(), RETRY_RING_CAPACITY);
    assert_eq!(held[0], 5);
    assert!(install(&engine, &logged(&key, Some(encode_entry(999, &landed(9))))));
    let held = ring_tokens(&engine, &key).unwrap();
    assert_eq!((held.len(), held[0], *held.last().unwrap()), (RETRY_RING_CAPACITY, 100, 999));
    // The evicted token is a new request again.
    assert!(!receipt(&send(&engine, &key, vec![adds(1)], Some(5))).3);
}

#[test]
fn a_key_that_holds_no_sample_remembers_no_token() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    let key = key_for(3);
    // Nothing was ever written under the key: the item installs as a no-op and creates no ring.
    assert!(install(&engine, &logged(&key, Some(encode_entry(1, &landed(1))))));
    assert_eq!(None, ring_tokens(&engine, &key));
    // A key that lost its series drops the ring it had, as the write does.
    assert!(send(&engine, &key, vec![adds(1)], Some(9)).status.ok);
    assert_eq!(Some(vec![9]), ring_tokens(&engine, &key));
    assert!(engine.execute(ExecuteRequest { shard_id: 1, command: Command::FeatureDelete { key: key.clone() } }).status.ok);
    assert_eq!(None, ring_tokens(&engine, &key));
}

#[test]
fn a_value_that_is_not_one_entry_in_its_spelling_is_refused_and_touches_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    let key = key_for(4);
    assert!(send(&engine, &key, vec![adds(1)], Some(1)).status.ok);
    let before = ring_bytes(&engine, &key);
    let good = encode_entry(42, &landed(2));

    let mut truncated = good.clone();
    truncated.pop();
    let mut trailing = good.clone();
    trailing.push(0);
    // Field 1 (the token, 42) written with a padded varint: it reads back to 42 but is not what
    // the writer spells.
    let padded = {
        let mut bytes = vec![0x08, 0xAA, 0x00];
        bytes.extend_from_slice(&good[2..]);
        bytes
    };
    // The same four fields with the first two swapped.
    let swapped = {
        let mut bytes = vec![0x10, 42, 0x08, 1];
        bytes.extend_from_slice(&good[4..]);
        bytes
    };
    for (name, value) in [
        ("no value", None),
        ("an empty value", Some(vec![])),
        ("a truncated entry", Some(truncated)),
        ("an entry with a byte after it", Some(trailing)),
        ("a padded varint", Some(padded)),
        ("fields out of order", Some(swapped)),
        ("a ring where an entry belongs", Some(vec![0x0A, 0x00])),
    ] {
        assert!(!install(&engine, &logged(&key, value)), "{name} was installed");
        assert_eq!(before, ring_bytes(&engine, &key), "{name} touched the ring");
    }
    // The control: the entry itself installs.
    assert!(install(&engine, &logged(&key, Some(good))));
    assert_eq!(Some(vec![1, 42]), ring_tokens(&engine, &key));
}

#[test]
fn the_item_survives_the_protobuf_record_with_its_kind_and_value() {
    let item = logged("some-key", Some(encode_entry(u64::MAX, &landed(u64::MAX - 1))));
    let proto = crate::wal_proto::items_to_proto(&[item.clone()], 1, None, None);
    assert_eq!(proto.len(), 1);
    // A kind this schema has no code for travels by name.
    assert_eq!((proto[0].kind_code, proto[0].kind_name.as_deref()), (None, Some("cell_retry")));
    let back = crate::wal_proto::items_from_proto(proto, 1, None, None);
    assert_eq!(back, vec![item]);
}
