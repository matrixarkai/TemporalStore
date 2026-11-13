// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::types::{
    BatchHashMultiSetValue, BatchHashTimestampSetRequest, BatchHashTimestampSetRow,
};
use prost::Message;

#[derive(Clone, PartialEq, prost::Message)]
struct Timestamp {
    #[prost(int64, tag = "1")]
    seconds: i64,
}

fn open(path: &std::path::Path, asynchronous: bool) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        32 * 1024 * 1024,
        path.join("cache"),
        path.join("blocks"),
        path.join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .set_config(crate::control::SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage: asynchronous,
                    ..Config::default()
                },
            })
            .ok
    );
    engine
}

fn request(rows: &[(i64, i64, usize)]) -> BatchHashTimestampSetRequest {
    BatchHashTimestampSetRequest {
        shard_id: 1,
        fields: vec!["timestamp".into(), "value".into()],
        timestamp_field: "timestamp".into(),
        ttl_ms: None,
        rows: rows
            .iter()
            .map(|&(argument, stored, padding)| {
                let mut timestamp = Timestamp { seconds: stored }.encode_to_vec();
                if padding > 0 {
                    // An unknown length-delimited protobuf field preserves a valid timestamp.
                    assert!(padding < 128);
                    timestamp.extend_from_slice(&[0x1a, padding as u8]);
                    timestamp.extend(std::iter::repeat_n(0, padding));
                }
                BatchHashTimestampSetRow(
                    "row".into(),
                    argument,
                    0,
                    vec![
                        BatchHashMultiSetValue(timestamp),
                        BatchHashMultiSetValue(vec![stored as u8; 48]),
                    ],
                )
            })
            .collect(),
    }
}

fn apply(
    engine: &TemporalEngine,
    request: BatchHashTimestampSetRequest,
    borrowed: bool,
) -> BatchConditionalWriteResponse {
    if borrowed {
        let body = rmp_serde::to_vec_named(&request).unwrap();
        engine
            .batch_execute_borrowed_hash_rows(rmp_serde::from_slice(&body).unwrap())
            .0
    } else {
        engine.batch_execute_packed_hash_rows(request).0
    }
}

fn assert_cached(engine: &TemporalEngine, expected: bool) {
    let shards = engine.shards.read().unwrap();
    let shard = shards.get(&1).unwrap();
    let address = shard
        .bucket_index
        .hashes
        .get("row")
        .unwrap()
        .get("value")
        .unwrap();
    assert!(crate::wal_record::is_wal_resident(address.block_slab_id()));
    let (start, end) = shard.routing_range();
    let key = CacheKey::page_with_slot(
        1,
        address.block_slab_id(),
        address.offset(),
        address.length(),
        Some(block_routing_bucket("row", start, end)),
    );
    assert_eq!(engine.cache.peek(&key), expected);
}

fn verify(engine: &TemporalEngine, value: u8) {
    let result = engine.batch_hash_multi_get_compact(BatchHashMultiGetRequest {
        shard_id: 1,
        keys: vec!["row".into()],
        fields: vec!["value".into()],
    });
    assert!(result.status.ok && result.errors.is_empty());
    assert_eq!(result.rows[0].0, vec![Some(vec![value; 48])]);
}

#[test]
fn read_filled_cache_registers_cold_frames_before_releasing_the_guard() {
    for borrowed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), false);
        BEFORE_SHARED_ROW_COMMIT.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(|engine, shard, _| {
                assert!(engine.shards.try_write().is_ok());
                let stats = engine.wal_store.stats_without_rescan(shard);
                assert_eq!((stats.writes, stats.syncs), (1, 0));
                assert_cached(engine, false);
                verify(engine, 20);
                assert_cached(engine, true);
            }))
        });
        let result = apply(&engine, request(&[(20, 20, 0)]), borrowed);
        assert!(result.status.ok && result.errors.is_empty());
        assert_eq!((result.applied, result.skipped), (1, 0));
        assert!(BEFORE_SHARED_ROW_COMMIT.with(|slot| slot.borrow().is_none()));
        verify(&engine, 20);
        let stats = engine.wal_store.stats_without_rescan(1);
        assert_eq!((stats.writes, stats.syncs), (1, 1));
    }
}

#[test]
fn read_filled_cache_recovers_without_an_eager_payload_copy() {
    for borrowed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), false);
        let result = apply(&engine, request(&[(20, 20, 0)]), borrowed);
        assert!(result.status.ok && result.errors.is_empty());
        assert_cached(&engine, false);
        drop(engine);
        let engine = open(dir.path(), false);
        verify(&engine, 20);
        let result = apply(&engine, request(&[(21, 21, 0)]), borrowed);
        assert!(result.status.ok && result.errors.is_empty());
        assert_eq!(result.applied, 1);
        assert_cached(&engine, true);
        verify(&engine, 21);
    }
}

#[test]
fn read_filled_cache_compares_actual_stored_timestamps_within_a_batch() {
    for borrowed in [false, true] {
        for padding in [0, 30, 70] {
            let dir = tempfile::tempdir().unwrap();
            let engine = open(dir.path(), false);
            let result = apply(
                &engine,
                request(&[
                    (10, 30, padding),
                    (20, 20, padding),
                    (40, 40, padding),
                    (39, 39, padding),
                ]),
                borrowed,
            );
            assert!(result.status.ok && result.errors.is_empty());
            assert_eq!((result.applied, result.skipped), (2, 2));
            assert_cached(&engine, padding > 64);
            verify(&engine, 40);
            let stats = engine.wal_store.stats_without_rescan(1);
            assert_eq!((stats.writes, stats.syncs), (1, 1));
        }
    }
}

#[test]
fn read_filled_cache_keeps_async_frames_eager() {
    for borrowed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), true);
        let result = apply(&engine, request(&[(20, 20, 0)]), borrowed);
        assert!(result.status.ok && result.errors.is_empty());
        assert_cached(&engine, true);
        verify(&engine, 20);
        assert_eq!(engine.wal_store.stats_without_rescan(1).syncs, 0);
    }
}

#[test]
fn read_filled_cache_stale_rows_do_not_warm_the_payload() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    assert!(apply(&engine, request(&[(20, 20, 0)]), true).status.ok);
    let result = apply(&engine, request(&[(19, 19, 0)]), true);
    assert!(result.status.ok && result.errors.is_empty());
    assert_eq!((result.applied, result.skipped), (0, 1));
    assert_cached(&engine, false);
    let stats = engine.wal_store.stats_without_rescan(1);
    assert_eq!((stats.writes, stats.syncs), (1, 1));
    verify(&engine, 20);
}

#[test]
fn read_filled_cache_eligibility_requires_a_shared_supported_projection() {
    let source = BatchCommandSource::Commands(Vec::new().into_iter());
    assert!(!source.supports_read_filled_frames());
    for (field, length, expected) in [
        (Some("timestamp"), 64, true),
        (Some("timestamp"), 65, false),
        (Some("absent"), 1, false),
        (None, 1, false),
    ] {
        let mut completed_keys = Vec::new();
        let source = BatchCommandSource::SharedTimestampRows {
            entries: vec![("timestamp".into(), Vec::new())],
            timestamp_field: field.map(str::to_owned),
            ttl_ms: None,
            rows: vec![BatchHashTimestampSetRow(
                "row".into(),
                20,
                0,
                vec![BatchHashMultiSetValue(vec![0; length])],
            )]
            .into_iter(),
            completed_keys: &mut completed_keys,
        };
        assert_eq!(source.supports_read_filled_frames(), expected);
    }
}

#[test]
fn read_filled_cache_unconditional_rows_remain_readable_after_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    for value in [10, 20] {
        let result = engine
            .batch_execute_shared_hash_rows(crate::types::BatchHashMultiSetRequest {
                shard_id: 1,
                fields: vec!["value".into()],
                ttl_ms: None,
                rows: vec![BatchHashMultiSetRow(
                    "row".into(),
                    vec![BatchHashMultiSetValue(vec![value; 48])],
                )],
            })
            .0;
        assert!(result.status.ok && result.errors.is_empty());
        assert_eq!(result.applied, 1);
        assert_cached(&engine, value == 20);
        verify(&engine, value);
        assert_cached(&engine, true);
    }
    drop(engine);
    verify(&open(dir.path(), false), 20);
}

#[test]
fn read_filled_cache_generic_batches_preserve_read_your_writes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    let result = engine.batch_execute(BatchExecuteRequest {
        shard_id: 1,
        commands: vec![
            Command::HashMultiSetIfTimestampOlder {
                key: "row".into(),
                timestamp_field: "timestamp".into(),
                timestamp_seconds: 20,
                timestamp_nanos: 0,
                ttl_ms: None,
                entries: vec![
                    (
                        "timestamp".into(),
                        Timestamp { seconds: 20 }.encode_to_vec(),
                    ),
                    ("value".into(), vec![20; 48]),
                ],
            },
            Command::HashGet {
                key: "row".into(),
                field: "value".into(),
            },
        ],
    });
    assert!(result.status.ok && result.responses.iter().all(|row| row.status.ok));
    assert_eq!(
        result.responses[1].response,
        CommandResponse::Bytes {
            value: Some(vec![20; 48])
        }
    );
    verify(&engine, 20);
}

#[test]
fn read_filled_cache_refreshes_demanded_rows_without_a_storage_miss() {
    for borrowed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), false);
        assert!(apply(&engine, request(&[(20, 20, 0)]), borrowed).status.ok);
        assert_cached(&engine, false);
        verify(&engine, 20);
        assert_cached(&engine, true);
        BEFORE_SHARED_ROW_COMMIT.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(|engine, _, _| {
                assert_cached(engine, true);
                let before = engine.wal_store.stats_without_rescan(1);
                verify(engine, 21);
                let after = engine.wal_store.stats_without_rescan(1);
                assert_eq!(
                    (before.reads, before.bytes_read),
                    (after.reads, after.bytes_read)
                );
            }))
        });
        let result = apply(&engine, request(&[(21, 21, 0)]), borrowed);
        assert!(result.status.ok && result.errors.is_empty());
        assert_eq!(result.applied, 1);
        assert!(BEFORE_SHARED_ROW_COMMIT.with(|slot| slot.borrow().is_none()));
        let stats = engine.wal_store.stats_without_rescan(1);
        assert_eq!((stats.writes, stats.syncs), (2, 2));
    }
}
