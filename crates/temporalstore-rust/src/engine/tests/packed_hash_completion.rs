// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::types::{BatchHashMultiSetRequest, BatchHashMultiSetRow, BatchHashMultiSetValue,
    BatchHashTimestampSetRequest, BatchHashTimestampSetRow};
use crate::engine::hash_field_map::{binary_search_count_for_test, reset_binary_search_count_for_test};
use crate::engine::storage_bucket_internals::bucket_block_upserts;

fn open(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(64 * 1024 * 1024,
        dir.join("cache"), dir.join("blocks"), dir.join("indexes"));
    engine.load_shard(1);
    engine
}

fn shared_rows(engine: &TemporalEngine, fields: &[String], generation: u8) {
    let (response, keys) = engine.batch_execute_shared_hash_rows(BatchHashMultiSetRequest {
        shard_id: 1, fields: fields.to_vec(), ttl_ms: None,
        rows: (0..4).map(|row| BatchHashMultiSetRow(format!("completion-{row}"),
            fields.iter().enumerate().map(|(column, _)|
                BatchHashMultiSetValue(vec![generation + column as u8; 48])).collect())).collect(),
    });
    assert!(response.status.ok, "{:?}", response.status);
    assert_eq!(response.applied, 4);
    assert_eq!(keys.len(), 4);
}

fn verify(engine: &TemporalEngine, fields: &[String], generation: u8) {
    for row in 0..4 {
        let response = engine.execute(ExecuteRequest { shard_id: 1,
            command: Command::HashMultiGet { key: format!("completion-{row}"), fields: fields.to_vec() } });
        assert!(response.status.ok, "{:?}", response.status);
        let CommandResponse::Values { values } = response.response else {
            panic!("expected projected hash values")
        };
        assert_eq!(values, fields.iter().enumerate()
            .map(|(column, _)| Some(vec![generation + column as u8; 48])).collect::<Vec<_>>());
        let shards = engine.shards.read().unwrap();
        let shard = shards.get(&1).unwrap();
        let key = format!("completion-{row}");
        let addresses: std::collections::BTreeSet<_> = shard.bucket_index.hashes.get(&key).unwrap()
            .values().map(crate::engine::live_block_key).collect();
        assert_eq!(addresses.len(), 1);
        let refs = shard.bucket_index.object_block_refs("hash", &key).unwrap();
        assert_eq!(refs.len(), 1, "the primary packed filing must remain reachable exactly once");
        let block_ref = refs.iter().next().unwrap();
        let bucket = shard.bucket_index.bucket_map.get(&block_ref.routing_bucket).unwrap();
        let block = bucket.block_index.get(&block_ref.block_ref_key).unwrap();
        assert!(!block.deleted());
        assert!(addresses.contains(&crate::engine::live_block_key(&block.address)));
    }
}

#[test]
fn packed_hash_completion_removes_the_second_index_pass() {
    for async_storage in [false, true] {
        for width in [1, 20, 64] {
            let dir = tempfile::tempdir().unwrap();
            let engine = open(dir.path());
            assert!(engine.set_config(crate::SetConfigRequest { shard_id: 1,
                config: crate::Config { version: 2, async_storage, ..crate::Config::default() } }).ok);
            let fields: Vec<_> = (0..width).map(|i| format!("f{i:03}")).collect();
            shared_rows(&engine, &fields, 0);
            reset_binary_search_count_for_test();
            let before = bucket_block_upserts();
            shared_rows(&engine, &fields, 1);
            assert_eq!(bucket_block_upserts() - before, 0, "no redundant maintenance filing");
            assert_eq!(binary_search_count_for_test(), 0, "WAL frames need no ordinal or maintenance lookups");
            // Exercise the skipped maintenance as a positive control on both counters.
            let before = bucket_block_upserts();
            reset_binary_search_count_for_test();
            {
                let mut shards = engine.shards.write().unwrap();
                let shard = shards.get_mut(&1).unwrap();
                for row in 0..4 {
                    assert!(crate::engine::storage_bucket_internals::sync_written_hash_fields(
                        shard, 1, &format!("completion-{row}"), fields.iter().map(String::as_str)));
                }
            }
            assert_eq!(bucket_block_upserts() - before, 4);
            assert_eq!(binary_search_count_for_test(), (4 * width) as u64);
            verify(&engine, &fields, 1);
            engine.write_ahead_log_store().flush(1).unwrap();
            drop(engine);
            verify(&open(dir.path()), &fields, 1);
        }
    }
}

#[test]
fn packed_hash_completion_timestamp_rows_keep_stale_writes_inert() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let request = |seconds| BatchHashTimestampSetRequest {
        shard_id: 1, fields: vec!["ts".into(), "value".into()], timestamp_field: "ts".into(), ttl_ms: None,
        rows: vec![BatchHashTimestampSetRow("timed".into(), seconds, 0, vec![
            BatchHashMultiSetValue(prost::Message::encode_to_vec(&HashTimestamp { seconds, nanos: 0 })),
            BatchHashMultiSetValue(vec![seconds as u8; 48]),
        ])],
    };
    assert_eq!(engine.batch_execute_packed_hash_rows(request(10)).0.applied, 1);
    reset_binary_search_count_for_test();
    let before = bucket_block_upserts();
    let response = engine.batch_execute_packed_hash_rows(request(20)).0;
    assert!(response.status.ok);
    assert_eq!((response.applied, response.skipped), (1, 0));
    assert_eq!(bucket_block_upserts() - before, 0);
    assert_eq!(binary_search_count_for_test(), 2, "the condition and its new source-bound hint each locate the timestamp field");
    reset_binary_search_count_for_test();
    let before = bucket_block_upserts();
    let response = engine.batch_execute_packed_hash_rows(request(15)).0;
    assert!(response.status.ok);
    assert_eq!((response.applied, response.skipped), (0, 1));
    assert_eq!(bucket_block_upserts() - before, 0);
    assert_eq!(binary_search_count_for_test(), 1);
    engine.write_ahead_log_store().flush(1).unwrap();
    drop(engine);
    let restored = open(dir.path());
    let response = restored.execute(ExecuteRequest { shard_id: 1,
        command: Command::HashGet { key: "timed".into(), field: "value".into() } });
    assert!(response.status.ok);
    assert_eq!(response.response, CommandResponse::Bytes { value: Some(vec![20; 48]) });
}

#[test]
fn packed_hash_completion_marker_excludes_empty_and_scalar_writes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let mut shards = engine.shards.write().unwrap();
    let shard = shards.get_mut(&1).unwrap();
    let (start, end) = shard.routing_range();
    let key = "marker";
    let bucket = block_routing_bucket(key, start, end);
    let object = stable_block_object_id(1, "hash", key);
    let (outcome, _, filed) = execute_hash_multi_set_reusing_metadata_at_bucket(
        &engine.cache, &engine.block_store, 1, bucket, object, shard, true, true,
        key.into(), Vec::new(), true, None);
    assert!(!outcome.mutated && filed.is_none());
    let (outcome, _, filed) = execute_hash_multi_set_reusing_metadata_at_bucket(
        &engine.cache, &engine.block_store, 1, bucket, object, shard, true, false,
        key.into(), vec![("a".into(), vec![1; 48])], true, None);
    assert!(outcome.mutated && filed.is_none());
    let (outcome, _, filed) = execute_hash_multi_set_reusing_metadata_at_bucket(
        &engine.cache, &engine.block_store, 1, bucket, object, shard, true, true,
        key.into(), vec![("b".into(), vec![2; 48])], true, None);
    assert!(outcome.mutated && filed.is_some());
    let (outcome, _, filed) = execute_packed_conditional_hash_write_at_bucket(
        &engine.cache, &engine.block_store, 1, bucket, object, shard, true,
        key.into(), "ts".into(), 30, 0, Some(60_000), Vec::new(), true, None, None);
    assert!(outcome.mutated && filed.is_none(), "expiry alone is not a completed packed write");
    let _ = crate::engine::block_in_wal::take_outcomes();
}

#[test]
fn packed_hash_completion_ordinal_is_lazy_only_for_wal_frames() {
    for async_storage in [false, true] {
        for length in [64, 8192] {
            let dir = tempfile::tempdir().unwrap();
            let engine = open(dir.path());
            let calls = std::cell::Cell::new(0);
            let address = crate::engine::append_owned_value_of_object(
                &engine.cache, &engine.block_store, 1, vec![3; length], 42, 1,
                async_storage, || { calls.set(calls.get() + 1); 7 }).unwrap();
            let physical = !async_storage || length >= 8192;
            assert_eq!(calls.get(), usize::from(physical));
            assert_eq!(address.block_id(), physical.then_some(7));
            let _ = crate::engine::block_in_wal::take_outcomes();
        }
    }
}
