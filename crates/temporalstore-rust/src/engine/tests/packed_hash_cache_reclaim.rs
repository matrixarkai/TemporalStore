// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::types::{BatchHashMultiSetValue, BatchHashMultiSetRow};

fn open(root: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs_block_store_options_and_disk_cache(
        16 * 1024 * 1024, root.join("cache"), root.join("blocks"), root.join("indexes"),
        crate::block_store::BlockStoreOptions::default(), false,
    );
    assert!(engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1, load_version: 1, local_node_id: None, shard_uri: String::new(),
        start_routing_bucket: 0, end_routing_bucket: 1023, readonly: false, table_name: "packed-cache".into(),
    }).status.ok);
    engine
}

fn put(engine: &TemporalEngine, key: &str, fields: &[&str], value: u8, bytes: usize) {
    let (result, _) = engine.batch_execute_shared_hash_rows(BatchHashMultiSetRequest {
        shard_id: 1, fields: fields.iter().map(|field| field.to_string()).collect(), ttl_ms: None,
        rows: vec![BatchHashMultiSetRow(key.to_string(), fields.iter()
            .map(|_| BatchHashMultiSetValue(vec![value; bytes])).collect())],
    });
    assert!(result.status.ok && result.errors.is_empty() && result.applied == 1);
}

fn page(engine: &TemporalEngine, key: &str, field: &str) -> CacheKey {
    let shards = engine.shards.read().unwrap();
    let shard = shards.get(&1).unwrap();
    let address = shard.bucket_index.hashes.get(key).unwrap().get(field).unwrap();
    CacheKey::page_with_slot(1, address.block_slab_id(), address.offset(), address.length(),
        Some(block_routing_bucket(key, 0, 1023)))
}

fn get(engine: &TemporalEngine, key: &str, field: &str) -> Vec<u8> {
    let response = engine.execute(ExecuteRequest { shard_id: 1,
        command: Command::HashGet { key: key.to_string(), field: field.to_string() } });
    assert!(response.status.ok);
    let CommandResponse::Bytes { value: Some(bytes) } = response.response else { panic!("hash value") };
    bytes
}

#[test]
fn full_replacement_retires_cached_frame_but_preserves_held_read_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    put(&engine, "row", &["a", "b"], 1, 48);
    let old = page(&engine, "row", "a");
    let held = engine.cache.get_shared(&old).unwrap().unwrap();
    let expected = held.to_vec();
    put(&engine, "row", &["a", "b"], 2, 48);
    let new = page(&engine, "row", "a");
    assert_ne!(old, new);
    assert!(!engine.cache.peek(&old));
    assert!(engine.cache.peek(&new));
    assert_eq!(held.as_ref(), expected);
    assert_eq!(get(&engine, "row", "a"), vec![2; 48]);
    assert_eq!(get(&engine, "row", "b"), vec![2; 48]);
}

#[test]
fn partial_replacement_keeps_the_frame_until_its_last_field_moves() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    put(&engine, "row", &["a", "b"], 1, 48);
    let old = page(&engine, "row", "a");
    put(&engine, "row", &["a"], 2, 48);
    let first = page(&engine, "row", "a");
    assert!(engine.cache.peek(&old));
    assert_eq!(get(&engine, "row", "b"), vec![1; 48]);
    put(&engine, "row", &["b"], 3, 48);
    assert!(!engine.cache.peek(&old));
    assert!(engine.cache.peek(&first));
    assert_eq!(get(&engine, "row", "a"), vec![2; 48]);
    assert_eq!(get(&engine, "row", "b"), vec![3; 48]);
}

#[test]
fn packed_replacement_retires_each_vacated_scalar_frame() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    for field in ["a", "b"] {
        assert!(engine.execute(ExecuteRequest { shard_id: 1, command: Command::HashSet {
            key: "row".into(), field: field.into(), value: vec![1; 48],
        }}).status.ok);
        assert_eq!(get(&engine, "row", field), vec![1; 48]);
    }
    let first = page(&engine, "row", "a");
    let second = page(&engine, "row", "b");
    assert_ne!(first, second);
    assert!(engine.cache.peek(&first) && engine.cache.peek(&second));
    put(&engine, "row", &["a", "b"], 2, 48);
    assert!(!engine.cache.peek(&first) && !engine.cache.peek(&second));
    assert_eq!(get(&engine, "row", "a"), vec![2; 48]);
    assert_eq!(get(&engine, "row", "b"), vec![2; 48]);
}

#[test]
fn whole_row_overwrites_keep_only_one_cached_generation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    for version in 1..=4 {
        for row in 0..128 {
            put(&engine, &format!("row-{row:03}"), &["a", "b", "c"], version, 128);
        }
        let bytes = engine.cache.stats().memory_bytes;
        let expected = 128 * engine.cache.get_shared(&page(&engine, "row-000", "a")).unwrap().unwrap().len() as u64;
        assert_eq!(bytes, expected, "only live packed frames should occupy the value cache");
    }
}

#[test]
fn physical_packed_frames_use_their_stored_address_for_cache_retirement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    put(&engine, "large", &["a", "b"], 1, 9000);
    let old = page(&engine, "large", "a");
    assert_eq!(get(&engine, "large", "a"), vec![1; 9000]);
    let held = engine.cache.get_shared(&old).unwrap().unwrap();
    let expected = held.to_vec();
    put(&engine, "large", &["a", "b"], 2, 9000);
    assert!(!engine.cache.peek(&old));
    assert_eq!(get(&engine, "large", "a"), vec![2; 9000]);
    assert_eq!(get(&engine, "large", "b"), vec![2; 9000]);
    assert_eq!(held.as_ref(), expected);
}
