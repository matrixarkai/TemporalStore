// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::engine::container_blocks::SharedHashFields;
use crate::engine::storage_bucket_internals::ReplacedBlock;
use crate::engine::TemporalEngine;
use crate::{Config, SetConfigRequest};
use crate::types::{BatchHashMultiSetRequest, BatchHashMultiSetRow, BatchHashMultiSetValue,
    Command, CommandResponse, ExecuteRequest};

#[test]
fn shared_hash_fields_require_exact_unique_projection() {
    let fields = SharedHashFields::new(["a", "b", ""].into_iter()).unwrap();
    assert!(fields.matches(["a", "b", ""].into_iter()));
    assert!(!fields.matches(["b", "a", ""].into_iter()));
    assert!(!fields.matches(["a", "b"].into_iter()));
    assert!(!fields.matches(["a", "b", "c"].into_iter()));
    assert!(fields.contains(""));
    assert!(!fields.contains("c"));
    assert!(SharedHashFields::new(["a", "a"].into_iter()).is_none());
    assert!(SharedHashFields::new(std::iter::empty()).unwrap().names().is_empty());
}

#[test]
fn shared_hash_fields_select_borrowed_or_actual_names() {
    let fields = SharedHashFields::new(["a", "b"].into_iter()).unwrap();
    for _ in 0..500 {
        let WrittenHashFields::Shared(selected) = written_hash_fields(["a", "b"].into_iter(), Some(&fields))
            else { panic!("matching rows must not build another membership set") };
        assert!(std::ptr::eq(selected, &fields));
    }
    for hint in [Some(&fields), None] {
        let selected = written_hash_fields(["a", "c"].into_iter(), hint);
        assert!(matches!(selected, WrittenHashFields::Owned(_)));
        assert!(selected.contains("a"));
        assert!(selected.contains("c"));
        assert!(!selected.contains("b"));
    }
}

#[test]
fn shared_hash_fields_keep_single_retirement_inline() {
    let key = (1, 2, 3);
    let mut selected = ReplacedBlock::from_keys(std::iter::repeat(key).take(64));
    assert_eq!(selected, ReplacedBlock::One(key));
    selected.remove((9, 9, 9));
    assert_eq!(selected, ReplacedBlock::One(key));
    selected.remove(key);
    assert!(selected.is_empty());
    let other = (4, 5, 6);
    let mut selected = ReplacedBlock::from_keys([key, other, key, other].into_iter());
    assert!(matches!(selected, ReplacedBlock::Many(ref keys) if keys.len() == 2));
    selected.remove(key);
    assert_eq!(selected, ReplacedBlock::One(other));
    selected.remove(other);
    assert!(selected.is_empty());
}

#[test]
fn shared_hash_fields_retirement_matches_set_operations() {
    for length in 0..64 {
        let keys: Vec<_> = (0..length).map(|index| (index % 5, index % 3, 48)).collect();
        let mut expected: HashSet<_> = keys.iter().copied().collect();
        let mut selected = ReplacedBlock::from_keys(keys.into_iter());
        for key in (0..30).map(|index| (index % 5, index % 3, 48)) {
            let actual: HashSet<_> = match &selected {
                ReplacedBlock::None => HashSet::new(),
                ReplacedBlock::One(key) => HashSet::from([*key]),
                ReplacedBlock::Many(keys) => { assert!(keys.len() > 1); keys.clone() },
            };
            assert_eq!(actual, expected);
            selected.remove(key);
            selected.remove(key);
            expected.remove(&key);
            assert_eq!(selected.is_empty(), expected.is_empty());
        }
        assert!(selected.is_empty());
    }
}

fn apply_rows(engine: &TemporalEngine, fields: &[&str], generation: u8) {
    let (result, keys) = engine.batch_execute_shared_hash_rows(BatchHashMultiSetRequest {
        shard_id: 1,
        fields: fields.iter().map(|field| field.to_string()).collect(),
        rows: (0..8u8).map(|row| BatchHashMultiSetRow(format!("projection-row-{row}"),
            fields.iter().map(|field| BatchHashMultiSetValue(payload(row, field, generation))).collect())).collect(),
        ttl_ms: None,
    });
    assert!(result.status.ok, "{:?}", result.status);
    assert_eq!(result.applied, 8);
    assert_eq!(keys.len(), 8);
}

fn payload(row: u8, field: &str, generation: u8) -> Vec<u8> {
    let column = match field { "a" => 0, "b" => 1, "c" => 2, _ => unreachable!() };
    vec![generation * 32 + row * 3 + column; 48]
}

fn verify_rows(engine: &TemporalEngine, fields: &[(&str, u8)], block_count: usize) {
    for row in 0..8u8 {
        let response = engine.execute(ExecuteRequest { shard_id: 1,
            command: Command::HashGetAll { key: format!("projection-row-{row}") } });
        assert!(response.status.ok, "{:?}", response.status);
        let CommandResponse::HashEntries { entries } = response.response else { panic!("hash entries") };
        let values: std::collections::BTreeMap<_, _> = entries.into_iter().collect();
        let expected: std::collections::BTreeMap<_, _> = fields.iter()
            .map(|(field, generation)| (field.to_string(), payload(row, field, *generation))).collect();
        assert_eq!(values, expected);
    }
    let shards = engine.shards.read().unwrap();
    let shard = shards.get(&1).unwrap();
    for row in 0..8 {
        let key = format!("projection-row-{row}");
        let addresses: std::collections::BTreeSet<_> = shard.bucket_index.hashes.get(&key).unwrap()
            .values().map(crate::engine::live_block_key).collect();
        assert_eq!(addresses.len(), block_count);
        let filed: Vec<_> = shard.bucket_index.object_block_refs("hash", &key).unwrap().iter()
            .filter_map(|block_ref| {
                let bucket = shard.bucket_index.bucket_map.get(&block_ref.routing_bucket).unwrap();
                let block = bucket.block_index.get(&block_ref.block_ref_key).unwrap();
                (!block.deleted()).then(|| crate::engine::live_block_key(&block.address))
            }).collect();
        assert_eq!(filed.len(), block_count);
        assert_eq!(filed.into_iter().collect::<std::collections::BTreeSet<_>>(), addresses);
        let (start, end) = shard.routing_range();
        let bucket = shard.bucket_index.bucket_map.get(
            &crate::engine::hashing::block_routing_bucket(&key, start, end)).unwrap();
        let kind = crate::engine::storage_bucket_internals::stored_model_kind("hash");
        assert_eq!(bucket.tombstone_element_block(&key, kind, "c").is_some(), fields.len() == 2);
        assert!(bucket.tombstone_element_block(&key, kind, "b").is_none());
    }
}

#[test]
fn shared_hash_fields_overwrites_preserve_partial_rows_and_tombstones() {
    for async_storage in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let open = || {
            let engine = TemporalEngine::with_local_dirs(64 * 1024 * 1024,
                dir.path().join("cache"), dir.path().join("blocks"), dir.path().join("indexes"));
            engine.load_shard(1);
            engine
        };
        let engine = open();
        assert!(engine.set_config(SetConfigRequest { shard_id: 1,
            config: Config { version: 2, async_storage, ..Config::default() } }).ok);
        apply_rows(&engine, &["a", "b", "c"], 0);
        verify_rows(&engine, &[("a", 0), ("b", 0), ("c", 0)], 1);
        apply_rows(&engine, &["a", "b"], 1);
        verify_rows(&engine, &[("a", 1), ("b", 1), ("c", 0)], 2);
        for row in 0..8 {
            for field in ["b", "c"] {
                assert!(engine.execute(ExecuteRequest { shard_id: 1,
                    command: Command::HashDelete { key: format!("projection-row-{row}"), field: field.to_string() } }).status.ok);
            }
        }
        apply_rows(&engine, &["a", "b"], 2);
        verify_rows(&engine, &[("a", 2), ("b", 2)], 1);
        engine.write_ahead_log_store().flush(1).unwrap();
        drop(engine);
        let recovered = open();
        verify_rows(&recovered, &[("a", 2), ("b", 2)], 1);
        apply_rows(&recovered, &["a", "b", "c"], 3);
        verify_rows(&recovered, &[("a", 3), ("b", 3), ("c", 3)], 1);
    }
}
