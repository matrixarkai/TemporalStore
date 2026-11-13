// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::engine::TemporalEngine;
use crate::types::{Command, CommandResponse, ExecuteRequest};
use prost::Message;

#[derive(Clone, PartialEq, Message)]
struct HashTimestamp {
    #[prost(int64, tag = "1")]
    seconds: i64,
    #[prost(int32, tag = "2")]
    nanos: i32,
}

fn address(id: u64) -> ElementEntry {
    ElementEntry::from_parts(1, id * 16, 16, Some(id), None)
}

fn map() -> RecordedMap<HashKind> {
    let mut map = RecordedMap::default();
    for field in ["ts", "other"] {
        map.insert_element_for_test("row", std::sync::Arc::from(field), address(1));
    }
    map
}

#[test]
fn hints_are_source_bound_and_do_not_cross_fields_or_objects() {
    let mut map = map();
    map.remember_timestamp("absent", "ts", Some((10, 1)));
    map.remember_timestamp("row", "absent", Some((10, 1)));
    assert!(map.timestamp_hints.is_empty());
    map.remember_timestamp("row", "ts", Some((10, 1)));
    assert_eq!(map.cached_timestamp("row", "ts"), Some(Some((10, 1))));
    assert_eq!(map.cached_timestamp("row", "other"), None);
    assert_eq!(map.cached_timestamp("other", "ts"), None);
    map.insert_element_for_test("row", std::sync::Arc::from("other"), address(2));
    assert_eq!(map.cached_timestamp("row", "ts"), Some(Some((10, 1))));
    map.insert_element_for_test("row", std::sync::Arc::from("ts"), address(2));
    assert_eq!(map.cached_timestamp("row", "ts"), None);
    map.remember_timestamp("row", "ts", None);
    assert_eq!(map.cached_timestamp("row", "ts"), Some(None));
    map.replay_remove_element("row", &std::sync::Arc::from("ts"));
    assert_eq!(map.cached_timestamp("row", "ts"), None);
    assert_eq!(map.timestamp_hints.len(), 1);
    map.remove_for_test("row");
    assert!(map.timestamp_hints.is_empty());
}

#[test]
fn hint_keys_are_shared_and_hints_are_not_cloned_or_serialized() {
    let mut map = map();
    let before = serde_json::to_vec(&serde_json::to_value(&map).unwrap()).unwrap();
    map.remember_timestamp("row", "ts", Some((10, 0)));
    let (held, hint) = map.timestamp_hints.get_key_value("row").unwrap();
    let (key, row) = map.entries.get_key_value("row").unwrap();
    assert!(std::sync::Arc::ptr_eq(held, key));
    assert!(std::sync::Arc::ptr_eq(&hint.field, row.elements.named_entry("ts").unwrap().0));
    let canonical: ModelKey = std::sync::Arc::from("row");
    assert!(map.adopt_key("row", &canonical));
    assert!(std::sync::Arc::ptr_eq(map.timestamp_hints.get_key_value("row").unwrap().0, &canonical));
    assert_eq!(before, serde_json::to_vec(&serde_json::to_value(&map).unwrap()).unwrap());
    let cloned = map.clone();
    assert_eq!(cloned, map);
    assert!(cloned.timestamp_hints.is_empty());
    let loaded: RecordedMap<HashKind> = serde_json::from_slice(&before).unwrap();
    assert_eq!(loaded, map);
    assert!(loaded.timestamp_hints.is_empty());
    assert_eq!(std::mem::size_of::<super::super::hash_field_map::HashFieldMap>(),
               std::mem::size_of::<Vec<(super::super::hash_field_map::FieldName, ElementEntry)>>());
}

#[test]
fn compaction_fold_and_reconcile_cannot_reuse_a_different_source() {
    let mut map = map();
    map.remember_timestamp("row", "ts", Some((10, 0)));
    for (_, mut values) in map.element_values_mut() {
        for (_, value) in values.iter_mut() { *value = address(2); }
    }
    assert_eq!(map.cached_timestamp("row", "ts"), None);
    map.remember_timestamp("row", "ts", Some((20, 0)));
    let rows = vec![(std::sync::Arc::<str>::from("ts"), address(3))];
    let live = std::collections::HashSet::from([super::super::live_block_key(&address(3))]);
    map.fold_carried_elements("row", Some(&serde_json::to_value(&rows).unwrap()), &live, &mut 0);
    assert!(map.timestamp_hints.is_empty());
    map.remember_timestamp("row", "ts", Some((30, 0)));
    map.reconcile_from_durable(HashMap::new(), &live, &mut 0);
    assert!(map.timestamp_hints.is_empty());
}

fn open(path: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(64 * 1024,
        path.join("cache"), path.join("blocks"), path.join("indexes"));
    engine.load_shard(1);
    engine
}

fn execute(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let response = engine.execute(ExecuteRequest { shard_id: 1, command });
    assert!(response.status.ok, "{:?}", response.status);
    response.response
}

fn stamp(seconds: i64) -> Vec<u8> {
    HashTimestamp { seconds, nanos: 0 }.encode_to_vec()
}

fn conditional(engine: &TemporalEngine, seconds: i64, entries: Vec<(String, Vec<u8>)>) -> i64 {
    let result = execute(engine, Command::HashMultiSetIfTimestampOlder {
        key: "row".into(), timestamp_field: "ts".into(),
        timestamp_seconds: seconds, timestamp_nanos: 0, ttl_ms: None, entries,
    });
    let CommandResponse::Integer { value } = result else { panic!("expected conditional result") };
    value
}

fn evict_payload_caches(engine: &TemporalEngine) {
    for key in engine.cache.keys_for_shard(1) { engine.cache.invalidate(&key).unwrap(); }
}

#[test]
fn comparisons_use_stored_bytes_and_survive_payload_cache_eviction() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    assert_eq!(conditional(&engine, 100, vec![("ts".into(), stamp(10)), ("v".into(), vec![1; 48])]), 1);
    evict_payload_caches(&engine);
    let before = engine.write_ahead_log_store().stats(1).bytes_read;
    assert_eq!(conditional(&engine, 5, vec![("ts".into(), stamp(5)), ("v".into(), vec![2; 48])]), 0);
    assert_eq!(engine.write_ahead_log_store().stats(1).bytes_read, before);
    assert_eq!(conditional(&engine, 20, vec![("ts".into(), stamp(20)), ("ts".into(), stamp(40))]), 1);
    evict_payload_caches(&engine);
    assert_eq!(conditional(&engine, 30, vec![("ts".into(), stamp(30))]), 0);
    assert_eq!(execute(&engine, Command::HashGet { key: "row".into(), field: "ts".into() }),
               CommandResponse::Bytes { value: Some(stamp(40)) });
}

#[test]
fn ordinary_writes_deletes_and_recreation_cannot_leave_stale_comparators() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    assert_eq!(conditional(&engine, 10, vec![("ts".into(), stamp(10)), ("v".into(), vec![1])]), 1);
    execute(&engine, Command::HashSet { key: "row".into(), field: "ts".into(), value: stamp(50) });
    evict_payload_caches(&engine);
    assert_eq!(conditional(&engine, 20, vec![("ts".into(), stamp(20))]), 0);
    execute(&engine, Command::HashDelete { key: "row".into(), field: "ts".into() });
    assert_eq!(conditional(&engine, 1, vec![("ts".into(), stamp(1))]), 1);
    execute(&engine, Command::CommonDelete { key: "row".into() });
    assert!(engine.shards.read().unwrap().get(&1).unwrap().bucket_index.hashes.timestamp_hints.is_empty());
    assert_eq!(conditional(&engine, 0, vec![("ts".into(), stamp(0))]), 1);
}

#[test]
fn expiry_and_reload_do_not_restore_derived_hints() {
    struct Clock;
    impl Drop for Clock { fn drop(&mut self) { crate::engine::set_replay_clock_ms(None); } }
    let _clock = Clock;
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    crate::engine::set_replay_clock_ms(Some(100000));
    let result = execute(&engine, Command::HashMultiSetIfTimestampOlder {
        key: "row".into(), timestamp_field: "ts".into(), timestamp_seconds: 50,
        timestamp_nanos: 0, ttl_ms: Some(10), entries: vec![("ts".into(), stamp(50))],
    });
    assert_eq!(result, CommandResponse::Integer { value: 1 });
    crate::engine::set_replay_clock_ms(Some(100020));
    assert_eq!(conditional(&engine, 1, vec![("ts".into(), stamp(1))]), 1);
    crate::engine::set_replay_clock_ms(None);
    engine.write_ahead_log_store().flush(1).unwrap();
    drop(engine);
    let engine = open(dir.path());
    assert!(engine.shards.read().unwrap().get(&1).unwrap().bucket_index.hashes.timestamp_hints.is_empty());
    assert_eq!(conditional(&engine, 0, vec![("ts".into(), stamp(0))]), 0);
    assert_eq!(conditional(&engine, 2, vec![("ts".into(), stamp(2))]), 1);
}

#[test]
#[cfg(feature = "alloc-probe")]
fn repeated_hint_updates_reuse_names_keys_and_reserved_slots() {
    let mut map = map();
    map.remember_timestamp("row", "ts", Some((1, 0)));
    let probe = crate::alloc_probe::Probe::start();
    for seconds in 0..1000 {
        map.remember_timestamp("row", "ts", Some((seconds, 0)));
        assert_eq!(map.cached_timestamp("row", "ts"), Some(Some((seconds, 0))));
    }
    let reading = probe.stop();
    assert_eq!(reading.allocs, 0);
    assert_eq!(map.timestamp_hints.len(), 1);
    println!("TIMESTAMP_HINT entry_bytes={} capacity={} repeated_updates=1000 allocations={}",
             std::mem::size_of::<(ModelKey, TimestampHint)>(), map.timestamp_hints.capacity(), reading.allocs);
}

#[test]
fn malformed_and_oversized_stored_timestamps_do_not_reuse_comparison_arguments() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    assert_eq!(conditional(&engine, 10, vec![("ts".into(), stamp(10))]), 1);
    assert_eq!(conditional(&engine, 20, vec![("ts".into(), vec![0xff])]), 1);
    evict_payload_caches(&engine);
    assert_eq!(conditional(&engine, 15, vec![("ts".into(), stamp(15))]), 1);
    let mut large = stamp(50);
    large.extend([0x1a, 80]);
    large.extend([b'x'; 80]);
    assert!(large.len() > 64);
    assert_eq!(conditional(&engine, 30, vec![("ts".into(), large.clone())]), 1);
    evict_payload_caches(&engine);
    assert_eq!(conditional(&engine, 40, vec![("ts".into(), stamp(40))]), 0);
    assert_eq!(execute(&engine, Command::HashGet { key: "row".into(), field: "ts".into() }),
               CommandResponse::Bytes { value: Some(large) });
}

#[test]
fn repeated_rows_in_one_batch_observe_the_latest_uncommitted_field_address() {
    use crate::types::{BatchHashTimestampSetRequest, BatchHashTimestampSetRow, BatchHashMultiSetValue};
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let (response, _) = engine.batch_execute_packed_hash_rows(BatchHashTimestampSetRequest {
        shard_id: 1, fields: vec!["ts".into(), "v".into()], timestamp_field: "ts".into(), ttl_ms: None,
        rows: [30, 20, 40].into_iter().map(|seconds| BatchHashTimestampSetRow(
            "row".into(), seconds, 0,
            vec![BatchHashMultiSetValue(stamp(seconds)), BatchHashMultiSetValue(vec![seconds as u8])],
        )).collect(),
    });
    assert!(response.status.ok);
    assert_eq!((response.applied, response.skipped), (2, 1));
    evict_payload_caches(&engine);
    assert_eq!(conditional(&engine, 35, vec![("ts".into(), stamp(35))]), 0);
    assert_eq!(execute(&engine, Command::HashGet { key: "row".into(), field: "v".into() }),
               CommandResponse::Bytes { value: Some(vec![40]) });
}

#[test]
#[cfg(feature = "alloc-probe")]
fn hint_population_cost_is_resident_metadata_not_a_payload_cache_budget() {
    let mut map = RecordedMap::<HashKind>::default();
    let field: super::super::hash_field_map::FieldName = std::sync::Arc::from("ts");
    let keys: Vec<_> = (0..52000).map(|row| format!("row-{row}")).collect();
    for key in &keys {
        map.insert_element_for_test(key, field.clone(), address(1));
    }
    let probe = crate::alloc_probe::Probe::start();
    for key in &keys {
        map.remember_timestamp_value(key, "ts", TimestampHintValue::from_bytes(&[0x08, 1]));
    }
    let reading = probe.stop();
    assert_eq!(map.timestamp_hints.len(), map.len());
    assert!(reading.allocs < 32, "keys and names must not be allocated per hint: {reading:?}");
    let retained_bytes = reading.alloc_bytes as i64 - reading.free_bytes as i64;
    assert!(retained_bytes >= (keys.len() * std::mem::size_of::<(ModelKey, TimestampHint)>()) as i64);
    println!("TIMESTAMP_HINT_POPULATION rows={} entries={} capacity={} entry_bytes={} allocated_bytes={} retained_requested_bytes={} allocations={}",
             keys.len(), map.timestamp_hints.len(), map.timestamp_hints.capacity(),
             std::mem::size_of::<(ModelKey, TimestampHint)>(), reading.alloc_bytes,
             retained_bytes, reading.allocs);
}

fn project(engine: &TemporalEngine, key: &str, fields: &[&str]) -> Vec<Option<Vec<u8>>> {
    let bytes = engine.batch_hash_multi_get_compact_msgpack(crate::types::BatchHashMultiGetRequest {
        shard_id: 1, keys: vec![key.into()], fields: fields.iter().map(|field| field.to_string()).collect(),
    });
    let mut result: crate::types::BatchHashMultiGetResponse = rmp_serde::from_slice(&bytes).unwrap();
    assert!(result.status.ok && result.errors.is_empty(), "{result:?}");
    assert_eq!(result.rows.len(), 1);
    result.rows.remove(0).0
}

#[test]
fn exact_bytes_are_source_bound_and_not_recreated_from_the_parsed_timestamp() {
    let mut map = map();
    let raw = [0x08, 0x8a, 0, 0x18, 7];
    map.remember_timestamp_value("row", "ts", TimestampHintValue::from_bytes(&raw));
    assert_eq!(map.cached_timestamp("row", "ts"), Some(Some((10, 0))));
    assert_eq!(map.cached_timestamp_bytes("row", "ts"), Some(raw.as_slice()));
    assert_eq!(map.cached_timestamp_bytes("row", "other"), None);
    assert_eq!(map.cached_timestamp_bytes("missing", "ts"), None);
    assert_eq!(map.clone().cached_timestamp_bytes("row", "ts"), None);
    map.insert_element_for_test("row", std::sync::Arc::from("ts"), address(2));
    assert_eq!(map.cached_timestamp_bytes("row", "ts"), None);
    map.remember_timestamp_value("row", "ts", TimestampHintValue::from_bytes(&[]));
    assert_eq!(map.cached_timestamp_bytes("row", "ts"), Some([].as_slice()));
    assert_eq!(map.cached_timestamp("row", "ts"), Some(Some((0, 0))));
    map.replay_remove_element("row", &std::sync::Arc::from("ts"));
    assert_eq!(map.cached_timestamp_bytes("row", "ts"), None);
}

#[test]
fn raw_point_and_projected_reads_preserve_unknown_noncanonical_empty_and_malformed_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    for (index, raw) in [vec![0x08, 10, 0x18, 7], vec![0x08, 0x8a, 0], Vec::new(), vec![0xff]].into_iter().enumerate() {
        assert_eq!(conditional(&engine, 1000 + index as i64,
            vec![("ts".into(), raw.clone()), ("v".into(), vec![index as u8; 48])]), 1);
        evict_payload_caches(&engine);
        assert_eq!(execute(&engine, Command::HashGet { key: "row".into(), field: "ts".into() }),
                   CommandResponse::Bytes { value: Some(raw.clone()) });
        assert_eq!(execute(&engine, Command::HashMultiGet { key: "row".into(), fields: vec!["ts".into()] }),
                   CommandResponse::Values { values: vec![Some(raw.clone())] });
        assert_eq!(project(&engine, "row", &["ts"]), vec![Some(raw.clone())]);
        assert_eq!(project(&engine, "row", &["ts", "ts", "v", "missing"]),
                   vec![Some(raw.clone()), Some(raw), Some(vec![index as u8; 48]), None]);
    }
}

#[test]
fn inline_limit_keeps_large_timestamp_reads_on_the_exact_storage_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    for length in [24, 25, 64, 65, 128] {
        let mut raw = stamp(10);
        let payload = length - raw.len() - 2;
        raw.extend([0x1a, payload as u8]);
        raw.resize(length, b'x');
        assert_eq!(conditional(&engine, 1000, vec![("ts".into(), raw.clone()), ("v".into(), vec![1; 48])]), 1);
        {
            let shards = engine.shards.read().unwrap();
            let hashes = &shards.get(&1).unwrap().bucket_index.hashes;
            assert_eq!(hashes.cached_timestamp_bytes("row", "ts"), (length <= 24).then_some(raw.as_slice()));
        }
        evict_payload_caches(&engine);
        assert_eq!(project(&engine, "row", &["ts"]), vec![Some(raw.clone())]);
        assert_eq!(conditional(&engine, 5, vec![("ts".into(), stamp(5))]), 0);
        assert_eq!(execute(&engine, Command::HashGet { key: "row".into(), field: "ts".into() }),
                   CommandResponse::Bytes { value: Some(raw) });
    }
}

#[test]
fn raw_hint_reads_follow_ordinary_replacement_deletion_and_stale_fallback_learning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    assert_eq!(conditional(&engine, 10, vec![("ts".into(), stamp(10)), ("v".into(), vec![1; 48])]), 1);
    let raw = vec![0x08, 90, 0x18, 9];
    execute(&engine, Command::HashMultiSet { key: "row".into(), entries: vec![("ts".into(), raw.clone())] });
    evict_payload_caches(&engine);
    assert_eq!(project(&engine, "row", &["ts"]), vec![Some(raw.clone())]);
    assert_eq!(conditional(&engine, 50, vec![("ts".into(), stamp(50))]), 0);
    evict_payload_caches(&engine);
    {
        let shards = engine.shards.read().unwrap();
        assert_eq!(shards.get(&1).unwrap().bucket_index.hashes.cached_timestamp_bytes("row", "ts"), Some(raw.as_slice()));
    }
    assert_eq!(project(&engine, "row", &["ts"]), vec![Some(raw)]);
    execute(&engine, Command::HashDelete { key: "row".into(), field: "ts".into() });
    assert_eq!(project(&engine, "row", &["ts"]), vec![None]);
    execute(&engine, Command::CommonDelete { key: "row".into() });
    assert_eq!(conditional(&engine, 1, vec![("ts".into(), stamp(1))]), 1);
    assert_eq!(project(&engine, "row", &["ts"]), vec![Some(stamp(1))]);
}

#[test]
fn raw_hint_reads_respect_expiry_and_survive_restart_without_persisting_hints() {
    struct Clock;
    impl Drop for Clock { fn drop(&mut self) { crate::engine::set_replay_clock_ms(None); } }
    let _clock = Clock;
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    crate::engine::set_replay_clock_ms(Some(100000));
    execute(&engine, Command::HashMultiSetIfTimestampOlder {
        key: "row".into(), timestamp_field: "ts".into(), timestamp_seconds: 10, timestamp_nanos: 0,
        ttl_ms: Some(10), entries: vec![("ts".into(), stamp(10)), ("v".into(), vec![1; 48])],
    });
    assert_eq!(project(&engine, "row", &["ts"]), vec![Some(stamp(10))]);
    crate::engine::set_replay_clock_ms(Some(100020));
    assert_eq!(project(&engine, "row", &["ts"]), vec![None]);
    assert_eq!(execute(&engine, Command::HashGet { key: "row".into(), field: "ts".into() }),
               CommandResponse::Bytes { value: None });
    let raw = vec![0x08, 10, 0x18, 7];
    assert_eq!(conditional(&engine, 20, vec![("ts".into(), raw.clone())]), 1);
    crate::engine::set_replay_clock_ms(None);
    engine.write_ahead_log_store().flush(1).unwrap();
    drop(engine);
    let engine = open(dir.path());
    assert!(engine.shards.read().unwrap().get(&1).unwrap().bucket_index.hashes.timestamp_hints.is_empty());
    assert_eq!(project(&engine, "row", &["ts"]), vec![Some(raw)]);
}

#[test]
#[cfg(feature = "alloc-probe")]
fn inline_timestamp_updates_allocate_no_payload_buffers() {
    let mut map = map();
    map.remember_timestamp_value("row", "ts", TimestampHintValue::from_bytes(&[0x08, 1]));
    let probe = crate::alloc_probe::Probe::start();
    for seconds in 0..1000 {
        let raw = [0x08, (seconds % 128) as u8];
        map.remember_timestamp_value("row", "ts", TimestampHintValue::from_bytes(&raw));
        assert_eq!(map.cached_timestamp_bytes("row", "ts"), Some(raw.as_slice()));
        assert_eq!(map.cached_timestamp("row", "ts"), Some(Some(((seconds % 128) as i64, 0))));
    }
    let reading = probe.stop();
    assert_eq!(reading.allocs, 0);
    println!("INLINE_TIMESTAMP_UPDATES updates=1000 allocations={} entry_bytes={}", reading.allocs,
             std::mem::size_of::<(ModelKey, TimestampHint)>());
}
