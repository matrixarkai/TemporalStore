// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn engine_on(path: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        path.join("cache"),
        path.join("blocks"),
        path.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let reply = engine.execute(ExecuteRequest { shard_id: 1, command });
    assert!(reply.status.ok, "{:?}", reply.status);
    reply.response
}

fn reset_counts() {
    PROJECTED_FIELD_CACHE_HITS.with(|count| count.set(0));
    PROJECTED_FRAME_FETCHES.with(|count| count.set(0));
}

fn counts() -> (u64, u64) {
    (PROJECTED_FIELD_CACHE_HITS.with(std::cell::Cell::get),
     PROJECTED_FRAME_FETCHES.with(std::cell::Cell::get))
}

fn request(keys: Vec<String>, fields: &[&str]) -> BatchHashMultiGetRequest {
    BatchHashMultiGetRequest {
        shard_id: 1, keys,
        fields: fields.iter().map(|field| field.to_string()).collect(),
    }
}

fn read(engine: &TemporalEngine, fields: &[&str]) -> Vec<Option<Vec<u8>>> {
    let body = engine.batch_hash_multi_get_compact_msgpack(request(vec!["row".into()], fields));
    let mut response: BatchHashMultiGetResponse = rmp_serde::from_slice(&body).unwrap();
    assert!(response.status.ok, "{:?}", response.status);
    assert!(response.errors.is_empty());
    assert_eq!(response.rows.len(), 1);
    response.rows.remove(0).0
}

fn seed(engine: &TemporalEngine, field: &str, value: Vec<u8>) {
    run(engine, Command::HashSet { key: "row".into(), field: field.into(), value: value.clone() });
    assert_eq!(run(engine, Command::HashGet { key: "row".into(), field: field.into() }),
               CommandResponse::Bytes { value: Some(value) });
}

#[test]
fn timestamp_projection_reuses_seeded_fields_and_has_a_frame_fetch_control() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    let mut keys = Vec::new();
    let timestamp = <HashTimestamp as prost::Message>::encode_to_vec(&HashTimestamp { seconds: 20, nanos: 7 });
    for index in 0..32 {
        let key = format!("entity-{index}");
        let mut entries = vec![("timestamp".into(), timestamp.clone())];
        entries.extend((0..20).map(|field| (format!("f{field}"), vec![field as u8; 4096])));
        run(&engine, Command::HashMultiSetIfTimestampOlder {
            key: key.clone(), timestamp_field: "timestamp".into(),
            timestamp_seconds: 20, timestamp_nanos: 7, ttl_ms: None, entries,
        });
        assert!(engine.cache.get_shared(&CacheKey::hash(1, &key, "timestamp")).unwrap().is_some());
        keys.push(key);
    }
    let req = request(keys.clone(), &["timestamp"]);
    reset_counts();
    let hot = engine.batch_hash_multi_get_compact_msgpack(req.clone());
    assert_eq!(counts(), (32, 0));
    let response: BatchHashMultiGetResponse = rmp_serde::from_slice(&hot).unwrap();
    assert!(response.status.ok && response.errors.is_empty());
    assert_eq!(response.rows.len(), 32);
    assert!(response.rows.iter().all(|row| row.0 == vec![Some(timestamp.clone())]));

    for key in &keys {
        engine.cache.invalidate(&CacheKey::hash(1, key, "timestamp")).unwrap();
    }
    reset_counts();
    let control = engine.batch_hash_multi_get_compact_msgpack(req);
    assert_eq!(counts(), (0, 32));
    assert_eq!(hot, control, "cache reuse must preserve exact response bytes");
}

#[test]
fn projected_field_retains_the_cache_buffer_including_large_and_empty_values() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    for value in [Vec::new(), vec![0xff; 64 * 1024]] {
        seed(&engine, "value", value.clone());
        let cached = engine.cache.get_shared(&CacheKey::hash(1, "row", "value")).unwrap().unwrap();
        let fields = vec!["value".to_string()];
        let plan = container_blocks::Utf8ProjectionPlan::new(&fields);
        let mut scratch = container_blocks::Utf8ProjectionScratch::new();
        let info = engine.request_shard_info(1, false);
        let mut shards = engine.shards.write().unwrap();
        reset_counts();
        let projected = execute_hash_multi_get_projected(
            &engine.cache, &engine.block_store, 1,
            block_routing_bucket("row", info.start_routing_bucket, info.end_routing_bucket),
            shards.get_mut(&1).unwrap(), "row", &fields, &plan, &mut scratch,
        );
        assert_eq!(counts(), (1, 0));
        let ProjectedHashValues::Shared { ref frame, .. } = projected else {
            panic!("cached bytes must stay shared");
        };
        assert!(std::sync::Arc::ptr_eq(frame, &cached));
        assert_eq!(projected.value_at(0), Some(value.as_slice()));
        assert_eq!(projected.into_owned(), vec![Some(value)]);
    }
}

#[test]
fn cached_field_does_not_resurrect_a_deleted_index_entry() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    seed(&engine, "value", vec![1, 2, 3]);
    run(&engine, Command::HashSet { key: "row".into(), field: "neighbor".into(), value: vec![9] });
    let cache_key = CacheKey::hash(1, "row", "value");
    let cached = engine.cache.get_shared(&cache_key).unwrap().unwrap();
    run(&engine, Command::HashDelete { key: "row".into(), field: "value".into() });
    engine.cache.put_memory_only(cache_key, cached.as_ref().to_vec());
    reset_counts();
    assert_eq!(read(&engine, &["value"]), vec![None]);
    assert_eq!(counts(), (0, 0));
    assert_eq!(read(&engine, &["neighbor"]), vec![Some(vec![9])]);
}

#[test]
fn ordinary_writes_invalidate_the_projected_field_cache() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    seed(&engine, "value", vec![1]);
    for value in [vec![2], vec![3]] {
        let command = if value[0] == 2 {
            Command::HashSet { key: "row".into(), field: "value".into(), value: value.clone() }
        } else {
            Command::HashMultiSet { key: "row".into(), entries: vec![("value".into(), value.clone())] }
        };
        run(&engine, command);
        reset_counts();
        assert_eq!(read(&engine, &["value"]), vec![Some(value.clone())]);
        assert_eq!(counts(), (0, 1));
        run(&engine, Command::HashGet { key: "row".into(), field: "value".into() });
        reset_counts();
        assert_eq!(read(&engine, &["value"]), vec![Some(value)]);
        assert_eq!(counts(), (1, 0));
    }
}

#[test]
fn unborrowable_or_negative_cache_entries_keep_the_storage_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    seed(&engine, "value", vec![4, 5, 6]);
    let legacy = rmp_serde::to_vec_named(&serde_json::json!({"kind": "bytes", "value": [9, 8, 7]})).unwrap();
    for encoded in [
        vec![0xc1],
        encode_cached_response(&CommandResponse::Bytes { value: None }).unwrap(),
        encode_cached_response(&CommandResponse::Integer { value: 2 }).unwrap(),
        legacy,
    ] {
        engine.cache.put_memory_only(CacheKey::hash(1, "row", "value"), encoded);
        reset_counts();
        assert_eq!(read(&engine, &["value"]), vec![Some(vec![4, 5, 6])]);
        assert_eq!(counts(), (0, 1));
    }
}

#[test]
fn expiry_still_removes_a_hot_field_before_projecting_it() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    seed(&engine, "value", vec![1]);
    engine.shards.write().unwrap().get_mut(&1).unwrap()
        .expires_at_ms_mut().insert(std::sync::Arc::from("row"), 1);
    reset_counts();
    assert_eq!(read(&engine, &["value"]), vec![None]);
    assert_eq!(counts().0, 0);
    run(&engine, Command::HashSet { key: "row".into(), field: "value".into(), value: vec![2] });
    assert_eq!(read(&engine, &["value"]), vec![Some(vec![2])]);
}

#[test]
fn multiple_fields_and_duplicates_keep_the_full_projection_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    run(&engine, Command::HashMultiSet {
        key: "row".into(), entries: vec![("a".into(), vec![1]), ("b".into(), vec![2])],
    });
    run(&engine, Command::HashGet { key: "row".into(), field: "a".into() });
    for (fields, wanted, fetches) in [
        (vec!["a", "a"], vec![Some(vec![1]), Some(vec![1])], 1),
        (vec!["b", "a", "missing"], vec![Some(vec![2]), Some(vec![1]), None], 2),
    ] {
        reset_counts();
        assert_eq!(read(&engine, &fields), wanted);
        assert_eq!(counts(), (0, fetches));
    }
}
