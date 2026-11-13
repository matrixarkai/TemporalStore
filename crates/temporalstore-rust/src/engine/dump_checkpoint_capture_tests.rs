// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn engine_at(root: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 << 20, root.join("cache"), root.join("blocks"), root.join("indexes"),
    );
    engine.load_shard(1);
    assert!(engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config { version: 2, async_storage: true, ..Config::default() },
    }).ok);
    engine
}

fn execute(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest { shard_id: 1, command });
    assert!(response.status.ok, "{:?}", response.status);
}

fn dump_without_round_trip(engine: &TemporalEngine, buckets: Vec<u32>) -> BucketDumpManifest {
    let control = CHECKPOINT_DECODES.with(|count| count.get());
    decode_index_bytes(&serialize_index(&ShardState::default())).unwrap();
    assert_eq!(CHECKPOINT_DECODES.with(|count| count.get()) - control, 1,
        "the decode counter must observe a real decode before its zero is trusted");
    let before = CHECKPOINT_DECODES.with(|count| count.get());
    reset_index_encode_counts();
    let manifest = engine.create_bucket_dump_manifest(1, buckets).unwrap();
    assert_eq!(CHECKPOINT_DECODES.with(|count| count.get()) - before, 0,
        "an owned checkpoint must not be decoded to create its manifest");
    assert_eq!(index_encode_counts().encodes_total, 1,
        "the final checkpoint must be encoded once, including retirement");
    manifest
}

#[test]
fn owned_dump_capture_preserves_model_state_and_reports_during_retirement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    for index in 0..64 {
        execute(&engine, Command::StringSet {
            key: format!("capture-{index:04}"), value: vec![index as u8; 128],
        });
    }
    for index in 0..16 {
        for command in [
            Command::HashSet { key: "hash".into(), field: format!("field-{index}"), value: vec![index; 8] },
            Command::SetAdd { key: "set".into(), member: vec![index] },
            Command::ZSetAdd { key: "zset".into(), member: vec![index], score: f64::from(index) },
            Command::ListPush { key: "list".into(), member: vec![index], left: false },
            Command::FeatureAppend { key: "feature".into(), points: vec![crate::types::FeaturePoint {
                timestamp_ms: u64::from(index) + 1, value: vec![index; 8],
            }] },
        ] {
            execute(&engine, command);
        }
    }
    execute(&engine, Command::CommonExpire { key: "hash".into(), ttl_ms: 3_600_000 });
    let (snapshot, _) = engine.capture_shard_checkpoint(1).unwrap();
    let expected_bytes = serialize_index(&snapshot);
    let normalized = decode_index_bytes(&expected_bytes).unwrap();
    let expected_lifecycle = object_lifecycle_report_from_entries(
        1, &normalized, collect_live_block_entries(&normalized), &BTreeSet::new(),
        |key| engine.routing_bucket_for_key(1, key),
    );
    let manifest = dump_without_round_trip(&engine, vec![]);
    assert_eq!(manifest.object_lifecycle, expected_lifecycle);
    let state = decode_index_bytes(&manifest.index_bytes).unwrap();
    // Retirement may remove obsolete placements, but no other persisted field
    // may change and every retained placement must still name the same record.
    let mut expected_state = serde_json::to_value(&normalized).unwrap();
    let mut actual_state = serde_json::to_value(&state).unwrap();
    expected_state.as_object_mut().unwrap().remove("wal_resident_blocks");
    actual_state.as_object_mut().unwrap().remove("wal_resident_blocks");
    assert_eq!(actual_state, expected_state);
    assert!(state.wal_resident_blocks.len() <= normalized.wal_resident_blocks.len());
    for (key, placement) in &state.wal_resident_blocks {
        let expected = normalized.wal_resident_blocks.get(key).unwrap();
        assert_eq!(placement.log_id, expected.log_id);
        assert_eq!(placement.sequence, expected.sequence);
    }
    assert_eq!(manifest.index_sha256, sha256_hex_bytes(&manifest.index_bytes));
    assert_eq!(state.strings().len(), 64);
    assert_eq!(state.bucket_index.hashes.get("hash").unwrap().len(), 16);
    assert_eq!(state.bucket_index.sets.get("set").unwrap().len(), 16);
    assert_eq!(state.bucket_index.zsets.get("zset").unwrap().len(), 16);
    assert_eq!(state.bucket_index.lists.get("list").unwrap().len(), 16);
    assert_eq!(state.features().get("feature").unwrap().len(), 16);
    assert!(state.expires_at_ms().get("hash").is_some());
}

#[test]
fn owned_dump_capture_encodes_once_after_registration_retirement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    for key in ["live", "gone"] {
        execute(&engine, Command::StringSet { key: key.into(), value: vec![7; 256] });
    }
    let before = engine.registration_count_for_test(1);
    assert_eq!(before, 2);
    execute(&engine, Command::StringDelete { key: "gone".into() });
    let manifest = dump_without_round_trip(&engine, vec![]);
    assert_eq!(engine.registration_count_for_test(1), 1);
    assert_eq!(engine.wal_resident_block_count(1), 1);
    let state = decode_index_bytes(&manifest.index_bytes).unwrap();
    assert_eq!(state.index_format_version, SHARD_INDEX_FORMAT_VERSION);
    assert_eq!(state.wal_resident_blocks.len(), 1);
    assert!(state.string_address("live").is_some());
    assert!(state.string_address("gone").is_none());
    assert_eq!(manifest.index_sha256, sha256_hex_bytes(&manifest.index_bytes));
    // This image retains a WAL dependency; only the slab-backed case is portable
    // without carrying source logs.
    let status = engine.validate_bucket_dump_manifest(&manifest).unwrap_err();
    assert_eq!(status.code, "slot_dump_missing_block_segments");
    drop(engine);
    let restored = engine_at(dir.path());
    for (key, expected) in [("live", Some(vec![7; 256])), ("gone", None)] {
        let response = restored.execute(ExecuteRequest {
            shard_id: 1, command: Command::StringGet { key: key.into() },
        });
        assert!(response.status.ok);
        assert_eq!(response.response, CommandResponse::Bytes { value: expected });
    }
}

#[test]
fn owned_dump_capture_keeps_released_buckets_and_installs_without_source_logs() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    for index in 0..32 {
        execute(&engine, Command::StringSet {
            key: format!("released-{index:04}"), value: vec![index as u8; 128],
        });
    }
    assert_eq!(engine.materialize_synthetic_blocks(1), 32);
    let first = engine.create_bucket_dump_manifest(1, []).unwrap();
    engine.clear_dumped_bucket_dirty_state(1, &first);
    let released = {
        let mut shards = engine.shards.write().unwrap();
        let shard = shards.get_mut(&1).unwrap();
        let buckets = shard.bucket_index.bucket_map.keys().copied().take(8).collect::<Vec<_>>();
        let outcome = storage_bucket_internals::release_bucket_blocks(shard, &buckets);
        assert_eq!(outcome.released_buckets, buckets);
        buckets
    };
    let selected = engine.routing_bucket_for_key(1, "released-0031");
    let (snapshot, _) = engine.capture_shard_checkpoint(1).unwrap();
    let expected = serialize_index(&snapshot);
    let manifest = dump_without_round_trip(&engine, vec![selected]);
    assert_eq!(manifest.index_bytes, expected);
    assert_eq!(manifest.bucket_ids, vec![selected]);
    assert_eq!(engine.shards.read().unwrap()[&1].bucket_index.released_buckets.len(), released.len());
    engine.validate_bucket_dump_manifest(&manifest).unwrap();
    drop(engine);
    let restored = TemporalEngine::with_local_dirs(
        16 << 20, dir.path().join("fresh-cache"), dir.path().join("blocks"),
        dir.path().join("fresh-indexes"),
    );
    restored.load_shard(1);
    assert_eq!(restored.wal_store.stats_without_rescan(1).last_sequence, 0);
    restored.install_bucket_dump_manifest(&manifest).unwrap();
    for index in 0..32 {
        let result = restored.execute(ExecuteRequest {
            shard_id: 1, command: Command::StringGet { key: format!("released-{index:04}") },
        });
        assert!(result.status.ok);
        assert_eq!(result.response, CommandResponse::Bytes { value: Some(vec![index as u8; 128]) });
    }
}
