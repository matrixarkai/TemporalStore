// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::engine::*;

fn seed() -> (tempfile::TempDir, TemporalEngine) {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        4096,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for i in 0..12 {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("row:{i:04}"),
                value: vec![i as u8; 128],
            },
        });
        assert!(response.status.ok, "{response:?}");
    }
    (dir, engine)
}

#[test]
fn checkpoint_rows_preserve_metadata_addresses_and_filed_ownership() {
    let (_dir, engine) = seed();
    let mut state = engine.shards.read().unwrap()[&1].clone();
    for slot in state.strings_mut().values_mut() {
        slot.record_mut()
            .set_flag(crate::engine::object_record::RecordFlags::INDEX_DIRTY, true);
        slot.record_mut()
            .set_flag(crate::engine::object_record::RecordFlags::IN_MEMORY, true);
        slot.record_mut().set_model_id(7);
        slot.record_mut().set_logical_size(129);
    }
    let before = serde_json::to_value(state.strings()).unwrap();
    let wire = serde_json::to_value(&state).unwrap();
    assert_eq!(
        wire["slot_index"]["objects"]["string"]
            .as_object()
            .unwrap()
            .len(),
        0
    );
    let embedded: usize = wire["slot_index"]["bucket_map"]
        .as_object()
        .unwrap()
        .values()
        .map(|bucket| {
            bucket["block_index"]
                .as_object()
                .unwrap()
                .values()
                .filter(|block| block.get("r").is_some())
                .count()
        })
        .sum();
    assert_eq!(embedded, 12);
    let decoded =
        crate::engine::decode_index_bytes(&crate::engine::encode_index_bytes(&state)).unwrap();
    assert_eq!(serde_json::to_value(decoded.strings()).unwrap(), before);
    let json_decoded: crate::engine::state::ShardState = serde_json::from_value(wire).unwrap();
    assert_eq!(
        serde_json::to_value(json_decoded.strings()).unwrap(),
        before
    );
    for key in state.strings().keys() {
        assert_eq!(
            decoded
                .bucket_index
                .object_block_refs("string", key)
                .unwrap()
                .len(),
            1
        );
        assert!(decoded
            .bucket_index
            .object_block_lookup
            .get("string", key)
            .is_none());
    }
}

#[test]
fn checkpoint_rows_keep_separate_rows_without_resident_matching_blocks() {
    let (_dir, engine) = seed();
    let mut state = engine.shards.read().unwrap()[&1].clone();
    let before = serde_json::to_value(state.strings()).unwrap();
    state.bucket_index.bucket_map.clear();
    let wire = serde_json::to_value(&state).unwrap();
    assert_eq!(wire["slot_index"]["objects"]["string"], before);
    let restored =
        crate::engine::decode_index_bytes(&crate::engine::encode_index_bytes(&state)).unwrap();
    assert_eq!(serde_json::to_value(restored.strings()).unwrap(), before);
}

#[test]
fn checkpoint_rows_reject_duplicate_metadata_and_conflicting_addresses() {
    let (_dir, engine) = seed();
    let state = engine.shards.read().unwrap()[&1].clone();
    let original = serde_json::to_value(&state).unwrap();
    let mut duplicate = original.clone();
    duplicate["slot_index"]["objects"]["string"] = serde_json::to_value(state.strings()).unwrap();
    let error = serde_json::from_value::<crate::engine::state::ShardState>(duplicate).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("duplicate embedded object record"),
        "{error}"
    );
    let mut conflict = original;
    let block = conflict["slot_index"]["bucket_map"]
        .as_object_mut()
        .unwrap()
        .values_mut()
        .flat_map(|bucket| bucket["block_index"].as_object_mut().unwrap().values_mut())
        .find(|block| block.get("r").is_some())
        .unwrap();
    block["r"]["a"] = serde_json::json!(1);
    let error = serde_json::from_value::<crate::engine::state::ShardState>(conflict).unwrap_err();
    assert!(error.to_string().contains("second address"), "{error}");
}

#[test]
fn checkpoint_rows_keep_an_ambiguous_owner_separate() {
    let (_dir, engine) = seed();
    let mut state = engine.shards.read().unwrap()[&1].clone();
    let owner = state.strings()["row:0000"].clone();
    state.strings_mut().insert("alias:owner".into(), owner);
    let before = serde_json::to_value(state.strings()).unwrap();
    let wire = serde_json::to_value(&state).unwrap();
    let separate = wire["slot_index"]["objects"]["string"].as_object().unwrap();
    assert!(separate.contains_key("alias:owner") || separate.contains_key("row:0000"));
    let restored =
        crate::engine::decode_index_bytes(&crate::engine::encode_index_bytes(&state)).unwrap();
    assert_eq!(serde_json::to_value(restored.strings()).unwrap(), before);
}

#[test]
fn checkpoint_rows_stage_only_fixed_record_metadata() {
    assert_eq!(
        std::mem::size_of::<(std::sync::Arc<str>, ObjectRecord)>(),
        32
    );
    assert!(std::mem::size_of::<ObjectRecord>() < std::mem::size_of::<ObjectSlot>());
}

fn dense_engine(root: &std::path::Path, cache: &str) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs_block_store_options_and_disk_cache(
        64 * 1024,
        root.join(cache),
        root.join("blocks"),
        root.join("indexes"),
        crate::block_store::BlockStoreOptions::default(),
        false,
    );
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: String::new(),
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: None,
    });
    assert!(response.status.ok, "{:?}", response.status);
    engine
}

fn dense_value(index: u64) -> Vec<u8> {
    [index.to_le_bytes(), (!index).to_le_bytes()].concat()
}

fn dense_restore(async_storage: bool, keys: u64) {
    assert!(keys > 0);
    let dir = tempfile::tempdir().unwrap();
    let source = dense_engine(dir.path(), "source-cache");
    assert!(
        source
            .set_config(crate::control::SetConfigRequest {
                shard_id: 1,
                config: crate::control::Config {
                    version: 2,
                    async_storage,
                    ..Default::default()
                },
            })
            .ok
    );
    let started = std::time::Instant::now();
    for index in 0..keys {
        let response = source.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("dense:{index:08}"),
                value: dense_value(index),
            },
        });
        assert!(response.status.ok, "{response:?}");
    }
    let (expected_class, buckets) = {
        let shards = source.shards.read().unwrap();
        let state = &shards[&1];
        assert_eq!(state.strings().len(), keys as usize);
        let buckets = state.bucket_index.bucket_map.len();
        assert!((1..=1024).contains(&buckets));
        if keys >= 100_000 {
            assert_eq!(buckets, 1024);
        }
        (state.strings()["dense:00000000"].logical_size(), buckets)
    };
    let manifest = source.create_bucket_dump_manifest(1, []).unwrap();
    assert_eq!(
        manifest.wal_sequence,
        source.wal_store.stats(1).last_sequence
    );
    {
        let image = crate::engine::decode_index_bytes(&manifest.index_bytes).unwrap();
        assert_eq!(image.strings().len(), keys as usize);
        assert_eq!(image.bucket_index.bucket_map.len(), buckets);
        for (key, row) in image.strings() {
            assert_eq!(row.logical_size(), expected_class);
            assert_eq!(
                image
                    .bucket_index
                    .object_block_refs("string", key)
                    .unwrap()
                    .len(),
                1
            );
        }
    }
    source.flush_shard_index(1);
    source.wal_store.flush(1).unwrap();
    drop(source);
    crate::engine::storage_bucket_internals::reset_view_rebuild_block_failure_counts();
    let restored = dense_engine(dir.path(), "restored-cache");
    assert_eq!(
        crate::engine::storage_bucket_internals::view_rebuild_block_failure_counts(),
        (0, 0)
    );
    assert_eq!(
        crate::engine::lifecycle::LAST_REPLAY_WATERMARK.load(std::sync::atomic::Ordering::SeqCst),
        manifest.wal_sequence
    );
    {
        let shards = restored.shards.read().unwrap();
        assert_eq!(shards[&1].strings().len(), keys as usize);
        for row in shards[&1].strings().values() {
            assert_eq!(row.logical_size(), expected_class);
        }
    }
    restored.cache().invalidate_shard(1).unwrap();
    for index in 0..keys {
        let response = restored.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: format!("dense:{index:08}"),
            },
        });
        assert!(response.status.ok, "{response:?}");
        assert_eq!(
            response.response,
            CommandResponse::Bytes {
                value: Some(dense_value(index))
            }
        );
    }
    eprintln!("CHECKPOINT_ROWS_DENSE keys={keys} buckets={buckets} async={async_storage} verified={keys} image_bytes={} size_class={expected_class:?} elapsed_ms={}", manifest.index_bytes.len(), started.elapsed().as_millis());
}

#[test]
fn checkpoint_rows_dense_small_restores_both_placement_modes() {
    dense_restore(false, 2048);
    dense_restore(true, 2048);
}

#[test]
#[ignore = "large dense synchronous checkpoint and cold-read validation"]
fn checkpoint_rows_large_sync_dense_restore() {
    dense_restore(false, 100_000);
}

#[test]
#[ignore = "large dense asynchronous checkpoint and cold-read validation"]
fn checkpoint_rows_large_async_dense_restore() {
    dense_restore(true, 100_000);
}

#[test]
#[ignore = "requires a captured preceding-layout MessagePack payload"]
fn checkpoint_rows_read_captured_preceding_layout() {
    let path = std::env::var_os("TS_CHECKPOINT_PAYLOAD").expect("TS_CHECKPOINT_PAYLOAD");
    let raw = std::fs::read(path).unwrap();
    let original: crate::engine::state::ShardState = rmp_serde::from_slice(&raw).unwrap();
    assert!(original.strings().len() >= 8000);
    let expected = serde_json::to_value(original.strings()).unwrap();
    let restored =
        crate::engine::decode_index_bytes(&crate::engine::encode_index_bytes(&original)).unwrap();
    assert_eq!(serde_json::to_value(restored.strings()).unwrap(), expected);
    for key in original.strings().keys() {
        assert_eq!(
            restored
                .bucket_index
                .object_block_refs("string", key)
                .unwrap()
                .len(),
            1
        );
    }
}
