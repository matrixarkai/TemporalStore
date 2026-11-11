// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

struct DefaultRelocationPolicy(Vec<(&'static str, Option<std::ffi::OsString>)>);

impl DefaultRelocationPolicy {
    fn enter() -> Self {
        let saved = ["TS_WAL_RESIDENT_BLOCKS", "TS_WAL_RESIDENT_PAGES"]
            .into_iter()
            .map(|key| {
                let value = std::env::var_os(key);
                std::env::remove_var(key);
                (key, value)
            })
            .collect();
        Self(saved)
    }
}

impl Drop for DefaultRelocationPolicy {
    fn drop(&mut self) {
        for (key, value) in &self.0 {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
}

fn engine_at(root: &std::path::Path, cache: &str) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs_block_store_options_and_disk_cache(
        64 * 1024,
        root.join(cache),
        root.join("blocks"),
        root.join("indexes"),
        crate::block_store::BlockStoreOptions::default(),
        false,
    );
    engine.load_shard(1);
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage: true,
                    ..Config::default()
                },
            })
            .ok
    );
    engine
}

fn value(index: usize) -> Vec<u8> {
    let mut bytes = vec![b'v'; 1024];
    bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
    bytes
}

fn put(engine: &TemporalEngine, key: usize, version: usize) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: format!("key-{key:05}"),
            value: value(version),
        },
    });
    assert!(response.status.ok, "{:?}", response.status);
}

fn get(engine: &TemporalEngine, key: usize, version: usize) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: format!("key-{key:05}"),
        },
    });
    assert!(response.status.ok, "{:?}", response.status);
    assert_eq!(
        response.response,
        CommandResponse::Bytes {
            value: Some(value(version))
        }
    );
}

#[test]
fn default_string_writes_bound_cache_without_slab_materialization() {
    let _policy = DefaultRelocationPolicy::enter();
    const KEYS: usize = 8_192;
    const CACHE_BYTES: u64 = 64 * 1024;
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path(), "cache-a");
    let slab_bytes = engine.block_store.stats().bytes_written;
    for index in 0..KEYS {
        put(&engine, index, index);
    }
    assert_eq!(
        engine.block_store.stats().bytes_written,
        slab_bytes,
        "needed log records must not gain duplicate slab copies because the locator index grew"
    );
    assert_eq!(engine.registration_count_for_test(1), KEYS);
    let stats = engine.cache.stats();
    assert!(stats.memory_bytes > 0);
    assert!(stats.memory_bytes <= CACHE_BYTES, "{:?}", stats);
    assert!(KEYS * value(0).len() > CACHE_BYTES as usize * 100);
    engine.cache.invalidate_shard(1).unwrap();
    assert_eq!(engine.cache.stats().memory_bytes, 0);
    for index in 0..KEYS {
        get(&engine, index, index);
    }
    assert!(engine.cache.stats().memory_bytes <= CACHE_BYTES);
    let (records, bytes) = block_in_wal::decoded_record_cache_shape_for_test();
    assert!(records > 0 && records <= 8);
    assert!(
        bytes > 0 && bytes <= 8 * 2048,
        "the decoded-record cache must not grow with the 8,192-key corpus"
    );
    let dump = engine.create_bucket_dump_manifest(1, []).unwrap();
    assert_eq!(engine.block_store.stats().bytes_written, slab_bytes);
    assert_eq!(dump.wal_sequence, KEYS as u64);
    let plan = engine.storage_wal_reclaim_plan(1, [], []);
    engine.apply_storage_wal_reclaim(plan);
    drop(engine);
    let restored = engine_at(dir.path(), "cache-b");
    for index in 0..KEYS {
        get(&restored, index, index);
    }
    assert_eq!(restored.block_store.stats().bytes_written, slab_bytes);
    assert!(restored.cache.stats().memory_bytes <= CACHE_BYTES);
}

#[test]
fn large_async_strings_keep_physical_placement_and_portable_restore() {
    let _policy = DefaultRelocationPolicy::enter();
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path(), "cache-large");
    let mut expected = vec![b'v'; 2 * 1024 * 1024];
    for index in 0..4u8 {
        expected[0] = index;
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: format!("large-{index}"),
                        value: expected.clone(),
                    },
                })
                .status
                .ok
        );
        assert!(engine.cache.stats().memory_bytes <= 64 * 1024);
    }
    let slab_bytes = engine.block_store.stats().bytes_written;
    assert!(slab_bytes > 0);
    assert_eq!(engine.registration_count_for_test(1), 4);
    engine.cache.invalidate_shard(1).unwrap();
    for index in 0..4u8 {
        expected[0] = index;
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: format!("large-{index}"),
            },
        });
        assert!(response.status.ok);
        assert_eq!(
            response.response,
            CommandResponse::Bytes {
                value: Some(expected.clone())
            }
        );
        assert!(engine.cache.stats().memory_bytes <= 64 * 1024);
    }
    assert_eq!(engine.block_store.stats().bytes_written, slab_bytes);
    assert_eq!(engine.materialize_synthetic_blocks(1), 0);
    let portable = engine.create_bucket_dump_manifest(1, []).unwrap();
    drop(engine);
    let target = TemporalEngine::with_local_dirs_block_store_options_and_disk_cache(
        64 * 1024,
        dir.path().join("portable-cache"),
        dir.path().join("blocks"),
        dir.path().join("portable-indexes"),
        crate::block_store::BlockStoreOptions::default(),
        false,
    );
    target.load_shard(1);
    assert_eq!(
        target
            .write_ahead_log_store()
            .stats_without_rescan(1)
            .last_sequence,
        0
    );
    target.install_bucket_dump_manifest(&portable).unwrap();
    for index in 0..4u8 {
        expected[0] = index;
        let response = target.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: format!("large-{index}"),
            },
        });
        assert!(response.status.ok);
        assert_eq!(
            response.response,
            CommandResponse::Bytes {
                value: Some(expected.clone())
            }
        );
    }
}

#[test]
fn overwrites_replace_locators_and_dump_retires_deleted_objects() {
    let _policy = DefaultRelocationPolicy::enter();
    const KEYS: usize = 64;
    const WRITES: usize = 8_192;
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path(), "cache");
    for version in 0..WRITES {
        put(&engine, version % KEYS, version);
    }
    assert_eq!(
        engine.registration_count_for_test(1),
        KEYS,
        "locator memory must follow live identities, not append history"
    );
    engine.cache.invalidate_shard(1).unwrap();
    for key in 0..KEYS {
        get(&engine, key, WRITES - KEYS + key);
    }
    for key in 1..KEYS {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringDelete {
                        key: format!("key-{key:05}")
                    },
                })
                .status
                .ok
        );
    }
    engine.create_bucket_dump_manifest(1, []).unwrap();
    assert_eq!(engine.registration_count_for_test(1), 1);
    assert_eq!(engine.wal_resident_block_count(1), 1);
    let reclaim = engine.apply_storage_wal_reclaim(engine.storage_wal_reclaim_plan(1, [], []));
    assert!(
        reclaim.wal_records_removed > 0,
        "obsolete history must remain reclaimable without count-driven relocation: {reclaim:?}"
    );
    engine.cache.invalidate_shard(1).unwrap();
    get(&engine, 0, WRITES - KEYS);
    assert_eq!(engine.block_store.stats().bytes_written, 0);
}
