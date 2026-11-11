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
fn large_async_strings_keep_log_placement_until_portable_export() {
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
    assert_eq!(engine.block_store.stats().bytes_written, 0);
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
    assert_eq!(engine.block_store.stats().bytes_written, 0);
    drop(engine);
    let engine = engine_at(dir.path(), "cache-large-reopened");
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
    }
    assert_eq!(engine.block_store.stats().bytes_written, 0);
    assert_eq!(engine.materialize_synthetic_blocks(1), 4);
    assert!(engine.block_store.stats().bytes_written > 0);
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

#[test]
fn string_write_variants_keep_log_placement_across_the_old_size_boundary() {
    let _policy = DefaultRelocationPolicy::enter();
    const CLOCK: u64 = 4_000_000_000_000;
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path(), "variants-a");
    let _clock = ReplayClockGuard::enter(Some(CLOCK));
    let mut expected = Vec::new();
    for variant in 0..3 {
        for size in [8191usize, 8192, 8193, 128 * 1024] {
            let key = format!("variant-{variant}-{size}");
            let value: Vec<u8> = (0..size)
                .map(|i| (i.wrapping_mul(17) % 251) as u8)
                .collect();
            let command = match variant {
                0 => Command::StringSet {
                    key: key.clone(),
                    value: value.clone(),
                },
                1 => Command::StringSetEx {
                    key: key.clone(),
                    value: value.clone(),
                    ttl_ms: 60_000,
                },
                _ => Command::StringSetConditional {
                    key: key.clone(),
                    value: value.clone(),
                    ttl_ms: Some(60_000),
                    condition: crate::types::StringSetCondition::IfNotExists,
                    return_old: false,
                    keep_ttl: false,
                },
            };
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            assert!(response.status.ok, "{:?}", response.status);
            let address = engine.string_block_address(1, &key).unwrap();
            assert!(crate::wal_record::is_wal_resident(address.block_slab_id()));
            assert_eq!(address.length() as usize, size);
            expected.push((variant, key, value));
        }
    }
    assert_eq!(engine.block_store.stats().bytes_written, 0);
    assert!(engine.cache.stats().memory_bytes <= 64 * 1024);
    engine.create_bucket_dump_manifest(1, []).unwrap();
    assert_eq!(engine.block_store.stats().bytes_written, 0);
    drop(engine);

    let restored = engine_at(dir.path(), "variants-b");
    let _restored_clock = ReplayClockGuard::enter(Some(CLOCK));
    restored.cache.invalidate_shard(1).unwrap();
    for (variant, key, value) in &expected {
        let response = restored.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet { key: key.clone() },
        });
        assert!(response.status.ok);
        assert_eq!(
            response.response,
            CommandResponse::Bytes {
                value: Some(value.clone())
            }
        );
        if *variant != 0 {
            let response = restored.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonTtl { key: key.clone() },
            });
            assert_eq!(
                response.response,
                CommandResponse::Integer { value: 60_000 }
            );
        }
    }
    assert_eq!(restored.block_store.stats().bytes_written, 0);
    assert!(restored.cache.stats().memory_bytes <= 64 * 1024);

    let _expired_clock = ReplayClockGuard::enter(Some(CLOCK + 60_000));
    for (variant, key, _) in expected {
        let response = restored.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonExists { key },
        });
        assert_eq!(
            response.response,
            CommandResponse::Integer {
                value: i64::from(variant == 0)
            }
        );
    }
}

#[test]
fn other_append_paths_keep_their_existing_large_frame_placement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path(), "placement-controls");
    let bytes = vec![b'v'; 8192];
    block_in_wal::take_staged();
    for object_id in [None, Some(42)] {
        let address = append_value(
            &engine.cache,
            &engine.block_store,
            1,
            &bytes,
            object_id,
            Some("field"),
            Some(0),
            true,
        )
        .unwrap();
        assert!(!crate::wal_record::is_wal_resident(address.block_slab_id()));
    }
    let address = append_owned_value_of_object(
        &engine.cache,
        &engine.block_store,
        1,
        bytes.clone(),
        43,
        0,
        true,
        || 1,
    )
    .unwrap();
    assert!(!crate::wal_record::is_wal_resident(address.block_slab_id()));
    let address =
        append_string_value(&engine.cache, &engine.block_store, 1, &bytes, 44, 0, false).unwrap();
    assert!(!crate::wal_record::is_wal_resident(address.block_slab_id()));
    block_in_wal::take_staged();
}
