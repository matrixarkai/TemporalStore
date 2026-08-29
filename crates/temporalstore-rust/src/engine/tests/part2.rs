// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Part 2 of engine tests, split from engine/tests.rs.
#![allow(clippy::all)]
use super::*;

#[test]
fn memory_miss_reads_local_page_file_using_index_address() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    let cache = engine.cache();
    let block_store = engine.block_store();
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert_eq!(block_store.stats().writes, 1);

    let first = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        first.response,
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );
    assert_eq!(block_store.stats().reads, 1);

    let second = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        second.response,
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );
    assert_eq!(block_store.stats().reads, 1);
    assert_eq!(cache.stats().memory_hits, 1);

    cache.clear_memory_for_test();
    let third = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        third.response,
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );
    assert_eq!(block_store.stats().reads, 1);
    assert_eq!(cache.stats().disk_hits, 1);
}

#[test]
fn three_layer_cache_reads_memory_then_block_cache_then_local_file() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    let cache = engine.cache();
    let block_store = engine.block_store();
    engine.load_shard(1);

    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });

    let first = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        first.response,
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );
    assert_eq!(block_store.stats().reads, 1);
    assert_eq!(cache.stats().puts, 2);
    assert!(cache.stats().memory_bytes > 0);
    assert!(cache.stats().disk_bytes > 0);

    let memory = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        memory.response,
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );
    assert!(cache.stats().memory_hits >= 1);
    assert_eq!(block_store.stats().reads, 1);

    cache.clear_memory_for_test();
    let block_cache = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        block_cache.response,
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );
    assert_eq!(cache.stats().disk_hits, 1);
    assert_eq!(block_store.stats().reads, 1);

    cache.invalidate_shard(1).unwrap();
    let local_file = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        local_file.response,
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );
    assert_eq!(block_store.stats().reads, 2);
    assert!(cache.stats().puts >= 4);
    assert!(cache.stats().memory_bytes > 0);
    assert!(cache.stats().disk_bytes > 0);

    let observation = engine.rust_storage_observation(1).unwrap();
    assert!(observation.observed_memory_hit);
    assert!(observation.observed_block_cache_hit);
    assert!(observation.observed_local_file_read);
    assert!(observation.observed_cache_invalidation);
    assert!(observation.cache_memory_bytes > 0);
    assert!(observation.cache_disk_bytes > 0);
    assert!(observation.local_page_bytes_written > 0);
    assert!(observation.local_page_bytes_read > 0);
}

#[test]
fn tiny_memory_cache_eviction_refills_from_persistence_then_block_cache() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        32,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    let cache = engine.cache();
    let block_store = engine.block_store();
    engine.load_shard(1);

    let target_value = b"target-value-0123456789".to_vec();
    for (key, value) in [
        ("target", target_value.clone()),
        ("evict-a", b"eviction-value-a-0123456789".to_vec()),
        ("evict-b", b"eviction-value-b-0123456789".to_vec()),
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.to_string(),
                value,
            },
        });
        assert!(response.status.ok, "{response:?}");
    }
    let first_read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "target".to_string(),
        },
    });
    assert_eq!(
        first_read.response,
        CommandResponse::Bytes {
            value: Some(target_value.clone())
        }
    );
    assert_eq!(block_store.stats().reads, 1);

    for key in ["evict-a", "evict-b"] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: key.to_string(),
            },
        });
        assert!(
            response.status.ok,
            "eviction pressure read should pass: {response:?}"
        );
    }
    assert!(
        cache.stats().memory_evictions > 0,
        "reading multiple persisted blocks through a tiny memory cache should evict older blocks"
    );
    assert!(
        cache.stats().disk_bytes > 0,
        "persistent page read should populate block-cache files"
    );

    let target_page_key = {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let address = shards
            .get(&1)
            .expect("shard should exist")
            .strings
            .get("target")
            .expect("target address should exist");
        CacheKey::page_with_slot(
            1,
            address.page_slab_id,
            address.offset,
            address.length,
            address.routing_bucket,
        )
    };
    assert_eq!(
        cache.get_memory(&target_page_key),
        None,
        "target page block should have been evicted from memory"
    );

    let disk_hits_before = cache.stats().disk_hits;
    let file_reads_before_block_hit = block_store.stats().reads;
    let second_read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "target".to_string(),
        },
    });
    assert_eq!(
        second_read.response,
        CommandResponse::Bytes {
            value: Some(target_value.clone())
        }
    );
    assert_eq!(
        block_store.stats().reads,
        file_reads_before_block_hit,
        "memory miss should hit disk block cache instead of rereading block store"
    );
    assert!(
        cache.stats().disk_hits > disk_hits_before,
        "block cache should serve the read and promote it to memory"
    );
    assert_eq!(
        cache.get_memory(&target_page_key),
        Some(target_value),
        "disk block hit should promote the page block into memory"
    );
}

#[test]
// shared-corpus: storage_cache_replacement_policy_soak;
fn cache_replacement_policy_soak() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        32,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let target_value = b"target-value-0123456789".to_vec();
    for round in 0..4 {
        for item in 0..8 {
            let key = format!("soak-{round}-{item}");
            let value = format!("soak-value-{round}-{item}").into_bytes();
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet { key, value },
            });
            assert!(response.status.ok, "{response:?}");
        }
    }
    let target_write = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "soak-target".to_string(),
            value: target_value.clone(),
        },
    });
    assert!(target_write.status.ok, "{target_write:?}");

    for round in 0..4 {
        for item in 0..8 {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: format!("soak-{round}-{item}"),
                },
            });
            assert!(response.status.ok, "{response:?}");
        }
    }
    let warm_target = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "soak-target".to_string(),
        },
    });
    assert_eq!(
        warm_target.response,
        CommandResponse::Bytes {
            value: Some(target_value.clone())
        }
    );

    assert!(
        engine.cache().stats().memory_evictions > 0,
        "tiny memory cache should perform replacement during the pressure pass"
    );
    assert!(
        engine.cache().stats().disk_bytes > 0,
        "pressure pass should leave a disk-cache tier for cold read refill"
    );

    let target_page_key = {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let address = shards
            .get(&1)
            .expect("loaded shard")
            .strings
            .get("soak-target")
            .expect("target page address")
            .clone();
        CacheKey::page_with_slot(
            1,
            address.page_slab_id,
            address.offset,
            address.length,
            address.routing_bucket,
        )
    };

    let evict_report = engine.apply_storage_eviction(1, 1, 4, true, false);
    assert_eq!(evict_report.mode, "evict_cache");
    assert!(evict_report.pressure_gate_open, "{evict_report:?}");
    assert!(evict_report.pressure_before > 0, "{evict_report:?}");
    assert!(
        !evict_report.selected_victims.is_empty(),
        "{evict_report:?}"
    );
    assert!(
        !evict_report.dump_manifest_ids.is_empty(),
        "dump-before-evict should durably dump dirty victim slots: {evict_report:?}"
    );
    assert!(
        evict_report.cache_entries_removed > 0 || evict_report.cache_disk_bytes_removed > 0,
        "eviction should remove at least one cache entry or disk-cache block: {evict_report:?}"
    );

    let _ = engine.cache().invalidate(&target_page_key);
    engine.cache().clear_memory_for_test();
    assert_eq!(engine.cache().get_memory(&target_page_key), None);
    let block_reads_before = engine.block_store().stats().reads;
    let disk_hits_before = engine.cache().stats().disk_hits;
    let cold_target = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "soak-target".to_string(),
        },
    });
    assert_eq!(
        cold_target.response,
        CommandResponse::Bytes {
            value: Some(target_value.clone())
        }
    );
    assert_eq!(
        engine.cache().get_memory(&target_page_key),
        Some(target_value.clone()),
        "cold read should refill memory from disk cache or page store"
    );
    assert!(
        engine.cache().stats().disk_hits > disk_hits_before
            || engine.block_store().stats().reads > block_reads_before,
        "cold read should be backed by disk cache or persistent page store"
    );
    engine.cache().clear_memory_for_test();
    assert_eq!(engine.cache().get_memory(&target_page_key), None);
    let refill_samples_before = engine.cache().stats().refill_latency_samples;
    let disk_refill_target = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "soak-target".to_string(),
        },
    });
    assert_eq!(
        disk_refill_target.response,
        CommandResponse::Bytes {
            value: Some(target_value.clone())
        }
    );
    assert!(
        engine.cache().stats().refill_latency_samples > refill_samples_before,
        "second cold read should hit disk cache and record refill latency"
    );
    engine.cache().set_async_writeback_queue_limit_for_test(1);
    engine
        .cache()
        .enqueue_async_writeback(
            CacheKey::page_with_slot(1, 50_000, 0, 8, Some(99)),
            b"writeback".to_vec(),
        )
        .unwrap();
    assert!(engine
        .cache()
        .enqueue_async_writeback(
            CacheKey::page_with_slot(1, 50_001, 0, 8, Some(99)),
            b"overflow".to_vec(),
        )
        .is_err());
    let drained = engine.cache().drain_async_writeback(8).unwrap();
    assert_eq!(drained.drained, 1);
    engine.cache().record_compaction_latency_micros(750);
    let stats = engine.cache().stats();
    assert!(stats.async_writeback_backpressure_rejections > 0);
    assert!(stats.async_writeback_max_queue_depth > 0);
    assert!(stats.async_writeback_max_queue_bytes > 0);
    assert_cache_latency_histograms_observed(stats);

    let drop_dir = tempfile::tempdir().unwrap();
    let drop_engine = TemporalEngine::with_local_dirs(
        64,
        drop_dir.path().join("cache"),
        drop_dir.path().join("pages"),
        drop_dir.path().join("indexes"),
    );
    drop_engine.load_shard(7);
    for item in 0..6 {
        let key = format!("drop-{item}");
        let response = drop_engine.execute(ExecuteRequest {
            shard_id: 7,
            command: Command::StringSet {
                key: key.clone(),
                value: format!("drop-value-{item}").repeat(10).into_bytes(),
            },
        });
        assert!(response.status.ok, "{response:?}");
        assert!(
            drop_engine
                .execute(ExecuteRequest {
                    shard_id: 7,
                    command: Command::StringGet { key },
                })
                .status
                .ok
        );
    }
    let drop_report = drop_engine.apply_storage_eviction(7, 1, 2, true, true);
    assert_eq!(drop_report.mode, "delete_drop");
    assert!(drop_report.pressure_gate_open, "{drop_report:?}");
    assert!(!drop_report.selected_victims.is_empty(), "{drop_report:?}");
    assert!(
        drop_report.dropped_object_count > 0,
        "delete/drop eviction mode should drop selected cold objects: {drop_report:?}"
    );
}

#[test]
// shared-corpus: storage_cache_replacement_policy_soak;
fn cache_dram_pmem_ssd_tiers_admit_refill_and_evict() {
    let dir = tempfile::tempdir().unwrap();
    let cache = matrixcache::MultiLayerCache::with_tiering_policy(
        dir.path(),
        matrixcache::CacheTieringPolicy {
            memory_capacity_bytes: 16,
            pmem_capacity_bytes: 24,
            ssd_capacity_bytes: 4096,
            data_placement: matrixcache::CacheDataPlacement::Tiered,
            data_placement_threshold_bytes: 0,
            memory_hotness_threshold: 10,
            pmem_admit_hotness_threshold: 5,
            ssd_admit_hotness_threshold: 2,
            max_memory_block_bytes: 32,
            max_pmem_block_bytes: 32,
            max_ssd_block_bytes: 128,
            ssd_write_through: true,
        },
        matrixcache::CacheBlockOptions::default(),
    );
    let key = CacheKey::page_with_slot(9, 1, 0, 16, Some(11));
    let request = matrixcache::CacheAdmissionRequest {
        block_kind: matrixcache::CacheBlockKind::Page,
        shard_id: 9,
        routing_slot: Some(11),
        block_bytes: 16,
        hotness: 6,
        pinned: false,
    };

    cache
        .put_with_admission(key.clone(), b"pmem-tier-value!".to_vec(), request)
        .unwrap();
    let stats = cache.stats();
    assert_eq!(stats.memory_bytes, 0, "PMEM admission should not warm DRAM");
    assert!(
        stats.pmem_bytes > 0,
        "PMEM tier should hold the admitted block"
    );
    assert!(
        stats.disk_bytes > 0,
        "SSD write-through should persist the block"
    );
    assert_eq!(stats.pmem_admission_accepted, 1);
    assert_eq!(stats.ssd_admission_accepted, 1);

    assert_eq!(
        cache.get(&key).unwrap(),
        Some(b"pmem-tier-value!".to_vec()),
        "PMEM hit should serve before SSD and promote to DRAM"
    );
    let stats = cache.stats();
    assert!(stats.pmem_hits > 0);
    assert!(stats.memory_bytes > 0);
    assert!(stats.refill_latency_samples > 0);

    for idx in 0..4 {
        let cold_key = CacheKey::page_with_slot(9, 2 + idx, 0, 16, Some(11));
        cache
            .put_with_admission(
                cold_key,
                vec![b'a' + idx as u8; 16],
                matrixcache::CacheAdmissionRequest {
                    block_kind: matrixcache::CacheBlockKind::Page,
                    shard_id: 9,
                    routing_slot: Some(11),
                    block_bytes: 16,
                    hotness: 6,
                    pinned: false,
                },
            )
            .unwrap();
    }
    let eviction = cache.eviction_report();
    assert!(
        eviction.pmem_evictions > 0,
        "PMEM capacity pressure should evict through the middle tier: {eviction:?}"
    );
    assert!(
        cache.stats().disk_bytes > 0,
        "SSD tier should retain refill copies after PMEM eviction"
    );
}

#[test]
fn restarted_engine_refills_tiny_memory_cache_from_persistent_block_cache() {
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let original =
        TemporalEngine::with_local_dirs(32, dir.path().join("cache-a"), &page_dir, &index_dir);
    original.load_shard(1);
    let target_value = b"restart-target-value-0123456789".to_vec();
    let write = original.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "target".to_string(),
            value: target_value.clone(),
        },
    });
    assert!(write.status.ok, "{write:?}");
    assert_eq!(original.block_store().stats().writes, 1);

    let restarted =
        TemporalEngine::with_local_dirs(32, dir.path().join("cache-b"), &page_dir, &index_dir);
    restarted.load_shard(1);
    let restarted_cache = restarted.cache();
    let restarted_block_store = restarted.block_store();
    let target_page_key = {
        let shards = restarted.shards.read().expect("shards lock poisoned");
        let address = shards
            .get(&1)
            .expect("shard should exist after index replay")
            .strings
            .get("target")
            .expect("target address should be restored from index")
            .clone();
        CacheKey::page_with_slot(
            1,
            address.page_slab_id,
            address.offset,
            address.length,
            address.routing_bucket,
        )
    };

    let first_read = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "target".to_string(),
        },
    });
    assert_eq!(
        first_read.response,
        CommandResponse::Bytes {
            value: Some(target_value.clone())
        }
    );
    assert_eq!(
        restarted_block_store.stats().reads,
        1,
        "restart should miss memory and load the persisted page once"
    );
    assert_eq!(
        restarted_cache.get_memory(&target_page_key),
        Some(target_value.clone()),
        "persistent page read should refill the memory cache"
    );
    assert!(
        restarted_cache.stats().disk_bytes > 0,
        "persistent page read should also write the disk block cache"
    );

    restarted_cache.clear_memory_for_test();
    assert_eq!(restarted_cache.get_memory(&target_page_key), None);
    let disk_hits_before = restarted_cache.stats().disk_hits;
    let page_reads_before = restarted_block_store.stats().reads;
    let second_read = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "target".to_string(),
        },
    });
    assert_eq!(
        second_read.response,
        CommandResponse::Bytes {
            value: Some(target_value.clone())
        }
    );
    assert_eq!(
        restarted_block_store.stats().reads,
        page_reads_before,
        "memory miss after restart should use the disk block cache"
    );
    assert!(
        restarted_cache.stats().disk_hits > disk_hits_before,
        "disk block cache should serve the second read"
    );
    assert_eq!(
        restarted_cache.get_memory(&target_page_key),
        Some(target_value),
        "disk block hit should promote the page block back into memory"
    );
}

#[test]
fn page_reads_fill_compressed_block_cache() {
    let dir = tempfile::tempdir().unwrap();
    let cache = MultiLayerCache::with_block_options(
        1024 * 1024,
        dir.path().join("cache"),
        matrixcache::CacheBlockOptions {
            compression: matrixcache::CacheCompression::Zstd { level: 1 },
            min_compress_bytes: 16,
        },
    );
    let engine = TemporalEngine::with_cache_block_store_and_index_dir(
        cache.clone(),
        LocalBlockStore::new(dir.path().join("pages")),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let value = vec![b'x'; 4096];
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "large".to_string(),
            value: value.clone(),
        },
    });

    let first = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "large".to_string(),
        },
    });
    assert_eq!(
        first.response,
        CommandResponse::Bytes { value: Some(value) }
    );
    assert!(cache.stats().compressed_puts >= 1);
    assert!(cache.stats().compression_bytes_saved > 0);

    cache.clear_memory_for_test();
    let _ = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "large".to_string(),
        },
    });
    assert!(cache.stats().compressed_hits >= 1);
}

#[test]
fn local_dirs_constructor_applies_block_store_compression_options() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs_and_block_store_options(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
        BlockStoreOptions {
            compression_enabled: false,
            ..BlockStoreOptions::default()
        },
    );
    engine.load_shard(1);
    let value = b"engine-page-policy-".repeat(80);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "large-policy".to_string(),
            value: value.clone(),
        },
    });

    let block_store = engine.block_store();
    let stats = block_store.stats();
    assert_eq!(stats.writes, 1);
    assert_eq!(stats.compressed_records_written, 0);
    assert_eq!(stats.compression_bytes_saved, 0);

    let read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "large-policy".to_string(),
        },
    });
    assert_eq!(read.response, CommandResponse::Bytes { value: Some(value) });
}

#[test]
fn write_invalidates_cached_string() {
    let dir = tempfile::tempdir().unwrap();
    let cache = MultiLayerCache::new(1024, dir.path());
    let engine = TemporalEngine::new(cache.clone());
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"old".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"new".to_vec(),
        },
    });
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        response.response,
        CommandResponse::Bytes {
            value: Some(b"new".to_vec())
        }
    );
    assert!(cache.stats().invalidations >= 2);
}

#[test]
fn async_storage_string_write_stays_on_hot_memory_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
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

    let write = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "hot".to_string(),
            value: b"value".to_vec(),
        },
    });
    assert!(write.status.ok);
    // The write records
    // a WAL entry even in async mode; async only means the commit does not
    // block (no fsync -> syncs == 0), and page/index materialization is deferred.
    assert_eq!(engine.block_store().stats().writes, 0);
    assert_eq!(engine.write_ahead_log_store().stats(1).writes, 1);
    assert_eq!(engine.write_ahead_log_store().stats(1).syncs, 0);
    assert_eq!(engine.index_log_store().stats(1).writes, 0);

    let read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "hot".to_string(),
        },
    });
    assert_eq!(
        read.response,
        CommandResponse::Bytes {
            value: Some(b"value".to_vec())
        }
    );
    assert_eq!(engine.block_store().stats().reads, 0);
    assert!(engine.cache().stats().memory_hits >= 1);
}

#[test]
fn async_hot_page_survives_memory_eviction_via_spill() {
    // Regression for the log-backed hot-page read-as-missing bug (gap P1): under async_storage a
    // write lives ONLY in the memory tier at a synthetic hot-page address. If the memory-only
    // entry is evicted before the next dump, a read used to miss the cache, hit a non-existent
    // slab file, and return value:None for an ACKED write. The eviction handler now spills the
    // evicted hot page to a real slab and a hot-address cache miss resolves the redirect, so the
    // value survives eviction on the live path (no reload needed).
    let dir = tempfile::tempdir().unwrap();
    // Deliberately tiny memory tier so the flood below forces capacity eviction of the target.
    let engine = TemporalEngine::with_local_dirs(
        2048,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
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

    // Async write -> memory-only hot page, nothing in the block store yet.
    let target_value = b"durable-async-value-that-must-survive-eviction".to_vec();
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "target".to_string(),
                    value: target_value.clone(),
                },
            })
            .status
            .ok
    );
    assert_eq!(engine.block_store().stats().writes, 0);

    // Flood the memory tier with far more async hot pages than it can hold, forcing the target's
    // page to be evicted (and spilled to a real slab by the eviction handler).
    for i in 0..400 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("filler-{i}"),
                value: vec![b'x'; 96],
            },
        });
    }

    // At least one hot page must have spilled to a durable slab (proves the handler fired).
    assert!(
        engine.block_store().stats().writes > 0,
        "evicted hot pages must spill to a durable slab"
    );

    // Drop EVERY cache tier for the shard so the only surviving copy of the target is the durable
    // spilled slab (plus the WAL). This defeats the cache's own cross-tier retention and forces
    // the read to resolve through the hot-page spill redirect -- the path this fix adds.
    let _ = engine.cache().invalidate_shard(1);

    let reads_before = engine.block_store().stats().reads;
    // The acked write must still read back as its value -- resolved via the spill redirect + a
    // durable slab read, NOT as value:None.
    let read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "target".to_string(),
        },
    });
    assert_eq!(
        read.response,
        CommandResponse::Bytes {
            value: Some(target_value)
        },
        "an acked async write must not read as missing after its hot page is evicted"
    );
    // The value came from the durable spilled slab, confirming the redirect fallback was used.
    assert!(
        engine.block_store().stats().reads > reads_before,
        "the evicted target must be served from the durable spilled slab"
    );
}

fn set_async_config(engine: &TemporalEngine, shard_id: ShardId) {
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id,
                config: Config {
                    version: 2,
                    async_storage: true,
                    ..Config::default()
                },
            })
            .ok
    );
}

#[test]
fn atomic_batch_survives_restart_when_complete() {
    // Positive control for gap E3: a fully-persisted atomic batch replays in full on restart.
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine =
        TemporalEngine::with_local_dirs(1024 * 1024, dir.path().join("cache-a"), &page_dir, &index_dir);
    engine.load_shard(1);
    set_async_config(&engine, 1);
    let batch = engine.batch_execute(BatchExecuteRequest {
        shard_id: 1,
        commands: vec![
            Command::StringSet { key: "b0".to_string(), value: b"v0".to_vec() },
            Command::StringSet { key: "b1".to_string(), value: b"v1".to_vec() },
            Command::StringSet { key: "b2".to_string(), value: b"v2".to_vec() },
        ],
    });
    assert!(batch.status.ok);
    drop(engine);

    let restarted =
        TemporalEngine::with_local_dirs(1024 * 1024, dir.path().join("cache-b"), &page_dir, &index_dir);
    restarted.load_shard(1);
    for (key, value) in [("b0", "v0"), ("b1", "v1"), ("b2", "v2")] {
        let read = restarted.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet { key: key.to_string() },
        });
        assert_eq!(
            read.response,
            CommandResponse::Bytes { value: Some(value.as_bytes().to_vec()) },
            "complete batch key {key} must survive restart"
        );
    }
}

#[test]
fn atomic_batch_is_all_or_nothing_when_commit_marker_lost() {
    // Gap E3: a crash between the batch's buffered appends and its single durability barrier can
    // leave a partial suffix on disk. Simulate that by dropping the batch's final (commit-marker)
    // WAL line; on restart the WHOLE batch must be discarded -- never a partially-applied prefix
    // that a retry would double-apply.
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine =
        TemporalEngine::with_local_dirs(1024 * 1024, dir.path().join("cache-a"), &page_dir, &index_dir);
    engine.load_shard(1);
    set_async_config(&engine, 1);
    // A standalone durable write before the batch, to prove only the batch is dropped.
    assert!(engine
        .execute_durable(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet { key: "keep".to_string(), value: b"kept".to_vec() },
        })
        .status
        .ok);
    let batch = engine.batch_execute(BatchExecuteRequest {
        shard_id: 1,
        commands: vec![
            Command::StringSet { key: "b0".to_string(), value: b"v0".to_vec() },
            Command::StringSet { key: "b1".to_string(), value: b"v1".to_vec() },
            Command::StringSet { key: "b2".to_string(), value: b"v2".to_vec() },
        ],
    });
    assert!(batch.status.ok);
    drop(engine);

    // Simulate the lost commit marker: drop the last WAL line (batch_index == batch_size).
    let wal_path = index_dir.join("wals").join("shard-1.wal.jsonl");
    // Read as BYTES and walk the records by their FRAMES, not by newlines. A record is only a
    // "line" while the frame ends with one; once it declares its own length the payload carries
    // 0x0A freely, and splitting on that byte cuts records in half -- which makes this test
    // build a log no writer would ever produce and then assert on how recovery handles it.
    // Asking the frame where each record ends works whichever frame wrote it.
    let contents = std::fs::read(&wal_path).expect("wal file should exist");
    let mut spans: Vec<(usize, usize)> = Vec::new();
    let mut at = 0usize;
    while at < contents.len() {
        // The preallocated zeros reservation trails the records and is not one of them.
        if contents[at] == 0 {
            break;
        }
        match crate::log_framing::next_frame(&contents[at..]) {
            Ok(Some((consumed, _))) if consumed > 0 => {
                spans.push((at, at + consumed));
                at += consumed;
            }
            _ => break,
        }
    }
    assert!(
        spans.len() >= 4,
        "expected keep + 3 batch records, got {}",
        spans.len()
    );
    spans.pop(); // drop the batch commit-marker record
    let mut truncated = Vec::new();
    for (start, end) in spans {
        truncated.extend_from_slice(&contents[start..end]);
    }
    std::fs::write(&wal_path, truncated).expect("rewrite wal");

    let restarted =
        TemporalEngine::with_local_dirs(1024 * 1024, dir.path().join("cache-b"), &page_dir, &index_dir);
    restarted.load_shard(1);

    // The pre-batch durable write survives.
    assert_eq!(
        restarted
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet { key: "keep".to_string() },
            })
            .response,
        CommandResponse::Bytes { value: Some(b"kept".to_vec()) }
    );
    // No member of the incomplete batch is applied -- not even the durable prefix (b0, b1).
    for key in ["b0", "b1", "b2"] {
        assert_eq!(
            restarted
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet { key: key.to_string() },
                })
                .response,
            CommandResponse::Bytes { value: None },
            "incomplete-batch key {key} must NOT be applied"
        );
    }
}

#[test]
fn durable_execute_overrides_async_storage_for_raft_local_file_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
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

    let write = engine.execute_durable(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "raft".to_string(),
            value: b"value".to_vec(),
        },
    });
    assert!(write.status.ok);
    assert_eq!(engine.block_store().stats().writes, 1);
    assert_eq!(engine.write_ahead_log_store().stats(1).writes, 1);
    assert_eq!(engine.index_log_store().stats(1).writes, 1);

    let read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "raft".to_string(),
        },
    });
    assert_eq!(
        read.response,
        CommandResponse::Bytes {
            value: Some(b"value".to_vec())
        }
    );
}

#[test]
fn durable_index_survives_restart_and_points_to_page_file() {
    let dir = tempfile::tempdir().unwrap();
    let cache_dir = dir.path().join("cache-a");
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(1024, &cache_dir, &page_dir, &index_dir);
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"persisted".to_vec(),
        },
    });

    let restarted =
        TemporalEngine::with_local_dirs(1024, dir.path().join("cache-b"), &page_dir, &index_dir);
    restarted.load_shard(1);
    let response = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        response.response,
        CommandResponse::Bytes {
            value: Some(b"persisted".to_vec())
        }
    );
    assert_eq!(restarted.block_store().stats().reads, 1);
}

#[test]
fn async_write_survives_restart_via_wal_replay_like_native() {
    // conformance: an async_storage write records a WAL entry but defers page/
    // index materialization to the background dump. If the crash beats the dump,
    // Startup load replays the wal and recovers the write. Rust must replay
    // its WAL on shard load the same way, or the async write is silently lost.
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-a"),
        &page_dir,
        &index_dir,
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
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"async-durable".to_vec(),
        },
    });
    assert_eq!(engine.block_store().stats().writes, 0);
    assert_eq!(engine.write_ahead_log_store().stats(1).writes, 1);
    drop(engine);

    let restarted = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-b"),
        &page_dir,
        &index_dir,
    );
    restarted.load_shard(1);
    let response = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        response.response,
        CommandResponse::Bytes {
            value: Some(b"async-durable".to_vec())
        },
        "async write must be recovered by replaying the WAL on shard load"
    );
}

// Readiness race regression: after a restart with WAL entries not yet reflected in the served
// index, the shard is published but NOT serving until `replay_wal_into_shard` completes
// (ShardInfo.recovering gates it). A read that arrives inside that window must NOT observe a
// false null from the half-reconstructed state -- it must be rejected with a retryable
// `shard_not_loaded`. Once replay completes the same read must return the recovered value.
// (The fix commit gated this deliberately without a deterministic test; this pins it.)
#[test]
fn read_during_wal_replay_recovery_returns_retryable_not_false_null() {
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-a"),
        &page_dir,
        &index_dir,
    );
    engine.load_shard(1);
    // async_storage: the write lands in the WAL but page/index materialization is deferred, so
    // it is absent from the served index until the WAL is replayed on the next load.
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
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"recovered-value".to_vec(),
        },
    });
    assert_eq!(engine.block_store().stats().writes, 0);
    assert_eq!(engine.write_ahead_log_store().stats(1).writes, 1);
    drop(engine);

    // Restart, and park the shard in the recovery window: base index loaded (the async write is
    // NOT in it yet), recovering=true, WAL not yet replayed.
    let restarted = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-b"),
        &page_dir,
        &index_dir,
    );
    let watermark = restarted.test_publish_recovering_shard(1);

    // A read inside the recovery window must be rejected retryably, NOT return a false null.
    let during = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert!(
        !during.status.ok,
        "read during WAL-replay recovery must be rejected, not served from half-built state"
    );
    assert_eq!(during.status.code, "shard_not_loaded");
    assert_eq!(
        during.response,
        CommandResponse::Empty,
        "recovering read must return a retryable error, never a false null (Bytes {{ None }})"
    );

    // After replay completes and the gate clears, the same read returns the recovered value.
    restarted.test_finish_recovery(1, watermark);
    let after = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert!(after.status.ok, "shard must serve once recovery completes");
    assert_eq!(
        after.response,
        CommandResponse::Bytes {
            value: Some(b"recovered-value".to_vec())
        },
        "the WAL-recovered value must be served after recovery completes"
    );
}

// Coalesced control-state persistence skips the per-write whole-series page rewrite
// (write-amplification) and relies on the index snapshot + WAL replay for durability, the
// same model control_state_changes/fol already use. This MUST stay crash-safe: increments
// recorded only in the WAL (no page materialization, no flush) must be recovered on reload.
#[test]
fn control_state_coalesced_write_survives_restart_via_wal_replay() {
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-a"),
        &page_dir,
        &index_dir,
    );
    engine.load_shard(1);
    let mut config = Config {
        version: 2,
        async_storage: true,
        ..Config::default()
    };
    config
        .extend_config
        .insert("control_coalesce_persist".to_string(), "on".to_string());
    assert!(engine.set_config(SetConfigRequest { shard_id: 1, config }).ok);

    for (timestamp_ms, amount) in [(10u64, 1i64), (20, 2), (30, 4), (40, 8)] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateIncrement {
                key: "cap".to_string(),
                timestamp_ms,
                amount,
            },
        });
    }
    // Coalesced: the per-write control_state page rewrite is skipped; the WAL carries durability.
    assert_eq!(
        engine.block_store().stats().writes,
        0,
        "coalesced control-state writes must not materialize a per-write page"
    );
    assert!(
        engine.write_ahead_log_store().stats(1).writes >= 4,
        "each coalesced increment must still be recorded in the WAL"
    );
    drop(engine);

    let restarted = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-b"),
        &page_dir,
        &index_dir,
    );
    restarted.load_shard(1);
    let response = restarted
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateCount {
                key: "cap".to_string(),
                start_ms: 0,
                end_ms: 100,
            },
        })
        .response;
    assert_eq!(
        response,
        CommandResponse::Integer { value: 15 },
        "coalesced control-state counter must be recovered by WAL replay on reload"
    );
}

#[test]
fn async_storage_batch_write_records_wal_without_sync_or_index() {
    // Batch analog of the single-command async conformance: the batch path (used by
    // ingestion) must also record a WAL entry per async write, unfsynced, with page/
    // index materialization deferred.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
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
    let batch = engine.batch_execute(BatchExecuteRequest {
        shard_id: 1,
        commands: vec![
            Command::StringSet {
                key: "a".to_string(),
                value: b"a".to_vec(),
            },
            Command::StringSet {
                key: "b".to_string(),
                value: b"b".to_vec(),
            },
        ],
    });
    assert!(batch.status.ok);
    assert_eq!(engine.block_store().stats().writes, 0);
    assert_eq!(engine.write_ahead_log_store().stats(1).writes, 2);
    assert_eq!(engine.write_ahead_log_store().stats(1).syncs, 0);
    assert_eq!(engine.index_log_store().stats(1).writes, 0);
}

#[test]
fn eviction_ranks_victims_least_recently_used_first_like_native() {
    // Eviction victims are ordered least-recently-used first, so a
    // repeatedly-read hot bucket is evicted last. Rust stamps per-bucket recency on
    // every read/write and sorts victims oldest-first.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for key in ["cold-a", "cold-b", "cold-c", "cold-d", "hot"] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.to_string(),
                value: b"value".to_vec(),
            },
        });
    }
    std::thread::sleep(std::time::Duration::from_millis(3));
    for _ in 0..3 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "hot".to_string(),
            },
        });
    }
    let report = engine.apply_storage_eviction(1, 0, 0, false, false);
    assert!(
        report.selected_victims.len() >= 2,
        "expected multiple victim buckets: {report:?}"
    );
    let recencies: Vec<u64> = report
        .selected_victims
        .iter()
        .map(|victim| victim.last_touched_ms)
        .collect();
    assert!(
        recencies.windows(2).all(|pair| pair[0] <= pair[1]),
        "victims must be ordered least-recently-used first: {report:?}"
    );
    assert!(
        recencies.last() > recencies.first(),
        "the hot bucket must have the newest recency and sort last: {report:?}"
    );
}

#[test]
fn wal_replay_gap_refuses_load_like_dataloss() {
    // WAL replay returns DataLoss and aborts load on a hole in the
    // retained wal. Rust must likewise refuse the load (not-loaded) rather than
    // silently serve a truncated prefix.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    // Seed the WAL with a sequence hole {1,2,4} (missing 3) before loading the shard;
    // there is no served index, so recovery replays from watermark 0.
    let wal = engine.write_ahead_log_store();
    for seq in [1u64, 2, 4] {
        wal.append_replayed_record(WriteAheadLogRecord {
            shard_id: 1,
            sequence: seq,
            command: Some(Command::StringSet {
                key: format!("k{seq}"),
                value: b"v".to_vec(),
            }),
            metadata: None,
            staged_pages: Vec::new(),
            outcomes: Vec::new(),
        })
        .unwrap();
    }
    let response = engine.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: u32::MAX,
        readonly: false,
        table_name: String::new(),
    });
    assert!(
        !response.status.ok,
        "load must refuse on a WAL sequence gap: {response:?}"
    );
    assert_eq!(response.status.code, "wal_replay_sequence_gap");
    // The shard must be left NOT loaded (unwound).
    let get = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k1".to_string(),
        },
    });
    assert!(
        !get.status.ok,
        "a refused load must leave the shard not-loaded: {get:?}"
    );
}

#[test]
fn expiry_sweep_emits_wal_tombstone_like_native() {
    // Active expiry is a logged, replicated delete. Rust's sweep must append a
    // WAL tombstone per expired key so followers / WAL replay observe the deletion,
    // instead of removing only in-memory + index.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CommonExpire {
            key: "k".to_string(),
            ttl_ms: 1,
        },
    });
    std::thread::sleep(std::time::Duration::from_millis(5));
    let wal_before_sweep = engine.write_ahead_log_store().stats(1).writes;
    let report = engine.sweep_expired_records(1).unwrap();
    assert_eq!(report.expired_records_removed, 1);
    let wal_after_sweep = engine.write_ahead_log_store().stats(1).writes;
    assert_eq!(
        wal_after_sweep - wal_before_sweep,
        1,
        "the expiry sweep must append one WAL delete (tombstone) per expired key"
    );
}


/// Pins the two clocks these recovery tests depend on: the timestamp stamped onto each WAL
/// record (which replay reads back as the leader clock) and the clock the engine computes
/// deadlines with. Both are restored on drop, including on panic, so a pinned clock can
/// never leak into another test on this thread.
static PINNED_LEADER_CLOCK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

struct PinnedLeaderClock(#[allow(dead_code)] std::sync::MutexGuard<'static, ()>);

impl PinnedLeaderClock {
    fn at(leader_now_ms: u64) -> Self {
        // The record clock is process-wide, so two tests pinning it at once would stamp each
        // other's records. Held for the few writes that need it, released on drop.
        let guard = PINNED_LEADER_CLOCK_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        crate::wal::set_test_record_clock_ms(Some(leader_now_ms));
        crate::engine::set_replay_clock_ms(Some(leader_now_ms));
        PinnedLeaderClock(guard)
    }

    fn real_now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock before epoch")
            .as_millis() as u64
    }
}

impl Drop for PinnedLeaderClock {
    fn drop(&mut self) {
        crate::wal::set_test_record_clock_ms(None);
        crate::engine::set_replay_clock_ms(None);
    }
}

#[test]
fn wal_replay_uses_leader_timestamp_for_ttl_deadline_like_native() {
    // resolves a TTL to an ABSOLUTE deadline on the leader before logging it
    // (deadline = request.ttl_ms + current time at log time); replay uses that
    // value. Rust must resolve replayed TTLs against the leader's logged timestamp, not
    // the (later) restart clock -- otherwise crash recovery resurrects a key past its
    // deadline with a fresh restart-time+ttl lifetime.
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-a"),
        &page_dir,
        &index_dir,
    );
    engine.load_shard(1);
    // async so the SETEX lives only in the WAL, forcing a replay on reload.
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
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSetEx {
            key: "k".to_string(),
            value: b"v".to_vec(),
            ttl_ms: 40,
        },
    });
    // Sleep well past the 40ms TTL so the leader deadline is in the past at reload.
    std::thread::sleep(std::time::Duration::from_millis(160));
    drop(engine);

    let restarted = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-b"),
        &page_dir,
        &index_dir,
    );
    restarted.load_shard(1);
    let get = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        get.response,
        CommandResponse::Bytes { value: None },
        "a key past its leader TTL deadline must not be resurrected with a replay-time deadline"
    );
}

#[test]
fn wal_replay_conditional_write_uses_leader_clock_for_lazy_expiry_like_native() {
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-a"),
        &page_dir,
        &index_dir,
    );
    engine.load_shard(1);
    // async so both records live only in the WAL, forcing a replay on reload.
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

    // An hour of downtime, STATED rather than slept for. Sleeping for it was the defect in
    // this test: the two writes below have to land while k is still live, so a real deadline
    // made that a race against whatever else the machine was doing, and any stall longer than
    // the TTL failed the test on a property it is not about.
    let leader_now_ms = PinnedLeaderClock::real_now_ms() - 60 * 60 * 1000;
    {
        let _clock = PinnedLeaderClock::at(leader_now_ms);
        // Record 1: create k with a one-minute deadline, an hour before restart.
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSetEx {
                key: "k".to_string(),
                value: b"v1".to_vec(),
                ttl_ms: 60 * 1000,
            },
        });
        // Record 2: a SET XX refreshing k=v2 with a deadline that OUTLASTS the downtime. On
        // the leader this fires because k is live at leader time, and it now fires
        // unconditionally, because the deadline and the check read the same pinned clock.
        let cond = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSetConditional {
                key: "k".to_string(),
                value: b"v2".to_vec(),
                ttl_ms: Some(2 * 60 * 60 * 1000),
                condition: StringSetCondition::IfExists,
                return_old: false,
            },
        });
        assert_eq!(
            cond.response,
            CommandResponse::Integer { value: 1 },
            "SET XX must fire on the leader while k is live"
        );
    }
    // These writes are asynchronous: the commit does not block, so a returned write does not
    // mean the record has reached the file. The engine has no Drop that flushes, so dropping
    // it is not a barrier either -- the successor below can open the log before the last
    // record lands in it, and a replay test whose log is missing its last record fails
    // looking exactly like a recovery defect. Flush, then check there is something to replay.
    let flushed = engine
        .write_ahead_log_store()
        .flush(1)
        .expect("flush the log before the engine goes away");
    let _ = flushed;
    assert!(
        engine.write_ahead_log_store().stats(1).writes >= 2,
        "premise: the log must hold the writes this test is about to replay, got {}",
        engine.write_ahead_log_store().stats(1).writes
    );
    drop(engine);

    // Restart on the REAL clock. Record 1's deadline lapsed 59 minutes ago; the deadline the
    // SET XX installed is still an hour out. Replay installs record 1's outcome (an absolute
    // leader-time deadline) and re-executes record 2, whose lazy-expiry check picks the
    // branch: against the leader clock k is live and the write survives; against the restart
    // clock k reads expired and a durably-committed write is silently lost.
    let restarted = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-b"),
        &page_dir,
        &index_dir,
    );
    // Assert the LOAD, not just what is readable after it. `load_shard` throws its response
    // away, so a shard that refused to recover looks identical to one that recovered empty --
    // every assertion below then reports a missing value and blames replay for dropping it,
    // which is the wrong end of the failure and is exactly how this was read for several
    // rounds.
    let loaded = restarted.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: u32::MAX,
        readonly: false,
        table_name: String::new(),
    });
    assert!(
        loaded.status.ok,
        "the shard refused the load: {:?}",
        loaded.status
    );

    let get = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        get.response,
        CommandResponse::Bytes {
            value: Some(b"v2".to_vec())
        },
        "replay must reproduce the leader's SET XX branch (k live at leader time), not drop \
         the committed write because the original deadline lapsed during downtime"
    );
}

#[test]
fn wal_replay_rearmed_expire_does_not_abort_recovery_like_native() {
    // A committed EXPIRE that re-arms a key's TTL must replay cleanly even if the key's
    // ORIGINAL (pre-re-arm) deadline has lapsed by restart time. Command preconditions are
    // a LEADER-time gate; re-running them during replay against the restart clock made a
    // replayed EXPIRE fail "not_found" and abort the WHOLE shard load (data unavailability
    // for every key on the shard). ReplayWal re-applies logged effects without
    // re-checking preconditions. The canary below proves the shard actually recovered.
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-a"),
        &page_dir,
        &index_dir,
    );
    engine.load_shard(1);
    // async so every record lives only in the WAL, forcing a full replay on reload.
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

    // An hour of downtime, stated rather than slept for, as in the sibling test. What this
    // test needs is that k's deadline has LAPSED by restart -- a premise that now holds by
    // construction. It previously held only if the sleep outlasted the TTL, and widening the
    // TTL to steady the race quietly inverted it: 250ms of downtime against a 1500ms
    // deadline left the key live at restart, so the test passed without ever building the
    // situation it exists to check.
    let leader_now_ms = PinnedLeaderClock::real_now_ms() - 60 * 60 * 1000;
    {
        let _clock = PinnedLeaderClock::at(leader_now_ms);
        // Durable canary that survives iff replay runs to completion (not aborted mid-way).
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "canary".to_string(),
                value: b"present".to_vec(),
            },
        });
        // Create k with a one-minute deadline, then re-arm it via EXPIRE. Both logged, both
        // an hour before restart, so both deadlines are in the past by the time replay runs.
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSetEx {
                key: "k".to_string(),
                value: b"v".to_vec(),
                ttl_ms: 60 * 1000,
            },
        });
        let rearmed = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonExpire {
                key: "k".to_string(),
                ttl_ms: 60 * 1000,
            },
        });
        assert!(
            rearmed.status.ok,
            "EXPIRE on a live key should succeed on the leader"
        );
    }
    // These writes are asynchronous: the commit does not block, so a returned write does not
    // mean the record has reached the file. The engine has no Drop that flushes, so dropping
    // it is not a barrier either -- the successor below can open the log before the last
    // record lands in it, and a replay test whose log is missing its last record fails
    // looking exactly like a recovery defect. Flush, then check there is something to replay.
    let flushed = engine
        .write_ahead_log_store()
        .flush(1)
        .expect("flush the log before the engine goes away");
    let _ = flushed;
    assert!(
        engine.write_ahead_log_store().stats(1).writes >= 3,
        "premise: the log must hold the writes this test is about to replay, got {}",
        engine.write_ahead_log_store().stats(1).writes
    );
    drop(engine);

    let restarted = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-b"),
        &page_dir,
        &index_dir,
    );
    // Assert the LOAD, not just what is readable after it. `load_shard` throws its response
    // away, so a shard that refused to recover looks identical to one that recovered empty --
    // every assertion below then reports a missing value and blames replay for dropping it,
    // which is the wrong end of the failure and is exactly how this was read for several
    // rounds.
    let loaded = restarted.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: u32::MAX,
        readonly: false,
        table_name: String::new(),
    });
    assert!(
        loaded.status.ok,
        "the shard refused the load: {:?}",
        loaded.status
    );

    // THE PREMISE, ASSERTED. This test is only meaningful if k's deadline actually lapsed
    // while the engine was down; if it did not, the situation it exists to check was never
    // built and the canary below passes for free. That is not hypothetical -- widening this
    // test's TTL to steady a race once left 250ms of downtime against a 1500ms deadline, and
    // it kept passing while checking nothing. So: k must be GONE at restart.
    let lapsed = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        lapsed.response,
        CommandResponse::Bytes { value: None },
        "premise: k's re-armed deadline must have lapsed during the downtime, or this test \
         is not exercising a replayed EXPIRE whose deadline is in the past"
    );
    let canary = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "canary".to_string(),
        },
    });
    assert_eq!(
        canary.response,
        CommandResponse::Bytes {
            value: Some(b"present".to_vec())
        },
        "shard must recover: a replayed re-arming EXPIRE whose original deadline lapsed must \
         not abort the whole WAL replay"
    );
}

#[test]
fn hdel_last_field_removes_key_like_native() {
    // hash2::Del removes the whole object once the last field is deleted; Rust left an
    // empty field map behind, so the key still reported as existing (EXISTS=1, TYPE=hash).
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashSet {
            key: "h".to_string(),
            field: "f".to_string(),
            value: b"v".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashDelete {
            key: "h".to_string(),
            field: "f".to_string(),
        },
    });
    let exists = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CommonExists {
            key: "h".to_string(),
        },
    });
    assert_eq!(
        exists.response,
        CommandResponse::Integer { value: 0 },
        "an emptied hash must not leave a phantom key (deletes the object on last HDEL)"
    );
}

#[test]
fn timestamped_models_survive_unload_reload_round_trip() {
    // Regression net for the membership-authoritative reconcile + serde round-trip: write a
    // string plus the reconcile page-reading timestamped kinds (feature + sequence), then
    // unload + reload and assert every value comes back intact (no loss, no resurrection).
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "s".to_string(),
            value: b"string-value".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "f".to_string(),
            points: vec![
                FeaturePoint {
                    timestamp_ms: 10,
                    value: b"a".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 20,
                    value: b"b".to_vec(),
                },
            ],
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SequenceAdd {
            key: "seq".to_string(),
            rows: vec![SequenceFeatureRow {
                timestamp_ms: 5,
                gid: 1,
                action_type: 2,
                duration: 3,
                author_id: 4,
            }],
        },
    });

    let feature_timestamps = |engine: &TemporalEngine| -> Vec<u64> {
        match engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureQuery {
                    key: "f".to_string(),
                    start_ms: 0,
                    end_ms: u64::MAX,
                    count: Some(100),
                },
            })
            .response
        {
            CommandResponse::FeaturePoints { points } => {
                points.iter().map(|p| p.timestamp_ms).collect()
            }
            other => panic!("expected FeaturePoints, got {other:?}"),
        }
    };
    let sequence_len = |engine: &TemporalEngine| -> usize {
        match engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::SequenceQuery {
                    key: "seq".to_string(),
                    start_ms: 0,
                    end_ms: u64::MAX,
                    count: 100,
                    filters: Vec::new(),
                },
            })
            .response
        {
            CommandResponse::SequenceRows { rows } => rows.len(),
            other => panic!("expected SequenceRows, got {other:?}"),
        }
    };
    let features_before = feature_timestamps(&engine);
    assert_eq!(features_before, vec![10, 20]);
    assert_eq!(sequence_len(&engine), 1);

    engine.unload_shard(1);
    engine.load_shard(1);

    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "s".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"string-value".to_vec()),
        },
        "string must survive reload"
    );
    assert_eq!(
        feature_timestamps(&engine),
        features_before,
        "feature series must survive reload unchanged"
    );
    assert_eq!(
        sequence_len(&engine),
        1,
        "sequence row must survive reload"
    );
}

#[test]
fn reconcile_does_not_resurrect_evicted_feature_points_on_reload() {
    // A single FeatureAppend packs all timestamps into one page; the feature_max_size trim
    // drops the oldest from the in-memory series, but the page still physically holds them.
    // reconcile-from-pages must keep the persisted (trimmed) membership -- the evicted points
    // must NOT resurrect on reload (writes per-timestamp deleted tombstones for this).
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache-a"),
        &page_dir,
        &index_dir,
    );
    engine.load_shard(1);
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    feature_max_size: 3,
                    ..Config::default()
                },
            })
            .ok
    );
    // One append of 5 points; the max_size=3 trim keeps the newest 3 in the series.
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "f".to_string(),
            points: (1..=5)
                .map(|i| FeaturePoint {
                    timestamp_ms: i * 10,
                    value: vec![i as u8],
                })
                .collect(),
        },
    });
    let live_timestamps = |engine: &TemporalEngine| -> Vec<u64> {
        match engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureQuery {
                    key: "f".to_string(),
                    start_ms: 0,
                    end_ms: u64::MAX,
                    count: Some(100),
                },
            })
            .response
        {
            CommandResponse::FeaturePoints { points } => {
                points.iter().map(|point| point.timestamp_ms).collect()
            }
            other => panic!("expected FeaturePoints, got {other:?}"),
        }
    };
    let before = live_timestamps(&engine);
    assert!(
        before.contains(&50) && !before.contains(&10),
        "trim should keep the newest points and drop the oldest: {before:?}"
    );
    drop(engine);

    let restarted = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache-b"),
        &page_dir,
        &index_dir,
    );
    restarted.load_shard(1);
    assert_eq!(
        live_timestamps(&restarted),
        before,
        "reload must not resurrect the evicted feature points that still physically live in the page"
    );
}

/// Sets an env gate for the duration of a test and removes it on drop (even on panic), so a
/// gated-behavior test never leaks its flag into the rest of the suite.
struct FoldEnvGuard {
    names: Vec<&'static str>,
}

impl FoldEnvGuard {
    fn set(pairs: &[(&'static str, &str)]) -> Self {
        let names = pairs.iter().map(|(name, _)| *name).collect();
        for (name, value) in pairs {
            std::env::set_var(name, value);
        }
        Self { names }
    }
}

impl Drop for FoldEnvGuard {
    fn drop(&mut self) {
        for name in &self.names {
            std::env::remove_var(name);
        }
    }
}

fn write_string(engine: &TemporalEngine, key: &str, value: &[u8]) {
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: key.to_string(),
            value: value.to_vec(),
        },
    });
}

fn read_string(engine: &TemporalEngine, key: &str) -> Option<Vec<u8>> {
    match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: key.to_string(),
            },
        })
        .response
    {
        CommandResponse::Bytes { value } => value,
        other => panic!("expected Bytes, got {other:?}"),
    }
}

#[test]
fn manifest_fold_threshold_dump_fires_only_past_the_gap_and_folds_the_catalog() {
    // MANIFEST-CONFORMANCE FOLD cadence: with a tiny WAL gap, a threshold dump fires once the
    // undumped index-log has grown past it, folding the band/zone catalog into an index-log
    // MetaItem anchor; below the gap nothing is dumped. Matches
    // index-meta dump background cadence -- never a per-write dump.
    let _guard = FoldEnvGuard::set(&[("TS_INDEX_CATALOG_FOLD", "1")]);
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine =
        TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache"), &page_dir, &index_dir);
    engine.load_shard(1);
    write_string(&engine, "k0", b"v0");
    // A gap far larger than the current index-log must NOT dump.
    assert!(
        !engine.maybe_dump_index_catalog_with_gap_for_test(1, u64::MAX),
        "no dump below the threshold"
    );
    assert!(
        engine.index_log_store().latest_zone_catalog(1).unwrap().is_none(),
        "no folded catalog before any dump"
    );
    // A gap of 1 byte crosses immediately: exactly one dump fires and folds the catalog.
    for i in 0..8 {
        write_string(&engine, &format!("k{i}"), b"value");
    }
    assert!(
        engine.maybe_dump_index_catalog_with_gap_for_test(1, 1),
        "dump fires once the undumped gap crosses the threshold"
    );
    let catalog = engine.index_log_store().latest_zone_catalog(1).unwrap();
    let catalog = catalog.expect("a folded band/zone catalog must be durable after the dump");
    assert!(
        !catalog.zones.is_empty(),
        "the dump must fold at least the active band into the anchor"
    );
    // The watermark advanced: the undumped gap reset, so an immediate re-check does not re-dump.
    assert!(
        !engine.maybe_dump_index_catalog_with_gap_for_test(1, u64::MAX),
        "the dumped watermark advanced; no immediate re-dump"
    );
}

#[test]
fn catalog_dump_reclaim_shrinks_both_logs_and_reload_stays_exact() {
    // Embedded-path log reclaim: once a threshold dump durably captures the shard, the
    // index-log records the base reflects and the WAL prefix below the anchor are redundant --
    // reclaiming them must SHRINK both files on disk, keep the cadence alive (watermark
    // measured from the post-reclaim length), and leave a reload byte-exact.
    let _guard = FoldEnvGuard::set(&[("TS_INDEX_CATALOG_FOLD", "1")]);
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine =
        TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-a"), &page_dir, &index_dir);
    engine.load_shard(1);
    for i in 0..60 {
        write_string(&engine, &format!("key-{i}"), format!("val-{i}").as_bytes());
    }
    let report = engine
        .maybe_dump_and_reclaim_with_gap_for_test(1, 1)
        .expect("past the gap, the dump + reclaim must fire");
    assert!(
        report.index_log_bytes_after < report.index_log_bytes_before,
        "the index log must shrink on disk after the dump ({} -> {})",
        report.index_log_bytes_before,
        report.index_log_bytes_after
    );
    assert!(
        report.wal_bytes_after < report.wal_bytes_before,
        "the WAL must shrink on disk after the dump ({} -> {})",
        report.wal_bytes_before,
        report.wal_bytes_after
    );
    assert!(report.index_log_records_removed > 0);
    assert!(report.wal_records_removed > 0);
    // The folded catalog anchor survives its own sweep -- it is the load-time catalog seed.
    assert!(
        engine
            .index_log_store()
            .latest_zone_catalog(1)
            .unwrap()
            .is_some(),
        "the folded catalog anchor must survive the index-log sweep"
    );
    // The watermark was re-marked at the post-reclaim length: no immediate re-dump, and the
    // cadence fires again once new writes regrow the gap (it must not wait for the file to
    // regrow past its PRE-reclaim size).
    assert!(engine.maybe_dump_and_reclaim_with_gap_for_test(1, u64::MAX).is_none());
    for i in 60..70 {
        write_string(&engine, &format!("key-{i}"), format!("val-{i}").as_bytes());
    }
    assert!(
        engine.maybe_dump_and_reclaim_with_gap_for_test(1, 1).is_some(),
        "the cadence must keep firing after a reclaim shrank the log"
    );
    drop(engine);
    let restarted = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache-b"),
        &page_dir,
        &index_dir,
    );
    restarted.load_shard(1);
    for i in 0..70 {
        assert_eq!(
            read_string(&restarted, &format!("key-{i}")),
            Some(format!("val-{i}").into_bytes()),
            "every acked key must survive reload after the logs were reclaimed"
        );
    }
}

#[test]
fn catalog_dump_reclaim_pins_wal_records_holding_block_in_wal_pages() {
    // An async write's page can live ONLY in its WAL record (served back through the
    // block-in-WAL registration). A post-dump WAL sweep must pin its floor at the lowest
    // registered sequence so that record survives, even when the dump anchor is far above it.
    let _guard = FoldEnvGuard::set(&[("TS_INDEX_CATALOG_FOLD", "1")]);
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    // One async write first: its WAL record carries its staged page.
    engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config {
            version: 2,
            async_storage: true,
            ..Config::default()
        },
    });
    write_string(&engine, "async-key", b"async-value");
    // Then a run of synchronous writes, which advance the dump anchor past the async record.
    engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config {
            version: 3,
            async_storage: false,
            ..Config::default()
        },
    });
    for i in 0..40 {
        write_string(&engine, &format!("sync-{i}"), format!("val-{i}").as_bytes());
    }
    let report = engine
        .maybe_dump_and_reclaim_with_gap_for_test(1, 1)
        .expect("dump + reclaim must fire");
    let floor = report
        .wal_retention_floor
        .expect("the staged async page must register a WAL retention floor");
    assert!(
        floor <= report.wal_anchor,
        "the floor ({floor}) pins a record below the dump anchor ({})",
        report.wal_anchor
    );
    // The pinned record survived: the async value still reads back exactly.
    assert_eq!(
        read_string(&engine, "async-key"),
        Some(b"async-value".to_vec()),
        "the async write's page must survive the WAL sweep"
    );
}

#[test]
fn manifest_fold_reload_reconstructs_catalog_with_band_manifest_deleted() {
    // MANIFEST-CONFORMANCE FOLD round-trip: write, dump (folds the catalog), delete the band-manifest
    // file, reload -- every acked key must survive AND the band lifecycle must reconstruct from
    // the folded index-log MetaItem, proving the fold is a lossless catalog source.
    let _guard = FoldEnvGuard::set(&[("TS_INDEX_CATALOG_FOLD", "1")]);
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine =
        TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-a"), &page_dir, &index_dir);
    engine.load_shard(1);
    for i in 0..50 {
        write_string(&engine, &format!("key-{i}"), format!("val-{i}").as_bytes());
    }
    assert!(engine.dump_index_catalog(1), "explicit dump must complete");
    let folded = engine
        .index_log_store()
        .latest_zone_catalog(1)
        .unwrap()
        .expect("catalog folded");
    drop(engine);
    // Delete the band-manifest file: the catalog must come back from the index-log fold, not the
    // per-write file.
    let manifest = page_dir.join("page_extent_manifest.json");
    if manifest.exists() {
        std::fs::remove_file(&manifest).unwrap();
    }
    let restarted = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache-b"),
        &page_dir,
        &index_dir,
    );
    restarted.load_shard(1);
    for i in 0..50 {
        assert_eq!(
            read_string(&restarted, &format!("key-{i}")),
            Some(format!("val-{i}").into_bytes()),
            "every acked key must survive reload after the manifest fold"
        );
    }
    // The folded lifecycle states are present in the reconstructed catalog.
    let recovered = restarted.block_store().zone_catalog(0);
    for zone in &folded.zones {
        assert!(
            recovered.iter().any(|z| z.page_slab_id == zone.page_slab_id),
            "band {} must be present after reload from the fold",
            zone.page_slab_id
        );
    }
}

#[test]
fn manifest_fold_on_does_not_resurrect_evicted_feature_points_on_reload() {
    // The #22 resurrection trap must still hold with the fold ON: the fold touches the band
    // catalog (M1), NOT the per-write served-index delta / key_states nor the WAL config-log, so
    // config-driven feature_max_size eviction stays durable and does not resurrect on reload.
    let _guard = FoldEnvGuard::set(&[("TS_INDEX_CATALOG_FOLD", "1")]);
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache-a"),
        &page_dir,
        &index_dir,
    );
    engine.load_shard(1);
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    feature_max_size: 3,
                    ..Config::default()
                },
            })
            .ok
    );
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "f".to_string(),
            points: (1..=5)
                .map(|i| FeaturePoint {
                    timestamp_ms: i * 10,
                    value: vec![i as u8],
                })
                .collect(),
        },
    });
    let live_timestamps = |engine: &TemporalEngine| -> Vec<u64> {
        match engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureQuery {
                    key: "f".to_string(),
                    start_ms: 0,
                    end_ms: u64::MAX,
                    count: Some(100),
                },
            })
            .response
        {
            CommandResponse::FeaturePoints { points } => {
                points.iter().map(|point| point.timestamp_ms).collect()
            }
            other => panic!("expected FeaturePoints, got {other:?}"),
        }
    };
    // Fold a catalog dump into the mix so the reload exercises the fold recovery path.
    assert!(engine.dump_index_catalog(1));
    let before = live_timestamps(&engine);
    assert!(
        before.contains(&50) && !before.contains(&10),
        "trim keeps the newest, drops the oldest: {before:?}"
    );
    drop(engine);
    let restarted = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache-b"),
        &page_dir,
        &index_dir,
    );
    restarted.load_shard(1);
    assert_eq!(
        live_timestamps(&restarted),
        before,
        "the fold must not resurrect config-evicted feature points on reload"
    );
}

#[test]
fn hash_incrby_rejects_non_integer_and_overflow_like_native() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashMultiSet {
            key: "h".to_string(),
            entries: vec![
                ("alpha".to_string(), b"abc".to_vec()),
                ("mixed".to_string(), b"123abc".to_vec()),
                ("max".to_string(), i64::MAX.to_string().into_bytes()),
                ("min".to_string(), i64::MIN.to_string().into_bytes()),
            ],
        },
    });

    for field in ["alpha", "mixed"] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashIncrBy {
                key: "h".to_string(),
                field: field.to_string(),
                increment: 1,
            },
        });
        assert_eq!(response.status.code, "unmatched");
        assert_eq!(response.response, CommandResponse::Empty);
    }

    let overflow = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashIncrBy {
            key: "h".to_string(),
            field: "max".to_string(),
            increment: 1,
        },
    });
    assert_eq!(overflow.status.code, "out_of_range");
    let underflow = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashIncrBy {
            key: "h".to_string(),
            field: "min".to_string(),
            increment: -1,
        },
    });
    assert_eq!(underflow.status.code, "out_of_range");
}

#[test]
fn hash_incrby_skips_leading_whitespace_in_stored_value_like_native() {
    // hash IncrBy parses the stored field with strtoll semantics, which
    // skips leading whitespace, so a field stored as " 5" is the valid integer 5. Rust parse_i64
    // previously used str::parse (rejects leading whitespace) and wrongly failed it as
    // "not an integer". A leading-space integer must now increment like.
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashSet {
            key: "h".to_string(),
            field: "padded".to_string(),
            value: b" 5".to_vec(),
        },
    });
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashIncrBy {
            key: "h".to_string(),
            field: "padded".to_string(),
            increment: 1,
        },
    });
    assert!(
        response.status.ok,
        "leading-whitespace integer must parse like C strtoll, got {:?}",
        response.status
    );
    assert_eq!(response.response, CommandResponse::Integer { value: 6 });

    // Trailing garbage must still be rejected on both sides (*end != '\0').
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashSet {
            key: "h".to_string(),
            field: "trailing".to_string(),
            value: b"5 ".to_vec(),
        },
    });
    let rejected = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashIncrBy {
            key: "h".to_string(),
            field: "trailing".to_string(),
            increment: 1,
        },
    });
    assert_eq!(
        rejected.status.code, "unmatched",
        "trailing whitespace must still be rejected like"
    );
}

#[test]
fn feature_append_packs_many_timestamp_values_into_one_page() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let first = SequenceFeatureRow {
        timestamp_ms: 10,
        gid: 1,
        action_type: 2,
        duration: 3,
        author_id: 4,
    };
    let second = SequenceFeatureRow {
        timestamp_ms: 20,
        gid: 5,
        action_type: 6,
        duration: 7,
        author_id: 8,
    };
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "packed-feature".to_string(),
            points: vec![
                FeaturePoint {
                    timestamp_ms: second.timestamp_ms,
                    value: second.encode_feature_proto_value(),
                },
                FeaturePoint {
                    timestamp_ms: first.timestamp_ms,
                    value: first.encode_feature_proto_value(),
                },
            ],
        },
    });
    assert!(response.status.ok);

    let (first_address, second_address) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let series = shards
            .get(&1)
            .and_then(|shard| shard.features.get("packed-feature"))
            .expect("feature series should exist");
        (
            series.get(&10).expect("first point").clone(),
            series.get(&20).expect("second point").clone(),
        )
    };
    assert_eq!(first_address, second_address);
    assert_eq!(
        first_address.object_id,
        Some(stable_page_object_id(1, "feature", "packed-feature", None))
    );
    let packed_bytes = engine.block_store().read(&first_address).unwrap();
    let packed_points = decode_feature_page(&packed_bytes).expect("packed feature page");
    assert_eq!(packed_points.len(), 2);
    assert_eq!(packed_points[0].timestamp_ms, 10);
    assert_eq!(packed_points[1].timestamp_ms, 20);

    let query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery {
            key: "packed-feature".to_string(),
            start_ms: 0,
            end_ms: 30,
            count: None,
        },
    });
    assert_eq!(
        query.response,
        CommandResponse::FeaturePoints {
            points: vec![
                FeaturePoint {
                    timestamp_ms: 10,
                    value: first.encode_feature_proto_value(),
                },
                FeaturePoint {
                    timestamp_ms: 20,
                    value: second.encode_feature_proto_value(),
                },
            ]
        }
    );

    let filtered = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQueryFiltered {
            key: "packed-feature".to_string(),
            start_ms: 0,
            end_ms: 30,
            count: None,
            filters: vec![FeatureFilter {
                field: "gid".to_string(),
                op: FeatureFilterOp::Equal,
                value: 5,
            }],
        },
    });
    assert_eq!(
        filtered.response,
        CommandResponse::FeaturePoints {
            points: vec![FeaturePoint {
                timestamp_ms: 20,
                value: second.encode_feature_proto_value(),
            }]
        }
    );

    let agg = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAggQuery {
            key: "packed-feature".to_string(),
            start_ms: 0,
            end_ms: 30,
            aggregator: "count".to_string(),
            count: None,
        },
    });
    assert_eq!(agg.response, CommandResponse::Aggregate { value: 2 });
}

// shared-corpus: feature_boundary_count_duplicate_semantics
#[test]
fn feature_policy_aliases_first_update_and_block() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    let base = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "native-policy-feature".to_string(),
            points: vec![FeaturePoint {
                timestamp_ms: 10,
                value: vec![b'a'],
            }],
        },
    });
    assert!(base.status.ok);

    let first_existing: Command = serde_json::from_value(serde_json::json!({
        "kind": "feature_append_with_policy",
        "key": "native-policy-feature",
        "policy": "FIRST",
        "points": [{"timestamp_ms": 10, "value": [120]}]
    }))
    .expect("FIRST policy alias should deserialize");
    let first_existing = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: first_existing,
    });
    assert_eq!(
        first_existing.response,
        CommandResponse::Integer { value: 0 }
    );

    let first_new: Command = serde_json::from_value(serde_json::json!({
        "kind": "feature_append_with_policy",
        "key": "native-policy-feature",
        "policy": "FIRST",
        "points": [{"timestamp_ms": 20, "value": [98]}]
    }))
    .expect("FIRST policy alias should deserialize");
    let first_new = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: first_new,
    });
    assert_eq!(first_new.response, CommandResponse::Integer { value: 1 });

    let update_missing: Command = serde_json::from_value(serde_json::json!({
        "kind": "feature_append_with_policy",
        "key": "native-policy-feature",
        "policy": "UPDATE",
        "points": [{"timestamp_ms": 30, "value": [99]}]
    }))
    .expect("UPDATE policy alias should deserialize");
    let update_missing = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: update_missing,
    });
    assert_eq!(
        update_missing.response,
        CommandResponse::Integer { value: 0 }
    );

    let update_existing: Command = serde_json::from_value(serde_json::json!({
        "kind": "feature_append_with_policy",
        "key": "native-policy-feature",
        "policy": "UPDATE",
        "points": [{"timestamp_ms": 10, "value": [65]}]
    }))
    .expect("UPDATE policy alias should deserialize");
    let update_existing = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: update_existing,
    });
    assert_eq!(
        update_existing.response,
        CommandResponse::Integer { value: 1 }
    );

    let block: Command = serde_json::from_value(serde_json::json!({
        "kind": "feature_append_with_policy",
        "key": "native-policy-feature",
        "policy": "BLOCK",
        "points": [{"timestamp_ms": 40, "value": [100]}]
    }))
    .expect("BLOCK policy alias should deserialize");
    let block = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: block,
    });
    assert!(!block.status.ok);
    assert_eq!(block.status.code, "invalid_argument");

    let query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery {
            key: "native-policy-feature".to_string(),
            start_ms: 0,
            end_ms: 50,
            count: None,
        },
    });
    assert_eq!(
        query.response,
        CommandResponse::FeaturePoints {
            points: vec![
                FeaturePoint {
                    timestamp_ms: 10,
                    value: vec![b'A'],
                },
                FeaturePoint {
                    timestamp_ms: 20,
                    value: vec![b'b'],
                },
            ]
        }
    );
}

#[test]
fn feature_append_chunks_and_persists_timestamped_kv_pages() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let points = (0..10)
        .map(|offset| FeaturePoint {
            timestamp_ms: 1_000 + offset,
            value: vec![b'a' + offset as u8; 10 * 1024],
        })
        .collect::<Vec<_>>();
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "chunked-feature".to_string(),
            points: points.clone(),
        },
    });
    assert!(response.status.ok);

    let addresses = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let series = shards
            .get(&1)
            .and_then(|shard| shard.features.get("chunked-feature"))
            .expect("feature series should exist");
        unique_timestamped_kv_page_addresses(series)
    };
    assert!(
        addresses.len() > 1,
        "large timestamped KV batch should be split into page chunks"
    );
    let mut persisted_timestamps = Vec::new();
    for address in &addresses {
        assert_eq!(
            address.object_id,
            Some(stable_page_object_id(1, "feature", "chunked-feature", None))
        );
        let bytes = engine.block_store().read(address).unwrap();
        let chunk = decode_feature_page(&bytes).expect("persisted packed page chunk");
        assert!(!chunk.is_empty());
        assert!(bytes.len() <= TIMESTAMPED_KV_PAGE_TARGET_BYTES + 12 * 1024);
        persisted_timestamps.extend(chunk.into_iter().map(|point| point.timestamp_ms));
    }
    persisted_timestamps.sort_unstable();
    assert_eq!(
        persisted_timestamps,
        points
            .iter()
            .map(|point| point.timestamp_ms)
            .collect::<Vec<_>>()
    );

    let query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery {
            key: "chunked-feature".to_string(),
            start_ms: 0,
            end_ms: 2_000,
            count: None,
        },
    });
    assert_eq!(query.response, CommandResponse::FeaturePoints { points });
}

#[test]
fn feature_append_keeps_oversized_single_timestamped_value_readable() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let points = vec![FeaturePoint {
        timestamp_ms: 1_000,
        value: vec![b'x'; TIMESTAMPED_KV_PAGE_TARGET_BYTES + 8 * 1024],
    }];
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "oversized-single-feature".to_string(),
            points: points.clone(),
        },
    });
    assert!(response.status.ok);

    let addresses = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let series = shards
            .get(&1)
            .and_then(|shard| shard.features.get("oversized-single-feature"))
            .expect("feature series should exist");
        unique_timestamped_kv_page_addresses(series)
    };
    assert_eq!(addresses.len(), 1);
    let bytes = engine.block_store().read(&addresses[0]).unwrap();
    assert!(bytes.len() > TIMESTAMPED_KV_PAGE_TARGET_BYTES);
    assert_eq!(decode_feature_page(&bytes).unwrap(), points);

    let query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery {
            key: "oversized-single-feature".to_string(),
            start_ms: 0,
            end_ms: 2_000,
            count: None,
        },
    });
    assert_eq!(query.response, CommandResponse::FeaturePoints { points });
    assert!(
        engine
            .storage_production_readiness_report(1)
            .production_ready
    );
}

#[test]
fn feature_recovery_validates_packed_page_layout() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "layout-feature".to_string(),
            points: vec![
                FeaturePoint {
                    timestamp_ms: 10,
                    value: b"ten".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 20,
                    value: b"twenty".to_vec(),
                },
            ],
        },
    });
    assert!(response.status.ok);

    let report = engine.storage_recovery_report(1);
    assert_eq!(report.feature_page_layout.indexed_feature_points, 2);
    assert_eq!(report.feature_page_layout.unique_feature_page_refs, 1);
    assert_eq!(report.feature_page_layout.packed_feature_pages, 1);
    assert_eq!(report.feature_page_layout.legacy_feature_value_pages, 0);
    assert!(report
        .feature_page_layout
        .corrupt_packed_feature_pages
        .is_empty());
    assert!(report
        .feature_page_layout
        .missing_indexed_timestamps
        .is_empty());
    assert!(report
        .feature_page_layout
        .orphan_packed_timestamps
        .is_empty());
}

#[test]
fn feature_recovery_reports_index_timestamp_missing_from_packed_page() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "layout-feature".to_string(),
            points: vec![
                FeaturePoint {
                    timestamp_ms: 10,
                    value: b"ten".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 20,
                    value: b"twenty".to_vec(),
                },
            ],
        },
    });
    assert!(response.status.ok);

    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let series = shards
            .get_mut(&1)
            .and_then(|shard| shard.features.get_mut("layout-feature"))
            .expect("feature series should exist");
        let address = series.get(&10).expect("packed page").clone();
        series.insert(30, address);
    }

    let report = engine.storage_recovery_report(1);
    assert_eq!(
        report
            .feature_page_layout
            .missing_indexed_timestamps
            .iter()
            .map(|mismatch| mismatch.timestamp_ms)
            .collect::<Vec<_>>(),
        vec![30]
    );
    let readiness = engine.storage_production_readiness_report(1);
    assert!(readiness
        .blockers
        .contains(&"feature_page_layout_mismatch".to_string()));
    assert_eq!(readiness.feature_page_layout_mismatch_count, 1);
}

#[test]
fn feature_recovery_reports_packed_timestamp_orphaned_from_index() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "layout-feature".to_string(),
            points: vec![
                FeaturePoint {
                    timestamp_ms: 10,
                    value: b"ten".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 20,
                    value: b"twenty".to_vec(),
                },
            ],
        },
    });
    assert!(response.status.ok);

    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let series = shards
            .get_mut(&1)
            .and_then(|shard| shard.features.get_mut("layout-feature"))
            .expect("feature series should exist");
        series.remove(&20);
    }

    let report = engine.storage_recovery_report(1);
    assert_eq!(
        report
            .feature_page_layout
            .orphan_packed_timestamps
            .iter()
            .map(|mismatch| mismatch.timestamp_ms)
            .collect::<Vec<_>>(),
        vec![20]
    );
    let readiness = engine.storage_production_readiness_report(1);
    assert!(readiness
        .blockers
        .contains(&"feature_page_layout_mismatch".to_string()));
    assert_eq!(readiness.feature_page_layout_mismatch_count, 1);
}

#[test]
fn feature_recovery_reports_duplicate_timestamps_inside_packed_page() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let duplicate_page = encode_feature_page(&[
        FeaturePoint {
            timestamp_ms: 10,
            value: b"ten".to_vec(),
        },
        FeaturePoint {
            timestamp_ms: 10,
            value: b"ten-duplicate".to_vec(),
        },
        FeaturePoint {
            timestamp_ms: 20,
            value: b"twenty".to_vec(),
        },
    ]);
    let address = engine
        .block_store()
        .append_with_page_metadata(
            &duplicate_page,
            Some(stable_page_object_id(1, "feature", "layout-feature", None)),
            Some(page_routing_bucket("layout-feature", 0, u32::MAX)),
        )
        .expect("duplicate packed page append");

    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("loaded shard");
        let series = shard
            .features
            .entry("layout-feature".to_string())
            .or_default();
        series.insert(10, address.clone());
        series.insert(20, address);
    }

    let report = engine.storage_recovery_report(1);
    assert_eq!(
        report
            .feature_page_layout
            .duplicate_packed_timestamps
            .iter()
            .map(|mismatch| mismatch.timestamp_ms)
            .collect::<Vec<_>>(),
        vec![10]
    );
    assert!(report
        .feature_page_layout
        .missing_indexed_timestamps
        .is_empty());
    assert!(report
        .feature_page_layout
        .orphan_packed_timestamps
        .is_empty());
    let readiness = engine.storage_production_readiness_report(1);
    assert!(readiness
        .blockers
        .contains(&"feature_page_layout_mismatch".to_string()));
    assert_eq!(readiness.feature_page_layout_mismatch_count, 1);
}

#[test]
fn feature_recovery_reports_corrupt_packed_timestamped_page() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let mut corrupt_page = FEATURE_PAGE_MAGIC.to_vec();
    corrupt_page.extend_from_slice(br#"{"version":1,"points":"not-a-point-list"}"#);
    let address = engine
        .block_store()
        .append_with_page_metadata(
            &corrupt_page,
            Some(stable_page_object_id(1, "feature", "corrupt-feature", None)),
            Some(page_routing_bucket("corrupt-feature", 0, u32::MAX)),
        )
        .expect("corrupt packed page append");

    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("loaded shard");
        shard
            .features
            .entry("corrupt-feature".to_string())
            .or_default()
            .insert(10, address);
    }

    let query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery {
            key: "corrupt-feature".to_string(),
            start_ms: 0,
            end_ms: 20,
            count: None,
        },
    });
    assert_eq!(
        query.response,
        CommandResponse::FeaturePoints { points: vec![] }
    );

    let readiness = engine.storage_production_readiness_report(1);
    assert!(!readiness.production_ready);
    assert!(readiness
        .blockers
        .contains(&"feature_page_layout_mismatch".to_string()));
    assert_eq!(readiness.corrupt_feature_page_count, 1);
    assert!(
        readiness.feature_page_layout.corrupt_packed_feature_pages[0]
            .error
            .contains("invalid packed feature page payload")
    );
}

#[test]
fn feature_recovery_reports_unsupported_packed_timestamped_page_version() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let page = PackedFeaturePage {
        version: 2,
        points: vec![FeaturePoint {
            timestamp_ms: 10,
            value: b"ten".to_vec(),
        }],
    };
    let mut bytes = FEATURE_PAGE_MAGIC.to_vec();
    bytes.extend_from_slice(&serde_json::to_vec(&page).unwrap());
    let address = engine
        .block_store()
        .append_with_page_metadata(
            &bytes,
            Some(stable_page_object_id(
                1,
                "feature",
                "versioned-feature",
                None,
            )),
            Some(page_routing_bucket("versioned-feature", 0, u32::MAX)),
        )
        .expect("unsupported packed page append");

    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("loaded shard");
        shard
            .features
            .entry("versioned-feature".to_string())
            .or_default()
            .insert(10, address);
    }

    let readiness = engine.storage_production_readiness_report(1);
    assert!(!readiness.production_ready);
    assert_eq!(readiness.corrupt_feature_page_count, 1);
    assert!(
        readiness.feature_page_layout.corrupt_packed_feature_pages[0]
            .error
            .contains("unsupported packed feature page version 2")
    );
}

#[test]
fn feature_compaction_rewrites_shared_packed_page_once() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "compact-packed-feature".to_string(),
            points: vec![
                FeaturePoint {
                    timestamp_ms: 10,
                    value: b"ten".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 20,
                    value: b"twenty".to_vec(),
                },
            ],
        },
    });
    assert!(response.status.ok);

    let before = engine.storage_recovery_report(1);
    assert_eq!(before.total_page_refs, 1);
    let report = engine.compact_shard_pages(1).unwrap();
    assert_eq!(report.rewritten_page_refs, 1);
    assert_eq!(report.after.live_page_refs, 1);

    let (first_address, second_address) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let series = shards
            .get(&1)
            .and_then(|shard| shard.features.get("compact-packed-feature"))
            .expect("feature series should exist");
        (
            series.get(&10).expect("first point").clone(),
            series.get(&20).expect("second point").clone(),
        )
    };
    assert_eq!(first_address, second_address);

    let query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery {
            key: "compact-packed-feature".to_string(),
            start_ms: 0,
            end_ms: 30,
            count: None,
        },
    });
    assert_eq!(
        query.response,
        CommandResponse::FeaturePoints {
            points: vec![
                FeaturePoint {
                    timestamp_ms: 10,
                    value: b"ten".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 20,
                    value: b"twenty".to_vec(),
                },
            ]
        }
    );
    let after = engine.storage_recovery_report(1);
    assert_eq!(after.total_page_refs, 1);
    assert_eq!(after.object_lifecycle.live_page_refs, 1);
    assert_eq!(after.object_lifecycle.reused_object_id_conflicts, 0);
}

#[test]
fn feature_append_rejects_hard_size_limit_before_mutation() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "huge-feature".to_string(),
            points: vec![FeaturePoint {
                timestamp_ms: 1,
                value: b"kept".to_vec(),
            }],
        },
    });

    let oversized_points = (0..FEATURE_ADD_HARD_MAX_SIZE)
        .map(|offset| FeaturePoint {
            timestamp_ms: 10 + offset as u64,
            value: b"x".to_vec(),
        })
        .collect::<Vec<_>>();
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "huge-feature".to_string(),
            points: oversized_points,
        },
    });
    assert_eq!(response.status.ok, false);
    assert_eq!(response.status.code, "invalid_argument");
    assert!(response
        .status
        .message
        .contains("huge-feature size bigger than 100000"));

    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery {
            key: "huge-feature".to_string(),
            start_ms: 0,
            end_ms: u64::MAX,
            count: Some(10),
        },
    });
    assert_eq!(
        response.response,
        CommandResponse::FeaturePoints {
            points: vec![FeaturePoint {
                timestamp_ms: 1,
                value: b"kept".to_vec(),
            }]
        }
    );
}

#[test]
fn native_feature_sequence_golden_corpus_passes() {
    let report = native_feature_sequence_golden_corpus_report();
    assert_eq!(report.corpus, "feature_sequence_proto_v1");
    assert_eq!(report.total_cases, 8);
    assert_eq!(report.passed_cases, report.total_cases);
    assert_eq!(report.failed_cases, 0);
    assert!(report.passed(), "{report:#?}");
}

#[test]
fn native_api_golden_corpus_passes() {
    let report = native_api_golden_corpus_report();
    assert_eq!(report.corpus, "native_api_golden_corpus_v1");
    assert_eq!(report.total_cases, 14);
    assert!(report.passed(), "{report:#?}");
    assert_eq!(report.passed_cases, report.total_cases);
    assert_eq!(report.failed_cases, 0);
}

#[test]
fn feature_replace_delete_and_agg_query() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "f".to_string(),
            points: vec![
                FeaturePoint {
                    timestamp_ms: 10,
                    value: b"2".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 20,
                    value: b"3".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 30,
                    value: b"4".to_vec(),
                },
            ],
        },
    });
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAggQuery {
                    key: "f".to_string(),
                    start_ms: 0,
                    end_ms: 40,
                    aggregator: "sum".to_string(),
                    count: None,
                },
            })
            .response,
        CommandResponse::Aggregate { value: 9 }
    );
    for (aggregator, count, expected) in [
        ("avg", None, 3),
        ("first", None, 2),
        ("last", None, 4),
        ("events", None, 3),
        ("last", Some(2), 3),
    ] {
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::FeatureAggQuery {
                        key: "f".to_string(),
                        start_ms: 0,
                        end_ms: 40,
                        aggregator: aggregator.to_string(),
                        count,
                    },
                })
                .response,
            CommandResponse::Aggregate { value: expected },
            "{aggregator} aggregate should Match window semantics"
        );
    }
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAggQuery {
                    key: "f".to_string(),
                    start_ms: 100,
                    end_ms: 200,
                    aggregator: "avg".to_string(),
                    count: None,
                },
            })
            .response,
        CommandResponse::Aggregate { value: 0 }
    );
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureReplace {
            key: "f".to_string(),
            start_ms: 0,
            end_ms: 20,
            points: vec![FeaturePoint {
                timestamp_ms: 15,
                value: b"10".to_vec(),
            }],
        },
    });
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAggQuery {
                    key: "f".to_string(),
                    start_ms: 0,
                    end_ms: 40,
                    aggregator: "sum".to_string(),
                    count: None,
                },
            })
            .response,
        CommandResponse::Aggregate { value: 14 }
    );
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureDelete {
            key: "f".to_string(),
        },
    });
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAggQuery {
                    key: "f".to_string(),
                    start_ms: 0,
                    end_ms: 40,
                    aggregator: "count".to_string(),
                    count: None,
                },
            })
            .response,
        CommandResponse::Aggregate { value: 0 }
    );
}

#[test]
fn common_delete_removes_all_data_types_for_key() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SetAdd {
            key: "k".to_string(),
            member: b"m".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CommonDelete {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "k".to_string()
                },
            })
            .response,
        CommandResponse::Bytes { value: None }
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::SetMembers {
                    key: "k".to_string()
                },
            })
            .response,
        CommandResponse::Members {
            members: Vec::new()
        }
    );
}

#[test]
fn common_delete_removes_control_state_family_records_for_logical_key() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for (family, amount) in [
        (ControlStateFamily::Counter, 5),
        (ControlStateFamily::Distinct, 7),
        (ControlStateFamily::Selection, 11),
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateSet {
                family,
                key: "control_state-native".to_string(),
                timestamp_ms: 10,
                amount,
            },
        });
        assert!(response.status.ok, "{response:?}");
    }

    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonExists {
                    key: "control_state-native".to_string(),
                },
            })
            .response,
        CommandResponse::Integer { value: 1 }
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonDelete {
                    key: "control_state-native".to_string(),
                },
            })
            .response,
        CommandResponse::Empty
    );
    for family in [ControlStateFamily::Counter, ControlStateFamily::Distinct, ControlStateFamily::Selection] {
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::ControlStateFamilyQuery {
                        family,
                        key: "control_state-native".to_string(),
                        start_ms: 0,
                        end_ms: 20,
                        aggregator: "sum".to_string(),
                    },
                })
                .response,
            CommandResponse::Integer { value: 0 }
        );
    }
}

#[test]
fn common_expire_and_ttl_work() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CommonExpire {
            key: "k".to_string(),
            ttl_ms: 0,
        },
    });
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonTtl {
                    key: "k".to_string()
                },
            })
            .response,
        CommandResponse::Integer { value: -2 }
    );
}

#[test]
fn common_expire_missing_key_matches_not_found() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CommonExpire {
            key: "missing".to_string(),
            ttl_ms: 1000,
        },
    });
    assert_eq!(response.status.code, "not_found");
}

#[test]
fn common_expire_and_ttl_cover_control_state_family_records_for_logical_key() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ControlStateSet {
            family: ControlStateFamily::Distinct,
            key: "control_state-expire".to_string(),
            timestamp_ms: 10,
            amount: 3,
        },
    });
    assert!(response.status.ok, "{response:?}");

    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonTtl {
                    key: "control_state-expire".to_string(),
                },
            })
            .response,
        CommandResponse::Integer { value: -1 }
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonExpire {
                    key: "control_state-expire".to_string(),
                    ttl_ms: 0,
                },
            })
            .status
            .ok
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonTtl {
                    key: "control_state-expire".to_string(),
                },
            })
            .response,
        CommandResponse::Integer { value: -2 }
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ControlStateFamilyQuery {
                    family: ControlStateFamily::Distinct,
                    key: "control_state-expire".to_string(),
                    start_ms: 0,
                    end_ms: 20,
                    aggregator: "sum".to_string(),
                },
            })
            .response,
        CommandResponse::Integer { value: 0 }
    );
}

#[test]
fn long_sequence_query_keeps_timestamp_order_and_applies_random_filters() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let base_ts = 1_700_000_000_000_u64;
    let row_count = 5_000_u64;
    let key = "long-sequence".to_string();

    let ordered_rows = (0..row_count)
        .map(|offset| SequenceFeatureRow {
            timestamp_ms: base_ts + offset,
            gid: 10_000 + offset,
            action_type: (offset % 7) as u32,
            duration: (50 + (offset * 37) % 1_000) as u32,
            author_id: 500 + (offset * 17) % 97,
        })
        .collect::<Vec<_>>();
    let shuffled_rows = (0..row_count)
        .map(|i| ordered_rows[((i * 2_919) % row_count) as usize].clone())
        .collect::<Vec<_>>();

    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SequenceAdd {
            key: key.clone(),
            rows: shuffled_rows,
        },
    });

    for seed in 0..20_u64 {
        let start_offset = (seed * 313) % 4_400;
        let end_offset = (start_offset + 250 + (seed * 97) % 700).min(row_count - 1);
        let count = 25 + (seed as usize % 40);
        let filters = vec![
            FeatureFilter {
                field: "action_type".to_string(),
                op: FeatureFilterOp::NotEqual,
                value: seed % 7,
            },
            FeatureFilter {
                field: "duration".to_string(),
                op: FeatureFilterOp::GreaterOrEqual,
                value: 100 + (seed * 29) % 500,
            },
            FeatureFilter {
                field: "author_id".to_string(),
                op: FeatureFilterOp::LessOrEqual,
                value: 560 + (seed * 11) % 30,
            },
        ];

        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SequenceQuery {
                key: key.clone(),
                start_ms: base_ts + start_offset,
                end_ms: base_ts + end_offset,
                count,
                filters: filters.clone(),
            },
        });
        let CommandResponse::SequenceRows { rows } = response.response else {
            panic!("expected sequence rows");
        };
        let expected = ordered_rows
            .iter()
            .filter(|row| row.timestamp_ms >= base_ts + start_offset)
            .filter(|row| row.timestamp_ms <= base_ts + end_offset)
            .take(count)
            .filter(|row| {
                filters
                    .iter()
                    .all(|filter| sequence_filter_matches(row, filter))
            })
            .cloned()
            .collect::<Vec<_>>();

        assert_eq!(rows, expected, "seed {seed}");
        assert!(rows
            .windows(2)
            .all(|pair| pair[0].timestamp_ms < pair[1].timestamp_ms));
        assert!(rows.len() <= count);
    }
}

#[test]
fn control_state_count_sums_window() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for (timestamp_ms, amount) in [(10, 1), (20, 2), (30, 4)] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateIncrement {
                key: "control_state".to_string(),
                timestamp_ms,
                amount,
            },
        });
    }
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ControlStateCount {
                    key: "control_state".to_string(),
                    start_ms: 15,
                    end_ms: 30,
                },
            })
            .response,
        CommandResponse::Integer { value: 6 }
    );
}

// Bounded distinct: small distinct sets stay exact (gate on or off); a high-cardinality set is
// converted to a fixed-size HLL sketch past the threshold, so the count becomes a bounded
// estimate (within tolerance) instead of an unbounded exact set.
#[test]
fn control_state_distinct_sketch_bounds_high_cardinality() {
    fn distinct_count(gate: bool, n: usize) -> i64 {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        if gate {
            let mut config = Config {
                version: 2,
                ..Config::default()
            };
            config
                .extend_config
                .insert("control_distinct_sketch".to_string(), "on".to_string());
            assert!(engine.set_config(SetConfigRequest { shard_id: 1, config }).ok);
        }
        for i in 0..n {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ControlStateChangeAdd {
                    key: "d".to_string(),
                    timestamp_ms: 1000,
                    value: format!("v{i}").into_bytes(),
                    precision_ms: None,
                    ttl_ms: None,
                },
            });
        }
        match engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ControlStateQuery {
                    key: "d".to_string(),
                    start_ms: 0,
                    end_ms: 10_000,
                    aggregator: "change".to_string(),
                },
            })
            .response
        {
            CommandResponse::Integer { value } => value,
            other => panic!("expected Integer, got {other:?}"),
        }
    }
    // Below the threshold: exact regardless of the gate.
    assert_eq!(distinct_count(true, 100), 100);
    assert_eq!(distinct_count(false, 100), 100);
    // Above the threshold (4096): exact path stays exact; sketch path is a bounded estimate.
    let n = 5_000usize;
    assert_eq!(distinct_count(false, n), n as i64, "exact distinct must be exact");
    let approx = distinct_count(true, n);
    let err = ((approx - n as i64).abs() as f64) / n as f64;
    assert!(err < 0.05, "sketch estimate {approx} not within 5% of {n}");
}

// The bounded distinct sketch must be durable: after conversion + flush, the estimate survives a
// restart (loaded from the index snapshot), not just the exact sets.
#[test]
fn control_state_distinct_sketch_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-a"),
        &page_dir,
        &index_dir,
    );
    engine.load_shard(1);
    // ChangeAdd is in-memory (no per-write page); durability here comes from the index-snapshot
    // flush, so sync mode is enough and avoids a per-write WAL fsync for every distinct value.
    let mut config = Config {
        version: 2,
        ..Config::default()
    };
    config
        .extend_config
        .insert("control_distinct_sketch".to_string(), "on".to_string());
    assert!(engine.set_config(SetConfigRequest { shard_id: 1, config }).ok);
    let n = 5_000usize; // just over the 4096 threshold to force conversion to a sketch
    for i in 0..n {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateChangeAdd {
                key: "d".to_string(),
                timestamp_ms: 1000,
                value: format!("v{i}").into_bytes(),
                precision_ms: None,
                ttl_ms: None,
            },
        });
    }
    engine.flush_shard_index(1); // checkpoint the sketch into the durable index snapshot
    drop(engine);

    let restarted = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-b"),
        &page_dir,
        &index_dir,
    );
    restarted.load_shard(1);
    let value = match restarted
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateQuery {
                key: "d".to_string(),
                start_ms: 0,
                end_ms: 10_000,
                aggregator: "change".to_string(),
            },
        })
        .response
    {
        CommandResponse::Integer { value } => value,
        other => panic!("expected Integer, got {other:?}"),
    };
    let err = ((value - n as i64).abs() as f64) / n as f64;
    assert!(err < 0.05, "restored sketch estimate {value} not within 5% of {n}");
}

// The gated feature-aggregate rollup (FeatureAggQuery routed through the shared rollup
// primitive) must return exactly the same sum-family window aggregates as the ungated raw
// scan. Per-point values are folded via aggregate_feature_values, so the rollup is exact.
#[test]
fn feature_agg_rollup_matches_raw_scan() {
    fn run(rollup_gate: bool) -> (Vec<i64>, u64) {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let mut config = Config {
            version: 2,
            feature_max_size: 30_000,
            ..Config::default()
        };
        if rollup_gate {
            config
                .extend_config
                .insert("control_rollup".to_string(), "on".to_string());
        }
        assert!(engine.set_config(SetConfigRequest { shard_id: 1, config }).ok);
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            state
        };
        let mut points = Vec::new();
        let mut ts = 1u64;
        for _ in 0..5000 {
            ts += 1 + (next() % 60_000); // strictly increasing, distinct-ms, spread over ~days
            let v = (next() % 200) as i64; // 0..=199
            points.push(FeaturePoint {
                timestamp_ms: ts,
                value: v.to_string().into_bytes(),
            });
        }
        let end = ts;
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "f".to_string(),
                points,
            },
        });
        let windows = [
            (0u64, 999u64),
            (0, 60_000),
            (0, 3_600_000),
            (0, 86_400_000),
            (end / 2, end),
            (0, end),
        ];
        let mut out = Vec::new();
        for (start_ms, end_ms) in windows {
            for aggregator in ["", "sum", "count"] {
                if let CommandResponse::Aggregate { value } = engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::FeatureAggQuery {
                            key: "f".to_string(),
                            start_ms,
                            end_ms,
                            aggregator: aggregator.to_string(),
                            count: None,
                        },
                    })
                    .response
                {
                    out.push(value);
                }
            }
        }
        (out, end)
    }
    let (with_rollup, _) = run(true);
    let (raw, _) = run(false);
    assert_eq!(with_rollup, raw, "feature-aggregate rollup diverged from raw scan");
    assert!(raw.iter().any(|value| *value != 0), "test series should be non-trivial");
}

// The gated control-state rollup ladder must return exactly the same sum-family window
// aggregates as the ungated raw scan, through the real execute() path. Functional-conformance
// gate for the O(levels) long-window control-state path (frequency caps / counts).
#[test]
fn control_state_rollup_matches_raw_scan_through_engine() {
    fn run(rollup_gate: bool) -> Vec<i64> {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        if rollup_gate {
            let mut config = Config {
                version: 2,
                ..Config::default()
            };
            config
                .extend_config
                .insert("control_rollup".to_string(), "on".to_string());
            assert!(
                engine
                    .set_config(SetConfigRequest {
                        shard_id: 1,
                        config,
                    })
                    .ok
            );
        }
        let mut state: u64 = 0x2545_F491_4F6C_DD1D;
        let mut next = || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            state
        };
        for _ in 0..1500 {
            let timestamp_ms = next() % (3 * 86_400_000);
            let amount = (next() % 21) as i64 - 10; // -10..=10, incl. negatives and zero
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ControlStateIncrement {
                    key: "cs".to_string(),
                    timestamp_ms,
                    amount,
                },
            });
        }
        let windows = [
            (0u64, 999u64),
            (0, 60_000),
            (500, 3_600_000),
            (0, 86_400_000),
            (1_000, 7 * 86_400_000),
            (37_000_000, 91_000_000),
            (0, 3 * 86_400_000),
        ];
        let mut out = Vec::new();
        for (start_ms, end_ms) in windows {
            for aggregator in ["", "sum", "count"] {
                if let CommandResponse::Integer { value } = engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::ControlStateQuery {
                            key: "cs".to_string(),
                            start_ms,
                            end_ms,
                            aggregator: aggregator.to_string(),
                        },
                    })
                    .response
                {
                    out.push(value);
                }
            }
            if let CommandResponse::Integer { value } = engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::ControlStateCount {
                        key: "cs".to_string(),
                        start_ms,
                        end_ms,
                    },
                })
                .response
            {
                out.push(value);
            }
        }
        out
    }
    let with_rollup = run(true);
    let raw = run(false);
    assert_eq!(with_rollup, raw, "rollup path diverged from raw scan");
    assert!(raw.iter().any(|value| *value != 0), "test series should be non-trivial");
}

// MANAGER op-code conformance: QUERY(2) / FIELD_LIST(5) / FIELD_COUNT(6) / ALL_DATA_VALUE(7)
// over the H family series, plus the default summary path.
#[test]
fn control_state_manager_op_codes_match_hash_manager() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for (timestamp_ms, amount) in [(10u64, 1i64), (20, 2), (30, 4)] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateSet {
                family: ControlStateFamily::Counter,
                key: "cs".to_string(),
                timestamp_ms,
                amount,
            },
        });
    }
    let manager = |op_type: Option<&str>,
                   field_list: Vec<(String, String)>,
                   start_offset: &str,
                   end_offset: &str| {
        match engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ControlStateManager {
                    key: "cs".to_string(),
                    op_type: op_type.map(str::to_string),
                    field_list,
                    start_offset: start_offset.to_string(),
                    end_offset: end_offset.to_string(),
                    is_distinct: false,
                },
            })
            .response
        {
            CommandResponse::HashEntries { entries } => entries,
            other => panic!("expected HashEntries, got {other:?}"),
        }
    };
    let text = |entries: &[(String, Vec<u8>)], field: &str| -> Option<String> {
        entries
            .iter()
            .find(|(name, _)| name == field)
            .map(|(_, value)| String::from_utf8_lossy(value).into_owned())
    };

    // FIELD_COUNT(6) -> number of buckets in the H series.
    assert_eq!(
        text(&manager(Some("6"), Vec::new(), "", ""), "size").as_deref(),
        Some("3")
    );
    // FIELD_LIST(5) over an inclusive [start,end] -> comma-joined timestamps, ascending.
    assert_eq!(
        text(&manager(Some("FIELD_LIST"), Vec::new(), "0", "100"), "key_list").as_deref(),
        Some("10,20,30")
    );
    // FIELD_LIST bounded to [15,25] -> only the ts=20 bucket.
    assert_eq!(
        text(&manager(Some("5"), Vec::new(), "15", "25"), "key_list").as_deref(),
        Some("20")
    );
    // QUERY(2) exact field lookups.
    let q = manager(
        Some("2"),
        vec![("20".to_string(), String::new()), ("30".to_string(), String::new())],
        "",
        "",
    );
    assert_eq!(text(&q, "20").as_deref(), Some("2"));
    assert_eq!(text(&q, "30").as_deref(), Some("4"));
    // QUERY for an absent field yields no entry.
    assert!(manager(Some("2"), vec![("99".to_string(), String::new())], "", "").is_empty());
    // ALL_DATA_VALUE(7) -> every (timestamp, value) pair.
    let all = manager(Some("7"), Vec::new(), "", "");
    assert_eq!(all.len(), 3);
    assert_eq!(text(&all, "10").as_deref(), Some("1"));
    assert_eq!(text(&all, "30").as_deref(), Some("4"));
    // Default (no op_type) still returns the cross-family summary.
    let summary = manager(None, Vec::new(), "", "");
    assert_eq!(text(&summary, "h_events").as_deref(), Some("3"));
    assert_eq!(text(&summary, "h_sum").as_deref(), Some("7"));
}

// shared-corpus: engine_lifecycle_load_config_membership_unload
#[test]
fn control_api_load_config_info_stats_membership_and_unload() {
    let engine = TemporalEngine::default();
    assert_eq!(
        engine.set_config(SetConfigRequest {
            shard_id: 7,
            config: Config {
                version: 2,
                feature_max_size: 123,
                ..Config::default()
            },
        }),
        Status::error("shard_not_found", "shard is not loaded")
    );
    assert_eq!(engine.get_config(7).status.code, "shard_not_found");
    assert!(
        engine
            .load_shard_with(LoadShardRequest {
                shard_id: 7,
                load_version: 42,
                local_node_id: Some(2),
                shard_uri: "file:///tmp/shard-7".to_string(),
                start_routing_bucket: 10,
                end_routing_bucket: 20,
                readonly: false,
                table_name: "table".to_string(),
            })
            .status
            .ok
    );
    let duplicate_load = engine.load_shard_with(LoadShardRequest {
        shard_id: 7,
        load_version: 43,
        local_node_id: Some(2),
        shard_uri: "file:///tmp/shard-7-duplicate".to_string(),
        start_routing_bucket: 10,
        end_routing_bucket: 20,
        readonly: false,
        table_name: "table".to_string(),
    });
    assert!(!duplicate_load.status.ok);
    assert_eq!(duplicate_load.status.code, "already_exists");
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 7,
                config: Config {
                    version: 2,
                    feature_max_size: 123,
                    maxmemory_bytes: Some(3000),
                    extend_config: BTreeMap::from([(
                        "test_config".to_string(),
                        "test_value".to_string(),
                    )]),
                    ..Config::default()
                },
            })
            .ok
    );
    let config = engine.get_config(7).config;
    assert_eq!(config.feature_max_size, 123);
    assert_eq!(config.maxmemory_bytes, Some(3000));
    assert_eq!(
        config.extend_config.get("test_config"),
        Some(&"test_value".to_string())
    );
    assert_eq!(
        engine.set_config(SetConfigRequest {
            shard_id: 7,
            config: Config {
                version: 1,
                feature_max_size: 456,
                ..Config::default()
            },
        }),
        Status::error("failed_precondition", "legacy config version")
    );
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 7,
                config: Config {
                    version: 2,
                    feature_max_size: 456,
                    ..Config::default()
                },
            })
            .ok
    );
    assert_eq!(engine.get_config(7).config.feature_max_size, 123);
    assert!(
        engine
            .update_membership(MembershipUpdateRequest {
                shard_id: 7,
                membership_version: 3,
                replica_membership_version: 4,
                replica_node_ids: vec![1, 2, 3],
                leader_node_id: Some(1),
            })
            .ok
    );
    let info = engine.get_info(7).info.unwrap();
    assert_eq!(info.load_version, 42);
    assert_eq!(info.replica_node_ids, vec![1, 2, 3]);
    assert_eq!(info.membership_version, 3);
    assert_eq!(info.replica_membership_version, 4);
    assert!(info.membership_valid);
    assert_eq!(
        engine.update_membership(MembershipUpdateRequest {
            shard_id: 7,
            membership_version: 2,
            replica_membership_version: 5,
            replica_node_ids: vec![1, 3],
            leader_node_id: Some(1),
        }),
        Status::error("failed_precondition", "legacy membership info")
    );
    assert_eq!(
        engine.update_membership(MembershipUpdateRequest {
            shard_id: 7,
            membership_version: 3,
            replica_membership_version: 3,
            replica_node_ids: vec![1, 3],
            leader_node_id: Some(1),
        }),
        Status::error("failed_precondition", "legacy membership unit info")
    );
    assert!(
        engine
            .update_membership(MembershipUpdateRequest {
                shard_id: 7,
                membership_version: 4,
                replica_membership_version: 5,
                replica_node_ids: vec![1, 3],
                leader_node_id: Some(1),
            })
            .ok
    );
    let info = engine.get_info(7).info.unwrap();
    assert_eq!(info.replica_node_ids, vec![1, 3]);
    assert!(!info.membership_valid);

    engine.execute(ExecuteRequest {
        shard_id: 7,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    let stats = engine.get_stats(7).stats.unwrap();
    assert_eq!(stats.string_records, 1);
    assert_eq!(stats.total_records, 1);
    assert_eq!(stats.load_version, 42);
    assert!(!stats.readonly);
    assert!(stats.storage_bytes > 0);
    assert_eq!(stats.block_store.writes, 1);

    assert!(
        engine
            .unload_shard_with(UnloadShardRequest { shard_id: 7 })
            .status
            .ok
    );
    let after_unload = engine.get_info(7);
    assert!(!after_unload.status.ok);
    assert_eq!(after_unload.status.code, "shard_not_found");
    assert_eq!(engine.get_config(7).status.code, "shard_not_found");
    let second_unload = engine.unload_shard_with(UnloadShardRequest { shard_id: 7 });
    assert!(!second_unload.status.ok);
    assert_eq!(second_unload.status.code, "shard_not_found");
}

// shared-corpus: engine_lifecycle_reload_metadata_readonly_stale_version
#[test]
fn engine_reload_shard_updates_metadata_and_rejects_stale_version() {
    let engine = TemporalEngine::default();
    assert!(
        engine
            .load_shard_with(LoadShardRequest {
                shard_id: 7,
                load_version: 42,
                local_node_id: Some(2),
                shard_uri: "file:///tmp/shard-7".to_string(),
                start_routing_bucket: 10,
                end_routing_bucket: 20,
                readonly: false,
                table_name: "old_table".to_string(),
            })
            .status
            .ok
    );
    assert!(
        engine
            .update_membership(MembershipUpdateRequest {
                shard_id: 7,
                membership_version: 3,
                replica_membership_version: 4,
                replica_node_ids: vec![1, 2, 3],
                leader_node_id: Some(1),
            })
            .ok
    );

    let stale = engine.reload_shard_with(LoadShardRequest {
        shard_id: 7,
        load_version: 41,
        local_node_id: Some(9),
        shard_uri: "file:///tmp/stale".to_string(),
        start_routing_bucket: 100,
        end_routing_bucket: 200,
        readonly: true,
        table_name: "stale_table".to_string(),
    });
    assert!(!stale.status.ok);
    assert_eq!(stale.status.code, "stale_load_version");
    let unchanged = engine.get_info(7).info.unwrap();
    assert_eq!(unchanged.load_version, 42);
    assert_eq!(unchanged.table_name, "old_table");
    assert!(!unchanged.readonly);

    let reload = engine.reload_shard_with(LoadShardRequest {
        shard_id: 7,
        load_version: 43,
        local_node_id: Some(9),
        shard_uri: "file:///tmp/shard-7-reloaded".to_string(),
        start_routing_bucket: 100,
        end_routing_bucket: 200,
        readonly: true,
        table_name: "new_table".to_string(),
    });
    assert!(reload.status.ok, "{reload:?}");
    let info = engine.get_info(7).info.unwrap();
    assert_eq!(info.load_version, 43);
    assert_eq!(info.local_node_id, Some(9));
    assert_eq!(info.table_name, "new_table");
    assert_eq!(info.start_routing_bucket, 100);
    assert_eq!(info.end_routing_bucket, 200);
    assert!(info.readonly);
    assert_eq!(info.replica_node_ids, vec![1, 2, 3]);
    assert_eq!(info.membership_version, 3);
    assert_eq!(info.replica_membership_version, 4);
    assert!(info.membership_valid);

    let write = engine.execute(ExecuteRequest {
        shard_id: 7,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert_eq!(write.status.code, "readonly_shard");
}

// After a crash that lost the un-synced served-index base + delta (only the WAL + config-log
// survive), WAL-tail replay must re-derive the feature_max_size trim rather than resurrecting the
// evicted points. The config-log is now persisted + restored UNCONDITIONALLY (not only under the
// single-barrier flag), so the trim is re-derived in every barrier mode -- previously, off the
// flag, config was not durable and the same crash resurrected the evicted points. This test pins
// the fixed behavior so the config-log's role is explicit.
#[test]
fn walonly_recovery_rederives_feature_trim_under_single_barrier() {
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache-a"),
        &page_dir,
        &index_dir,
    );
    engine.load_shard(1);
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    feature_max_size: 3,
                    ..Config::default()
                },
            })
            .ok
    );
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "f".to_string(),
            points: (1..=5)
                .map(|i| FeaturePoint {
                    timestamp_ms: i * 10,
                    value: vec![i as u8],
                })
                .collect(),
        },
    });
    let live = |e: &TemporalEngine| -> Vec<u64> {
        match e
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureQuery {
                    key: "f".to_string(),
                    start_ms: 0,
                    end_ms: u64::MAX,
                    count: Some(100),
                },
            })
            .response
        {
            CommandResponse::FeaturePoints { points } => {
                points.iter().map(|p| p.timestamp_ms).collect()
            }
            other => panic!("expected FeaturePoints, got {other:?}"),
        }
    };
    let before = live(&engine);
    assert_eq!(before, vec![30, 40, 50]);
    drop(engine);
    // Simulate a WAL-only crash: the served index base + delta were not fsync'd and are
    // lost; only the WAL and the data pages survive.
    std::fs::remove_file(index_dir.join("shard-1.index.json")).ok();
    if let Ok(rd) = std::fs::read_dir(index_dir.join("indexlogs")) {
        for e in rd.flatten() {
            std::fs::remove_file(e.path()).ok();
        }
    }
    let restarted = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache-b"),
        &page_dir,
        &index_dir,
    );
    restarted.load_shard(1);
    let after = live(&restarted);
    // The config-log is durable regardless of barrier mode, so WAL-only replay re-derives the
    // feature_max_size trim in every mode -- the evicted points stay evicted, no resurrection.
    assert_eq!(
        after, before,
        "WAL-only replay must re-derive the feature trim from the durable config-log (no resurrection)"
    );
    assert_eq!(after, vec![30, 40, 50]);
}

