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
            address.page_segment_id,
            address.offset,
            address.length,
            address.routing_slot,
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
            address.page_segment_id,
            address.offset,
            address.length,
            address.routing_slot,
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
            address.page_segment_id,
            address.offset,
            address.length,
            address.routing_slot,
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
    assert_eq!(engine.block_store().stats().writes, 0);
    assert_eq!(engine.write_ahead_log_store().stats(1).writes, 0);
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
fn hash_incrby_rejects_non_integer_and_overflow_like_cpp() {
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
                    value: second.encode_cpp_feature_value(),
                },
                FeaturePoint {
                    timestamp_ms: first.timestamp_ms,
                    value: first.encode_cpp_feature_value(),
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
                    value: first.encode_cpp_feature_value(),
                },
                FeaturePoint {
                    timestamp_ms: 20,
                    value: second.encode_cpp_feature_value(),
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
                value: second.encode_cpp_feature_value(),
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

// shared-corpus: feature_cpp_boundary_count_duplicate_semantics
#[test]
fn feature_cpp_policy_aliases_first_update_and_block() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    let base = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "cpp-policy-feature".to_string(),
            points: vec![FeaturePoint {
                timestamp_ms: 10,
                value: vec![b'a'],
            }],
        },
    });
    assert!(base.status.ok);

    let first_existing: Command = serde_json::from_value(serde_json::json!({
        "kind": "feature_append_with_policy",
        "key": "cpp-policy-feature",
        "policy": "FIRST",
        "points": [{"timestamp_ms": 10, "value": [120]}]
    }))
    .expect("C++ FIRST policy alias should deserialize");
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
        "key": "cpp-policy-feature",
        "policy": "FIRST",
        "points": [{"timestamp_ms": 20, "value": [98]}]
    }))
    .expect("C++ FIRST policy alias should deserialize");
    let first_new = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: first_new,
    });
    assert_eq!(first_new.response, CommandResponse::Integer { value: 1 });

    let update_missing: Command = serde_json::from_value(serde_json::json!({
        "kind": "feature_append_with_policy",
        "key": "cpp-policy-feature",
        "policy": "UPDATE",
        "points": [{"timestamp_ms": 30, "value": [99]}]
    }))
    .expect("C++ UPDATE policy alias should deserialize");
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
        "key": "cpp-policy-feature",
        "policy": "UPDATE",
        "points": [{"timestamp_ms": 10, "value": [65]}]
    }))
    .expect("C++ UPDATE policy alias should deserialize");
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
        "key": "cpp-policy-feature",
        "policy": "BLOCK",
        "points": [{"timestamp_ms": 40, "value": [100]}]
    }))
    .expect("C++ BLOCK policy alias should deserialize");
    let block = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: block,
    });
    assert!(!block.status.ok);
    assert_eq!(block.status.code, "invalid_argument");

    let query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery {
            key: "cpp-policy-feature".to_string(),
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
            Some(page_routing_slot("layout-feature", 0, u32::MAX)),
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
            Some(page_routing_slot("corrupt-feature", 0, u32::MAX)),
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
            Some(page_routing_slot("versioned-feature", 0, u32::MAX)),
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
fn feature_append_rejects_cpp_hard_size_limit_before_mutation() {
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
fn cpp_feature_sequence_golden_corpus_passes() {
    let report = cpp_feature_sequence_golden_corpus_report();
    assert_eq!(report.corpus, "feature_sequence_cpp_proto_v1");
    assert_eq!(report.total_cases, 8);
    assert_eq!(report.passed_cases, report.total_cases);
    assert_eq!(report.failed_cases, 0);
    assert!(report.passed(), "{report:#?}");
}

#[test]
fn cpp_api_golden_corpus_passes() {
    let report = cpp_api_golden_corpus_report();
    assert_eq!(report.corpus, "cpp_api_golden_corpus_v1");
    assert_eq!(report.total_cases, 16);
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
            "{aggregator} aggregate should match C++ window semantics"
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
fn common_delete_removes_cpp_risk_family_records_for_logical_key() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for (family, amount) in [
        (RiskFamily::H, 5),
        (RiskFamily::Cpc, 7),
        (RiskFamily::Fol, 11),
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::RiskSet {
                family,
                key: "risk-cpp".to_string(),
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
                    key: "risk-cpp".to_string(),
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
                    key: "risk-cpp".to_string(),
                },
            })
            .response,
        CommandResponse::Empty
    );
    for family in [RiskFamily::H, RiskFamily::Cpc, RiskFamily::Fol] {
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskFamilyQuery {
                        family,
                        key: "risk-cpp".to_string(),
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
fn common_expire_missing_key_matches_cpp_not_found() {
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
fn common_expire_and_ttl_cover_cpp_risk_family_records_for_logical_key() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::RiskSet {
            family: RiskFamily::Cpc,
            key: "risk-expire".to_string(),
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
                    key: "risk-expire".to_string(),
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
                    key: "risk-expire".to_string(),
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
                    key: "risk-expire".to_string(),
                },
            })
            .response,
        CommandResponse::Integer { value: -2 }
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::RiskFamilyQuery {
                    family: RiskFamily::Cpc,
                    key: "risk-expire".to_string(),
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
fn ips_query_last_returns_recent_instances() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for (timestamp_ms, value) in [(1, b"a".to_vec()), (2, b"b".to_vec()), (3, b"c".to_vec())] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::IpsAdd {
                key: "ips".to_string(),
                timestamp_ms,
                instance: value,
            },
        });
    }
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::IpsQueryLast {
                    key: "ips".to_string(),
                    count: 2,
                },
            })
            .response,
        CommandResponse::FeaturePoints {
            points: vec![
                FeaturePoint {
                    timestamp_ms: 3,
                    value: b"c".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 2,
                    value: b"b".to_vec(),
                }
            ]
        }
    );
}

#[test]
fn risk_count_sums_window() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for (timestamp_ms, amount) in [(10, 1), (20, 2), (30, 4)] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::RiskIncrement {
                key: "risk".to_string(),
                timestamp_ms,
                amount,
            },
        });
    }
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::RiskCount {
                    key: "risk".to_string(),
                    start_ms: 15,
                    end_ms: 30,
                },
            })
            .response,
        CommandResponse::Integer { value: 6 }
    );
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
                start_routing_slot: 10,
                end_routing_slot: 20,
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
        start_routing_slot: 10,
        end_routing_slot: 20,
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
                start_routing_slot: 10,
                end_routing_slot: 20,
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
        start_routing_slot: 100,
        end_routing_slot: 200,
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
        start_routing_slot: 100,
        end_routing_slot: 200,
        readonly: true,
        table_name: "new_table".to_string(),
    });
    assert!(reload.status.ok, "{reload:?}");
    let info = engine.get_info(7).info.unwrap();
    assert_eq!(info.load_version, 43);
    assert_eq!(info.local_node_id, Some(9));
    assert_eq!(info.table_name, "new_table");
    assert_eq!(info.start_routing_slot, 100);
    assert_eq!(info.end_routing_slot, 200);
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

