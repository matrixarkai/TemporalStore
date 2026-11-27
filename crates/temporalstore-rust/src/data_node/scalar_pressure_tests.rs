// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn assert_pressure(runtime: &DataNodeRuntime, engine: &TemporalEngine) {
    for detailed in [false, true] {
        let expected = engine.cache().stats();
        let (pressure, _) = runtime.storage_manager_pressure_snapshot_with_report_details(
            1, &StorageManagerOptions::default(), detailed,
        );
        assert_eq!(pressure.cache_memory_bytes, expected.memory_bytes);
        assert_eq!(pressure.cache_disk_bytes, expected.disk_bytes);
        assert_eq!(pressure.bucket_index_resident_bytes, engine.bucket_index_resident_bytes(1));
        assert_eq!(pressure.eviction_memory_pressure_bytes, pressure.bucket_index_resident_bytes);
        assert_eq!(pressure.memory_cache_pressure_score,
            expected.memory_bytes.saturating_add(expected.disk_bytes)
                .saturating_add(expected.async_writeback_queue_bytes)
                .saturating_add(expected.async_writeback_queue_depth)
                .saturating_add(pressure.bucket_index_resident_bytes));
    }
}

#[test]
fn scalar_pressure_matches_full_stats_with_real_pins_and_writeback_mutations() {
    let root = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20, root.path().join("cache"), root.path().join("blocks"),
        root.path().join("indexes"),
    );
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine.clone(),
        DataNodeRuntimeOptions { worker_threads: 0, max_queue_depth: 4, max_background_queue_depth: 2 },
    );
    assert_pressure(&runtime, &engine);
    let cache = engine.cache();
    let key = matrixcache::CacheKey::string(1, "resident");
    cache.put(key.clone(), vec![7; 64]).unwrap();
    let pin = cache.acquire(&key).unwrap().unwrap();
    assert!(cache.stats().pinned_entries > 0);
    assert_pressure(&runtime, &engine);
    let queued = matrixcache::CacheKey::string(1, "queued");
    cache.enqueue_async_writeback(queued.clone(), vec![3; 20]).unwrap();
    assert_pressure(&runtime, &engine);
    cache.enqueue_async_writeback(queued, vec![4; 37]).unwrap();
    let pending = cache.stats();
    assert_eq!(pending.async_writeback_queue_depth, 1);
    assert_eq!(pending.async_writeback_queue_bytes, 37);
    assert_pressure(&runtime, &engine);
    drop(pin);
    cache.remove(&key).unwrap();
    assert_pressure(&runtime, &engine);
}
