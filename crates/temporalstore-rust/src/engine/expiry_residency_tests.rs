// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

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

fn sweep(engine: &TemporalEngine, load_cold_buckets: bool) -> ShardExpirySweepReport {
    engine
        .sweep_expired_records_with_request(ShardExpirySweepRequest {
            shard_id: 1,
            max_hot_buckets_per_round: 8,
            max_cold_buckets_per_round: 8,
            load_cold_buckets,
            ..ShardExpirySweepRequest::default()
        })
        .unwrap()
}

fn live_value(index: usize) -> Vec<u8> {
    let mut value = vec![b'v'; 16];
    value[..8].copy_from_slice(&(index as u64).to_le_bytes());
    value
}

fn seed(engine: &TemporalEngine, commands: Vec<Command>) {
    for chunk in commands.chunks(1000) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        });
        assert!(response.status.ok, "{:?}", response.status);
    }
}

fn mixed_expiry_scale(live_counts: &[usize]) {
    const COLD: usize = 32;
    const HOT: usize = 4;
    let mut baseline_bytes = None;
    for &live in live_counts {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_at(dir.path(), "cache-a");
        seed(
            &engine,
            (0..live)
                .map(|index| Command::StringSet {
                    key: format!("live-{index:08}"),
                    value: live_value(index),
                })
                .collect(),
        );
        let cold_keys: Vec<_> = (0..COLD)
            .map(|index| format!("cold-due-{index:08}"))
            .collect();
        let cold_buckets: BTreeSet<_> = cold_keys
            .iter()
            .map(|key| engine.routing_bucket_for_key(1, key))
            .collect();
        let hot_keys: Vec<_> = (0..)
            .map(|index| format!("hot-due-{index:08}"))
            .filter(|key| !cold_buckets.contains(&engine.routing_bucket_for_key(1, key)))
            .take(HOT)
            .collect();
        seed(
            &engine,
            cold_keys
                .iter()
                .chain(&hot_keys)
                .map(|key| Command::StringSetEx {
                    key: key.clone(),
                    value: vec![b'd'; 16],
                    ttl_ms: 1,
                })
                .collect(),
        );
        let dump = engine.create_bucket_dump_manifest(1, []).unwrap();
        engine.clear_dumped_bucket_dirty_state(1, &dump);
        {
            let mut shards = engine.shards.write().unwrap();
            let shard = shards.get_mut(&1).unwrap();
            let buckets: Vec<_> = cold_buckets.iter().copied().collect();
            let result = storage_bucket_internals::release_bucket_blocks(shard, &buckets);
            assert_eq!(result.released_buckets, buckets);
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
        let before_sequence = engine.wal_store.cached_last_sequence(1);
        let before_index = engine.index_log_store.stats(1).bytes_written;
        let before_reads = engine.block_store.stats().bytes_read;
        storage_bucket_internals::reset_model_map_addresses_visited();
        CACHE_SWEEP_COUNTS.reset();
        CACHE_SWEEP_COUNTS.set_armed(false);
        let phases = &recovery_sweep_compact::EXPIRY_GUARD_NANOS;
        phases.reset();
        let started = std::time::Instant::now();
        let blocked = sweep(&engine, false);
        assert_eq!(blocked.expired_records_removed, HOT);
        assert_eq!(blocked.hot_buckets_scanned, HOT);
        assert_eq!(blocked.cold_buckets_scanned, 8);
        assert_eq!(blocked.skipped_records, 8);
        assert_eq!(blocked.loaded_for_expire, 0);
        {
            let shards = engine.shards.read().unwrap();
            let shard = &shards[&1];
            for key in &cold_keys {
                assert!(shard.strings().contains_key(key.as_str()));
            }
            for key in &hot_keys {
                assert!(!shard.strings().contains_key(key.as_str()));
            }
        }
        for _ in 0..COLD / 8 {
            let report = sweep(&engine, true);
            assert_eq!(report.expired_records_removed, 8);
            assert_eq!(report.hot_buckets_scanned, 0);
            assert_eq!(report.cold_buckets_scanned, 8);
            assert_eq!(report.loaded_for_expire, 0);
        }
        let elapsed_us = started.elapsed().as_micros();
        let (hold_ns, select_ns, delete_ns, invalidate_ns, wal_ns, checkpoint_ns) = phases.read();
        let (_, _, listings, _, cache_rows) = CACHE_SWEEP_COUNTS.read_batched();
        assert_eq!(storage_bucket_internals::model_map_addresses_visited(), 0);
        assert_eq!(engine.block_store.stats().bytes_read, before_reads);
        assert_eq!(
            engine.wal_store.cached_last_sequence(1) - before_sequence,
            (COLD + HOT) as u64
        );
        let index_bytes = engine.index_log_store.stats(1).bytes_written - before_index;
        assert!(index_bytes > 0);
        let baseline = *baseline_bytes.get_or_insert(index_bytes);
        assert!(
            index_bytes <= baseline + 4096,
            "expiry delta grew with live population: {index_bytes} versus {baseline}"
        );
        {
            let shards = engine.shards.read().unwrap();
            let shard = &shards[&1];
            assert_eq!(shard.strings().len(), live);
            assert!(shard.expires_at_ms().is_empty());
        }
        eprintln!("EXPIRY_RESIDENCY live={live} expired={} cold={COLD} hot={HOT} index_bytes={index_bytes} model_visits=0 payload_reads=0 elapsed_us={elapsed_us}", COLD + HOT);
        eprintln!("EXPIRY_PHASE live={live} hold_us={} select_us={} delete_us={} invalidate_us={} wal_us={} checkpoint_us={} cache_listings={listings} cache_keys_copied={cache_rows}", hold_ns / 1000, select_ns / 1000, delete_ns / 1000, invalidate_ns / 1000, wal_ns / 1000, checkpoint_ns / 1000);
        engine.wal_store.flush(1).unwrap();
        drop(engine);
        let restored = engine_at(dir.path(), "cache-b");
        {
            let shards = restored.shards.read().unwrap();
            let shard = &shards[&1];
            assert_eq!(shard.strings().len(), live);
            assert!(shard.expires_at_ms().is_empty());
            for key in &cold_keys {
                assert!(!shard.strings().contains_key(key.as_str()));
            }
            for key in &hot_keys {
                assert!(!shard.strings().contains_key(key.as_str()));
            }
        }
        if live > 0 {
            for index in [0, live / 2, live - 1] {
                let response = restored.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: format!("live-{index:08}"),
                    },
                });
                assert!(response.status.ok);
                assert_eq!(
                    response.response,
                    CommandResponse::Bytes {
                        value: Some(live_value(index))
                    }
                );
            }
        }
    }
}

#[test]
fn mixed_hot_and_cold_expiry_keeps_due_work_independent_of_live_population() {
    mixed_expiry_scale(&[0, 2000, 20_000]);
}

#[test]
#[ignore = "release-scale maintenance validation"]
fn mixed_hot_and_cold_expiry_at_a_hundred_thousand_live_objects() {
    mixed_expiry_scale(&[0, 100_000]);
}

#[test]
fn released_objects_use_the_cold_expiry_policy_without_payload_reload() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path(), "cache-a");
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSetEx {
                    key: "cold-due".to_string(),
                    value: vec![b'v'; 1024],
                    ttl_ms: 1,
                },
            })
            .status
            .ok
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "live".to_string(),
                    value: b"live-value".to_vec(),
                },
            })
            .status
            .ok
    );
    let dump = engine.create_bucket_dump_manifest(1, []).unwrap();
    engine.clear_dumped_bucket_dirty_state(1, &dump);
    let bucket = engine.routing_bucket_for_key(1, "cold-due");
    {
        let mut shards = engine.shards.write().unwrap();
        let shard = shards.get_mut(&1).unwrap();
        let released = storage_bucket_internals::release_bucket_blocks(shard, &[bucket]);
        assert_eq!(released.released_buckets, vec![bucket]);
        assert!(shard.strings().contains_key("cold-due"));
    }
    std::thread::sleep(std::time::Duration::from_millis(5));
    let request = reports::StorageManagerCycleRequest {
        shard_id: 1,
        enable_prepare: false,
        enable_wal_reclaim: false,
        enable_evict: false,
        enable_block_reclaim: false,
        enable_block_compaction: false,
        enable_index_gc: false,
        ..reports::StorageManagerCycleRequest::default()
    };
    assert!(request.load_cold_buckets_for_expire);
    let mut blocked_request = request.clone();
    blocked_request.load_cold_buckets_for_expire = false;
    let blocked_cycle = engine.run_storage_manager_cycle(blocked_request);
    assert!(blocked_cycle.completed, "{:?}", blocked_cycle.errors);
    let blocked = blocked_cycle.expiry_report.unwrap();
    assert_eq!(
        blocked.expired_records_removed, 0,
        "released metadata must not be classified as a hot object: {blocked:?}"
    );
    assert_eq!(blocked.hot_buckets_scanned, 0);
    assert_eq!(blocked.cold_buckets_scanned, 1);
    assert_eq!(blocked.skipped_records, 1);
    {
        let shards = engine.shards.read().unwrap();
        let shard = &shards[&1];
        assert!(shard.strings().contains_key("cold-due"));
        assert!(shard.bucket_index.released_buckets.contains(&bucket));
    }
    let bytes_read = engine.block_store.stats().bytes_read;
    let allowed_cycle = engine.run_storage_manager_cycle(request);
    assert!(allowed_cycle.completed, "{:?}", allowed_cycle.errors);
    let allowed = allowed_cycle.expiry_report.unwrap();
    assert_eq!(allowed.expired_records_removed, 1);
    assert_eq!(allowed.hot_buckets_scanned, 0);
    assert_eq!(allowed.cold_buckets_scanned, 1);
    assert_eq!(
        allowed.loaded_for_expire, 0,
        "retained locators allow a metadata-only delete"
    );
    assert_eq!(engine.block_store.stats().bytes_read, bytes_read);
    {
        let shards = engine.shards.read().unwrap();
        let shard = &shards[&1];
        assert!(!shard.strings().contains_key("cold-due"));
        assert!(!shard.expires_at_ms().contains_key("cold-due"));
    }
    engine.create_bucket_dump_manifest(1, []).unwrap();
    drop(engine);
    let restored = engine_at(dir.path(), "cache-b");
    assert!(!restored.shards.read().unwrap()[&1]
        .strings()
        .contains_key("cold-due"));
    let response = restored.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "live".to_string(),
        },
    });
    assert_eq!(
        response.response,
        CommandResponse::Bytes {
            value: Some(b"live-value".to_vec())
        }
    );
}

#[test]
fn periodic_cold_expiry_defaults_match_between_typed_and_serialized_requests() {
    assert!(reports::StorageManagerCycleRequest::default().load_cold_buckets_for_expire);
    let omitted: reports::StorageManagerCycleRequest =
        serde_json::from_value(serde_json::json!({ "shard_id": 1 })).unwrap();
    assert!(omitted.load_cold_buckets_for_expire);
    let disabled: reports::StorageManagerCycleRequest = serde_json::from_value(
        serde_json::json!({ "shard_id": 1, "load_cold_buckets_for_expire": false }),
    )
    .unwrap();
    assert!(!disabled.load_cold_buckets_for_expire);
}
