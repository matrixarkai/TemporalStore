// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a write leaves behind for the reclaim floor to stand on.
//!
//! A dirty bucket that no dump manifest covers is allowed to hold the two logs only from its own
//! OLDEST UNDUMPED WRITE, and it has to be able to name that point in BOTH of them --
//! `first_dirty_wal_sequence` and `first_dirty_index_log_sequence`. A bucket that can name neither
//! is not treated as needing nothing; it is treated as unknown, and unknown refuses the whole
//! reclaim plan (`slot_generation_without_durable_dump`). So a write path that dirties buckets
//! without stamping the two claims stops the logs being reclaimed at all until a dump happens to
//! cover every bucket it touched -- and a dump is capped per round.

use super::*;

const RECLAIM_DUMP_SHARD: ShardId = 1;

/// Both halves of every bucket's claim, read straight off the bucket index.
///
/// Returned as two numbers per bucket and never as one total. The plan REQUIRES both halves and
/// they count in different sequences, so a state where one half is whole and the other is zero is
/// exactly the failure a combined count would report as "half full".
fn claims_by_bucket(engine: &TemporalEngine, shard_id: ShardId) -> Vec<(u32, u64, u64)> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let Some(shard) = shards.get(&shard_id) else {
        return Vec::new();
    };
    let mut out = shard
        .bucket_index
        .bucket_map
        .iter()
        .filter(|(_, bucket)| bucket.dirty())
        .map(|(routing_bucket, bucket)| {
            (
                *routing_bucket,
                bucket.first_dirty_wal_sequence,
                bucket.first_dirty_index_log_sequence,
            )
        })
        .collect::<Vec<_>>();
    out.sort();
    out
}

fn reclaim_dump_engine(dir: &std::path::Path, name: &str) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        1 << 20,
        dir.join(format!("{name}-cache")),
        dir.join(format!("{name}-blocks")),
        dir.join(format!("{name}-indexes")),
    )
}

fn reclaim_dump_keys(count: usize) -> Vec<String> {
    (0..count).map(|index| format!("claim-{index:04}")).collect()
}

/// The two write paths, one command at a time and one batch.
fn write_one_at_a_time(engine: &TemporalEngine, keys: &[String]) {
    for key in keys {
        let response = engine.execute(ExecuteRequest {
            shard_id: RECLAIM_DUMP_SHARD,
            command: Command::StringSet {
                key: key.clone(),
                value: vec![118u8; 64],
            },
        });
        assert!(response.status.ok, "write {key} failed: {:?}", response.status);
    }
}

fn write_as_batches(engine: &TemporalEngine, keys: &[String], batch: usize) {
    for chunk in keys.chunks(batch) {
        let commands = chunk
            .iter()
            .map(|key| Command::StringSet {
                key: key.clone(),
                value: vec![118u8; 64],
            })
            .collect::<Vec<_>>();
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: RECLAIM_DUMP_SHARD,
            commands,
        });
        assert!(response.status.ok, "batch write failed: {:?}", response.status);
    }
}

/// Dump every dirty bucket, so the claims clear the way a real round clears them.
fn dump_everything(engine: &TemporalEngine) {
    engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: RECLAIM_DUMP_SHARD,
        min_undumped_wal_records: 0,
        min_undumped_wal_bytes: 0,
        max_dump_buckets_per_round: 0,
        ..StorageManagerCycleRequest::default()
    });
}

fn wal_record_count(engine: &TemporalEngine) -> usize {
    let (records, _truncated) = engine
        .wal_store()
        .scan_decoded(RECLAIM_DUMP_SHARD, 0, u64::MAX, u64::MAX)
        .expect("scan wal");
    records.len()
}

#[test]
fn manifest_install_keeps_index_only_state_without_the_source_wal() {
    let dir = tempfile::tempdir().unwrap();
    let blocks = dir.path().join("shared-blocks");
    let source = TemporalEngine::with_local_dirs(
        1 << 20, dir.path().join("source-cache"), &blocks, dir.path().join("source-index"),
    );
    source.load_shard(RECLAIM_DUMP_SHARD);
    for command in [
        Command::StringSet { key: "anchor".to_string(), value: b"anchor-value".to_vec() },
        Command::SeenCheck { key: "seen-only".to_string(), member: b"member".to_vec(), window_ms: 600_000 },
        Command::BucketTake { key: "quota-only".to_string(), tokens: 2.0, capacity: 2.0, refill_per_sec: 0.0 },
        Command::CommonExpire { key: "seen-only".to_string(), ttl_ms: 600_000 },
    ] {
        let result = source.execute(ExecuteRequest { shard_id: RECLAIM_DUMP_SHARD, command });
        assert!(result.status.ok, "source write: {:?}", result.status);
    }
    let peek = Command::BucketPeek {
        key: "quota-only".to_string(), tokens: 1.0, capacity: 2.0, refill_per_sec: 0.0,
    };
    let expected_quota = source.execute(ExecuteRequest {
        shard_id: RECLAIM_DUMP_SHARD, command: peek.clone(),
    });
    assert!(expected_quota.status.ok);
    match &expected_quota.response {
        CommandResponse::Members { members } => {
            assert_eq!(members.first().map(Vec::as_slice), Some(b"0".as_slice()),
                "the source quota must be exhausted, not a fresh full bucket");
        }
        other => panic!("unexpected quota response: {other:?}"),
    }
    let expected_deadline = source.shards.read().unwrap()[&RECLAIM_DUMP_SHARD]
        .expires_at_ms().get("seen-only").copied().expect("the seen set has a deadline");
    source.flush_shard_index(RECLAIM_DUMP_SHARD);
    let selected = source.bucket_storage_summaries(RECLAIM_DUMP_SHARD)[0].routing_bucket;
    let manifest = source.create_bucket_dump_manifest(RECLAIM_DUMP_SHARD, [selected]).unwrap();
    assert_eq!(manifest.bucket_ids, vec![selected]);
    assert!(manifest.wal_sequence > 0);

    let target_indexes = dir.path().join("target-index");
    let target = TemporalEngine::with_local_dirs(
        1 << 20, dir.path().join("target-cache"), &blocks, &target_indexes,
    );
    target.load_shard(RECLAIM_DUMP_SHARD);
    assert_eq!(target.write_ahead_log_store().stats(RECLAIM_DUMP_SHARD).last_sequence, 0);
    target.install_bucket_dump_manifest(&manifest).unwrap();
    let check = |engine: &TemporalEngine| {
        let seen = engine.execute(ExecuteRequest {
            shard_id: RECLAIM_DUMP_SHARD,
            command: Command::SeenCard { key: "seen-only".to_string() },
        });
        assert!(seen.status.ok);
        assert_eq!(seen.response, CommandResponse::Integer { value: 1 });
        let quota = engine.execute(ExecuteRequest {
            shard_id: RECLAIM_DUMP_SHARD, command: peek.clone(),
        });
        assert!(quota.status.ok);
        assert_eq!(quota.response, expected_quota.response);
        assert_eq!(engine.shards.read().unwrap()[&RECLAIM_DUMP_SHARD]
            .expires_at_ms().get("seen-only").copied(), Some(expected_deadline));
        assert_eq!(engine.write_ahead_log_store().stats(RECLAIM_DUMP_SHARD).last_sequence, 0);
    };
    check(&target);
    drop(target);
    let restarted = TemporalEngine::with_local_dirs(
        1 << 20, dir.path().join("restart-cache"), &blocks, &target_indexes,
    );
    restarted.load_shard(RECLAIM_DUMP_SHARD);
    check(&restarted);
}

#[test]
fn object_runtime_report_builds_one_identity_map_for_all_buckets() {
    let dir = tempfile::tempdir().unwrap();
    for count in [32, 128] {
        let engine = reclaim_dump_engine(dir.path(), &format!("identity-{count}"));
        engine.load_shard(RECLAIM_DUMP_SHARD);
        write_one_at_a_time(&engine, &reclaim_dump_keys(count));
        let shards = engine.shards.read().unwrap();
        let shard = &shards[&RECLAIM_DUMP_SHARD];
        assert!(shard.bucket_index.bucket_map.len() > 16);
        let (report, builds) = crate::engine::state::measure_block_identity_builds_for_test(|| {
            crate::engine::object_manager::runtime_report(shard)
        });
        assert_eq!(report.live_object_count, count);
        assert_eq!(report.live_block_ref_count, count);
        assert_eq!(builds, 1, "{count} objects must share one identity map");
    }
}

#[test]
fn physical_index_report_identity_builds_do_not_scale_with_buckets() {
    let dir = tempfile::tempdir().unwrap();
    for count in [32, 128] {
        let engine = reclaim_dump_engine(dir.path(), &format!("physical-{count}"));
        engine.load_shard(RECLAIM_DUMP_SHARD);
        write_one_at_a_time(&engine, &reclaim_dump_keys(count));
        let shards = engine.shards.read().unwrap();
        let shard = &shards[&RECLAIM_DUMP_SHARD];
        assert!(shard.bucket_index.bucket_map.len() > 16);
        let (report, builds) = crate::engine::state::measure_block_identity_builds_for_test(|| {
            crate::engine::storage_reporting::storage_physical_index_report(
                RECLAIM_DUMP_SHARD, shard, Vec::new(),
            )
        });
        assert_eq!(report.block_index_count, count);
        assert_eq!(report.bucket_count, shard.bucket_index.bucket_map.len());
        assert_eq!(report.missing_object_id_count, 0);
        assert!(builds <= 2, "{count} objects required {builds} identity builds");
    }
}

#[test]
fn compacted_hash_elements_release_their_wal_positions_before_restart() {
    let dir = tempfile::tempdir().unwrap();
    let engine = reclaim_dump_engine(dir.path(), "retire");
    engine.load_shard(RECLAIM_DUMP_SHARD);
    assert!(engine.set_config(SetConfigRequest {
        shard_id: RECLAIM_DUMP_SHARD,
        config: Config { version: 2, async_storage: true, ..Config::default() },
    }).ok);
    for index in 0..16 {
        assert!(engine.execute(ExecuteRequest {
            shard_id: RECLAIM_DUMP_SHARD,
            command: Command::HashSet {
                key: "many-fields".to_string(),
                field: format!("field-{index}"),
                value: vec![index as u8; 64],
            },
        }).status.ok);
    }
    assert_eq!(engine.registration_count_for_test(RECLAIM_DUMP_SHARD), 16);
    assert_eq!(engine.wal_resident_block_count(RECLAIM_DUMP_SHARD), 16);
    engine.compact_shard_blocks(RECLAIM_DUMP_SHARD).unwrap();
    assert_eq!(engine.registration_count_for_test(RECLAIM_DUMP_SHARD), 0);
    assert_eq!(engine.wal_resident_block_count(RECLAIM_DUMP_SHARD), 0);
    engine.flush_shard_index(RECLAIM_DUMP_SHARD);
    drop(engine);
    let restarted = reclaim_dump_engine(dir.path(), "retire");
    restarted.load_shard(RECLAIM_DUMP_SHARD);
    assert_eq!(restarted.wal_resident_block_count(RECLAIM_DUMP_SHARD), 0);
    for index in 0..16 {
        let response = restarted.execute(ExecuteRequest {
            shard_id: RECLAIM_DUMP_SHARD,
            command: Command::HashGet {
                key: "many-fields".to_string(), field: format!("field-{index}"),
            },
        });
        assert!(response.status.ok);
        assert_eq!(response.response, CommandResponse::Bytes { value: Some(vec![index as u8; 64]) });
    }
}

#[test]
fn a_hash_dump_keeps_wal_blocks_needed_by_restore() {
    const KEYS: usize = 48;
    let dir = tempfile::tempdir().unwrap();
    let blocks = dir.path().join("blocks");
    let indexes = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache-a"),
        &blocks,
        &indexes,
    );
    engine.load_shard(RECLAIM_DUMP_SHARD);
    assert!(engine
        .set_config(SetConfigRequest {
            shard_id: RECLAIM_DUMP_SHARD,
            config: Config {
                version: 2,
                async_storage: true,
                ..Config::default()
            },
        })
        .ok);
    for index in 0..KEYS {
        let response = engine.execute(ExecuteRequest {
            shard_id: RECLAIM_DUMP_SHARD,
            command: Command::HashSet {
                key: format!("hash-{index:04}"),
                field: "f".to_string(),
                value: format!("value-{index:04}").into_bytes(),
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    assert_eq!(engine.registration_count_for_test(RECLAIM_DUMP_SHARD), KEYS);
    let report = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: RECLAIM_DUMP_SHARD,
        min_undumped_wal_records: 0,
        min_undumped_wal_bytes: 0,
        max_dump_buckets_per_round: 0,
        enable_prepare: false,
        enable_evict: false,
        enable_expire: false,
        enable_block_reclaim: false,
        enable_block_compaction: false,
        enable_index_gc: false,
        ..StorageManagerCycleRequest::default()
    });
    assert!(report.lifecycle_report.as_ref().and_then(|stage| stage.dump_manifest.as_ref()).is_some());
    assert_eq!(
        engine.wal_store().block_retention_floor(RECLAIM_DUMP_SHARD),
        Some(1),
        "reclaim report: {:?}",
        report.wal_reclaim_report
    );
    assert_eq!(wal_record_count(&engine), KEYS, "reclaim removed live WAL records");
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: RECLAIM_DUMP_SHARD,
                command: Command::HashGet {
                    key: "hash-0000".to_string(),
                    field: "f".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"value-0000".to_vec()),
        },
        "the live shard lost its first hash before restart"
    );
    drop(engine);

    let restarted = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache-b"),
        &blocks,
        &indexes,
    );
    let load = restarted.load_shard_with(LoadShardRequest {
        shard_id: RECLAIM_DUMP_SHARD,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: u32::MAX,
        readonly: false,
        table_name: String::new(),
    });
    assert!(load.status.ok, "restart refused: {:?}", load.status);
    for index in 0..KEYS {
        assert_eq!(
            restarted
                .execute(ExecuteRequest {
                    shard_id: RECLAIM_DUMP_SHARD,
                    command: Command::HashGet {
                        key: format!("hash-{index:04}"),
                        field: "f".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(format!("value-{index:04}").into_bytes()),
            },
            "hash-{index:04} was lost after reclaim and restart"
        );
    }
}

#[test]
fn a_live_only_string_dump_keeps_wal_pages_without_slab_rewrites() {
    let dir = tempfile::tempdir().unwrap();
    let keys = reclaim_dump_keys(64);
    for batch in [1, 8] {
        let name = format!("live-only-{batch}");
        let engine = reclaim_dump_engine(dir.path(), &name);
        engine.load_shard(RECLAIM_DUMP_SHARD);
        assert!(engine.set_config(SetConfigRequest {
            shard_id: RECLAIM_DUMP_SHARD,
            config: Config { version: 2, async_storage: true, ..Config::default() },
        }).ok);
        write_as_batches(&engine, &keys, batch);
        assert_eq!(engine.registration_count_for_test(RECLAIM_DUMP_SHARD), keys.len());
        let records_before = wal_record_count(&engine);
        assert!(!crate::engine::block_in_wal::registered_record_span_has_holes(
            &engine.block_store, RECLAIM_DUMP_SHARD, &engine.wal_store,
        ));
        assert_eq!(records_before, keys.len() / batch);
        let block_bytes_before = engine.block_store.stats().bytes_written;
        let report = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
            shard_id: RECLAIM_DUMP_SHARD,
            max_dump_buckets_per_round: 0,
            enable_prepare: false,
            enable_evict: false,
            enable_expire: false,
            enable_block_reclaim: false,
            enable_block_compaction: false,
            enable_index_gc: false,
            ..StorageManagerCycleRequest::default()
        });
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert!(report.lifecycle_report.as_ref().and_then(|stage| stage.dump_manifest.as_ref()).is_some());
        assert!(report.stages.iter().all(|stage| stage.wal_resident_blocks_materialised == 0));
        assert_eq!(engine.block_store.stats().bytes_written, block_bytes_before);
        assert_eq!(wal_record_count(&engine), records_before);
        assert_eq!(engine.registration_count_for_test(RECLAIM_DUMP_SHARD), keys.len());
        if batch == 8 {
            assert!(engine.execute(ExecuteRequest {
                shard_id: RECLAIM_DUMP_SHARD,
                command: Command::StringSet { key: keys[0].clone(), value: vec![119; 64] },
            }).status.ok);
            assert!(crate::engine::block_in_wal::registered_record_span_has_holes(
                &engine.block_store, RECLAIM_DUMP_SHARD, &engine.wal_store,
            ), "a partially superseded batch must still create relocation pressure");
        }
        drop(engine);
        let restored = TemporalEngine::with_local_dirs(
            1 << 20, dir.path().join(format!("restart-{batch}")),
            dir.path().join(format!("{name}-blocks")), dir.path().join(format!("{name}-indexes")),
        );
        restored.load_shard(RECLAIM_DUMP_SHARD);
        for key in &keys {
            assert_eq!(restored.execute(ExecuteRequest {
                shard_id: RECLAIM_DUMP_SHARD, command: Command::StringGet { key: key.clone() },
            }).response, CommandResponse::Bytes {
                value: Some(vec![if batch == 8 && key == &keys[0] { 119 } else { 118 }; 64]),
            });
        }
    }
}

#[test]
fn an_async_string_dump_reclaims_without_losing_restore() {
    const KEYS: usize = 24;
    let dir = tempfile::tempdir().unwrap();
    let blocks = dir.path().join("blocks");
    let indexes = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache-a"),
        &blocks,
        &indexes,
    );
    engine.load_shard(RECLAIM_DUMP_SHARD);
    assert!(engine
        .set_config(SetConfigRequest {
            shard_id: RECLAIM_DUMP_SHARD,
            config: Config {
                version: 2,
                async_storage: true,
                ..Config::default()
            },
        })
        .ok);
    for index in 0..KEYS {
        let response = engine.execute(ExecuteRequest {
            shard_id: RECLAIM_DUMP_SHARD,
            command: Command::StringSet {
                key: format!("string-{index:04}"),
                value: format!("value-{index:04}").into_bytes(),
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    // Supersede a record inside the live span so relocation can recover stale WAL space.
    for value in [b"temporary".to_vec(), format!("value-{:04}", KEYS / 2).into_bytes()] {
        assert!(engine.execute(ExecuteRequest {
            shard_id: RECLAIM_DUMP_SHARD,
            command: Command::StringSet { key: format!("string-{:04}", KEYS / 2), value },
        }).status.ok);
    }
    let report = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: RECLAIM_DUMP_SHARD,
        min_undumped_wal_records: 0,
        min_undumped_wal_bytes: 0,
        max_dump_buckets_per_round: 0,
        enable_prepare: false,
        enable_evict: false,
        enable_expire: false,
        enable_block_reclaim: false,
        enable_block_compaction: false,
        enable_index_gc: false,
        ..StorageManagerCycleRequest::default()
    });
    assert!(report.lifecycle_report.as_ref().and_then(|stage| stage.dump_manifest.as_ref()).is_some());
    assert!(report.stages.iter().any(|stage| stage.wal_resident_blocks_materialised > 0));
    assert!(
        report
            .wal_reclaim_report
            .as_ref()
            .is_some_and(|stage| stage.wal_records_removed > 0),
        "reclaim report: {:?}",
        report.wal_reclaim_report
    );
    drop(engine);

    let restarted = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache-b"),
        &blocks,
        &indexes,
    );
    restarted.load_shard(RECLAIM_DUMP_SHARD);
    for index in 0..KEYS {
        assert_eq!(
            restarted
                .execute(ExecuteRequest {
                    shard_id: RECLAIM_DUMP_SHARD,
                    command: Command::StringGet {
                        key: format!("string-{index:04}"),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(format!("value-{index:04}").into_bytes()),
            },
            "string-{index:04} was lost after reclaim and restart"
        );
    }
}

#[test]
fn container_blocks_remain_readable_after_dump_and_restart() {
    const KEYS: usize = 12;
    let dir = tempfile::tempdir().unwrap();
    let blocks = dir.path().join("blocks");
    let indexes = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache-a"),
        &blocks,
        &indexes,
    );
    engine.load_shard(RECLAIM_DUMP_SHARD);
    assert!(engine
        .set_config(SetConfigRequest {
            shard_id: RECLAIM_DUMP_SHARD,
            config: Config {
                version: 2,
                async_storage: true,
                ..Config::default()
            },
        })
        .ok);
    for index in 0..KEYS {
        let member = format!("member-{index:04}").into_bytes();
        for command in [
            Command::SetAdd {
                key: format!("set-{index:04}"),
                member: member.clone(),
            },
            Command::ListPush {
                key: format!("list-{index:04}"),
                member: member.clone(),
                left: false,
            },
            Command::ZSetAdd {
                key: format!("zset-{index:04}"),
                member: member.clone(),
                score: index as f64,
            },
        ] {
            let response = engine.execute(ExecuteRequest {
                shard_id: RECLAIM_DUMP_SHARD,
                command,
            });
            assert!(response.status.ok, "write {index}: {:?}", response.status);
        }
    }
    assert_eq!(engine.registration_count_for_test(RECLAIM_DUMP_SHARD), KEYS * 3);
    let report = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: RECLAIM_DUMP_SHARD,
        min_undumped_wal_records: 0,
        min_undumped_wal_bytes: 0,
        max_dump_buckets_per_round: 0,
        enable_prepare: false,
        enable_evict: false,
        enable_expire: false,
        enable_block_reclaim: false,
        enable_block_compaction: false,
        enable_index_gc: false,
        ..StorageManagerCycleRequest::default()
    });
    assert!(report.lifecycle_report.as_ref().and_then(|stage| stage.dump_manifest.as_ref()).is_some());
    assert_eq!(engine.wal_store().block_retention_floor(RECLAIM_DUMP_SHARD), Some(1));
    drop(engine);

    let restarted = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache-b"),
        &blocks,
        &indexes,
    );
    restarted.load_shard(RECLAIM_DUMP_SHARD);
    for index in 0..KEYS {
        let member = format!("member-{index:04}").into_bytes();
        for (command, expected) in [
            (
                Command::SetMembers {
                    key: format!("set-{index:04}"),
                },
                CommandResponse::Members {
                    members: vec![member.clone()],
                },
            ),
            (
                Command::ListRange {
                    key: format!("list-{index:04}"),
                    start: 0,
                    stop: -1,
                },
                CommandResponse::Members {
                    members: vec![member.clone()],
                },
            ),
            (
                Command::ZSetScore {
                    key: format!("zset-{index:04}"),
                    member,
                },
                CommandResponse::Bytes {
                    value: Some(index.to_string().into_bytes()),
                },
            ),
        ] {
            let got = restarted
                .execute(ExecuteRequest {
                    shard_id: RECLAIM_DUMP_SHARD,
                    command,
                })
                .response;
            assert_eq!(got, expected, "container {index} changed after restart");
        }
    }
}

#[test]
fn a_container_tombstone_retains_its_wal_record_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let blocks = dir.path().join("blocks");
    let indexes = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache-a"),
        &blocks,
        &indexes,
    );
    engine.load_shard(RECLAIM_DUMP_SHARD);
    assert!(engine
        .set_config(SetConfigRequest {
            shard_id: RECLAIM_DUMP_SHARD,
            config: Config {
                version: 2,
                async_storage: true,
                ..Config::default()
            },
        })
        .ok);
    for command in [
        Command::SetAdd {
            key: "removed-set".to_string(),
            member: b"former-member".to_vec(),
        },
        Command::SetRemove {
            key: "removed-set".to_string(),
            member: b"former-member".to_vec(),
        },
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: RECLAIM_DUMP_SHARD,
            command,
        });
        assert!(response.status.ok, "write failed: {:?}", response.status);
    }
    let request = StorageManagerCycleRequest {
        shard_id: RECLAIM_DUMP_SHARD,
        min_undumped_wal_records: 0,
        min_undumped_wal_bytes: 0,
        max_dump_buckets_per_round: 0,
        enable_prepare: false,
        enable_evict: false,
        enable_expire: false,
        enable_block_reclaim: false,
        enable_block_compaction: false,
        enable_index_gc: false,
        ..StorageManagerCycleRequest::default()
    };
    engine.run_storage_manager_cycle(request.clone());
    drop(engine);

    let restarted = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache-b"),
        &blocks,
        &indexes,
    );
    restarted.load_shard(RECLAIM_DUMP_SHARD);
    assert_eq!(restarted.registration_count_for_test(RECLAIM_DUMP_SHARD), 1);
    restarted.run_storage_manager_cycle(request);
    assert_eq!(restarted.wal_store().block_retention_floor(RECLAIM_DUMP_SHARD), Some(2));
    assert_eq!(
        restarted
            .execute(ExecuteRequest {
                shard_id: RECLAIM_DUMP_SHARD,
                command: Command::SetMembers {
                    key: "removed-set".to_string(),
                },
            })
            .response,
        CommandResponse::Members { members: Vec::new() }
    );
}

#[test]
fn batched_tombstones_rehydrate_from_one_wal_record() {
    const MEMBERS: usize = 64;
    let dir = tempfile::tempdir().unwrap();
    let blocks = dir.path().join("blocks");
    let indexes = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache-a"),
        &blocks,
        &indexes,
    );
    engine.load_shard(RECLAIM_DUMP_SHARD);
    assert!(engine
        .set_config(SetConfigRequest {
            shard_id: RECLAIM_DUMP_SHARD,
            config: Config {
                version: 2,
                async_storage: true,
                ..Config::default()
            },
        })
        .ok);
    for remove in [false, true] {
        let commands = (0..MEMBERS)
            .map(|index| {
                let key = format!("removed-set-{index}");
                let member = format!("member-{index}").into_bytes();
                if remove {
                    Command::SetRemove { key, member }
                } else {
                    Command::SetAdd { key, member }
                }
            })
            .collect();
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: RECLAIM_DUMP_SHARD,
            commands,
        });
        assert!(response.status.ok, "batch write failed: {:?}", response.status);
    }
    assert_eq!(engine.registration_count_for_test(RECLAIM_DUMP_SHARD), MEMBERS);
    engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: RECLAIM_DUMP_SHARD,
        max_dump_buckets_per_round: 0,
        enable_prepare: false,
        enable_evict: false,
        enable_expire: false,
        enable_block_reclaim: false,
        enable_block_compaction: false,
        enable_index_gc: false,
        ..StorageManagerCycleRequest::default()
    });
    assert_eq!(engine.registration_count_for_test(RECLAIM_DUMP_SHARD), MEMBERS);
    drop(engine);

    let restarted = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache-b"),
        &blocks,
        &indexes,
    );
    let before = restarted.wal_store().stats(RECLAIM_DUMP_SHARD).bytes_read;
    let load = restarted.load_shard_with(LoadShardRequest {
        shard_id: RECLAIM_DUMP_SHARD,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: u32::MAX,
        readonly: false,
        table_name: String::new(),
    });
    assert!(load.status.ok, "restart refused: {:?}", load.status);
    let bytes_read = restarted.wal_store().stats(RECLAIM_DUMP_SHARD).bytes_read - before;
    assert_eq!(restarted.registration_count_for_test(RECLAIM_DUMP_SHARD), MEMBERS);
    assert!(
        bytes_read < 1 << 20,
        "rehydrating {MEMBERS} tombstones read {bytes_read} WAL bytes"
    );
    for index in 0..MEMBERS {
        assert_eq!(
            restarted
                .execute(ExecuteRequest {
                    shard_id: RECLAIM_DUMP_SHARD,
                    command: Command::SetMembers {
                        key: format!("removed-set-{index}"),
                    },
                })
                .response,
            CommandResponse::Members { members: Vec::new() }
        );
    }
}

/// A bucket dirtied by a BATCH write can name its oldest undumped write, as one dirtied by a
/// single-command write already could.
///
/// The single-command path stamps both halves of the claim (engine.rs, after the WAL append and
/// after the index-log delta). The batch path marks the same buckets dirty through the same
/// `mark_object_dirty` and appends to both logs, and stamped neither -- so every bucket it
/// dirtied reported "no claim recorded", which the reclaim plan reads as unknown and refuses on.
///
/// The single-command arm is the CONTROL: it proves the fixture can produce claims at all, so a
/// zero on the batch arm is the path and not the fixture.
#[test]
fn a_bucket_dirtied_by_a_batch_write_can_name_its_oldest_undumped_write() {
    const KEYS: usize = 64;
    const BATCH: usize = 8;

    let dir = tempfile::tempdir().unwrap();
    let keys = reclaim_dump_keys(KEYS);

    let one_at_a_time = reclaim_dump_engine(dir.path(), "single");
    one_at_a_time.load_shard(RECLAIM_DUMP_SHARD);
    write_one_at_a_time(&one_at_a_time, &keys);

    let batched = reclaim_dump_engine(dir.path(), "batch");
    batched.load_shard(RECLAIM_DUMP_SHARD);
    write_as_batches(&batched, &keys, BATCH);

    let single_claims = claims_by_bucket(&one_at_a_time, RECLAIM_DUMP_SHARD);
    let batch_claims = claims_by_bucket(&batched, RECLAIM_DUMP_SHARD);

    // DENOMINATORS FIRST. Without dirty buckets on both sides every count below is 0 == 0.
    assert!(
        !single_claims.is_empty(),
        "the control wrote {KEYS} keys and dirtied no bucket, so it proves nothing"
    );
    assert!(
        !batch_claims.is_empty(),
        "the batch arm wrote {KEYS} keys and dirtied no bucket, so it proves nothing"
    );
    assert_eq!(
        single_claims
            .iter()
            .map(|(bucket, _, _)| *bucket)
            .collect::<Vec<_>>(),
        batch_claims
            .iter()
            .map(|(bucket, _, _)| *bucket)
            .collect::<Vec<_>>(),
        "the two arms dirtied different buckets, so they are not comparable"
    );

    // THE CONTROL, both halves separately.
    let single_wal = single_claims
        .iter()
        .filter(|(_, wal, _)| *wal > 0)
        .count();
    let single_index_log = single_claims
        .iter()
        .filter(|(_, _, index_log)| *index_log > 0)
        .count();
    assert_eq!(
        single_wal,
        single_claims.len(),
        "the single-command control left a dirty bucket with no WAL claim: {single_claims:?}"
    );
    assert_eq!(
        single_index_log,
        single_claims.len(),
        "the single-command control left a dirty bucket with no index-log claim: \
         {single_claims:?}"
    );

    // THE SUBJECT, the same two halves, never added together.
    let batch_wal = batch_claims.iter().filter(|(_, wal, _)| *wal > 0).count();
    let batch_index_log = batch_claims
        .iter()
        .filter(|(_, _, index_log)| *index_log > 0)
        .count();
    assert_eq!(
        batch_wal,
        batch_claims.len(),
        "{} of {} batch-dirtied buckets could not name their oldest undumped write in the WAL, so \
         the reclaim plan reads them as unknown and refuses: {batch_claims:?}",
        batch_claims.len() - batch_wal,
        batch_claims.len()
    );
    assert_eq!(
        batch_index_log,
        batch_claims.len(),
        "{} of {} batch-dirtied buckets could not name their oldest undumped write in the index \
         log: {batch_claims:?}",
        batch_claims.len() - batch_index_log,
        batch_claims.len()
    );

    // A CLAIM MUST NOT OVERSTATE. `retain_from = claim - 1 + 1`, so a claim ABOVE the bucket's
    // oldest undumped write frees records that bucket still needs. Every claim must sit at or
    // below the first sequence the arm ever wrote, and no batch bucket may claim a point later
    // than the control's own bucket does.
    let batch_oldest = batch_claims
        .iter()
        .map(|(_, wal, _)| *wal)
        .min()
        .expect("a claim");
    assert_eq!(
        batch_oldest, 1,
        "the oldest batch claim is {batch_oldest}, not the shard's first record; a claim above a \
         bucket's oldest undumped write frees records it still needs"
    );
}

/// The consequence: a shard written in batches can have its logs reclaimed.
///
/// A bucket falls back on its own claims exactly when no dump manifest covers it, and on any real
/// shard a capped dump leaves buckets there every round. With no claims to fall back on every one
/// of them lands in `missing_bucket_generations`, `safe_to_reclaim` is false, the WAL frees
/// nothing and index-log GC is skipped for "durable WAL/index frontier not safe".
///
/// The fixture reaches that state the way a running shard does: write, let a cycle dump and clear
/// the claims, then write again. The second write is what the surviving floor has to stand on.
/// The single-command arm runs the IDENTICAL sequence and is the control -- it is what says the
/// numbers below come from the write path rather than from the fixture.
#[test]
fn a_batch_written_shard_reclaims_the_way_a_single_command_one_does() {
    const KEYS: usize = 64;
    const BATCH: usize = 8;

    let dir = tempfile::tempdir().unwrap();
    let keys = reclaim_dump_keys(KEYS);
    let more = (0..KEYS)
        .map(|index| format!("second-{index:04}"))
        .collect::<Vec<_>>();

    let one_at_a_time = reclaim_dump_engine(dir.path(), "single");
    one_at_a_time.load_shard(RECLAIM_DUMP_SHARD);
    write_one_at_a_time(&one_at_a_time, &keys);
    dump_everything(&one_at_a_time);
    write_one_at_a_time(&one_at_a_time, &more);

    let batched = reclaim_dump_engine(dir.path(), "batch");
    batched.load_shard(RECLAIM_DUMP_SHARD);
    write_as_batches(&batched, &keys, BATCH);
    dump_everything(&batched);
    write_as_batches(&batched, &more, BATCH);

    let single_plan =
        one_at_a_time.storage_wal_reclaim_plan(RECLAIM_DUMP_SHARD, Vec::new(), Vec::new());
    let batch_plan = batched.storage_wal_reclaim_plan(RECLAIM_DUMP_SHARD, Vec::new(), Vec::new());

    // DENOMINATORS FIRST.
    //
    // A plan over a shard with no buckets, or one with nothing left in the log, would satisfy
    // every assertion below without the reclaim having anything to do.
    assert!(
        single_plan.covered_bucket_count + single_plan.uncovered_bucket_count > 0,
        "the control plan saw no buckets at all, so it proves nothing: {single_plan:?}"
    );
    assert!(
        batch_plan.covered_bucket_count + batch_plan.uncovered_bucket_count > 0,
        "the batch plan saw no buckets at all, so it proves nothing: {batch_plan:?}"
    );
    let single_records_before = wal_record_count(&one_at_a_time);
    let batch_records_before = wal_record_count(&batched);
    assert!(
        single_records_before > 0 && batch_records_before > 0,
        "one of the two arms holds no WAL records, so a reclaim freeing nothing would pass \
         vacuously (control {single_records_before}, batch {batch_records_before})"
    );
    // The second write must have left dirty buckets, or there is nothing for a claim to anchor.
    let batch_dirty = claims_by_bucket(&batched, RECLAIM_DUMP_SHARD);
    assert!(
        !batch_dirty.is_empty(),
        "the second batch dirtied no bucket, so the claim branch is never reached"
    );

    // THE CONTROL: the single-command arm anchors and reclaims.
    assert!(
        single_plan.missing_bucket_generations.is_empty(),
        "the single-command control could not anchor its own buckets: {:?}",
        single_plan.missing_bucket_generations
    );
    assert!(
        single_plan.safe_to_reclaim,
        "the single-command control refuses its own reclaim, so it cannot be a control \
         (blockers {:?})",
        single_plan.blocker_reasons
    );

    // THE SUBJECT.
    assert!(
        batch_plan.missing_bucket_generations.is_empty(),
        "{} of {} buckets on the batch-written shard cannot be anchored, so the whole reclaim is \
         refused: {:?}",
        batch_plan.missing_bucket_generations.len(),
        batch_plan.covered_bucket_count + batch_plan.uncovered_bucket_count,
        batch_plan.missing_bucket_generations
    );
    assert!(
        batch_plan.safe_to_reclaim,
        "the batch-written shard refuses reclaim while the identically-written single-command \
         shard allows it (blockers {:?})",
        batch_plan.blocker_reasons
    );

    // Both halves of the frontier, separately: a plan that anchored one log and not the other
    // would read as safe on a combined view and free nothing.
    assert!(
        batch_plan.retain_from_wal_sequence > 0,
        "the batch plan named no WAL retain point: {batch_plan:?}"
    );
    assert!(
        batch_plan.retain_from_index_log_sequence > 0,
        "the batch plan named no index-log retain point: {batch_plan:?}"
    );

    // And it actually frees, on both arms, counted separately.
    let single_removed = one_at_a_time
        .apply_storage_wal_reclaim(single_plan)
        .wal_records_removed;
    let batch_removed = batched.apply_storage_wal_reclaim(batch_plan).wal_records_removed;
    assert!(
        single_removed > 0,
        "the control reclaim freed nothing out of {single_records_before} records"
    );
    assert!(
        batch_removed > 0,
        "the batch-written shard freed {batch_removed} of {batch_records_before} WAL records \
         while the control freed {single_removed} of {single_records_before}"
    );
}

/// The same claim, at corpus scale.
///
/// #1623's healthy small column did not generalise, so this asks the question again at a size
/// where the dirty set is far larger than a round's dump cap -- which is the state the claim
/// branch exists for. `TS_RECLAIM_DUMP_SCALE` raises the corpus without editing the test; the
/// default is the size this runs at on every gate.
#[test]
fn a_batch_written_corpus_anchors_every_bucket_it_dirties() {
    let keys_count = std::env::var("TS_RECLAIM_DUMP_SCALE")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(8_000);
    const BATCH: usize = 500;

    let dir = tempfile::tempdir().unwrap();
    let keys = reclaim_dump_keys(keys_count);
    let engine = reclaim_dump_engine(dir.path(), "scale");
    engine.load_shard(RECLAIM_DUMP_SHARD);
    write_as_batches(&engine, &keys, BATCH);

    let claims = claims_by_bucket(&engine, RECLAIM_DUMP_SHARD);
    // DENOMINATOR: the corpus has to dirty more buckets than one round may dump, or the claim
    // branch is never the thing being tested.
    assert!(
        claims.len() > 64,
        "{keys_count} keys dirtied only {} buckets, which a single uncapped round would dump \
         whole -- the claim branch is never reached",
        claims.len()
    );

    // The two halves, counted apart.
    let missing_wal = claims.iter().filter(|(_, wal, _)| *wal == 0).count();
    let missing_index_log = claims
        .iter()
        .filter(|(_, _, index_log)| *index_log == 0)
        .count();
    assert_eq!(
        0, missing_wal,
        "{missing_wal} of {} dirty buckets at {keys_count} records hold no WAL claim",
        claims.len()
    );
    assert_eq!(
        0, missing_index_log,
        "{missing_index_log} of {} dirty buckets at {keys_count} records hold no index-log claim",
        claims.len()
    );

    // No claim may sit above the bucket's oldest undumped write. Every key was written once, in
    // ascending batches, so the oldest claim is the shard's first record.
    let oldest = claims.iter().map(|(_, wal, _)| *wal).min().expect("a claim");
    assert_eq!(
        1, oldest,
        "the oldest claim at {keys_count} records is {oldest}, not the shard's first record"
    );
    println!(
        "reclaim_dump scale: keys={keys_count} dirty_buckets={} wal_claims={} index_log_claims={}",
        claims.len(),
        claims.len() - missing_wal,
        claims.len() - missing_index_log
    );
}
