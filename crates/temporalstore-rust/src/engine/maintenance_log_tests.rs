// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn engine_at(root: &std::path::Path, indexes: &str) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        16 << 20,
        root.join("cache"),
        root.join("blocks"),
        root.join(indexes),
    )
}

fn write(engine: &TemporalEngine, key: &str, value: &[u8]) {
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: key.into(),
                    value: value.to_vec()
                },
            })
            .status
            .ok
    );
}

fn value(engine: &TemporalEngine, key: &str) -> Option<Vec<u8>> {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet { key: key.into() },
    });
    assert!(response.status.ok);
    let CommandResponse::Bytes { value } = response.response else {
        panic!("expected bytes");
    };
    value
}

#[derive(Debug, Default)]
struct OutcomeMirror(std::sync::Mutex<Vec<Vec<crate::wal::WalOutcomeItem>>>);

impl crate::data_node::SharedWalSink for OutcomeMirror {
    fn record_write(&self, _: ShardId, _: &Command) {
        panic!("maintenance must mirror exact outcomes");
    }
    fn record_outcomes(&self, _: ShardId, outcomes: &[crate::wal::WalOutcomeItem]) {
        self.0.lock().unwrap().push(outcomes.to_vec());
    }
}

#[test]
fn expiry_keeps_independently_live_records_in_memory_and_wal_recovery() {
    for cold in [false, true] {
        for expire_family in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let engine = engine_at(dir.path(), "indexes");
            engine.load_shard(1);
            assert!(engine.set_config(SetConfigRequest { shard_id: 1,
                config: Config { version: 2, async_storage: true, ..Default::default() },
            }).ok);
            let base = "aaa-expiry".to_string();
            let family = control_state_family_key(ControlStateFamily::Counter, &base);
            for key in [&base, &family] {
                assert!(engine.execute(ExecuteRequest { shard_id: 1,
                    command: Command::StringSetEx { key: key.clone(), value: key.as_bytes().to_vec(), ttl_ms: 60_000 },
                }).status.ok);
            }
            let (due, live) = if expire_family { (&family, &base) } else { (&base, &family) };
            assert_eq!(value(&engine, live), Some(live.as_bytes().to_vec()));
            assert!(engine.execute(ExecuteRequest { shard_id: 1,
                command: Command::StringSetEx { key: due.clone(), value: due.as_bytes().to_vec(), ttl_ms: 1 },
            }).status.ok);
            // Restore the family deadline after the base TTL outcome, which replays family-wide.
            if !expire_family {
                assert!(engine.execute(ExecuteRequest { shard_id: 1,
                    command: Command::StringSetEx { key: live.clone(), value: live.as_bytes().to_vec(), ttl_ms: 60_000 },
                }).status.ok);
            }
            {
                let shards = engine.shards.read().unwrap();
                assert!(shards[&1].strings().contains_key(due.as_str()));
                assert!(shards[&1].strings().contains_key(live.as_str()));
            }
            if cold {
                let bucket = engine.routing_bucket_for_key(1, due);
                assert_ne!(bucket, engine.routing_bucket_for_key(1, live));
                let mut shards = engine.shards.write().unwrap();
                let released = storage_bucket_internals::release_bucket_blocks_preserving_inline_pages(
                    shards.get_mut(&1).unwrap(), &[bucket]);
                assert_eq!(released.released_buckets, vec![bucket]);
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
            let before = engine.wal_store.cached_last_sequence(1);
            let mirror = Arc::new(OutcomeMirror::default());
            engine.set_maintenance_wal_mirror(mirror.clone());
            let report = engine.sweep_expired_records_for_maintenance(ShardExpirySweepRequest {
                shard_id: 1, max_hot_buckets_per_round: usize::from(!cold),
                max_cold_buckets_per_round: usize::from(cold), load_cold_buckets: true,
                ..Default::default()
            }).unwrap();
            assert_eq!(report.expired_records_removed, 1);
            {
                let shards = engine.shards.read().unwrap();
                assert!(!shards[&1].strings().contains_key(due.as_str()));
                assert!(shards[&1].strings().contains_key(live.as_str()),
                    "expiring {due} must preserve independently live {live}");
            }
            let (records, truncated) = engine.wal_store.scan_decoded(1, 0, u64::MAX, u64::MAX).unwrap();
            assert!(!truncated);
            let deleted: Vec<_> = records.iter().filter(|(_, record)| record.sequence > before)
                .flat_map(|(_, record)| &record.outcomes).map(|item| item.object_key.clone()).collect();
            assert_eq!(deleted, vec![due.clone()]);
            let frames = mirror.0.lock().unwrap();
            assert_eq!(frames.len(), 1);
            assert_eq!(frames[0].len(), 1);
            assert_eq!(frames[0][0].object_key, *due);
            let follower = TemporalEngine::with_local_dirs(16 << 20,
                dir.path().join("follower-cache"), dir.path().join("follower-blocks"), dir.path().join("follower-indexes"));
            follower.load_shard(1);
            write(&follower, due, b"due");
            write(&follower, live, live.as_bytes());
            assert_eq!(value(&follower, due), Some(b"due".to_vec()));
            assert_eq!(value(&follower, live), Some(live.as_bytes().to_vec()));
            for key in [due, live] {
                for field in ["alpha", "beta", "gamma"] {
                    assert!(follower.execute(ExecuteRequest { shard_id: 1,
                        command: Command::HashSet { key: key.clone(), field: field.into(), value: b"cached".to_vec() },
                    }).status.ok);
                    assert_eq!(follower.execute(ExecuteRequest { shard_id: 1,
                        command: Command::HashGet { key: key.clone(), field: field.into() },
                    }).response, CommandResponse::Bytes { value: Some(b"cached".to_vec()) });
                    // Point reads keep only physical payloads; seed supported query-response entries explicitly.
                    follower.cache.put_memory_only(CacheKey::hash(1, key, field),
                        encode_cached_response(&CommandResponse::Bytes { value: Some(b"cached".to_vec()) }).unwrap());
                }
            }
            for key in [due, live] {
                for field in ["alpha", "beta", "gamma"] {
                    assert!(follower.cache.get(&CacheKey::hash(1, key, field)).unwrap().is_some());
                }
            }
            assert!(follower.install_shared_outcomes(1, &frames[0]));
            for (key, expected) in [(due, None), (live, Some(b"cached".to_vec()))] {
                for field in ["alpha", "beta", "gamma"] {
                    assert_eq!(follower.cache.get(&CacheKey::hash(1, key, field)).unwrap().is_some(), key == live);
                    assert_eq!(follower.execute(ExecuteRequest { shard_id: 1,
                        command: Command::HashGet { key: key.clone(), field: field.into() },
                    }).response, CommandResponse::Bytes { value: expected.clone() });
                }
            }
            assert_eq!(value(&follower, due), None);
            assert_eq!(value(&follower, live), Some(live.as_bytes().to_vec()));
            drop(follower);
            engine.wal_store.flush(1).unwrap();
            drop(engine);
            let indexed = TemporalEngine::with_local_dirs(16 << 20,
                dir.path().join("indexed-cache"), dir.path().join("blocks"), dir.path().join("indexes"));
            indexed.load_shard(1);
            assert_eq!(value(&indexed, due), None);
            assert_eq!(value(&indexed, live), Some(live.as_bytes().to_vec()));
            drop(indexed);
            let mut restored = TemporalEngine::with_local_dirs(16 << 20, dir.path().join("fresh-cache"),
                dir.path().join("blocks"), dir.path().join("empty-indexes"));
            restored.wal_store = LocalWriteAheadLogStore::new(dir.path().join("indexes/wals"));
            restored.load_shard(1);
            {
                let shards = restored.shards.read().unwrap();
                assert!(!shards[&1].strings().contains_key(due.as_str()));
                assert!(shards[&1].strings().contains_key(live.as_str()));
            }
            assert_eq!(value(&restored, live), Some(live.as_bytes().to_vec()));
        }
    }
}

#[test]
fn expiry_budget_counts_each_due_record_without_expanding_its_family() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path(), "indexes");
    engine.load_shard(1);
    let base = "aaa-budget";
    let family = control_state_family_key(ControlStateFamily::Counter, base);
    write(&engine, base, b"base");
    write(&engine, &family, b"family");
    assert!(engine.execute(ExecuteRequest { shard_id: 1,
        command: Command::CommonExpire { key: base.into(), ttl_ms: 1 },
    }).status.ok);
    std::thread::sleep(std::time::Duration::from_millis(10));
    for (first, retained) in [(true, family.as_str()), (false, "")] {
        let report = engine.sweep_expired_records_for_maintenance(ShardExpirySweepRequest {
            shard_id: 1, max_hot_buckets_per_round: 1, load_cold_buckets: true,
            ..Default::default()
        }).unwrap();
        assert_eq!(report.expired_records_removed, 1);
        let shards = engine.shards.read().unwrap();
        assert!(!shards[&1].strings().contains_key(base));
        if first {
            assert!(shards[&1].strings().contains_key(retained));
        } else {
            assert!(!shards[&1].strings().contains_key(family.as_str()));
        }
    }
}

#[test]
fn single_object_bucket_deletions_keep_the_key_projection_fast_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path(), "indexes");
    engine.load_shard(1);
    let keys: Vec<_> = (0..32).map(|n| format!("sparse-delete-{n:04}")).collect();
    for key in &keys {
        write(&engine, key, b"remove");
    }
    let mut shards = engine.shards.write().unwrap();
    let shard = shards.get_mut(&1).unwrap();
    assert_eq!(shard.bucket_index.bucket_map.len(), keys.len());
    let deletions = keys.iter().map(|key| MaintenanceDeletion::capture(shard, key).unwrap()).collect();
    let (result, builds) = state::measure_block_identity_builds_for_test(|| {
        engine.append_maintenance_deletions(1, shard, deletions, || {})
    });
    assert!(result.error.is_none());
    assert_eq!(result.removed_keys, keys.len());
    assert_eq!(builds, 0, "unique buckets must keep the key-local projection");
    drop(shards);
    for key in &keys {
        assert_eq!(value(&engine, key), None);
    }
}

/// The dense-bucket deletion run, by handle or by naming the bucket. Returns (identity builds, owner visits, frames).
fn dense_bucket_deletions(object_local: bool) -> (usize, usize, usize) {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path(), "indexes");
    let load = LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: 7,
        readonly: false,
        table_name: String::new(),
    };
    assert!(engine.load_shard_with(load.clone()).status.ok);
    let keys: Vec<_> = (0..513).map(|n| format!("dense-delete-{n:04}")).collect();
    for key in &keys {
        write(&engine, key, b"remove");
    }
    let survivors: Vec<_> = (0..32).map(|n| format!("dense-keep-{n:04}")).collect();
    for key in &survivors {
        write(&engine, key, b"keep");
    }
    engine.flush_shard_index(1);
    let mut shards = engine.shards.write().unwrap();
    let shard = shards.get_mut(&1).unwrap();
    shard.bucket_index.ensure_bucket_owners();
    assert_eq!(shard.bucket_index.bucket_map.len(), 8);
    let deletions = keys.iter().map(|key| MaintenanceDeletion::capture(shard, key).unwrap()).collect();
    let mut appends = 0;
    crate::engine::disable_object_local_delete_for_test(!object_local);
    let ((result, visits), builds) = state::measure_block_identity_builds_for_test(|| {
        state::measure_block_identity_visits_for_test(|| {
            engine.append_maintenance_deletions(1, shard, deletions, || appends += 1)
        })
    });
    crate::engine::disable_object_local_delete_for_test(false);
    assert!(result.error.is_none());
    assert_eq!(result.removed_keys, keys.len());
    drop(shards);
    for key in &keys {
        assert_eq!(value(&engine, key), None);
    }
    for key in &survivors {
        assert_eq!(value(&engine, key), Some(b"keep".to_vec()));
    }
    engine.wal_store.flush(1).unwrap();
    drop(engine);
    let restored = engine_at(dir.path(), "indexes");
    assert!(restored.load_shard_with(load).status.ok);
    for key in &keys {
        assert_eq!(value(&restored, key), None);
    }
    for key in &survivors {
        assert_eq!(value(&restored, key), Some(b"keep".to_vec()));
    }
    (builds, visits, appends)
}

#[test]
fn dense_bucket_deletions_reuse_owner_names_across_logged_frames() {
    // The path that names the bucket: used when no object-block lookup is retained.
    let (builds, visits, appends) = dense_bucket_deletions(false);
    assert_eq!(appends, 3);
    assert_eq!(builds, 8, "each touched bucket is named once, not once per deletion");
    assert!(visits <= 513 + 32, "owner visits={visits}");
    println!("dense deletion capture, naming: buckets=8 builds={builds} visits={visits} frames={appends}");
}

#[test]
fn dense_bucket_deletions_by_handle_name_no_bucket() {
    let (builds, visits, appends) = dense_bucket_deletions(true);
    assert_eq!(appends, 3);
    assert_eq!(builds, 0, "a delete by handle names no bucket");
    assert_eq!(visits, 0, "a delete by handle visits no owner");
    println!("dense deletion capture, by handle: buckets=8 builds={builds} visits={visits} frames={appends}");
}

fn deletion_round_replays(expire: bool) {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path(), "indexes");
    engine.load_shard(1);
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage: true,
                    ..Default::default()
                },
            })
            .ok
    );
    let keys = (0..513)
        .map(|i| format!("delete-frame-{i:04}"))
        .collect::<Vec<_>>();
    for key in &keys {
        write(&engine, key, b"old");
    }
    let family = control_state_family_key(ControlStateFamily::Counter, &keys[0]);
    write(&engine, &family, b"family");
    if expire {
        write(&engine, "survivor", b"keep");
        let mut shards = engine.shards.write().unwrap();
        let shard = shards.get_mut(&1).unwrap();
        for key in &keys {
            set_expiry(shard, key.clone(), now_ms().saturating_sub(1));
        }
    }
    let before = engine.wal_store.stats_without_rescan(1).last_sequence;
    if expire {
        let report = engine
            .sweep_expired_records_with_request(ShardExpirySweepRequest {
                shard_id: 1,
                max_hot_buckets_per_round: 1024,
                max_cold_buckets_per_round: 1024,
                load_cold_buckets: true,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(report.expired_records_removed, keys.len());
    } else {
        let report = engine.apply_storage_eviction(1, 0, 0, false, true);
        assert!(report.dropped_object_count >= keys.len());
        assert!(report.skipped_reason.is_empty());
    }
    let after = engine.wal_store.stats_without_rescan(1).last_sequence;
    let (records, truncated) = engine
        .wal_store
        .scan_decoded(1, 0, u64::MAX, u64::MAX)
        .unwrap();
    assert!(!truncated);
    let deletion_records = records
        .iter()
        .filter(|(_, record)| record.sequence > before)
        .collect::<Vec<_>>();
    assert_eq!(deletion_records.len(), 3);
    assert_eq!(after - before, 3);
    let mut removed = BTreeSet::new();
    for (_, record) in deletion_records {
        assert!(record.command.is_none());
        assert!(!record.outcomes.is_empty() && record.outcomes.len() <= MAX_DELETION_ITEMS);
        for item in &record.outcomes {
            assert_eq!(item.kind, "record");
            assert!(item.deleted && item.meta && item.address.is_none());
            assert_eq!(
                item.routing_bucket,
                engine.routing_bucket_for_key(1, &item.object_key)
            );
            removed.insert(item.object_key.clone());
        }
    }
    assert_eq!(
        removed,
        keys.iter().cloned().chain((!expire).then(|| family.clone())).collect()
    );
    if expire {
        let deltas = engine.index_log_store.read_delta_records(1, 0).unwrap();
        let delta = deltas.last().expect("expiry checkpoint");
        let covered: BTreeSet<String> = delta.key_states.iter()
            .map(|state| state["key"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(covered, removed, "checkpoint and WAL must name the same records");
        assert_eq!(delta.applied_wal_sequence, Some(after));
    }
    write(&engine, &keys[0], b"new");
    engine.wal_store.flush(1).unwrap();
    drop(engine);

    // Recover from the actual WAL files without any base or delta checkpoint.
    let mut restored = engine_at(dir.path(), "empty-indexes");
    restored.wal_store = LocalWriteAheadLogStore::new(dir.path().join("indexes/wals"));
    restored.load_shard(1);
    assert_eq!(value(&restored, &keys[0]), Some(b"new".to_vec()));
    for key in &keys[1..] {
        assert_eq!(value(&restored, key), None);
    }
    assert_eq!(value(&restored, &family), expire.then(|| b"family".to_vec()));
    if expire {
        assert_eq!(value(&restored, "survivor"), Some(b"keep".to_vec()));
    }
}

#[test]
fn maintenance_log_batches_expiry_results_with_bucket_ownership() {
    deletion_round_replays(true);
}

#[test]
fn maintenance_log_batches_delete_eviction_and_preserves_later_writes() {
    deletion_round_replays(false);
}

#[test]
fn maintenance_log_failure_leaves_unlogged_records_live() {
    for expire in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = engine_at(dir.path(), "indexes");
        engine.load_shard(1);
        write(&engine, "victim", b"old");
        engine.flush_shard_index(1);
        let base = std::fs::read(engine.index_path(1)).unwrap();
        let blocked = dir.path().join("not-a-directory");
        std::fs::write(&blocked, b"blocked").unwrap();
        let before = engine.shards.read().unwrap()[&1].checkpoint_wal_sequence();
        engine.wal_store = LocalWriteAheadLogStore::new(blocked);
        if expire {
            set_expiry(
                engine.shards.write().unwrap().get_mut(&1).unwrap(),
                "victim".to_string(),
                now_ms().saturating_sub(1),
            );
            let error = engine.sweep_expired_records(1).unwrap_err();
            assert_eq!(error.code, "maintenance_log_append_failed");
        } else {
            let report = engine.apply_storage_eviction(1, 0, 0, false, true);
            assert_eq!(report.skipped_reason, "maintenance_log_append_failed");
            assert_eq!(report.dropped_object_count, 0);
        }
        assert_eq!(
            engine.shards.read().unwrap()[&1].checkpoint_wal_sequence(),
            before
        );
        assert!(engine.shards.read().unwrap()[&1]
            .strings()
            .contains_key("victim"));
        let before_image = decode_index_bytes(&base).unwrap();
        let after_image =
            decode_index_bytes(&std::fs::read(engine.index_path(1)).unwrap()).unwrap();
        assert_eq!(
            after_image.applied_wal_sequence,
            before_image.applied_wal_sequence
        );
        assert_eq!(
            after_image.string_address("victim"),
            before_image.string_address("victim")
        );
        drop(engine);
        let restored = engine_at(dir.path(), "indexes");
        restored.load_shard(1);
        assert_eq!(value(&restored, "victim"), Some(b"old".to_vec()));
    }
}

#[test]
fn maintenance_log_byte_target_keeps_related_records_together() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path(), "indexes");
    engine.load_shard(1);
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage: true,
                    ..Default::default()
                },
            })
            .ok
    );
    let keys = (0..32)
        .map(|i| format!("long-{i:02}-{}", "x".repeat(16 << 10)))
        .collect::<Vec<_>>();
    for key in &keys {
        write(&engine, key, b"old");
    }
    let families = [
        ControlStateFamily::Counter,
        ControlStateFamily::Distinct,
        ControlStateFamily::Selection,
    ]
    .into_iter()
    .map(|family| control_state_family_key(family, &keys[0]))
    .collect::<Vec<_>>();
    for key in &families {
        write(&engine, key, b"family");
    }
    let before = engine.wal_store.stats_without_rescan(1).last_sequence;
    let mut shards = engine.shards.write().unwrap();
    let shard = shards.get_mut(&1).unwrap();
    let deletions = keys
        .iter()
        .filter_map(|key| MaintenanceDeletion::capture(shard, key))
        .collect();
    let result = engine.append_maintenance_deletions(1, shard, deletions, || {});
    assert!(result.error.is_none());
    assert_eq!(result.removed_keys, keys.len());
    drop(shards);
    let (records, _) = engine
        .wal_store
        .scan_decoded(1, 0, u64::MAX, u64::MAX)
        .unwrap();
    let batches = records
        .iter()
        .filter(|(_, record)| record.sequence > before)
        .collect::<Vec<_>>();
    assert_eq!(batches.len(), 3);
    assert!(batches.iter().all(|(_, record)| record
        .outcomes
        .iter()
        .map(|item| item.object_key.len() + 64)
        .sum::<usize>()
        <= TARGET_DELETION_BYTES));
    let family_frame = batches
        .iter()
        .find(|(_, record)| {
            record
                .outcomes
                .iter()
                .any(|item| item.object_key == keys[0])
        })
        .unwrap();
    for key in &families {
        assert!(family_frame
            .1
            .outcomes
            .iter()
            .any(|item| &item.object_key == key));
    }
}

#[test]
fn maintenance_log_partial_failure_applies_and_mirrors_only_complete_frames() {
    #[derive(Debug, Default)]
    struct Mirror(std::sync::Mutex<Vec<String>>);
    impl crate::data_node::SharedWalSink for Mirror {
        fn record_write(&self, _: ShardId, _: &Command) {
            panic!("expected exact outcomes");
        }
        fn record_outcomes(&self, _: ShardId, outcomes: &[crate::wal::WalOutcomeItem]) {
            self.0.lock().unwrap().extend(outcomes.iter().map(|item| item.object_key.clone()));
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path(), "indexes");
    engine.load_shard(1);
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage: true,
                    ..Default::default()
                },
            })
            .ok
    );
    let keys = (0..513)
        .map(|i| format!("partial-{i:04}"))
        .collect::<Vec<_>>();
    for key in &keys {
        write(&engine, key, b"old");
    }
    let before = engine.wal_store.stats_without_rescan(1).last_sequence;
    let mirror = std::sync::Arc::new(Mirror::default());
    engine.set_maintenance_wal_mirror(mirror.clone());
    FAIL_APPEND_AFTER.with(|remaining| remaining.set(Some(1)));
    let report = engine.apply_storage_eviction(1, 0, 0, false, true);
    assert_eq!(report.dropped_object_count, MAX_DELETION_ITEMS);
    assert_eq!(report.skipped_reason, "maintenance_log_append_failed");
    assert_eq!(
        engine.shards.read().unwrap()[&1].checkpoint_wal_sequence(),
        Some(before + 1)
    );
    assert_eq!(*mirror.0.lock().unwrap(), keys[..MAX_DELETION_ITEMS]);
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(
            value(&engine, key),
            (index >= MAX_DELETION_ITEMS).then(|| b"old".to_vec())
        );
    }
    engine.wal_store.flush(1).unwrap();
    drop(engine);
    let restored = engine_at(dir.path(), "indexes");
    restored.load_shard(1);
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(
            value(&restored, key),
            (index >= MAX_DELETION_ITEMS).then(|| b"old".to_vec())
        );
    }
}

#[test]
fn maintenance_log_replay_counts_overlapping_families_once() {
    for expire in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_at(dir.path(), "indexes");
        engine.load_shard(1);
        let family = control_state_family_key(ControlStateFamily::Counter, "aaa");
        write(&engine, "aaa", b"base");
        write(&engine, &family, b"family");
        if expire {
            let mut shards = engine.shards.write().unwrap();
            let shard = shards.get_mut(&1).unwrap();
            let due = now_ms().saturating_sub(20);
            set_expiry(shard, "aaa".to_string(), due);
            set_expiry(shard, family.clone(), due + 1);
        }
        let before = engine.wal_store.stats_without_rescan(1).last_sequence;
        let _replay = WalReplayGuard::enter();
        if expire {
            let report = engine
                .sweep_expired_records_with_request(ShardExpirySweepRequest {
                    shard_id: 1,
                    load_cold_buckets: true,
                    max_hot_buckets_per_round: 100,
                    max_cold_buckets_per_round: 100,
                    ..Default::default()
                })
                .unwrap();
            assert_eq!(report.expired_records_removed, 2);
        } else {
            assert_eq!(
                engine
                    .apply_storage_eviction(1, 0, 0, false, true)
                    .dropped_object_count,
                1
            );
        }
        assert_eq!(
            engine.wal_store.stats_without_rescan(1).last_sequence,
            before
        );
        let shards = engine.shards.read().unwrap();
        assert!(!shards[&1].strings().contains_key("aaa"));
        assert!(!shards[&1].strings().contains_key(family.as_str()));
    }
}
