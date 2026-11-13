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

#[test]
fn one_identity_capture_covers_multiple_logged_deletion_frames() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path(), "indexes");
    engine.load_shard(1);
    let keys = (0..513)
        .map(|n| format!("captured-delete-{n:04}"))
        .collect::<Vec<_>>();
    for key in &keys {
        write(&engine, key, b"remove");
    }
    write(&engine, "captured-survivor", b"keep");
    let mut shards = engine.shards.write().unwrap();
    let shard = shards.get_mut(&1).unwrap();
    let deletions = keys
        .iter()
        .map(|key| MaintenanceDeletion::capture(shard, key).unwrap())
        .collect();
    let mut appends = 0;
    let (result, builds) = state::measure_block_identity_builds_for_test(|| {
        engine.append_maintenance_deletions(1, shard, deletions, || appends += 1)
    });
    assert!(result.error.is_none());
    assert_eq!(result.removed_keys, keys.len());
    assert_eq!(appends, 3);
    assert_eq!(
        builds, 1,
        "the deletion-only run must reuse its first identity capture"
    );
    drop(shards);
    for key in &keys {
        assert_eq!(value(&engine, key), None);
    }
    assert_eq!(value(&engine, "captured-survivor"), Some(b"keep".to_vec()));
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
        keys.iter().cloned().chain([family.clone()]).collect()
    );
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
    assert_eq!(value(&restored, &family), None);
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
        fn record_write(&self, _: ShardId, command: &Command) {
            let Command::CommonDelete { key } = command else {
                panic!("expected delete");
            };
            self.0.lock().unwrap().push(key.clone());
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
            assert_eq!(report.expired_records_removed, 1);
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
