// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

#[test]
fn topology_deleted_samples_need_no_live_identity_inversion() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    for command in [
        Command::SetAdd {
            key: "order".into(),
            member: b"alive".to_vec(),
        },
        Command::SetAdd {
            key: "order".into(),
            member: b"gone".to_vec(),
        },
        Command::SetRemove {
            key: "order".into(),
            member: b"gone".to_vec(),
        },
    ] {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command
                })
                .status
                .ok
        );
    }
    let shards = engine.shards.read().unwrap();
    let shard = shards.get(&1).unwrap();
    let entries = collect_live_block_entries(shard);
    let (snapshot, builds) = crate::engine::state::measure_block_identity_builds_for_test(|| {
        storage_topology_snapshot_with_samples_from_entries(1, shard, &entries, Default::default())
    });
    assert_eq!(builds, 0);
    assert!(snapshot.bucket_samples.iter().any(|bucket| bucket
        .delete_markers
        .iter()
        .any(|marker| marker == "set:order")));
}

fn open(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn seed(engine: &TemporalEngine, count: usize) {
    for i in 0..count {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("key-{i}"),
                value: vec![i as u8; 96],
            },
        });
        assert!(response.status.ok);
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: format!("key-{i}"),
            },
        });
        assert!(response.status.ok);
    }
}

#[test]
fn operational_lifecycle_omits_only_the_two_diagnostic_identity_walks() {
    for count in [32, 512] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path());
        seed(&engine, count);
        let request = StorageLifecycleRequest {
            shard_id: 1,
            min_undumped_wal_records: u64::MAX,
            min_undumped_wal_bytes: u64::MAX,
            ..Default::default()
        };
        let (operational, short_walks) =
            crate::engine::state::measure_block_identity_builds_for_test(|| {
                engine.apply_storage_lifecycle_with_report_details(request.clone(), false)
            });
        let (detailed, full_walks) =
            crate::engine::state::measure_block_identity_builds_for_test(|| {
                engine.apply_storage_lifecycle(request)
            });
        assert_eq!(
            operational.plan.selected_dump_buckets,
            detailed.plan.selected_dump_buckets
        );
        assert_eq!(operational.plan.dirty_buckets, detailed.plan.dirty_buckets);
        assert!(operational.dump_manifest.is_none());
        assert!(detailed.dump_manifest.is_none());
        assert!(operational
            .storage_index_snapshot
            .page_index_entry_samples
            .is_empty());
        assert!(!detailed
            .storage_index_snapshot
            .page_index_entry_samples
            .is_empty());
        assert!(
            full_walks >= short_walks + 2,
            "full={full_walks} operational={short_walks}"
        );
        println!("records={count} full_identity_builds={full_walks} operational_identity_builds={short_walks}");
    }
}

#[test]
fn operational_and_detailed_lifecycle_both_dump_invalidate_warm_and_recover() {
    for detailed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path());
        seed(&engine, 32);
        let report = engine.apply_storage_lifecycle_with_report_details(
            StorageLifecycleRequest {
                shard_id: 1,
                min_undumped_wal_records: 1,
                invalidate_cache: true,
                warm_cache: true,
                ..Default::default()
            },
            detailed,
        );
        assert!(report
            .dump_manifest
            .as_ref()
            .is_some_and(|manifest| !manifest.bucket_ids.is_empty()));
        assert!(report.cache_entries_removed > 0);
        assert!(report.cache_warmup_block_refs > 0);
        drop(engine);
        let recovered = open(dir.path());
        for i in 0..32 {
            let response = recovered.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: format!("key-{i}"),
                },
            });
            assert!(response.status.ok);
            assert!(
                matches!(response.response, CommandResponse::Bytes { value: Some(value) } if value == vec![i as u8; 96])
            );
        }
    }
}

#[test]
fn operational_dump_keeps_follower_and_snapshot_index_gc_fences() {
    for (follower_sequence, snapshot_sequence) in [(1, 7), (7, 1)] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path());
        seed(&engine, 32);
        let request = StorageLifecycleRequest {
            shard_id: 1,
            min_undumped_wal_records: 1,
            follower_replay_cursors: vec![BucketDumpFollowerReplayCursor {
                follower_id: "reader".into(),
                shard_id: 1,
                wal_sequence: follower_sequence,
                index_log_sequence: follower_sequence,
            }],
            block_gc_raft_snapshot_refs: vec![BucketDumpRaftSnapshotRef {
                snapshot_id: "snapshot".into(),
                shard_id: 1,
                wal_sequence: snapshot_sequence,
                index_log_sequence: snapshot_sequence,
                ..Default::default()
            }],
            ..Default::default()
        };
        let plan = engine.storage_lifecycle_plan(request.clone());
        assert!(!plan.selected_dump_buckets.is_empty());
        let operational =
            engine.apply_storage_lifecycle_with_report_details(request.clone(), false);
        assert!(operational.dump_manifest.is_some());
        let wal_plan = engine.storage_wal_reclaim_plan(
            1,
            request.follower_replay_cursors,
            request.block_gc_raft_snapshot_refs,
        );
        let cycle = StorageManagerCycleRequest {
            shard_id: 1,
            enable_index_gc: true,
            dry_run: false,
            index_gc_index_log_bytes_threshold: 0,
            index_gc_usage_ratio_trigger_basis_points: 0,
            index_gc_commit_dirty_buckets_before_truncation: true,
            ..Default::default()
        };
        let protected = engine.index_log_store.read_delta_records(1, 1).unwrap();
        assert!(!protected.is_empty());
        let blocked = engine.storage_index_gc_report(&plan, &wal_plan, None, &cycle);
        assert!(!blocked.safe_to_truncate);
        assert!(!blocked.applied);
        let gc = engine.storage_index_gc_report(&plan, &wal_plan, Some(&operational), &cycle);
        assert!(gc.dirty_buckets_committed_before_truncate);
        assert!(gc.threshold_triggered && gc.usage_ratio_triggered);
        assert!(gc.safe_to_truncate);
        assert_eq!(gc.retain_from_index_log_sequence, 2);
        assert!(gc.records_removed <= 1);
        assert_eq!(
            engine.index_log_store.read_delta_records(1, 1).unwrap(),
            protected
        );
    }
}
