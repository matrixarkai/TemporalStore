// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Part 4 of engine tests, split from engine/tests.rs.
#![allow(clippy::all)]
use super::*;

#[test]
fn object_manager_runtime_report_tracks_residency_layout_and_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 10,
        end_routing_bucket: 12,
        readonly: false,
        table_name: String::new(),
    });

    for command in [
        Command::StringSet {
            key: "object-a".to_string(),
            value: b"a".to_vec(),
        },
        Command::HashSet {
            key: "hash-a".to_string(),
            field: "field-a".to_string(),
            value: b"field".to_vec(),
        },
        Command::FeatureReplace {
            key: "feature-a".to_string(),
            start_ms: 0,
            end_ms: 100,
            points: vec![FeaturePoint {
                timestamp_ms: 42,
                value: b"42".to_vec(),
            }],
        },
        Command::StringSet {
            key: "object-delete".to_string(),
            value: b"delete".to_vec(),
        },
        Command::CommonDelete {
            key: "object-delete".to_string(),
        },
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "{response:?}");
    }

    let report = engine.object_manager_runtime_report(1);
    assert!(report.runtime_ready, "{report:?}");
    assert!(report.routing_bucket_count >= 1);
    assert!(report.object_count >= 4);
    assert!(report.page_ref_count >= 3);
    // Freshly written, materialized, in-memory pages are hot (not log-backed), so
    // their objects are hot; cold residency only applies to reloaded-from-disk pages.
    assert!(report.hot_object_count >= 3);
    assert!(report.tombstone_object_count >= 1);
    assert!(report.dirty_object_count >= 4);
    assert!(report.dirty_bucket_count >= 1);
    assert!(report.max_dirty_generation >= 1);
    assert!(report.object_page_count >= 2);
    assert_eq!(report.missing_owner_page_ref_count, 0);
    assert_eq!(report.owner_mismatch_page_ref_count, 0);
    assert_eq!(report.reused_object_id_conflict_count, 0);
    assert!(report.blockers.is_empty());
    assert!(report
        .evidence
        .iter()
        .any(|item| item.contains("hot/cold/tombstone object state")));

    let compaction = engine.compact_shard_pages(1).unwrap();
    assert!(compaction.model_layout_compaction_ready, "{compaction:?}");
    let after_compaction = engine.object_manager_runtime_report(1);
    assert!(after_compaction.runtime_ready, "{after_compaction:?}");
    assert!(after_compaction.object_count >= report.object_count.saturating_sub(1));
    assert_eq!(after_compaction.missing_owner_page_ref_count, 0);
    assert_eq!(after_compaction.owner_mismatch_page_ref_count, 0);
    assert!(after_compaction.object_page_count >= 1);

    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("slot dump manifest should persist");
    engine
        .install_bucket_dump_manifest(&manifest)
        .expect("slot dump manifest should install");
    let after_dump_load = engine.object_manager_runtime_report(1);
    assert!(after_dump_load.runtime_ready, "{after_dump_load:?}");
    assert_eq!(after_dump_load.object_count, after_compaction.object_count);
    assert_eq!(
        after_dump_load.tombstone_object_count,
        after_compaction.tombstone_object_count
    );

    engine.unload_shard(1);
    engine.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 1,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 10,
        end_routing_bucket: 12,
        readonly: false,
        table_name: String::new(),
    });
    let reloaded = engine.object_manager_runtime_report(1);
    assert!(reloaded.runtime_ready, "{reloaded:?}");
    assert_eq!(reloaded.object_count, after_dump_load.object_count);
    assert_eq!(reloaded.page_ref_count, after_dump_load.page_ref_count);
    assert_eq!(
        reloaded.tombstone_object_count,
        after_dump_load.tombstone_object_count
    );
    assert_eq!(
        reloaded.object_page_count,
        after_dump_load.object_page_count
    );
}

// shared-corpus: native_storage_object_page_bucket_parity_surfaces;
#[test]
fn object_manager_runtime_report_tracks_residency_layout_and_tombstones_parity() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 10,
        end_routing_bucket: 12,
        readonly: false,
        table_name: String::new(),
    });

    for command in [
        Command::StringSet {
            key: "object-a".to_string(),
            value: b"a".to_vec(),
        },
        Command::HashSet {
            key: "hash-a".to_string(),
            field: "field-a".to_string(),
            value: b"field".to_vec(),
        },
        Command::FeatureReplace {
            key: "feature-a".to_string(),
            start_ms: 0,
            end_ms: 100,
            points: vec![FeaturePoint {
                timestamp_ms: 42,
                value: b"42".to_vec(),
            }],
        },
        Command::StringSet {
            key: "object-delete".to_string(),
            value: b"delete".to_vec(),
        },
        Command::CommonDelete {
            key: "object-delete".to_string(),
        },
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "{response:?}");
    }

    let report = engine.object_manager_runtime_report(1);
    assert!(report.runtime_ready, "{report:?}");
    assert!(report.routing_bucket_count >= 1);
    assert!(report.object_count >= 4);
    assert!(report.page_ref_count >= 3);
    // Freshly written, materialized, in-memory pages are hot (not log-backed), so
    // their objects are hot; cold residency only applies to reloaded-from-disk pages.
    assert!(report.hot_object_count >= 3);
    assert!(report.tombstone_object_count >= 1);
    assert!(report.dirty_object_count >= 4);
    assert!(report.dirty_bucket_count >= 1);
    assert!(report.max_dirty_generation >= 1);
    assert!(report.object_page_count >= 2);
    assert!(report.packed_timestamped_page_count >= 1);
    assert_eq!(report.missing_owner_page_ref_count, 0);
    assert_eq!(report.owner_mismatch_page_ref_count, 0);
    assert_eq!(report.reused_object_id_conflict_count, 0);
    assert!(report.blockers.is_empty());
    assert!(report
        .evidence
        .iter()
        .any(|item| item.contains("hot/cold/tombstone object state")));

    engine.unload_shard(1);
    engine.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 1,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 10,
        end_routing_bucket: 12,
        readonly: false,
        table_name: String::new(),
    });
    let reloaded = engine.object_manager_runtime_report(1);
    assert!(reloaded.runtime_ready, "{reloaded:?}");
    assert_eq!(reloaded.object_count, report.object_count);
    assert_eq!(reloaded.page_ref_count, report.page_ref_count);
    assert_eq!(
        reloaded.tombstone_object_count,
        report.tombstone_object_count
    );
    assert_eq!(
        reloaded.packed_timestamped_page_count,
        report.packed_timestamped_page_count
    );
}

#[test]
fn bucket_dump_manifest_validation_rejects_checksum_and_missing_slabs() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
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
    let mut manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    manifest.logical_bytes = manifest.logical_bytes.saturating_add(1);
    assert!(
        !engine
            .validate_bucket_dump_manifest(&manifest)
            .unwrap_err()
            .ok
    );

    let mut missing = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    missing.page_slab_ids.push(999_999);
    missing.checksum = bucket_dump_manifest_checksum(&missing).unwrap();
    let missing_preflight = engine.bucket_dump_install_preflight_report(&missing);
    assert!(!missing_preflight.install_safe);
    assert_eq!(missing_preflight.missing_page_slab_ids, vec![999_999]);
    assert!(missing_preflight
        .blockers
        .contains(&"missing_page_segments".to_string()));
    assert!(!engine.validate_bucket_dump_manifest(&missing).unwrap_err().ok);

    let mut incomplete = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    incomplete.page_slab_ids.clear();
    incomplete.checksum = bucket_dump_manifest_checksum(&incomplete).unwrap();
    assert_eq!(
        engine
            .validate_bucket_dump_manifest(&incomplete)
            .unwrap_err()
            .code,
        "slot_dump_page_segment_mismatch"
    );

    let corrupt = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    let slab_id = corrupt.page_slab_ids[0];
    let mut slab = engine.block_store().read_slab(slab_id).unwrap();
    *slab.last_mut().unwrap() ^= 0xff;
    let _ = engine.block_store().install_slab(slab_id, &slab);
    let corrupt_preflight = engine.bucket_dump_install_preflight_report(&corrupt);
    assert!(!corrupt_preflight.install_safe);
    assert!(corrupt_preflight
        .corrupt_page_slab_ids
        .contains(&slab_id));
    assert!(corrupt_preflight.unreadable_page_ref_count > 0);
    assert!(corrupt_preflight.unreadable_page_bytes > 0);
    assert!(corrupt_preflight
        .blockers
        .contains(&"unreadable_page_refs".to_string()));
    assert_eq!(
        engine
            .validate_bucket_dump_manifest(&corrupt)
            .unwrap_err()
            .code,
        "slot_dump_unreadable_page_refs"
    );
}

#[test]
fn bucket_dump_manifest_watermark_tracks_embedded_index_not_wal_tail() {
    // Regression for silent data loss. create_bucket_dump_manifest embeds the on-disk index, but
    // under MATRIXARK_BULK_INGEST that index lags the WAL tail (per-command index persist is a
    // no-op in bulk mode). It used to stamp the manifest's wal_sequence from the LIVE WAL tail,
    // so on reload install set replay_watermark past records the embedded index never captured
    // and WAL replay skipped them -> gone. couples the two: Load replays from the DumpedLogId
    // stored inside the dumped index (object_manager.cc). We reproduce the same tail-ahead-of-index
    // divergence deterministically (no bulk env) by appending straight to the WAL.
    let dir = tempfile::tempdir().unwrap();
    let pages = dir.path().join("pages");
    let indexes = dir.path().join("indexes");
    let engine =
        TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-a"), &pages, &indexes);
    engine.load_shard(1);
    // Applied + persisted normally: on-disk index anchored to WAL seq 1, containing k1.
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k1".to_string(),
            value: b"v1".to_vec(),
        },
    });
    // Advance the WAL tail past the persisted index anchor WITHOUT touching the on-disk index --
    // exactly the state bulk mode produces (deferred index persist, live WAL).
    engine
        .write_ahead_log_store()
        .append_with_sync(
            1,
            Command::StringSet {
                key: "k2".to_string(),
                value: b"v2".to_vec(),
            },
            true,
        )
        .expect("direct wal append");
    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    assert_eq!(
        manifest.wal_sequence, 1,
        "manifest watermark must track the embedded index anchor (1), not the live WAL tail (2)"
    );

    // Fresh engine over the SAME pages+index dirs: load installs the manifest + replays the WAL.
    let reloaded =
        TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-b"), &pages, &indexes);
    reloaded.load_shard(1);
    assert_eq!(
        reloaded
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "k2".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"v2".to_vec())
        },
        "a WAL record past the embedded index anchor must be replayed on reload, not skipped"
    );
    assert_eq!(
        reloaded
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "k1".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"v1".to_vec())
        },
    );
}

#[test]
fn reversed_range_bounds_return_empty_not_a_lock_poisoning_panic() {
    // BTreeMap::range PANICS when start > end, and range queries run under the shard write lock,
    // so a reversed-bounds query would poison the lock and take the whole shard down (every later
    // lock().expect() panics). RangeGet returns an empty result with OK for min > max. Assert
    // reversed bounds return empty AND leave the engine usable.
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
        command: Command::FeatureAppend {
            key: "f".to_string(),
            points: vec![FeaturePoint {
                timestamp_ms: 20,
                value: b"x".to_vec(),
            }],
        },
    });
    let feat = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery {
            key: "f".to_string(),
            start_ms: 100,
            end_ms: 50,
            count: None,
        },
    });
    assert_eq!(
        feat.response,
        CommandResponse::FeaturePoints { points: vec![] },
        "a reversed-bounds feature range must return empty, not panic"
    );
    // The shard lock must not be poisoned: the engine stays usable.
    let ok = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"1".to_vec(),
        },
    });
    assert!(
        ok.status.ok,
        "engine must stay usable after a reversed-bounds query (lock not poisoned): {:?}",
        ok.status
    );
}

#[test]
fn batch_execute_on_unloaded_shard_returns_a_batch_level_topology_error() {
    // returns a batch-level topology error with ZERO response entries when the partition is
    // not loaded (partition_manager.cc), so the topology-retryable client refreshes + retries.
    // Rust previously returned an OK batch full of per-command shard_not_loaded errors, leaving the
    // batch-level status ok, so the client (which keys retry on the batch status) never refreshed.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    // Shard 1 is deliberately NOT loaded.
    let response = engine.batch_execute(BatchExecuteRequest {
        shard_id: 1,
        commands: vec![
            Command::StringSet {
                key: "a".to_string(),
                value: b"1".to_vec(),
            },
            Command::StringSet {
                key: "b".to_string(),
                value: b"2".to_vec(),
            },
        ],
    });
    assert_eq!(
        response.status.code, "shard_not_loaded",
        "an unloaded-shard batch must fail at the batch level, got {:?}",
        response.status
    );
    assert!(
        response.responses.is_empty(),
        "a batch-level topology failure must carry zero per-command responses, got {}",
        response.responses.len()
    );
}

#[test]
fn dump_selection_prioritizes_the_least_recently_dumped_bucket_not_the_lowest_id() {
    // The WAL-reclaim routine dumps dirty slots oldest-first (by first-dirty-log-id,
    // storage_manager.cc). Rust selected dirty buckets by ascending routing_bucket id then
    // truncated to the per-round cap, so a high-id bucket dirtied once was starved forever by
    // low-id buckets re-dirtied every round. The fix orders by last_dump_sequence (0 = never
    // dumped) ascending. Here we dump the LOW-id bucket (raising its last_dump_sequence) then
    // re-dirty it; a cap-1 plan must now pick the never-dumped HIGH-id bucket, not the low-id one.
    use std::collections::BTreeMap;
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    // Find one key in each of two distinct routing buckets.
    let mut key_by_bucket: BTreeMap<u32, String> = BTreeMap::new();
    for i in 0..200 {
        let key = format!("k{i}");
        let bucket = engine.routing_bucket_for_key(1, &key);
        key_by_bucket.entry(bucket).or_insert(key);
        if key_by_bucket.len() >= 2 {
            break;
        }
    }
    assert!(key_by_bucket.len() >= 2, "need two distinct routing buckets");
    let mut buckets = key_by_bucket.into_iter();
    let (low_bucket, low_key) = buckets.next().unwrap();
    let (high_bucket, high_key) = buckets.next().unwrap();

    for key in [&low_key, &high_key] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.clone(),
                value: b"v1".to_vec(),
            },
        });
    }
    // Dump ONLY the low-id bucket: raises its last_dump_sequence and clears its dirty flag.
    engine.apply_storage_lifecycle(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: vec![low_bucket],
        ..Default::default()
    });
    // Re-dirty the low-id bucket so both buckets are dirty, but low was just dumped.
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: low_key.clone(),
            value: b"v2".to_vec(),
        },
    });

    let plan = engine.storage_lifecycle_plan(StorageLifecycleRequest {
        shard_id: 1,
        max_dump_buckets_per_round: 1,
        ..Default::default()
    });
    assert_eq!(
        plan.selected_dump_buckets,
        vec![high_bucket],
        "the overdue never-dumped bucket ({high_bucket}) must be selected before the just-dumped \
         low-id bucket ({low_bucket}) under the per-round cap"
    );
}

#[test]
fn delete_drop_eviction_emits_a_wal_tombstone_and_does_not_resurrect() {
    // A delete_drop eviction is a logical delete and must emit a WAL tombstone + advance the
    // replay anchor like the expiry sweep, or the deletion is unreplicated and (under bulk mode)
    // resurrects on reload. Assert the eviction appends a CommonDelete to the WAL (observable as a
    // WAL sequence advance) and that a reload -- which replays the WAL past the anchor -- does not
    // bring the key back.
    let dir = tempfile::tempdir().unwrap();
    let pages = dir.path().join("pages");
    let indexes = dir.path().join("indexes");
    let engine =
        TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-a"), &pages, &indexes);
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "evict-me".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let wal_before = engine.write_ahead_log_store().stats(1).last_sequence;
    // memory_pressure_threshold = 0 forces the eviction to run; delete_drop = true.
    let report = engine.apply_storage_eviction(1, 0, 1024, false, true);
    assert!(
        report.dropped_object_count >= 1,
        "delete_drop eviction should have dropped the record: {report:?}"
    );
    let wal_after = engine.write_ahead_log_store().stats(1).last_sequence;
    assert!(
        wal_after > wal_before,
        "delete_drop must emit a WAL tombstone (sequence must advance): {wal_before} -> {wal_after}"
    );

    // Reload over the same dirs: WAL replay past the anchor must NOT resurrect the deleted key.
    let reloaded =
        TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-b"), &pages, &indexes);
    reloaded.load_shard(1);
    assert_eq!(
        reloaded
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "evict-me".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes { value: None },
        "a delete_drop-evicted key must stay deleted after reload, not resurrect"
    );
}

#[test]
fn partial_compaction_failure_durably_persists_the_consistent_partial_index() {
    // CP4 regression. A mid-compaction read failure used to return with the in-memory index
    // half-advanced (relocated pages point at the fresh slab) but UNPERSISTED, so it diverged from
    // the on-disk index -- and the independent reclaim path could then physically purge a
    // fully-vacated old slab the durable index still referenced -> silent data loss on reload. The
    // fix durably commits the consistent partial state before propagating the error. avoids the
    // desync structurally (page_compactor.cc: update_index=false on failure + one atomic Commit).
    //
    // Setup: k1 (string, relocated FIRST by compact_shard_pages) lives in an earlier slab; k2 (hash,
    // relocated after) lives in a later slab. We corrupt ONLY the latest slab (k2's), so compaction
    // relocates k1 (vacating its old slab) and then FAILS reading k2. A tiny cache forces the reads
    // to hit disk. Assert the on-disk index CHANGED -- i.e. the relocated partial state was durably
    // persisted (it stays byte-identical without the fix).
    let dir = tempfile::tempdir().unwrap();
    let pages = dir.path().join("pages");
    let indexes = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(16, dir.path().join("cache"), &pages, &indexes);
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k1".to_string(),
            value: b"v1".to_vec(),
        },
    });
    // Force k2 into a later slab than k1 so corrupting it cannot touch k1's slab.
    engine.block_store().roll_slab().unwrap();
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashSet {
            key: "h".to_string(),
            field: "f".to_string(),
            value: b"v2".to_vec(),
        },
    });

    let index_before = fs::read(engine.index_path(1)).unwrap();

    // Corrupt only the newest page slab (k2's): truncate it to empty so k2's page cannot be read.
    let mut slabs = fs::read_dir(&pages)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.starts_with("page_segment_") && name.ends_with(".seg"))
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    slabs.sort();
    let k2_slab = slabs.last().expect("at least two page slabs after the roll").clone();
    fs::write(&k2_slab, b"").unwrap();

    let result = engine.compact_shard_pages(1);
    assert!(
        result.is_err(),
        "compaction must fail when a live page's slab is unreadable, got {result:?}"
    );
    let index_after = fs::read(engine.index_path(1)).unwrap();
    assert_ne!(
        index_before, index_after,
        "a partial-failure compaction must durably persist the relocated partial index (CP4), \
         so the volatile and on-disk indexes cannot diverge and let reclaim purge a needed slab"
    );
}

#[test]
fn sync_write_surfaces_wal_commit_failure_instead_of_acking_ok() {
    // Durability parity: a synchronous write whose durable WAL commit fails must NOT be acked ok.
    // The WAL is the recovery source of truth, so a swallowed append error would tell the client a
    // write that is gone after a crash succeeded. surfaces the wal Commit failure
    // (partition.h OnExecuteCmdDone). We inject the failure by replacing the WAL file with a
    // directory so the next durable append cannot open it for writing (EISDIR fails even for
    // root, unlike a chmod which root bypasses).
    let dir = tempfile::tempdir().unwrap();
    let indexes = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("pages"),
        &indexes,
    );
    engine.load_shard(1);
    let baseline = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k1".to_string(),
            value: b"v1".to_vec(),
        },
    });
    assert!(
        baseline.status.ok,
        "baseline write should succeed: {:?}",
        baseline.status
    );
    let wal_path = indexes.join("wals").join("shard-1.wal.jsonl");
    fs::remove_file(&wal_path).unwrap();
    fs::create_dir(&wal_path).unwrap();
    let failed = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k2".to_string(),
            value: b"v2".to_vec(),
        },
    });
    assert_eq!(
        failed.status.code, "wal_commit_failed",
        "a sync write whose durable WAL commit failed must return an error, got {:?}",
        failed.status
    );
}

#[test]
fn bucket_dump_manifest_install_restores_index_and_rejects_partial_or_stale() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "restore-me".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");

    let restore_engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("restore-cache"),
        dir.path().join("pages"),
        dir.path().join("restore-indexes"),
    );
    restore_engine.load_shard(1);
    let safe_preflight = restore_engine.bucket_dump_install_preflight_report(&manifest);
    assert!(safe_preflight.install_safe, "{safe_preflight:?}");
    assert!(safe_preflight.blockers.is_empty());
    assert_eq!(
        safe_preflight.manifest_index_log_sequence,
        manifest.index_log_sequence
    );
    restore_engine
        .install_bucket_dump_manifest(&manifest)
        .expect("manifest should install");
    assert!(
        fs::read_dir(dir.path().join("restore-indexes"))
            .unwrap()
            .filter_map(Result::ok)
            .all(|entry| !entry.file_name().to_string_lossy().contains(".tmp-")),
        "slot dump install should not leave atomic index temp files"
    );
    assert!(restore_engine.interrupted_bucket_dump_installs(1).is_empty());
    let markers = list_bucket_dump_install_markers_at(&restore_engine.index_dir, 1).unwrap();
    assert!(markers.iter().any(|marker| marker.phase == "prepare"));
    assert!(markers.iter().any(|marker| marker.phase == "install"));
    assert!(markers.iter().any(|marker| marker.phase == "commit"));
    let response = restore_engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "restore-me".to_string(),
        },
    });
    assert_eq!(
        response.response,
        CommandResponse::Bytes {
            value: Some(b"v1".to_vec())
        }
    );

    let mut partial = manifest.clone();
    partial.index_bytes.clear();
    partial.checksum = bucket_dump_manifest_checksum(&partial).unwrap();
    assert_eq!(
        restore_engine
            .install_bucket_dump_manifest(&partial)
            .unwrap_err()
            .code,
        "slot_dump_partial_manifest"
    );

    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "newer".to_string(),
            value: b"v2".to_vec(),
        },
    });
    let stale_preflight = engine.bucket_dump_install_preflight_report(&manifest);
    assert!(!stale_preflight.install_safe);
    assert!(stale_preflight.stale_manifest);
    assert!(stale_preflight
        .blockers
        .contains(&"stale_manifest_sequence".to_string()));
    assert_eq!(
        engine
            .install_bucket_dump_manifest(&manifest)
            .unwrap_err()
            .code,
        "slot_dump_stale_manifest"
    );
}

// shared-corpus: storage_merged_dump_load_policy
#[test]
fn storage_merged_dump_load_policy_coordinates_dump_load_replay_and_index_gc() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "merged-a".to_string(),
            value: b"v1".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "merged-feature".to_string(),
            points: vec![FeaturePoint {
                timestamp_ms: 10,
                value: b"seven".to_vec(),
            }],
        },
    });

    let report =
        engine.storage_merged_dump_load_policy_report(StorageMergedDumpLoadPolicyRequest {
            lifecycle: StorageLifecycleRequest {
                shard_id: 1,
                max_dump_buckets_per_round: 16,
                prune_bucket_dump_manifests: true,
                roll_forward_bucket_dump_installs: true,
                invalidate_cache: true,
                warm_cache: true,
                ..StorageLifecycleRequest::default()
            },
            create_dump_manifest: true,
            install_dump_manifest: false,
        });
    assert!(report.policy_ready, "{report:?}");
    assert!(report.dump_manifest_created);
    assert!(report.load_preflight_safe);
    assert!(report.replay_boundary_safe);
    assert!(report.manifest_chain_valid);
    assert!(report.follower_retention_safe);
    assert!(report.index_gc_ready);
    assert!(report.manifest_id.is_some());
    assert!(!report.manifest_bucket_ids.is_empty());
    // The merged dump/load policy report was restructured: the granular
    // `*_validated` booleans and conflict/interruption counters are now
    // expressed through `blockers` (empty == all validations passed) and the
    // recovery `boundary`. On this clean path there are no blockers, no
    // interrupted installs, and no roll-forward recoveries.
    assert!(report.blockers.is_empty(), "{report:?}");
    assert!(report.boundary.interrupted_bucket_dump_installs.is_empty());
    assert!(report.install_roll_forward_reports.is_empty());

    let manifest = latest_bucket_dump_manifest_at(&engine.index_dir, 1).unwrap();
    let restore_engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("restore-cache"),
        dir.path().join("pages"),
        dir.path().join("restore-indexes"),
    );
    assert!(
        restore_engine
            .load_shard_with(LoadShardRequest {
                shard_id: 1,
                load_version: 0,
                local_node_id: Some(10),
                shard_uri: "local://restore/1".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 16_383,
                readonly: false,
                table_name: "restore".to_string(),
            })
            .status
            .ok
    );
    let merged_manifest = engine
        .create_merged_bucket_dump_manifest(
            1,
            manifest.bucket_ids.clone(),
            vec![manifest.manifest_id.clone()],
            Some(1),
        )
        .expect("merged manifest with load-version handoff");
    let install_report = restore_engine.install_merged_bucket_dump_manifest(&merged_manifest);
    assert!(install_report.installed, "{install_report:?}");
    assert!(install_report.rollback_marker_written);
    assert!(install_report.prepare_marker_written);
    assert!(install_report.install_marker_written);
    assert!(install_report.commit_marker_written);
    assert_eq!(install_report.source_manifest_count, 1);
    assert_eq!(install_report.stale_object_conflict_count, 0);
    assert_eq!(install_report.stale_page_conflict_count, 0);
    assert!(install_report
        .load_version_handoff
        .as_ref()
        .is_some_and(|handoff| handoff.previous_load_version == 0
            && handoff.next_load_version == 1
            && handoff.applied));
    assert_eq!(restore_engine.get_info(1).info.unwrap().load_version, 1);
    let get = restore_engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "merged-a".to_string(),
        },
    });
    assert_eq!(
        get.response,
        CommandResponse::Bytes {
            value: Some(b"v1".to_vec())
        }
    );
    let feature = restore_engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery {
            key: "merged-feature".to_string(),
            start_ms: 0,
            end_ms: 20,
            count: None,
        },
    });
    assert!(matches!(
        feature.response,
        CommandResponse::FeaturePoints { ref points } if points.len() == 1
    ));

    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "merged-a".to_string(),
            value: b"v2".to_vec(),
        },
    });
    let stale_preflight = engine.bucket_dump_install_preflight_report(&manifest);
    assert!(!stale_preflight.install_safe, "{stale_preflight:?}");
    assert!(stale_preflight
        .blockers
        .contains(&"stale_page_conflicts".to_string()));
    assert!(stale_preflight.stale_page_conflict_count > 0);
    assert_eq!(stale_preflight.stale_object_conflict_count, 0);
    assert!(!engine
        .install_bucket_dump_manifest(&manifest)
        .unwrap_err()
        .code
        .is_empty());

    write_bucket_dump_install_marker(
        &engine.index_dir,
        &BucketDumpInstallMarker {
            shard_id: manifest.shard_id,
            manifest_id: manifest.manifest_id.clone(),
            phase: "install".to_string(),
            wal_sequence: manifest.wal_sequence,
            index_log_sequence: manifest.index_log_sequence,
            created_unix_ms: now_ms(),
        },
    )
    .unwrap();
    let restarted = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache-restarted"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    restarted.load_shard(1);
    assert_eq!(restarted.interrupted_bucket_dump_installs(1).len(), 1);
    let restart_boundary = restarted.storage_recovery_boundary_report(1);
    assert_eq!(restart_boundary.interrupted_bucket_dump_installs.len(), 1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "merged-c".to_string(),
            value: b"v3".to_vec(),
        },
    });
    let recovered =
        engine.storage_merged_dump_load_policy_report(StorageMergedDumpLoadPolicyRequest {
            lifecycle: StorageLifecycleRequest {
                shard_id: 1,
                max_dump_buckets_per_round: 16,
                prune_bucket_dump_manifests: true,
                roll_forward_bucket_dump_installs: true,
                invalidate_cache: true,
                warm_cache: true,
                ..StorageLifecycleRequest::default()
            },
            create_dump_manifest: true,
            install_dump_manifest: false,
        });
    assert!(recovered.policy_ready, "{recovered:?}");
    // Recovery path: the interrupted install has been rolled forward, so no
    // interrupted installs remain in the boundary and at least one roll-forward
    // report was produced (was: interrupted_install_count==0,
    // roll_forward_recovery_count>=1, rollback_marker_count>=1).
    assert!(recovered.boundary.interrupted_bucket_dump_installs.is_empty());
    assert!(!recovered.install_roll_forward_reports.is_empty());
    assert!(engine.interrupted_bucket_dump_installs(1).is_empty());

    let mismatch_restore = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("mismatch-cache"),
        dir.path().join("pages"),
        dir.path().join("mismatch-indexes"),
    );
    assert!(
        mismatch_restore
            .load_shard_with(LoadShardRequest {
                shard_id: 1,
                load_version: 2,
                local_node_id: Some(11),
                shard_uri: "local://mismatch/1".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 16_383,
                readonly: false,
                table_name: "mismatch".to_string(),
            })
            .status
            .ok
    );
    let mismatch = mismatch_restore.bucket_dump_install_preflight_report(&merged_manifest);
    assert!(!mismatch.install_safe, "{mismatch:?}");
    assert!(mismatch
        .blockers
        .contains(&"load_version_handoff_mismatch".to_string()));
}

#[test]
fn bucket_dump_install_markers_report_interrupted_prepare() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "marker".to_string(),
            value: b"value".to_vec(),
        },
    });
    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    write_bucket_dump_install_marker(
        &engine.index_dir,
        &BucketDumpInstallMarker {
            shard_id: manifest.shard_id,
            manifest_id: "interrupted".to_string(),
            phase: "prepare".to_string(),
            wal_sequence: manifest.wal_sequence,
            index_log_sequence: manifest.index_log_sequence,
            created_unix_ms: now_ms(),
        },
    )
    .unwrap();

    let interrupted = engine.interrupted_bucket_dump_installs(1);
    assert_eq!(interrupted.len(), 1);
    assert_eq!(interrupted[0].phase, "prepare");
    let boundary = engine.storage_recovery_boundary_report(1);
    assert_eq!(boundary.interrupted_bucket_dump_installs, interrupted);
    assert_eq!(boundary.prepared_bucket_dump_install_count, 1);
    assert_eq!(boundary.installed_bucket_dump_install_count, 0);
    assert_eq!(boundary.unknown_bucket_dump_install_count, 0);
    let readiness = engine.storage_production_readiness_report(1);
    assert_eq!(readiness.interrupted_bucket_dump_install_count, 1);
    assert_eq!(readiness.prepared_bucket_dump_install_count, 1);
    assert_eq!(readiness.installed_bucket_dump_install_count, 0);
    assert_eq!(readiness.unknown_bucket_dump_install_count, 0);
}

#[test]
fn bucket_dump_install_roll_forward_completes_safe_installed_marker() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "roll".to_string(),
            value: b"value".to_vec(),
        },
    });
    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    write_bucket_dump_install_marker(
        &engine.index_dir,
        &BucketDumpInstallMarker {
            shard_id: manifest.shard_id,
            manifest_id: manifest.manifest_id.clone(),
            phase: "install".to_string(),
            wal_sequence: manifest.wal_sequence,
            index_log_sequence: manifest.index_log_sequence,
            created_unix_ms: now_ms(),
        },
    )
    .unwrap();

    let dry_run = engine.bucket_dump_install_roll_forward_reports(1);
    assert_eq!(dry_run.len(), 1);
    assert!(dry_run[0].can_roll_forward);
    assert_eq!(dry_run[0].reason, "commit_ready");

    let applied = engine.roll_forward_bucket_dump_installs(1);
    assert_eq!(applied.len(), 1);
    assert!(applied[0].completed_commit);
    assert!(applied[0].obsolete_marker_files_removed > 0);
    assert!(engine.interrupted_bucket_dump_installs(1).is_empty());
    let marker_files =
        bucket_dump_install_marker_files_at(&engine.index_dir, 1).expect("marker files");
    assert!(marker_files
        .iter()
        .all(|(marker, _)| marker.phase == "commit"));
}

#[test]
fn bucket_dump_install_roll_forward_retries_safe_prepare_marker() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "retry-prepare".to_string(),
            value: b"value".to_vec(),
        },
    });
    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    write_bucket_dump_install_marker(
        &engine.index_dir,
        &BucketDumpInstallMarker {
            shard_id: manifest.shard_id,
            manifest_id: manifest.manifest_id.clone(),
            phase: "prepare".to_string(),
            wal_sequence: manifest.wal_sequence,
            index_log_sequence: manifest.index_log_sequence,
            created_unix_ms: now_ms(),
        },
    )
    .unwrap();

    let dry_run = engine.bucket_dump_install_roll_forward_reports(1);
    assert_eq!(dry_run.len(), 1);
    assert!(dry_run[0].can_retry_install);
    assert!(!dry_run[0].can_roll_forward);
    assert_eq!(dry_run[0].reason, "install_retry_ready");

    let applied = engine.roll_forward_bucket_dump_installs(1);
    assert_eq!(applied.len(), 1);
    assert!(applied[0].completed_install);
    assert!(applied[0].completed_commit);
    assert!(applied[0].obsolete_marker_files_removed > 0);
    assert!(engine.interrupted_bucket_dump_installs(1).is_empty());
    let marker_files =
        bucket_dump_install_marker_files_at(&engine.index_dir, 1).expect("marker files");
    assert!(marker_files
        .iter()
        .all(|(marker, _)| marker.phase == "commit"));
}

#[test]
fn bucket_dump_recovery_reports_broken_manifest_parent_chain() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "chain".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let parent = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("parent manifest should persist");
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "chain".to_string(),
            value: b"v2".to_vec(),
        },
    });
    let child = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("child manifest should persist");
    assert_eq!(child.parent_manifest_id, Some(parent.manifest_id.clone()));

    fs::remove_file(bucket_dump_manifest_path(
        &engine.index_dir,
        1,
        &parent.manifest_id,
    ))
    .unwrap();
    let boundary = engine.storage_recovery_boundary_report(1);
    assert_eq!(boundary.manifest_chain_issues.len(), 1);
    assert_eq!(
        boundary.manifest_chain_issues[0].manifest_id,
        child.manifest_id
    );
    assert_eq!(
        boundary.manifest_chain_issues[0].reason,
        "missing_parent_manifest"
    );
}

#[test]
fn bucket_dump_manifest_prune_keeps_latest_parent_chain_and_removes_obsolete_fork() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "prune".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let parent = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "prune".to_string(),
            value: b"v2".to_vec(),
        },
    });
    let child = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    let mut fork = parent.clone();
    fork.manifest_id = format!("{}-fork", fork.manifest_id);
    fork.parent_manifest_id = None;
    fork.dump_generation_id = bucket_dump_generation_id(&fork);
    fork.checksum = bucket_dump_manifest_checksum(&fork).unwrap();
    engine.persist_bucket_dump_manifest(&fork).unwrap();
    write_bucket_dump_install_marker(
        &engine.index_dir,
        &BucketDumpInstallMarker {
            shard_id: 1,
            manifest_id: fork.manifest_id.clone(),
            phase: "commit".to_string(),
            wal_sequence: fork.wal_sequence,
            index_log_sequence: fork.index_log_sequence,
            created_unix_ms: now_ms(),
        },
    )
    .unwrap();

    let plan = engine.bucket_dump_manifest_prune_plan(1);
    assert!(plan.retained_manifest_ids.contains(&parent.manifest_id));
    assert!(plan.retained_manifest_ids.contains(&child.manifest_id));
    assert_eq!(plan.prunable_manifest_ids, vec![fork.manifest_id.clone()]);
    assert_eq!(
        plan.prunable_marker_manifest_ids,
        vec![fork.manifest_id.clone()]
    );

    let lifecycle = engine.apply_storage_lifecycle(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: Vec::new(),
        max_dump_buckets_per_round: 0,
        min_undumped_wal_records: 0,
        purge_delayed_destroy: false,
        prune_bucket_dump_manifests: true,
        roll_forward_bucket_dump_installs: false,
        follower_replay_cursors: Vec::new(),
        invalidate_cache: false,
        warm_cache: false,
        ..StorageLifecycleRequest::default()
    });
    let report = lifecycle
        .manifest_prune_report
        .expect("lifecycle should apply manifest prune");
    assert_eq!(report.removed_manifest_ids, vec![fork.manifest_id.clone()]);
    assert_eq!(report.removed_marker_files, 1);
    assert_eq!(
        lifecycle.manifest_prune_plan.prunable_manifest_ids,
        vec![fork.manifest_id.clone()]
    );
    assert!(bucket_dump_manifest_path(&engine.index_dir, 1, &parent.manifest_id).exists());
    assert!(bucket_dump_manifest_path(&engine.index_dir, 1, &child.manifest_id).exists());
    assert!(!bucket_dump_manifest_path(&engine.index_dir, 1, &fork.manifest_id).exists());
}

#[test]
fn torn_manifest_does_not_hide_valid_manifests() {
    // Crash-consistency: a torn/corrupt manifest .json (from a crash mid-write, now
    // prevented by the atomic write) must not fail the whole manifest listing and
    // silently drop all dumped state on load. list_bucket_dump_manifests skips the bad
    // file and keeps the valid one.
    let dir = tempfile::tempdir().unwrap();
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        &index_dir,
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::<u32>::new())
        .unwrap();
    let manifest_dir = index_dir.join("slot-dumps").join("shard-1");
    std::fs::write(manifest_dir.join("torn-9999.json"), b"{ not valid json").unwrap();
    let listed = engine.list_bucket_dump_manifests(1);
    assert!(
        listed
            .iter()
            .any(|entry| entry.manifest_id == manifest.manifest_id),
        "a valid manifest must survive a torn sibling file: {listed:?}"
    );
}

#[test]
fn dump_clears_dumped_buckets_so_they_are_not_redumped() {
    // Parity with SlotStore::DumpSlotInMemory -> Index::ClearSlotDirty: once a
    // dirty bucket is dumped it is no longer dirty, so the storage cycle does not
    // re-select and re-dump the same buckets (and re-export the whole index) forever.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for i in 0..4 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("k{i}"),
                value: b"v".to_vec(),
            },
        });
    }
    let dirty_before: u64 = engine
        .bucket_storage_summaries(1)
        .iter()
        .map(|summary| summary.dirty_object_count)
        .sum();
    assert!(dirty_before > 0, "writes should leave dirty objects to dump");

    engine.apply_storage_lifecycle(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: Vec::new(),
        max_dump_buckets_per_round: 0,
        min_undumped_wal_records: 0,
        ..StorageLifecycleRequest::default()
    });
    assert!(
        !engine.list_bucket_dump_manifests(1).is_empty(),
        "a dump manifest should have been produced for the dirty buckets"
    );

    let dirty_after: u64 = engine
        .bucket_storage_summaries(1)
        .iter()
        .map(|summary| summary.dirty_object_count)
        .sum();
    assert_eq!(
        dirty_after, 0,
        "dumped buckets must be cleared, not left dirty to be re-dumped every cycle"
    );
}

#[test]
fn bucket_dump_manifest_prune_is_blocked_by_lagging_follower_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "cursor".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let parent = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "cursor".to_string(),
            value: b"v2".to_vec(),
        },
    });
    let child = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    let mut fork = parent.clone();
    fork.manifest_id = format!("{}-follower-anchor", fork.manifest_id);
    fork.parent_manifest_id = None;
    fork.created_unix_ms = parent.created_unix_ms.saturating_add(1);
    fork.dump_generation_id = bucket_dump_generation_id(&fork);
    fork.checksum = bucket_dump_manifest_checksum(&fork).unwrap();
    engine.persist_bucket_dump_manifest(&fork).unwrap();

    let no_cursor = engine.bucket_dump_manifest_prune_plan(1);
    assert_eq!(
        no_cursor.prunable_manifest_ids,
        vec![fork.manifest_id.clone()]
    );

    let lagging_cursor = BucketDumpFollowerReplayCursor {
        follower_id: "follower-a".to_string(),
        shard_id: 1,
        wal_sequence: fork.wal_sequence,
        index_log_sequence: fork.index_log_sequence,
    };
    let blocked =
        engine.bucket_dump_manifest_prune_plan_with_follower_cursors(1, vec![lagging_cursor.clone()]);
    assert!(blocked.prunable_manifest_ids.is_empty());
    assert!(blocked.retained_manifest_ids.contains(&fork.manifest_id));
    assert_eq!(blocked.follower_blocks.len(), 1);
    assert_eq!(blocked.follower_blocks[0].follower_id, "follower-a");
    assert_eq!(blocked.follower_blocks[0].manifest_id, fork.manifest_id);
    assert!(blocked
        .reasons
        .contains(&"follower_cursor_blocks_prune".to_string()));

    let caught_up = engine.bucket_dump_manifest_prune_plan_with_follower_cursors(
        1,
        vec![BucketDumpFollowerReplayCursor {
            wal_sequence: child.wal_sequence,
            index_log_sequence: child.index_log_sequence,
            ..lagging_cursor
        }],
    );
    assert_eq!(
        caught_up.prunable_manifest_ids,
        vec![fork.manifest_id.clone()]
    );
}

#[test]
fn bucket_dump_manifest_prune_is_blocked_by_raft_snapshot_reference() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "snapshot".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let parent = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "snapshot".to_string(),
            value: b"v2".to_vec(),
        },
    });
    let child = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    let mut fork = parent.clone();
    fork.manifest_id = format!("{}-snapshot-anchor", fork.manifest_id);
    fork.parent_manifest_id = None;
    fork.created_unix_ms = parent.created_unix_ms.saturating_add(1);
    fork.dump_generation_id = bucket_dump_generation_id(&fork);
    fork.checksum = bucket_dump_manifest_checksum(&fork).unwrap();
    engine.persist_bucket_dump_manifest(&fork).unwrap();

    let no_snapshot = engine.bucket_dump_manifest_prune_plan(1);
    assert_eq!(
        no_snapshot.prunable_manifest_ids,
        vec![fork.manifest_id.clone()]
    );

    let snapshot_ref = BucketDumpRaftSnapshotRef {
        snapshot_id: "raft-snapshot-0007".to_string(),
        shard_id: 1,
        last_included_index: 7,
        last_included_term: 2,
        wal_sequence: fork.wal_sequence,
        index_log_sequence: fork.index_log_sequence,
    };
    let blocked = engine.bucket_dump_manifest_prune_plan_with_retention_refs(
        1,
        Vec::<BucketDumpFollowerReplayCursor>::new(),
        vec![snapshot_ref.clone()],
    );
    assert!(blocked.prunable_manifest_ids.is_empty());
    assert!(blocked.retained_manifest_ids.contains(&fork.manifest_id));
    assert_eq!(blocked.raft_snapshot_blocks.len(), 1);
    assert_eq!(
        blocked.raft_snapshot_blocks[0].snapshot_id,
        "raft-snapshot-0007"
    );
    assert_eq!(
        blocked.raft_snapshot_blocks[0].manifest_id,
        fork.manifest_id
    );
    assert!(blocked
        .reasons
        .contains(&"raft_snapshot_blocks_prune".to_string()));

    let advanced = engine.bucket_dump_manifest_prune_plan_with_retention_refs(
        1,
        Vec::<BucketDumpFollowerReplayCursor>::new(),
        vec![BucketDumpRaftSnapshotRef {
            wal_sequence: child.wal_sequence,
            index_log_sequence: child.index_log_sequence,
            ..snapshot_ref
        }],
    );
    assert_eq!(
        advanced.prunable_manifest_ids,
        vec![fork.manifest_id.clone()]
    );
}

// shared-corpus: storage_wal_index_gc_generation_retention
#[test]
fn storage_wal_index_gc_reclaim_requires_durable_generation_and_retention_release() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "reclaim-slot".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let parent = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "reclaim-slot".to_string(),
            value: b"v2".to_vec(),
        },
    });
    let child = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    assert!(child.wal_sequence > parent.wal_sequence);
    assert!(child.index_log_sequence > parent.index_log_sequence);

    let lagging_cursor = BucketDumpFollowerReplayCursor {
        follower_id: "follower-lagging".to_string(),
        shard_id: 1,
        wal_sequence: parent.wal_sequence,
        index_log_sequence: parent.index_log_sequence,
    };
    let lagging_snapshot = BucketDumpRaftSnapshotRef {
        snapshot_id: "raft-snapshot-lagging".to_string(),
        shard_id: 1,
        last_included_index: 11,
        last_included_term: 2,
        wal_sequence: parent.wal_sequence,
        index_log_sequence: parent.index_log_sequence,
    };
    let blocked = engine.storage_wal_reclaim_plan(
        1,
        vec![lagging_cursor.clone()],
        vec![lagging_snapshot.clone()],
    );
    assert!(!blocked.safe_to_reclaim, "{blocked:?}");
    assert_eq!(blocked.follower_cursor_block_count, 1);
    assert_eq!(blocked.raft_snapshot_block_count, 1);
    assert_eq!(
        blocked.durable_bucket_generation_frontier_wal_sequence,
        child.wal_sequence
    );
    assert_eq!(
        blocked.durable_bucket_generation_frontier_index_log_sequence,
        child.index_log_sequence
    );
    assert_eq!(blocked.retain_from_wal_sequence, 0);
    assert_eq!(blocked.retain_from_index_log_sequence, 0);
    assert!(blocked
        .blocker_reasons
        .contains(&"follower_cursor_retains_logs:follower-lagging".to_string()));
    assert!(blocked
        .blocker_reasons
        .contains(&"raft_snapshot_retains_logs:raft-snapshot-lagging".to_string()));

    let blocked_cycle = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: 1,
        follower_replay_cursors: vec![lagging_cursor],
        raft_snapshot_refs: vec![lagging_snapshot],
        index_gc_index_log_bytes_threshold: 0,
        index_gc_usage_ratio_trigger_basis_points: 0,
        index_gc_max_entries_per_round: 8,
        min_undumped_wal_records: 0,
        ..StorageManagerCycleRequest::default()
    });
    let blocked_wal = blocked_cycle.wal_reclaim_report.as_ref().unwrap();
    assert!(!blocked_wal.applied);
    assert_eq!(blocked_wal.wal_records_removed, 0);
    assert!(!blocked_cycle.index_gc_report.as_ref().unwrap().applied);
    assert_eq!(
        blocked_cycle
            .index_gc_report
            .as_ref()
            .unwrap()
            .skipped_reason,
        "durable WAL/index frontier not safe"
    );
    assert!(
        blocked_cycle
            .stages
            .iter()
            .find(|stage| stage.stage == "reclaim_wal")
            .unwrap()
            .retention_blockers
            >= 2
    );

    let released_anchor = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    assert!(released_anchor.wal_sequence >= child.wal_sequence);
    assert!(released_anchor.index_log_sequence >= child.index_log_sequence);
    let released_cursor = BucketDumpFollowerReplayCursor {
        follower_id: "follower-caught-up".to_string(),
        shard_id: 1,
        wal_sequence: released_anchor.wal_sequence,
        index_log_sequence: released_anchor.index_log_sequence,
    };
    let released_snapshot = BucketDumpRaftSnapshotRef {
        snapshot_id: "raft-snapshot-caught-up".to_string(),
        shard_id: 1,
        last_included_index: 12,
        last_included_term: 2,
        wal_sequence: released_anchor.wal_sequence,
        index_log_sequence: released_anchor.index_log_sequence,
    };
    let released = engine.storage_wal_reclaim_plan(
        1,
        vec![released_cursor.clone()],
        vec![released_snapshot.clone()],
    );
    assert!(released.safe_to_reclaim, "{released:?}");
    assert_eq!(released.follower_cursor_block_count, 0);
    assert_eq!(released.raft_snapshot_block_count, 0);
    assert_eq!(
        released.retain_from_wal_sequence,
        released_anchor.wal_sequence.saturating_add(1)
    );
    assert_eq!(
        released.retain_from_index_log_sequence,
        released_anchor.index_log_sequence.saturating_add(1)
    );

    let threshold_blocked_cycle = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: 1,
        follower_replay_cursors: vec![released_cursor.clone()],
        raft_snapshot_refs: vec![released_snapshot.clone()],
        index_gc_index_log_bytes_threshold: u64::MAX,
        index_gc_usage_ratio_trigger_basis_points: 0,
        index_gc_max_entries_per_round: 1,
        min_undumped_wal_records: 0,
        enable_wal_reclaim: false,
        ..StorageManagerCycleRequest::default()
    });
    let threshold_blocked_index_gc = threshold_blocked_cycle.index_gc_report.as_ref().unwrap();
    assert!(!threshold_blocked_index_gc.applied);
    assert_eq!(
        threshold_blocked_index_gc.skipped_reason,
        "index-log byte threshold not reached"
    );

    let final_anchor = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    let final_cursor = BucketDumpFollowerReplayCursor {
        follower_id: "follower-final".to_string(),
        shard_id: 1,
        wal_sequence: final_anchor.wal_sequence,
        index_log_sequence: final_anchor.index_log_sequence,
    };
    let final_snapshot = BucketDumpRaftSnapshotRef {
        snapshot_id: "raft-snapshot-final".to_string(),
        shard_id: 1,
        last_included_index: 13,
        last_included_term: 2,
        wal_sequence: final_anchor.wal_sequence,
        index_log_sequence: final_anchor.index_log_sequence,
    };
    let released_cycle = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: 1,
        follower_replay_cursors: vec![final_cursor],
        raft_snapshot_refs: vec![final_snapshot],
        index_gc_index_log_bytes_threshold: 0,
        index_gc_usage_ratio_trigger_basis_points: 0,
        index_gc_max_entries_per_round: 1,
        min_undumped_wal_records: 0,
        ..StorageManagerCycleRequest::default()
    });
    let released_wal = released_cycle.wal_reclaim_report.as_ref().unwrap();
    assert!(released_wal.plan.safe_to_reclaim, "{released_wal:?}");
    assert!(released_wal.applied, "{released_wal:?}");
    assert!(released_wal.wal_records_removed > 0, "{released_wal:?}");
    let released_index_gc = released_cycle.index_gc_report.as_ref().unwrap();
    assert!(released_index_gc.safe_to_truncate, "{released_index_gc:?}");
    assert!(released_index_gc.applied, "{released_index_gc:?}");
    assert_eq!(released_index_gc.records_removed, 1);
    assert!(released_index_gc.budget_exhausted);

    let restarted = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("restart-cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    restarted.load_shard(1);
    let get = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "reclaim-slot".to_string(),
        },
    });
    assert_eq!(
        get.response,
        CommandResponse::Bytes {
            value: Some(b"v2".to_vec())
        }
    );
    let restart_boundary = restarted.storage_recovery_boundary_report(1);
    assert!(restart_boundary.latest_safe_index_log_sequence >= final_anchor.index_log_sequence);
    assert!(restart_boundary.stale_index_page_refs.is_empty());
    assert_eq!(restart_boundary.missing_owner_page_refs, 0);
}

// shared-corpus: storage_gc_dependency_retention_matrix
#[test]
fn storage_page_gc_blocks_all_retention_dependencies_before_reclaim() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "gc-key".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let parent = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    engine.block_store().roll_slab().unwrap();
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "gc-key".to_string(),
            value: b"v2".to_vec(),
        },
    });
    assert_eq!(engine.live_page_slab_ids(1), vec![1]);
    let delayed = engine
        .block_store()
        .gc_slabs_before_with_live_refs_delayed_destroy(1, engine.live_page_slab_ids(1))
        .unwrap();
    assert_eq!(delayed.delayed_destroy_page_slab_ids, vec![0]);

    let matrix = engine.storage_page_gc_dependency_plan(
        1,
        vec![0, 1],
        vec![StoragePageGcReplayCursor {
            cursor_id: "shared-follower-a".to_string(),
            shard_id: 1,
            retain_from_page_slab_id: 0,
            reason: "shared-store follower is behind segment zero".to_string(),
        }],
        vec![BucketDumpRaftSnapshotRef {
            snapshot_id: "raft-snapshot-a".to_string(),
            shard_id: 1,
            last_included_index: 7,
            last_included_term: 2,
            wal_sequence: parent.wal_sequence,
            index_log_sequence: 0,
        }],
        Some(0),
        Some(0),
        60_000,
    );
    assert!(!matrix.safe_to_reclaim, "{matrix:?}");
    assert_eq!(matrix.candidate_page_slab_ids, vec![0, 1]);
    assert_eq!(matrix.live_ref_block_count, 1);
    assert_eq!(matrix.bucket_dump_manifest_block_count, 1);
    assert_eq!(matrix.shared_store_cursor_block_count, 2);
    assert_eq!(matrix.raft_snapshot_ref_block_count, 2);
    assert_eq!(matrix.checkpoint_snapshot_floor_block_count, 2);
    assert_eq!(matrix.raft_snapshot_install_floor_block_count, 2);
    assert_eq!(matrix.delayed_destroy_grace_block_count, 1);
    for expected in [
        "live_page_ref",
        "slot_dump_manifest",
        "shared_store_replay_cursor",
        "raft_snapshot_ref",
        "checkpoint_snapshot_floor",
        "raft_snapshot_install_floor",
        "delayed_destroy_grace_period",
    ] {
        assert!(
            matrix.blocker_reasons.contains(&expected.to_string()),
            "{matrix:?}"
        );
    }

    let released = engine.storage_page_gc_dependency_plan(
        1,
        vec![0],
        Vec::<StoragePageGcReplayCursor>::new(),
        Vec::<BucketDumpRaftSnapshotRef>::new(),
        None,
        None,
        0,
    );
    assert!(!released.safe_to_reclaim, "{released:?}");
    assert_eq!(released.bucket_dump_manifest_block_count, 1);
    assert_eq!(released.delayed_destroy_grace_block_count, 0);
    assert!(released
        .blocker_reasons
        .contains(&"slot_dump_manifest".to_string()));
}

#[test]
fn bucket_dump_manifest_rejects_generation_mismatch_and_conflicts() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "generation".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    assert_eq!(manifest.version, 3);
    assert!(!manifest.dump_generation_id.is_empty());
    assert_eq!(manifest.object_lifecycle.live_object_ids, 1);
    assert_eq!(manifest.object_lifecycle.live_page_refs, 1);

    let mut legacy_v2 = manifest.clone();
    legacy_v2.version = 2;
    legacy_v2.object_lifecycle = StorageObjectLifecycleReport::default();
    let legacy_generation_id = bucket_dump_generation_id(&legacy_v2);
    legacy_v2.object_lifecycle.live_object_ids = 99;
    assert_eq!(bucket_dump_generation_id(&legacy_v2), legacy_generation_id);

    let mut mismatched = manifest.clone();
    mismatched.dump_generation_id = "wrong-generation".to_string();
    mismatched.checksum = bucket_dump_manifest_checksum(&mismatched).unwrap();
    assert_eq!(
        engine
            .validate_bucket_dump_manifest(&mismatched)
            .unwrap_err()
            .code,
        "slot_dump_generation_mismatch"
    );

    let restore_engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("restore-cache"),
        dir.path().join("pages"),
        dir.path().join("restore-indexes"),
    );
    restore_engine.load_shard(1);
    restore_engine
        .install_bucket_dump_manifest(&manifest)
        .expect("first generation should install");

    let mut fork = manifest.clone();
    let extra_bucket = fork
        .bucket_ids
        .iter()
        .copied()
        .max()
        .unwrap_or_default()
        .saturating_add(1);
    fork.bucket_ids.push(extra_bucket);
    fork.dump_generation_id = bucket_dump_generation_id(&fork);
    fork.manifest_id = format!("{}-fork", fork.manifest_id);
    fork.checksum = bucket_dump_manifest_checksum(&fork).unwrap();
    assert_eq!(
        restore_engine
            .install_bucket_dump_manifest(&fork)
            .unwrap_err()
            .code,
        "slot_dump_generation_conflict"
    );
}

#[test]
fn bucket_dump_manifest_rejects_object_lifecycle_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "lifecycle".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    engine
        .validate_bucket_dump_manifest(&manifest)
        .expect("fresh manifest should validate");

    let mut stale_lifecycle = manifest.clone();
    stale_lifecycle.object_lifecycle.live_object_ids = stale_lifecycle
        .object_lifecycle
        .live_object_ids
        .saturating_add(1);
    stale_lifecycle.dump_generation_id = bucket_dump_generation_id(&stale_lifecycle);
    stale_lifecycle.checksum = bucket_dump_manifest_checksum(&stale_lifecycle).unwrap();
    assert_eq!(
        engine
            .validate_bucket_dump_manifest(&stale_lifecycle)
            .unwrap_err()
            .code,
        "slot_dump_object_lifecycle_mismatch"
    );

    let mut reused_owner = manifest.clone();
    {
        let mut restored = serde_json::from_slice::<ShardState>(&reused_owner.index_bytes)
            .expect("manifest index should decode");
        let address = restored
            .strings
            .get_mut("lifecycle")
            .expect("manifest string address");
        address.object_id = Some(address.object_id.unwrap_or_default().wrapping_add(1));
        reused_owner.index_bytes = serde_json::to_vec(&restored).unwrap();
        reused_owner.index_sha256 = sha256_hex_bytes(&reused_owner.index_bytes);
        reused_owner.dump_generation_id = bucket_dump_generation_id(&reused_owner);
        reused_owner.checksum = bucket_dump_manifest_checksum(&reused_owner).unwrap();
    }
    assert_eq!(
        engine
            .validate_bucket_dump_manifest(&reused_owner)
            .unwrap_err()
            .code,
        "slot_dump_object_lifecycle_mismatch"
    );
}

#[test]
fn bucket_dump_manifest_rejects_bucket_summary_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "slot-summary".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    engine
        .validate_bucket_dump_manifest(&manifest)
        .expect("fresh manifest should validate");

    let mut stale_summary = manifest.clone();
    let summary = stale_summary
        .bucket_summaries
        .first_mut()
        .expect("slot summary should exist");
    summary.page_ref_count = summary.page_ref_count.saturating_add(1);
    stale_summary.dump_generation_id = bucket_dump_generation_id(&stale_summary);
    stale_summary.checksum = bucket_dump_manifest_checksum(&stale_summary).unwrap();

    assert_eq!(
        engine
            .validate_bucket_dump_manifest(&stale_summary)
            .unwrap_err()
            .code,
        "slot_dump_slot_summary_mismatch"
    );
}

#[test]
fn bucket_dump_manifest_rejects_byte_accounting_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "byte-accounting".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    engine
        .validate_bucket_dump_manifest(&manifest)
        .expect("fresh manifest should validate");

    let mut stale_bytes = manifest.clone();
    stale_bytes.logical_bytes = stale_bytes.logical_bytes.saturating_add(1);
    stale_bytes.checksum = bucket_dump_manifest_checksum(&stale_bytes).unwrap();

    assert_eq!(
        engine
            .validate_bucket_dump_manifest(&stale_bytes)
            .unwrap_err()
            .code,
        "slot_dump_byte_accounting_mismatch"
    );
}

#[test]
fn bucket_dump_manifest_rejects_non_canonical_bucket_and_page_slab_ids() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "canonical".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    engine
        .validate_bucket_dump_manifest(&manifest)
        .expect("fresh manifest should validate");

    let mut duplicate_bucket = manifest.clone();
    duplicate_bucket.bucket_ids.push(
        duplicate_bucket
            .bucket_ids
            .first()
            .copied()
            .expect("slot id should exist"),
    );
    duplicate_bucket.dump_generation_id = bucket_dump_generation_id(&duplicate_bucket);
    duplicate_bucket.checksum = bucket_dump_manifest_checksum(&duplicate_bucket).unwrap();
    assert_eq!(
        engine
            .validate_bucket_dump_manifest(&duplicate_bucket)
            .unwrap_err()
            .code,
        "slot_dump_slot_ids_not_canonical"
    );

    let mut duplicate_page_slab = manifest.clone();
    duplicate_page_slab.page_slab_ids.push(
        duplicate_page_slab
            .page_slab_ids
            .first()
            .copied()
            .expect("page segment id should exist"),
    );
    duplicate_page_slab.dump_generation_id = bucket_dump_generation_id(&duplicate_page_slab);
    duplicate_page_slab.checksum = bucket_dump_manifest_checksum(&duplicate_page_slab).unwrap();
    assert_eq!(
        engine
            .validate_bucket_dump_manifest(&duplicate_page_slab)
            .unwrap_err()
            .code,
        "slot_dump_page_segment_ids_not_canonical"
    );
}

#[test]
fn storage_lifecycle_plan_and_boundary_report_cover_dirty_and_orphan_slabs() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v1".to_vec(),
        },
    });
    engine.block_store().roll_slab().unwrap();
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v2".to_vec(),
        },
    });

    let plan = engine.storage_lifecycle_plan(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: Vec::new(),
        max_dump_buckets_per_round: 0,
        min_undumped_wal_records: 0,
        purge_delayed_destroy: false,
        prune_bucket_dump_manifests: false,
        roll_forward_bucket_dump_installs: false,
        follower_replay_cursors: Vec::new(),
        invalidate_cache: false,
        warm_cache: false,
        ..StorageLifecycleRequest::default()
    });
    assert!(!plan.dirty_buckets.is_empty());
    assert_eq!(plan.selected_dump_buckets, plan.dirty_buckets);
    assert!(plan.reasons.contains(&"dirty_slot_dump".to_string()));
    assert!(plan.stale_page_slab_ids.contains(&0));
    assert!(plan
        .reasons
        .contains(&"ranked_reclaim_candidates".to_string()));
    assert!(!plan.reclaim_candidates.is_empty());
    assert_eq!(plan.reclaim_candidates[0].page_slab_id, 0);
    assert_eq!(plan.reclaim_candidates[0].reason, "orphan_segment");
    assert!(plan.reclaim_candidates[0].stale_physical_bytes > 0);
    assert!(plan.reclaim_candidates[0].reclaim_score > 0);

    let report = engine.apply_storage_lifecycle(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: plan.selected_dump_buckets.clone(),
        max_dump_buckets_per_round: 0,
        min_undumped_wal_records: 0,
        purge_delayed_destroy: false,
        prune_bucket_dump_manifests: false,
        roll_forward_bucket_dump_installs: false,
        follower_replay_cursors: Vec::new(),
        invalidate_cache: true,
        warm_cache: false,
        ..StorageLifecycleRequest::default()
    });
    assert!(report.dump_manifest.is_some());
    assert_eq!(report.object_lifecycle.live_object_ids, 1);
    assert_eq!(report.object_lifecycle.live_page_refs, 1);
    assert_eq!(report.object_lifecycle.stale_object_ids, 1);
    let boundary = engine.storage_recovery_boundary_report(1);
    assert_eq!(boundary.latest_safe_wal_sequence, 2);
    assert_eq!(boundary.latest_dump_wal_sequence, 2);
    assert!(boundary.orphan_page_slab_ids.contains(&0));
}

#[test]
fn storage_production_readiness_reports_warnings_without_blocking_dirty_shard() {
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
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "ready-key".to_string(),
                    value: b"ready-value".to_vec(),
                },
            })
            .status
            .ok
    );

    let report = engine.storage_production_readiness_report(1);
    assert!(report.production_ready, "{report:?}");
    assert!(report.blockers.is_empty());
    assert_eq!(report.dirty_bucket_count, 1);
    assert!(report
        .warnings
        .contains(&"dirty_slots_pending_dump".to_string()));
    assert!(report.slab_integrity.integrity_ok);
    assert_eq!(report.slab_integrity.unreadable_page_ref_count, 0);
    assert_eq!(report.unreadable_page_ref_count, 0);
    assert_eq!(report.owner_mismatch_page_ref_count, 0);
    assert!(report.log_compatibility.rust_native_replay_safe);
    assert!(!report.log_compatibility.cxx_binary_compatible);
    assert_eq!(
        report.log_compatibility.wal_format,
        "rust-jsonl-command-v1"
    );
    assert_eq!(
        report.log_compatibility.index_log_format,
        "rust-jsonl-shard-index-v1"
    );
    assert_eq!(
        report.log_compatibility.compatibility_mode,
        "rust_native_migration_only"
    );
    assert!(report.log_compatibility.migration_required);
    assert!(report.log_compatibility.golden_conversion_required);
    assert!(!report.log_compatibility.cxx_reader_supported);
    assert!(!report.log_compatibility.cxx_writer_supported);
    assert!(report.page_format_compatibility.rust_native_read_safe);
    assert!(!report.page_format_compatibility.cxx_page_header_compatible);
    assert_eq!(
        report.page_format_compatibility.page_format,
        "rust-page-envelope-v6"
    );
    assert_eq!(
        report.page_format_compatibility.compatibility_mode,
        "rust_envelope_migration_only"
    );
    assert!(report.page_format_compatibility.migration_required);
    assert!(report.page_format_compatibility.golden_conversion_required);
    assert!(
        !report
            .page_format_compatibility
            .cxx_page_header_reader_supported
    );
    assert!(
        !report
            .page_format_compatibility
            .cxx_page_header_writer_supported
    );
    assert!(report.page_format_compatibility.checksum_protected);
    assert!(report.page_format_compatibility.object_ids_embedded);
    assert!(report.block_store_bytes_written > 0);
}

#[test]
fn storage_log_compatibility_report_counts_jsonl_sequences_and_cxx_gaps() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..2 {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: format!("log-key-{index}"),
                        value: format!("log-value-{index}").into_bytes(),
                    },
                })
                .status
                .ok
        );
    }

    let report = engine.storage_log_compatibility_report(1);
    assert_eq!(report.shard_id, 1);
    assert_eq!(report.compatibility_mode, "rust_native_migration_only");
    assert!(report.migration_required);
    assert!(!report.cxx_reader_supported);
    assert!(!report.cxx_writer_supported);
    assert!(report.golden_conversion_required);
    assert_eq!(report.wal_last_sequence, 2);
    assert_eq!(report.index_log_last_sequence, 2);
    assert_eq!(report.wal_records, 2);
    assert_eq!(report.index_log_records, 2);
    assert!(report.wal_bytes > 0);
    assert!(report.index_log_bytes > 0);
    assert!(report.rust_native_replay_safe);
    assert!(!report.cxx_binary_compatible);
    assert!(report
        .compatibility_gaps
        .iter()
        .any(|gap| { gap.contains("migration-only") && gap.contains("binary log") }));
    assert!(report
        .compatibility_gaps
        .iter()
        .any(|gap| gap.contains("binary/protobuf wal")));
    assert!(report
        .compatibility_gaps
        .iter()
        .any(|gap| gap.contains("golden log conversion/replay")));
}

#[test]
fn storage_page_format_compatibility_report_counts_zones_and_header_gaps() {
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
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "page-format-key".to_string(),
                    value: vec![11; 512],
                },
            })
            .status
            .ok
    );
    engine.block_store().roll_slab().unwrap();

    let report = engine.storage_page_format_compatibility_report(1);
    assert_eq!(report.shard_id, 1);
    assert_eq!(report.page_format, "rust-page-envelope-v6");
    assert_eq!(report.rust_envelope_version, 6);
    assert_eq!(report.compatibility_mode, "rust_envelope_migration_only");
    assert!(report.migration_required);
    assert!(!report.cxx_page_header_reader_supported);
    assert!(!report.cxx_page_header_writer_supported);
    assert!(report.golden_conversion_required);
    assert!(report.rust_native_read_safe);
    assert!(!report.cxx_page_header_compatible);
    assert!(report.checksum_protected);
    assert!(report.object_ids_embedded);
    assert!(report.routing_buckets_embedded);
    assert!(report.compression_supported);
    assert_eq!(report.sealed_bands, 1);
    assert_eq!(report.active_bands, 1);
    assert!(report.live_physical_bytes > 0);
    assert!(report.page_store_writes > 0);
    assert!(report.page_store_bytes_written > 0);
    assert!(report.logical_bytes_written >= 512);
    assert!(report.compressed_records_written > 0);
    assert!(report
        .compatibility_gaps
        .iter()
        .any(|gap| gap.contains("migration-only") && gap.contains("page-header")));
    assert!(report
        .compatibility_gaps
        .iter()
        .any(|gap| gap.contains("binary protobuf page header")));
    assert!(report
        .compatibility_gaps
        .iter()
        .any(|gap| gap.contains("golden page conversion/replay")));
}

#[test]
fn storage_production_readiness_policy_can_block_dirty_dump_lag_and_missing_manifest() {
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
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "policy-key".to_string(),
                    value: b"policy-value".to_vec(),
                },
            })
            .status
            .ok
    );

    let report = engine.storage_production_readiness_report_with_policy(
        1,
        StorageProductionReadinessPolicy {
            max_dirty_buckets: Some(0),
            max_undumped_wal_records: Some(0),
            require_bucket_dump_manifest: true,
            ..StorageProductionReadinessPolicy::default()
        },
    );

    assert!(!report.production_ready, "{report:?}");
    assert_eq!(report.policy.max_dirty_buckets, Some(0));
    assert_eq!(report.dirty_bucket_count, 1);
    assert!(report.undumped_wal_records > 0);
    assert!(report
        .blockers
        .contains(&"dirty_slots_exceed_policy".to_string()));
    assert!(report
        .blockers
        .contains(&"undumped_wal_records_exceed_policy".to_string()));
    assert!(report
        .blockers
        .contains(&"slot_dump_manifest_required".to_string()));
}

#[test]
fn storage_production_readiness_policy_can_promote_warnings_to_blockers() {
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
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "warn-key".to_string(),
                    value: b"warn-value".to_vec(),
                },
            })
            .status
            .ok
    );

    let report = engine.storage_production_readiness_report_with_policy(
        1,
        StorageProductionReadinessPolicy {
            block_on_warnings: true,
            ..StorageProductionReadinessPolicy::default()
        },
    );

    assert!(!report.production_ready, "{report:?}");
    assert!(report
        .warnings
        .contains(&"dirty_slots_pending_dump".to_string()));
    assert!(report
        .blockers
        .contains(&"warnings_exceed_policy".to_string()));
}

#[test]
fn storage_production_readiness_blocks_corrupt_live_page_slabs() {
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
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "corrupt-key".to_string(),
                    value: b"corrupt-value".to_vec(),
                },
            })
            .status
            .ok
    );
    let slab_id = engine.live_page_slab_ids(1)[0];
    let mut bytes = engine.block_store().read_slab(slab_id).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    engine
        .block_store()
        .install_slab(slab_id, &bytes)
        .unwrap();

    let report = engine.storage_production_readiness_report(1);
    assert!(!report.production_ready, "{report:?}");
    assert!(report
        .blockers
        .contains(&"corrupt_page_segments".to_string()));
    assert!(report
        .blockers
        .contains(&"unreadable_live_page_refs".to_string()));
    assert!(report
        .blockers
        .contains(&"storage_segment_integrity_failed".to_string()));
    assert!(!report.slab_integrity.integrity_ok);
    assert!(report.slab_integrity.corrupt_page_slab_count > 0);
    assert!(report.slab_integrity.unreadable_page_ref_count > 0);
    assert!(report.corrupt_page_slab_count > 0);
    assert!(report.unreadable_page_ref_count > 0);
}

#[test]
fn storage_lifecycle_apply_warms_cache_from_page_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        32,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "warm-me".to_string(),
            value: vec![7; 128],
        },
    });
    engine.cache().invalidate_shard(1).unwrap();
    let plan = engine.storage_lifecycle_plan(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: Vec::new(),
        max_dump_buckets_per_round: 0,
        min_undumped_wal_records: 0,
        purge_delayed_destroy: false,
        prune_bucket_dump_manifests: false,
        roll_forward_bucket_dump_installs: false,
        follower_replay_cursors: Vec::new(),
        invalidate_cache: false,
        warm_cache: false,
        ..StorageLifecycleRequest::default()
    });
    let report = engine.apply_storage_lifecycle(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: plan.selected_dump_buckets,
        max_dump_buckets_per_round: 0,
        min_undumped_wal_records: 0,
        purge_delayed_destroy: false,
        prune_bucket_dump_manifests: false,
        roll_forward_bucket_dump_installs: false,
        follower_replay_cursors: Vec::new(),
        invalidate_cache: false,
        warm_cache: true,
        ..StorageLifecycleRequest::default()
    });
    assert!(report.cache_warmup_page_refs >= 1);
    assert_eq!(
        report.cache_warmup.warmed_page_refs,
        report.cache_warmup_page_refs
    );
    assert!(report.cache_warmup.considered_page_refs >= 1);
    assert!(report.cache_warmup.block_store_reads >= 1);
    assert!(report.cache_warmup.warmed_bytes >= 128);
    assert_eq!(report.cache_warmup.failed_page_refs, 0);
    assert!(engine.cache().stats().puts >= 1);
}

#[test]
fn storage_cache_warmup_report_filters_buckets_and_counts_cache_hits() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let first_key = "warm-slot-a";
    let first_bucket = engine.routing_bucket_for_key(1, first_key);
    let second_key = (0..100)
        .map(|index| format!("warm-slot-b-{index}"))
        .find(|key| engine.routing_bucket_for_key(1, key) != first_bucket)
        .expect("test should find a key in another slot");
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: first_key.to_string(),
            value: b"value-a".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: second_key,
            value: b"value-b".to_vec(),
        },
    });
    engine.cache().invalidate_shard(1).unwrap();

    let bucket = first_bucket;
    let first = engine.storage_cache_warmup_report(1, [bucket]);
    assert_eq!(first.selected_buckets, vec![bucket]);
    assert_eq!(first.considered_page_refs, 1);
    assert_eq!(first.skipped_page_refs, 1);
    assert_eq!(first.block_store_reads, 1);
    assert_eq!(first.already_cached_page_refs, 0);
    assert_eq!(first.failed_page_refs, 0);
    assert!(first.warmed_bytes > 0);

    let second = engine.storage_cache_warmup_report(1, [bucket]);
    assert_eq!(second.considered_page_refs, 1);
    assert_eq!(second.skipped_page_refs, 1);
    assert_eq!(second.block_store_reads, 0);
    assert_eq!(second.already_cached_page_refs, 1);
    assert_eq!(second.warmed_page_refs, 1);
}

#[test]
fn storage_cache_inspection_reports_bucket_entries_and_invalidates_bucket() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let key = "slot-cache-key";
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: key.to_string(),
                    value: b"slot-cache-value".to_vec(),
                },
            })
            .status
            .ok
    );
    engine.cache().invalidate_shard(1).unwrap();
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: key.to_string(),
                },
            })
            .status
            .ok
    );

    let bucket = engine.routing_bucket_for_key(1, key);
    let report = engine.storage_cache_inspection_report(1);
    assert!(report.stats.disk_fills >= 1);
    assert!(report
        .entries
        .iter()
        .any(|entry| entry.selector.starts_with(&format!("slot-{bucket}:"))));
    assert!(report
        .bucket_summaries
        .iter()
        .any(|summary| summary.routing_bucket == bucket && summary.entry_count >= 1));

    let invalidated = engine
        .invalidate_storage_cache_bucket(StorageCacheInvalidateBucketRequest {
            shard_id: 1,
            routing_bucket: bucket,
        })
        .unwrap();
    assert!(invalidated.memory_entries_removed >= 1);
    let after = engine.storage_cache_inspection_report(1);
    assert!(!after
        .entries
        .iter()
        .any(|entry| entry.selector.starts_with(&format!("slot-{bucket}:"))));
}

#[test]
fn tiny_cache_dump_load_restart_refills_from_disk_block_cache() {
    let dir = tempfile::tempdir().unwrap();
    let cache_dir = dir.path().join("cache");
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let restore_index_dir = dir.path().join("restore-indexes");
    let engine = TemporalEngine::with_local_dirs(32, &cache_dir, &page_dir, &index_dir);
    engine.load_shard(1);
    let target_value = b"dump-load-target-1234".to_vec();
    for (key, value) in [
        ("target", target_value.clone()),
        ("churn-a", b"cache-churn-a-1234".to_vec()),
        ("churn-b", b"cache-churn-b-1234".to_vec()),
    ] {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: key.to_string(),
                        value,
                    },
                })
                .status
                .ok
        );
    }
    let target_page_key = {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let address = shards
            .get(&1)
            .unwrap()
            .strings
            .get("target")
            .unwrap()
            .clone();
        CacheKey::page_with_slot(
            1,
            address.page_slab_id,
            address.offset,
            address.length,
            address.routing_bucket,
        )
    };

    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "target".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(target_value.clone())
        }
    );
    for key in ["churn-a", "churn-b"] {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: key.to_string(),
                    },
                })
                .status
                .ok
        );
    }
    assert!(engine.cache().stats().memory_evictions > 0);
    assert!(engine.cache().stats().disk_bytes > 0);
    assert_eq!(engine.cache().get_memory(&target_page_key), None);
    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("slot dump manifest should persist");
    engine.validate_bucket_dump_manifest(&manifest).unwrap();

    let restored = TemporalEngine::with_local_dirs(32, &cache_dir, &page_dir, &restore_index_dir);
    restored.load_shard(1);
    restored
        .install_bucket_dump_manifest(&manifest)
        .expect("slot dump should install after restart");
    let page_reads_before = restored.block_store().stats().reads;
    let disk_hits_before = restored.cache().stats().disk_hits;
    let response = restored.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "target".to_string(),
        },
    });
    assert_eq!(
        response.response,
        CommandResponse::Bytes {
            value: Some(target_value)
        }
    );
    assert_eq!(
        restored.block_store().stats().reads,
        page_reads_before,
        "restored engine should refill from disk block cache before block store"
    );
    assert!(restored.cache().stats().disk_hits > disk_hits_before);

    let bucket = restored.routing_bucket_for_key(1, "target");
    let cache_report = restored.storage_cache_inspection_report(1);
    assert!(cache_report
        .bucket_summaries
        .iter()
        .any(|summary| summary.routing_bucket == bucket && summary.entry_count >= 1));
    let invalidated = restored
        .invalidate_storage_cache_bucket(StorageCacheInvalidateBucketRequest {
            shard_id: 1,
            routing_bucket: bucket,
        })
        .unwrap();
    assert!(invalidated.memory_entries_removed >= 1);
    let readiness = restored.storage_production_readiness_report(1);
    assert!(readiness.production_ready, "{readiness:?}");
    assert_eq!(readiness.unreadable_page_ref_count, 0);
    assert_eq!(readiness.corrupt_page_slab_count, 0);
}

#[test]
fn storage_lifecycle_plan_matches_delayed_and_limited_dirty_bucket_dump_policy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for i in 0..128 {
        let key = format!("slot-{i}");
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.clone(),
                value: key.as_bytes().to_vec(),
            },
        });
        let observed = engine.storage_lifecycle_plan(StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_buckets: Vec::new(),
            max_dump_buckets_per_round: 0,
            min_undumped_wal_records: 0,
            purge_delayed_destroy: false,
            prune_bucket_dump_manifests: false,
            roll_forward_bucket_dump_installs: false,
            follower_replay_cursors: Vec::new(),
            invalidate_cache: false,
            warm_cache: false,
            ..StorageLifecycleRequest::default()
        });
        if observed.dirty_buckets.len() >= 3 {
            break;
        }
    }

    let delayed = engine.storage_lifecycle_plan(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: Vec::new(),
        max_dump_buckets_per_round: 0,
        min_undumped_wal_records: 99,
        purge_delayed_destroy: false,
        prune_bucket_dump_manifests: false,
        roll_forward_bucket_dump_installs: false,
        follower_replay_cursors: Vec::new(),
        invalidate_cache: false,
        warm_cache: false,
        ..StorageLifecycleRequest::default()
    });
    assert!(delayed.dump_delayed);
    assert!(delayed.selected_dump_buckets.is_empty());
    assert!(delayed
        .reasons
        .contains(&"dirty_slot_dump_delayed".to_string()));

    let limited = engine.storage_lifecycle_plan(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: Vec::new(),
        max_dump_buckets_per_round: 2,
        min_undumped_wal_records: 1,
        purge_delayed_destroy: false,
        prune_bucket_dump_manifests: false,
        roll_forward_bucket_dump_installs: false,
        follower_replay_cursors: Vec::new(),
        invalidate_cache: false,
        warm_cache: false,
        ..StorageLifecycleRequest::default()
    });
    assert!(!limited.dump_delayed);
    assert!(limited.undumped_wal_records >= 3);
    assert_eq!(limited.selected_dump_buckets.len(), 2);
    assert!(limited.dirty_buckets.len() >= limited.selected_dump_buckets.len());

    let explicit = engine.storage_lifecycle_plan(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: vec![delayed.dirty_buckets[0]],
        max_dump_buckets_per_round: 0,
        min_undumped_wal_records: 99,
        purge_delayed_destroy: false,
        prune_bucket_dump_manifests: false,
        roll_forward_bucket_dump_installs: false,
        follower_replay_cursors: Vec::new(),
        invalidate_cache: false,
        warm_cache: false,
        ..StorageLifecycleRequest::default()
    });
    assert!(!explicit.dump_delayed);
    assert_eq!(explicit.selected_dump_buckets, vec![delayed.dirty_buckets[0]]);
}
