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
    // stored inside the dumped index. We reproduce the same tail-ahead-of-index
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
    // not loaded, so the topology-retryable client refreshes + retries.
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
    // The WAL-reclaim routine dumps dirty buckets oldest-first (by first-dirty-log-id).
    // Rust selected dirty buckets by ascending routing_bucket id then
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
    // desync structurally (the compactor leaves the index unchanged on failure + one atomic commit).
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

    // On the delta served-index path the per-write base rewrite is deferred, so the base file
    // may not exist yet before the first compaction (the served index is the live shard; a
    // compaction is what materializes the base). Tolerate an absent base here: the assertion
    // below still holds -- compaction must durably write the relocated partial index, so
    // `index_after` differs from an absent/empty `index_before` exactly as it differs from a
    // per-write-persisted one on the default path.
    let index_before = fs::read(engine.index_path(1)).unwrap_or_default();

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
    // Durability conformance: a synchronous write whose durable WAL commit fails must NOT be acked ok.
    // The WAL is the recovery source of truth, so a swallowed append error would tell the client a
    // write that is gone after a crash succeeded. surfaces the wal Commit failure
    // We inject the failure by replacing the WAL file with a
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

    // The interrupted-install roll-forward phase below spins up a SECOND engine (`restarted`) on
    // the SAME index dir (WAL) AND pages dir as `engine`, then keeps writing on `engine`. On the
    // default path the second engine's load folds the served-index delta and reuses the existing
    // page addresses, so the shared page store is not mutated. Base-only single-barrier recovery
    // re-derives pages by REPLAYING the shared WAL, which rewrites pages into the shared slab files
    // and desynchronizes the two engines' independent block-store write offsets -- corrupting this
    // two-instances-on-one-storage construction. That is a split-brain scenario, not single-engine
    // crash recovery (whose zero-loss guarantee is proven exhaustively by the subprocess crash
    // harness in tests/wal_single_barrier_recovery.rs, including data-page loss after a dump and
    // exactly-once counter replay). This phase is therefore exercised on the default path only.
    if !crate::engine::wal_single_barrier() {
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
    // Retention keeps the NEWEST manifest and nothing else. The parent is a complete,
    // self-contained index that nothing recovers through, so with no cursor pinning it, it is
    // prunable just like the off-chain fork.
    assert!(plan.retained_manifest_ids.contains(&child.manifest_id));
    assert!(!plan.retained_manifest_ids.contains(&parent.manifest_id));
    assert!(plan.prunable_manifest_ids.contains(&parent.manifest_id));
    assert!(plan.prunable_manifest_ids.contains(&fork.manifest_id));
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
    // Retention keeps the newest manifest only, so the obsolete fork AND the older parent are
    // both removed -- with no follower cursor or snapshot ref supplied, nothing pins them.
    assert!(report.removed_manifest_ids.contains(&fork.manifest_id));
    assert!(report.removed_manifest_ids.contains(&parent.manifest_id));
    assert_eq!(report.removed_marker_files, 1);
    assert!(!bucket_dump_manifest_path(&engine.index_dir, 1, &parent.manifest_id).exists());
    assert!(!bucket_dump_manifest_path(&engine.index_dir, 1, &fork.manifest_id).exists());
    let surviving = list_bucket_dump_manifests_at(&engine.index_dir, 1).unwrap();
    assert_eq!(
        surviving.len(),
        1,
        "only the newest manifest survives, got {surviving:?}"
    );
    // Pruning detaches the survivor from its removed parent, so a dangling parent link keeps
    // meaning corruption rather than ordinary history.
    assert!(surviving[0].parent_manifest_id.is_none());
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
    // Once a dirty bucket is dumped its dirty flag is cleared, so the storage cycle does not
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

/// A follower behind EVERY dump must be reported, not passed over in silence.
///
/// A cursor pins the newest dump at or below it. One that sits below all of them matched nothing
/// and fell straight through, so the case where NO retained dump can serve a follower -- the one
/// that most needs saying -- produced no signal at all, and pruning went ahead and threw away the
/// only dump that could ever have helped. The oldest is kept instead, and reported with a reason
/// that separates "pins an older dump" from "is behind everything we kept".
#[test]
fn a_follower_behind_every_dump_is_reported_and_keeps_the_oldest() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for value in ["v1", "v2", "v3"] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "behind".to_string(),
                value: value.as_bytes().to_vec(),
            },
        });
        engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    }
    let plan_before = engine.bucket_dump_manifest_prune_plan(1);
    let oldest = plan_before
        .prunable_manifest_ids
        .first()
        .cloned()
        .expect("older dumps are prunable when nothing pins them");

    // A follower older than every dump: sequence zero precedes them all.
    let plan = engine.bucket_dump_manifest_prune_plan_with_follower_cursors(
        1,
        vec![BucketDumpFollowerReplayCursor {
            follower_id: "follower-behind-everything".to_string(),
            shard_id: 1,
            wal_sequence: 0,
            index_log_sequence: 0,
        }],
    );

    assert_eq!(
        plan.follower_blocks.len(),
        1,
        "a follower no retained dump can serve must be reported, not skipped: {plan:?}"
    );
    assert_eq!(
        plan.follower_blocks[0].reason,
        "follower_cursor_precedes_every_manifest"
    );
    assert!(
        plan.retained_manifest_ids.contains(&plan.follower_blocks[0].manifest_id),
        "the dump reported as blocking must actually be kept"
    );
    assert!(
        !plan.prunable_manifest_ids.contains(&oldest),
        "the oldest dump is this follower's only chance and must not be pruned"
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
    // Without a cursor only the newest manifest is retained, so both the older parent and the
    // off-chain fork are prunable.
    assert!(no_cursor.prunable_manifest_ids.contains(&fork.manifest_id));
    assert!(no_cursor.prunable_manifest_ids.contains(&parent.manifest_id));
    assert!(!no_cursor.prunable_manifest_ids.contains(&child.manifest_id));

    let lagging_cursor = BucketDumpFollowerReplayCursor {
        follower_id: "follower-a".to_string(),
        shard_id: 1,
        wal_sequence: fork.wal_sequence,
        index_log_sequence: fork.index_log_sequence,
    };
    let blocked =
        engine.bucket_dump_manifest_prune_plan_with_follower_cursors(1, vec![lagging_cursor.clone()]);
    // The lagging cursor pins the manifest it would replay from, so that manifest is retained
    // and reported as the thing blocking the prune.
    assert_eq!(blocked.follower_blocks.len(), 1);
    assert_eq!(blocked.follower_blocks[0].follower_id, "follower-a");
    let anchored = blocked.follower_blocks[0].manifest_id.clone();
    assert!(blocked.retained_manifest_ids.contains(&anchored));
    assert!(!blocked.prunable_manifest_ids.contains(&anchored));
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
    // Once the cursor catches up to the newest manifest it pins nothing older, so every older
    // manifest -- the parent and the off-chain fork -- becomes prunable again.
    assert!(caught_up.prunable_manifest_ids.contains(&fork.manifest_id));
    assert!(caught_up.prunable_manifest_ids.contains(&parent.manifest_id));
    assert!(!caught_up.prunable_manifest_ids.contains(&child.manifest_id));
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
    // Same as the follower case: only the newest manifest is retained without a pin.
    assert!(no_snapshot.prunable_manifest_ids.contains(&fork.manifest_id));
    assert!(no_snapshot.prunable_manifest_ids.contains(&parent.manifest_id));
    assert!(!no_snapshot.prunable_manifest_ids.contains(&child.manifest_id));

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
    // The snapshot ref pins the manifest it would install from; anything else older is still
    // prunable, since only the newest manifest is retained unconditionally.
    assert!(blocked.retained_manifest_ids.contains(&fork.manifest_id));
    assert!(!blocked.prunable_manifest_ids.contains(&fork.manifest_id));
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
    // Once the snapshot ref advances to the newest manifest it pins nothing older, so both the
    // parent and the off-chain fork become prunable again.
    assert!(advanced.prunable_manifest_ids.contains(&fork.manifest_id));
    assert!(advanced.prunable_manifest_ids.contains(&parent.manifest_id));
    assert!(!advanced.prunable_manifest_ids.contains(&child.manifest_id));
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
        // A manifest carries the index in the format it was written in; decode and re-encode it
        // through the funnel rather than assuming JSON on either side.
        let mut restored = crate::engine::decode_index_bytes(&reused_owner.index_bytes)
            .expect("manifest index should decode");
        let address = restored
            .strings
            .get_mut("lifecycle")
            .expect("manifest string address");
        address.object_id = Some(address.object_id.unwrap_or_default().wrapping_add(1));
        reused_owner.index_bytes = crate::engine::encode_index_bytes(&restored);
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
    assert!(!report.log_compatibility.native_binary_compatible);
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
    assert!(!report.log_compatibility.native_reader_supported);
    assert!(!report.log_compatibility.native_writer_supported);
    assert!(report.page_format_compatibility.rust_native_read_safe);
    assert!(!report.page_format_compatibility.native_page_header_compatible);
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
            .native_page_header_reader_supported
    );
    assert!(
        !report
            .page_format_compatibility
            .native_page_header_writer_supported
    );
    assert!(report.page_format_compatibility.checksum_protected);
    assert!(report.page_format_compatibility.object_ids_embedded);
    assert!(report.block_store_bytes_written > 0);
}

#[test]
fn storage_log_compatibility_report_counts_jsonl_sequences_and_native_gaps() {
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
    assert!(!report.native_reader_supported);
    assert!(!report.native_writer_supported);
    assert!(report.golden_conversion_required);
    assert_eq!(report.wal_last_sequence, 2);
    assert_eq!(report.index_log_last_sequence, 2);
    assert_eq!(report.wal_records, 2);
    assert_eq!(report.index_log_records, 2);
    assert!(report.wal_bytes > 0);
    assert!(report.index_log_bytes > 0);
    assert!(report.rust_native_replay_safe);
    assert!(!report.native_binary_compatible);
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
    assert!(!report.native_page_header_reader_supported);
    assert!(!report.native_page_header_writer_supported);
    assert!(report.golden_conversion_required);
    assert!(report.rust_native_read_safe);
    assert!(!report.native_page_header_compatible);
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

// E1 regression: one engine hosts many shards over a SINGLE page_store with a global slab
// cursor, so two shards' pages can share a slab. Page reclaim used to compute the live set from
// only the shard whose cycle was running, so a slab holding another shard's committed pages
// looked like an orphan and was deleted -- silent cross-shard data loss.
//
// This test builds exactly that: shard B's page lands in slab 0, the slab is sealed by a roll,
// shard A's page lands in slab 1, then shard A's storage-manager cycle runs page reclaim. Slab 0
// is absent from shard A's live set, below the retention floor, and not the current slab, so the
// legacy per-shard live set deletes it. The fix unions live slab ids across ALL loaded shards, so
// slab 0 (live in shard B) is retained.
//
// FAIL-before / PASS-after is demonstrated with the TS_CROSS_SHARD_RECLAIM_GUARD kill-switch:
// running this test with TS_CROSS_SHARD_RECLAIM_GUARD=0 reproduces the legacy per-shard behavior
// and the assertions below fail (slab 0 is moved out of the read path); the default (guard on)
// passes.
#[test]
fn cross_shard_page_reclaim_retains_another_shards_live_slab() {
    let dir = tempfile::tempdir().unwrap();
    let pages = dir.path().join("pages");
    let indexes = dir.path().join("indexes");
    // Tiny cache so reads hit the slab on disk rather than an in-memory copy.
    let engine =
        TemporalEngine::with_local_dirs(16, dir.path().join("cache"), &pages, &indexes);
    // Two shards hosted by the SAME engine => the SAME page_store + global slab cursor.
    engine.load_shard(1); // shard A -- runs the reclaim cycle
    engine.load_shard(2); // shard B -- owns the shared slab's live page

    // Shard B's committed page lands in the current slab (slab 0).
    let response = engine.execute(ExecuteRequest {
        shard_id: 2,
        command: Command::StringSet {
            key: "shard-b-key".to_string(),
            value: b"shard-b-committed-value".to_vec(),
        },
    });
    assert!(response.status.ok, "{response:?}");

    // Identify the slab file backing shard B's page (the only .seg under the pages root).
    let shard_b_slab_path = fs::read_dir(&pages)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.starts_with("page_segment_") && name.ends_with(".seg"))
                .unwrap_or(false)
        })
        .expect("shard B write must create a page slab");

    // Seal slab 0 so future appends go to a fresh slab, then write shard A into slab 1. Slab 0 is
    // now a non-current slab that is live ONLY in shard B.
    engine.block_store().roll_slab().unwrap();
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "shard-a-key".to_string(),
            value: b"shard-a-committed-value".to_vec(),
        },
    });
    assert!(response.status.ok, "{response:?}");

    // Sanity: from shard A's own viewpoint, slab 0 is NOT live (it holds no shard-A pages), which
    // is precisely why the legacy per-shard reclaim would delete it.
    assert!(
        !engine.live_page_slab_ids(1).contains(&0),
        "slab 0 must be absent from shard A's per-shard live set"
    );
    assert!(
        engine.live_page_slab_ids_all_shards().contains(&0),
        "slab 0 must be present in the cross-shard union live set"
    );

    // Run shard A's storage-manager cycle with page reclaim (only). Other stages are off so the
    // test isolates the reclaim decision.
    let cycle = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: 1,
        enable_prepare: true,
        enable_wal_reclaim: false,
        enable_evict: false,
        enable_expire: false,
        enable_page_reclaim: true,
        enable_page_compaction: false,
        enable_index_gc: false,
        ..StorageManagerCycleRequest::default()
    });
    assert!(
        cycle.errors.is_empty(),
        "reclaim cycle should not error: {:?}",
        cycle.errors
    );

    // Decisive, cache-independent check: shard B's slab file must still be in the read path (the
    // legacy per-shard reclaim moves it into the delayed-destroy trash, vanishing shard B's data).
    assert!(
        shard_b_slab_path.exists(),
        "shard B's live slab was reclaimed by shard A's cycle -> cross-shard data loss"
    );

    // Behavioral check: shard B's committed value is still readable after shard A's cycle.
    let read_back = engine.execute(ExecuteRequest {
        shard_id: 2,
        command: Command::StringGet {
            key: "shard-b-key".to_string(),
        },
    });
    assert_eq!(
        read_back.response,
        CommandResponse::Bytes {
            value: Some(b"shard-b-committed-value".to_vec()),
        },
        "shard B's committed value must survive shard A's reclaim cycle"
    );
}

// E5 regression: FeatureQuery / FeatureQueryFiltered used to skip lazy expiry, so a key past its
// deadline but not yet swept read live points from FeatureQuery while FeatureAggQuery (which
// applies remove_if_expired) read empty -- an inconsistency between two reads of the same key.
// All feature reads must agree: an expired-but-unswept key reads empty everywhere.
#[test]
fn expired_feature_key_reads_empty_consistently_across_feature_reads() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for command in [
        Command::FeatureAppend {
            key: "expiring-feature".to_string(),
            points: vec![FeaturePoint {
                timestamp_ms: 10,
                value: b"ten".to_vec(),
            }],
        },
        Command::CommonExpire {
            key: "expiring-feature".to_string(),
            ttl_ms: 1,
        },
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "{response:?}");
    }
    // Cross the deadline but run NO sweep: this is the window where lazy expiry must fire.
    std::thread::sleep(std::time::Duration::from_millis(5));

    let feature_query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery {
            key: "expiring-feature".to_string(),
            start_ms: 0,
            end_ms: 1000,
            count: None,
        },
    });
    assert_eq!(
        feature_query.response,
        CommandResponse::FeaturePoints { points: Vec::new() },
        "FeatureQuery must apply lazy expiry and read empty for an expired-but-unswept key"
    );

    let filtered_query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQueryFiltered {
            key: "expiring-feature".to_string(),
            start_ms: 0,
            end_ms: 1000,
            count: None,
            filters: Vec::new(),
        },
    });
    assert_eq!(
        filtered_query.response,
        CommandResponse::FeaturePoints { points: Vec::new() },
        "FeatureQueryFiltered must apply lazy expiry and read empty for an expired-but-unswept key"
    );

    let agg_query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAggQuery {
            key: "expiring-feature".to_string(),
            start_ms: 0,
            end_ms: 1000,
            aggregator: "count".to_string(),
            count: None,
        },
    });
    assert_eq!(
        agg_query.response,
        CommandResponse::Aggregate { value: 0 },
        "FeatureAggQuery reads empty for an expired key -- the other feature reads must match"
    );
}

// RN2: a storage-manager cycle must be an inert no-op while the shard is RECOVERING (WAL
// replay in progress). A GC/compaction/reclaim round interleaved with an in-flight replay
// would observe a half-reconstructed bucket index and could mis-reclaim a still-live page.
#[test]
fn storage_manager_cycle_is_a_noop_while_shard_is_recovering() {
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
    for i in 0..4 {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("k{i}"),
                value: b"v".to_vec(),
            },
        });
        assert!(response.status.ok, "{response:?}");
    }
    drop(engine);

    // Restart and park the shard in the recovery window (recovering=true, replay not yet run).
    let restarted = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-b"),
        &page_dir,
        &index_dir,
    );
    let watermark = restarted.test_publish_recovering_shard(1);
    let wal_before = restarted.write_ahead_log_store().stats(1).last_sequence;

    let report = restarted.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: 1,
        max_dump_buckets_per_round: 16,
        min_undumped_wal_records: 0,
        warm_cache: true,
        enable_wal_reclaim: true,
        enable_page_reclaim: true,
        enable_page_compaction: true,
        enable_index_gc: true,
        ..StorageManagerCycleRequest::default()
    });
    assert!(
        !report.completed,
        "a storage-manager cycle must not complete while the shard is recovering: {report:?}"
    );
    assert!(
        report.stages.is_empty(),
        "a recovering cycle must build no stages (it returns early before any mutation)"
    );
    assert!(
        report
            .errors
            .iter()
            .any(|error| error.contains("recovering") || error.contains("recovery")),
        "the skip reason must name recovery: {:?}",
        report.errors
    );
    // The WAL tail is untouched (nothing reclaimed), and recovery still completes normally.
    assert_eq!(
        restarted.write_ahead_log_store().stats(1).last_sequence,
        wal_before,
        "no WAL record may be reclaimed during recovery"
    );
    restarted.test_finish_recovery(1, watermark);
    let after = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k0".to_string(),
        },
    });
    assert!(after.status.ok, "the shard serves once recovery completes");
}

// F4: a corrupt / unreadable WAL scan on load is DATA LOSS, not "nothing to replay". The load
// must refuse rather than silently serving the stale base index (dropping the committed WAL
// tail). A value-preserving bit-flip in a committed, newline-terminated record exercises this.
#[test]
fn corrupt_wal_scan_refuses_load_rather_than_truncating() {
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
    for key in ["keepme", "corruptme"] {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: key.to_string(),
                        value: b"v".to_vec(),
                    },
                })
                .status
                .ok
        );
    }
    drop(engine);

    let wal_path = index_dir.join("wals").join("shard-1.wal.jsonl");
    let mut bytes = std::fs::read(&wal_path).unwrap();
    let position = bytes
        .windows(9)
        .position(|window| window == b"corruptme")
        .expect("the second WAL record is present");
    bytes[position + 1] = b'0'; // "corruptme" -> "c0rruptme": still valid JSON, wrong digest
    std::fs::write(&wal_path, &bytes).unwrap();

    let restarted = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-b"),
        &page_dir,
        &index_dir,
    );
    let response = restarted.load_shard_with(LoadShardRequest {
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
        "a corrupt WAL scan must refuse the load, not serve a truncated prefix: {:?}",
        response.status
    );
}

// F3: a corrupt INTERIOR served-index delta record must abort the load, not be silently
// skipped (a skipped delta loses a removal/eviction recorded only there -> resurrection or
// dangling ref).
#[test]
fn corrupt_index_log_delta_refuses_load_rather_than_silently_skipping() {
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
    for key in ["alpha", "beta", "gamma"] {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: key.to_string(),
                        value: b"v".to_vec(),
                    },
                })
                .status
                .ok
        );
    }
    drop(engine);

    let index_log_path = index_dir
        .join("indexlogs")
        .join("shard-1.indexlog.jsonl");
    let contents = std::fs::read(&index_log_path).unwrap();
    let mut lines: Vec<Vec<u8>> = contents
        .split(|&byte| byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| line.to_vec())
        .collect();
    assert!(
        lines.len() >= 2,
        "the write path must have appended index-log delta records"
    );
    // Corrupt an interior record (not the tail) so it is committed corruption, not a torn tail.
    lines[0] = b"corrupt-not-a-record".to_vec();
    let mut rebuilt = Vec::new();
    for line in &lines {
        rebuilt.extend_from_slice(line);
        rebuilt.push(b'\n');
    }
    std::fs::write(&index_log_path, &rebuilt).unwrap();

    let restarted = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-b"),
        &page_dir,
        &index_dir,
    );
    let response = restarted.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: u32::MAX,
        readonly: false,
        table_name: String::new(),
    });
    if crate::engine::wal_single_barrier() {
        // Base-only single-barrier recovery (the DEFAULT) never folds the served-index delta:
        // it re-derives state from the durable base checkpoint + WAL replay (+ config-log),
        // which is a COMPLETE source of truth (evictions/removals are WAL/config-log
        // re-derivable, not delta-only). A corrupt delta is therefore never parsed and cannot
        // cause a silent skip of a delta-only removal -- so the load must SUCCEED and reconstruct
        // the EXACT logical state. We prove no silent loss by reading every acked key back at its
        // written value (a strictly stronger guarantee than the original binary refuse/accept),
        // so this generalization preserves the test's protection rather than weakening it. The
        // delta-fold refuse-on-corruption path is still asserted below under the legacy escape
        // hatch (and covered by the subprocess crash harness's drop-indexlog cases).
        assert!(
            response.status.ok,
            "base-only recovery ignores the un-folded delta and must load: {:?}",
            response.status
        );
        for key in ["alpha", "beta", "gamma"] {
            let got = restarted.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: key.to_string(),
                },
            });
            assert!(
                got.status.ok,
                "read of {key} must succeed after base-only recovery: {:?}",
                got.status
            );
            assert_eq!(
                got.response,
                CommandResponse::Bytes {
                    value: Some(b"v".to_vec())
                },
                "base-only recovery must preserve the exact value for {key} (no silent loss)"
            );
        }
    } else {
        assert!(
            !response.status.ok,
            "a corrupt interior index-log delta must refuse the load, not be silently skipped: {:?}",
            response.status
        );
    }
}

/// Eviction cost must track how many victims are wanted, not how much the shard holds.
///
/// Measured with the live-page scan counter rather than a clock, so the number is the work done
/// rather than the speed of the machine, and the test cannot pass by happening to run fast.
///
/// Serialized against other tests in this file only by the fact that it reads a process-wide
/// counter around its own call; it resets immediately before measuring.
#[test]
fn sampled_eviction_scan_volume_does_not_grow_with_the_store() {
    fn scan_volume_for(page_count: usize, sampled: bool) -> (u64, usize) {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..page_count {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("scaling-key-{index}"),
                    value: vec![b'v'; 64],
                },
            });
        }

        if sampled {
            std::env::set_var("TS_EVICT_SAMPLED_LRU", "1");
        } else {
            std::env::remove_var("TS_EVICT_SAMPLED_LRU");
        }
        crate::engine::reset_live_page_scan_entries();
        // Threshold 0 so the pressure gate always admits and the selection path actually runs.
        let report = engine.apply_storage_eviction(1, 0, 4, false, false);
        let scanned = crate::engine::live_page_scan_entries();
        std::env::remove_var("TS_EVICT_SAMPLED_LRU");
        (scanned, report.selected_victims.len())
    }

    let (small_full, _) = scan_volume_for(200, false);
    let (large_full, _) = scan_volume_for(1600, false);
    let (small_sampled, small_victims) = scan_volume_for(200, true);
    let (large_sampled, large_victims) = scan_volume_for(1600, true);

    println!(
        "\n  full-scan   200 pages -> {small_full:>6} live-page entries scanned\n  \
         full-scan  1600 pages -> {large_full:>6} live-page entries scanned\n  \
         sampled     200 pages -> {small_sampled:>6} live-page entries scanned ({small_victims} victims)\n  \
         sampled    1600 pages -> {large_sampled:>6} live-page entries scanned ({large_victims} victims)\n"
    );

    // The default path pays for the whole store: 8x the pages costs materially more.
    assert!(
        large_full > small_full * 4,
        "full scan should grow with the store, got {small_full} -> {large_full}"
    );

    // The sampled path must not. It may still read the pages of the buckets it picked, which is
    // bounded by batch_limit, so this is a ceiling rather than an equality.
    assert!(
        large_sampled < large_full / 4,
        "sampled scan should be far below the full scan, got {large_sampled} vs {large_full}"
    );
    assert!(
        large_sampled <= small_sampled.saturating_mul(2).max(64),
        "sampled scan should not grow with the store, got {small_sampled} -> {large_sampled}"
    );
}

/// An async write whose cache entry is dropped before any dump must still read back.
///
/// Without this its only durable copy is the WAL record, at an address naming no file, so the
/// read returns MISSING for a write that was acked -- the hole the spill workaround was added
/// to paper over. Here the record itself serves the value.
#[test]
fn an_evicted_async_write_is_served_from_its_wal_record() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config {
            version: 2,
            async_storage: true,
            ..Config::default()
        },
    });

    std::env::set_var("TS_BLOCK_IN_WAL", "1");
    // The spill path would otherwise mask what is being tested by copying the value to a real
    // slab on eviction.
    std::env::set_var("TS_HOT_PAGE_SPILL", "0");

    let key = "block-in-wal-key";
    let value = b"block-in-wal-value".to_vec();
    let write = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: key.to_string(),
            value: value.clone(),
        },
    });
    assert!(write.status.ok, "the write must be acked");

    // Drop every cached copy: the value now exists only in its WAL record.
    engine.cache().invalidate_shard(1).unwrap();

    let read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: key.to_string(),
        },
    });
    std::env::remove_var("TS_BLOCK_IN_WAL");
    std::env::remove_var("TS_HOT_PAGE_SPILL");

    assert_eq!(
        read.response,
        CommandResponse::Bytes {
            value: Some(value)
        },
        "an acked write must not read back as missing once its cache entry is gone"
    );
}

/// A page that is DERIVED state must also survive its cache entry being dropped.
///
/// A hash field set stores a serialized map, so unlike a plain string the page cannot be
/// reconstructed from the command that wrote it. Serving it back therefore requires the record
/// to carry the page itself, which is what staging does.
#[test]
fn an_evicted_derived_page_is_served_from_its_wal_record() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config {
            version: 2,
            async_storage: true,
            ..Config::default()
        },
    });

    std::env::set_var("TS_BLOCK_IN_WAL", "1");
    std::env::set_var("TS_HOT_PAGE_SPILL", "0");

    let write = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashSet {
            key: "derived-page-key".to_string(),
            field: "f1".to_string(),
            value: b"derived-page-value".to_vec(),
        },
    });
    assert!(write.status.ok, "the write must be acked: {write:?}");

    // Drop every cached copy: the page now exists only inside its WAL record.
    engine.cache().invalidate_shard(1).unwrap();

    let read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashGet {
            key: "derived-page-key".to_string(),
            field: "f1".to_string(),
        },
    });
    std::env::remove_var("TS_BLOCK_IN_WAL");
    std::env::remove_var("TS_HOT_PAGE_SPILL");

    assert_eq!(
        read.response,
        CommandResponse::Bytes {
            value: Some(b"derived-page-value".to_vec())
        },
        "a derived page must not read back as missing once its cache entry is gone"
    );
}


/// A write handed its pages must log THOSE pages, not the ones it would have derived.
///
/// This is what lets a replayed write reproduce the bytes that were acked somewhere else
/// rather than this node's reconstruction of them.
#[test]
fn a_carried_page_is_what_reaches_the_log_record() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config {
            version: 2,
            async_storage: true,
            ..Config::default()
        },
    });
    std::env::set_var("TS_BLOCK_IN_WAL", "1");

    let carried = vec![crate::wal::StagedPage {
        object_id: 4242,
        bytes: b"pages-from-somewhere-else".to_vec(),
    }];
    let write = engine.execute_with_carried_pages(
        ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: "carried".to_string(),
                field: "f".to_string(),
                value: b"local-derivation".to_vec(),
            },
        },
        carried.clone(),
    );
    std::env::remove_var("TS_BLOCK_IN_WAL");
    assert!(write.status.ok, "the write must be acked: {write:?}");

    let records = engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .unwrap();
    let logged = records
        .iter()
        .filter_map(|(_, line)| crate::wal::decode_wal_line(line).ok())
        .find(|record| !record.staged_pages.is_empty())
        .expect("the record must carry pages");
    assert_eq!(
        logged.staged_pages, carried,
        "the carried pages belong on the record, not this node's re-derivation"
    );
}

// ---------------------------------------------------------------------------------------
// Interleavings: the shard does not hold still between a decision and the act it authorizes.
//
// Most of this suite is sequential -- set up, act, assert -- and that shape found none of the
// concurrency-adjacent defects in the reclaim sweep. These exercise the seam this codebase
// actually has: plan and apply are separate calls, so anything that happens in between is a
// real interleaving, reproducible without threads or timing.
// ---------------------------------------------------------------------------------------

/// A write that lands between planning a reclaim and applying it must survive.
///
/// The plan is computed, handed back -- over an RPC, or through a cycle stage -- and applied
/// later. The shard keeps taking writes the whole time. A plan authorizes dropping what the
/// durable index could replace WHEN IT WAS MADE, and a record that did not exist then was
/// never covered by that proof.
#[test]
fn a_reclaim_plan_does_not_authorize_dropping_writes_that_landed_after_it() {
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
            key: "before".to_string(),
            value: b"v1".to_vec(),
        },
    });
    engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();

    let plan = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());

    // The interleaving: a write lands after the plan was made, before it is applied.
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "after".to_string(),
                    value: b"v2".to_vec(),
                },
            })
            .status
            .ok
    );
    let after_sequence = engine.write_ahead_log_store().stats(1).last_sequence;

    let _report = engine.apply_storage_wal_reclaim(plan);

    let records = engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .unwrap();
    let highest = records
        .iter()
        .filter_map(|(_, line)| crate::wal::decode_wal_line(line).ok())
        .map(|record| record.sequence)
        .max()
        .unwrap_or(0);
    assert!(
        highest >= after_sequence,
        "the later write's record was reclaimed by a plan made before it existed \
         (highest kept {highest}, the write was at {after_sequence})"
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "after".to_string()
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"v2".to_vec())
        },
        "and it must still read back"
    );
}

/// A dump taken between planning a manifest prune and applying it must not be pruned.
///
/// The prune decides which manifests are redundant. A dump that completes after that decision
/// is the freshest thing the shard has, and pruning it would throw away the only manifest
/// covering the current generation.
#[test]
fn a_manifest_created_after_a_prune_plan_is_not_pruned_by_it() {
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
            value: b"v1".to_vec(),
        },
    });
    engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v2".to_vec(),
        },
    });
    engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();

    let _plan = engine.bucket_dump_manifest_prune_plan(1);

    // The interleaving: a third dump completes after the prune was planned.
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v3".to_vec(),
        },
    });
    let newest = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();

    let _report = engine.apply_bucket_dump_manifest_prune(1);

    let remaining = engine.list_bucket_dump_manifests(1);
    assert!(
        remaining
            .iter()
            .any(|manifest| manifest.manifest_id == newest.manifest_id),
        "the dump taken after the prune was planned is the freshest manifest and must survive; \
         remaining: {:?}",
        remaining
            .iter()
            .map(|manifest| &manifest.manifest_id)
            .collect::<Vec<_>>()
    );
}

/// The same page must still read back after the shard is RELOADED.
///
/// Registrations are live-path state: they are dropped when a shard unloads, and the standing
/// claim is that reload replays the WAL and re-derives every page. That only holds if replay
/// revisits the record carrying the page -- and replay starts ABOVE the persisted watermark,
/// which this very write advanced past its own record. If nothing rebuilds the mapping, the
/// served index is left pointing at a synthetic address naming no file, and an acked write
/// reads back as MISSING after a restart: exactly the hole staging was added to close,
/// reopened by a reload.
#[test]
fn a_block_in_wal_page_still_reads_back_after_a_shard_reload() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config {
            version: 2,
            async_storage: true,
            ..Config::default()
        },
    });

    std::env::set_var("TS_BLOCK_IN_WAL", "1");
    std::env::set_var("TS_HOT_PAGE_SPILL", "0");

    let key = "reload-block-key";
    let value = b"reload-block-value".to_vec();
    let write = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: key.to_string(),
            value: value.clone(),
        },
    });
    assert!(write.status.ok, "the write must be acked: {write:?}");

    // Drop every cached copy, then take the shard down and bring it back. This is the restart,
    // and it is what clears the registrations.
    engine.cache().invalidate_shard(1).unwrap();
    engine.unload_shard(1);
    engine.load_shard(1);

    let read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: key.to_string(),
        },
    });
    std::env::remove_var("TS_BLOCK_IN_WAL");
    std::env::remove_var("TS_HOT_PAGE_SPILL");

    assert_eq!(
        read.response,
        CommandResponse::Bytes {
            value: Some(value)
        },
        "an acked write must not read back as missing after a reload"
    );
}

/// A page that lives only in a WAL record must have its location written down where the index
/// keeps it, not only in a table this process happens to hold.
///
/// The address in the served index is synthetic -- a counter, not a position -- so on its own it
/// cannot be turned back into bytes. The resolver's table can, but it starts empty after a
/// restart, which is why the location has to travel with the index that depends on it.
#[test]
fn a_wal_resident_page_records_where_it_lives_in_the_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config {
            version: 2,
            async_storage: true,
            ..Config::default()
        },
    });
    std::env::set_var("TS_BLOCK_IN_WAL", "1");
    std::env::set_var("TS_HOT_PAGE_SPILL", "0");

    assert_eq!(
        engine.wal_resident_page_count(1),
        0,
        "nothing is in the log yet"
    );

    let value = b"only-durable-copy-is-the-record".to_vec();
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "logged".to_string(),
                    value: value.clone(),
                },
            })
            .status
            .ok
    );

    assert!(
        engine.wal_resident_page_count(1) > 0,
        "the index must carry where the page went"
    );

    // Take the shard down and back up: the resolver's table is dropped, so what comes back has
    // to come from the index.
    engine.cache().invalidate_shard(1).unwrap();
    engine.unload_shard(1);
    engine.load_shard(1);

    assert!(
        engine.wal_resident_page_count(1) > 0,
        "the location has to survive the reload, or it was never durable"
    );
    let read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "logged".to_string(),
        },
    });
    std::env::remove_var("TS_BLOCK_IN_WAL");
    std::env::remove_var("TS_HOT_PAGE_SPILL");
    assert_eq!(
        read.response,
        CommandResponse::Bytes { value: Some(value) },
        "an acked write must read back after the reload"
    );
}

/// With the feature off, nothing is recorded -- the index does not grow for stores that put no
/// pages in the log.
#[test]
fn a_store_that_puts_no_pages_in_the_log_carries_no_locations() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    std::env::set_var("TS_BLOCK_IN_WAL", "0");
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "plain".to_string(),
                    value: b"v".to_vec(),
                },
            })
            .status
            .ok
    );
    std::env::remove_var("TS_BLOCK_IN_WAL");
    assert_eq!(engine.wal_resident_page_count(1), 0);
}

/// A record can state what the write DID, and that statement has to match what the command
/// actually produced.
///
/// This is the obligation that has to be discharged before replay can install outcomes instead
/// of re-running commands. If an item disagreed with the index entry the command built, then
/// switching replay over would silently rebuild a different shard.
#[test]
fn a_recorded_outcome_matches_the_index_entry_the_command_produced() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    std::env::set_var("TS_WAL_OUTCOME_ITEMS", "1");

    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "outcome-key".to_string(),
                    value: b"outcome-value".to_vec(),
                },
            })
            .status
            .ok
    );
    std::env::remove_var("TS_WAL_OUTCOME_ITEMS");

    let records = engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .unwrap();
    let record = records
        .iter()
        .filter_map(|(_, line)| crate::wal::decode_wal_line(line).ok())
        .find(|record| !record.outcomes.is_empty())
        .expect("the record must state what the write did");

    let item = record
        .outcomes
        .iter()
        .find(|item| item.object_key == "outcome-key")
        .expect("the object it touched must be named");
    assert_eq!(item.kind, "string");
    assert!(!item.deleted);
    assert_eq!(
        item.object_id,
        item.address
            .as_ref()
            .and_then(|address| address.object_id)
            .unwrap_or_default()
    );

    // The claim has to equal what the index actually holds. This is the whole point.
    let indexed = engine
        .string_page_address(1, "outcome-key")
        .expect("the index holds an address for the key");
    assert_eq!(
        item.address.as_ref(),
        Some(&indexed),
        "the recorded outcome disagrees with the index entry the command built"
    );
}

/// With the gate off a record carries no outcomes, so it is byte-identical to before.
#[test]
fn a_record_carries_no_outcomes_unless_asked() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    std::env::set_var("TS_WAL_OUTCOME_ITEMS", "0");
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "plain".to_string(),
                    value: b"v".to_vec(),
                },
            })
            .status
            .ok
    );
    std::env::remove_var("TS_WAL_OUTCOME_ITEMS");

    let records = engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .unwrap();
    assert!(
        records
            .iter()
            .filter_map(|(_, line)| crate::wal::decode_wal_line(line).ok())
            .all(|record| record.outcomes.is_empty()),
        "no record should carry outcomes with the gate off"
    );
}

// ---------------------------------------------------------------------------------------
// Multiple engines in ONE process. The embedded path (the gateway proxy) does exactly this,
// and every embedded engine serves shard 1 -- so anything keyed on (shard, object) alone is
// shared between engines that have nothing to do with each other.
// ---------------------------------------------------------------------------------------

/// Two engines, same shard id, same object key: each must read back its OWN value.
///
/// The page resolver is a process-wide table keyed on (shard, object id), and a page's object
/// id is derived from kind + key -- not from which engine wrote it. Two embedded engines
/// therefore collide on every key they happen to share, and the only thing separating them is
/// that a registration also remembers WHICH log it points into.
#[test]
fn two_engines_in_one_process_do_not_read_each_others_pages() {
    let dir = tempfile::tempdir().unwrap();
    let make = |name: &str| {
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join(format!("{name}-cache")),
            dir.path().join(format!("{name}-pages")),
            dir.path().join(format!("{name}-index")),
        );
        engine.load_shard(1);
        engine.set_config(SetConfigRequest {
            shard_id: 1,
            config: Config {
                version: 2,
                async_storage: true,
                ..Config::default()
            },
        });
        engine
    };

    std::env::set_var("TS_BLOCK_IN_WAL", "1");
    std::env::set_var("TS_HOT_PAGE_SPILL", "0");

    let first = make("first");
    let second = make("second");

    // Same shard, same key, different values, different engines.
    for (engine, value) in [(&first, b"from-first".to_vec()), (&second, b"from-second".to_vec())] {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "shared-key".to_string(),
                        value,
                    },
                })
                .status
                .ok
        );
    }

    // Drop every cached copy so the read has to go through the resolver, which is the shared
    // thing. If it resolved by (shard, object) alone, one engine would serve the other's bytes.
    first.cache().invalidate_shard(1).unwrap();
    second.cache().invalidate_shard(1).unwrap();

    let read = |engine: &TemporalEngine| {
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "shared-key".to_string(),
                },
            })
            .response
    };
    let first_read = read(&first);
    let second_read = read(&second);
    std::env::remove_var("TS_BLOCK_IN_WAL");
    std::env::remove_var("TS_HOT_PAGE_SPILL");

    assert_eq!(
        first_read,
        CommandResponse::Bytes {
            value: Some(b"from-first".to_vec())
        },
        "the first engine read the wrong engine's page"
    );
    assert_eq!(
        second_read,
        CommandResponse::Bytes {
            value: Some(b"from-second".to_vec())
        },
        "the second engine read the wrong engine's page"
    );
}

/// One engine's log-resident pages must not pin another engine's reclaim floor.
///
/// The retention floor is the lowest sequence any live registration still depends on. Computed
/// across the whole process it would be pinned by every OTHER engine's writes forever, and a
/// shard that had dumped everything would never be able to reclaim.
#[test]
fn one_engines_retention_floor_ignores_another_engines_registrations() {
    let dir = tempfile::tempdir().unwrap();
    let make = |name: &str| {
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join(format!("{name}-cache")),
            dir.path().join(format!("{name}-pages")),
            dir.path().join(format!("{name}-index")),
        );
        engine.load_shard(1);
        engine.set_config(SetConfigRequest {
            shard_id: 1,
            config: Config {
                version: 2,
                async_storage: true,
                ..Config::default()
            },
        });
        engine
    };

    std::env::set_var("TS_BLOCK_IN_WAL", "1");
    let busy = make("busy");
    let quiet = make("quiet");

    // The busy engine registers pages; the quiet one writes nothing at all.
    for index in 0..8 {
        assert!(
            busy.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("busy-{index}"),
                    value: vec![b'v'; 64],
                },
            })
            .status
            .ok
        );
    }
    std::env::remove_var("TS_BLOCK_IN_WAL");

    // The quiet engine's log has nothing registered against it, so nothing holds its floor.
    assert_eq!(
        quiet.write_ahead_log_store().block_retention_floor(1),
        None,
        "another engine's registrations pinned this engine's reclaim floor"
    );
}

/// Every command that changes stored state has to record what it did, or replay cannot be
/// switched from re-running commands to installing outcomes without silently losing that state.
///
/// This is a COVERAGE probe, not a spot check: it drives a spread of write commands and reports
/// every one whose record carries no outcome, so the gap list comes from the engine rather than
/// from reading call sites and hoping the list is complete.
#[test]
fn every_mutating_command_records_what_it_did() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    std::env::set_var("TS_WAL_OUTCOME_ITEMS", "1");

    let commands = vec![
        Command::StringSet {
            key: "probe-string".to_string(),
            value: b"v".to_vec(),
        },
        Command::StringSetEx {
            key: "probe-setex".to_string(),
            value: b"v".to_vec(),
            ttl_ms: 60_000,
        },
        Command::HashSet {
            key: "probe-hash".to_string(),
            field: "f".to_string(),
            value: b"v".to_vec(),
        },
        Command::HashIncrBy {
            key: "probe-hash".to_string(),
            field: "counter".to_string(),
            increment: 3,
        },
        Command::SetAdd {
            key: "probe-set".to_string(),
            member: b"m".to_vec(),
        },
        Command::ZSetAdd {
            key: "probe-zset".to_string(),
            member: b"m".to_vec(),
            score: 1.5,
        },
        Command::ListPush {
            key: "probe-list".to_string(),
            member: b"m".to_vec(),
            left: true,
        },
        Command::SeenCheck {
            key: "probe-seen".to_string(),
            member: b"m".to_vec(),
            window_ms: 60_000,
        },
        Command::BucketTake {
            key: "probe-bucket".to_string(),
            tokens: 1.0,
            capacity: 10.0,
            refill_per_sec: 1.0,
        },
        Command::ControlStateIncrement {
            key: "probe-control".to_string(),
            timestamp_ms: 1_787_270_070_000,
            amount: 2,
        },
        Command::CommonExpire {
            key: "probe-string".to_string(),
            ttl_ms: 30_000,
        },
        // Removals go through mark_bucket_index_page_deleted, not the upsert -- a different
        // path, so a different chance to record nothing.
        Command::HashDelete {
            key: "probe-hash".to_string(),
            field: "f".to_string(),
        },
        Command::SetRemove {
            key: "probe-set".to_string(),
            member: b"m".to_vec(),
        },
        Command::ZSetRemove {
            key: "probe-zset".to_string(),
            member: b"m".to_vec(),
        },
        Command::ListPop {
            key: "probe-list".to_string(),
            left: true,
        },
        Command::StringDelete {
            key: "probe-setex".to_string(),
        },
        // Feature series have their own map again.
        Command::FeatureAppend {
            key: "probe-feature".to_string(),
            points: vec![crate::types::FeaturePoint {
                timestamp_ms: 1_787_270_070_000,
                value: b"fv".to_vec(),
            }],
        },
        Command::CommonDelete {
            key: "probe-string".to_string(),
        },
    ];

    let mut attempted = Vec::new();
    for command in commands {
        let label = format!("{command:?}");
        let label = label
            .split_once(' ')
            .map(|(head, _)| head.to_string())
            .unwrap_or(label);
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        // Only commands the engine ACCEPTED are evidence about outcome coverage.
        if response.status.ok {
            attempted.push(label);
        }
    }
    std::env::remove_var("TS_WAL_OUTCOME_ITEMS");

    // Which of the accepted writes left a record that says nothing about what changed?
    let records = engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .unwrap();
    let mut silent = Vec::new();
    for (_, line) in records {
        let Ok(record) = crate::wal::decode_wal_line(&line) else {
            continue;
        };
        if record.outcomes.is_empty() {
            let label = format!("{:?}", record.command);
            let label = label
                .split_once(' ')
                .map(|(head, _)| head.to_string())
                .unwrap_or(label);
            silent.push(label);
        }
    }
    silent.sort();
    silent.dedup();

    assert!(
        silent.is_empty(),
        "these accepted writes recorded nothing about what they changed, so replay could not \
         install them: {silent:?} (attempted: {attempted:?})"
    );
}

/// THE GATE. A shard rebuilt by installing recorded outcomes must equal one built by running
/// the commands.
///
/// Until this holds, replay cannot be switched over: an apply that drops or garbles a kind
/// produces a shard that is subtly wrong rather than one that fails, and nothing downstream
/// would notice. Kinds are brought over one at a time and this test grows with them.
#[test]
fn a_shard_rebuilt_from_outcomes_equals_one_rebuilt_from_commands() {
    let dir = tempfile::tempdir().unwrap();
    let ran = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("ran-cache"),
        dir.path().join("ran-pages"),
        dir.path().join("ran-index"),
    );
    ran.load_shard(1);
    std::env::set_var("TS_WAL_OUTCOME_ITEMS", "1");

    // A bound tight enough that the workload below overflows it. Only this engine is told --
    // which is the whole point: the shard rebuilt from records has to arrive at the same three
    // points without ever learning what the bound was.
    assert!(
        ran.set_config(SetConfigRequest {
            shard_id: 1,
            config: Config {
                version: 2,
                feature_max_size: 3,
                ..Config::default()
            },
        })
        .ok
    );

    // The kinds the apply path claims to handle so far.
    let workload = vec![
        Command::StringSet {
            key: "eq-a".to_string(),
            value: b"first".to_vec(),
        },
        Command::StringSet {
            key: "eq-b".to_string(),
            value: b"second".to_vec(),
        },
        Command::StringSet {
            key: "eq-a".to_string(),
            value: b"first-overwritten".to_vec(),
        },
        Command::SeenCheck {
            key: "eq-seen".to_string(),
            member: b"m1".to_vec(),
            window_ms: 60_000,
        },
        Command::BucketTake {
            key: "eq-bucket".to_string(),
            tokens: 2.0,
            capacity: 10.0,
            refill_per_sec: 1.0,
        },
        Command::HashSet {
            key: "eq-hash".to_string(),
            field: "f1".to_string(),
            value: b"hv".to_vec(),
        },
        Command::HashSet {
            key: "eq-hash".to_string(),
            field: "f2".to_string(),
            value: b"hv2".to_vec(),
        },
        Command::SetAdd {
            key: "eq-set".to_string(),
            member: b"member-one".to_vec(),
        },
        Command::ZSetAdd {
            key: "eq-zset".to_string(),
            member: b"zm".to_vec(),
            score: 2.5,
        },
        Command::ListPush {
            key: "eq-list".to_string(),
            member: b"lm".to_vec(),
            left: true,
        },
        Command::ListPush {
            key: "eq-list".to_string(),
            member: b"lm2".to_vec(),
            left: false,
        },
        Command::CommonExpire {
            key: "eq-b".to_string(),
            ttl_ms: 60_000,
        },
        Command::StringSet {
            key: "eq-doomed".to_string(),
            value: b"gone".to_vec(),
        },
        Command::CommonDelete {
            key: "eq-doomed".to_string(),
        },
        // Five points into a series bounded at three. The two oldest are dropped by config the
        // command never mentions -- so unless the trim itself is recorded, the rebuilt shard
        // keeps points this one has thrown away.
        Command::FeatureAppend {
            key: "eq-feature".to_string(),
            points: (0..5)
                .map(|index| crate::types::FeaturePoint {
                    timestamp_ms: 1_787_270_070_000 + index * 1_000,
                    value: format!("point-{index}").into_bytes(),
                })
                .collect(),
        },
        // A replace drops a range and writes over it: removals and inserts in one command.
        Command::FeatureReplace {
            key: "eq-feature".to_string(),
            start_ms: 1_787_270_073_000,
            end_ms: 1_787_270_074_000,
            points: vec![crate::types::FeaturePoint {
                timestamp_ms: 1_787_270_073_500,
                value: b"replacement".to_vec(),
            }],
        },
    ];
    for command in workload {
        let response = ran.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "workload write failed: {response:?}");
    }
    // If this stops being true the trim is no longer under test and the gate has gone quiet.
    // Five points, bounded to the newest three, then a replace drops two of those and writes one
    // back: the oldest two must be gone by the trim, and the series must be down to two.
    let ran_feature = ran
        .index_shape_for_test(1)
        .lines()
        .filter(|line| line.starts_with("feature eq-feature "))
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        ran_feature.len(),
        2,
        "the workload was supposed to leave a trimmed series, got {ran_feature:?}"
    );
    assert!(
        !ran_feature
            .iter()
            .any(|line| line.contains("at=1787270070000") || line.contains("at=1787270071000")),
        "the two oldest points should have been trimmed, got {ran_feature:?}"
    );

    // Take the outcomes off the log, in the order they were written.
    let records = ran
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .unwrap();
    let mut outcomes = Vec::new();
    for (_, line) in records {
        let Ok(record) = crate::wal::decode_wal_line(&line) else {
            continue;
        };
        outcomes.push((record.sequence, record.outcomes));
    }
    outcomes.sort_by_key(|(sequence, _)| *sequence);
    std::env::remove_var("TS_WAL_OUTCOME_ITEMS");
    assert!(
        outcomes.iter().any(|(_, items)| !items.is_empty()),
        "the workload recorded no outcomes at all"
    );

    // A second shard that never sees a command -- only what the first one did.
    let installed = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("inst-cache"),
        dir.path().join("inst-pages"),
        dir.path().join("inst-index"),
    );
    installed.load_shard(1);
    let mut refused = Vec::new();
    for (sequence, items) in &outcomes {
        for item in items {
            if !installed.apply_outcome_item(1, item) {
                refused.push(format!("seq={sequence} kind={}", item.kind));
            }
        }
    }
    assert!(
        refused.is_empty(),
        "the apply path refused outcomes it should understand: {refused:?}"
    );

    assert_eq!(
        installed.index_shape_for_test(1),
        ran.index_shape_for_test(1),
        "installing the outcomes did not reproduce the shard the commands built"
    );
}

/// What waiting before a dump saves, and what the wait costs.
///
/// A bucket is dumped, dirtied again by the very next write, and dumped again. Letting the log
/// accumulate first lets those writes merge into one dump. The knob for that exists and is off by
/// default, so today every cycle dumps every dirty bucket.
///
/// Off is a defensible choice -- the dumps saved are paid for with a longer log to replay after a
/// restart -- so this reports both sides rather than asserting one is right. The dumps are taken,
/// not merely planned: planning alone never advances the dumped watermark, and then the backlog
/// never falls and the counts mean nothing.
#[test]
fn what_delaying_a_dump_saves_and_costs() {
    for threshold in [0u64, 10, 50] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1 << 20,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);

        let writes = 100u64;
        let mut dumps = 0u64;
        let mut worst_backlog = 0u64;
        for index in 0..writes {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    // One key, written over and over: the case where merging matters.
                    key: "hot".to_string(),
                    value: format!("v{index}").into_bytes(),
                },
            });
            let plan = engine.storage_lifecycle_plan(StorageLifecycleRequest {
                shard_id: 1,
                min_undumped_wal_records: threshold,
                max_dump_buckets_per_round: 16,
                ..Default::default()
            });
            worst_backlog = worst_backlog.max(plan.undumped_wal_records);
            if !plan.selected_dump_buckets.is_empty() {
                // Take the dump, so the watermark moves and the next backlog is real.
                if engine
                    .create_bucket_dump_manifest(1, plan.selected_dump_buckets.clone())
                    .is_ok()
                {
                    dumps += 1;
                }
            }
        }
        println!(
            "  threshold {threshold:>3} records: {dumps:>4} dumps for {writes} writes to one bucket, \
             worst replay backlog {worst_backlog} records"
        );
    }
}

/// One expiry round, as the number of keys carrying a deadline grows.
///
/// A round only ever acts on a bounded window. Choosing that window copies and sorts every key
/// that has a deadline, so the cost tracks the whole set instead of the window -- and it is paid on
/// every round, forever.
#[test]
fn what_one_expiry_round_costs_as_deadlines_accumulate() {
    for keys in [1_000usize, 5_000, 20_000] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1 << 24,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..keys {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSetEx {
                    key: format!("k{index:08}"),
                    value: b"v".to_vec(),
                    // Far enough out that nothing expires during the measurement.
                    ttl_ms: 3_600_000,
                },
            });
        }

        let rounds = 20;
        let started = std::time::Instant::now();
        for _ in 0..rounds {
            let report = engine.sweep_expired_records_with_request(ShardExpirySweepRequest {
                shard_id: 1,
                // A small window, which is the point: the round should cost the window.
                max_hot_buckets_per_round: 16,
                max_cold_buckets_per_round: 16,
                ..Default::default()
            });
            assert!(report.is_ok(), "the sweep should run");
        }
        let per_round = started.elapsed().as_secs_f64() * 1e6 / rounds as f64;
        println!(
            "  {keys:>6} keys with a deadline: {per_round:>9.0} us per round \
             ({:.2} us per 1k keys) for a window of 16",
            per_round / (keys as f64 / 1000.0)
        );
    }
}

/// Paging by cursor reaches every deadline exactly once.
///
/// The window is read from an ordered set, so paging only works if the cursor advances past
/// everything examined. If it advanced only past what was taken, a run of keys the round rejected
/// would be re-examined forever and the sweep would never reach what lies beyond them.
#[test]
fn paging_the_expiry_window_reaches_every_deadline_once() {
    use std::collections::BTreeMap;

    let mut deadlines: BTreeMap<String, u64> = BTreeMap::new();
    for index in 0..250u64 {
        deadlines.insert(format!("key-{index:04}"), index);
    }

    for window in [1usize, 7, 64, 250, 400] {
        let mut seen: Vec<String> = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..2_000 {
            let (selected, next) = crate::engine::expiry_window(
                &deadlines,
                cursor.as_deref(),
                window,
                window.saturating_mul(8).max(64),
                |_| true,
            );
            assert!(
                selected.len() <= window,
                "a window of {window} returned {} keys",
                selected.len()
            );
            let mut sorted = selected.clone();
            sorted.sort_by(|left, right| left.0.cmp(&right.0));
            assert_eq!(selected, sorted, "the window should come back in key order");
            seen.extend(selected.iter().map(|(key, _)| key.clone()));
            match next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        let mut unique = seen.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), seen.len(), "window {window} repeated a key");
        assert_eq!(
            unique.len(),
            deadlines.len(),
            "window {window} never reached every deadline"
        );
    }
}

/// A long run of keys the round rejects does not stall the sweep.
///
/// This is the case the scan budget exists for: looking for the few keys in one category when the
/// set is almost entirely the other. The round stops early, but the cursor has moved, so the next
/// one resumes past what was examined and the sweep still finishes.
#[test]
fn a_run_of_rejected_keys_does_not_stall_the_sweep() {
    use std::collections::BTreeMap;

    let mut deadlines: BTreeMap<String, u64> = BTreeMap::new();
    for index in 0..1_000u64 {
        deadlines.insert(format!("key-{index:04}"), index);
    }
    // Only the last handful are wanted; everything before them is examined and rejected.
    let wanted = |key: &str| key >= "key-0990";

    let mut found: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut rounds = 0;
    loop {
        let (selected, next) =
            crate::engine::expiry_window(&deadlines, cursor.as_deref(), 8, 64, wanted);
        found.extend(selected.iter().map(|(key, _)| key.clone()));
        rounds += 1;
        match next {
            Some(next) => cursor = Some(next),
            None => break,
        }
        assert!(rounds < 200, "the sweep is not making progress");
    }
    assert_eq!(found.len(), 10, "every wanted key should be reached");
    assert!(
        rounds > 1,
        "this should take several bounded rounds, or the budget is not bounding anything"
    );
}

/// No limit means every match after the cursor, and nothing to resume from.
#[test]
fn an_unlimited_expiry_window_returns_every_match() {
    use std::collections::BTreeMap;

    let mut deadlines: BTreeMap<String, u64> = BTreeMap::new();
    for index in 0..40u64 {
        deadlines.insert(format!("key-{index:04}"), index);
    }
    let (selected, next) = crate::engine::expiry_window(&deadlines, None, 0, 0, |_| true);
    assert_eq!(selected.len(), 40);
    assert!(next.is_none(), "there is nothing left to resume from");

    let (after, _) =
        crate::engine::expiry_window(&deadlines, Some("key-0019"), 0, 0, |_| true);
    assert_eq!(after.len(), 20, "the cursor is exclusive");
    assert_eq!(after.first().map(|(key, _)| key.as_str()), Some("key-0020"));
}


/// A cycle request that does not mention the ordering guard must still get it.
///
/// Index-log records may only be discarded once the buckets they describe have been dumped;
/// truncating first throws away the record of state that is not durable anywhere else. The guard
/// that enforces that is a field on the request, and the request is parsed from an HTTP body --
/// so what an OMITTED field decodes to is the behaviour every caller gets who does not name it.
/// `#[serde(default)]` on a bool decodes to `false`, which is the unsafe order, even though the
/// type's own `Default` says true.
#[test]
fn omitting_the_commit_before_truncate_guard_still_commits_before_truncating() {
    // Take a well-formed body and remove ONLY the guard, so this tests what silence means rather
    // than whether every other field happens to be optional.
    let mut body: serde_json::Value =
        serde_json::to_value(StorageManagerCycleRequest::default()).unwrap();
    let removed = body
        .as_object_mut()
        .unwrap()
        .remove("index_gc_commit_dirty_slots_before_truncation");
    assert!(removed.is_some(), "the guard should be present in a serialised request");

    let silent: StorageManagerCycleRequest = serde_json::from_value(body).unwrap();
    assert!(
        silent.index_gc_commit_dirty_buckets_before_truncation,
        "a request that does not mention the guard decoded to the unsafe order: index-log records \
         would be discarded before the buckets they describe had been dumped"
    );

    // Naming it false is still allowed -- this is about what silence means, not about removing
    // the choice.
    let mut off: serde_json::Value =
        serde_json::to_value(StorageManagerCycleRequest::default()).unwrap();
    off.as_object_mut().unwrap().insert(
        "index_gc_commit_dirty_slots_before_truncation".to_string(),
        serde_json::Value::Bool(false),
    );
    let explicit: StorageManagerCycleRequest = serde_json::from_value(off).unwrap();
    assert!(!explicit.index_gc_commit_dirty_buckets_before_truncation);
}

/// Every stage records how long it took, instead of every stage reporting zero.
///
/// `duration_ms` is published as `temporalstore_storage_manager_phase_duration_ms`, and nothing
/// ever set it -- so the metric read zero for every phase of every shard, always. A missing series
/// is obviously missing; a series that reads zero says the cycle is instantaneous, which is a claim
/// and a false one.
///
/// Asserted as a property rather than a threshold, because a fast stage legitimately rounds to
/// zero milliseconds and a threshold would be flaky: the stages must TILE the cycle -- their total
/// cannot exceed the wall time of the call that produced them.
#[test]
fn the_storage_cycle_records_how_long_its_stages_take() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    // Enough to make the cycle do real work, and no more: an earlier version used 4000 records
    // and took ten minutes on a shared machine, which is not a cost a suite should carry.
    for index in 0..400 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("k{index:06}"),
                value: vec![118u8; 128],
            },
        });
    }

    let started = std::time::Instant::now();
    let cycle = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: 1,
        ..Default::default()
    });
    let wall_ms = started.elapsed().as_millis() as u64;

    assert!(!cycle.stages.is_empty(), "the cycle should have run stages");
    let total: u64 = cycle.stages.iter().map(|stage| stage.duration_ms).sum();
    assert!(
        total <= wall_ms + 50,
        "stage durations total {total} ms but the whole call took {wall_ms} ms -- they should tile          the cycle, not overlap it"
    );
    assert!(
        cycle.duration_ms >= total,
        "the round total {} ms should cover the stages that tile it ({total} ms)",
        cycle.duration_ms
    );
    // The tiling assertion above is the one that matters, and it is what caught the first
    // implementation: a closure capturing the clock by value restarted a COPY, so every stage
    // reported the time since the cycle began and the total came to eight times the wall clock.
    // Deliberately NOT asserting some stage exceeds zero -- a quick cycle rounds every stage to
    // zero milliseconds, and that assertion would fail on a fast machine for no reason.
}
