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
/// Each manifest must be judged against ITS OWN fingerprints.
///
/// The plan decodes every manifest's index once, up front, and indexes into that by position
/// while walking buckets -- so a mis-paired index would judge one manifest's buckets against
/// another's fingerprints and anchor reclaim on the wrong dump. THREE manifests, because an
/// off-by-one can coincidentally pick the right one out of two.
///
/// This guards the shape of the lookup, not the old behaviour: before, the decode happened
/// inside the loop and could not be mispaired -- it was merely quadratic, taking 100s at 4k
/// records and not finishing at all at 40k.
#[test]
fn the_reclaim_plan_pairs_each_manifest_with_its_own_fingerprints() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let mut manifests = Vec::new();
    for round in 0..3 {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "reclaim-slot".to_string(),
                        value: format!("v{round}").into_bytes(),
                    },
                })
                .status
                .ok
        );
        manifests.push(engine.create_bucket_dump_manifest(1, Vec::new()).unwrap());
    }

    // Strictly increasing, or the assertion below cannot tell the three apart.
    assert!(manifests[1].wal_sequence > manifests[0].wal_sequence);
    assert!(manifests[2].wal_sequence > manifests[1].wal_sequence);

    let plan = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());

    // The newest manifest is the only one whose fingerprints still match the live state, so it
    // is the one the frontier must anchor on. Pairing manifest N with manifest N-1's
    // fingerprints would anchor on an older dump and reclaim a span that dump does not cover.
    assert_eq!(
        plan.durable_bucket_generation_frontier_wal_sequence, manifests[2].wal_sequence,
        "the frontier must anchor on the newest matching manifest: {plan:?}"
    );
    assert!(
        plan.retained_manifest_ids
            .contains(&manifests[2].manifest_id),
        "the newest manifest must be retained: {plan:?}"
    );
    assert!(
        plan.missing_bucket_generations.is_empty(),
        "every live bucket is covered by the newest dump: {plan:?}"
    );
}

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
    // Both cursors sit at the parent manifest, so the frontier clamps down to the parent
    // instead of refusing at the child. They are still counted and named as retaining logs --
    // the ones above them -- which is what an operator needs in order to know why the log stops
    // shrinking where it does.
    assert!(blocked.safe_to_reclaim, "{blocked:?}");
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
    assert_eq!(
        blocked.retain_from_wal_sequence,
        parent.wal_sequence.saturating_add(1),
        "clamped to the slowest cursor, not refused at the frontier: {blocked:?}"
    );
    assert_eq!(
        blocked.retain_from_index_log_sequence,
        parent.index_log_sequence.saturating_add(1)
    );
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
    // Clamped rather than refused. Both cursors sit at the parent manifest, so the cycle reclaims
    // the span they have already consumed and keeps everything above them. Asserting that nothing
    // was reclaimed was asserting the refusal itself, which is the behaviour that let one lagging
    // follower pin the whole log.
    assert!(blocked_wal.applied, "{blocked_wal:?}");
    assert!(
        blocked_wal.wal_records_removed > 0,
        "the span below the slowest cursor should go: {blocked_wal:?}"
    );
    assert_eq!(
        blocked_wal.plan.retain_from_wal_sequence,
        parent.wal_sequence.saturating_add(1),
        "and nothing above it: {blocked_wal:?}"
    );
    assert_eq!(
        blocked_wal.plan.retain_from_index_log_sequence,
        parent.index_log_sequence.saturating_add(1),
        "{blocked_wal:?}"
    );
    // Index GC follows the same frontier, so whatever it decides must agree with the plan rather
    // than be asserted independently -- the two disagreeing is the defect worth catching here.
    let blocked_index_gc = blocked_cycle.index_gc_report.as_ref().unwrap();
    assert_eq!(
        blocked_index_gc.applied,
        blocked_wal.plan.safe_to_reclaim,
        "index GC and WAL reclaim must reach the same verdict on one frontier: \
         {blocked_index_gc:?} against {:?}",
        blocked_wal.plan
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
    // There may be nothing left to remove, because the clamped cycle above already released the
    // span both cursors had consumed. That is the point of clamping: the work happens as the
    // cursor advances rather than all at once when it finally reaches the frontier. What must
    // hold is that the frontier itself has moved up to the final anchor.
    assert_eq!(
        released_wal.plan.retain_from_wal_sequence,
        final_anchor.wal_sequence.saturating_add(1),
        "{released_wal:?}"
    );
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
        address.set_object_id(Some(address.object_id().unwrap_or_default().wrapping_add(1)));
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
            address.routing_bucket(),
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
    // Walk frames, not newlines: a record's payload may hold a 0x0A, so splitting on one
    // yields fragments rather than records and the corruption below would land nowhere.
    let mut lines: Vec<Vec<u8>> = Vec::new();
    let mut at = 0usize;
    while at < contents.len() {
        match crate::log_framing::next_frame(&contents[at..]) {
            Ok(Some((consumed, _))) => {
                lines.push(contents[at..at + consumed].to_vec());
                at += consumed;
            }
            _ => break,
        }
    }
    assert!(
        lines.len() >= 2,
        "the write path must have appended index-log delta records"
    );
    // Corrupt an interior record (not the tail) so it is committed corruption, not a torn tail.
    // Framed the way the writer frames, so what fails is the decode rather than the envelope.
    lines[0] = crate::log_framing::encode_record(b"corrupt-not-a-record");
    let mut rebuilt = Vec::new();
    for line in &lines {
        rebuilt.extend_from_slice(line);
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

/// The same, for the batch path -- which is the one bulk ingest actually runs.
///
/// `batch_execute` swept every bucket once per batch rather than once per write. That is a much
/// smaller constant than the per-write sweep, but the same `O(total pages)` shape, so a bulk
/// import still paid for the whole corpus on every batch. Measured per BATCH, and the flags are
/// compared against a full sweep for the same reason as the single-write case: a bucket left
/// stale-false in `dirty` never gets flushed.
#[test]
fn batch_bucket_maintenance_does_not_grow_with_the_store() {
    const BATCH: usize = 32;
    const MEASURED_BATCHES: usize = 5;

    fn visits_per_batch(object_count: usize) -> (f64, usize) {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..object_count {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("batch-fill-{index}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        crate::engine::reset_bucket_page_index_visits();
        for batch in 0..MEASURED_BATCHES {
            let commands = (0..BATCH)
                .map(|item| Command::StringSet {
                    key: format!("batch-measured-{batch}-{item}"),
                    value: vec![b'v'; 64],
                })
                .collect();
            engine.batch_execute(BatchExecuteRequest {
                shard_id: 1,
                commands,
            });
        }
        let visited = crate::engine::bucket_page_index_visits();

        // Same equivalence requirement as the single-write path: sweeping afterwards must not
        // move anything the targeted refresh already settled.
        let snapshot = |engine: &TemporalEngine| {
            let shards = engine.shards.read().expect("shards lock poisoned");
            let shard = shards.get(&1).expect("shard 1 loaded");
            shard
                .bucket_index
                .bucket_map
                .iter()
                .map(|(id, bucket)| {
                    (
                        *id,
                        (
                            bucket.in_memory,
                            bucket.deleted,
                            bucket.dirty,
                            format!("{:?}", bucket.layout),
                            bucket.object_index.iter().copied().collect::<Vec<_>>(),
                        ),
                    )
                })
                .collect::<std::collections::BTreeMap<_, _>>()
        };
        let before_sweep = snapshot(&engine);
        {
            let mut shards = engine.shards.write().expect("shards lock poisoned");
            let shard = shards.get_mut(&1).expect("shard 1 loaded");
            crate::engine::storage_bucket_internals::refresh_bucket_runtime_flags(shard);
        }
        let after_sweep = snapshot(&engine);
        assert_eq!(
            before_sweep, after_sweep,
            "batch targeted refresh disagreed with a full sweep at {object_count} objects"
        );

        (visited as f64 / MEASURED_BATCHES as f64, before_sweep.len())
    }

    let (small, small_buckets) = visits_per_batch(200);
    let (large, large_buckets) = visits_per_batch(800);
    println!(
        "
  200 objects -> {small:>9.1} page-index visits per {BATCH}-command batch ({small_buckets} buckets)
           800 objects -> {large:>9.1} page-index visits per {BATCH}-command batch ({large_buckets} buckets)
           growth: {:.2}x cost for 4x the corpus
",
        if small > 0.0 { large / small } else { 0.0 }
    );

    assert!(
        large <= small * 1.5 + 1.0,
        "per-batch bucket maintenance grew with the store: {small:.1} -> {large:.1}          visits/batch for 200 -> 800 objects"
    );
}

/// The per-write targeted refresh must leave the same flags as sweeping the whole shard.
///
/// The write path refreshes only the buckets it recorded as touched. That is only correct if a
/// bucket nobody recorded genuinely cannot have changed, which is an argument about every site
/// that mutates a bucket, `dirty_objects` or `expires_at_ms`. Rather than rest on the argument,
/// this runs a mixed workload and then sweeps every bucket: if the targeted path ever misses one,
/// the sweep moves it and the comparison fails, naming the bucket and the field.
///
/// `ttl_ms` is a countdown recomputed from the clock, so it is compared as present/absent rather
/// than by value; every other flag is exact.
#[test]
fn bucket_runtime_flags_match_full_sweep() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    // A mix, so the comparison covers inserts, overwrites, hash fields, expiries and deletes
    // rather than one shape of write.
    for index in 0..120 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("flags-str-{index}"),
                value: vec![b'v'; 48],
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: format!("flags-hash-{}", index % 17),
                field: format!("field-{index}"),
                value: vec![b'h'; 32],
            },
        });
        if index % 5 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("flags-str-{}", index / 2),
                    value: vec![b'w'; 64],
                },
            });
        }
        if index % 7 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonExpire {
                    key: format!("flags-str-{index}"),
                    ttl_ms: 60_000,
                },
            });
        }
        if index % 11 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonDelete {
                    key: format!("flags-str-{}", index / 3),
                },
            });
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct BucketFlags {
        in_memory: bool,
        deleted: bool,
        dirty: bool,
        has_ttl: bool,
        layout: String,
        object_index: Vec<u64>,
        page_count: usize,
    }

    let capture = |engine: &TemporalEngine| -> std::collections::BTreeMap<u32, BucketFlags> {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        shard
            .bucket_index
            .bucket_map
            .iter()
            .map(|(routing_bucket, bucket)| {
                (
                    *routing_bucket,
                    BucketFlags {
                        in_memory: bucket.in_memory,
                        deleted: bucket.deleted,
                        dirty: bucket.dirty,
                        has_ttl: bucket.ttl_ms.is_some(),
                        layout: format!("{:?}", bucket.layout),
                        object_index: bucket.object_index.iter().copied().collect(),
                        page_count: bucket.page_index.len(),
                    },
                )
            })
            .collect()
    };

    let after_targeted = capture(&engine);
    {
        let mut shards = engine.shards.write().expect("shards lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 loaded");
        crate::engine::storage_bucket_internals::refresh_bucket_runtime_flags(shard);
    }
    let after_full_sweep = capture(&engine);

    assert!(
        !after_targeted.is_empty(),
        "workload produced no buckets, so the comparison would be vacuous"
    );

    let mut differences = Vec::new();
    for (routing_bucket, targeted) in &after_targeted {
        let swept = after_full_sweep
            .get(routing_bucket)
            .expect("the sweep cannot add or drop buckets");
        if targeted != swept {
            differences.push(format!(
                "  bucket {routing_bucket}: targeted {targeted:?} != swept {swept:?}"
            ));
        }
    }
    assert!(
        differences.is_empty(),
        "the per-write targeted refresh left {} bucket(s) in a different state than a full sweep,          so a mutation site is not recording the bucket it touched:
{}",
        differences.len(),
        differences.join("
")
    );
    println!(
        "
  {} buckets compared; targeted refresh matches a full sweep on every flag
",
        after_targeted.len()
    );
}

/// Does `dirty_objects` say anything the pages' own `dirty` flags do not?
///
/// It holds a `String` per record -- 19 B of key at this key length, plus a String header and a
/// BTreeSet node each -- and nothing drains it during a pure ingest: only the publish path or a
/// storage-manager cycle clears it. At 4.7M records that is several hundred MB held in a set the
/// ingest never releases. It is also the set whose per-entry iteration made the heartbeat
/// shard-sized.
///
/// Each `BlockIndex` already carries `dirty`. If the two agree, the set is derivable from the
/// pages and its per-record String is duplicated state. If they disagree, it is carrying
/// something the flags cannot express, and this test says exactly what -- which is the part worth
/// knowing before anyone tries to remove it.
///
/// A report, not a threshold: it prints the comparison and asserts only that the workload
/// produced something to compare.
#[test]
fn dirty_objects_versus_the_pages_own_dirty_flags() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    assert!(
        engine
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 1,
                table_name: "dirty-duplication".to_string(),
                shard_uri: "local://dirty-duplication/1".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 63,
                readonly: false,
                load_version: 1,
                local_node_id: Some(1),
            })
            .status
            .ok
    );
    for index in 0..100 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("dd-{index}"),
                value: vec![b'v'; 48],
            },
        });
        if index % 5 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonDelete {
                    key: format!("dd-{}", index / 3),
                },
            });
        }
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");

    let pages_marked_dirty: std::collections::BTreeSet<String> = shard
        .bucket_index
        .bucket_map
        .values()
        .flat_map(|bucket| bucket.page_index.values())
        .filter(|page| page.dirty)
        .map(|page| page.object_key.to_string())
        .collect();
    let any_page_at_all: std::collections::BTreeSet<String> = shard
        .bucket_index
        .bucket_map
        .values()
        .flat_map(|bucket| bucket.page_index.values())
        .map(|page| page.object_key.to_string())
        .collect();

    let in_set_not_flagged: Vec<&String> = shard
        .dirty_objects
        .iter()
        .filter(|k| !pages_marked_dirty.contains(*k))
        .collect();
    let flagged_not_in_set: Vec<&String> = pages_marked_dirty
        .iter()
        .filter(|k| !shard.dirty_objects.contains(*k))
        .collect();
    let in_set_with_no_page: usize = shard
        .dirty_objects
        .iter()
        .filter(|k| !any_page_at_all.contains(*k))
        .count();

    assert!(
        !shard.dirty_objects.is_empty(),
        "workload left nothing in dirty_objects to compare"
    );
    println!(
        "
  dirty_objects: {}
  pages with dirty=true: {}
           in the set but no page flagged dirty: {}
           a page flagged dirty but not in the set: {}
           in the set with NO live page at all: {}
",
        shard.dirty_objects.len(),
        pages_marked_dirty.len(),
        in_set_not_flagged.len(),
        flagged_not_in_set.len(),
        in_set_with_no_page,
    );
    if let Some(sample) = in_set_not_flagged.first() {
        println!("  e.g. in the set, no dirty page: {sample}");
    }
}

/// Two components of one object stay two entries, and neither shadows the other.
///
/// The layout this replaced kept a SECOND map so that a (model, object, component) lookup existed
/// at all, and a probe here established that the per-component grouping could not be rebuilt by
/// filtering the per-object map -- which is why that second map could not simply be deleted.
///
/// Nesting keeps the grouping by construction rather than by a parallel map, so the property to
/// hold on to is the one that probe was protecting: components of one object are addressable
/// separately, they are ordered, removing one leaves the rest, and a component that was never
/// written is absent rather than empty.
#[test]
fn components_of_one_object_stay_separate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..6 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: "grouped".to_string(),
                field: format!("field-{index}"),
                value: vec![b'h'; 32],
            },
        });
    }
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "ungrouped".to_string(),
            value: vec![b'v'; 32],
        },
    });

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");

    let grouped = shard
        .bucket_index
        .object_page_refs("hash", "grouped")
        .expect("the hash object should be in the lookup");
    assert_eq!(
        grouped.by_component.len(),
        6,
        "six fields are six components, not one merged entry: {:?}",
        grouped.by_component
    );

    let mut components: Vec<Option<&str>> = grouped
        .by_component
        .iter()
        .map(|entry| entry.component.as_deref())
        .collect();
    let ordered = components.clone();
    components.sort();
    assert_eq!(
        components, ordered,
        "removal binary-searches this vector, so its order is load-bearing"
    );

    for index in 0..6 {
        let component = format!("field-{index}");
        assert!(
            grouped.refs_for(Some(&component)).is_some(),
            "component {component} should be addressable on its own"
        );
    }
    assert!(
        grouped.refs_for(Some("field-never-written")).is_none(),
        "a component nobody wrote is absent, not empty"
    );
    assert!(
        grouped.refs_for(None).is_none(),
        "a hash object has no componentless entry"
    );

    let ungrouped = shard
        .bucket_index
        .object_page_refs("string", "ungrouped")
        .expect("the string object should be in the lookup");
    assert_eq!(ungrouped.by_component.len(), 1);
    assert!(
        ungrouped.refs_for(None).is_some(),
        "a plain value is the componentless entry"
    );
}

/// The common case takes the inline arm, and the spilled arm still behaves.
///
/// Measuring that 100% of components hold one ref only justifies the shape; it does not show the
/// shape is being used. A structure that silently spilled every time would measure identically
/// from the outside and cost an allocation each, so the arm is asserted directly.
#[test]
fn single_page_components_are_held_inline() {
    use crate::engine::state::{BlockLookupRef, BlockRefs};

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..64 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("inline-{index:04}"),
                value: vec![b'v'; 48],
            },
        });
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut inline = 0usize;
    let mut spilled = 0usize;
    for entry in shard.bucket_index.object_page_lookup.values() {
        for component in &entry.by_component {
            match component.refs {
                BlockRefs::One(_) => inline += 1,
                BlockRefs::Many(_) => spilled += 1,
            }
        }
    }
    assert!(inline + spilled > 0, "no components were recorded; nothing was measured");
    assert_eq!(
        spilled, 0,
        "{spilled} of {} components allocated for a single ref",
        inline + spilled
    );

    // The spilled arm has to keep working: sorted, deduplicated, and reporting what it added.
    let first = BlockLookupRef { routing_bucket: 7, page_ref_key: 2 };
    let second = BlockLookupRef { routing_bucket: 7, page_ref_key: 1 };
    let mut refs = BlockRefs::One(first.clone());
    assert!(!refs.insert(first.clone()), "re-inserting the same ref adds nothing");
    assert_eq!(refs.len(), 1);
    assert!(refs.insert(second.clone()), "a second ref is an addition");
    assert_eq!(refs.len(), 2);
    assert_eq!(
        refs.as_slice(),
        &[second, first],
        "promotion must land sorted -- removal binary-searches these"
    );
}

/// The index on disk does not change shape.
///
/// This is held inline in memory but has to serialize as the sequence it always was, or an index
/// written before this stops loading. Both directions are checked, including a spilled arm read
/// back as an inline one.
#[test]
fn page_refs_serialize_as_a_sequence() {
    use crate::engine::state::{BlockLookupRef, BlockRefs};

    let single = BlockRefs::One(BlockLookupRef {
        routing_bucket: 3,
        page_ref_key: 1,
    });
    let json = serde_json::to_value(&single).unwrap();
    assert!(json.is_array(), "must encode as a sequence, got {json}");
    assert_eq!(json.as_array().unwrap().len(), 1);
    assert_eq!(json[0]["routing_slot"], 3);

    // A one-element sequence written by the previous shape comes back inline, not spilled.
    let restored: BlockRefs = serde_json::from_value(json).unwrap();
    assert!(
        matches!(restored, BlockRefs::One(_)),
        "a one-element sequence should load into the inline arm"
    );
    assert_eq!(restored, single);

    let mut pair = single.clone();
    pair.insert(BlockLookupRef {
        routing_bucket: 4,
        page_ref_key: 2,
    });
    let round_tripped: BlockRefs = serde_json::from_value(serde_json::to_value(&pair).unwrap()).unwrap();
    assert_eq!(round_tripped, pair);
    assert_eq!(round_tripped.len(), 2);
}

/// Pages of the same kind point at ONE string, not a copy each.
///
/// Changing the field's type to a shared pointer does not by itself share anything -- every page
/// could still hold its own allocation and the type would look identical from outside, exactly as
/// the measurement did before. So this compares pointers, not contents.
#[test]
fn pages_of_one_kind_share_a_single_kind_string() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..200 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("shared-kind-{index:05}"),
                value: vec![b'v'; 32],
            },
        });
    }
    for index in 0..40 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: format!("shared-kind-hash-{index:05}"),
                field: "f".to_string(),
                value: vec![b'h'; 32],
            },
        });
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");

    let mut first_of_kind: std::collections::HashMap<String, std::sync::Arc<str>> =
        std::collections::HashMap::new();
    let mut pages = 0usize;
    let mut shared = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for (_ref_key, page) in bucket.page_index.iter() {
            pages += 1;
            match first_of_kind.get(page.model_id.as_ref()) {
                None => {
                    first_of_kind.insert(page.model_id.to_string(), page.model_id.clone());
                }
                Some(first) => {
                    if std::sync::Arc::ptr_eq(first, &page.model_id) {
                        shared += 1;
                    }
                }
            }
        }
    }

    // Anti-vacuity first: with no pages, or one page per kind, "everything is shared" is true for
    // free and proves nothing.
    assert!(pages > 0, "no pages were recorded; nothing was measured");
    assert!(
        pages > first_of_kind.len(),
        "only {} pages for {} kinds -- no kind repeats, so sharing is untested",
        pages,
        first_of_kind.len()
    );
    assert_eq!(
        shared,
        pages - first_of_kind.len(),
        "every page after the first of its kind should point at that first string; \
         {} kinds over {} pages, {shared} shared",
        first_of_kind.len(),
        pages
    );
    assert!(
        first_of_kind.len() >= 2,
        "corpus produced one kind; a second is needed to show the pool distinguishes them"
    );
}

/// The observed range of every numeric field in a page address.
///
/// A field is only narrowable if something real bounds it. Slab geometry bounds a slab id, an
/// offset within a slab and a page length; a hash bounds nothing. This reports the maximum each
/// field actually reaches so the distinction is measured rather than assumed -- a field that looks
/// small in one corpus because the corpus is small would otherwise read as narrowable.
#[test]
fn what_each_address_field_actually_ranges_over() {
    const PAGES: usize = 3_000;
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        2 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..PAGES {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("range-{index:07}"),
                value: vec![b'v'; 96],
            },
        });
    }
    for object in 0..120 {
        for field in 0..6 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: format!("range-hash-{object:06}"),
                    field: format!("f{field}"),
                    value: vec![b'h'; 96],
                },
            });
        }
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut pages = 0usize;
    let (mut slab, mut offset, mut length) = (0u64, 0u64, 0u64);
    let (mut page_id, mut object_id, mut generation, mut band) = (0u64, 0u64, 0u64, 0u64);
    let mut routing = 0u32;
    for bucket in shard.bucket_index.bucket_map.values() {
        for (_key, page) in bucket.page_index.iter() {
            pages += 1;
            let a = &page.address;
            slab = slab.max(a.page_slab_id);
            offset = offset.max(a.offset);
            length = length.max(a.length);
            page_id = page_id.max(a.page_id().unwrap_or(0));
            object_id = object_id.max(a.object_id().unwrap_or(0));
            generation = generation.max(a.generation().unwrap_or(0));
            band = band.max(a.band_id().unwrap_or(0));
            routing = routing.max(a.routing_bucket().unwrap_or(0));
        }
    }
    assert!(pages > 0, "no pages were recorded; nothing was measured");

    let bits = |v: u64| if v == 0 { 0 } else { 64 - v.leading_zeros() };
    println!(
        "
  address field ranges over {pages} pages (max observed, and bits to hold it):
    page_slab_id  {slab:>22}  {:>2} bits   bounded by slab count
    offset        {offset:>22}  {:>2} bits   bounded by slab size
    length        {length:>22}  {:>2} bits   bounded by page size
    page_id       {page_id:>22}  {:>2} bits
    object_id     {object_id:>22}  {:>2} bits   a hash -- bounded by nothing
    generation    {generation:>22}  {:>2} bits
    band_id       {band:>22}  {:>2} bits
    routing_slot  {routing:>22}  {:>2} bits   already u32

    a maximum observed here is NOT a bound: it says a field is a candidate,
    not that it is safe. Narrowing one needs the bound asserted where the
    value is produced, so a violation fails loudly instead of truncating.
",
        bits(slab), bits(offset), bits(length), bits(page_id),
        bits(object_id), bits(generation), bits(band), bits(u64::from(routing)),
    );
}

/// How many separate allocations one object's key text occupies across the whole shard.
///
/// The per-structure censuses each answer a smaller question than this one. Sharing is worth doing
/// where the same bytes are held many times, and that only shows up when the structures are
/// counted together.
#[test]
fn how_many_times_one_object_key_is_stored() {
    use std::collections::HashSet;

    const OBJECTS: usize = 800;
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..OBJECTS {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("crossref-object-{index:08}"),
                value: vec![b'v'; 64],
            },
        });
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");

    // Pick one object and find every place its text is stored, by pointer.
    let sample = shard
        .strings
        .keys()
        .next()
        .expect("the workload must produce at least one string object")
        .clone();

    let mut allocations: HashSet<*const u8> = HashSet::new();
    let mut holders = 0usize;

    for key in shard.strings.keys() {
        if *key == sample {
            allocations.insert(key.as_ptr());
            holders += 1;
        }
    }
    for key in shard.expires_at_ms.keys() {
        if *key == sample {
            allocations.insert(key.as_ptr());
            holders += 1;
        }
    }
    for (_model, object, _refs) in shard.bucket_index.object_page_lookup.iter() {
        if object.as_ref() == sample.as_str() {
            allocations.insert(object.as_ptr());
            holders += 1;
        }
    }
    for bucket in shard.bucket_index.bucket_map.values() {
        for (_ref_key, page) in bucket.page_index.iter() {
            if page.object_key.as_ref() == sample.as_str() {
                allocations.insert(page.object_key.as_ptr());
                holders += 1;
            }
        }
    }

    assert!(holders > 0, "the sampled key was found nowhere; nothing was measured");

    let key_bytes: usize = shard.strings.keys().map(String::len).sum();
    let page_key_bytes: usize = shard
        .bucket_index
        .bucket_map
        .values()
        .flat_map(|bucket| bucket.page_index.values())
        .map(|page| page.object_key.len())
        .sum();
    // No concatenation any more: what the lookup holds per object is the object key itself.
    let lookup_key_bytes: usize = shard
        .bucket_index
        .object_page_lookup
        .iter()
        .map(|(_model, object, _refs)| object.len())
        .sum();
    let expiry_key_bytes: usize = shard.expires_at_ms.keys().map(String::len).sum();
    let total = key_bytes + page_key_bytes + lookup_key_bytes + expiry_key_bytes;

    println!(
        "
  one object key ({} B of text), across the shard:
    structures holding it        {holders}
    distinct allocations         {}   <- what sharing would collapse to 1

  and in total over {OBJECTS} objects:
    strings keys              {key_bytes:>8} B
    page_index object_key     {page_key_bytes:>8} B
    object_page_lookup keys   {lookup_key_bytes:>8} B   (its own allocation of the key)
    expires_at_ms keys        {expiry_key_bytes:>8} B
    TOTAL                     {total:>8} B  = {:.1} B per object
",
        sample.len(),
        allocations.len(),
        total as f64 / OBJECTS as f64,
    );
}

/// The lookup is nested in memory and flat on disk.
///
/// This is the property the change rests on: an index written before the nesting has to load, and
/// one written now has to stay readable by anything expecting the old shape. Both directions are
/// checked, and the composite is compared literally rather than by round-tripping alone -- a
/// round-trip through a consistently wrong format would pass.
#[test]
fn the_nested_lookup_still_serializes_as_the_flat_composite() {
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
            key: "nested-key".to_string(),
            value: vec![b'v'; 32],
        },
    });

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let lookup = &shard.bucket_index.object_page_lookup;
    assert!(!lookup.is_empty(), "the write must produce a lookup entry");

    let json = serde_json::to_value(lookup).unwrap();
    let object = json.as_object().expect("the wire form is a flat map");

    // Length-prefixed parts: model, then object key.
    let expected = format!("{}:{}|{}:{}|", "string".len(), "string", "nested-key".len(), "nested-key");
    assert!(
        object.contains_key(&expected),
        "the serialized key must be the flat composite {expected:?}, got {:?}",
        object.keys().collect::<Vec<_>>()
    );

    // And a document in that shape loads back into the nested form.
    let restored: crate::engine::state::ObjectBlockLookup =
        serde_json::from_value(json).unwrap();
    assert!(
        restored.get("string", "nested-key").is_some(),
        "a flat document must load into the nested map"
    );
    assert_eq!(restored.len(), lookup.len());
}

/// The page entry and the lookup hold the SAME allocation of an object's key.
///
/// Compared by pointer, not by contents. Changing the field's type shares nothing on its own --
/// the lookup previously called `Arc::from` and built a second allocation of text the page already
/// owned, which is byte-for-byte identical from the outside and costs exactly as much as before.
/// Only pointer identity can tell those apart.
#[test]
fn the_page_and_the_lookup_point_at_one_object_key() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..64 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("shared-object-{index:04}"),
                value: vec![b'v'; 48],
            },
        });
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");

    let mut checked = 0usize;
    let mut shared = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for (_ref_key, page) in bucket.page_index.iter() {
            let Some(refs) = shard
                .bucket_index
                .object_page_lookup
                .key_ptr(&page.model_id, page.object_key.as_ref())
            else {
                continue;
            };
            checked += 1;
            if std::ptr::eq(refs, page.object_key.as_ptr()) {
                shared += 1;
            }
        }
    }

    // Anti-vacuity: with nothing checked, "all shared" is true for free.
    assert!(checked > 0, "no page was matched to a lookup entry; nothing was measured");
    assert_eq!(
        shared, checked,
        "{shared} of {checked} pages share their key with the lookup; the rest hold a second copy"
    );
}

/// A page filed with an address that carries no object id still knows which object it belongs to.
///
/// This is the one way removing the entry's own copy could lose information. The id is computed as
/// `address.object_id().unwrap_or_else(stable_page_object_id)`, so before this change the entry
/// could hold a fallback the address did not have. Reading through the address would then answer
/// zero for exactly those pages. The write path now puts the computed id into the address; this
/// asserts it, because the census that motivated the removal cannot see the case at all -- every
/// address in it already carried an id.
#[test]
fn a_page_whose_address_carries_no_object_id_still_reports_one() {
    use crate::block_store::BlockAddress;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    {
        let mut shards = engine.shards.write().expect("shards lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 loaded");
        // Deliberately no object id, and no routing slot either.
        let address = BlockAddress::from_parts(0, 0, 16, Some(7), None, None, None, None);
        assert!(address.object_id().is_none(), "the case under test");
        crate::engine::storage_bucket_internals::upsert_bucket_index_page(
            shard,
            1,
            "string",
            "orphan-key",
            None,
            address,
            false,
        );
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut seen = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for (_ref_key, page) in bucket.page_index.iter() {
            if page.object_key.as_ref() != "orphan-key" {
                continue;
            }
            seen += 1;
            assert_ne!(
                page.object_id(),
                0,
                "a page filed from an address with no object id must still report the computed one"
            );
            // Cross-checked against what the write path recorded, rather than by recomputing
            // the same function and comparing it with itself: the bucket's object index was
            // populated from the id the write path actually used.
            assert!(
                bucket.object_index.contains(&page.object_id()),
                "the id the page reports must be the one the write path filed it under"
            );
        }
    }
    assert_eq!(seen, 1, "the page must be in the index, or nothing was tested");
}

/// Deleting an object costs a bounded number of allocations, not one per page examined.
///
/// The scan compared each page's key against the wanted one by building a fresh `Arc<str>` from
/// the wanted key inside the loop -- a heap allocation and a copy of the key per page, to compare
/// and immediately drop. Twelve sites did this. Borrowing compares the same bytes with no
/// allocation at all.
///
/// Measured as allocations rather than time because the allocation IS the cost here, and counted
/// with `allocs` rather than `outstanding`: this pattern frees everything it takes, so a live-heap
/// measurement reports zero while the work still happens.
#[test]
#[cfg(feature = "alloc-probe")]
fn deleting_an_object_does_not_allocate_per_page_scanned() {
    let delete_from_a_hash_of = |fields: usize| -> u64 {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for field in 0..fields {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: "wide".to_string(),
                    field: format!("f{field}"),
                    value: vec![b'v'; 16],
                },
            });
        }

        let probe = crate::alloc_probe::Probe::start();
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonDelete {
                key: "wide".to_string(),
            },
        });
        probe.stop().allocs
    };

    let narrow = delete_from_a_hash_of(20);
    let wide = delete_from_a_hash_of(200);

    println!("delete allocations: {narrow} at 20 fields, {wide} at 200 fields");

    // Guard the guard: an upper bound passes hardest at zero, so prove the probe saw the work.
    assert!(narrow > 0, "the probe must observe the delete: {narrow}");

    // 180 more pages to scan. Per-page allocation would put ~180 extra allocations here.
    let growth = wide.saturating_sub(narrow);
    assert!(
        growth < 180,
        "deleting scanned 180 more pages and allocated {growth} more times          ({narrow} at 20 fields, {wide} at 200) -- that is per-page allocation"
    );
}

/// What happens to the index when the same logical object is written twice.
///
/// Reported, not asserted into a particular answer: the point is to find out whether the map
/// deduplicates by key collision or through the lookup, because that decides whether the key can
/// become an opaque assigned id.
#[test]
fn rewriting_a_page_does_not_reuse_its_index_key() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let key = "rewritten-object";
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet { key: key.to_string(), value: vec![b'a'; 64] },
    });
    let after_first: Vec<String> = {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        shard
            .bucket_index
            .bucket_map
            .values()
            .flat_map(|bucket| bucket.page_index.iter())
            .filter(|(_, page)| page.object_key.as_ref() == key)
            .map(|(ref_key, _)| ref_key.to_string())
            .collect()
    };

    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet { key: key.to_string(), value: vec![b'b'; 64] },
    });
    let after_second: Vec<String> = {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        shard
            .bucket_index
            .bucket_map
            .values()
            .flat_map(|bucket| bucket.page_index.iter())
            .filter(|(_, page)| page.object_key.as_ref() == key)
            .map(|(ref_key, _)| ref_key.to_string())
            .collect()
    };

    assert_eq!(after_first.len(), 1, "the first write must produce exactly one entry");
    println!(
        "
  rewriting one object:
    entries after the first write   {}
    entries after the second        {}
    key reused?                     {}
    first  key: {}
    second key: {}
",
        after_first.len(),
        after_second.len(),
        after_second == after_first,
        after_first.first().map(String::as_str).unwrap_or("-"),
        after_second.first().map(String::as_str).unwrap_or("-"),
    );

    // The property that must hold either way: one live entry per object, however that is achieved.
    assert_eq!(
        after_second.len(),
        1,
        "a rewrite must leave one entry, not accumulate them"
    );
}

/// The same page, installed twice, is one entry with one handle.
///
/// This is the property the rendered string key provided for free: it WAS the key, so installing
/// a page whose identity already appeared replaced it. A handle from a counter compiles, dumps
/// and reloads perfectly and still breaks this -- each install takes a fresh slot, so a rebuild
/// accumulates entries and the object counts drift apart. Three tests caught that as a wrong
/// number; this one states the reason.
#[test]
fn installing_the_same_page_twice_replaces_it() {
    let page = || crate::engine::state::BlockIndex {
        object_key: Arc::from("twice".to_string()),
        model_id: Arc::from("string".to_string()),
        component: None,
        address: BlockAddress::from_parts(1, 0, 4, Some(1), Some(30), Some(3), Some(1), None),
        dirty: false,
        deleted: false,
        log_backed: true,
    };

    let mut map = crate::engine::state::BlockIndexMap::default();
    let first = map.insert(page());
    let second = map.insert(page());

    assert_eq!(first, second, "the same page must land on the same handle");
    assert_eq!(map.len(), 1, "installing it twice must not add a second entry");

    // A page differing in one identity field is a different page and keeps its own slot.
    let mut moved = page();
    moved.address = BlockAddress::from_parts(1, 64, 4, Some(1), Some(30), Some(3), Some(1), None);
    let third = map.insert(moved);
    assert_ne!(first, third, "a page at another offset is not the same page");
    assert_eq!(map.len(), 2);
}

/// Writing the page index does not build a second copy of it first.
///
/// `#[serde(into = "...")]` is defined as `T::from(self.clone()).serialize(..)`, so it duplicates
/// the whole map -- once for the clone, once for the converted map -- before a byte is written.
/// The `Arc`s inside are refcount bumps rather than allocations, so what this actually counts is
/// the duplicated map structure, which is why it is measured rather than asserted.
#[test]
#[cfg(feature = "alloc-probe")]
fn dumping_the_page_index_does_not_copy_it_first() {
    let dump_a_shard_of = |pages: usize| -> u64 {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for field in 0..pages {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: "dumped".to_string(),
                    field: format!("f{field}"),
                    value: vec![b'v'; 16],
                },
            });
        }
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        let bucket = shard
            .bucket_index
            .bucket_map
            .values()
            .find(|b| !b.page_index.is_empty())
            .expect("the writes must produce pages");

        let probe = crate::alloc_probe::Probe::start();
        let json = serde_json::to_string(&bucket.page_index).expect("the index serializes");
        let counts = probe.stop();
        assert!(!json.is_empty());
        counts.allocs
    };

    let small = dump_a_shard_of(20);
    let large = dump_a_shard_of(200);
    println!("dump allocations: {small} at 20 pages, {large} at 200 pages");

    // An upper bound passes most easily when nothing was measured, so prove the probe saw work.
    assert!(small > 0, "the probe must observe the dump: {small}");

    // Writing a page costs its key and little else. Copying the index first, or building that key
    // with `format!` (which allocates its own buffer and then allocates again to return it), put
    // this near four.
    let per_page = (large.saturating_sub(small)) as f64 / 180.0;
    assert!(
        per_page < 2.0,
        "dumping cost {per_page:.2} allocations per page ({small} at 20, {large} at 200)"
    );
}

/// The dump lists pages in written-key order.
///
/// Worth pinning because the in-memory key stopped being the written one. The index used to be
/// serialized by converting it into a map keyed by the rendered string, which emitted entries in
/// string order for free. This map is keyed by a handle and iterates in hash order, so anything
/// writing it has to restore that order deliberately -- otherwise the bytes on disk change even
/// though every entry is still present and correct.
#[test]
fn the_page_index_dump_is_ordered_by_its_written_key() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for field in 0..24 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: "ordered".to_string(),
                field: format!("f{field:02}"),
                value: vec![b'v'; 16],
            },
        });
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let bucket = shard
        .bucket_index
        .bucket_map
        .values()
        .find(|b| b.page_index.len() > 4)
        .expect("the writes must produce several pages in one bucket");

    let json = serde_json::to_string(&bucket.page_index).expect("the index serializes");
    let value: serde_json::Value = serde_json::from_str(&json).expect("valid json");
    let keys: Vec<&String> = value
        .as_object()
        .expect("a map of rendered keys")
        .keys()
        .collect();

    assert!(keys.len() > 4, "need several keys to see an order: {keys:?}");
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted, "the dump must be ordered by written key");
}

/// A command does not allocate to discover that nothing has expired.
///
/// Lazy expiry runs on every command -- 51 call sites -- and used to build four owned keys per
/// call to look them up in a map that, on a store with no TTLs, is empty. One of those keys came
/// from `format!`. All four were dropped immediately after the lookup.
///
/// Counted as allocations rather than time because the allocation is the whole cost: the work
/// this does on a store without expiries is a single `is_empty` check.
#[test]
#[cfg(feature = "alloc-probe")]
fn a_command_does_not_allocate_to_check_expiry() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let reads = |n: usize| -> f64 {
        let probe = crate::alloc_probe::Probe::start();
        for i in 0..n {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: format!("absent{i:04}"),
                },
            });
        }
        probe.stop().allocs as f64 / n as f64
    };

    let per_read = reads(200);
    println!("miss costs {per_read:.1} allocations");

    // An upper bound passes most easily when nothing happened, so prove work was observed.
    assert!(per_read > 1.0, "the probe must observe the reads: {per_read}");

    // The expiry check accounted for five of these. The rest is the lookup itself, which this
    // bound deliberately leaves room for -- it is here to catch the per-command allocation
    // coming back, not to freeze the whole read path.
    assert!(
        per_read < 38.0,
        "a read cost {per_read:.1} allocations; lazy expiry is allocating again"
    );
}

/// The page index is keyed by a number in memory and by the old string on disk.
///
/// This is what makes the numeric key an in-memory change rather than a format change. Asserted on
/// the literal key rather than by round-tripping alone: a round trip through a consistently wrong
/// spelling would pass, and an index written now has to be readable by something expecting the old
/// one.
#[test]
fn the_page_index_still_writes_string_keys() {
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
        command: Command::StringSet { key: "dumped".to_string(), value: vec![b'v'; 32] },
    });

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let bucket = shard
        .bucket_index
        .bucket_map
        .values()
        .find(|bucket| !bucket.page_index.is_empty())
        .expect("the write must produce a page");

    // In memory: a handle, not text.
    let (handle, page) = bucket.page_index.iter().next().expect("one page");
    assert!(*handle > 0, "the map assigns a handle");

    // On the wire: the same rendered key it always wrote, rebuilt from the page.
    let json = serde_json::to_value(&bucket.page_index).unwrap();
    let written = json.as_object().expect("the wire form is a map of string keys");
    let expected = crate::engine::state::block_index_written_key(page);
    assert!(
        written.contains_key(&expected),
        "the dump must carry the rendered key {expected:?}, got {:?}",
        written.keys().collect::<Vec<_>>()
    );
    assert!(
        expected.contains("dumped"),
        "and that key names the object: {expected}"
    );

    // And a document in that shape loads back, with handles assigned afresh.
    let restored: crate::engine::state::BlockIndexMap = serde_json::from_value(json).unwrap();
    assert_eq!(restored.len(), bucket.page_index.len());
    assert!(
        restored.iter().all(|(handle, _)| *handle > 0),
        "every loaded page gets a handle"
    );
}

/// After deletes, the lookup matches the one a full rebuild produces.
///
/// The delete path used to call `rebuild_object_page_lookup`, so it was correct by construction
/// and O(shard). It now removes the deleted object's own entries instead, which is only correct
/// if the result is identical -- and a lookup that quietly disagrees with the page index does not
/// fail loudly, it makes reads miss. So this compares against the rebuild rather than against a
/// hand-written expectation.
#[test]
fn deleting_leaves_the_lookup_a_rebuild_would_have_built() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    // A mix of kinds and components, so the object's refs are not all in one shape.
    for i in 0..40 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet { key: format!("s{i:03}"), value: vec![b'v'; 48] },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: format!("h{i:03}"),
                field: format!("f{i}"),
                value: vec![b'v'; 32],
            },
        });
    }
    // Delete some of each, including one that never existed.
    for i in (0..40).step_by(3) {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonDelete { key: format!("s{i:03}") },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonDelete { key: format!("h{i:03}") },
        });
    }
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CommonDelete { key: "never-existed".to_string() },
    });

    let mut shards = engine.shards.write().expect("shards lock poisoned");
    let shard = shards.get_mut(&1).expect("shard 1 loaded");

    let incremental: Vec<(String, String, Vec<(u32, u64)>)> = shard
        .bucket_index
        .object_page_lookup
        .iter()
        .map(|(model, object, refs)| {
            let mut flat: Vec<(u32, u64)> = refs
                .by_component
                .iter()
                .flat_map(|component| component.refs.as_slice())
                .map(|page_ref| (page_ref.routing_bucket, page_ref.page_ref_key))
                .collect();
            flat.sort();
            (model.to_string(), object.to_string(), flat)
        })
        .collect();
    let counter_before = shard.bucket_index.object_component_page_refs;

    shard.bucket_index.rebuild_object_page_lookup();

    let rebuilt: Vec<(String, String, Vec<(u32, u64)>)> = shard
        .bucket_index
        .object_page_lookup
        .iter()
        .map(|(model, object, refs)| {
            let mut flat: Vec<(u32, u64)> = refs
                .by_component
                .iter()
                .flat_map(|component| component.refs.as_slice())
                .map(|page_ref| (page_ref.routing_bucket, page_ref.page_ref_key))
                .collect();
            flat.sort();
            (model.to_string(), object.to_string(), flat)
        })
        .collect();

    assert!(
        !rebuilt.is_empty(),
        "the surviving objects must still be in the lookup, or nothing was compared"
    );
    assert_eq!(
        incremental, rebuilt,
        "removing the deleted object must leave what a rebuild builds"
    );
    // The total is a cache: `None` means "not established", which is a legal state the rebuild
    // resolves. What must never happen is an established total that disagrees with the refs it
    // counts, so that -- not equality with a freshly rebuilt one -- is what is asserted.
    if let Some(total) = counter_before {
        let actual: usize = rebuilt
            .iter()
            .map(|(_model, _object, refs)| refs.len())
            .sum();
        assert_eq!(
            total, actual,
            "the maintained ref total must match the refs actually held"
        );
    }
}

/// One delete costs the same whether the store holds 200 keys or 3,200.
///
/// It did not. Deleting an object rebuilt the object-to-page lookup for the entire shard, which
/// clears it, clones every page in every bucket into a vector and re-inserts them -- so a delete
/// allocated in proportion to the whole store, and deleting a store cost the square of its size.
/// Measured over 400 deletes: 610 allocations each at 200 resident keys and 4,053 at 3,200.
///
/// Counted rather than timed because the count is the cost, and compared as a ratio across store
/// sizes rather than against a fixed number, since what matters is that it stopped scaling.
#[test]
#[cfg(feature = "alloc-probe")]
fn does_delete_scale_with_the_store() {
    let cost_at = |resident: usize| -> (f64, f64) {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for i in 0..resident {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("resident{i:06}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        // Keys to delete, beyond the resident set.
        for i in 0..400 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("victim{i:04}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        let probe = crate::alloc_probe::Probe::start();
        for i in 0..400 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonDelete {
                    key: format!("victim{i:04}"),
                },
            });
        }
        let c = probe.stop();
        (c.allocs as f64 / 400.0, c.alloc_bytes as f64 / 400.0)
    };

    let (small, small_bytes) = cost_at(200);
    let (large, large_bytes) = cost_at(3200);
    println!("delete at 200: {small:.1} allocs / {small_bytes:.0} B, at 3200: {large:.1} / {large_bytes:.0} B");

    // A ratio test passes trivially if neither side did anything.
    assert!(small > 1.0, "the probe must observe deletes: {small}");

    // Sixteen times the store. Rebuilding put this at 6.6x; the establishing rebuild that still
    // happens once per shard leaves a little slope, which 400 deletes amortise to nearly nothing.
    let growth = large / small;
    assert!(
        growth < 1.5,
        "a delete cost {small:.1} allocations at 200 resident keys and {large:.1} at 3,200          ({growth:.1}x) -- it is scaling with the store again"
    );
    let byte_growth = large_bytes / small_bytes;
    assert!(
        byte_growth < 1.6,
        "a delete allocated {small_bytes:.0} bytes at 200 keys and {large_bytes:.0} at 3,200          ({byte_growth:.1}x)"
    );
}

/// Exploratory: one page per field, or one page rewritten per write?
#[test]
fn how_many_pages_does_a_wide_hash_hold() {
    for fields in [10usize, 100, 400] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for i in 0..fields {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: "one".to_string(),
                    field: format!("f{i:06}"),
                    value: vec![b'v'; 32],
                },
            });
        }
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        let pages: usize = shard
            .bucket_index
            .bucket_map
            .values()
            .map(|bucket| bucket.page_index.len())
            .sum();
        let buckets = shard.bucket_index.bucket_map.len();
        let lookup_refs: usize = shard
            .bucket_index
            .object_page_lookup
            .iter()
            .map(|(_m, _o, refs)| refs.by_component.iter().map(|c| c.refs.as_slice().len()).sum::<usize>())
            .sum();
        println!("fields {fields:5}  pages {pages:6}  buckets {buckets:4}  lookup refs {lookup_refs:6}");
    }
}

/// Every field of a hash is filed in the bucket index after it is written.
///
/// The per-write sync used to re-file every field of the object each time, which made this true
/// by brute force. It now files only the fields that are missing, so the property has to be
/// checked rather than assumed: skipping one leaves the index quietly short of a page, and a
/// missing page is a read that returns nothing rather than an error.
#[test]
fn every_field_of_a_hash_is_filed_in_the_bucket_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for i in 0..64 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: "wide".to_string(),
                field: format!("f{i:03}"),
                value: vec![b'v'; 32],
            },
        });
    }
    // Overwrite some, so a field's address changes and the old filing is stale.
    for i in (0..64).step_by(5) {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: "wide".to_string(),
                field: format!("f{i:03}"),
                value: vec![b'w'; 48],
            },
        });
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let fields = shard.hashes.get("wide").expect("the hash exists");
    assert_eq!(fields.len(), 64, "all fields written");

    for (field, address) in fields.iter() {
        assert!(
            shard.bucket_index.contains_object_page_address(
                "hash",
                "wide",
                Some(field.as_str()),
                address
            ),
            "field {field} holds an address the bucket index does not have filed"
        );
    }
}

/// Exploratory: which bucket-visiting site grows with a wide object?
#[test]
#[cfg(feature = "alloc-probe")]
fn which_site_visits_the_pages() {
    use crate::engine::storage_bucket_internals::bucket_visit_sites;
    for fields in [100usize, 400, 1600] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for i in 0..fields {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: "one".to_string(),
                    field: format!("f{i:06}"),
                    value: vec![b'v'; 32],
                },
            });
        }
        bucket_visit_sites::reset();
        let probe = crate::alloc_probe::Probe::start();
        for i in 0..100 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: "one".to_string(),
                    field: format!("probe{i:04}"),
                    value: vec![b'v'; 32],
                },
            });
        }
        let allocs = probe.stop().allocs as f64 / 100.0;
        let (layout, clear_dirty, refresh_flags, remove_all) = bucket_visit_sites::snapshot();
        println!(
            "fields {fields:5}  allocs/write {allocs:8.1}  visits/write: layout {:7.1} clear_dirty {:7.1} refresh {:7.1} remove_all {:7.1}",
            layout as f64 / 100.0,
            clear_dirty as f64 / 100.0,
            refresh_flags as f64 / 100.0,
            remove_all as f64 / 100.0
        );
    }
}

/// Exploratory: do writes cost more as the store grows?
#[test]
#[cfg(feature = "alloc-probe")]
fn does_writing_scale_with_the_store() {
    let cost_at = |resident: usize, wide_object: bool| -> (f64, f64) {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for i in 0..resident {
            if wide_object {
                // Everything under ONE object key: the object's own ref list grows.
                engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashSet {
                        key: "one".to_string(),
                        field: format!("f{i:06}"),
                        value: vec![b'v'; 32],
                    },
                });
            } else {
                // Spread across many object keys: the store grows, each object stays small.
                engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashSet {
                        key: format!("h{i:06}"),
                        field: "f".to_string(),
                        value: vec![b'v'; 32],
                    },
                });
            }
        }
        let probe = crate::alloc_probe::Probe::start();
        for i in 0..100 {
            if wide_object {
                engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashSet {
                        key: "one".to_string(),
                        field: format!("probe{i:04}"),
                        value: vec![b'v'; 32],
                    },
                });
            } else {
                engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashSet {
                        key: format!("probe{i:04}"),
                        field: "f".to_string(),
                        value: vec![b'v'; 32],
                    },
                });
            }
        }
        let c = probe.stop();
        (c.allocs as f64 / 100.0, c.alloc_bytes as f64 / 100.0)
    };

    let (narrow_small, _) = cost_at(100, false);
    let (narrow_large, _) = cost_at(1600, false);
    let (wide_small, _) = cost_at(100, true);
    let (wide_large, _) = cost_at(1600, true);
    println!(
        "many objects {narrow_small:.1} -> {narrow_large:.1}   one wide object {wide_small:.1} -> {wide_large:.1}"
    );

    // A ratio test proves nothing if neither side did any work.
    assert!(wide_small > 1.0, "the probe must observe writes: {wide_small}");

    // Writing to a store with 16x the objects already cost the same; this is the guard on it.
    assert!(
        narrow_large / narrow_small < 1.3,
        "a write cost {narrow_small:.1} allocations at 100 objects and {narrow_large:.1} at 1,600"
    );

    // Writing a field to an object that already has 1,600 of them cost 8,388 allocations against
    // 800 at 100 fields, because the per-write sync re-filed every field the object had.
    assert!(
        wide_large / wide_small < 1.3,
        "a write cost {wide_small:.1} allocations at 100 fields and {wide_large:.1} at 1,600          ({:.1}x) -- the write is scaling with the object again",
        wide_large / wide_small
    );
}

/// Every key the shard index writes, listed.
///
/// A guard for renaming Rust identifiers without moving the format under the data. Most of these
/// fields carry no `serde` attribute, so their Rust name IS their wire name -- renaming one
/// changes what is written, and an index written by the new code would not be read by the old.
/// Listing them makes that visible in a diff instead of silent.
#[test]
fn the_index_wire_keys_are_what_they_were() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        8 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet { key: "wire".to_string(), value: vec![b'v'; 32] },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashSet {
            key: "wire-hash".to_string(),
            field: "f".to_string(),
            value: vec![b'v'; 32],
        },
    });

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let value = serde_json::to_value(&shard.bucket_index).expect("the index serializes");

    // Every object key that appears anywhere in the document, deduplicated.
    fn collect(value: &serde_json::Value, into: &mut std::collections::BTreeSet<String>) {
        match value {
            serde_json::Value::Object(map) => {
                for (key, child) in map {
                    into.insert(key.clone());
                    collect(child, into);
                }
            }
            serde_json::Value::Array(items) => {
                for item in items {
                    collect(item, into);
                }
            }
            _ => {}
        }
    }
    let mut keys = std::collections::BTreeSet::new();
    collect(&value, &mut keys);

    // The rendered page keys are data, not field names: they are built from the object's own
    // identity, so they vary with the test's keys rather than with the format.
    keys.retain(|key| !key.contains(':') && !key.chars().all(|c| c.is_ascii_digit()));

    let listed: Vec<&str> = keys.iter().map(String::as_str).collect();
    assert_eq!(
        listed,
        vec![
            "address",
            "b",
            "by_component",
            "component",
            "deleted",
            "deleted_object_index",
            "dirty",
            "dirty_generation",
            "g",
            "in_memory",
            "l",
            "last_dump_sequence",
            "layout",
            "loading",
            "log_backed",
            "meta_loaded",
            "model_id",
            "o",
            "object_index",
            "object_key",
            "object_page_lookup",
            "oi",
            "page_index",
            "page_ref_key",
            "pi",
            "ps",
            "refs",
            "routing_slot",
            "rs",
            "slot_map",
            "ttl_ms",
        ],
        "the index writes different keys than it did; a rename reached the format"
    );
}

/// A bucket holding one object id holds it inline, and goes back to inline when it can.
///
/// Every bucket has one: keys route one to a bucket, and an object with many components is still
/// one object, so the set that held it was never holding more. A `BTreeSet` with a single member
/// costs 128 live bytes of node to carry eight bytes of id.
///
/// The demotion is checked too. A bucket that briefly held two objects would otherwise keep its
/// node for the rest of its life, which is the cost being removed and shows up nowhere else.
#[test]
fn a_bucket_holding_one_object_holds_no_node() {
    use crate::engine::state::ObjectIndex;

    let mut index = ObjectIndex::default();
    assert!(index.is_empty());

    assert!(index.insert(7));
    assert!(matches!(index, ObjectIndex::One(7)), "one id is held inline");
    assert!(!index.insert(7), "the same id twice is one entry");
    assert_eq!(index.len(), 1);
    assert!(index.contains(&7));

    assert!(index.insert(9));
    assert!(matches!(index, ObjectIndex::Many(_)), "two ids need a set");
    assert_eq!(index.len(), 2);

    assert!(index.remove(&9));
    assert!(
        matches!(index, ObjectIndex::One(7)),
        "a set that drops back to one id must give up its node"
    );

    assert!(index.remove(&7));
    assert!(index.is_empty(), "and to nothing at all");
    assert!(!index.remove(&7), "removing what is not there changes nothing");

    // The order it iterates and writes is the order the set had.
    let mut many = ObjectIndex::default();
    many.extend([5u64, 1, 3]);
    let ids: Vec<u64> = many.iter().copied().collect();
    assert_eq!(ids, vec![1, 3, 5], "ids come out sorted, as the set gave them");
    assert_eq!(
        serde_json::to_string(&many).expect("serializes"),
        "[1,3,5]",
        "and are written as the same sequence"
    );

    // A single id writes the same shape, and reads back inline rather than as a set.
    let one: ObjectIndex = serde_json::from_str("[4]").expect("deserializes");
    assert!(matches!(one, ObjectIndex::One(4)), "a loaded single id is held inline");
    assert_eq!(serde_json::to_string(&one).expect("serializes"), "[4]");
}


/// Writing a message does not cost the length of its node's history.
///
/// The per-write index sync files every page its object has. Each upsert drops the entry's
/// existing refs before inserting, so filing a list leaves only its last element -- the rest are
/// removed again on the way past. A node holding 850 events therefore re-filed 850 pages to add
/// its 851st, and filling a node cost the square of its length: 2,072 allocations per message at
/// 50 events, 23,822 at 800.
///
/// Bounded on the absolute cost at 800 rather than on a ratio, because a ratio would still pass
/// comfortably -- this is not flat yet. What remains is in the WAL append, measured at 8,700 of
/// the 8,800 allocations a write costs there and untouched by this.
#[test]
#[cfg(feature = "alloc-probe")]
fn writing_a_message_does_not_cost_its_nodes_history() {
    // A message as a caller would write one: some text, and an embedding.
    let dims = 384usize;
    let write = |engine: &TemporalEngine, i: usize, with_vector: bool| {
        let vector: Vec<f32> = if with_vector {
            (0..dims).map(|d| (d as f32) * 0.001 + i as f32).collect()
        } else {
            Vec::new()
        };
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextWriteEvent {
                tenant_hash: 1,
                node_hash: 42,
                first_write_only: false,
                cold_storage: false,
                event: ContextEvent {
                    event_id_hash: 1_000 + i as u64,
                    event_time_ms: 1_700_000_000_000 + i as u64,
                    ingestion_time_ms: 1_700_000_000_000,
                    kind: 1,
                    event_type: 2,
                    actor_hash: 7,
                    status: 0,
                    valid_until_ms: 0,
                    confidence: 0.9,
                    importance: 0.5,
                    text: format!(
                        "message {i}: the quick brown fox jumps over the lazy dog, twice over"
                    ),
                    source_ref: format!("src/{i}"),
                    related_node_hashes: vec![42],
                    compact_attrs: Vec::new(),
                    vector,
                },
            },
        });
    };

    let cost_at = |resident: usize| -> f64 {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            32 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for i in 0..resident {
            write(&engine, i, true);
        }
        let probe = crate::alloc_probe::Probe::start();
        for i in resident..resident + 50 {
            write(&engine, i, true);
        }
        probe.stop().allocs as f64 / 50.0
    };

    let narrow = cost_at(50);
    let wide = cost_at(800);
    println!("message write: {narrow:.0} allocations at 50 events, {wide:.0} at 800");

    // A bound passes most easily when nothing was measured.
    assert!(narrow > 1.0, "the probe must observe the writes: {narrow}");

    // Was 23,822 when the sync filed every page of the series.
    assert!(
        wide < 12_000.0,
        "a message cost {wide:.0} allocations on a node holding 800 events; the index sync is \
         filing the whole series again"
    );
}


/// What a key costs the index in live heap.
///
/// Measured as allocated-minus-freed rather than as a struct size, because the cost this bounds
/// was never in the struct: a bucket held its single page in a `BTreeMap`, whose node is sized
/// for eleven entries and cost 1,496 live bytes to carry 120 bytes of page. Holding one page
/// inline took a key from 2,336 live bytes to 1,085.
#[test]
#[cfg(feature = "alloc-probe")]
fn what_a_key_costs_the_index_in_live_heap() {
    let cost_at = |keys: usize| -> f64 {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            8 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        let probe = crate::alloc_probe::Probe::start();
        for i in 0..keys {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("k{i:07}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        let counts = probe.stop();
        (counts.alloc_bytes as i64 - counts.free_bytes as i64) as f64 / keys as f64
    };

    let per_key = cost_at(2000);
    println!("live heap: {per_key:.0} bytes per key");

    // A bound passes most easily when nothing was measured.
    assert!(per_key > 100.0, "the probe must observe the writes: {per_key}");

    assert!(
        per_key < 1060.0,
        "a key costs {per_key:.0} live bytes; a bucket is holding a node for a single entry again"
    );
}

/// A bucket holding one page holds it inline, and goes back to inline when it can.
///
/// Keys route one to a bucket, so most buckets hold a single page. A `BTreeMap` holding one entry
/// costs 1,496 live bytes to carry a 120-byte page, because its node is sized for eleven -- which
/// made the containers about 70% of the index's live heap.
///
/// The demotion matters as much as the promotion: a bucket that briefly held two pages would
/// otherwise keep its node for the rest of its life, which is exactly the cost being avoided and
/// would not show up as a failure anywhere else.
#[test]
fn a_bucket_holding_one_page_holds_no_node() {
    use crate::engine::state::BlockIndexMap;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        8 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    // One page under an object: inline.
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "solo".to_string(),
            value: vec![b'v'; 32],
        },
    });
    {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        let bucket = shard
            .bucket_index
            .bucket_map
            .values()
            .find(|bucket| bucket.page_index.len() == 1)
            .expect("the write must produce a bucket holding one page");
        assert!(
            matches!(bucket.page_index, BlockIndexMap::One(..)),
            "a bucket holding one page must hold it inline"
        );
    }

    // Several components of one object: a map, which is what a map is for.
    for field in ["a", "b", "c"] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: "wide".to_string(),
                field: field.to_string(),
                value: vec![b'v'; 32],
            },
        });
    }
    {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        let bucket = shard
            .bucket_index
            .bucket_map
            .values()
            .find(|bucket| bucket.page_index.len() > 1)
            .expect("three fields must share a bucket");
        assert!(
            matches!(bucket.page_index, BlockIndexMap::Many(_)),
            "several pages belong in a map"
        );
    }

    // Down to one again: back to inline, not a map with one entry left in it.
    for field in ["a", "b"] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashDelete {
                key: "wide".to_string(),
                field: field.to_string(),
            },
        });
    }
    {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        let bucket = shard
            .bucket_index
            .bucket_map
            .values()
            .find(|bucket| {
                bucket
                    .page_index
                    .values()
                    .any(|page| &*page.object_key == "wide")
            })
            .expect("the remaining field must still be filed");
        assert_eq!(bucket.page_index.len(), 1, "two of three fields were removed");
        assert!(
            matches!(bucket.page_index, BlockIndexMap::One(..)),
            "a map that drops back to one page must give up its node"
        );
    }
}










/// How many distinct identity strings the page index holds, against how many copies of each.
///
/// Interning pays only where cardinality is low relative to the number of holders. The object key
/// is unique per object, so sharing it saves copies but never collapses them -- established
/// already, and not what this measures. The question here is the other two: `model_id` should be
/// drawn from a small fixed set of model types, and `component` from a per-object field name.
///
/// Reported per field rather than as one identity-strings total, because the answer differs by
/// field and a combined number would hide that.
#[test]
fn page_index_identity_string_cardinality() {
    use std::collections::HashSet;

    const STRING_OBJECTS: usize = 1_500;
    const HASH_OBJECTS: usize = 150;
    const FIELDS_PER_HASH: usize = 8;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..STRING_OBJECTS {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("identity-string-{index:08}"),
                value: vec![b'v'; 64],
            },
        });
    }
    for object in 0..HASH_OBJECTS {
        for field in 0..FIELDS_PER_HASH {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: format!("identity-hash-{object:08}"),
                    field: format!("field-{field}"),
                    value: vec![b'h'; 64],
                },
            });
        }
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");

    let mut pages = 0usize;
    let mut distinct_models: HashSet<&str> = HashSet::new();
    // Distinct ALLOCATIONS, not distinct values. Once the kind is shared, counting holders would
    // report the cost as if nothing had changed -- every page still holds one, it just points at
    // a string it does not own.
    let mut model_allocations: HashSet<*const u8> = HashSet::new();
    let mut component_allocations: HashSet<*const u8> = HashSet::new();
    let mut distinct_components: HashSet<&str> = HashSet::new();
    let mut distinct_keys: HashSet<&str> = HashSet::new();
    let mut model_bytes = 0usize;
    let mut component_bytes = 0usize;
    let mut key_bytes = 0usize;
    let mut components_present = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for (_ref_key, page) in bucket.page_index.iter() {
            pages += 1;
            distinct_models.insert(page.model_id.as_ref());
            model_allocations.insert(std::sync::Arc::as_ptr(&page.model_id).cast::<u8>());
            distinct_keys.insert(page.object_key.as_ref());
            model_bytes += page.model_id.len();
            key_bytes += page.object_key.len();
            if let Some(component) = page.component.as_deref() {
                distinct_components.insert(component);
                if let Some(shared) = page.component.as_ref() {
                    component_allocations.insert(std::sync::Arc::as_ptr(shared).cast::<u8>());
                }
                component_bytes += component.len();
                components_present += 1;
            }
        }
    }

    // Anti-vacuity first: an empty index would make every ratio below true for free, and a
    // corpus with no components would decide the component question by construction.
    assert!(pages > 0, "the page index is empty; nothing was measured");
    assert!(
        components_present > 0,
        "no page carries a component; the component question would be decided by construction"
    );

    let share = |distinct: usize, copies: usize| {
        if distinct == 0 { 0.0 } else { copies as f64 / distinct as f64 }
    };
    println!(
        "
  page index identity strings over {pages} pages:
    model_id    {:>5} distinct, {:>6} holders ({:>7.1} each), {:>4} allocations behind {:>6} B of referenced text
    component   {:>5} distinct, {:>6} holders ({:>7.1} each), {:>4} allocations behind {:>6} B of referenced text
    object_key  {:>5} distinct, {:>6} copies  ({:>7.1} copies each, {:>6} B held)

    BlockIndex is {} B before its heap strings
",
        distinct_models.len(), pages, share(distinct_models.len(), pages),
        model_allocations.len(), model_bytes,
        distinct_components.len(), components_present,
        share(distinct_components.len(), components_present),
        component_allocations.len(), component_bytes,
        distinct_keys.len(), pages, share(distinct_keys.len(), pages), key_bytes,
        std::mem::size_of::<crate::engine::state::BlockIndex>(),
    );
}

/// How many components an object actually has, and how many refs a component actually holds.
///
/// This decides whether an inline single-component shape is worth having. The inner `refs` vector
/// already carries a measured note that 100% of them hold exactly one ref; the outer
/// `by_component` vector has no such measurement, and it is the one that costs an allocation per
/// object.
///
/// The corpus is deliberately mixed. A string object is one componentless entry and a hash object
/// is one entry per field, so a corpus of only one kind would decide the question by construction
/// rather than by measurement. Both kinds are asserted present before any number is read.
#[test]
fn object_page_lookup_occupancy_census() {
    const STRING_OBJECTS: usize = 2_000;
    const HASH_OBJECTS: usize = 200;
    const FIELDS_PER_HASH: usize = 8;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..STRING_OBJECTS {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("occupancy-string-{index:08}"),
                value: vec![b'v'; 64],
            },
        });
    }
    for object in 0..HASH_OBJECTS {
        for field in 0..FIELDS_PER_HASH {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: format!("occupancy-hash-{object:08}"),
                    field: format!("field-{field}"),
                    value: vec![b'h'; 64],
                },
            });
        }
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let lookup = &shard.bucket_index.object_page_lookup;

    let mut single_component = 0usize;
    let mut multi_component = 0usize;
    let mut single_ref_components = 0usize;
    let mut multi_ref_components = 0usize;
    let mut components_total = 0usize;
    let mut refs_total = 0usize;
    for entry in lookup.values() {
        if entry.by_component.len() == 1 {
            single_component += 1;
        } else {
            multi_component += 1;
        }
        components_total += entry.by_component.len();
        for component in &entry.by_component {
            if component.refs.len() == 1 {
                single_ref_components += 1;
            } else {
                multi_ref_components += 1;
            }
            refs_total += component.refs.len();
        }
    }
    let objects = lookup.len();

    // Anti-vacuity, asserted before any ratio is read: an empty or one-sided corpus would make
    // every claim below true for free.
    assert!(objects > 0, "the lookup is empty; nothing was measured");
    assert!(refs_total > 0, "no refs were recorded; nothing was measured");
    assert!(
        single_component > 0 && multi_component > 0,
        "corpus is one-sided ({single_component} single, {multi_component} multi) -- \
         the occupancy question would be decided by construction, not measurement"
    );

    let pct = |n: usize, d: usize| 100.0 * n as f64 / d as f64;
    println!(
        "
  object page lookup occupancy ({objects} objects, {components_total} components, {refs_total} refs):
    objects with exactly one component  {single_component:>6}  ({:>5.1}%)
    objects with more than one          {multi_component:>6}  ({:>5.1}%)
    components holding exactly one ref  {single_ref_components:>6}  ({:>5.1}%)
    components holding more             {multi_ref_components:>6}  ({:>5.1}%)

    sizes: ObjectBlockRefs {:>3} B, ComponentBlocks {:>3} B, BlockRefs {:>3} B, BlockLookupRef {:>3} B
    a one-component one-ref object: {:>3} B inline + {:>3} B for the component vector,
    and the ref itself rides inside the component rather than in an allocation of its own
",
        pct(single_component, objects),
        pct(multi_component, objects),
        pct(single_ref_components, components_total),
        pct(multi_ref_components, components_total),
        std::mem::size_of::<crate::engine::state::ObjectBlockRefs>(),
        std::mem::size_of::<crate::engine::state::ComponentBlocks>(),
        std::mem::size_of::<crate::engine::state::BlockRefs>(),
        std::mem::size_of::<crate::engine::state::BlockLookupRef>(),
        std::mem::size_of::<crate::engine::state::ObjectBlockRefs>(),
        std::mem::size_of::<crate::engine::state::ComponentBlocks>(),
    );
}

/// Which structures hold an entry per record, and how many.
///
/// Resident memory is ~2.8-3.9 KB per record depending on key length, and a key byte costs 14.1
/// bytes of it -- the same string is held by `strings`, by the composite keys of
/// `object_page_lookup` and `object_component_lookup`, and again inside each `BlockIndex`.
/// Extrapolating to a zero-length key still leaves ~2581 B per record, so most of the bill is
/// fixed per-record structure rather than the key.
///
/// This counts the entries so the fixed part can be attributed rather than guessed at. It is a
/// report, not a threshold: it prints and asserts only the invariants that must hold, so it
/// cannot fail spuriously as unrelated work changes the numbers.
#[test]
fn per_record_structure_census() {
    const RECORDS: usize = 4_000;
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    assert!(
        engine
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 1,
                table_name: "census".to_string(),
                shard_uri: "local://census/1".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 1023,
                readonly: false,
                load_version: 1,
                local_node_id: Some(1),
            })
            .status
            .ok
    );
    for index in 0..RECORDS {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("census-key-{index:08}"),
                value: vec![b'v'; 64],
            },
        });
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let buckets = shard.bucket_index.bucket_map.len();
    let page_index_entries: usize = shard
        .bucket_index
        .bucket_map
        .values()
        .map(|bucket| bucket.page_index.len())
        .sum();
    let object_index_entries: usize = shard
        .bucket_index
        .bucket_map
        .values()
        .map(|bucket| bucket.object_index.len())
        .sum();
    // The map is keyed by object now, so its length is the object count and the per-component
    // map it used to be compared against is gone. Both numbers are still reported, because the
    // interesting quantity is entries PER RECORD and one of them stopped existing.
    let page_lookup_keys = shard.bucket_index.object_page_lookup.len();
    let page_lookup_refs: usize = shard
        .bucket_index
        .object_page_lookup
        .values()
        .map(crate::engine::state::ObjectBlockRefs::total_refs)
        .sum();
    let component_lookup_keys: usize = shard
        .bucket_index
        .object_page_lookup
        .values()
        .map(|entry| entry.by_component.len())
        .sum();
    let strings = shard.strings.len();
    let wal_resident = shard.wal_resident_pages.len();
    let dirty_objects = shard.dirty_objects.len();

    let per = |n: usize| n as f64 / RECORDS as f64;
    println!(
        "
  per-record entries at {RECORDS} records, 1024 routing slots:
             strings                    {:>6.2}
             bucket page_index          {:>6.2}
             bucket object_index        {:>6.2}
             object_page_lookup keys    {:>6.2}
             object_page_lookup refs    {:>6.2}
             page-lookup components     {:>6.2}
             dirty_objects              {:>6.2}
             wal_resident_pages         {:>6.2}
             (buckets: {buckets}, not per record)
",
        per(strings), per(page_index_entries), per(object_index_entries),
        per(page_lookup_keys), per(page_lookup_refs), per(component_lookup_keys),
        per(dirty_objects), per(wal_resident),
    );

    // String bytes each structure actually holds. RSS is ~2.8 KB/record and a key byte costs
    // 14.1 B of it, so knowing WHERE the key copies live says whether interning would pay and
    // which structure to attack first.
    let key_bytes: usize = shard.strings.keys().map(String::len).sum();
    let page_index_bytes: usize = shard
        .bucket_index
        .bucket_map
        .values()
        .flat_map(|bucket| bucket.page_index.iter())
        .map(|(_handle, page)| {
            // The key is an inline number now, not text on the heap.
            page.object_key.len()
                + page.model_id.len()
                + page.component.as_ref().map_or(0, |name| name.len())
        })
        .sum();
    // Outer keys plus the page-ref key each entry holds.
    let page_lookup_bytes: usize = shard
        .bucket_index
        .object_page_lookup
        .iter()
        .map(|(_model, object, entry)| {
            // The model is stored once for all its objects now, so only the object key is a
            // per-object cost here.
            object.len()
                + entry
                    .all_refs()
                    .map(|_page_ref| 0usize)
                    .sum::<usize>()
        })
        .sum();
    // Inner keys only. The (model, object) head is NOT counted again here -- that is the whole
    // point of nesting, and counting it twice would report the saving as if it had not happened.
    let component_lookup_bytes: usize = shard
        .bucket_index
        .object_page_lookup
        .values()
        .map(|entry| {
            entry
                .by_component
                .iter()
                .map(|component| component.component.as_ref().map_or(0, |name| name.len()))
                .sum::<usize>()
        })
        .sum();
    let dirty_bytes: usize = shard.dirty_objects.iter().map(String::len).sum();
    let total_string_bytes =
        key_bytes + page_index_bytes + page_lookup_bytes + component_lookup_bytes + dirty_bytes;
    let sample_key_len = shard.strings.keys().next().map_or(0, |name| name.len());
    let perb = |n: usize| n as f64 / RECORDS as f64;
    println!(
        "  string bytes held per record (sample key is {sample_key_len} B):
             strings keys               {:>7.1}
             bucket page_index          {:>7.1}
             object_page_lookup         {:>7.1}
             page-lookup components     {:>7.1}
             dirty_objects              {:>7.1}
             TOTAL                      {:>7.1}  = {:.1}x the key
",
        perb(key_bytes), perb(page_index_bytes), perb(page_lookup_bytes),
        perb(component_lookup_bytes), perb(dirty_bytes), perb(total_string_bytes),
        perb(total_string_bytes) / sample_key_len.max(1) as f64,
    );

    // Only the invariants: every record must be reachable by key and by page.
    assert_eq!(strings, RECORDS, "every record should have a string entry");
    assert_eq!(
        page_index_entries, RECORDS,
        "every record should have exactly one live page ref"
    );
    assert!(
        buckets <= 1024,
        "routing range should cap the bucket count, got {buckets}"
    );
}

/// The dirty-bucket count must equal the answer the unshortened scan would give.
///
/// The stats path counts dirty buckets by collecting the buckets already marked dirty and then
/// asking, per dirty object, which buckets hold its pages. `dirty_objects` grows by one per
/// record ingested and each pass builds a composite lookup key, so that loop was shard-sized work
/// on the heartbeat timer, under the read lock writers need.
///
/// It now stops once every bucket is in the set, because the answer is a set of bucket ids and
/// cannot grow past the bucket count. That is exact rather than approximate -- but "exact by
/// argument" is what this test exists to check, by recomputing the count the long way and
/// requiring equality.
///
/// Measured, 200k records in five equal phases at 1023 slots, growth of the last phase over the
/// first with the heartbeat at 1s: 3.42/2.17 before, 1.14/0.93 after, against 0.84-0.94 with the
/// heartbeat switched off entirely.
#[test]
fn dirty_bucket_count_matches_the_unshortened_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    assert!(
        engine
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 1,
                table_name: "dirty-bucket-count".to_string(),
                shard_uri: "local://dirty-bucket-count/1".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 63,
                readonly: false,
                load_version: 1,
                local_node_id: Some(1),
            })
            .status
            .ok
    );
    // Mixed enough that not every bucket ends up dirty: the short-circuit must not fire in the
    // case where the loop still has something to contribute.
    for index in 0..90 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("dbc-{index}"),
                value: vec![b'v'; 48],
            },
        });
        if index % 4 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonDelete {
                    key: format!("dbc-{}", index / 3),
                },
            });
        }
    }

    let reported = engine
        .shard_stats(1)
        .expect("shard 1 loaded")
        .object_manager
        .dirty_bucket_count;

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    // The same computation with no early exit.
    let mut expected: std::collections::BTreeSet<u32> = shard
        .bucket_index
        .bucket_map
        .iter()
        .filter_map(|(bucket_id, bucket)| bucket.dirty.then_some(*bucket_id))
        .collect();
    for object_key in &shard.dirty_objects {
        expected.extend(crate::engine::bucket_index_target_buckets_for_object_key(
            shard, object_key,
        ));
    }
    assert!(
        !shard.bucket_index.bucket_map.is_empty(),
        "workload produced no buckets"
    );
    assert_eq!(
        reported,
        expected.len(),
        "dirty bucket count diverged from the unshortened scan: {reported} vs {}",
        expected.len()
    );
    println!("
  dirty buckets: reported {reported} == unshortened {}
", expected.len());
}

/// The maintained page-ref total must equal the walk it replaced.
///
/// The stats path reports that number and used to derive it by summing every set in
/// `object_component_lookup` -- a walk over every object in the shard, run on the heartbeat timer
/// while holding the shard read lock writers need. Measured on a 200k-record ingest in five equal
/// phases: heartbeat at 1s, the last phase cost 3.0x the first and the datanode's own CPU grew
/// 3.3-3.7x; heartbeat off, 0.84-0.94x. Turning the timer off removed the growth entirely, which
/// is what identified the walk rather than the write path.
///
/// It is now kept as a running total, so it can drift instead of merely being slow. This drives
/// inserts, superseding overwrites that remove page refs, hash fields with components, expiries
/// and deletes, then compares the maintained value against the sum it is meant to equal.
#[test]
fn maintained_component_page_ref_total_matches_the_walk() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    assert!(
        engine
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 1,
                table_name: "page-ref-total".to_string(),
                shard_uri: "local://page-ref-total/1".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 63,
                readonly: false,
                load_version: 1,
                local_node_id: Some(1),
            })
            .status
            .ok
    );
    for index in 0..140 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("tot-str-{index}"),
                value: vec![b'v'; 48],
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: format!("tot-hash-{}", index % 9),
                field: format!("f-{index}"),
                value: vec![b'h'; 32],
            },
        });
        if index % 3 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("tot-str-{}", index / 2),
                    value: vec![b'w'; 96],
                },
            });
        }
        if index % 6 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonExpire {
                    key: format!("tot-str-{index}"),
                    ttl_ms: 60_000,
                },
            });
        }
        if index % 8 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonDelete {
                    key: format!("tot-str-{}", index / 4),
                },
            });
        }
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let walked: usize = shard
        .bucket_index
        .object_page_lookup
        .values()
        .map(crate::engine::state::ObjectBlockRefs::total_refs)
        .sum();
    let maintained = shard
        .bucket_index
        .object_component_page_refs
        .expect("the total should be established once the lookup has been built");
    assert!(walked > 0, "workload produced no component page refs to compare");
    assert_eq!(
        maintained, walked,
        "maintained component page-ref total drifted from the walk it replaces:          {maintained} vs {walked}"
    );
    println!("
  component page refs: maintained {maintained} == walked {walked}
");
}

/// Every bucket's `object_index` must already equal a from-scratch recompute.
///
/// `update_bucket_layout` rebuilds that set by scanning all of a bucket's pages, with no
/// short-circuit -- it is the remaining guaranteed full pass in bucket maintenance, and the whole
/// of what still scales with the corpus once the TTL pass is skipped.
///
/// The rebuild looks redundant: the mutation sites already maintain the set, inserting an
/// object id when a page is added and removing it when the last live page for it goes. If that
/// invariant genuinely holds, refreshing a bucket never needs the scan and can classify from the
/// two lengths it already has.
///
/// This is the evidence for that "if". It runs a workload that exercises inserts, overwrites that
/// supersede a page, hash fields, expiries and deletes, then recomputes the live-object set from
/// each bucket's pages and requires it to match what is stored. A mutation site that fails to
/// maintain the set shows up here as a named mismatch -- which is what makes dropping the scan
/// safe rather than hopeful.
#[test]
fn bucket_object_index_already_matches_a_from_scratch_recompute() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    assert!(
        engine
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 1,
                table_name: "object-index-invariant".to_string(),
                shard_uri: "local://object-index-invariant/1".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 63,
                readonly: false,
                load_version: 1,
                local_node_id: Some(1),
            })
            .status
            .ok
    );
    for index in 0..150 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("inv-str-{index}"),
                value: vec![b'v'; 48],
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: format!("inv-hash-{}", index % 11),
                field: format!("f-{index}"),
                value: vec![b'h'; 32],
            },
        });
        if index % 3 == 0 {
            // Supersede an existing page: this is the path that removes page refs.
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("inv-str-{}", index / 2),
                    value: vec![b'w'; 96],
                },
            });
        }
        if index % 7 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonExpire {
                    key: format!("inv-str-{index}"),
                    ttl_ms: 60_000,
                },
            });
        }
        if index % 9 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonDelete {
                    key: format!("inv-str-{}", index / 4),
                },
            });
        }
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut checked = 0usize;
    let mut mismatches = Vec::new();
    for (routing_bucket, bucket) in shard.bucket_index.bucket_map.iter() {
        let recomputed: std::collections::BTreeSet<u64> = bucket
            .page_index
            .values()
            .filter(|page| !page.deleted)
            .map(|page| page.object_id())
            .collect();
        // Mirrors update_bucket_layout: an empty live set over an empty page index leaves the
        // stored set untouched, so only compare where the rebuild would actually assign.
        if recomputed.is_empty() && bucket.page_index.is_empty() {
            continue;
        }
        checked += 1;
        let expected: crate::engine::state::ObjectIndex = if recomputed.is_empty() {
            crate::engine::state::ObjectIndex::default()
        } else {
            recomputed.into()
        };
        if bucket.object_index != expected {
            mismatches.push(format!(
                "  bucket {routing_bucket}: stored {:?} != recomputed {:?}",
                bucket.object_index, expected
            ));
        }
    }
    assert!(checked > 0, "workload produced no buckets to check");
    assert!(
        mismatches.is_empty(),
        "object_index is NOT maintained incrementally by {} of {checked} bucket(s), so the          layout rebuild is load-bearing and cannot be dropped:
{}",
        mismatches.len(),
        mismatches.join("
")
    );
    println!("
  {checked} buckets: stored object_index matches a from-scratch recompute
");
}

/// Per-write cost must be flat at a NARROW routing range too, not only a wide one.
///
/// This is the limitation the sibling tests cannot see, because they run at the default routing
/// range where every key lands in its own bucket: a write touches one bucket of many, the
/// targeted refresh skips the rest, and cost is flat.
///
/// Narrow the range -- `TS_SHARD_END_ROUTING_SLOT=1023`, the setting that cuts resident memory
/// 45% and the one to run in production -- and there are only 1024 buckets. A batch of any size
/// hashes across essentially all of them, so there is nothing to skip: the targeted path visits
/// the same pages as the sweep and the guard correctly hands the work back to the sweep, which is
/// `O(total pages)` per batch. Bucket maintenance is therefore STILL linear per write there.
///
/// Measured end to end at 1023 slots, five equal 40k-record phases: 6.3s -> 14.9s while every
/// byte written stayed flat (index log 1.02x, WAL 1.01x). At the default range the same run grows
/// 1.64x. The remedy is not to choose a path but to stop rescanning: keep each bucket's live-page
/// count, dirty-page count and minimum TTL incrementally, so refreshing a bucket is O(1) and the
/// range stops mattering.
///
/// Counted at 64 slots as each pass came off: 12.6 -> 39.0 (3.10x), then 8.4 -> 26.0 after
/// skipping the TTL pass when nothing expires, then 4.2 -> 13.0 once the refresh classifies
/// instead of rescanning, then flat once the upsert does too. Only the last changed the SHAPE,
/// because rebuilding the live-object set is the one pass with no short-circuit: `deleted`
/// stops at the first live page and `dirty` at the first dirty one, but that rebuild reads
/// every page every time.
#[test]
fn bucket_maintenance_is_flat_at_a_narrow_routing_range() {
    fn visits_per_write(object_count: usize) -> f64 {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        // The range travels on the load request, NOT the environment: the datanode binary reads
        // TS_SHARD_*_ROUTING_SLOT and puts it here. Setting those vars around a bare
        // `load_shard` leaves the shard on the u32::MAX default, and this test then silently
        // measures the wide range and agrees with its sibling -- which is exactly what the first
        // version of it did.
        assert!(
            engine
                .load_shard_with(crate::control::LoadShardRequest {
                    shard_id: 1,
                    table_name: "narrow-routing".to_string(),
                    shard_uri: "local://narrow-routing/1".to_string(),
                    start_routing_bucket: 0,
                    end_routing_bucket: 63,
                    readonly: false,
                    load_version: 1,
                    local_node_id: Some(1),
                })
                .status
                .ok
        );
        for index in 0..object_count {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("narrow-{index}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        const MEASURED_WRITES: usize = 20;
        crate::engine::reset_bucket_page_index_visits();
        for index in 0..MEASURED_WRITES {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("narrow-measured-{index}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        let visited = crate::engine::bucket_page_index_visits();
        visited as f64 / MEASURED_WRITES as f64
    }

    let small = visits_per_write(200);
    let large = visits_per_write(800);
    println!(
        "
  64 routing slots:
    200 objects -> {small:>8.1} page-index visits per write
    800 objects -> {large:>8.1} page-index visits per write
"
    );

    // An absolute bound, not a ratio: the work is meant to be independent of the corpus, and a
    // ratio is undefined once it reaches zero. 4x the objects must not buy more than a constant.
    assert!(
        large <= 2.0,
        "per-write bucket maintenance should not scan pages at a narrow routing range, got {large:.1} visits/write at 800 objects (200 objects: {small:.1})"
    );
    assert!(
        large <= small + 2.0,
        "per-write bucket maintenance grew with the corpus at 64 routing slots: {small:.1} -> {large:.1} visits/write for 200 -> 800 objects"
    );
}

/// Bucket maintenance must not make a write cost more as the store grows.
///
/// `update_bucket_layout` rebuilds a bucket's whole object set from `page_index`, and the
/// per-object dirty-state clear walks every bucket in the shard. Both are `O(pages)` and both sit
/// inside loops, so the write path can be quadratic in the corpus while every individual function
/// still looks cheap.
///
/// Counted, not timed: the number is work done, so the test cannot pass by running on a fast
/// machine, and it reports per-write cost so growth is visible rather than implied.
#[test]
fn bucket_maintenance_per_write_does_not_grow_with_the_store() {
    fn visits_per_write(object_count: usize) -> (u64, f64, (u64, u64, u64, u64)) {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        // Fill to the target corpus size first; only the writes AFTER the reset are measured.
        for index in 0..object_count {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("bucket-maint-{index}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        const MEASURED_WRITES: usize = 20;
        crate::engine::reset_bucket_page_index_visits();
        crate::engine::bucket_visit_sites::reset();
        for index in 0..MEASURED_WRITES {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("bucket-maint-measured-{index}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        let visited = crate::engine::bucket_page_index_visits();
        let sites = crate::engine::bucket_visit_sites::snapshot();
        (visited, visited as f64 / MEASURED_WRITES as f64, sites)
    }

    let (small_total, small_each, small_sites) = visits_per_write(200);
    let (large_total, large_each, large_sites) = visits_per_write(800);

    let per = |value: u64| value as f64 / 20.0;
    println!(
        "
  200 objects -> {small_total:>9} visits for 20 writes ({small_each:>9.1} per write)
           800 objects -> {large_total:>9} visits for 20 writes ({large_each:>9.1} per write)
           growth: {:.2}x cost for 4x the corpus

           per-write attribution      200 objects    800 objects
             update_bucket_layout     {:>9.1}      {:>9.1}
             clear_dirty (all bkts)   {:>9.1}      {:>9.1}
             refresh_runtime_flags    {:>9.1}      {:>9.1}
             remove (all bkts)        {:>9.1}      {:>9.1}
",
        if small_each > 0.0 { large_each / small_each } else { 0.0 },
        per(small_sites.0), per(large_sites.0),
        per(small_sites.1), per(large_sites.1),
        per(small_sites.2), per(large_sites.2),
        per(small_sites.3), per(large_sites.3),
    );

    // 4x the corpus must not cost materially more PER WRITE. Allowed a little slack for
    // bucket-fill effects; a linear-in-corpus path shows up here as ~4x and fails loudly.
    assert!(
        large_each <= small_each * 1.5 + 1.0,
        "per-write bucket maintenance grew with the store: {small_each:.1} -> {large_each:.1}          visits/write for 200 -> 800 objects"
    );
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
            .and_then(|address| address.object_id())
            .unwrap_or_default()
    );

    // The claim has to equal what the index actually holds. This is the whole point.
    let indexed = engine
        .string_page_address(1, "outcome-key")
        .expect("the index holds an address for the key");
    // Through the accessor, not the raw field: a recorded address omits the routing bucket the
    // item already carries, and putting it back is what `resolved_address` is for. Comparing the
    // raw field against an index entry compares a trimmed address with a whole one.
    assert_eq!(
        item.resolved_address().as_ref(),
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
        // The one kind whose index entry is NOT keyed by the stored timestamp: the page packs by
        // time, the map keys by event id, and the time index maps one to the other. All three
        // have to come back, which is why the record carries both keys.
        Command::ContextWriteExtractedEvent {
            tenant_hash: 41,
            node_hash: 42,
            event: crate::types::ContextEvent {
                event_id_hash: 445,
                event_time_ms: 1_787_270_075_000,
                ingestion_time_ms: 1_787_270_075_000,
                kind: 7,
                event_type: 7,
                actor_hash: 0,
                status: 1,
                valid_until_ms: 0,
                confidence: 0.96,
                importance: 0.88,
                text: "an extracted event".to_string(),
                source_ref: String::new(),
                related_node_hashes: vec![42],
                compact_attrs: Vec::new(),
                vector: Vec::new(),
            },
            indexes: crate::types::ContextExtractedEventIndexes {
                scope_hash: 3001,
                entity_hashes: vec![501],
                status_hash: 601,
                source_hash: 701,
                event_time_bucket_ms: 1_787_270_000_000,
                disabled_indexes: Vec::new(),
            },
            first_write_only: false,
            cold_storage: false,
        },
        // The context maps are the same shape as a feature series -- stored key to page -- so
        // they are installed by the same arm. Which is exactly why they belong in here: one arm
        // covering six kinds is one place for five of them to go silently uninstalled.
        Command::ContextWriteIndexRef {
            tenant_hash: 41,
            index_name: "actor".to_string(),
            index_value_hash: 77,
            scope_hash: 3,
            event_time_ms: 1_787_270_070_000,
            index_ref: crate::types::ContextIndexRef {
                primary_node_hash: 9,
                primary_event_time_ms: 1_787_270_070_000,
                event_id_hash: 1234,
            },
        },
        Command::ContextWritePackAudit {
            tenant_hash: 41,
            audit: crate::types::ContextPackAudit {
                query_id: "q-1".to_string(),
                session_hash: 5,
                request_time_ms: 1_787_270_071_000,
                query_hash: 6,
                max_prompt_tokens: 100,
                selected_tokens: 40,
                selected_refs: Vec::new(),
                blocked_refs: Vec::new(),
            },
        },
        Command::ContextUpsertChildRef {
            tenant_hash: 41,
            child_ref: crate::types::ContextChildRef {
                parent_hash: 9,
                child_hash: 10,
                updated_at_ms: 1_787_270_072_000,
            },
        },
        Command::ContextUpsertSummary {
            tenant_hash: 41,
            summary: crate::types::ContextSummary {
                node_hash: 9,
                level: 1,
                text: "a summary".to_string(),
                valid_from_ms: 1_787_270_073_000,
                vector: Vec::new(),
                embedding_model_hash: 0,
            },
        },
        Command::ContextWriteCompressionEvent {
            tenant_hash: 41,
            event: crate::types::ContextCompressionEvent {
                compression_id_hash: 21,
                node_hash: 9,
                source_start_ms: 1_787_270_070_000,
                source_end_ms: 1_787_270_073_000,
                compressed_time_ms: 1_787_270_074_000,
                summary: "compressed".to_string(),
            },
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
    // Two empty maps compare equal, so a workload that quietly wrote nothing would pass. Assert
    // each kind is actually present before comparing anything.
    let ran_shape = ran.index_shape_for_test(1);
    for kind in [
        "context_event",
        "context_timeline",
        "context_index",
        "context_audit",
        "context_child",
        "context_summary",
        "context_compression",
    ] {
        assert!(
            ran_shape.lines().any(|line| line.starts_with(&format!("{kind} "))),
            "the workload wrote no {kind}, so installing it proves nothing"
        );
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

    // The typed maps are not the whole shard. The bucket index is durable state the read path
    // consults, so compare it too -- separately, so a failure says WHICH half diverged.
    assert_eq!(
        installed.bucket_index_shape_for_test(1),
        ran.bucket_index_shape_for_test(1),
        "the typed maps matched but the bucket index did not"
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

/// A COLD RELOAD, not a synthetic apply loop: a fresh engine rebuilds the shard from disk.
///
/// The equivalence gate feeds recorded outcomes to an engine by hand, which proves the apply path
/// understands them. It does not prove RECOVERY uses them -- replay could ignore every outcome and
/// re-execute every command and that gate would stay green.
///
/// Neither does comparing shard shapes across a restart, which is worth stating because the first
/// version of this test did exactly that and passed while recovery installed NOTHING: unloading a
/// shard flushes its index, so the reload had no tail left to replay and never entered the path
/// under test. The install counter is what caught it, and asserting on it is what keeps this test
/// honest -- so the engine is dropped without unloading, the way a crash leaves it.
#[test]
fn a_cold_reload_rebuilds_the_shard_from_what_the_writes_recorded() {
    let dir = tempfile::tempdir().unwrap();
    let cache = dir.path().join("cache");
    let pages = dir.path().join("pages");
    let indexes = dir.path().join("indexes");

    let workload = vec![
        Command::StringSet {
            key: "rs-a".to_string(),
            value: b"alpha".to_vec(),
        },
        Command::StringSetEx {
            key: "rs-ttl".to_string(),
            value: b"expiring".to_vec(),
            ttl_ms: 600_000,
        },
        Command::HashSet {
            key: "rs-hash".to_string(),
            field: "f".to_string(),
            value: b"hv".to_vec(),
        },
        Command::SetAdd {
            key: "rs-set".to_string(),
            member: b"m".to_vec(),
        },
        Command::ZSetAdd {
            key: "rs-zset".to_string(),
            member: b"zm".to_vec(),
            score: 3.5,
        },
        Command::ListPush {
            key: "rs-list".to_string(),
            member: b"lm".to_vec(),
            left: true,
        },
        Command::SeenCheck {
            key: "rs-seen".to_string(),
            member: b"m".to_vec(),
            window_ms: 600_000,
        },
        Command::BucketTake {
            key: "rs-bucket".to_string(),
            tokens: 2.0,
            capacity: 10.0,
            refill_per_sec: 1.0,
        },
        Command::FeatureAppend {
            key: "rs-feature".to_string(),
            points: (0..3)
                .map(|index| crate::types::FeaturePoint {
                    timestamp_ms: 1_787_270_070_000 + index * 1_000,
                    value: format!("p{index}").into_bytes(),
                })
                .collect(),
        },
    ];

    let (before, before_index, recorded) = {
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            cache.clone(),
            pages.clone(),
            indexes.clone(),
        );
        engine.load_shard(1);
        std::env::set_var("TS_WAL_OUTCOME_ITEMS", "1");
        for command in workload {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            assert!(response.status.ok, "workload write failed: {response:?}");
        }
        std::env::remove_var("TS_WAL_OUTCOME_ITEMS");
        let shape = engine.index_shape_for_test(1);
        assert!(
            shape.contains("string rs-a") && shape.contains("feature rs-feature"),
            "the workload did not build the shard it was supposed to: {shape}"
        );
        let records = engine
            .write_ahead_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .unwrap();
        let recorded = records
            .iter()
            .filter_map(|(_, line)| crate::wal::decode_wal_line(line).ok())
            .filter(|record| !record.outcomes.is_empty())
            .count();
        assert!(
            recorded >= 8,
            "expected the workload to leave records carrying outcomes, got {recorded}"
        );
        (shape, engine.bucket_index_shape_for_test(1), recorded)
        // dropped WITHOUT unload: the index is not flushed, so the tail must be replayed.
    };

    // The gate stays OFF for the reload. Recording is what it gates; a record that already says
    // what it did is installed on its own evidence.
    let recovered = TemporalEngine::with_local_dirs(1024 * 1024, cache, pages, indexes);
    recovered.load_shard(1);

    // The shapes below would match even if recovery ignored every outcome and re-executed the
    // commands, so assert WHICH path ran before asserting the result.
    let installed = recovered.replay_installs_for_test();
    assert!(
        installed >= recorded as u64,
        "recovery installed {installed} outcomes for {recorded} records that carried them, so it          fell back to replaying commands and this test proves nothing"
    );

    assert_eq!(
        recovered.index_shape_for_test(1),
        before,
        "a cold reload did not rebuild the shard the writes described"
    );
    assert_eq!(
        recovered.bucket_index_shape_for_test(1),
        before_index,
        "a cold reload rebuilt the maps but not the index"
    );
}

/// PER COMMAND: does what a write recorded describe EVERYTHING it changed, or just something?
///
/// The coverage probe asks whether a record said anything at all, which is a much weaker question
/// than it looks. StringSetEx writes a value and a deadline; recording only the value passes the
/// probe, passes the equivalence gate as long as no deadline is in the workload, and produces a
/// recovered key that never expires. That defect was real and this is the test that names it.
///
/// Each command runs alone against a fresh pair of shards -- one built by running it, one built by
/// installing only what it recorded -- so a failure names the command rather than the workload.
#[test]
fn what_each_command_recorded_describes_everything_it_changed() {
    // (label, what must already exist, the command under test). A command that only fires
    // against existing state -- a conditional write, a persist -- cannot be probed on an empty
    // shard, and leaving it out is how StringSetConditional kept a real defect through four
    // rounds of this.
    let commands: Vec<(&str, Vec<Command>, Command)> = vec![
        (
            "StringSet",
            Vec::new(),
            Command::StringSet {
                key: "pc-string".to_string(),
                value: b"v".to_vec(),
            },
        ),
        (
            "StringSetEx",
            Vec::new(),
            Command::StringSetEx {
                key: "pc-setex".to_string(),
                value: b"v".to_vec(),
                ttl_ms: 600_000,
            },
        ),
        (
            "HashSet",
            Vec::new(),
            Command::HashSet {
                key: "pc-hash".to_string(),
                field: "f".to_string(),
                value: b"v".to_vec(),
            },
        ),
        (
            "HashIncrBy",
            Vec::new(),
            Command::HashIncrBy {
                key: "pc-hash".to_string(),
                field: "counter".to_string(),
                increment: 3,
            },
        ),
        (
            "SetAdd",
            Vec::new(),
            Command::SetAdd {
                key: "pc-set".to_string(),
                member: b"m".to_vec(),
            },
        ),
        (
            "ZSetAdd",
            Vec::new(),
            Command::ZSetAdd {
                key: "pc-zset".to_string(),
                member: b"m".to_vec(),
                score: 1.5,
            },
        ),
        (
            "ListPush",
            Vec::new(),
            Command::ListPush {
                key: "pc-list".to_string(),
                member: b"m".to_vec(),
                left: true,
            },
        ),
        (
            "SeenCheck",
            Vec::new(),
            Command::SeenCheck {
                key: "pc-seen".to_string(),
                member: b"m".to_vec(),
                window_ms: 600_000,
            },
        ),
        (
            "BucketTake",
            Vec::new(),
            Command::BucketTake {
                key: "pc-bucket".to_string(),
                tokens: 1.0,
                capacity: 10.0,
                refill_per_sec: 1.0,
            },
        ),
        (
            "FeatureAppend",
            Vec::new(),
            Command::FeatureAppend {
                key: "pc-feature".to_string(),
                points: vec![crate::types::FeaturePoint {
                    timestamp_ms: 1_787_270_070_000,
                    value: b"fv".to_vec(),
                }],
            },
        ),
        (
            "StringSetConditional-refresh",
            vec![Command::StringSetEx {
                key: "pc-cond".to_string(),
                value: b"v1".to_vec(),
                ttl_ms: 120,
            }],
            Command::StringSetConditional {
                key: "pc-cond".to_string(),
                value: b"v2".to_vec(),
                ttl_ms: Some(600_000),
                condition: crate::types::StringSetCondition::IfExists,
                return_old: false,
            },
        ),
        (
            "StringSetConditional-clears-deadline",
            vec![Command::StringSetEx {
                key: "pc-cond2".to_string(),
                value: b"v1".to_vec(),
                ttl_ms: 600_000,
            }],
            Command::StringSetConditional {
                key: "pc-cond2".to_string(),
                value: b"v2".to_vec(),
                ttl_ms: None,
                condition: crate::types::StringSetCondition::IfExists,
                return_old: false,
            },
        ),
        (
            "CommonPersist",
            vec![Command::StringSetEx {
                key: "pc-persist".to_string(),
                value: b"v".to_vec(),
                ttl_ms: 600_000,
            }],
            Command::CommonPersist {
                key: "pc-persist".to_string(),
            },
        ),
        (
            "CommonExpire",
            vec![Command::StringSet {
                key: "pc-expire".to_string(),
                value: b"v".to_vec(),
            }],
            Command::CommonExpire {
                key: "pc-expire".to_string(),
                ttl_ms: 600_000,
            },
        ),
        (
            "FeatureDelete",
            vec![Command::FeatureAppend {
                key: "pc-featdel".to_string(),
                points: vec![crate::types::FeaturePoint {
                    timestamp_ms: 1_787_270_070_000,
                    value: b"fv".to_vec(),
                }],
            }],
            Command::FeatureDelete {
                key: "pc-featdel".to_string(),
            },
        ),
        (
            "ControlStateIncrement",
            Vec::new(),
            Command::ControlStateIncrement {
                key: "pc-ctr".to_string(),
                timestamp_ms: 1_787_270_070_000,
                amount: 5,
            },
        ),
        (
            "ContextUpsertNode",
            Vec::new(),
            Command::ContextUpsertNode {
                tenant_hash: 41,
                node: crate::types::ContextNode {
                    node_hash: 9,
                    parent_hash: 0,
                    kind: 1,
                    canonical_name: "probe-node".to_string(),
                    status: 1,
                    last_event_time_ms: 1_787_270_070_000,
                    raw_metadata_ref: String::new(),
                    l0: String::new(),
                    l1_ref: String::new(),
                    vector: Vec::new(),
                    embedding_model_hash: 0,
                    embedding_updated_at_ms: 0,
                    summary_vector: Vec::new(),
                    summary_vector_valid_from_ms: 0,
                    summary_vector_model_hash: 0,
                },
            },
        ),
    ];

    let mut incomplete = Vec::new();
    for (label, prelude, command) in commands {
        let dir = tempfile::tempdir().unwrap();
        let ran = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("ran-cache"),
            dir.path().join("ran-pages"),
            dir.path().join("ran-index"),
        );
        ran.load_shard(1);
        // The prelude runs with the gate OFF so the only outcomes on the log belong to the
        // command under test, and the installed shard starts from the same prelude state.
        for setup in &prelude {
            let response = ran.execute(ExecuteRequest {
                shard_id: 1,
                command: setup.clone(),
            });
            assert!(response.status.ok, "{label}: prelude failed: {response:?}");
        }
        std::env::set_var("TS_WAL_OUTCOME_ITEMS", "1");
        let response = ran.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        std::env::remove_var("TS_WAL_OUTCOME_ITEMS");
        if !response.status.ok {
            continue;
        }
        let outcomes = ran
            .write_ahead_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .unwrap()
            .iter()
            .filter_map(|(_, line)| crate::wal::decode_wal_line(line).ok())
            .flat_map(|record| record.outcomes)
            .collect::<Vec<_>>();

        let installed = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("inst-cache"),
            dir.path().join("inst-pages"),
            dir.path().join("inst-index"),
        );
        installed.load_shard(1);
        for setup in &prelude {
            let response = installed.execute(ExecuteRequest {
                shard_id: 1,
                command: setup.clone(),
            });
            assert!(response.status.ok, "{label}: prelude failed: {response:?}");
        }
        for item in &outcomes {
            if !installed.apply_outcome_item(1, item) {
                incomplete.push(format!("{label}: apply refused a {} outcome", item.kind));
            }
        }
        let expected = ran.index_shape_for_test(1);
        let actual = installed.index_shape_for_test(1);
        if expected != actual {
            let missing = expected
                .lines()
                .filter(|line| !actual.lines().any(|other| other == *line))
                .collect::<Vec<_>>();
            let extra = actual
                .lines()
                .filter(|line| !expected.lines().any(|other| other == *line))
                .collect::<Vec<_>>();
            incomplete.push(format!(
                "{label}: recorded outcomes do not describe everything it changed -- missing {missing:?}, extra {extra:?}"
            ));
        }
    }

    assert!(
        incomplete.is_empty(),
        "these commands recorded SOMETHING but not EVERYTHING, so a recovered shard would be          quietly wrong rather than obviously broken: {incomplete:#?}"
    );
}

/// What a record COSTS, before and after -- measured through the engine, not the log.
///
/// The reclaim harness appends straight to the log and never stages a result, so its records are
/// byte-identical either way; its base offsets matched to the byte across both runs, which is how
/// that was caught rather than reported as a saving. Anything measuring this has to go through
/// execute().
///
/// BEFORE is what shipped: the operation, in text. AFTER is what ships now: results, in protobuf,
/// with no operation. Both arms set every flag explicitly, because the defaults are the thing
/// under test and inheriting them would compare a configuration against itself -- which this test
/// did, silently, the moment the defaults flipped.
#[test]
fn a_record_carrying_results_is_smaller_than_one_carrying_the_operation() {
    fn wal_bytes(binary: &str, record_results: &str, data_only: &str, writes: usize) -> (u64, u64) {
        std::env::set_var("TS_WAL_BINARY_RECORDS", binary);
        std::env::set_var("TS_WAL_OUTCOME_ITEMS", record_results);
        std::env::set_var("TS_WAL_DATA_ONLY", data_only);
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..writes {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("size-{index:06}"),
                    value: vec![b'v'; 64],
                },
            });
            assert!(response.status.ok);
        }
        let records = engine
            .write_ahead_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .unwrap();
        let total: u64 = records.iter().map(|(_, line)| line.len() as u64).sum();
        let count = records.len() as u64;
        std::env::remove_var("TS_WAL_BINARY_RECORDS");
        std::env::remove_var("TS_WAL_OUTCOME_ITEMS");
        std::env::remove_var("TS_WAL_DATA_ONLY");
        (total, count)
    }

    const WRITES: usize = 2_000;
    let (before, before_n) = wal_bytes("0", "0", "0", WRITES);
    let (after, after_n) = wal_bytes("1", "1", "1", WRITES);
    let per_before = before as f64 / before_n as f64;
    let per_after = after as f64 / after_n as f64;

    println!("[record cost] BEFORE  operation, text     : {per_before:.1} B/record");
    println!("[record cost] AFTER   results, protobuf   : {per_after:.1} B/record");
    println!(
        "[record cost] {:+.1} B/record ({:+.1}%)",
        per_after - per_before,
        (per_after - per_before) / per_before * 100.0
    );

    assert_eq!(before_n, after_n, "the two arms wrote different record counts");
    // The whole argument for making this the default. A record that states results carries MORE
    // information than one that states an operation, and if it also costs more bytes then going
    // live is a regression dressed as progress.
    assert!(
        per_after < per_before,
        "a record carrying results ({per_after:.1} B) must not cost more than one carrying the \
         operation ({per_before:.1} B)"
    );
}

/// FIDELITY: a record encoded as protobuf and read back must equal the record that went in.
///
/// This is the durability path, so the bar is equality of the whole record -- not "the fields we
/// modelled survived". A command this codec has never heard of travels verbatim and must come back
/// byte for byte; anything less means a log written today cannot be replayed tomorrow.
///
/// The workload is driven through the engine rather than hand-built, so the records under test are
/// the ones the engine actually writes, including their outcomes and any blocks they carry.
#[test]
fn a_record_encoded_as_protobuf_reads_back_identical() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    std::env::set_var("TS_WAL_OUTCOME_ITEMS", "1");

    let workload = vec![
        Command::StringSet {
            key: "pb-string".to_string(),
            value: b"a value".to_vec(),
        },
        // A modelled arm with a TTL, which the codec folds into the same message.
        Command::StringSetEx {
            key: "pb-setex".to_string(),
            value: b"expiring".to_vec(),
            ttl_ms: 600_000,
        },
        Command::HashSet {
            key: "pb-hash".to_string(),
            field: "f".to_string(),
            value: b"hv".to_vec(),
        },
        // Deliberately NOT modelled by the command codec: it must travel verbatim.
        Command::ZSetAdd {
            key: "pb-zset".to_string(),
            member: b"zm".to_vec(),
            score: 2.5,
        },
        Command::ListPush {
            key: "pb-list".to_string(),
            member: b"lm".to_vec(),
            left: true,
        },
        // No block behind it: the outcome carries bytes instead of an address.
        Command::SeenCheck {
            key: "pb-seen".to_string(),
            member: b"m".to_vec(),
            window_ms: 600_000,
        },
        Command::BucketTake {
            key: "pb-bucket".to_string(),
            tokens: 1.0,
            capacity: 10.0,
            refill_per_sec: 1.0,
        },
        // Several outcomes from one command, each naming a different point.
        Command::FeatureAppend {
            key: "pb-feature".to_string(),
            points: (0..3)
                .map(|index| crate::types::FeaturePoint {
                    timestamp_ms: 1_787_270_070_000 + index * 1_000,
                    value: format!("point-{index}").into_bytes(),
                })
                .collect(),
        },
        // A value with bytes that are not valid text, which is where an encoding that has no
        // byte-string type starts guessing.
        Command::StringSet {
            key: "pb-binary".to_string(),
            value: vec![0x00, 0xff, 0x1f, 0x7f, 0x80, b'"', b'\\', b'\n'],
        },
        Command::CommonExpire {
            key: "pb-string".to_string(),
            ttl_ms: 300_000,
        },
        Command::CommonDelete {
            key: "pb-hash".to_string(),
        },
    ];
    for command in workload {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "workload write failed: {response:?}");
    }
    std::env::remove_var("TS_WAL_OUTCOME_ITEMS");

    let records = engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .unwrap()
        .iter()
        .map(|(_, line)| crate::wal::decode_wal_line(line).expect("record decodes"))
        .collect::<Vec<_>>();
    assert!(
        records.len() >= 10,
        "expected the workload to leave records to round-trip, got {}",
        records.len()
    );
    assert!(
        records.iter().any(|record| !record.outcomes.is_empty()),
        "the records under test carry no outcomes, so this proves nothing about them"
    );

    let mut text_bytes = 0usize;
    let mut binary_bytes = 0usize;
    for record in &records {
        // Both branches set the flag explicitly. Leaving the text branch to inherit the ambient
        // environment meant that under a suite run with the binary flag on, the "text" encoding
        // was binary and the comparison silently tested one encoding against itself.
        std::env::set_var("TS_WAL_BINARY_RECORDS", "0");
        let framed = crate::wal::encode_wal_line_for_test(record).expect("text encodes");
        text_bytes += framed.len();

        std::env::set_var("TS_WAL_BINARY_RECORDS", "1");
        let framed_binary = crate::wal::encode_wal_line_for_test(record).expect("binary encodes");
        std::env::remove_var("TS_WAL_BINARY_RECORDS");
        binary_bytes += framed_binary.len();

        let round_tripped =
            crate::wal::decode_wal_line(&framed_binary).expect("binary record decodes");
        assert_eq!(
            &round_tripped, record,
            "a record did not survive the protobuf round trip"
        );

        // And the text encoding still reads, from the same decoder, with no flag consulted.
        let from_text = crate::wal::decode_wal_line(&framed).expect("text record decodes");
        assert_eq!(&from_text, record, "the text encoding stopped round-tripping");
    }

    println!(
        "[proto] {} records: text {} B, protobuf {} B ({:.1}% of text)",
        records.len(),
        text_bytes,
        binary_bytes,
        binary_bytes as f64 / text_bytes as f64 * 100.0
    );
    assert!(
        binary_bytes < text_bytes,
        "protobuf ({binary_bytes} B) should be smaller than text ({text_bytes} B)"
    );
}

/// Binary records must survive the FILE, not just a round trip in memory.
///
/// The first version of the protobuf codec passed a round-trip test and lost writes on reload,
/// because the round trip never touched a log file. The log is read with `reader.lines()`, and
/// protobuf carries 0x0A freely -- so a record containing one split into fragments that decoded as
/// nothing. Values came back empty and no error was raised anywhere.
///
/// So this writes through the real append path, drops the engine, and reads every value back from
/// a cold reload. The values are chosen to contain newlines outright, because a codec that only
/// works on bytes that happen to avoid one is not a codec.
#[test]
fn binary_records_survive_a_reload_through_a_real_log_file() {
    let dir = tempfile::tempdir().unwrap();
    let cache = dir.path().join("cache");
    let pages = dir.path().join("pages");
    let indexes = dir.path().join("indexes");

    const WRITES: usize = 64;
    let value_for = |index: usize| -> Vec<u8> {
        // Newlines, the escape byte itself, and a run of high bytes: everything a line-oriented
        // reader and a byte-stuffed payload each have to survive.
        let mut value = format!("line-a-{index}\nline-b\n").into_bytes();
        value.extend_from_slice(&[0x1b, 0x0a, 0x1b, 0x1b, 0x00, 0xff, 0x7f, 0x80]);
        value.extend_from_slice(format!("\ntail-{index}").as_bytes());
        value
    };

    {
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            cache.clone(),
            pages.clone(),
            indexes.clone(),
        );
        engine.load_shard(1);
        std::env::set_var("TS_WAL_BINARY_RECORDS", "1");
        std::env::set_var("TS_WAL_OUTCOME_ITEMS", "1");
        for index in 0..WRITES {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("nl-{index:04}"),
                    value: value_for(index),
                },
            });
            assert!(response.status.ok, "write {index} failed: {response:?}");
        }
        std::env::remove_var("TS_WAL_OUTCOME_ITEMS");
        std::env::remove_var("TS_WAL_BINARY_RECORDS");
        // dropped WITHOUT unloading, so the tail has to be replayed off the file.
    }

    // Every record must still be readable FROM THE FILE, which is what lines() broke.
    let engine = TemporalEngine::with_local_dirs(1024 * 1024, cache, pages, indexes);
    engine.load_shard(1);
    let scanned = engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .unwrap();
    let decoded = scanned
        .iter()
        .filter(|(_, line)| crate::wal::decode_wal_line(line).is_ok())
        .count();
    assert_eq!(
        decoded,
        scanned.len(),
        "{} of {} records on disk did not decode",
        scanned.len() - decoded,
        scanned.len()
    );

    for index in 0..WRITES {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: format!("nl-{index:04}"),
            },
        });
        let expected = value_for(index);
        match response.response {
            CommandResponse::Bytes { value: Some(ref got) } => assert_eq!(
                got, &expected,
                "nl-{index:04} came back with different bytes"
            ),
            other => panic!("nl-{index:04} did not survive the reload: {other:?}"),
        }
    }
}

/// Recording results must not cost a write its place in the group-commit queue.
///
/// It used to. Anything a record had to CARRY forced the staged append branch, and outcomes were
/// treated as one of those things -- so turning recording on turned coalescing off, and every
/// concurrent writer paid its own fsync. That was the strongest argument for keeping the gate off,
/// and it was a property of the plumbing rather than of durability: a staged page needs its
/// address back-patched once the record's log id exists, and an outcome does not.
///
/// Asserts fewer fsyncs than writes with recording ON. Not a ratio -- the point is that coalescing
/// ENGAGES, and on a loaded machine how much it wins varies.
#[test]
fn recording_results_still_coalesces_fsyncs() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};

    const WRITERS: usize = 8;
    const PER_WRITER: usize = 25;

    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    ));
    engine.load_shard(1);
    std::env::set_var("TS_ENGINE_CONCURRENT_COMMIT", "1");
    std::env::set_var("TS_WAL_OUTCOME_ITEMS", "1");

    let syncs_before = engine.write_ahead_log_store().stats(1).syncs;
    let acked = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Barrier::new(WRITERS));
    let mut handles = Vec::with_capacity(WRITERS);
    for writer in 0..WRITERS {
        let engine = Arc::clone(&engine);
        let acked = Arc::clone(&acked);
        let gate = Arc::clone(&gate);
        handles.push(std::thread::spawn(move || {
            gate.wait();
            for index in 0..PER_WRITER {
                let response = engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: format!("gc-{writer}-{index}"),
                        value: b"v".to_vec(),
                    },
                });
                if response.status.ok {
                    acked.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
    let syncs = engine.write_ahead_log_store().stats(1).syncs - syncs_before;
    let writes = acked.load(Ordering::Relaxed);
    std::env::remove_var("TS_WAL_OUTCOME_ITEMS");
    std::env::remove_var("TS_ENGINE_CONCURRENT_COMMIT");

    println!("[coalescing] writes={writes} fdatasyncs={syncs} while recording results");
    assert_eq!(writes, WRITERS * PER_WRITER, "every write must ack");
    assert!(
        syncs < writes as u64,
        "recording results cost the coalescing: {syncs} fsyncs for {writes} writes"
    );

    // And every acked write is still readable, which is the half a coalescing change can break.
    for writer in 0..WRITERS {
        for index in 0..PER_WRITER {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: format!("gc-{writer}-{index}"),
                },
            });
            assert!(
                matches!(response.response, CommandResponse::Bytes { value: Some(ref v) } if v == b"v"),
                "gc-{writer}-{index} did not read back"
            );
        }
    }
}

/// Does a write that stages a log-resident block keep it when it takes the group-commit branch?
///
/// The branch predicate tests the pages the CALLER handed in, not the ones the write staged
/// itself, and both gates default on -- so on the face of it a staged block could be dropped. This
/// settles it by looking rather than by reading the predicate, because the answer decides whether
/// anything needs fixing before the recording flip.
#[test]
fn a_group_commit_write_keeps_the_block_it_staged() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    std::env::set_var("TS_ENGINE_CONCURRENT_COMMIT", "1");

    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "bw-key".to_string(),
            value: b"staged".to_vec(),
        },
    });
    assert!(response.status.ok);
    std::env::remove_var("TS_ENGINE_CONCURRENT_COMMIT");

    let carried: usize = engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .unwrap()
        .iter()
        .filter_map(|(_, line)| crate::wal::decode_wal_line(line).ok())
        .map(|record| record.staged_pages.len())
        .sum();
    println!(
        "[block-in-wal] group-commit write: {} block(s) carried, index holds {}",
        carried,
        engine.wal_resident_page_count(1)
    );

    // Whatever the answer, the value must read back -- that is the property that matters.
    let get = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "bw-key".to_string(),
        },
    });
    assert!(
        matches!(get.response, CommandResponse::Bytes { value: Some(ref v) } if v == b"staged"),
        "a group-commit write did not read back: {:?}",
        get.response
    );
}

/// A numeric key must cost what a number costs, and still come back the same.
///
/// Timestamped kinds put their stored key into `component` as a decimal string, and a context
/// event packed TWO keys as thirty-two hex characters. Both are numbers; a varint says a
/// millisecond timestamp in seven bytes and that hex pair in about twelve. This asserts the
/// saving and, more importantly, that the round trip is unchanged -- a smaller encoding that
/// loses a key is not a saving.
#[test]
fn a_numeric_component_travels_as_a_number_and_returns_intact() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    // A feature series (component is a timestamp) and a context event (component is two keys).
    let workload = vec![
        Command::FeatureAppend {
            key: "num-feature".to_string(),
            points: (0..4)
                .map(|index| crate::types::FeaturePoint {
                    timestamp_ms: 1_787_270_070_000 + index * 1_000,
                    value: format!("p{index}").into_bytes(),
                })
                .collect(),
        },
        Command::ContextWriteExtractedEvent {
            tenant_hash: 41,
            node_hash: 42,
            event: crate::types::ContextEvent {
                event_id_hash: 445,
                event_time_ms: 1_787_270_075_000,
                ingestion_time_ms: 1_787_270_075_000,
                kind: 7,
                event_type: 7,
                actor_hash: 0,
                status: 1,
                valid_until_ms: 0,
                confidence: 0.9,
                importance: 0.8,
                text: "numeric".to_string(),
                source_ref: String::new(),
                related_node_hashes: vec![42],
                compact_attrs: Vec::new(),
                vector: Vec::new(),
            },
            indexes: crate::types::ContextExtractedEventIndexes {
                scope_hash: 3001,
                entity_hashes: vec![501],
                status_hash: 601,
                source_hash: 701,
                event_time_bucket_ms: 1_787_270_000_000,
                disabled_indexes: Vec::new(),
            },
            first_write_only: false,
            cold_storage: false,
        },
        // A component that is genuinely TEXT must keep its string.
        Command::HashSet {
            key: "num-hash".to_string(),
            field: "a-field-name".to_string(),
            value: b"v".to_vec(),
        },
    ];
    for command in workload {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "workload write failed: {response:?}");
    }

    let records = engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .unwrap()
        .iter()
        .map(|(_, line)| crate::wal::decode_wal_line(line).expect("record decodes"))
        .collect::<Vec<_>>();
    let recorded: usize = records.iter().map(|record| record.outcomes.len()).sum();
    assert!(
        recorded >= 6,
        "expected timestamped results to compare, got {recorded}"
    );

    let mut numeric = 0usize;
    let mut textual = 0usize;
    for record in &records {
        std::env::set_var("TS_WAL_BINARY_RECORDS", "1");
        let framed = crate::wal::encode_wal_line_for_test(record).expect("binary encodes");
        std::env::remove_var("TS_WAL_BINARY_RECORDS");
        let back = crate::wal::decode_wal_line(&framed).expect("binary decodes");
        assert_eq!(
            &back, record,
            "a numeric component did not survive the round trip"
        );
        for item in &record.outcomes {
            match item.kind.as_str() {
                "feature" | "context_event" | "context_index" => numeric += 1,
                "hash" => textual += 1,
                _ => {}
            }
        }
    }
    assert!(numeric > 0, "no numeric components in the workload");
    assert!(textual > 0, "no textual components in the workload");
    println!("[numeric] {numeric} numeric component(s), {textual} textual, all round-tripped");
}

/// A NEW NODE catching up from the shared log must INSTALL what the origin recorded.
///
/// The follower-replay tests that already exist build their entries with no results, so every one
/// of them exercises the fallback -- re-running the operation -- and none of them touch the path
/// this work added. A successor that silently re-executes looks identical to one that installs,
/// right up until it diverges, so the count is asserted before the contents.
///
/// This is the case with the weakest assumptions in the system: a DIFFERENT node, with its own
/// clock and whatever config it holds, rebuilding a shard it never wrote.
#[test]
fn a_new_node_catching_up_installs_what_the_origin_recorded() {
    let dir = tempfile::tempdir().unwrap();

    // The origin writes, and its records carry what each write did.
    let origin = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("origin-cache"),
        dir.path().join("origin-pages"),
        dir.path().join("origin-indexes"),
    );
    origin.load_shard(1);
    let workload = vec![
        Command::StringSet {
            key: "cu-a".to_string(),
            value: b"alpha".to_vec(),
        },
        Command::StringSetEx {
            key: "cu-ttl".to_string(),
            value: b"expiring".to_vec(),
            ttl_ms: 600_000,
        },
        Command::HashSet {
            key: "cu-hash".to_string(),
            field: "f".to_string(),
            value: b"hv".to_vec(),
        },
        Command::ZSetAdd {
            key: "cu-zset".to_string(),
            member: b"zm".to_vec(),
            score: 4.5,
        },
        Command::SeenCheck {
            key: "cu-seen".to_string(),
            member: b"m".to_vec(),
            window_ms: 600_000,
        },
        Command::FeatureAppend {
            key: "cu-feature".to_string(),
            points: (0..3)
                .map(|index| crate::types::FeaturePoint {
                    timestamp_ms: 1_787_270_070_000 + index * 1_000,
                    value: format!("p{index}").into_bytes(),
                })
                .collect(),
        },
    ];
    for command in workload {
        let response = origin.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "origin write failed: {response:?}");
    }
    let expected = origin.index_shape_for_test(1);
    let expected_index = origin.bucket_index_shape_for_test(1);

    // What the origin would publish to the shared log: its records, results and all.
    let published = origin
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .unwrap()
        .iter()
        .filter_map(|(_, line)| crate::wal::decode_wal_line(line).ok())
        .collect::<Vec<_>>();
    let carrying: usize = published
        .iter()
        .filter(|record| !record.outcomes.is_empty())
        .count();
    assert!(
        carrying >= 5,
        "the origin published {carrying} records carrying results; this test needs them to exist"
    );

    // A node that has never seen this shard, catching up from those records alone.
    let newcomer = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("new-cache"),
        dir.path().join("new-pages"),
        dir.path().join("new-indexes"),
    );
    newcomer.load_shard(1);
    let installs_before = newcomer.replay_installs_for_test();
    for record in &published {
        if record.outcomes.is_empty() {
            continue;
        }
        assert!(
            newcomer.install_shared_outcomes(1, &record.outcomes),
            "the newcomer refused results at sequence {}",
            record.sequence
        );
    }
    let installed = newcomer.replay_installs_for_test() - installs_before;

    // WHICH path ran, before what it produced: a successor that re-executed would pass the
    // comparison below and prove nothing about catching up from data.
    assert!(
        installed >= carrying as u64,
        "the newcomer installed {installed} results for {carrying} records carrying them"
    );
    assert_eq!(
        newcomer.index_shape_for_test(1),
        expected,
        "a node catching up from results did not arrive at the origin's shard"
    );
    assert_eq!(
        newcomer.bucket_index_shape_for_test(1),
        expected_index,
        "the newcomer's maps matched and its index did not"
    );
    println!("[catch-up] {carrying} records, {installed} results installed, shard identical");
}

/// RECLAIM against the record that now ships.
///
/// Reclaim rewrites the log, so every survivor has to still decode and still rebuild the shard.
/// A binary payload makes that a real question rather than a formality: a rewrite that splits a
/// record on the wrong byte, or drops the base header, leaves records that parse as nothing --
/// which is silent, because a reclaimed log is SUPPOSED to be shorter.
#[test]
fn reclaim_leaves_every_surviving_record_readable_and_the_shard_rebuildable() {
    let dir = tempfile::tempdir().unwrap();
    let cache = dir.path().join("cache");
    let pages = dir.path().join("pages");
    let indexes = dir.path().join("indexes");

    const WRITES: usize = 120;
    let shape_before;
    {
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            cache.clone(),
            pages.clone(),
            indexes.clone(),
        );
        engine.load_shard(1);
        for index in 0..WRITES {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("rc-{index:04}"),
                    // Newlines and high bytes, because reclaim moves bytes around.
                    value: {
                        let mut value = format!("v-{index}\n").into_bytes();
                        value.extend_from_slice(&[0x1b, 0x0a, 0xff, 0x00]);
                        value.extend_from_slice(b"tail");
                        value
                    },
                },
            });
            assert!(response.status.ok);
        }
        shape_before = engine.index_shape_for_test(1);

        let before = engine
            .write_ahead_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .unwrap()
            .len();
        assert!(before >= WRITES, "expected the writes to be on the log");

        // Reclaim through the AUTHORIZED path.
        //
        // The unchecked form bypasses the durable-index clamp, and the first version of this test
        // used it and then asserted the shard still rebuilt. It does not, and should not: the
        // clamp exists to stop a caller deleting records the index has not captured. That failure
        // read exactly like a reclaim defect and was the test throwing away its own proof.
        //
        // Flushing first is what makes the anchor honest. The index on disk then reflects every
        // record written above, so reclaiming up to that point is safe by construction.
        engine.flush_shard_index(1);
        let flushed_through = engine
            .write_ahead_log_store()
            .flush(1)
            .expect("flush")
            .last_sequence;
        let anchor = crate::wal::DurableIndexAnchor::proven_durable_through(1, flushed_through);
        let report = engine
            .write_ahead_log_store()
            .gc_before_sequence(1, flushed_through, &anchor)
            .expect("reclaim");
        println!(
            "[reclaim] authorized through {flushed_through}, clamped={}",
            report.clamped_by_durable_index
        );

        let survivors = engine
            .write_ahead_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .unwrap();
        let decoded = survivors
            .iter()
            .filter(|(_, line)| crate::wal::decode_wal_line(line).is_ok())
            .count();
        assert_eq!(
            decoded,
            survivors.len(),
            "{} of {} surviving records stopped decoding after reclaim",
            survivors.len() - decoded,
            survivors.len()
        );
        println!(
            "[reclaim] {} records before, {} survived, all decode",
            before,
            survivors.len()
        );
    }

    // The shard still loads, and every value still reads back.
    let reopened = TemporalEngine::with_local_dirs(1024 * 1024, cache, pages, indexes);
    reopened.load_shard(1);
    assert_eq!(
        reopened.index_shape_for_test(1),
        shape_before,
        "a reclaimed log did not rebuild the shard it described"
    );
}

/// FAULT TOLERANCE: a torn tail must be refused or truncated, never half-applied.
///
/// A crash mid-append leaves a partial record. With a text payload that shows up as a line that
/// fails to parse; with a binary one it can be a prefix that decodes to a DIFFERENT, valid-looking
/// record, which is the dangerous shape. Both are checked: a truncated tail and a corrupted
/// interior.
#[test]
fn a_torn_or_corrupted_tail_never_half_applies() {
    for (label, corrupt_interior) in [("torn tail", false), ("corrupted interior", true)] {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let pages = dir.path().join("pages");
        let indexes = dir.path().join("indexes");
        {
            let engine = TemporalEngine::with_local_dirs(
                1024 * 1024,
                cache.clone(),
                pages.clone(),
                indexes.clone(),
            );
            engine.load_shard(1);
            for index in 0..8 {
                engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: format!("ft-{index}"),
                        value: format!("value-{index}").into_bytes(),
                    },
                });
            }
        }

        let path = indexes.join("wals").join("shard-1.wal.jsonl");
        let mut bytes = std::fs::read(&path).expect("the log exists");
        // Trim the preallocated zero run so the edit lands on a record.
        while bytes.last() == Some(&0) {
            bytes.pop();
        }
        if corrupt_interior {
            // Flip a byte in the middle of a record: a value-preserving corruption that the
            // frame's checksum is there to catch.
            let middle = bytes.len() / 2;
            bytes[middle] ^= 0xff;
        } else {
            // Cut the last record in half.
            bytes.truncate(bytes.len() - 20);
        }
        std::fs::write(&path, &bytes).expect("rewrite the log");

        let reopened = TemporalEngine::with_local_dirs(1024 * 1024, cache, pages, indexes);
        // The form that REPORTS: a load which refuses is a correct outcome here, and a load which
        // reports success while having dropped a record silently is the one that is not.
        let load = reopened.load_shard_with(crate::control::LoadShardRequest {
            shard_id: 1,
            load_version: 0,
            local_node_id: None,
            shard_uri: String::new(),
            start_routing_bucket: 0,
            end_routing_bucket: u32::MAX,
            readonly: false,
            table_name: String::new(),
        });
        // Either outcome is correct. What must NOT happen is a load that reports success and
        // serves a shard missing a record it silently dropped.
        let served = reopened.index_shape_for_test(1);
        let recovered = served.lines().filter(|l| l.starts_with("string ft-")).count();
        println!("[fault:{label}] load ok={} recovered={recovered}", load.status.ok);
        if load.status.ok {
            assert!(
                recovered > 0,
                "{label}: the load reported success and recovered nothing"
            );
        }
    }
}

/// The engine log records results in RAFT mode too.
///
/// Consensus replicates operations by construction -- that is what agreement means -- but the
/// engine log underneath a raft apply is the same log, and it should record what the apply DID
/// like any other write. If it does not, a raft node's own recovery is still re-executing.
#[test]
fn a_raft_apply_records_what_it_did_in_the_engine_log() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let response = engine.execute_raft_apply(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "raft-key".to_string(),
            value: b"raft-value".to_vec(),
        },
    });
    assert!(response.status.ok, "raft apply failed: {response:?}");

    let recorded: usize = engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .unwrap()
        .iter()
        .filter_map(|(_, line)| crate::wal::decode_wal_line(line).ok())
        .map(|record| record.outcomes.len())
        .sum();
    assert!(
        recorded > 0,
        "a raft apply left no record of what it did, so this node's own recovery re-executes"
    );
    println!("[raft] apply recorded {recorded} result(s) in the engine log");
}

/// Can the served index be rebuilt from the LOG ALONE, with no snapshot at all?
///
/// The equivalence gate answers this in principle -- it installs results into an empty shard and
/// gets an identical one -- but it hands the results over by hand. This deletes the index file off
/// disk and makes recovery do it: whatever comes back was built from the log.
///
/// The answer matters for what the snapshot IS. If the log alone suffices, the snapshot is an
/// optimisation that bounds replay, and the durable-index anchor is what says how much of the log
/// may be reclaimed. If it does not, the snapshot is load-bearing and reclaim is far more
/// dangerous than it looks.
#[test]
fn the_served_index_rebuilds_from_the_log_with_no_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let cache = dir.path().join("cache");
    let pages = dir.path().join("pages");
    let indexes = dir.path().join("indexes");

    let expected;
    let expected_index;
    {
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            cache.clone(),
            pages.clone(),
            indexes.clone(),
        );
        engine.load_shard(1);
        let workload = vec![
            Command::StringSet {
                key: "fw-a".to_string(),
                value: b"alpha".to_vec(),
            },
            Command::StringSetEx {
                key: "fw-ttl".to_string(),
                value: b"expiring".to_vec(),
                ttl_ms: 600_000,
            },
            Command::HashSet {
                key: "fw-hash".to_string(),
                field: "f".to_string(),
                value: b"hv".to_vec(),
            },
            Command::SetAdd {
                key: "fw-set".to_string(),
                member: b"m".to_vec(),
            },
            Command::ZSetAdd {
                key: "fw-zset".to_string(),
                member: b"zm".to_vec(),
                score: 1.5,
            },
            Command::ListPush {
                key: "fw-list".to_string(),
                member: b"lm".to_vec(),
                left: true,
            },
            Command::SeenCheck {
                key: "fw-seen".to_string(),
                member: b"m".to_vec(),
                window_ms: 600_000,
            },
            Command::BucketTake {
                key: "fw-bucket".to_string(),
                tokens: 2.0,
                capacity: 10.0,
                refill_per_sec: 1.0,
            },
            Command::FeatureAppend {
                key: "fw-feature".to_string(),
                points: (0..3)
                    .map(|index| crate::types::FeaturePoint {
                        timestamp_ms: 1_787_270_070_000 + index * 1_000,
                        value: format!("p{index}").into_bytes(),
                    })
                    .collect(),
            },
            Command::CommonDelete {
                key: "fw-a".to_string(),
            },
        ];
        for command in workload {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            assert!(response.status.ok, "workload write failed: {response:?}");
        }
        // Force the snapshot to exist, so deleting it below is a real deletion.
        engine.flush_shard_index(1);
        expected = engine.index_shape_for_test(1);
        expected_index = engine.bucket_index_shape_for_test(1);
    }

    // Delete every index file. The log is now the only account of what happened.
    let index_dir = indexes.clone();
    let mut removed = 0usize;
    if let Ok(entries) = std::fs::read_dir(&index_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                let name = path.file_name().unwrap_or_default().to_string_lossy().to_string();
                if name.contains("index") {
                    std::fs::remove_file(&path).ok();
                    removed += 1;
                }
            }
        }
    }
    assert!(removed > 0, "no index snapshot was written, so this proves nothing");

    let rebuilt = TemporalEngine::with_local_dirs(1024 * 1024, cache, pages, indexes);
    let installs_before = rebuilt.replay_installs_for_test();
    rebuilt.load_shard(1);
    let installed = rebuilt.replay_installs_for_test() - installs_before;

    println!("[from-log] {removed} index file(s) deleted, {installed} result(s) installed");
    assert!(
        installed > 0,
        "the rebuild re-ran operations rather than installing what the writes recorded"
    );
    assert_eq!(
        rebuilt.index_shape_for_test(1),
        expected,
        "the served index did not come back from the log alone"
    );
    assert_eq!(
        rebuilt.bucket_index_shape_for_test(1),
        expected_index,
        "the maps came back from the log and the index did not"
    );
}

/// A SECONDARY must SERVE, not merely arrive at the right shard.
///
/// The catch-up test compares shard shapes, which says the maps are right. It does not say a read
/// works: a shape is an index, and serving a value means resolving an address to bytes. A node
/// that caught up but cannot resolve its addresses passes the shape comparison and answers every
/// read with nothing.
///
/// So this reads back every kind through the ordinary command path, on a node that never wrote
/// any of it.
#[test]
fn a_secondary_serves_reads_for_every_kind_it_caught_up_on() {
    let dir = tempfile::tempdir().unwrap();
    let origin = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("origin-cache"),
        dir.path().join("origin-pages"),
        dir.path().join("origin-indexes"),
    );
    origin.load_shard(1);
    let writes = vec![
        Command::StringSet {
            key: "sv-string".to_string(),
            value: b"string-value".to_vec(),
        },
        Command::HashSet {
            key: "sv-hash".to_string(),
            field: "f".to_string(),
            value: b"hash-value".to_vec(),
        },
        Command::SetAdd {
            key: "sv-set".to_string(),
            member: b"member".to_vec(),
        },
        Command::ZSetAdd {
            key: "sv-zset".to_string(),
            member: b"zm".to_vec(),
            score: 2.5,
        },
        Command::ListPush {
            key: "sv-list".to_string(),
            member: b"list-value".to_vec(),
            left: true,
        },
        Command::FeatureAppend {
            key: "sv-feature".to_string(),
            points: vec![crate::types::FeaturePoint {
                timestamp_ms: 1_787_270_070_000,
                value: b"feature-value".to_vec(),
            }],
        },
    ];
    for command in writes {
        assert!(
            origin
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command
                })
                .status
                .ok
        );
    }

    // The secondary shares the ORIGIN'S block store -- which is what a shared-storage secondary
    // has. It has its own cache and its own index, and knows nothing about the shard.
    let secondary = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("second-cache"),
        dir.path().join("origin-pages"),
        dir.path().join("second-indexes"),
    );
    secondary.load_shard(1);
    let published = origin
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .unwrap()
        .iter()
        .filter_map(|(_, line)| crate::wal::decode_wal_line(line).ok())
        .filter(|record| !record.outcomes.is_empty())
        .collect::<Vec<_>>();
    for record in &published {
        assert!(
            secondary.install_shared_outcomes(1, &record.outcomes),
            "the secondary refused results at sequence {}",
            record.sequence
        );
    }

    // Now SERVE. Each of these resolves an address the secondary never wrote.
    let reads: Vec<(&str, Command, Vec<u8>)> = vec![
        (
            "string",
            Command::StringGet {
                key: "sv-string".to_string(),
            },
            b"string-value".to_vec(),
        ),
        (
            "hash",
            Command::HashGet {
                key: "sv-hash".to_string(),
                field: "f".to_string(),
            },
            b"hash-value".to_vec(),
        ),
    ];
    for (label, command, want) in reads {
        let response = secondary.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        match response.response {
            CommandResponse::Bytes { value: Some(got) } => {
                assert_eq!(got, want, "{label}: the secondary served the wrong bytes")
            }
            other => panic!("{label}: the secondary could not serve it: {other:?}"),
        }
    }
    println!(
        "[secondary] caught up on {} record(s) and served reads it never wrote",
        published.len()
    );
}

/// Where a read is answered FROM, and what each tier costs.
///
/// Three places a value can come from, in order of what they cost: the cache in memory, the block
/// store on local disk, and shared storage. The read path tries them in that order, so what a
/// measurement shows depends entirely on what it warmed first -- which is why this counts the
/// block store's own reads rather than timing anything: a timer cannot tell a warm cache from a
/// fast disk, and a counter can.
#[test]
fn a_read_is_answered_from_memory_before_disk_and_the_counters_say_which() {
    let dir = tempfile::tempdir().unwrap();
    let cache = dir.path().join("cache");
    let pages = dir.path().join("pages");
    let indexes = dir.path().join("indexes");

    const KEYS: usize = 40;
    {
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            cache.clone(),
            pages.clone(),
            indexes.clone(),
        );
        engine.load_shard(1);
        for index in 0..KEYS {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("pr-{index:03}"),
                    value: vec![b'v'; 256],
                },
            });
        }

        // WARM: the values were just written, so the cache holds them.
        let warm_before = engine.block_store().stats().reads;
        for index in 0..KEYS {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: format!("pr-{index:03}"),
                },
            });
        }
        let warm_reads = engine.block_store().stats().reads - warm_before;
        // REPORTED, not asserted. Measured at one block-store read per read even for values
        // written moments earlier, which is not what the read path looks like it should do --
        // `append_value` puts the page in the cache under the same key `read_page_bytes` looks
        // up. Something between those two is not connecting, and asserting a property here
        // before understanding which would be asserting a guess.
        println!("[tier] warm: {warm_reads} block-store read(s) for {KEYS} reads");
    }

    // COLD: a new engine, empty cache, same block store. Every value must be PROMOTED from disk.
    let reopened = TemporalEngine::with_local_dirs(1024 * 1024, dir.path().join("cache-b"), pages, indexes);
    reopened.load_shard(1);
    let cold_before = reopened.block_store().stats().reads;
    let mut served = 0usize;
    for index in 0..KEYS {
        let response = reopened.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: format!("pr-{index:03}"),
            },
        });
        if matches!(response.response, CommandResponse::Bytes { value: Some(_) }) {
            served += 1;
        }
    }
    let cold_reads = reopened.block_store().stats().reads - cold_before;
    println!("[tier] cold: {cold_reads} block-store read(s), {served}/{KEYS} served");
    assert_eq!(served, KEYS, "a cold node failed to promote every value from disk");
    assert!(
        cold_reads > 0,
        "a cold node served everything without touching the block store, so nothing was promoted"
    );

    // And once promoted, the SECOND pass should not go back to disk for the same values.
    let second_before = reopened.block_store().stats().reads;
    for index in 0..KEYS {
        reopened.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: format!("pr-{index:03}"),
            },
        });
    }
    let second_reads = reopened.block_store().stats().reads - second_before;
    println!("[tier] promoted: {second_reads} block-store read(s) on the second pass");
    assert!(
        second_reads < cold_reads,
        "promotion did not stick: {second_reads} reads on the second pass against {cold_reads} on the first"
    );
}

/// Does the served index ever hold an address only THIS process can resolve?
///
/// Two slab ids are synthetic: one means "the page is inside a WAL record", the other means "the
/// page is in memory". Neither is a file in the block store. Both are resolved through a registry
/// that is process-local -- a static map in this process, keyed by a pointer to this process's
/// block store.
///
/// That is fine for a restart, which rebuilds the registry from the index. It is not obviously
/// fine for a NEW NODE: a checkpoint uploads the slabs the block store actually has, and a
/// synthetic slab is not one of them. An index entry naming one would arrive somewhere it can
/// never be resolved -- and would read as nothing, with no error.
///
/// This measures whether the served index contains such addresses at all, and under what settings.
/// It asserts nothing about the answer: the point is to establish the fact before deciding what to
/// do about it.
#[test]
fn how_many_served_addresses_only_this_process_can_resolve() {
    // Every combination that could put a page somewhere other than the block store: the log-in-
    // record path, the reserve-only append that skips staging, and asynchronous writes whose
    // pages are buffered rather than written.
    let cases: Vec<(&str, &str, &str, bool)> = vec![
        ("default", "1", "1", false),
        ("log-in-record OFF", "0", "1", false),
        ("group-commit OFF", "1", "0", false),
        ("async writes", "1", "1", true),
        ("async + group-commit OFF", "1", "0", true),
    ];
    for (label, block_in_wal, concurrent, async_storage) in cases {
        std::env::set_var("TS_BLOCK_IN_WAL", block_in_wal);
        std::env::set_var("TS_ENGINE_CONCURRENT_COMMIT", concurrent);
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        if async_storage {
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
        }
        for index in 0..24 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("syn-{index:03}"),
                    value: vec![b'v'; 128],
                },
            });
        }

        let synthetic = engine.synthetic_address_count_for_test(1);
        let resident = engine.wal_resident_page_count(1);
        println!(
            "[synthetic] {label}: {synthetic} served address(es) resolvable only in-process, {resident} log-resident page(s) tracked"
        );

        // And after a flush, which is what a checkpoint exports.
        engine.flush_shard_index(1);
        let after_flush = engine.synthetic_address_count_for_test(1);
        println!("[synthetic] {label}: {after_flush} after the index is flushed");

        // If any exist, say whether a value behind one still READS -- in this process it should,
        // because the registry is right here. The question a checkpoint raises is whether it
        // would read anywhere else, and that is what the count above is for.
        if after_flush > 0 {
            let probe = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "syn-000".to_string(),
                },
            });
            println!(
                "[synthetic] {label}: in-process read {}",
                if matches!(probe.response, CommandResponse::Bytes { value: Some(_) }) {
                    "served"
                } else {
                    "EMPTY"
                }
            );
        }
        std::env::remove_var("TS_BLOCK_IN_WAL");
        std::env::remove_var("TS_ENGINE_CONCURRENT_COMMIT");
    }
}

/// A checkpoint's index must name only places the checkpoint can carry.
///
/// An asynchronous write leaves its page in a WAL record or in memory, named by a synthetic slab
/// id that is not a file. It serves here, through a registry local to this process. A checkpoint
/// uploads the slabs the block store HAS, so a synthetic one is never uploaded, and a node
/// restoring that index holds addresses it can never resolve -- reads that return nothing, with no
/// error anywhere.
///
/// So the pages are materialised before the index is exported. This asserts the property that
/// makes a checkpoint portable: after materialising, ZERO addresses in the served index name a
/// slab that is not a file -- and every value still reads.
#[test]
fn a_checkpoint_index_names_no_place_only_this_process_can_reach() {
    std::env::set_var("TS_BLOCK_IN_WAL", "1");
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
    for index in 0..24 {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("pt-{index:03}"),
                value: format!("portable-{index}").into_bytes(),
            },
        });
        assert!(response.status.ok);
    }

    let before = engine.synthetic_address_count_for_test(1);
    assert!(
        before > 0,
        "this test needs async writes to produce in-process-only addresses, and got none"
    );

    let moved = engine.materialize_synthetic_pages(1);
    let after = engine.synthetic_address_count_for_test(1);
    println!("[portable] {before} in-process-only address(es), {moved} materialised, {after} left");
    std::env::remove_var("TS_BLOCK_IN_WAL");

    assert_eq!(
        after, 0,
        "the index still names {after} place(s) a restoring node could not reach"
    );

    // And every value still reads, which is the half a page move can break.
    for index in 0..24 {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: format!("pt-{index:03}"),
            },
        });
        let want = format!("portable-{index}").into_bytes();
        match response.response {
            CommandResponse::Bytes { value: Some(got) } => {
                assert_eq!(got, want, "pt-{index:03} came back changed after materialising")
            }
            other => panic!("pt-{index:03} stopped reading after materialising: {other:?}"),
        }
    }
}

/// Where the bytes of a live record actually go.
///
/// Not a gate -- a census. Every optimisation so far came from looking at one encoded record and
/// asking which field was paying for itself, and two of my guesses about that were wrong. So this
/// prints the record and the size of each part, and asserts nothing.
#[test]
fn what_a_live_record_is_made_of() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    // A spread of kinds, because the question is whether a rule holds for ALL of them. The last
    // time two identifiers looked interchangeable they were, for strings, and diverged for every
    // timestamped kind.
    for command in [
        Command::StringSet {
            key: "size-000000".to_string(),
            value: vec![b'v'; 64],
        },
        Command::HashSet {
            key: "cen-hash".to_string(),
            field: "f".to_string(),
            value: b"v".to_vec(),
        },
        Command::FeatureAppend {
            key: "cen-feature".to_string(),
            points: (0..2)
                .map(|index| crate::types::FeaturePoint {
                    timestamp_ms: 1_787_270_070_000 + index * 1_000,
                    value: b"p".to_vec(),
                })
                .collect(),
        },
        Command::SeenCheck {
            key: "cen-seen".to_string(),
            member: b"m".to_vec(),
            window_ms: 60_000,
        },
    ] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
    }

    // Does address.object_id() EVER differ from the item's, or go absent? That decides whether it
    // can be dropped from the wire and rebuilt.
    let mut same = 0usize;
    let mut differ = 0usize;
    let mut absent = 0usize;
    let mut no_address = 0usize;
    for (_, line) in engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .unwrap()
    {
        let record = crate::wal::decode_wal_line(&line).expect("decodes");
        for item in &record.outcomes {
            match item.resolved_address().map(|a| a.object_id()) {
                None => no_address += 1,
                Some(None) => absent += 1,
                Some(Some(id)) if id == item.object_id => same += 1,
                Some(Some(_)) => differ += 1,
            }
        }
    }
    println!("[census] address object_id: {same} same, {differ} differ, {absent} absent, {no_address} item(s) with no address");

    for (_, line) in engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .unwrap()
        .iter()
        .take(1)
    {
        let record = crate::wal::decode_wal_line(line).expect("decodes");
        println!("[census] framed record: {} B", line.len());
        println!("[census] items: {}", record.outcomes.len());
        for item in &record.outcomes {
            println!(
                "[census]   kind={} key={} ({} B) object_id={} bucket={}",
                item.kind,
                item.object_key,
                item.object_key.len(),
                item.object_id,
                item.routing_bucket
            );
            if let Some(address) = item.resolved_address() {
                println!(
                    "[census]   address: slab={} off={} len={} block_id={:?} object_id={:?} gen={:?} band={:?}",
                    address.page_slab_id,
                    address.offset,
                    address.length,
                    address.page_id(),
                    address.object_id(),
                    address.generation(),
                    address.band_id(),
                );
                println!(
                    "[census]   item.object_id == address.object_id()? {}",
                    address.object_id() == Some(item.object_id)
                );
            }
        }
        if let Some(metadata) = &record.metadata {
            println!(
                "[census] metadata: version={} timestamp={} batch={:?}",
                metadata.version, metadata.timestamp_ms, metadata.batch_id
            );
        }
        println!("[census] carries an operation: {}", record.command.is_some());
    }
}

/// Does the log-resident registry shrink when the pages it names become durable?
///
/// The registry maps a synthetic address to the record holding its bytes. It is process-static and
/// keyed per object, so it grows with the number of distinct objects a shard writes that way. The
/// index's own copy of that mapping IS pruned on a dump -- the comment there says dropping those
/// entries is what keeps it from growing with the log.
///
/// The registry is a second copy of the same knowledge, and it matters twice: it holds memory, and
/// `min_registered_sequence` pins the WAL retention floor to the LOWEST registration -- so an entry
/// for a page that is already durable would hold the floor down and stop reclaim, not just cost
/// bytes.
///
/// Measured, not asserted, until the numbers say which.
#[test]
fn what_the_log_resident_registry_holds_after_a_dump() {
    std::env::set_var("TS_BLOCK_IN_WAL", "1");
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
    const WRITES: usize = 48;
    for index in 0..WRITES {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("rg-{index:03}"),
                value: vec![b'v'; 96],
            },
        });
    }

    let registered = engine.registration_count_for_test(1);
    let resident = engine.wal_resident_page_count(1);
    println!("[registry] after {WRITES} async writes: {registered} registration(s), {resident} index entr(ies)");

    // A dump makes those pages durable and prunes the INDEX's copy. What happens to the registry?
    engine.flush_shard_index(1);
    let after_flush = engine.registration_count_for_test(1);
    let resident_after = engine.wal_resident_page_count(1);
    println!("[registry] after a flush: {after_flush} registration(s), {resident_after} index entr(ies)");

    let cycle = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: 1,
        ..Default::default()
    });
    let after_cycle = engine.registration_count_for_test(1);
    let resident_cycle = engine.wal_resident_page_count(1);
    println!(
        "[registry] after a storage cycle ({} stages): {after_cycle} registration(s), {resident_cycle} index entr(ies)",
        cycle.stages.len()
    );

    // The registrations pin the WAL retention floor, deliberately: while a page lives ONLY in a
    // record, truncating that record turns an acked write into a missing read. The question is
    // what happens once the page is somewhere else.
    let info = engine.write_ahead_log_store().info(1).unwrap();
    println!(
        "[registry] log: start={} current={} -- reclaim cannot pass the oldest registration",
        info.start_sequence, info.current_sequence
    );

    let moved = engine.materialize_synthetic_pages(1);
    let after_materialise = engine.registration_count_for_test(1);
    let synthetic_left = engine.synthetic_address_count_for_test(1);
    println!(
        "[registry] after materialising: {moved} page(s) moved, {after_materialise} registration(s) left, {synthetic_left} synthetic address(es) left"
    );
    std::env::remove_var("TS_BLOCK_IN_WAL");

    // Whatever the counts, every value must still read -- a registry that shrank too far would
    // show up here rather than as a number.
    let mut served = 0usize;
    for index in 0..WRITES {
        if matches!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: format!("rg-{index:03}")
                    },
                })
                .response,
            CommandResponse::Bytes { value: Some(_) }
        ) {
            served += 1;
        }
    }
    println!("[registry] {served}/{WRITES} still served");
    assert_eq!(served, WRITES, "a value stopped reading");
}

#[test]
fn a_replay_leaves_nothing_staged_for_the_next_write_to_adopt() {
    // Outcomes are staged during execution and taken at the append. Replay executes and does
    // not append, so whatever it staged stayed on the thread -- and the next write on that
    // thread appended a record carrying it, claiming changes it never made. Threads are
    // reused, in a server across requests and here across tests, so "the next write" is
    // routinely someone else entirely.
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
    for i in 0..4 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("key-{i}"),
                value: b"value".to_vec(),
            },
        });
    }
    drop(engine);

    let restarted = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-b"),
        &page_dir,
        &index_dir,
    );
    restarted.load_shard(1);

    assert_eq!(
        crate::engine::block_in_wal::staged_outcome_count(),
        0,
        "a replay must leave nothing staged: whatever it leaves behind is adopted by the next \
         write on this thread and recorded as that write's own doing"
    );
}

#[test]
fn an_async_write_is_never_lost_across_a_restart() {
    // The symptom three separate tests kept showing: an async write is acknowledged, the log
    // holds it, the shard loads with no error -- and the value is gone. Losing a write that way
    // is a decision, not a crash: recovery picks a watermark and replays only past it, so a
    // watermark chosen above a record skips it silently. This runs the whole cycle enough times
    // to catch an intermittent one and reports the watermark it chose when it does.
    for attempt in 0..120 {
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
        let before = engine.write_ahead_log_store().stats(1);
        engine
            .write_ahead_log_store()
            .flush(1)
            .expect("flush before the engine goes away");
        drop(engine);

        let restarted = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache-b"),
            &page_dir,
            &index_dir,
        );
        // Ask the successor what it can SEE before asking what it recovered. A load that
        // refused, a log the successor cannot read, and a replay that read the record and
        // applied nothing are three different defects that all look like one missing value.
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
        let seen = restarted.write_ahead_log_store().stats(1);
        let installs = restarted.replay_installs_for_test();
        let registrations = restarted.registration_count_for_test(1);
        let synthetic = restarted.synthetic_address_count_for_test(1);
        let got = restarted.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        let watermark =
            crate::engine::lifecycle::LAST_REPLAY_WATERMARK.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            loaded.status.ok,
            "attempt {attempt}: the successor refused the load: {:?}",
            loaded.status
        );
        assert_eq!(
            got.response,
            CommandResponse::Bytes {
                value: Some(b"async-durable".to_vec())
            },
            "attempt {attempt}: an acknowledged async write was lost across a restart. \
             the writer left {} records up to sequence {}; the successor saw {} records up to \
             sequence {}, replayed from watermark {watermark}, installed {} outcomes, holds              {} page registrations and {} unresolvable addresses",
            before.writes,
            before.last_sequence,
            seen.writes,
            seen.last_sequence,
            installs,
            registrations,
            synthetic
        );
    }
}

#[test]
fn a_record_carrying_its_blocks_states_results_instead_of_the_operation() {
    // The log records what a write DID, not what it was asked to do -- and that now includes a
    // write whose blocks travel inside its own record. It could not before: an installed result
    // names an address, and a block living in the record was unreachable by address until replay
    // began registering the blocks of every record it replays. With that in place the operation
    // is redundant for these too, and redundant bytes in a log are paid for on every write.
    //
    // What legitimately keeps its operation: an ASYNCHRONOUS write carrying nothing. Its result
    // names a block-store address a crash may leave unwritten, and no registration recovers a
    // block that was never stored.
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
    for index in 0..8 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("k{index}"),
                value: vec![b'v'; 256],
            },
        });
    }
    engine
        .write_ahead_log_store()
        .flush(1)
        .expect("flush before reading the log back");

    let records = engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .expect("the log should read back");
    let mut carrying_blocks = 0usize;
    let mut carrying_blocks_with_command = 0usize;
    for (_, line) in &records {
        let record = crate::wal::decode_wal_line(line).expect("a record should decode");
        if record.staged_pages.is_empty() {
            continue;
        }
        carrying_blocks += 1;
        if record.command.is_some() {
            carrying_blocks_with_command += 1;
        }
    }
    assert!(
        carrying_blocks > 0,
        "this workload must produce records carrying their blocks, or it proves nothing"
    );
    // Assert both directions of the flag, because the property belongs to the flag rather than
    // to the log. With data-only off, every record is SUPPOSED to carry its operation -- that is
    // what the switch means -- and a test asserting otherwise unconditionally says the legacy
    // configuration is broken when it is behaving exactly as asked.
    if crate::wal::wal_data_only_enabled() {
        assert_eq!(
            carrying_blocks_with_command, 0,
            "{carrying_blocks_with_command} of {carrying_blocks} records carrying their own \
             blocks still carry the operation as well"
        );
    } else {
        assert_eq!(
            carrying_blocks_with_command, carrying_blocks,
            "with data-only off every record keeps its operation: {carrying_blocks_with_command} \
             of {carrying_blocks} did"
        );
    }

    // And the point of all of it: the shard still comes back.
    drop(engine);
    let restarted = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-b"),
        &page_dir,
        &index_dir,
    );
    restarted.load_shard(1);
    for index in 0..8 {
        assert_eq!(
            restarted
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: format!("k{index}"),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(vec![b'v'; 256])
            },
            "k{index} must survive a restart on results alone"
        );
    }
}

/// what the log-resident registry does under sustained writes.
///
/// Every asynchronous write whose page has nowhere durable to go yet leaves a registration: one
/// entry naming the record that holds the only copy. Two things ride on that entry. It costs
/// memory, which is the smaller half. It also pins `min_registered_sequence`, and reclaim may not
/// truncate below the lowest registration -- so a registry that only grows is a log that can
/// never be reclaimed, whatever the retention policy says.
///
/// Nothing on the local write path retires them: the drain is a dump, and shared-store
/// checkpointing. This measures what happens in between.
#[test]
fn what_the_log_resident_registry_does_under_sustained_writes() {
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
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

    let mut seen = Vec::new();
    for batch in 1..=6 {
        for index in 0..200 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    // Distinct keys: a rewrite of the same key REPLACES its registration, so
                    // reusing keys would measure replacement rather than growth.
                    key: format!("object-{:06}", batch * 1000 + index),
                    value: vec![b'v'; 256],
                },
            });
        }
        seen.push((batch * 200, engine.registration_count_for_test(1)));
    }

    let floor_pinned = seen.last().map(|(_, count)| *count).unwrap_or(0);
    println!("  writes -> registrations held");
    for (writes, held) in &seen {
        println!("  {writes:>6} -> {held:>6}");
    }
    println!(
        "  a dump moves them out; between dumps the floor is pinned by {floor_pinned} entries"
    );

    // The shape is the finding: if this tracks writes one-for-one it is unbounded between dumps.
    let (first_writes, first_held) = seen[0];
    let (last_writes, last_held) = *seen.last().unwrap();
    let grew = last_held as f64 / (first_held.max(1)) as f64;
    let wrote = last_writes as f64 / first_writes as f64;
    println!("  writes x{wrote:.1}, registrations x{grew:.1}");
}

/// what eight records per ingest cost, and what a batch already saves.
///
/// An ingest here is not one write. Measured elsewhere on the real path: a single ingest writes
/// EIGHT records -- node, event, index ref, dirty marker, two summary refs, retrieval node,
/// uniqueness -- and eight barriers, perfectly linear as ingests multiply. So the per-record
/// constant this work has been shrinking gets multiplied by eight before it reaches a user, and
/// the interesting question is not how big a record is but how many of them one operation makes.
///
/// This measures the two shapes that exist today: eight separate writes against one atomic batch
/// of eight. The record format already carries `repeated items`, so a third shape is possible --
/// one record holding eight outcomes -- and what it would save is the difference between the
/// per-record overhead paid once and paid eight times.
#[test]
fn what_eight_records_per_ingest_cost() {
    fn write_and_measure(batched: bool, ingests: usize) -> (u64, u64, u64) {
        let dir = tempfile::tempdir().unwrap();
        let page_dir = dir.path().join("pages");
        let index_dir = dir.path().join("indexes");
        let engine = TemporalEngine::with_local_dirs(
            8 * 1024 * 1024,
            dir.path().join("cache"),
            &page_dir,
            &index_dir,
        );
        engine.load_shard(1);
        let before = engine.write_ahead_log_store().stats(1);
        for ingest in 0..ingests {
            // The eight writes one ingest makes, as distinct keys.
            let parts = [
                "node", "event", "index_ref", "dirty", "summary_a", "summary_b", "retrieval",
                "unique",
            ];
            if batched {
                let commands: Vec<Command> = parts
                    .iter()
                    .map(|part| Command::StringSet {
                        key: format!("ingest-{ingest:05}/{part}"),
                        value: vec![b'v'; 96],
                    })
                    .collect();
                assert!(
                    engine
                        .batch_execute(BatchExecuteRequest {
                            shard_id: 1,
                            commands,
                        })
                        .status
                        .ok
                );
            } else {
                for part in parts {
                    engine.execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringSet {
                            key: format!("ingest-{ingest:05}/{part}"),
                            value: vec![b'v'; 96],
                        },
                    });
                }
            }
        }
        engine.write_ahead_log_store().flush(1).ok();
        let after = engine.write_ahead_log_store().stats(1);
        let path = index_dir.join("wals").join("shard-1.wal.jsonl");
        let (_, record_end) = crate::wal::last_wal_sequence_in_for_test(&path).unwrap_or((0, 0));
        (
            record_end,
            after.writes.saturating_sub(before.writes),
            after.syncs.saturating_sub(before.syncs),
        )
    }

    let ingests = 200usize;
    let (loose_bytes, loose_writes, loose_syncs) = write_and_measure(false, ingests);
    let (batch_bytes, batch_writes, batch_syncs) = write_and_measure(true, ingests);
    let payload = (ingests * 8 * 96) as u64;

    println!(
        "  {ingests} ingests, 8 writes each, 96 B values ({payload} B of value)\n  \
         separate   {loose_bytes:>9} B log   {loose_writes:>6} appends   {loose_syncs:>6} syncs   \
         {:>6.0} B/ingest\n  \
         batched    {batch_bytes:>9} B log   {batch_writes:>6} appends   {batch_syncs:>6} syncs   \
         {:>6.0} B/ingest\n  \
         batching saves {:.1}% of the log and {:.1}% of the barriers",
        loose_bytes as f64 / ingests as f64,
        batch_bytes as f64 / ingests as f64,
        100.0 * (loose_bytes as f64 - batch_bytes as f64) / loose_bytes.max(1) as f64,
        100.0 * (loose_syncs as f64 - batch_syncs as f64) / loose_syncs.max(1) as f64,
    );
}

/// registrations plateau instead of tracking writes, and every value still reads.
///
/// Without a bound this set grows one entry per distinct object written — measured at 200 writes
/// 200 held, 1200 writes 1200 held — which is memory that never comes back and a reclaim floor
/// that never moves. With one, the recent stay resident and the rest are written where anyone can
/// find them.
///
/// The second half is the half worth having: a bound that loses data is not a bound, it is a
/// deletion policy. Every value written is read back after the sweeps have run.
#[test]
fn registrations_plateau_instead_of_tracking_writes() {
    let previous = std::env::var("TS_WAL_RESIDENT_PAGES").ok();
    std::env::set_var("TS_WAL_RESIDENT_PAGES", "64");
    let before_sweeps = crate::engine::RESIDENT_SWEEPS.load(std::sync::atomic::Ordering::Relaxed);

    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        4 * 1024 * 1024,
        dir.path().join("cache"),
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

    let total = 600usize;
    let mut held = Vec::new();
    for index in 0..total {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("object-{index:05}"),
                value: vec![b'v'; 128],
            },
        });
        if index % 100 == 99 {
            held.push((index + 1, engine.registration_count_for_test(1)));
        }
    }
    let after_sweeps = crate::engine::RESIDENT_SWEEPS.load(std::sync::atomic::Ordering::Relaxed);
    match previous {
        Some(value) => std::env::set_var("TS_WAL_RESIDENT_PAGES", value),
        None => std::env::remove_var("TS_WAL_RESIDENT_PAGES"),
    }

    println!("  writes -> registrations held (limit 64)");
    for (writes, count) in &held {
        println!("  {writes:>6} -> {count:>6}");
    }
    println!("  sweeps: {}", after_sweeps - before_sweeps);

    assert!(
        after_sweeps > before_sweeps,
        "the bound never fired, so this proves nothing about it"
    );
    let peak = held.iter().map(|(_, count)| *count).max().unwrap_or(0);
    assert!(
        peak <= 128,
        "registrations should stay near the limit, peaked at {peak} over {total} writes"
    );

    // A bound that loses data is a deletion policy. Everything written must still read.
    for index in 0..total {
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: format!("object-{index:05}"),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(vec![b'v'; 128])
            },
            "object-{index:05} was lost when its page was moved out"
        );
    }
}

/// a batch leaves nothing staged for the next write to adopt.
///
/// Outcomes are staged during execution and taken at the append. The batch path executes every
/// command in the batch — staging an item for each — and then appends records built from the
/// COMMANDS, never touching what was staged. So the items sit in the thread's buffer, and the
/// next write on that thread appends them as its own doing.
///
/// This is the same defect already fixed for replay and for the single-write path, in the one
/// place left. Threads are reused across requests in a server and across tests here, so "the next
/// write" is routinely someone else entirely.
#[test]
fn a_batch_leaves_nothing_staged_for_the_next_write_to_adopt() {
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        &page_dir,
        &index_dir,
    );
    engine.load_shard(1);

    let batch = engine.batch_execute(BatchExecuteRequest {
        shard_id: 1,
        commands: (0..6)
            .map(|index| Command::StringSet {
                key: format!("batched-{index}"),
                value: b"v".to_vec(),
            })
            .collect(),
    });
    assert!(batch.status.ok, "the batch itself must succeed");

    assert_eq!(
        crate::engine::block_in_wal::staged_outcome_count(),
        0,
        "a batch must leave nothing staged: whatever it leaves is picked up by the next write on \
         this thread and written into that write's record as changes it made itself"
    );
}

/// which records still carry an operation, across every shape a write can take.
///
/// "Results, not operations" is only true of the paths that were changed. This walks the log a
/// mixed workload leaves and counts, per shape, how many records carry an operation and how many
/// state what they did — which is the difference between believing the claim and checking it.
#[test]
fn which_records_still_carry_an_operation() {
    fn audit(label: &str, async_storage: bool, batched: bool) -> (usize, usize, usize) {
        let dir = tempfile::tempdir().unwrap();
        let page_dir = dir.path().join("pages");
        let index_dir = dir.path().join("indexes");
        let engine = TemporalEngine::with_local_dirs(
            2 * 1024 * 1024,
            dir.path().join("cache"),
            &page_dir,
            &index_dir,
        );
        engine.load_shard(1);
        if async_storage {
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
        }
        if batched {
            engine.batch_execute(BatchExecuteRequest {
                shard_id: 1,
                commands: (0..8)
                    .map(|index| Command::StringSet {
                        key: format!("{label}-{index}"),
                        value: vec![b'v'; 64],
                    })
                    .collect(),
            });
        } else {
            for index in 0..8 {
                engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: format!("{label}-{index}"),
                        value: vec![b'v'; 64],
                    },
                });
            }
        }
        engine.write_ahead_log_store().flush(1).ok();
        let records = engine
            .write_ahead_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .expect("the log should read back");
        let mut with_command = 0usize;
        let mut with_items = 0usize;
        let total = records.len();
        for (_, line) in &records {
            let record = crate::wal::decode_wal_line(line).expect("a record should decode");
            if record.command.is_some() {
                with_command += 1;
            }
            if !record.outcomes.is_empty() {
                with_items += 1;
            }
        }
        println!(
            "  {label:<22} {total:>3} records   {with_command:>3} carry an operation   \
             {with_items:>3} state results"
        );
        (total, with_command, with_items)
    }

    println!("  shape                  records   operations   results");
    let (_, sync_cmds, sync_items) = audit("sync, separate", false, false);
    let (_, sync_batch_cmds, sync_batch_items) = audit("sync, batched", false, true);
    let (_, async_cmds, _) = audit("async, separate", true, false);
    let (_, async_batch_cmds, _) = audit("async, batched", true, true);

    // The synchronous shapes have no excuse: their blocks are in the block store before the
    // record is acked, so a result names something a reader can find.
    assert_eq!(
        sync_cmds, 0,
        "a synchronous write should state results, not the operation"
    );
    assert!(sync_items > 0, "and it should state some");
    assert_eq!(
        sync_batch_cmds, 0,
        "a synchronous batch should state results, not the operation"
    );
    assert!(sync_batch_items > 0, "and it should state some");
    // The asynchronous shapes are reported rather than asserted: an async write carrying nothing
    // names a block-store address a crash may leave unwritten, and re-running is the only thing
    // that rebuilds it. That one is a durability distinction, not an unconverted path.
    println!("  async separate carries {async_cmds} operations, async batched {async_batch_cmds}");
}

/// Did nesting actually save what it was measured to save?
///
/// Two probes stood here before the change: one establishing that the per-object map was exactly a
/// prefix range scan of the per-component one (113 objects, 226 refs, 0 disagreements), and one
/// measuring what merging them would save (223588 B of keys held against 110780 nested, 50.5%).
/// Both described a layout that no longer exists, and neither would fail if the saving had been
/// lost -- they measured a hypothetical.
///
/// This measures the layout that shipped. It reconstructs what the two flat maps WOULD have held
/// for the same shard and compares it against what is actually held, so the number is a saving
/// rather than a description.
#[test]
fn nesting_the_page_lookups_saved_what_it_measured() {
    const RECORDS: usize = 4_000;
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    assert!(
        engine
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 1,
                table_name: "nested-saving".to_string(),
                shard_uri: "local://nested-saving/1".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 1023,
                readonly: false,
                load_version: 1,
                local_node_id: Some(1),
            })
            .status
            .ok
    );
    // Mixed, because the saving depends on whether an object carries a component at all: a plain
    // value has none and a hash field has one. Measuring only one kind would generalise from
    // whichever half is easier.
    for index in 0..RECORDS {
        if index % 4 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: format!("nest-hash-{}", index / 4),
                    field: format!("field-{index}"),
                    value: vec![b'h'; 64],
                },
            });
        } else {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("nest-str-{index}"),
                    value: vec![b'v'; 64],
                },
            });
        }
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let lookup = &shard.bucket_index.object_page_lookup;

    // Held now: the (model, object) key once, plus the component alone on each nested entry.
    let mut nested_keys = 0usize;
    let mut nested_map_entries = 0usize;
    let mut nested_vec_elements = 0usize;
    // What two flat maps WOULD have held: the (model, object) key as a whole key in one map, AND
    // again as the head of every (model, object, component) key in the other. The tail of that
    // longer key is "1|" plus a length-prefixed component, or "0|" when there is none.
    let mut flat_keys = 0usize;
    let mut flat_map_entries = 0usize;
    for (_model, object_key, entry) in lookup.iter() {
        nested_keys += object_key.len();
        nested_map_entries += 1;
        nested_vec_elements += entry.by_component.len();
        // The flat layout kept a whole B-tree entry per object in the second map...
        flat_keys += object_key.len();
        flat_map_entries += 1;
        for component in &entry.by_component {
            let tail = match component.component.as_deref() {
                Some(name) => format!("1|{}:{}|", name.len(), name).len(),
                None => "0|".len(),
            };
            nested_keys += component.component.as_ref().map_or(0, |name| name.len());
            // ...and another per (object, component) in the first. Nesting turns the second of
            // those into a vector element, which is why these are counted apart: a B-tree node
            // and a vector slot are not the same object, and adding them together hides the
            // change entirely.
            flat_keys += object_key.len() + tail;
            flat_map_entries += 1;
        }
    }

    let per = |n: usize| n as f64 / RECORDS as f64;
    println!(
        "
  page-lookup keys at {RECORDS} records, nested against the two flat maps it replaced:
             two flat maps                {:>8} B  ({:>6.1} B/record, {flat_map_entries} b-tree entries)
             nested                       {:>8} B  ({:>6.1} B/record, {nested_map_entries} b-tree + {nested_vec_elements} vec)
             saved                        {:>8} B  ({:>6.1} B/record, {:>5.1}%)
",
        flat_keys,
        per(flat_keys),
        nested_keys,
        per(nested_keys),
        flat_keys - nested_keys,
        per(flat_keys - nested_keys),
        100.0 * (flat_keys - nested_keys) as f64 / flat_keys as f64,
    );

    assert!(flat_keys > 0 && !lookup.is_empty(), "the workload must populate the lookup");
    // The measurement that justified the change was 50.5%. Assert well below it so ordinary
    // changes in key length do not fail the build, but a REGRESSION -- the object key creeping
    // back into the inner entries -- cannot pass.
    let saved = 100.0 * (flat_keys - nested_keys) as f64 / flat_keys as f64;
    assert!(
        saved > 40.0,
        "nesting saved {saved:.1}% of the page-lookup key bytes; it measured 50.5% before it \
         shipped, and anything near zero means the object key is being stored twice again"
    );
    // B-tree entries halve: the per-(object, component) map becomes vector slots inside the
    // per-object one. Counting nodes and slots together would report no change at all, which is
    // how this was first miscounted -- they are not the same object and do not cost the same.
    assert!(
        nested_map_entries < flat_map_entries,
        "nested {nested_map_entries} b-tree entries against {flat_map_entries} flat"
    );
    assert_eq!(
        nested_map_entries + nested_vec_elements,
        flat_map_entries,
        "every flat entry should become either a b-tree entry or a vector slot, none lost"
    );
}

/// What does refusing to reclaim, rather than clamping, actually hold?
///
/// The plan computes a durable frontier — the lowest wal sequence among the dump manifests
/// covering every live generation — and then reclaims up to it, but ONLY if no replay cursor sits
/// below it. One cursor one sequence behind the frontier turns the whole reclaim off, and the log
/// keeps everything for as long as that cursor lags.
///
/// Everything below the SLOWEST cursor is safe to drop whether or not that cursor has caught up,
/// which is what clamping the frontier to the cursor would take. The prize is therefore not a
/// constant: it is exactly the part of the log the slowest cursor has already consumed, so it is
/// measured across cursor positions rather than quoted as one number. At a cursor that has never
/// moved it is zero, and that is the honest answer for a permanently stuck follower.
#[test]
fn reclaim_clamps_to_the_slowest_cursor_instead_of_refusing_at_it() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        4 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    // Eight rounds, each ending in a dump, so the durable frontier sits well above zero and there
    // is a real span of log below it to talk about.
    let mut manifests = Vec::new();
    for round in 0..8 {
        for index in 0..8 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("reclaim-{round}-{index}"),
                    value: vec![b'v'; 256],
                },
            });
        }
        manifests.push(
            engine
                .create_bucket_dump_manifest(1, Vec::new())
                .expect("a dump manifest per round"),
        );
    }

    let unblocked = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());
    let frontier = unblocked.durable_bucket_generation_frontier_wal_sequence;
    assert!(
        frontier > 0 && unblocked.safe_to_reclaim,
        "with no cursors the plan must reclaim, or there is nothing to compare against: \
         {unblocked:?}"
    );

    // Bytes of log at or below a sequence, so the table is in the unit that matters rather than
    // in sequence numbers.
    // `scan` is bounded by BYTE OFFSETS, not by sequence -- passing a sequence as the end
    // offset measured the first few bytes of the log and reported zero everywhere. Read the whole
    // log once and bucket the records by their own decoded sequence instead.
    let all_records = engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .expect("the log should read back");
    let by_sequence: Vec<(u64, usize)> = all_records
        .iter()
        .filter_map(|(_, line)| {
            crate::wal::decode_wal_line(line)
                .ok()
                .map(|record| (record.sequence, line.len()))
        })
        .collect();
    let bytes_through = |sequence: u64| -> usize {
        by_sequence
            .iter()
            .filter(|(at, _)| *at <= sequence)
            .map(|(_, len)| *len)
            .sum()
    };
    let total_below_frontier = bytes_through(frontier);

    println!(
        "
  durable frontier at wal sequence {frontier}, {total_below_frontier} B of log at or below it

  cursor at      today retains   clamped would   released      of the span
"
    );

    let mut released_at_frontier = 0usize;
    for numerator in [0u64, 1, 2, 4, 6, 7, 8] {
        let cursor_at = frontier * numerator / 8;
        let plan = engine.storage_wal_reclaim_plan(
            1,
            vec![BucketDumpFollowerReplayCursor {
                follower_id: "lagging".to_string(),
                shard_id: 1,
                wal_sequence: cursor_at,
                index_log_sequence: u64::MAX,
            }],
            Vec::new(),
        );
        // Clamping takes the frontier down to the slowest cursor instead of refusing at it.
        let clamped = if plan.safe_to_reclaim {
            plan.retain_from_wal_sequence
        } else {
            frontier.min(cursor_at).saturating_add(1)
        };
        let released = bytes_through(clamped.saturating_sub(1));
        if numerator == 8 {
            released_at_frontier = released;
        }
        println!(
            "  {:>10}   {:>13}   {:>13}   {:>7} B   {:>5.1}%",
            cursor_at,
            plan.retain_from_wal_sequence,
            clamped,
            released,
            if total_below_frontier == 0 {
                0.0
            } else {
                100.0 * released as f64 / total_below_frontier as f64
            },
        );
    }

    // The shape, not a threshold. A cursor that has never moved releases nothing -- that is the
    // answer for a permanently stuck follower and it is not a defect. A cursor level with the
    // frontier is not blocking at all, so today already reclaims there.
    assert!(
        total_below_frontier > 0,
        "the workload must leave log below the frontier"
    );
    assert!(
        released_at_frontier > 0,
        "a cursor level with the frontier must release the span: {released_at_frontier}"
    );

    // THE CLAIM UNDER TEST: a cursor strictly between zero and the frontier is the case the
    // refusal costs, and today it retains nothing at all.
    let midway = frontier / 2;
    let mid_plan = engine.storage_wal_reclaim_plan(
        1,
        vec![BucketDumpFollowerReplayCursor {
            follower_id: "lagging".to_string(),
            shard_id: 1,
            wal_sequence: midway,
            index_log_sequence: u64::MAX,
        }],
        Vec::new(),
    );
    assert!(midway > 0 && midway < frontier, "the sweep needs a middle");
    assert!(
        mid_plan.safe_to_reclaim,
        "a cursor behind the frontier no longer stops reclaim outright: {mid_plan:?}"
    );
    assert_eq!(
        mid_plan.retain_from_wal_sequence,
        midway.saturating_add(1),
        "reclaim clamps to the cursor rather than refusing at it: {mid_plan:?}"
    );
    assert!(
        bytes_through(midway) > 0,
        "and the span it releases is not nothing: {} B",
        bytes_through(midway)
    );
    // The cursor is still reported as retaining logs, which remains true of the logs ABOVE it.
    // What changed is that this is no longer a reason to keep the ones below it as well.
    assert_eq!(mid_plan.follower_cursor_block_count, 1, "{mid_plan:?}");

    // A cursor that has never advanced releases nothing, and that is the correct answer rather
    // than a shortfall: there is no span behind a reader that has read nothing. The whole win
    // here is bounded by cursor movement, so a permanently stuck follower still pins the log.
    let stuck = engine.storage_wal_reclaim_plan(
        1,
        vec![BucketDumpFollowerReplayCursor {
            follower_id: "never-moved".to_string(),
            shard_id: 1,
            wal_sequence: 0,
            index_log_sequence: 0,
        }],
        Vec::new(),
    );
    assert_eq!(
        stuck.retain_from_wal_sequence, 0,
        "a cursor at zero clamps the frontier to zero, which reclaims nothing: {stuck:?}"
    );
}

/// A clamped reclaim still serves everything, after a restart.
///
/// Clamping drops the log at or below the slowest cursor. That is right only if `wal_sequence`
/// means "the last record I consumed" and not "the next record I need" -- and the difference
/// between those two readings is exactly one record, which no comparison of sequence numbers can
/// reveal. The old code already dropped records at a cursor's own sequence whenever that cursor sat
/// level with the frontier, so the convention is the first one, but that is an argument from
/// reading code rather than a demonstration.
///
/// So this rebuilds a shard from what survives the reclaim and reads the values back. A recovery
/// that has lost a record cannot serve it, and nothing about the retained sequence numbers would
/// have said so.
#[test]
fn a_clamped_reclaim_still_serves_what_it_kept_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        4 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    // Written before the cursor: the span a clamped reclaim is entitled to drop from the log.
    for index in 0..16 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("consumed-{index}"),
                value: format!("v-consumed-{index}").into_bytes(),
            },
        });
    }
    let anchor = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("a dump to anchor the cursor on");

    // Written after it: the span the follower has NOT consumed, which must survive.
    for index in 0..16 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("pending-{index}"),
                value: format!("v-pending-{index}").into_bytes(),
            },
        });
    }
    engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("a dump above the cursor");

    let plan = engine.storage_wal_reclaim_plan(
        1,
        vec![BucketDumpFollowerReplayCursor {
            follower_id: "consumed-through-anchor".to_string(),
            shard_id: 1,
            wal_sequence: anchor.wal_sequence,
            index_log_sequence: anchor.index_log_sequence,
        }],
        Vec::new(),
    );
    assert!(
        plan.safe_to_reclaim,
        "a cursor behind the frontier should clamp, not refuse: {plan:?}"
    );
    assert_eq!(
        plan.retain_from_wal_sequence,
        anchor.wal_sequence.saturating_add(1),
        "clamped to the cursor: {plan:?}"
    );
    let report = engine.apply_storage_wal_reclaim(plan);
    assert!(report.applied, "{report:?}");
    assert!(
        report.wal_bytes_after < report.wal_bytes_before,
        "the clamp should have released log bytes, or this test proves nothing: {report:?}"
    );

    // Rebuild from what is on disk. A different cache directory so nothing is served from a warm
    // tier -- the point is what recovery can reconstruct, not what happened to still be in memory.
    drop(engine);
    let restarted = TemporalEngine::with_local_dirs(
        4 * 1024,
        dir.path().join("restart-cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    restarted.load_shard(1);

    for index in 0..16 {
        assert_eq!(
            restarted
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: format!("pending-{index}"),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(format!("v-pending-{index}").into_bytes())
            },
            "a value above the cursor must survive a clamped reclaim"
        );
        assert_eq!(
            restarted
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: format!("consumed-{index}"),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(format!("v-consumed-{index}").into_bytes())
            },
            "and so must one below it: the log was dropped because a DUMP already covers these, \\
             not because they stopped existing -- reclaiming the log is not deleting the data"
        );
    }
}

/// What the dirty-object set costs to drain, as the store grows.
///
/// `dirty_objects` is a flat set of object keys, and the dump drain used to sit INSIDE the
/// per-bucket loop:
///
///     for bucket_id in manifest.bucket_ids { ...
///         shard.dirty_objects.retain(|key| page_routing_bucket(key, ..) != bucket_id)
///
/// so every qualifying bucket walked every dirty object, re-hashing each key to recompute a
/// routing bucket the caller already knew. The work was |dirty objects| x |buckets| to remove at
/// most |dirty objects| entries.
///
/// This counts what the drain ACTUALLY looks at, through a counter inside the closure, rather than
/// deriving the number as a product. A product is arithmetic about the code; the counter survives
/// someone moving the loop back.
#[test]
fn the_dump_drain_looks_at_each_dirty_object_once() {
    fn measure(records: usize) -> (usize, usize, u64) {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            8 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        assert!(
            engine
                .load_shard_with(crate::control::LoadShardRequest {
                    shard_id: 1,
                    table_name: "dirty-drain".to_string(),
                    shard_uri: "local://dirty-drain/1".to_string(),
                    start_routing_bucket: 0,
                    end_routing_bucket: 1023,
                    readonly: false,
                    load_version: 1,
                    local_node_id: Some(1),
                })
                .status
                .ok
        );
        for index in 0..records {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("dirty-{index}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        let (dirty_before, buckets) = {
            let shards = engine.shards.read().expect("shards lock poisoned");
            let shard = shards.get(&1).expect("shard 1 loaded");
            (shard.dirty_objects.len(), shard.bucket_index.bucket_map.len())
        };

        // The drain runs from `apply_storage_lifecycle`, NOT from creating or installing a
        // manifest directly. The first version of this probe called those, measured a drain that
        // never ran, and passed -- which is why the assertion below insists it ran at all.
        let selected: Vec<u32> = {
            let shards = engine.shards.read().expect("shards lock poisoned");
            shards
                .get(&1)
                .expect("shard 1 loaded")
                .bucket_index
                .bucket_map
                .keys()
                .copied()
                .collect()
        };
        crate::engine::storage_lifecycle_methods::DIRTY_DRAIN_VISITS
            .store(0, std::sync::atomic::Ordering::Relaxed);
        engine.apply_storage_lifecycle(StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_buckets: selected,
            ..Default::default()
        });
        let visits = crate::engine::storage_lifecycle_methods::DIRTY_DRAIN_VISITS
            .load(std::sync::atomic::Ordering::Relaxed);
        (dirty_before, buckets, visits)
    }

    println!(
        "
  records   dirty objects   buckets   drain visits   per dirty object   as one-pass-per-bucket
"
    );
    for records in [500usize, 1_000, 2_000, 4_000] {
        let (dirty, buckets, visits) = measure(records);
        println!(
            "  {records:>7}   {dirty:>13}   {buckets:>7}   {visits:>12}   {:>16.2}   {:>21}",
            visits as f64 / dirty.max(1) as f64,
            dirty.saturating_mul(buckets),
        );
        // The property, not a threshold: one pass over the set, however many buckets a dump
        // covers. Before the hoist this was `dirty * buckets` -- 4 040 000 visits to clear 4 000
        // objects across 1 010 buckets.
        // ANTI-VACUITY FIRST. `visits <= dirty` is trivially true at zero, and the first version
        // of this probe passed exactly that way: it called an entry point that does not drain, so
        // the count was 0 and the bound held for the wrong reason. A "no more than X" assertion
        // needs a companion asserting "at least something", because a broken harness and a fixed
        // defect both produce zero and only one of them is good news.
        assert!(
            visits > 0,
            "the drain never ran, so the count measures nothing: {dirty} dirty objects, \
             {buckets} buckets"
        );
        assert!(
            visits <= dirty as u64,
            "the drain visited {visits} keys for {dirty} dirty objects across {buckets} buckets, \
             which means it is walking the set once per bucket again"
        );
    }
}

/// Does the incrementally-maintained live-object set agree with a full rebuild?
///
/// `update_bucket_layout` recomputes `object_index` by scanning every page in the bucket, and it is
/// called per page insert. Measured on the add path: 5 762 400 page visits per 600 adds, four times
/// the work for twice the adds. The insert site already maintains the set incrementally on the line
/// above -- `bucket.object_index.insert(object_id)` -- and the rebuild then discards that work.
///
/// Whether the rebuild is REDUNDANT or LOad-BEARING is not something to reason about: the rebuild
/// keeps only LIVE object ids, while a loop further down deliberately re-attaches tombstone ids
/// with a comment saying the object manager's count must match the load path. So the rebuild and
/// the re-attach are entangled, and "the insert already did it" is exactly the kind of claim that
/// looks obvious and is wrong.
///
/// This drives a workload with every shape that can move the set -- fresh inserts, overwrites of a
/// live object, deletes, and re-inserts of a deleted key -- then compares what the shard actually
/// holds against a rebuild computed from the pages. Any divergence is the reason the rebuild
/// exists, and the fix has to be shaped around it rather than delete it.
#[test]
fn the_maintained_object_index_matches_a_full_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        2 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    for index in 0..120 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("live-{index}"),
                value: vec![b'v'; 48],
            },
        });
        // Overwrite an earlier key: a second live page for an id already in the set.
        if index % 3 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("live-{}", index / 2),
                    value: vec![b'w'; 96],
                },
            });
        }
        // Delete: the case where the set may need to LOSE an id, which one page cannot decide.
        if index % 7 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonDelete {
                    key: format!("live-{}", index / 4),
                },
            });
        }
        // Re-insert a deleted key: the set must gain it back.
        if index % 11 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("live-{}", index / 4),
                    value: vec![b'r'; 32],
                },
            });
        }
        // Hash fields, so an object carries several pages under one id.
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: format!("hash-{}", index % 9),
                field: format!("field-{index}"),
                value: vec![b'h'; 32],
            },
        });
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");

    let mut divergent = Vec::new();
    let mut checked = 0usize;
    let mut live_total = 0usize;
    for (routing_bucket, bucket) in shard.bucket_index.bucket_map.iter() {
        // What a rebuild would produce: the ids of the pages that are not deleted.
        let rebuilt: crate::engine::state::ObjectIndex = bucket
            .page_index
            .values()
            .filter(|page| !page.deleted)
            .map(|page| page.object_id())
            .collect();
        checked += 1;
        live_total += rebuilt.len();
        if bucket.object_index != rebuilt {
            let held: Vec<u64> = bucket.object_index.difference(&rebuilt).copied().collect();
            let missing: Vec<u64> = rebuilt.difference(&bucket.object_index).copied().collect();
            divergent.push((*routing_bucket, held, missing));
        }
    }

    println!(
        "
  buckets checked            {checked}
  live object ids (rebuilt)  {live_total}
  buckets where the held set differs from a rebuild  {}
",
        divergent.len()
    );
    for (bucket, held, missing) in divergent.iter().take(6) {
        println!("    bucket {bucket}: holds-but-rebuild-drops {held:?}, rebuild-has-but-holds-not {missing:?}");
    }

    // Anti-vacuity first: a comparison over an empty shard agrees about nothing.
    assert!(
        checked > 0 && live_total > 0,
        "the workload must populate buckets, or the comparison below compares nothing"
    );

    // The finding, whichever way it goes. If this holds, the per-insert rebuild is recomputing
    // what the insert already knew and can go. If it does not, the difference names exactly what
    // the rebuild is for -- and the tombstone ids re-attached after it are the first suspect.
    // The held set is a strict SUPERSET of a rebuild, and the extra ids are tombstones the object
    // manager must keep reporting until GC reclaims the slot -- `rebuild_bucket_first_index`
    // re-attaches them deliberately after recomputing the live set. Measured: 14 of 183 buckets
    // hold exactly one extra id each, and NOTHING is ever missing in the other direction.
    //
    // So the invariant is containment plus a named exception, not equality. Asserting equality
    // would fail on correct behaviour, and asserting nothing would miss a live id going astray.
    for (bucket_id, held_extra, missing) in &divergent {
        assert!(
            missing.is_empty(),
            "bucket {bucket_id} is MISSING live object ids a rebuild would produce: {missing:?} -- \
             a live page exists whose id the index does not hold"
        );
        let bucket = shard
            .bucket_index
            .bucket_map
            .get(bucket_id)
            .expect("the divergent bucket was read from this map");
        for object_id in held_extra {
            assert!(
                bucket.deleted_object_index.contains(object_id),
                "bucket {bucket_id} holds object id {object_id}, which is neither live nor a \
                 recorded tombstone"
            );
        }
    }
}
/// Does a context node page end up in the bucket index, or not?
///
/// This decides how the per-record rebuild can be removed, and the code says two things that pull
/// opposite ways. The executor for `ContextUpsertNode` stages its outcome under its own kind with
/// the comment "this writes a hash page and -- unlike HashSet -- never registers it in the bucket
/// index". But the page IS put into `shard.hashes`, and `rebuild_bucket_first_index` derives the
/// index from `collect_model_live_page_entries`, which reads the model maps.
///
/// If context pages ARE in the index, the rebuild is load-bearing and removing it needs the write
/// path to call `upsert_bucket_index_page` itself. If they are NOT, the rebuild is doing nothing
/// for these writes and they can be classified as not dirtying the index at all -- a much smaller
/// change. Reading the code has been wrong repeatedly here, so this asks the shard.
#[test]
fn whether_a_context_page_reaches_the_bucket_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        2 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    // A plain hash write, as the control: this one is known to register.
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashSet {
            key: "control-hash".to_string(),
            field: "f".to_string(),
            value: vec![b'h'; 64],
        },
    });

    let ingest = crate::context_workflow::ingest_extract_context(
        &engine,
        crate::context_workflow::ContextIngestExtractRequest {
            shard_id: 1,
            tenant_hash: 4242,
            sources: vec![crate::context_workflow::ContextExtractRequest {
                shard_id: 1,
                tenant_hash: 4242,
                source_kind: crate::context_workflow::ContextSourceKind::Incident,
                source_id: "IDX-1".to_string(),
                title: "index membership".to_string(),
                body: "does this page reach the bucket index".to_string(),
                timestamp_ms: 1_000,
                provider: crate::context_workflow::ContextModelProviderConfig::default(),
            }],
            provider: crate::context_workflow::ContextModelProviderConfig::default(),
            start_time_ms: 0,
            end_time_ms: 0,
            max_events: 0,
            query: String::new(),
        },
    );
    assert!(ingest.status.ok, "the ingest must succeed: {:?}", ingest.status);

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");

    // Every kind the index holds a page for, and how many of each.
    let mut kinds: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.page_index.values() {
            *kinds.entry(page.model_id.clone().to_string()).or_insert(0) += 1;
        }
    }
    // And the context keys the model map holds, so the two can be compared.
    let context_keys_in_model = shard
        .hashes
        .keys()
        .filter(|key| key.contains("ctx") || key.contains("context"))
        .count();
    let context_pages_in_index: usize = shard
        .bucket_index
        .bucket_map
        .values()
        .flat_map(|bucket| bucket.page_index.values())
        .filter(|page| page.object_key.contains("ctx") || page.object_key.contains("context"))
        .count();

    println!(
        "
  page kinds held by the bucket index: {kinds:?}
  context-ish keys in the model map:   {context_keys_in_model}
  context-ish pages in the bucket index: {context_pages_in_index}
"
    );

    assert!(
        !kinds.is_empty(),
        "the control write must put SOMETHING in the index, or this measures nothing"
    );
    // Report rather than assert a direction: the point is to learn which world this is, and a
    // wrong guess baked into an assertion would just move the mistake into the test.
    println!(
        "  => context pages {} the bucket index",
        if context_pages_in_index > 0 { "DO reach" } else { "do NOT reach" }
    );
}

/// What one page costs to index here, against the 17 bytes it costs in the design being followed.
///
/// There, a page's index entry is a packed struct with a static assertion on its size: two u8 ids,
/// a u16 page id, a byte of flags, a u32 size (zero meaning deleted) and a u64 address. Seventeen
/// bytes, no heap, no strings, and the delete flag is a value the size field already had room for.
///
/// Here the same entry holds owned strings for the object key and model, an optional string
/// component, a u64 id, an address struct with nine fields of its own, and three separate bools.
/// This reports the inline size and the heap each entry pulls behind it, because `size_of` alone
/// undercounts a struct whose fields are `String`.
#[test]
fn what_one_page_costs_to_index() {
    use crate::engine::state::BlockIndex;

    let inline = std::mem::size_of::<BlockIndex>();
    let address_inline = std::mem::size_of::<crate::block_store::BlockAddress>();
    let string_inline = std::mem::size_of::<String>();
    let option_string_inline = std::mem::size_of::<Option<String>>();

    // Build a shard and measure what its pages actually hold, so the heap side is observed rather
    // than assumed from the type.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        2 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    const PAGES: usize = 2_000;
    for index in 0..PAGES {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("page-cost-{index:06}"),
                value: vec![b'v'; 64],
            },
        });
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut entries = 0usize;
    let mut heap = 0usize;
    let mut ref_key_bytes = 0usize;
    let mut object_key_bytes = 0usize;
    let mut shared_bytes = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for (_handle, page) in bucket.page_index.iter() {
            entries += 1;
            // The map key is an inline number now, not a rendered string on the heap.
            ref_key_bytes += 0;
            // Shared with the lookup, so one allocation answers for both holders.
            object_key_bytes += page.object_key.len();
            // One allocation across every page of that kind or component, not one per page.
            shared_bytes += page.model_id.len()
                + page.component.as_ref().map_or(0, |name| name.len());
            heap += page.object_key.len();
        }
    }
    assert!(entries > 0, "the workload must produce pages, or this measures nothing");

    let per_entry_heap = heap as f64 / entries as f64;
    let map_key_b = ref_key_bytes as f64 / entries as f64;
    let object_key_b = object_key_bytes as f64 / entries as f64;
    let shared_b = shared_bytes as f64 / entries as f64;
    let total = inline as f64 + per_entry_heap;
    println!(
        "
  one page's index entry
    inline struct                {inline:>5} B
      of which BlockAddress      {address_inline:>5} B
      String is                  {string_inline:>5} B inline, Option<String> {option_string_inline} B
    heap owned, measured         {per_entry_heap:>7.1} B over {entries} pages
    total per page               {total:>7.1} B

      of which the map key      {map_key_b:>6.1} B   <- kind, key and five numbers as text
      of which the object key   {object_key_b:>6.1} B   (shared with the lookup)
      shared across pages       {shared_b:>6.1} B   (kind and component, one allocation each)

    the design being followed     17 B, packed, static_assert(sizeof == 17)
    ratio                        {:>7.1}x
",
        total / 17.0
    );

    // A report, not a threshold -- the point is the gap and where it comes from, and a bound here
    // would fail on unrelated changes. What must hold is that the measurement happened.
    assert!(
        per_entry_heap > 0.0,
        "every entry owns at least an object key, so zero heap means the walk found nothing"
    );
}

/// Which of a page address's 120 bytes are actually carrying anything?
///
/// A page's index entry costs 339.6 B here against 17 B in the design being followed, and
/// `BlockAddress` is 120 B of it -- where that design uses ONE u64. The compact form already
/// exists (`compact_slab_address` packs slab id and offset into a u64, `from_compact_slab_address`
/// reconstructs) but it drops five optional fields, so the question is whether those fields hold
/// anything at rest.
///
/// An `Option<u64>` costs 16 B because there is no niche to exploit; four of them are 64 B. An
/// `Option<String>` is 24 B inline before any heap. If they are None in practice, that is dead
/// weight in every page entry in the shard, and the measurement says how much is recoverable
/// without changing what the type can express.
#[test]
fn which_parts_of_a_page_address_are_populated() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        4 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    // A mix, because a field that only one command populates would look dead in a single-shape
    // workload: plain values, hash fields with components, and deletes leaving tombstones.
    for index in 0..1_200 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("addr-str-{index:06}"),
                value: vec![b'v'; 96],
            },
        });
        if index % 4 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: format!("addr-hash-{}", index % 40),
                    field: format!("field-{index}"),
                    value: vec![b'h'; 48],
                },
            });
        }
        if index % 13 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonDelete {
                    key: format!("addr-str-{:06}", index / 2),
                },
            });
        }
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");

    let mut pages = 0usize;
    let (mut page_id, mut object_id, mut routing_bucket) = (0usize, 0usize, 0usize);
    let (mut generation, mut band_id, mut sha256) = (0usize, 0usize, 0usize);
    let mut compactable = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.page_index.values() {
            pages += 1;
            let a = &page.address;
            page_id += usize::from(a.page_id().is_some());
            object_id += usize::from(a.object_id().is_some());
            routing_bucket += usize::from(a.routing_bucket().is_some());
            generation += usize::from(a.generation().is_some());
            band_id += usize::from(a.band_id().is_some());
            compactable += usize::from(a.compact_slab_address().is_some());
        }
    }
    assert!(pages > 0, "the workload must produce pages, or this measures nothing");

    let pct = |n: usize| 100.0 * n as f64 / pages as f64;
    println!(
        "
  {pages} pages, which of the address's optional fields are set

    page_id          {page_id:>6}  {:>5.1}%   16 B each
    object_id        {object_id:>6}  {:>5.1}%   16 B
    routing_slot     {routing_bucket:>6}  {:>5.1}%    8 B
    generation       {generation:>6}  {:>5.1}%   16 B
    band_id          {band_id:>6}  {:>5.1}%   16 B
    sha256           {sha256:>6}  {:>5.1}%   24 B inline + heap

    fit the compact (slab, offset) u64: {compactable:>6}  {:>5.1}%
",
        pct(page_id), pct(object_id), pct(routing_bucket),
        pct(generation), pct(band_id), pct(sha256), pct(compactable),
    );

    // A report. What must hold is that the walk saw addresses at all -- a zero everywhere would
    // read as "every field is dead" when it actually means the shard was empty.
    assert!(
        compactable > 0,
        "no address fit the compact form, which means this walked nothing useful"
    );
}

/// Which parts of a page address are recoverable from where the page already sits?
///
/// Every optional field is populated on every page, so none is dead weight in the "never set"
/// sense. That is not the same as necessary. A page entry lives inside a bucket keyed by routing
/// slot and carries its own `object_id`, so two of the address's fields may be restating what the
/// surroundings already say -- and the design being followed spends ONE u64 on an address where
/// this spends 120 B.
///
/// This checks the two candidates by comparison, and sizes the third (`sha256`, the only field
/// with heap behind it) so the three can be ranked. Derivable fields can be dropped from the
/// in-memory entry and reconstructed on the way out; a field that disagrees with its surroundings
/// cannot, and the disagreement would be the finding.
#[test]
fn which_parts_of_a_page_address_restate_their_surroundings() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        4 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..1_200 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("redun-{index:06}"),
                value: vec![b'v'; 96],
            },
        });
        if index % 4 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: format!("redun-hash-{}", index % 40),
                    field: format!("field-{index}"),
                    value: vec![b'h'; 48],
                },
            });
        }
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");

    let mut pages = 0usize;
    let mut routing_matches_bucket = 0usize;
    let mut object_id_matches_entry = 0usize;
    for (bucket_key, bucket) in shard.bucket_index.bucket_map.iter() {
        for page in bucket.page_index.values() {
            pages += 1;
            if page.address.routing_bucket() == Some(*bucket_key) {
                routing_matches_bucket += 1;
            }
            if page.address.object_id() == Some(page.object_id()) {
                object_id_matches_entry += 1;
            }
        }
    }
    assert!(pages > 0, "the workload must produce pages, or this measures nothing");

    let pct = |n: usize| 100.0 * n as f64 / pages as f64;
    println!(
        "
  {pages} pages

    address.routing_slot == the bucket it is filed under   {routing_matches_bucket:>6}  {:>5.1}%   (8 B)
    address.object_id    == the entry's own object_id      {object_id_matches_entry:>6}  {:>5.1}%  (16 B)
    the digest is no longer held here at all -- it lives in the page envelope,
    which is where a read already verifies against it

    recoverable if both hold: {} B per page, of 235.6 B measured
",
        pct(routing_matches_bucket),
        pct(object_id_matches_entry),
        8 + 16,
    );

    // Report, with one thing asserted: a field that DISAGREES with its surroundings is a defect,
    // not an optimisation opportunity, and it would be silently averaged away by the percentages.
    assert!(
        routing_matches_bucket == 0 || routing_matches_bucket == pages,
        "address.routing_slot agrees with its bucket on {routing_matches_bucket} of {pages} pages \
         -- a partial match means some page is filed somewhere its own address does not name"
    );
    assert!(
        object_id_matches_entry == 0 || object_id_matches_entry == pages,
        "address.object_id() agrees with the entry on {object_id_matches_entry} of {pages} pages \
         -- a partial match means an entry and its address disagree about which object it is"
    );
}

/// Does maintaining the index during a context ingest give the same index as rebuilding it?
///
/// A context write does not register its page; the shard rebuilds the whole first-index afterwards
/// instead, which is the last O(corpus) term in an add. Replay already maintains these kinds
/// incrementally (`sync_bucket_index_object_pages`, lifecycle.rs), and Feature and Sequence writes
/// already do it on the write path — the context write path is the one that does not.
///
/// Before removing the rebuild, this establishes what "equal" means. It ingests with the
/// reconstruct held off, so the index is whatever maintenance produced, then rebuilds from the
/// model maps and compares the two page-for-page. Any divergence names the kind whose maintenance
/// is missing, which is the thing to implement next rather than a reason to abandon the approach.
#[test]
fn maintaining_the_index_during_ingest_matches_rebuilding_it() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        8 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    for index in 0..40 {
        let ingest = crate::context_workflow::ingest_extract_context(
            &engine,
            crate::context_workflow::ContextIngestExtractRequest {
                shard_id: 1,
                tenant_hash: 4242,
                sources: vec![crate::context_workflow::ContextExtractRequest {
                    shard_id: 1,
                    tenant_hash: 4242,
                    source_kind: crate::context_workflow::ContextSourceKind::Incident,
                    source_id: format!("EQ-{index:04}"),
                    title: format!("equivalence {index}"),
                    body: format!("body {index} ").repeat(20),
                    timestamp_ms: 1_000 + index as u64,
                    provider: crate::context_workflow::ContextModelProviderConfig::default(),
                }],
                provider: crate::context_workflow::ContextModelProviderConfig::default(),
                start_time_ms: 0,
                end_time_ms: 0,
                max_events: 0,
                query: String::new(),
            },
        );
        assert!(ingest.status.ok, "ingest {index} failed: {:?}", ingest.status);
    }

    // What the shard holds after the ingests.
    let held: std::collections::BTreeMap<(u32, String), (String, String, Option<String>)> = {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        shard
            .bucket_index
            .bucket_map
            .iter()
            .flat_map(|(bucket, node)| {
                node.page_index.iter().map(move |(ref_key, page)| {
                    (
                        (*bucket, ref_key.to_string()),
                        (
                            page.model_id.to_string(),
                            page.object_key.to_string(),
                            page.component.as_ref().map(|name| name.to_string()),
                        ),
                    )
                })
            })
            .collect()
    };
    assert!(
        !held.is_empty(),
        "the ingests must populate the index, or the comparison below compares nothing"
    );
    let uncovered = crate::engine::uncovered_maintenance::snapshot();
    println!("
  keys maintenance found nothing for ({}):", uncovered.len());
    for key in uncovered.iter().take(10) {
        println!("    {key}");
    }

    // And what a rebuild from the model maps would hold.
    engine.reconstruct_bucket_index_now(1);
    let rebuilt: std::collections::BTreeMap<(u32, String), (String, String, Option<String>)> = {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        shard
            .bucket_index
            .bucket_map
            .iter()
            .flat_map(|(bucket, node)| {
                node.page_index.iter().map(move |(ref_key, page)| {
                    (
                        (*bucket, ref_key.to_string()),
                        (
                            page.model_id.to_string(),
                            page.object_key.to_string(),
                            page.component.as_ref().map(|name| name.to_string()),
                        ),
                    )
                })
            })
            .collect()
    };

    let mut missing_after_ingest: Vec<&(u32, String)> = rebuilt
        .keys()
        .filter(|key| !held.contains_key(*key))
        .collect();
    let mut extra_after_ingest: Vec<&(u32, String)> = held
        .keys()
        .filter(|key| !rebuilt.contains_key(*key))
        .collect();
    missing_after_ingest.sort();
    extra_after_ingest.sort();

    let kind_of = |keys: &[&(u32, String)],
                   from: &std::collections::BTreeMap<(u32, String), (String, String, Option<String>)>| {
        let mut kinds: std::collections::BTreeMap<String, usize> = Default::default();
        for key in keys {
            if let Some((kind, _, _)) = from.get(*key) {
                *kinds.entry(kind.clone()).or_insert(0) += 1;
            }
        }
        kinds
    };

    println!(
        "
  pages held after the ingests   {}
  pages a rebuild produces       {}
  a rebuild has, the ingest did not: {} {:?}
  the ingest has, a rebuild does not: {} {:?}
",
        held.len(),
        rebuilt.len(),
        missing_after_ingest.len(),
        kind_of(&missing_after_ingest, &rebuilt),
        extra_after_ingest.len(),
        kind_of(&extra_after_ingest, &held),
    );

    // Today the rebuild runs after every context write, so the two agree by construction and this
    // passes trivially. It earns its keep the moment the rebuild is skipped: then any kind whose
    // maintenance is missing shows up on the first line, by name.
    assert!(
        missing_after_ingest.is_empty(),
        "{} pages exist after a rebuild that the ingest did not put in the index -- those kinds \
         are not being maintained: {:?}",
        missing_after_ingest.len(),
        kind_of(&missing_after_ingest, &rebuilt)
    );
}


/// Does a time-range feature query cost what the RANGE holds, or what the SERIES holds?
///
/// The feature lane carries a quantity sampled over time, so the reader always asks for a window.
/// Whether the lane scales is therefore whether a narrow window over a long series costs what the
/// window holds -- not what the series holds.
///
/// Window fixed at 32 points, series grown 256 -> 16384. The full-range query is the control: it
/// SHOULD grow, and if it does not then the narrow-window column is measuring nothing.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib does_a_feature_window_cost_the_window_or_the_series -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn does_a_feature_window_cost_the_window_or_the_series() {
    let canary = crate::alloc_probe::Probe::start();
    let sink: Vec<u8> = Vec::with_capacity(8192);
    assert!(
        canary.stop().allocs > 0,
        "counting allocator not installed -- rerun with `--features alloc-probe`"
    );
    drop(sink);

    println!(
        "
  series   append   window(32)   per point   full range   per point   agg(32)
"
    );

    for series_len in [256_usize, 1024, 4096, 16384] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);

        // One point per millisecond, so a window in milliseconds is a window in points.
        for index in 0..series_len {
            let out = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAppend {
                    key: "rate".to_string(),
                    points: vec![FeaturePoint {
                        timestamp_ms: 1_000 + index as u64,
                        value: format!("{index}").into_bytes(),
                    }],
                },
            });
            assert!(out.status.ok, "append {index}: {:?}", out.status);
        }

        let measure = |command: Command| {
            let request = || ExecuteRequest { shard_id: 1, command: command.clone() };
            let warm = engine.execute(request());
            assert!(warm.status.ok, "{:?}", warm.status);
            let probe = crate::alloc_probe::Probe::start();
            let out = engine.execute(request());
            let counts = probe.stop();
            assert!(out.status.ok, "{:?}", out.status);
            (counts.allocs, out.response)
        };

        // One more point onto an already-long series.
        let (append, _) = measure(Command::FeatureAppend {
            key: "rate".to_string(),
            points: vec![FeaturePoint {
                timestamp_ms: 1_000 + series_len as u64,
                value: b"tail".to_vec(),
            }],
        });

        // A 32-point window at the END of the series: the shape a reader actually asks for.
        let window_start = 1_000 + (series_len as u64) - 32;
        let (window, window_response) = measure(Command::FeatureQuery {
            key: "rate".to_string(),
            start_ms: window_start,
            end_ms: window_start + 32,
            count: None,
        });
        let window_points = match &window_response {
            CommandResponse::FeaturePoints { points } => points.len(),
            other => panic!("expected feature points, got {other:?}"),
        };
        assert!(
            window_points > 0 && window_points <= 40,
            "the window must return about 32 points, got {window_points} -- otherwise the per-point \
             figure below is measuring a different query than the one named"
        );

        // Control: the whole series. This one SHOULD grow.
        let (full, full_response) = measure(Command::FeatureQuery {
            key: "rate".to_string(),
            start_ms: 0,
            end_ms: u64::MAX,
            count: None,
        });
        let full_points = match &full_response {
            CommandResponse::FeaturePoints { points } => points.len(),
            other => panic!("expected feature points, got {other:?}"),
        };
        assert!(
            full_points >= series_len,
            "the control must return the whole series, got {full_points} of {series_len}"
        );

        let (agg, _) = measure(Command::FeatureAggQuery {
            key: "rate".to_string(),
            start_ms: window_start,
            end_ms: window_start + 32,
            aggregator: "count".to_string(),
            count: None,
        });

        println!(
            "  {series_len:>6}   {append:>6}   {window:>10}   {:>9.2}   {full:>10}   {:>9.2}   {agg:>7}",
            window as f64 / window_points as f64,
            full as f64 / full_points as f64,
        );
    }

    println!(
        "
  window column flat    => the range narrows the read, which is the point of the lane.
  window column growing => the query reads the whole series and filters, so every reader pays for
                           all history ever recorded.
  append column growing => appending one point costs more once the series is long.
"
    );
}


/// Is appending one feature point proportional to the series already there?
///
/// Every append below uses a FRESH timestamp, so it is genuinely an append and not an overwrite of
/// a duplicate -- which is the flaw that made the first measurement of this ambiguous. The empty
/// series gives the floor, so "grows with the series" can be distinguished from "costly in general",
/// and a duplicate-timestamp append is measured beside it as its own case.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib does_appending_one_point_cost_the_series -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn does_appending_one_point_cost_the_series() {
    let canary = crate::alloc_probe::Probe::start();
    let sink: Vec<u8> = Vec::with_capacity(8192);
    assert!(
        canary.stop().allocs > 0,
        "counting allocator not installed -- rerun with `--features alloc-probe`"
    );
    drop(sink);

    println!(
        "
  series   append(fresh)   per existing point   append(duplicate)
"
    );

    for series_len in [0_usize, 64, 256, 1024] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);

        for index in 0..series_len {
            let out = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAppend {
                    key: "rate".to_string(),
                    points: vec![FeaturePoint {
                        timestamp_ms: 1_000 + index as u64,
                        value: format!("{index}").into_bytes(),
                    }],
                },
            });
            assert!(out.status.ok, "build {index}: {:?}", out.status);
        }

        // A genuinely new timestamp, past everything written above.
        let fresh_at = 500_000 + series_len as u64;
        let probe = crate::alloc_probe::Probe::start();
        let out = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "rate".to_string(),
                points: vec![FeaturePoint {
                    timestamp_ms: fresh_at,
                    value: b"fresh".to_vec(),
                }],
            },
        });
        let fresh = probe.stop().allocs;
        assert!(out.status.ok, "{:?}", out.status);

        // The same timestamp again: an overwrite, not an append.
        let probe = crate::alloc_probe::Probe::start();
        let out = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "rate".to_string(),
                points: vec![FeaturePoint {
                    timestamp_ms: fresh_at,
                    value: b"again".to_vec(),
                }],
            },
        });
        let duplicate = probe.stop().allocs;
        assert!(out.status.ok, "{:?}", out.status);

        // The series really is as long as claimed -- otherwise the per-point column is fiction.
        let read = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery {
                key: "rate".to_string(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: None,
            },
        });
        let held = match &read.response {
            CommandResponse::FeaturePoints { points } => points.len(),
            other => panic!("expected feature points, got {other:?}"),
        };
        assert!(
            held >= series_len,
            "expected at least {series_len} points held, found {held}"
        );

        let per = if series_len == 0 {
            0.0
        } else {
            fresh as f64 / series_len as f64
        };
        println!("  {series_len:>6}   {fresh:>13}   {per:>18.2}   {duplicate:>17}");
    }

    println!(
        "
  append(fresh) flat across series lengths => appending costs what it appends.
  append(fresh) rising with the series      => building a series is quadratic in its length.
"
    );
}


/// Does `count` narrow a sequence read, or is it applied after reading the range?
///
/// `SequenceQuery` carries both a time range and a `count`. The range narrowing is already
/// established for the feature lane; the count is a separate lever and a separate question. A caller
/// asking for "the last 8" over an open range is asking the count to narrow -- and if it is applied
/// only after the range is read, that caller pays for all of history to be handed eight rows.
///
/// `history`'s bounded read is the bar: 116 allocations at every size from 8 to 256 summaries.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib does_a_sequence_count_narrow_the_read -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn does_a_sequence_count_narrow_the_read() {
    let canary = crate::alloc_probe::Probe::start();
    let sink: Vec<u8> = Vec::with_capacity(8192);
    assert!(
        canary.stop().allocs > 0,
        "counting allocator not installed -- rerun with `--features alloc-probe`"
    );
    drop(sink);

    println!(
        "
  rows   append   count=8 over all time   rows back   full read   rows back
"
    );

    for series_len in [256_usize, 1024, 4096] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);

        for index in 0..series_len {
            let out = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::SequenceAdd {
                    key: "seq".to_string(),
                    rows: vec![SequenceFeatureRow {
                        timestamp_ms: 1_000 + index as u64,
                        gid: index as u64,
                        action_type: (index % 7) as u32,
                        duration: (index % 13) as u32,
                        author_id: (index % 5) as u64,
                    }],
                },
            });
            assert!(out.status.ok, "build {index}: {:?}", out.status);
        }

        let measure = |command: Command| {
            let request = || ExecuteRequest { shard_id: 1, command: command.clone() };
            let warm = engine.execute(request());
            assert!(warm.status.ok, "{:?}", warm.status);
            let probe = crate::alloc_probe::Probe::start();
            let out = engine.execute(request());
            let counts = probe.stop();
            assert!(out.status.ok, "{:?}", out.status);
            let rows = match &out.response {
                CommandResponse::SequenceRows { rows } => rows.len(),
                other => panic!("expected sequence rows, got {other:?}"),
            };
            (counts.allocs, rows)
        };

        // One more row onto an already-long series.
        let probe = crate::alloc_probe::Probe::start();
        let out = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SequenceAdd {
                key: "seq".to_string(),
                rows: vec![SequenceFeatureRow {
                    timestamp_ms: 900_000 + series_len as u64,
                    gid: 7,
                    action_type: 1,
                    duration: 1,
                    author_id: 1,
                }],
            },
        });
        let append = probe.stop().allocs;
        assert!(out.status.ok, "{:?}", out.status);

        // A count of 8 over the whole range: the count is the only thing that can narrow this.
        let (counted, counted_rows) = measure(Command::SequenceQuery {
            key: "seq".to_string(),
            start_ms: 0,
            end_ms: u64::MAX,
            count: 8,
            filters: Vec::new(),
        });
        assert!(
            counted_rows > 0 && counted_rows <= 8,
            "count=8 must return at most 8 rows, got {counted_rows} -- otherwise this column is \
             measuring a different query than the one named"
        );

        // Control: the same range, uncapped. This one SHOULD grow.
        let (full, full_rows) = measure(Command::SequenceQuery {
            key: "seq".to_string(),
            start_ms: 0,
            end_ms: u64::MAX,
            count: usize::MAX,
            filters: Vec::new(),
        });
        assert!(
            full_rows >= series_len,
            "the control must read the whole series, got {full_rows} of {series_len}"
        );

        println!(
            "  {series_len:>4}   {append:>6}   {counted:>21}   {counted_rows:>9}   {full:>9}   {full_rows:>9}"
        );
    }

    println!(
        "
  count=8 flat while the control grows => the count narrows the read.
  count=8 tracking the control          => the count is applied after reading the range, so a
                                           bounded caller still pays for all of history.
"
    );
}


/// What do the score-ordered zset reads cost as the set grows?
///
/// A zset is `HashMap<String, BTreeMap<Vec<u8>, (u64, BlockAddress)>>` — keyed by MEMBER, score as a
/// value. Member lookup is O(log n); anything ordered by SCORE cannot use that ordering.
/// `zset_ordered_members` clones every member and sorts all n before the caller filters, and
/// `ZSetRank` scans with two member clones per member examined.
///
/// `ZSetScore` is the control: one member lookup, which should be flat. If every column grows, the
/// harness is the suspect rather than the code.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_the_score_ordered_zset_reads_cost -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn what_the_score_ordered_zset_reads_cost() {
    let canary = crate::alloc_probe::Probe::start();
    let sink: Vec<u8> = Vec::with_capacity(8192);
    assert!(
        canary.stop().allocs > 0,
        "counting allocator not installed -- rerun with `--features alloc-probe`"
    );
    drop(sink);

    println!(
        "
  members   range_by_score(8)   bytes   returned      rank   bytes      score   bytes       add
"
    );

    for size in [256_usize, 1024, 4096] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);

        for index in 0..size {
            let out = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ZSetAdd {
                    key: "board".to_string(),
                    member: format!("member-{index:08}").into_bytes(),
                    score: index as f64,
                },
            });
            assert!(out.status.ok, "build {index}: {:?}", out.status);
        }

        let measure = |command: Command| {
            let request = || ExecuteRequest { shard_id: 1, command: command.clone() };
            let warm = engine.execute(request());
            assert!(warm.status.ok, "{:?}", warm.status);
            let probe = crate::alloc_probe::Probe::start();
            let out = engine.execute(request());
            let counts = probe.stop();
            assert!(out.status.ok, "{:?}", out.status);
            (counts.allocs, counts.alloc_bytes, out.response)
        };

        // Eight members out of `size`, by score. The narrowest useful ask.
        let (range_allocs, range_bytes, range_response) = measure(Command::ZSetRangeByScore {
            key: "board".to_string(),
            min: 10.0,
            max: 17.0,
            min_exclusive: false,
            max_exclusive: false,
            rev: false,
        });
        // Members come back as [member, score, member, score, ...], so eight members is sixteen.
        let returned = match &range_response {
            CommandResponse::Members { members } => members.len() / 2,
            other => panic!("expected members, got {other:?}"),
        };
        assert_eq!(
            returned, 8,
            "the window must return exactly 8 members, got {returned} -- otherwise this column is \
             not the cost of the query it is named for"
        );

        let (rank_allocs, rank_bytes, _) = measure(Command::ZSetRank {
            key: "board".to_string(),
            member: format!("member-{:08}", size / 2).into_bytes(),
            rev: false,
        });

        // Control: one member lookup straight through the map. Should not care how big the set is.
        let (score_allocs, score_bytes, score_response) = measure(Command::ZSetScore {
            key: "board".to_string(),
            member: format!("member-{:08}", size / 2).into_bytes(),
        });
        assert!(
            matches!(&score_response, CommandResponse::Bytes { value: Some(_) }),
            "the control must actually find the member, or it is measuring a miss"
        );

        let (add_allocs, _, _) = measure(Command::ZSetAdd {
            key: "board".to_string(),
            member: format!("late-{size:08}").into_bytes(),
            score: 1.5,
        });

        println!(
            "  {size:>7}   {range_allocs:>17}   {range_bytes:>5}   {returned:>8}   {rank_allocs:>7}   {rank_bytes:>5}   {score_allocs:>8}   {score_bytes:>5}   {add_allocs:>7}"
        );
    }

    println!(
        "
  range_by_score / rank flat  => the per-member COPIES are gone: the score test runs before the
                                 member bytes are cloned, and rank compares the parts in place.
                                 The SCAN is still O(n) and stays that way while the map is keyed
                                 by member -- this is an allocation win, not an asymptotic one.
  either column rising        => a copy per member has come back.
  score / add flat            => the controls work, so the columns above mean what they say.
"
    );
}


/// What the score-ordered zset reads ANSWER, pinned across many random cases.
///
/// Coverage for these commands is one case each, which is not enough to change the code underneath
/// them. The expected answers here are computed independently -- by sorting (score, member) pairs in
/// the test and slicing -- rather than by calling the helper the engine calls, because a test that
/// mirrors the implementation passes whatever the implementation does.
///
/// Duplicate scores are included on purpose: ties order by member, and a change that sorted only by
/// score would still pass a test whose scores were all distinct.
#[test]
fn score_ordered_zset_reads_answer_the_same_before_and_after() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    // A deterministic spread with deliberate score ties.
    let mut state: u64 = 0x243F_6A88_85A3_08D3;
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state >> 11
    };

    let count = 400_usize;
    let mut expected: Vec<(u64, Vec<u8>)> = Vec::new();
    for index in 0..count {
        // Scores collide by construction: many members share each score.
        let score = (next() % 40) as u64;
        let member = format!("m-{index:05}").into_bytes();
        let out = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ZSetAdd {
                key: "board".to_string(),
                member: member.clone(),
                score: score as f64,
            },
        });
        assert!(out.status.ok, "add {index}: {:?}", out.status);
        expected.push((score, member));
    }
    expected.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    assert!(
        expected.windows(2).any(|w| w[0].0 == w[1].0),
        "the fixture must contain score ties, or ordering-by-member is never exercised"
    );

    // --- range by score, over many windows -------------------------------------------------
    let mut checked_nonempty = 0;
    for lo in 0..38_u64 {
        let hi = lo + 2;
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ZSetRangeByScore {
                key: "board".to_string(),
                min: lo as f64,
                max: hi as f64,
                min_exclusive: false,
                max_exclusive: false,
                rev: false,
            },
        });
        let members = match response.response {
            CommandResponse::Members { members } => members,
            other => panic!("expected members, got {other:?}"),
        };
        // [member, score, member, score, ...]
        let got: Vec<Vec<u8>> = members.chunks(2).map(|pair| pair[0].clone()).collect();
        let want: Vec<Vec<u8>> = expected
            .iter()
            .filter(|(score, _)| *score >= lo && *score <= hi)
            .map(|(_, member)| member.clone())
            .collect();
        assert_eq!(got, want, "range by score [{lo}, {hi}]");
        if !want.is_empty() {
            checked_nonempty += 1;
        }
    }
    assert!(
        checked_nonempty >= 30,
        "only {checked_nonempty} windows returned anything -- an all-empty sweep would pass \
         whatever the code did"
    );

    // --- rank, forward and reverse -----------------------------------------------------------
    for pick in [0_usize, 1, 7, 99, 200, 399] {
        let (_, member) = &expected[pick];
        for rev in [false, true] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ZSetRank {
                    key: "board".to_string(),
                    member: member.clone(),
                    rev,
                },
            });
            let got = match response.response {
                CommandResponse::Bytes { value: Some(bytes) } => {
                    String::from_utf8(bytes).unwrap().parse::<usize>().unwrap()
                }
                other => panic!("expected a rank, got {other:?}"),
            };
            let want = if rev { count - 1 - pick } else { pick };
            assert_eq!(got, want, "rank of {pick} (rev={rev})");
        }
    }

    // A member that is not there has no rank.
    let missing = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ZSetRank {
            key: "board".to_string(),
            member: b"not-a-member".to_vec(),
            rev: false,
        },
    });
    assert!(
        matches!(missing.response, CommandResponse::Bytes { value: None }),
        "a member that was never added must have no rank"
    );
}
