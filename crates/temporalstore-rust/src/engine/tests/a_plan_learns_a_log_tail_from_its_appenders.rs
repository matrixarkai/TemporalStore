// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A MAINTENANCE PLAN LEARNS EACH LOG'S LAST SEQUENCE FROM THE SEQUENCE ITS APPENDERS KEEP.
//!
//! `stats` answers `last_sequence` by opening the piece being written and decoding the head of
//! every record in it. The reclaim plan, the lifecycle plan, the index-log gate and the pressure
//! report each asked, so one periodic round read the active piece of both logs back about ten
//! times to learn two numbers that every append had already written down. Measured on one quiet
//! round of a drained 100-key shard, `LocalIndexLogStore::stats` was 14.1% of the round's
//! instructions, nearly all of it `decode_index_payload`.
//!
//! TWO GUARDS, BECAUSE THEY FAIL ON DISJOINT MUTATIONS.
//!   * `the_cached_tail_equals_the_scanned_tail_through_a_history` holds the cached answer to the
//!     scanned one after every kind of step that moves a log: writes, overwrites, deletes, a dump,
//!     a WAL reclaim, an index-log collection and a cold reload. A cache that went stale passes
//!     the other guard and fails this one.
//!   * `a_maintenance_plan_reads_neither_log_tail_back` is the speed-up: the plans and the pressure
//!     report scan nothing. A plan that went back to `stats` passes the first guard and fails this
//!     one. Its control reads `stats` directly and must be counted, or the counters are blind.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

fn engine_on(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ));
    engine.load_shard(1);
    engine
}

fn set(engine: &TemporalEngine, key: &str, value: &[u8]) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: key.to_string(),
            value: value.to_vec(),
        },
    });
    assert!(response.status.ok, "{:?}", response.status);
}

fn dump(engine: &TemporalEngine, which: &str) {
    let manifest = engine.create_bucket_dump_manifest(1, Vec::new());
    assert!(manifest.is_ok(), "{which}: {:?}", manifest.err());
}

fn delete(engine: &TemporalEngine, key: &str) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringDelete {
            key: key.to_string(),
        },
    });
    assert!(response.status.ok, "{:?}", response.status);
}

/// (wal, index log) last sequences as the cached read and the scanning read give them.
fn tails(engine: &TemporalEngine) -> ((u64, u64), (u64, u64)) {
    (
        (
            engine.wal_store.stats_without_rescan(1).last_sequence,
            engine.wal_store.stats(1).last_sequence,
        ),
        (
            engine.index_log_store.stats_without_rescan(1).last_sequence,
            engine.index_log_store.stats(1).last_sequence,
        ),
    )
}

#[test]
fn the_cached_tail_equals_the_scanned_tail_through_a_history() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    let mut seen = Vec::new();
    let mut check = |engine: &TemporalEngine, step: &str| {
        let (wal, index) = tails(engine);
        assert_eq!(wal.0, wal.1, "{step}: the WAL's cached tail {} != its scanned tail {}", wal.0, wal.1);
        assert_eq!(
            index.0, index.1,
            "{step}: the index log's cached tail {} != its scanned tail {}",
            index.0, index.1
        );
        seen.push((step.to_string(), wal.1, index.1));
    };

    check(&engine, "empty shard");
    for index in 0..40 {
        set(&engine, &format!("t-{index:03}"), &[b'a'; 48]);
    }
    check(&engine, "after writes");
    dump(&engine, "dump");
    check(&engine, "after a dump");
    for index in 0..20 {
        set(&engine, &format!("t-{index:03}"), &[b'b'; 64]);
    }
    for index in 20..30 {
        delete(&engine, &format!("t-{index:03}"));
    }
    check(&engine, "after overwrites and deletes");
    dump(&engine, "second dump");
    let plan = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());
    let _ = engine.apply_storage_wal_reclaim(plan);
    check(&engine, "after a WAL reclaim");
    let _ = engine.apply_periodic_index_gc(
        crate::engine::reports::StorageLifecycleRequest {
            shard_id: 1,
            prune_bucket_dump_manifests: true,
            roll_forward_bucket_dump_installs: true,
            ..crate::engine::reports::StorageLifecycleRequest::default()
        },
        None,
        crate::engine::reports::DEFAULT_INDEX_GC_MAX_ENTRIES_PER_ROUND,
    );
    check(&engine, "after an index-log collection");

    // A COLD RELOAD: a new engine over the same directories has an empty cache, so the first
    // cached read must scan and find what the log holds.
    drop(engine);
    let reloaded = engine_on(dir.path());
    check(&reloaded, "after a cold reload");
    set(&reloaded, "t-after-reload", &[b'c'; 48]);
    check(&reloaded, "after a write following the reload");

    // NON-VACUITY. A history that never moved either log would let a cache that always says 0
    // agree with a scan that always says 0.
    let wal_values = seen.iter().map(|(_, wal, _)| *wal).collect::<std::collections::BTreeSet<_>>();
    let index_values = seen.iter().map(|(_, _, index)| *index).collect::<std::collections::BTreeSet<_>>();
    assert!(seen.iter().any(|(_, wal, index)| *wal > 0 && *index > 0), "no step left a log with a record in it");
    assert!(
        wal_values.len() >= 4 && index_values.len() >= 4,
        "the history moved the WAL through {} distinct tails and the index log through {}",
        wal_values.len(),
        index_values.len()
    );
}

#[test]
fn a_maintenance_plan_reads_neither_log_tail_back() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    for index in 0..200 {
        set(&engine, &format!("p-{index:03}"), &[b'v'; 64]);
    }
    dump(&engine, "dump");

    let wal_scans = |engine: &TemporalEngine| engine.wal_store.raw_stats(1).stats_full_scans;
    let index_scans = crate::index_log::index_log_tail_scans;

    let (wal_before, index_before) = (wal_scans(&engine), index_scans());
    let reclaim = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());
    let lifecycle = engine.storage_lifecycle_plan(crate::engine::reports::StorageLifecycleRequest {
        shard_id: 1,
        ..crate::engine::reports::StorageLifecycleRequest::default()
    });
    let pressure = engine.storage_log_pressure_report(1);
    let _ = engine.apply_periodic_index_gc(
        crate::engine::reports::StorageLifecycleRequest {
            shard_id: 1,
            ..crate::engine::reports::StorageLifecycleRequest::default()
        },
        None,
        crate::engine::reports::DEFAULT_INDEX_GC_MAX_ENTRIES_PER_ROUND,
    );
    let (wal_after, index_after) = (wal_scans(&engine), index_scans());
    assert_eq!(wal_after, wal_before, "the plans scanned the WAL tail {} time(s)", wal_after - wal_before);
    assert_eq!(index_after, index_before, "the plans scanned the index-log tail {} time(s)", index_after - index_before);

    // THE ANSWERS ARE THE SCANNED ONES.
    let scanned_wal = engine.wal_store.stats(1).last_sequence;
    let scanned_index = engine.index_log_store.stats(1).last_sequence;
    assert!(scanned_wal > 0 && scanned_index > 0, "the fixture wrote neither log");
    assert_eq!(reclaim.current_wal_sequence, scanned_wal);
    assert_eq!(reclaim.current_index_log_sequence, scanned_index);
    assert_eq!(lifecycle.undumped_wal_records, scanned_wal.saturating_sub(
        engine.list_bucket_dump_manifests(1).last().map(|m| m.wal_sequence).unwrap_or(0)
    ));
    assert_eq!(pressure.wal_last_sequence, scanned_wal);
    assert_eq!(pressure.index_log_last_sequence, scanned_index);

    // THE CONTROL. The two reads this guard replaces, taken directly, must be counted -- or the
    // zero above says nothing about the plans.
    assert_eq!(index_scans(), index_after + 1, "a direct index-log `stats` was not counted");
    assert_eq!(wal_scans(&engine), wal_after + 1, "a direct WAL `stats` was not counted");
}
