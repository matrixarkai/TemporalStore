// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Test part 3, split from tests.rs.
#![allow(clippy::all)]
use super::*;
use super::helpers::*;

/// Every maintenance phase is on under the options a server actually starts with.
///
/// The server passes `StorageManagerOptions::default()` and prints a banner naming seven phases.
/// The banner is a string; this checks the report.
///
/// Asserted on the SUFFIX rather than against a list of names, because the failure this guards
/// against is a default going quiet, and the worst version of that is a phase added later whose
/// default is off -- which a fixed list would not mention. Any stage the runtime records as
/// `<name>_disabled` fails this, including one that does not exist yet.
///
/// The second half is the other way to be wrong: a phase that stops reporting at all. A stage
/// missing from both lists is not "off", it is absent, and the first assertion cannot see it.
#[test]
fn every_maintenance_phase_is_enabled_under_the_shipped_default() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    // Real content, so a phase that declines for want of work is distinguishable from one that
    // declines because it is switched off.
    for index in 0..64 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("phase-{index:03}"),
                value: vec![b'v'; 64],
            },
        });
    }
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );

    let report = runtime.run_storage_manager_once(1, StorageManagerOptions::default());

    let disabled = report
        .skipped_stages
        .iter()
        .filter(|stage| stage.ends_with("_disabled"))
        .cloned()
        .collect::<Vec<_>>();
    // `evict` is the one stage that ships OFF, deliberately: wiring it made eviction reachable
    // from this loop at all, and turning it on by default is a production eviction-policy change
    // (`eviction_delete_drop` can discard unflushed state). Pinning the exact list rather than
    // dropping the assertion keeps both teeth: another stage going off still fails here, and so
    // does `evict` being quietly switched on.
    assert_eq!(
        disabled,
        vec!["evict_disabled".to_string()],
        "phases switched off under the options the server ships with: {disabled:?} \
         (executed: {:?}, skipped: {:?})",
        report.executed_stages,
        report.skipped_stages
    );

    // Every phase must have reported SOMETHING -- ran, or declined for a reason that is not
    // "disabled". A phase in neither list has gone missing from the cycle.
    for phase in [
        "prepare",
        "reclaim_wal",
        "reclaim_memory",
        "expire",
        "reclaim_page",
        "compact_pages",
        "reclaim_index",
        "reap_metrics",
        "evict",
    ] {
        let ran = report.executed_stages.iter().any(|stage| stage == phase);
        let declined = report
            .skipped_stages
            .iter()
            .any(|stage| stage.starts_with(phase));
        assert!(
            ran || declined,
            "{phase} appears in neither list, so the cycle no longer reports it \
             (executed: {:?}, skipped: {:?})",
            report.executed_stages,
            report.skipped_stages
        );
    }
}

/// A dump actually happens under the shipped default, once enough has accumulated to be worth
/// one.
///
/// Being ENABLED is not being REACHED. The default holds a dump back until
/// `min_undumped_wal_records` = 1000 records are undumped, so a handful of writes produces no
/// dump at all -- correctly, because the delay exists to let repeated writes to the same bucket
/// coalesce into one dumped generation. A guard that only checked the phase flags would pass on
/// a store that never dumps, which is the state this whole line of work started from.
///
/// So: write past the threshold, run one round with the options a server ships with, and require
/// that buckets were actually selected and a manifest written.
#[test]
fn a_dump_fires_under_the_shipped_default_once_the_threshold_is_crossed() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    // Every default except the dump cap, which this fixture pins on purpose.
    //
    // A data-node round calls `apply_storage_lifecycle` more than once, and only the first one
    // that finds undumped records dumps -- the rest correctly see a threshold the dump reset.
    // So WHICH apply carries `dump_manifest` depends on how many buckets the first one covered,
    // and with the cap off the first one covers them all and the reported apply has nothing
    // left to do. Pinning the cap keeps this a test of the RECORD THRESHOLD, which is what its
    // name is about; `the_shipped_dump_cap_still_lets_the_log_be_reclaimed` is where the cap's
    // own behaviour is pinned.
    let options = StorageManagerOptions {
        max_dump_buckets_per_round: 64,
        ..StorageManagerOptions::default()
    };
    // Past the coalescing delay, and not by one: the threshold counts UNDUMPED records, so a
    // round that dumps resets it, and a fixture sitting exactly on the line would be deciding
    // the test on an off-by-one in the counter rather than on whether a dump happens.
    let writes = options.min_undumped_wal_records as usize + 256;
    for index in 0..writes {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("dumped-{index:05}"),
                value: vec![b'v'; 32],
            },
        });
    }

    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    let report = runtime.run_storage_manager_once(1, options);

    assert!(
        !report.lifecycle_plan.dump_delayed,
        "past {writes} writes the dump is still being delayed: {:?}",
        report.lifecycle_plan.undumped_wal_records
    );
    assert!(
        !report.lifecycle_plan.selected_dump_buckets.is_empty(),
        "the round selected no buckets to dump, so nothing was going to be written"
    );
    let lifecycle = report
        .lifecycle_report
        .as_ref()
        .expect("a round that selected buckets must report what it did with them");
    let manifest = lifecycle
        .dump_manifest
        .as_ref()
        .expect("selected buckets must produce a dump manifest");
    assert!(
        !manifest.bucket_ids.is_empty(),
        "the manifest names no buckets, so the dump captured nothing"
    );
}

/// A big log dumps on its SIZE, while the record count is still saying wait.
///
/// The record threshold cannot bound a log. A thousand hundred-byte records is a hundred
/// kilobytes and a thousand megabyte records is a gigabyte, and neither reaches 1000 records any
/// sooner than the other -- so a workload with large values holds the dump off across an
/// arbitrarily large log, and reclaim only follows a dump.
///
/// The fixture is the case the record count is blind to: FAR fewer records than the record
/// threshold, but past the byte threshold. If the byte threshold did nothing, the record count
/// alone would still be delaying, which is what the first assertion states.
#[test]
fn a_large_log_dumps_on_bytes_while_the_record_count_still_says_wait() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    // A byte threshold small enough to reach in a test, and a record threshold far out of reach,
    // so only the byte one can release the dump.
    //
    // 2 KiB, not the 64 KiB this asked for originally. The threshold now counts the bytes the
    // log has TAKEN since its last dump; it used to read `persistent_bytes`, which on a
    // preallocated segment is 262,144 from the very first record and never moves. So the old
    // 64 KiB was cleared by the preallocation, not by anything written -- the fixture below
    // holds about 3.7 KiB of records, and would have passed with no writes at all.
    let options = StorageManagerOptions {
        min_undumped_wal_records: 1_000_000,
        min_undumped_wal_bytes: 2 * 1024,
        ..StorageManagerOptions::default()
    };

    let records = 48;
    for index in 0..records {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("fat-{index:03}"),
                value: vec![b'v'; 4 * 1024],
            },
        });
    }
    assert!(
        (records as u64) < options.min_undumped_wal_records,
        "the fixture must stay far below the record threshold, or it proves nothing"
    );

    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    let report = runtime.run_storage_manager_once(1, options.clone());

    assert!(
        !report.lifecycle_plan.dump_delayed,
        "the log is past {} bytes and still delayed, so the byte threshold did nothing",
        options.min_undumped_wal_bytes
    );
    assert!(
        !report.lifecycle_plan.selected_dump_buckets.is_empty(),
        "released, but no bucket was selected, so nothing would be written"
    );

    // The control: the same store, the same records, with only the byte threshold taken away.
    // Now nothing can release the dump and it must be delayed -- which is what proves the first
    // assertion was the BYTE threshold's doing and not the record count quietly being satisfied.
    let without_bytes = StorageManagerOptions {
        min_undumped_wal_bytes: 0,
        ..options
    };
    let control = runtime.run_storage_manager_once(1, without_bytes);
    assert!(
        control.lifecycle_plan.dump_delayed,
        "with no byte threshold the record count alone must still be delaying this dump"
    );

    // And the half that was missing: the threshold must track HOW MUCH, not merely that a log
    // exists. Set it above what this fixture wrote and the dump has to go back to being delayed.
    // Without this the test passes on any reading that is large for an unrelated reason -- which
    // is exactly how the preallocated segment size passed it before.
    let above_what_was_written = StorageManagerOptions {
        min_undumped_wal_bytes: 64 * 1024,
        ..options
    };
    let still_delayed = runtime.run_storage_manager_once(1, above_what_was_written);
    assert!(
        still_delayed.lifecycle_plan.dump_delayed,
        "a threshold above what the log has taken must delay the dump; releasing here means the \
         byte reading is not measuring this shard's log growth"
    );
}

/// The log's size threshold is set in the options a server actually starts with.
///
/// The test that proves the byte threshold WORKS passes its own options, so it cannot see the
/// shipped default at all -- zeroing that default left it green. This is the half that watches
/// production: a threshold nothing sets is a threshold that does not exist, which is the exact
/// shape of the seven round bounds that were implemented, enforced, tested and left at zero.
#[test]
fn the_shipped_default_bounds_the_log_by_size() {
    let options = StorageManagerOptions::default();
    assert_eq!(options.min_undumped_wal_bytes, 96 * 1024 * 1024);
    assert!(
        options.min_undumped_wal_bytes > 0,
        "with no byte threshold the record count alone decides, and a record count does not \
         bound a file"
    );
    // Both thresholds are set, because each bounds something the other cannot: records bound how
    // much replay a restart faces, bytes bound the file.
    assert!(options.min_undumped_wal_records > 0);
}

/// Switching WAL reclaim off stops the reclaim, however large the log is.
///
/// The two thresholds are alternatives for WHEN to dump, not for WHETHER to reclaim. With the
/// byte threshold crossed and reclaim disabled, no WAL reclaim may run.
///
/// Each arm gets its OWN store, because a round changes what the next round sees. Sharing one
/// failed twice, in opposite directions: with the disabled arm second it inherited an
/// already-reclaimed log and declined for want of work, so deleting the flag check changed
/// nothing; with it first, its own prepare consumed the dump pressure -- the dump is not gated
/// by this flag on this path -- and the control then had nothing left to reclaim. Two
/// experiments, two fixtures.
#[test]
fn switching_reclaim_off_stops_the_reclaim_however_large_the_log() {
    fn store_past_the_byte_threshold(
        dir: &tempfile::TempDir,
    ) -> DataNodeRuntime {
        let engine = TemporalEngine::with_local_dirs(
            1 << 20,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..48 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("suppressed-{index:03}"),
                    value: vec![b'v'; 4 * 1024],
                },
            });
        }
        DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        )
    }

    // Past the byte threshold and far short of the record one, so only the byte threshold can
    // release the dump that reclaim follows.
    // 2 KiB for the same reason as the test above: the byte threshold counts what the log has
    // taken since its last dump, and this fixture writes about 3.7 KiB of records. The 64 KiB
    // this asked for originally was cleared by the segment's preallocated size, not by writes.
    let reclaiming = StorageManagerOptions {
        min_undumped_wal_records: 1_000_000,
        min_undumped_wal_bytes: 2 * 1024,
        ..StorageManagerOptions::default()
    };
    let not_reclaiming = StorageManagerOptions {
        enable_wal_reclaim: false,
        ..reclaiming.clone()
    };

    let control_dir = tempfile::tempdir().unwrap();
    let control = store_past_the_byte_threshold(&control_dir)
        .run_storage_manager_once(1, reclaiming);
    assert!(
        !control.lifecycle_plan.dump_delayed,
        "the byte threshold must release this dump, or nothing below is demonstrated"
    );
    assert!(
        control.executed_stages.iter().any(|stage| stage == "reclaim_wal"),
        "the control must actually reclaim: {:?}",
        control.executed_stages
    );

    let suppressed_dir = tempfile::tempdir().unwrap();
    let suppressed = store_past_the_byte_threshold(&suppressed_dir)
        .run_storage_manager_once(1, not_reclaiming);
    assert!(
        !suppressed.executed_stages.iter().any(|stage| stage == "reclaim_wal"),
        "reclaim is off, so no WAL reclaim may run however large the log: {:?}",
        suppressed.executed_stages
    );
    assert!(
        suppressed
            .skipped_stages
            .iter()
            .any(|stage| stage == "reclaim_wal_disabled"),
        "and it must say it was disabled rather than silently doing nothing: {:?}",
        suppressed.skipped_stages
    );
}

/// The RUNNING scheduler reaches every maintenance phase, not just one round of it.
///
/// The guard beside this one proves a single round enables every phase. That is not the same
/// claim: a loop that ran once, or only ever visited its first shard, or died on the first error,
/// would satisfy it. This starts the scheduler the server starts, lets it tick, and requires that
/// every per-phase counter moved.
///
/// Asserted as "at least once each" rather than "once per loop". Measured over four loops:
/// prepare 4, reclaim_wal 1, reclaim_memory 3, expire 4, reclaim_page 4, compact 4, index_gc 4 --
/// reclaim_wal ran once because after it reclaimed there was nothing left, and reclaim_memory is
/// pressure-dependent. Pinning either to the loop count would pin this fixture's luck.
#[test]
fn the_running_scheduler_reaches_every_phase() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        256 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let options = StorageManagerOptions::default();
    // Pressure of several kinds, so a phase that declines for want of work is distinguishable
    // from one the loop never reaches: volume for the dump threshold, overwrites for stale
    // pages, deletes for tombstones, and a small cache so memory pressure is reachable.
    let writes = options.min_undumped_wal_records as usize + 256;
    for index in 0..writes {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("periodic-{index:05}"),
                value: vec![b'v'; 64],
            },
        });
    }
    for index in 0..(writes / 2) {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("periodic-{index:05}"),
                value: vec![b'w'; 96],
            },
        });
    }
    for index in 0..(writes / 4) {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonDelete { key: format!("periodic-{index:05}") },
        });
    }

    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    let scheduler = runtime.start_storage_manager_scheduler_for_all_shards(
        std::time::Duration::from_millis(5),
        options,
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while std::time::Instant::now() < deadline {
        if runtime.stats().storage_manager_loops >= 4 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let stats = runtime.stats();
    drop(scheduler);

    assert!(
        stats.storage_manager_loops >= 4,
        "the scheduler only completed {} rounds, so nothing below is about periodic behaviour",
        stats.storage_manager_loops
    );
    for (phase, runs) in [
        ("prepare", stats.storage_manager_prepare_runs),
        ("reclaim_wal", stats.storage_manager_reclaim_wal_runs),
        ("reclaim_memory", stats.storage_manager_reclaim_memory_runs),
        ("expire", stats.storage_manager_expire_runs),
        ("reclaim_page", stats.storage_manager_reclaim_page_runs),
        ("compact", stats.storage_manager_compact_runs),
        ("index_gc", stats.storage_manager_index_gc_runs),
    ] {
        assert!(
            runs > 0,
            "{phase} never ran across {} scheduler rounds",
            stats.storage_manager_loops
        );
    }
}

#[test]
fn runtime_enforces_authorized_lifecycle_token_when_installed() {
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        TemporalEngine::default(),
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    runtime.require_lifecycle_token(SchedulerLifecycleToken {
        task_id: 12,
        shard_id: 7,
        operation: "load".to_string(),
        load_version: 43,
        generation: 900,
    });

    let stale = runtime.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 7,
        table_name: "tbl".to_string(),
        shard_uri: "local://tbl/stale".to_string(),
        start_routing_bucket: 10,
        end_routing_bucket: 19,
        readonly: false,
        load_version: 42,
        local_node_id: Some(3),
    });
    assert_eq!(stale.status.code, "lifecycle_token_mismatch");
    let failed = runtime.lifecycle_report();
    assert_eq!(failed.failed_count, 1);
    assert_eq!(failed.transitions[0].scheduler_task_id, Some(12));
    assert_eq!(failed.transitions[0].scheduler_generation, Some(900));

    let load = runtime.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 7,
        table_name: "tbl".to_string(),
        shard_uri: "local://tbl/7".to_string(),
        start_routing_bucket: 10,
        end_routing_bucket: 19,
        readonly: false,
        load_version: 43,
        local_node_id: Some(3),
    });
    assert!(load.status.ok, "{load:?}");
    let lifecycle = runtime.lifecycle_report();
    assert_eq!(lifecycle.failed_count, 0);
    assert_eq!(lifecycle.transitions[0].state, "serving");
    assert_eq!(lifecycle.transitions[0].scheduler_task_id, Some(12));
    assert_eq!(lifecycle.transitions[0].scheduler_generation, Some(900));
}

#[test]
fn runtime_lifecycle_snapshot_restores_transitions_and_tokens() {
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        TemporalEngine::default(),
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    runtime.require_lifecycle_token(SchedulerLifecycleToken {
        task_id: 21,
        shard_id: 7,
        operation: "load".to_string(),
        load_version: 42,
        generation: 700,
    });
    runtime.require_lifecycle_token(SchedulerLifecycleToken {
        task_id: 22,
        shard_id: 7,
        operation: "reload".to_string(),
        load_version: 43,
        generation: 701,
    });
    let load = runtime.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 7,
        table_name: "tbl".to_string(),
        shard_uri: "local://tbl/7".to_string(),
        start_routing_bucket: 10,
        end_routing_bucket: 19,
        readonly: false,
        load_version: 42,
        local_node_id: Some(3),
    });
    assert!(load.status.ok, "{load:?}");
    let reload = runtime.reload_shard_with(crate::control::LoadShardRequest {
        shard_id: 7,
        table_name: "tbl".to_string(),
        shard_uri: "local://tbl/7".to_string(),
        start_routing_bucket: 10,
        end_routing_bucket: 19,
        readonly: true,
        load_version: 43,
        local_node_id: Some(3),
    });
    assert!(reload.status.ok, "{reload:?}");

    let snapshot = runtime.lifecycle_snapshot();
    assert_eq!(snapshot.format_version, 1);
    assert_eq!(snapshot.tokens.len(), 2);
    assert_eq!(snapshot.transitions.len(), 1);
    assert_eq!(snapshot.transitions[0].operation, "reload");
    assert_eq!(snapshot.transitions[0].state, "readonly");
    assert_eq!(snapshot.transitions[0].scheduler_task_id, Some(22));

    let restored = DataNodeRuntime::new_without_workers_with_options(
        TemporalEngine::default(),
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    assert!(restored.restore_lifecycle_snapshot(snapshot.clone()).ok);
    assert_eq!(restored.lifecycle_snapshot(), snapshot);
    assert_eq!(restored.lifecycle_tokens(), snapshot.tokens);
    let lifecycle = restored.lifecycle_report();
    assert_eq!(lifecycle.loaded_shard_count, 0);
    assert_eq!(lifecycle.failed_count, 0);
    assert_eq!(lifecycle.max_load_version, 43);
    assert_eq!(lifecycle.transitions[0].operation, "reload");
    assert_eq!(lifecycle.transitions[0].state, "readonly");

    let stale_reload = restored.reload_shard_with(crate::control::LoadShardRequest {
        shard_id: 7,
        table_name: "stale".to_string(),
        shard_uri: "local://tbl/stale".to_string(),
        start_routing_bucket: 10,
        end_routing_bucket: 19,
        readonly: true,
        load_version: 42,
        local_node_id: Some(3),
    });
    assert_eq!(stale_reload.status.code, "lifecycle_token_mismatch");

    let good_reload = restored.reload_shard_with(crate::control::LoadShardRequest {
        shard_id: 7,
        table_name: "tbl-restored".to_string(),
        shard_uri: "local://tbl/restored".to_string(),
        start_routing_bucket: 10,
        end_routing_bucket: 19,
        readonly: true,
        load_version: 43,
        local_node_id: Some(3),
    });
    assert!(good_reload.status.ok, "{good_reload:?}");
    let lifecycle = restored.lifecycle_report();
    assert_eq!(lifecycle.readonly_count, 1);
    assert_eq!(lifecycle.failed_count, 0);
    assert_eq!(lifecycle.transitions[0].scheduler_task_id, Some(22));
}

#[test]
fn runtime_auto_persists_lifecycle_snapshot_across_transitions() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("data-node-lifecycle.json");
    let runtime = DataNodeRuntime::new_without_workers_with_options_and_lifecycle_snapshot_path(
        TemporalEngine::default(),
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
        Some(path.clone()),
    );

    runtime.require_lifecycle_token(SchedulerLifecycleToken {
        task_id: 31,
        shard_id: 8,
        operation: "load".to_string(),
        load_version: 42,
        generation: 800,
    });
    let saved: DataNodeLifecycleSnapshot =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    assert_eq!(saved.tokens.len(), 1);
    assert!(saved.transitions.is_empty());
    let report = runtime.lifecycle_persistence_report();
    assert!(report.enabled);
    assert_eq!(report.path.as_deref(), Some(path.to_str().unwrap()));
    assert_eq!(report.persist_success_total, 1);
    assert_eq!(report.persist_failure_total, 0);
    assert_eq!(report.last_persist_status.as_ref().unwrap().code, "ok");

    let load = runtime.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 8,
        table_name: "storage_lifecycle".to_string(),
        shard_uri: "local://storage-lifecycle/8".to_string(),
        start_routing_bucket: 10,
        end_routing_bucket: 19,
        readonly: false,
        load_version: 42,
        local_node_id: Some(3),
    });
    assert!(load.status.ok, "{load:?}");
    let saved: DataNodeLifecycleSnapshot =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    assert_eq!(saved.transitions[0].operation, "load");
    assert_eq!(saved.transitions[0].state, "serving");
    assert_eq!(saved.transitions[0].scheduler_task_id, Some(31));
    assert!(runtime.lifecycle_persistence_report().persist_success_total >= 3);

    let restored = DataNodeRuntime::new_without_workers_with_options_and_lifecycle_snapshot_path(
        TemporalEngine::default(),
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
        Some(path.clone()),
    );
    assert_eq!(restored.lifecycle_snapshot(), saved);
    assert_eq!(restored.lifecycle_report().transitions[0].operation, "load");
    let report = restored.lifecycle_persistence_report();
    assert_eq!(report.restore_success_total, 1);
    assert_eq!(report.restore_failure_total, 0);
    assert_eq!(report.last_restore_status.as_ref().unwrap().code, "ok");

    restored.require_lifecycle_token(SchedulerLifecycleToken {
        task_id: 32,
        shard_id: 8,
        operation: "reload".to_string(),
        load_version: 43,
        generation: 801,
    });
    let reload = restored.reload_shard_with(crate::control::LoadShardRequest {
        shard_id: 8,
        table_name: "storage_lifecycle_reloaded".to_string(),
        shard_uri: "local://storage-lifecycle/8-reload".to_string(),
        start_routing_bucket: 10,
        end_routing_bucket: 19,
        readonly: true,
        load_version: 43,
        local_node_id: Some(3),
    });
    assert!(reload.status.ok, "{reload:?}");
    let saved: DataNodeLifecycleSnapshot =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    assert_eq!(saved.transitions[0].operation, "reload");
    assert_eq!(saved.transitions[0].state, "readonly");
    assert_eq!(saved.transitions[0].scheduler_task_id, Some(32));

    restored.require_lifecycle_token(SchedulerLifecycleToken {
        task_id: 33,
        shard_id: 8,
        operation: "unload".to_string(),
        load_version: 43,
        generation: 802,
    });
    let unload = restored.unload_shard_with(crate::control::UnloadShardRequest { shard_id: 8 });
    assert!(unload.status.ok, "{unload:?}");
    let saved: DataNodeLifecycleSnapshot =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    assert_eq!(saved.tokens.len(), 3);
    assert_eq!(saved.transitions[0].operation, "unload");
    assert_eq!(saved.transitions[0].state, "unloaded");
    assert_eq!(saved.transitions[0].scheduler_task_id, Some(33));
}

#[test]
fn runtime_reports_bad_lifecycle_snapshot_restore_in_preflight() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("bad-data-node-lifecycle.json");
    fs::write(&path, b"{not json").unwrap();

    let runtime = DataNodeRuntime::new_without_workers_with_options_and_lifecycle_snapshot_path(
        TemporalEngine::default(),
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
        Some(path.clone()),
    );
    let report = runtime.lifecycle_persistence_report();
    assert!(report.enabled);
    assert_eq!(report.restore_success_total, 0);
    assert_eq!(report.restore_failure_total, 1);
    assert_eq!(
        report.last_restore_status.as_ref().unwrap().code,
        "bad_lifecycle_snapshot"
    );

    let preflight = runtime.preflight_report();
    assert_eq!(preflight.status.code, "degraded");
    assert!(preflight
        .degraded_reasons
        .contains(&"lifecycle_snapshot_restore_failed".to_string()));
    assert_eq!(preflight.lifecycle_persistence, report);
}

#[test]
fn runtime_lifecycle_snapshot_rejects_unknown_format() {
    let runtime = DataNodeRuntime::new_without_workers_for_test(TemporalEngine::default(), 4);
    let status = runtime.restore_lifecycle_snapshot(DataNodeLifecycleSnapshot {
        format_version: 99,
        transitions: Vec::new(),
        tokens: Vec::new(),
    });
    assert_eq!(status.code, "bad_lifecycle_snapshot");
}

#[test]
fn runtime_async_lifecycle_jobs_report_progress_and_outputs() {
    let runtime = DataNodeRuntime::new(
        TemporalEngine::default(),
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 4,
        },
    );

    let submitted = runtime.submit_load(
        crate::control::LoadShardRequest {
            shard_id: 7,
            table_name: "tbl".to_string(),
            shard_uri: "local://tbl/7".to_string(),
            start_routing_bucket: 10,
            end_routing_bucket: 19,
            readonly: false,
            load_version: 42,
            local_node_id: Some(3),
        },
        RequestController { timeout_ms: 1000 },
    );
    assert!(submitted.status.ok, "{submitted:?}");
    assert_eq!(submitted.kind, DataNodeTaskKind::Load);
    assert!(submitted.finished_at_ms.is_none());

    let finished = wait_for_job(&runtime, submitted.job_id);
    assert!(finished.status.ok, "{finished:?}");
    let Some(DataNodeTaskOutput::Load(output)) = finished.output else {
        panic!("expected load output");
    };
    assert!(output.status.ok, "{output:?}");
    let lifecycle = runtime.lifecycle_report();
    assert_eq!(lifecycle.serving_count, 1);
    assert_eq!(lifecycle.transitions[0].operation, "load");
    assert_eq!(lifecycle.transitions[0].state, "serving");

    let reloaded = runtime.submit_reload(
        crate::control::LoadShardRequest {
            shard_id: 7,
            table_name: "tbl-new".to_string(),
            shard_uri: "local://tbl/7-new".to_string(),
            start_routing_bucket: 10,
            end_routing_bucket: 19,
            readonly: true,
            load_version: 43,
            local_node_id: Some(3),
        },
        RequestController { timeout_ms: 1000 },
    );
    let reloaded = wait_for_job(&runtime, reloaded.job_id);
    let Some(DataNodeTaskOutput::Reload(output)) = reloaded.output else {
        panic!("expected reload output");
    };
    assert!(output.status.ok, "{output:?}");
    assert_eq!(runtime.lifecycle_report().readonly_count, 1);

    let unloaded = runtime.submit_unload(
        crate::control::UnloadShardRequest { shard_id: 7 },
        RequestController { timeout_ms: 1000 },
    );
    let unloaded = wait_for_job(&runtime, unloaded.job_id);
    let Some(DataNodeTaskOutput::Unload(output)) = unloaded.output else {
        panic!("expected unload output");
    };
    assert!(output.status.ok, "{output:?}");
    assert_eq!(runtime.lifecycle_report().loaded_shard_count, 0);
}

#[test]
fn runtime_rejects_foreground_writes_during_lifecycle_transition() {
    let runtime = DataNodeRuntime::new_without_workers_for_test(TemporalEngine::default(), 8);
    let load = runtime.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 7,
        table_name: "tbl".to_string(),
        shard_uri: "local://tbl/7".to_string(),
        start_routing_bucket: 10,
        end_routing_bucket: 19,
        readonly: false,
        load_version: 42,
        local_node_id: Some(3),
    });
    assert!(load.status.ok, "{load:?}");
    let seed = runtime.execute(ExecuteRequest {
        shard_id: 7,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert!(seed.status.ok, "{seed:?}");

    record_lifecycle_state_inner(&runtime.inner, 7, "reloading", "reload", 43, None);

    let write = runtime.execute(ExecuteRequest {
        shard_id: 7,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"blocked".to_vec(),
        },
    });
    assert_eq!(write.status.code, "lifecycle_write_blocked");
    let checked = runtime.execute_checked(CheckedExecuteRequest {
        shard_id: 7,
        load_version: 42,
        command: Command::StringDelete {
            key: "k".to_string(),
        },
    });
    assert_eq!(checked.status.code, "lifecycle_write_blocked");
    let batch = runtime.batch_execute(BatchExecuteRequest {
        shard_id: 7,
        commands: vec![Command::HashSet {
            key: "h".to_string(),
            field: "f".to_string(),
            value: b"v".to_vec(),
        }],
    });
    assert_eq!(batch.status.code, "lifecycle_write_blocked");

    let read = runtime.execute(ExecuteRequest {
        shard_id: 7,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert!(read.status.ok, "{read:?}");
    assert_eq!(
        read.response,
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );
}

#[test]
fn runtime_rejects_queued_foreground_write_during_lifecycle_transition() {
    let runtime = DataNodeRuntime::new_without_workers_for_test(TemporalEngine::default(), 8);
    let load = runtime.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 7,
        table_name: "tbl".to_string(),
        shard_uri: "local://tbl/7".to_string(),
        start_routing_bucket: 10,
        end_routing_bucket: 19,
        readonly: false,
        load_version: 42,
        local_node_id: Some(3),
    });
    assert!(load.status.ok, "{load:?}");
    record_lifecycle_state_inner(&runtime.inner, 7, "unloading", "unload", 42, None);
    let submitted = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 7,
            command: Command::StringSet {
                key: "queued".to_string(),
                value: b"v".to_vec(),
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    let task = runtime
        .inner
        .queue
        .lock()
        .expect("runtime queue lock poisoned")
        .pop_ready()
        .expect("queued write should be ready");
    assert_eq!(task.job_id, submitted.job_id);

    let output = execute_task(&runtime.inner, &task);
    let DataNodeTaskOutput::Execute(response) = output else {
        panic!("expected execute output");
    };
    assert_eq!(response.status.code, "lifecycle_write_blocked");
}

#[test]
fn runtime_executes_async_tracks_dirty_and_dump_clears_it() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    let job = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    let finished = wait_for_job(&runtime, job.job_id);
    assert!(finished.status.ok);
    assert_eq!(runtime.dirty_objects().len(), 1);

    let dump = runtime.submit_dump(
        DumpShardRequest {
            shard_id: 1,
            selected_routing_buckets: Vec::new(),
        },
        RequestController { timeout_ms: 1000 },
    );
    let finished = wait_for_job(&runtime, dump.job_id);
    let Some(DataNodeTaskOutput::Dump(output)) = finished.output else {
        panic!("expected dump output");
    };
    assert_eq!(output.dirty_objects_flushed, 1);
    assert!(runtime.dirty_objects().is_empty());
}

#[test]
fn runtime_dump_can_flush_only_selected_dirty_buckets() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let mut key_a = String::new();
    let mut key_b = String::new();
    let mut bucket_a = 0;
    for index in 0..128 {
        let key = format!("slot-key-{index}");
        let bucket = engine.routing_bucket_for_key(1, &key);
        if key_a.is_empty() {
            key_a = key;
            bucket_a = bucket;
        } else if bucket != bucket_a {
            key_b = key;
            break;
        }
    }
    assert!(!key_b.is_empty(), "test needs two distinct routing slots");
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    for key in [&key_a, &key_b] {
        let job = runtime.submit_execute(
            ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: key.to_string(),
                    value: key.as_bytes().to_vec(),
                },
            },
            RequestController { timeout_ms: 1000 },
        );
        assert!(wait_for_job(&runtime, job.job_id).status.ok);
    }
    assert_eq!(runtime.dirty_objects().len(), 2);

    let dump = runtime.submit_dump(
        DumpShardRequest {
            shard_id: 1,
            selected_routing_buckets: vec![bucket_a],
        },
        RequestController { timeout_ms: 1000 },
    );
    let finished = wait_for_job(&runtime, dump.job_id);
    let Some(DataNodeTaskOutput::Dump(output)) = finished.output else {
        panic!("expected dump output");
    };
    assert!(output.status.ok);
    assert_eq!(output.dirty_objects_flushed, 1);
    assert_eq!(
        output.bucket_dump_manifest.as_ref().unwrap().bucket_ids,
        vec![bucket_a]
    );
    let remaining = runtime.dirty_objects();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].key, key_b);
}

#[test]
fn previously_misclassified_writes_mark_shard_dirty() {
    // The data_node write classifier delegates to the engine's authoritative one, so writes it
    // used to omit (context / control-state change+fol / conditional
    // string) now correctly mark the shard dirty (and hit the lifecycle write gate). Regression:
    // a ControlStateSelectionSet -- previously classified READ here -- must mark the shard dirty.
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    let job = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateSelectionSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
                occur_time_ms: 1,
                ttl_ms: 0,
                selection_type: crate::types::ControlStateSelectionType::First,
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    assert!(wait_for_job(&runtime, job.job_id).status.ok);
    assert_eq!(
        runtime.dirty_shards(),
        vec![1],
        "a control-state FOL write must be classified as a write and mark the shard dirty"
    );
}

#[test]
fn gc_does_not_clear_the_dirty_scheduling_tracker() {
    // GC (block/index reclaim) never touches the dirty-bucket set -- a bucket leaves it
    // only via a completed dump/replay that clears its dirty flag. GC must not drop the re-dump
    // scheduling state, or schedule_dirty_shard_dumps would stop scheduling those objects.
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    let write = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    assert!(wait_for_job(&runtime, write.job_id).status.ok);
    assert_eq!(runtime.dirty_shards(), vec![1]);

    let gc = runtime.submit_gc(
        GcRequest {
            shard_id: 1,
            retain_wal_from_sequence: None,
            retain_index_log_from_sequence: None,
            retain_block_slabs_from_id: None,
            page_gc_delayed_destroy: false,
            page_gc_invalidate_removed_slabs_only: false,
        },
        RequestController { timeout_ms: 1000 },
    );
    assert!(wait_for_job(&runtime, gc.job_id).status.ok);
    assert_eq!(
        runtime.dirty_shards(),
        vec![1],
        "GC must not clear the dirty-scheduling tracker (only a completed dump does)"
    );
}

#[test]
fn runtime_schedules_dumps_for_dirty_shards() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine.load_shard(2);
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 2,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );

    for (shard_id, key) in [(1, "alpha"), (2, "beta")] {
        let job = runtime.submit_execute(
            ExecuteRequest {
                shard_id,
                command: Command::StringSet {
                    key: key.to_string(),
                    value: key.as_bytes().to_vec(),
                },
            },
            RequestController { timeout_ms: 1000 },
        );
        assert!(wait_for_job(&runtime, job.job_id).status.ok);
    }

    assert_eq!(runtime.dirty_shards(), vec![1, 2]);
    let dumps = runtime.schedule_dirty_shard_dumps(RequestController { timeout_ms: 1000 });
    assert_eq!(dumps.len(), 2);
    for dump in dumps {
        let finished = wait_for_job(&runtime, dump.job_id);
        let Some(DataNodeTaskOutput::Dump(output)) = finished.output else {
            panic!("expected dump output");
        };
        assert!(output.status.ok);
        assert_eq!(output.dirty_objects_flushed, 1);
    }
    assert!(runtime.dirty_objects().is_empty());
    assert!(runtime.dirty_shards().is_empty());
}

#[test]
fn runtime_preflight_reports_dirty_backlog_and_queue_degradation() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 1,
            max_background_queue_depth: 1,
        },
    );
    mark_dirty(&runtime.inner.dirty, 1, Some("dirty-key"));
    let first = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "queued".to_string(),
                value: b"v".to_vec(),
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    assert!(first.status.ok);
    let rejected = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "queued".to_string(),
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    assert_eq!(rejected.status.code, "queue_full");

    let preflight = runtime.preflight_report();

    assert!(!preflight.status.ok);
    assert!(preflight
        .degraded_reasons
        .contains(&"foreground_queue_full".to_string()));
    assert!(preflight
        .degraded_reasons
        .contains(&"rejected_requests".to_string()));
    assert_eq!(preflight.stats.queue_depth, 1);
    assert_eq!(preflight.queued_workers.len(), 1);
    assert_eq!(preflight.dirty_shards, vec![1]);
    assert_eq!(preflight.dirty_objects.len(), 1);
}

#[test]
fn runtime_builds_style_server_load_report() {
    let engine = TemporalEngine::default();
    assert!(
        engine
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 7,
                table_name: "tbl".to_string(),
                shard_uri: "local://tbl/7".to_string(),
                start_routing_bucket: 10,
                end_routing_bucket: 19,
                readonly: false,
                load_version: 42,
                local_node_id: Some(3),
            })
            .status
            .ok
    );
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine.clone(),
        DataNodeRuntimeOptions {
            worker_threads: 4,
            max_queue_depth: 2,
            max_background_queue_depth: 1,
        },
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 7,
                command: Command::StringSet {
                    key: "k".to_string(),
                    value: b"v".to_vec(),
                },
            })
            .status
            .ok
    );
    mark_dirty(&runtime.inner.dirty, 7, Some("k"));
    let queued = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 7,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    assert!(queued.status.ok);

    let load = runtime.server_runtime_load();
    assert_eq!(load.queue_depth, 1);
    assert_eq!(load.queued_shard_count, 1);
    assert_eq!(load.dirty_object_count, 1);
    assert_eq!(load.last_meta_topology_version, 0);
    runtime.record_metaserver_heartbeat(&ServerHeartbeatResponse {
        status: Status::error("resource_frozen", "server frozen"),
        forbid_auto_register: true,
        topology_version: 99,
        server_state: "frozen".to_string(),
    });
    let preflight = runtime.preflight_report();
    assert_eq!(preflight.metaserver.last_topology_version, 99);
    assert_eq!(preflight.metaserver.last_server_state, "frozen");
    assert_eq!(preflight.metaserver.consecutive_failures, 1);
    assert!(preflight
        .degraded_reasons
        .contains(&"metaserver_heartbeat_failed".to_string()));
    assert!(preflight
        .degraded_reasons
        .contains(&"metaserver_forbid_auto_register".to_string()));
    let load = runtime.server_runtime_load();
    assert_eq!(load.last_meta_topology_version, 99);
    assert_eq!(load.meta_heartbeat_consecutive_failures, 1);
    assert!(load.meta_forbid_auto_register);
    runtime.record_metaserver_heartbeat(&ServerHeartbeatResponse {
        status: Status::ok(),
        forbid_auto_register: false,
        topology_version: 100,
        server_state: "normal".to_string(),
    });
    let recovered = runtime.preflight_report();
    assert_eq!(recovered.metaserver.consecutive_failures, 0);
    assert_eq!(recovered.metaserver.last_topology_version, 100);
    assert_eq!(
        recovered.topology_validation.last_meta_topology_version,
        100
    );
    assert_eq!(recovered.topology_validation.loaded_shards, vec![7]);
    assert!(recovered.topology_validation.validation_limited);
    let topology = TableTopologyResponse {
        status: Status::ok(),
        table: Some(crate::meta::TableMetaInfo {
            table_id: 1,
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            state: crate::meta::MetaEntityState::Normal,
            topology_version: 100,
            first_shard_id: 7,
            shard_count: 1,
            replica_count: 1,
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        }),
        shards: vec![crate::meta::TableShard {
            load_version: 0,
            shard_id: 7,
            start_bucket: 10,
            end_bucket: 19,
            primary: Some("server-a".to_string()),
            replicas: vec!["server-a".to_string()],
            primary_endpoint: None,
            replica_endpoints: Vec::new(),
        }],
        unchanged: false,
    };
    let validated = runtime.validate_topology_against_metaserver("server-a", &[topology]);
    assert!(validated.validated_against_metaserver);
    assert!(!validated.validation_limited);
    assert_eq!(validated.authoritative_topology_version, 100);
    assert_eq!(validated.mismatch_count, 0);
    let mismatch_topology = TableTopologyResponse {
        status: Status::ok(),
        table: None,
        shards: vec![crate::meta::TableShard {
            load_version: 0,
            shard_id: 7,
            start_bucket: 0,
            end_bucket: 9,
            primary: Some("server-b".to_string()),
            replicas: vec!["server-b".to_string()],
            primary_endpoint: None,
            replica_endpoints: Vec::new(),
        }],
        unchanged: false,
    };
    let mismatched = runtime.validate_topology_against_metaserver("server-a", &[mismatch_topology]);
    assert!(mismatched.mismatch_count >= 2);
    let shard_states = runtime.shard_serving_states();
    assert_eq!(shard_states.len(), 1);
    let shard = &shard_states[0];
    assert_eq!(shard.shard_id, 7);
    assert_eq!(shard.serving_state, "queued");
    assert_eq!(shard.worker_index, 0);
    assert_eq!(shard.worker_threads, 1);
    assert!(!shard.readonly);
    assert_eq!(shard.load_version, 42);
    assert_eq!(shard.table_name, "tbl");
    assert_eq!(shard.dirty_object_count, 1);
    assert_eq!(shard.wal_sequence, 1);
}

#[test]
fn runtime_dirty_dump_scheduler_periodically_flushes_dirty_shards() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine.load_shard(2);
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 2,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    let scheduler = runtime.start_dirty_dump_scheduler(
        Duration::from_millis(5),
        RequestController { timeout_ms: 1000 },
    );

    for (shard_id, key) in [(1, "periodic-a"), (2, "periodic-b")] {
        let job = runtime.submit_execute(
            ExecuteRequest {
                shard_id,
                command: Command::StringSet {
                    key: key.to_string(),
                    value: b"v".to_vec(),
                },
            },
            RequestController { timeout_ms: 1000 },
        );
        assert!(wait_for_job(&runtime, job.job_id).status.ok);
    }

    wait_until(Duration::from_secs(1), || {
        runtime.dirty_objects().is_empty() && runtime.stats().dump_runs >= 2
    });
    scheduler.stop();
    assert!(runtime.dirty_objects().is_empty());
    assert_eq!(runtime.dirty_shards(), Vec::<ShardId>::new());
}

#[test]
fn runtime_dirty_dump_scheduler_skips_already_queued_dump_for_shard() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    mark_dirty(&runtime.inner.dirty, 1, Some("queued"));
    let first = runtime.schedule_dirty_shard_dumps(RequestController { timeout_ms: 1000 });
    let second = runtime.schedule_dirty_shard_dumps(RequestController { timeout_ms: 1000 });

    assert_eq!(first.len(), 1);
    assert!(first[0].status.ok);
    assert!(second.is_empty());
    assert_eq!(runtime.stats().background_queue_depth, 1);
}

#[test]
fn runtime_storage_lifecycle_scheduler_runs_periodically() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    let job = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "lifecycle-scheduler".to_string(),
                value: b"v".to_vec(),
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    assert!(wait_for_job(&runtime, job.job_id).status.ok);
    let scheduler = runtime.start_storage_lifecycle_scheduler(
        Duration::from_millis(5),
        StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_buckets: Vec::new(),
            max_dump_buckets_per_round: 0,
            min_undumped_wal_records: 0,
            min_undumped_wal_bytes: 0,
            purge_delayed_destroy: false,
            prune_bucket_dump_manifests: false,
            roll_forward_bucket_dump_installs: false,
            follower_replay_cursors: Vec::new(),
            page_gc_shared_store_cursors: Vec::new(),
            page_gc_raft_snapshot_refs: Vec::new(),
            page_gc_checkpoint_floor_slab_id: None,
            page_gc_raft_install_floor_slab_id: None,
            page_gc_delayed_destroy_grace_ms: 0,
            invalidate_cache: false,
            warm_cache: true,
        },
    );
    wait_until(Duration::from_secs(1), || {
        runtime.stats().storage_lifecycle_runs >= 1
    });
    scheduler.stop();
    assert!(runtime.stats().storage_lifecycle_runs >= 1);
}

#[test]
// shared-corpus: storage_dump_load_recovery storage_cache_refill;
fn runtime_storage_manager_loop_runs_style_pressure_stages() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine.clone(), 8);

    for (key, value) in [
        ("manager-a", b"old".to_vec()),
        ("manager-a", b"new".to_vec()),
        ("manager-b", b"two".to_vec()),
    ] {
        let response = runtime.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.to_string(),
                value,
            },
        });
        assert!(response.status.ok, "{response:?}");
    }
    let ttl = runtime.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSetEx {
            key: "manager-ttl".to_string(),
            value: b"gone".to_vec(),
            ttl_ms: 1,
        },
    });
    assert!(ttl.status.ok, "{ttl:?}");
    let cached = runtime.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "manager-a".to_string(),
        },
    });
    assert!(cached.status.ok, "{cached:?}");
    std::thread::sleep(Duration::from_millis(2));

    let report = runtime.run_storage_manager_once(
        1,
        StorageManagerOptions {
            max_dump_buckets_per_round: 16,
            min_undumped_wal_records: 1,
            dirty_bucket_pressure: 1,
            stale_block_slab_pressure: 1,
            reclaimable_physical_bytes_pressure: 1,
            cache_memory_bytes_pressure: 1,
            cache_disk_bytes_pressure: 1,
            ..StorageManagerOptions::default()
        },
    );

    assert!(report.status.ok, "{report:?}");
    for stage in [
        "prepare",
        "reclaim_wal",
        "reclaim_memory",
        "expire",
        "reclaim_page",
        "compact_pages",
        "reclaim_index",
        "reap_metrics",
    ] {
        assert!(
            report
                .executed_stages
                .iter()
                .any(|executed| executed == stage),
            "missing stage {stage}: {report:?}"
        );
    }
    assert!(report.pressure.dirty_bucket_count >= 1);
    assert!(report.pressure.undumped_wal_records >= 1);
    // Nine since the evict stage was wired: eight that ship on, plus evict, which reports a
    // decision every round whether or not it is enabled.
    assert_eq!(report.pressure_decisions.len(), 9, "{report:?}");
    for stage in [
        "prepare",
        "reclaim_wal",
        "reclaim_memory",
        "expire",
        "reclaim_page",
        "compact_pages",
        "reclaim_index",
        "reap_metrics",
    ] {
        let decision = report
            .pressure_decisions
            .iter()
            .find(|decision| decision.stage == stage)
            .unwrap_or_else(|| panic!("missing pressure decision {stage}: {report:?}"));
        assert!(decision.enabled, "{decision:?}");
        assert!(decision.executed, "{decision:?}");
        assert!(!decision.signals.is_empty(), "{decision:?}");
        assert!(decision.skip_reason.is_none(), "{decision:?}");
    }
    let reclaim_wal = report
        .pressure_decisions
        .iter()
        .find(|decision| decision.stage == "reclaim_wal")
        .unwrap();
    assert!(reclaim_wal.pressure_active, "{reclaim_wal:?}");
    assert!(reclaim_wal
        .signals
        .iter()
        .any(|signal| signal.name == "dirty_slot_count" && signal.over_threshold));
    assert!(reclaim_wal
        .signals
        .iter()
        .any(|signal| signal.name == "undumped_wal_records" && signal.over_threshold));
    assert!(reclaim_wal
        .trigger_reasons
        .iter()
        .any(|reason| reason == "dirty_slot_pressure" || reason == "undumped_wal_pressure"));
    let reclaim_memory = report
        .pressure_decisions
        .iter()
        .find(|decision| decision.stage == "reclaim_memory")
        .unwrap();
    assert!(reclaim_memory
        .signals
        .iter()
        .any(|signal| signal.name == "cache_memory_bytes"));
    assert!(reclaim_memory
        .signals
        .iter()
        .any(|signal| signal.name == "cache_disk_bytes"));
    let compact_pages = report
        .pressure_decisions
        .iter()
        .find(|decision| decision.stage == "compact_pages")
        .unwrap();
    assert!(compact_pages
        .signals
        .iter()
        .any(|signal| signal.name == "reclaimable_physical_bytes"));
    let reclaim_index = report
        .pressure_decisions
        .iter()
        .find(|decision| decision.stage == "reclaim_index")
        .unwrap();
    assert!(reclaim_index
        .signals
        .iter()
        .any(|signal| signal.name == "manifest_prune_reasons"));
    assert!(reclaim_index
        .signals
        .iter()
        .any(|signal| signal.name == "install_roll_forward_reasons"));
    assert!(!report.lifecycle_plan.reclaim_candidates.is_empty());
    assert!(report.lifecycle_report.is_some());
    assert!(report.compaction_report.as_ref().unwrap().status.ok);
    assert!(report.gc_report.as_ref().unwrap().status.ok);
    assert!(report.expired_records_removed >= 1);
    assert!(runtime.dirty_objects().is_empty());

    let stats = runtime.stats();
    assert_eq!(stats.storage_manager_loops, 1);
    assert_eq!(stats.storage_manager_prepare_runs, 1);
    assert_eq!(stats.storage_manager_reclaim_wal_runs, 1);
    assert_eq!(stats.storage_manager_reclaim_memory_runs, 1);
    assert_eq!(stats.storage_manager_expire_runs, 1);
    assert_eq!(stats.storage_manager_reclaim_page_runs, 1);
    assert_eq!(stats.storage_manager_compact_runs, 1);
    assert_eq!(stats.storage_manager_index_gc_runs, 1);
}

#[test]
// shared-corpus: storage_manager_pressure_scale_evidence;
fn runtime_storage_manager_scale_repeats_style_pressure_stages() {
    let dir = tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        512,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(17);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 8);
    let mut reports = Vec::new();

    for round in 0..4 {
        for key_index in 0..16 {
            let key = format!("manager-scale-{round}-{key_index}");
            let first = runtime.execute(ExecuteRequest {
                shard_id: 17,
                command: Command::StringSet {
                    key: key.clone(),
                    value: vec![round as u8; 128 + key_index],
                },
            });
            assert!(first.status.ok, "{first:?}");
            let second = runtime.execute(ExecuteRequest {
                shard_id: 17,
                command: Command::StringSet {
                    key: key.clone(),
                    value: vec![(round + 1) as u8; 192 + key_index],
                },
            });
            assert!(second.status.ok, "{second:?}");
            let cached = runtime.execute(ExecuteRequest {
                shard_id: 17,
                command: Command::StringGet { key },
            });
            assert!(cached.status.ok, "{cached:?}");
        }
        let ttl_key = format!("manager-scale-ttl-{round}");
        let ttl = runtime.execute(ExecuteRequest {
            shard_id: 17,
            command: Command::StringSetEx {
                key: ttl_key,
                value: b"expire-me".repeat(16),
                ttl_ms: 1,
            },
        });
        assert!(ttl.status.ok, "{ttl:?}");
        std::thread::sleep(Duration::from_millis(3));

        let report = runtime.run_storage_manager_once(
            17,
            StorageManagerOptions {
                max_dump_buckets_per_round: 64,
                min_undumped_wal_records: 1,
                dirty_bucket_pressure: 1,
                stale_block_slab_pressure: 1,
                reclaimable_physical_bytes_pressure: 1,
                cache_memory_bytes_pressure: 1,
                cache_disk_bytes_pressure: 1,
                ..StorageManagerOptions::default()
            },
        );
        assert!(report.status.ok, "{report:?}");
        reports.push(report);
    }

    let required_stages = [
        "prepare",
        "reclaim_wal",
        "reclaim_memory",
        "expire",
        "reclaim_page",
        "compact_pages",
        "reclaim_index",
        "reap_metrics",
    ];
    for report in &reports {
        for stage in required_stages {
            assert!(
                report
                    .pressure_decisions
                    .iter()
                    .any(|decision| decision.stage == stage
                        && decision.enabled
                        && decision.executed
                        && !decision.signals.is_empty()),
                "missing executed pressure decision {stage}: {report:?}"
            );
        }
        assert!(report.pressure.dirty_bucket_count >= 1, "{report:?}");
        assert!(report.pressure.undumped_wal_records >= 1, "{report:?}");
        assert!(report.lifecycle_report.is_some(), "{report:?}");
        assert!(
            report.gc_report.as_ref().is_some_and(|gc| gc.status.ok),
            "{report:?}"
        );
        assert!(
            report
                .compaction_report
                .as_ref()
                .is_some_and(|compaction| compaction.status.ok),
            "{report:?}"
        );
    }

    assert!(reports
        .iter()
        .any(|report| report.expired_records_removed >= 1));
    assert!(reports.iter().any(|report| {
        report
            .pressure_decisions
            .iter()
            .any(|decision| decision.stage == "reclaim_memory" && decision.pressure_active)
    }));
    assert!(reports.iter().any(|report| {
        report
            .pressure_decisions
            .iter()
            .any(|decision| decision.stage == "compact_pages" && decision.pressure_active)
    }));
    assert!(runtime.dirty_objects().is_empty());

    let stats = runtime.stats();
    assert_eq!(stats.storage_manager_loops, reports.len() as u64);
    assert_eq!(stats.storage_manager_prepare_runs, reports.len() as u64);
    assert_eq!(
        stats.storage_manager_reclaim_wal_runs,
        reports.len() as u64
    );
    assert_eq!(
        stats.storage_manager_reclaim_memory_runs,
        reports.len() as u64
    );
    assert_eq!(stats.storage_manager_expire_runs, reports.len() as u64);
    assert_eq!(
        stats.storage_manager_reclaim_page_runs,
        reports.len() as u64
    );
    assert_eq!(stats.storage_manager_compact_runs, reports.len() as u64);
    assert_eq!(stats.storage_manager_index_gc_runs, reports.len() as u64);
}

/// The all-shards loop visits EVERY loaded shard, and picks up one loaded after it started.
///
/// The per-shard scheduler beside this one has to be told which shard it serves, which is why
/// nothing in the server ever started one -- a server does not know its shards up front. This
/// asks the engine each tick, so the thing worth asserting is not that it ran, but that it
/// reached a shard nobody named when it was started.
///
/// Asserted through the SHARD ID the last round recorded, not through a loop counter. The first
/// version of this counted `storage_manager_loops` and waited for it to climb, which at a 5 ms
/// interval it does whatever the loop visits -- so it measured the clock, and BOTH mutations
/// (visit only the first shard; snapshot the shard list at startup) passed it. A count per tick
/// cannot see which shard a tick chose. The report's shard id can.
#[test]
fn the_all_shards_scheduler_reaches_a_shard_loaded_after_it_started() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new(
        engine.clone(),
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    let response = runtime.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "all-shards-1".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert!(response.status.ok, "{response:?}");

    let scheduler = runtime.start_storage_manager_scheduler_for_all_shards(
        Duration::from_millis(5),
        StorageManagerOptions::default(),
    );
    wait_until(Duration::from_secs(2), || {
        runtime.stats().storage_manager_last_shard_id == Some(1)
    });
    assert_eq!(
        runtime.stats().storage_manager_last_shard_id,
        Some(1),
        "the loop never ran for the shard it started with"
    );

    // A shard the scheduler was never told about, loaded while it is already running.
    engine.load_shard(2);
    let response = runtime.execute(ExecuteRequest {
        shard_id: 2,
        command: Command::StringSet {
            key: "all-shards-2".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert!(response.status.ok, "{response:?}");
    assert!(
        engine.loaded_shard_ids().contains(&2),
        "the engine must be holding the second shard for this to prove anything"
    );

    // The report has to name shard 2 at some point, which only happens if a tick chose it.
    wait_until(Duration::from_secs(5), || {
        runtime.stats().storage_manager_last_shard_id == Some(2)
    });
    let reached = runtime.stats().storage_manager_last_shard_id;
    scheduler.stop();
    assert_eq!(
        reached,
        Some(2),
        "the loop never reached the shard loaded after it started; last report was for {reached:?}"
    );
}

#[test]
// rust-internal: validates periodic runtime scheduling for the storage-manager loop
fn runtime_storage_manager_scheduler_runs_continuous_loop() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    let response = runtime.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "manager-scheduler".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert!(response.status.ok, "{response:?}");

    let scheduler = runtime.start_storage_manager_scheduler(
        Duration::from_millis(5),
        1,
        StorageManagerOptions::default(),
    );
    wait_until(Duration::from_secs(1), || {
        runtime.stats().storage_manager_loops >= 1
    });
    scheduler.stop();
    assert!(runtime.stats().storage_manager_loops >= 1);
}

#[test]
fn runtime_expiry_sweep_scheduler_removes_expired_records() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new(
        engine.clone(),
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSetEx {
                    key: "ttl".to_string(),
                    value: b"gone".to_vec(),
                    ttl_ms: 1,
                },
            })
            .status
            .ok
    );
    let scheduler = runtime.start_expiry_sweep_scheduler(Duration::from_millis(5));
    wait_until(Duration::from_secs(1), || {
        runtime.stats().expired_records_removed >= 1
    });
    scheduler.stop();
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "ttl".to_string()
                },
            })
            .response,
        CommandResponse::Bytes { value: None }
    );
    assert!(runtime.stats().expiry_sweeps >= 1);
}

#[test]
fn runtime_rejects_when_queue_is_full() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 1,
            max_background_queue_depth: 1,
        },
    );
    let _first = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "a".to_string(),
                value: b"1".to_vec(),
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    let rejected = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "b".to_string(),
                value: b"2".to_vec(),
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    if !rejected.status.ok {
        assert_eq!(rejected.status.code, "queue_full");
    }
}

#[test]
fn runtime_cancel_reports_not_found_and_already_finished_jobs() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new(engine, DataNodeRuntimeOptions::default());

    assert_eq!(runtime.cancel_job(42).status.code, "job_not_found");

    let submitted = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    let finished = wait_for_job(&runtime, submitted.job_id);
    assert!(finished.status.ok);
    assert_eq!(
        runtime.cancel_job(submitted.job_id).status.code,
        "job_already_finished"
    );
}

#[test]
fn runtime_compaction_rewrites_live_pages_and_reports_stale_slabs() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for (key, value) in [
        ("a", b"old".to_vec()),
        ("a", b"one".to_vec()),
        ("b", b"two".to_vec()),
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.to_string(),
                value,
            },
        });
        assert!(response.status.ok);
    }
    assert_eq!(engine.live_block_slab_ids(1), vec![0]);

    let runtime = DataNodeRuntime::new(
        engine.clone(),
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    let submitted = runtime.submit_compaction(
        CompactionRequest { shard_id: 1 },
        RequestController { timeout_ms: 1000 },
    );
    assert!(submitted.status.ok, "{submitted:?}");
    let finished = wait_for_job(&runtime, submitted.job_id);
    let Some(DataNodeTaskOutput::Compact(output)) = finished.output else {
        panic!("expected compaction output");
    };
    assert!(output.status.ok);
    assert_eq!(output.compacted_objects, 2);
    assert_eq!(output.previous_block_slab_id, 0);
    assert_eq!(output.compacted_block_slab_id, 1);
    assert_eq!(output.stale_block_slab_ids, vec![0]);
    assert_eq!(output.before.total_page_count, 3);
    assert_eq!(output.before.live_page_refs, 2);
    assert_eq!(output.before.stale_page_estimate, 1);
    assert_eq!(output.before.live_ref_density_basis_points, 6_666);
    assert_eq!(output.after.total_page_count, 2);
    assert_eq!(output.after.live_page_refs, 2);
    assert_eq!(output.after.stale_page_estimate, 0);
    assert_eq!(output.after.live_ref_density_basis_points, 10_000);
    assert_eq!(engine.live_block_slab_ids(1), vec![1]);
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "a".to_string()
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"one".to_vec())
        }
    );
}

#[test]
fn runtime_gc_reclaims_log_tails_and_reports_counts() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for key in ["a", "b", "c"] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.to_string(),
                value: key.as_bytes().to_vec(),
            },
        });
        assert!(response.status.ok);
    }
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "a".to_string()
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"a".to_vec())
        }
    );
    engine.block_store().install_slab(1, b"old").unwrap();
    engine.block_store().install_slab(2, b"new").unwrap();

    let runtime = DataNodeRuntime::new(
        engine.clone(),
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    let submitted = runtime.submit_gc(
        GcRequest {
            shard_id: 1,
            retain_wal_from_sequence: Some(3),
            retain_index_log_from_sequence: Some(2),
            retain_block_slabs_from_id: Some(2),
            page_gc_delayed_destroy: false,
            page_gc_invalidate_removed_slabs_only: false,
        },
        RequestController { timeout_ms: 1000 },
    );
    let finished = wait_for_job(&runtime, submitted.job_id);
    let Some(DataNodeTaskOutput::Gc(output)) = finished.output else {
        panic!("expected gc output");
    };
    assert!(output.status.ok);
    assert_eq!(output.cache_entries_removed, 2);
    assert!(output.cache_disk_bytes_removed > 0);
    assert_eq!(output.wal_records_removed, 2);
    assert_eq!(output.index_log_records_removed, 1);
    assert_eq!(output.block_slabs_removed, 1);
    assert!(output.block_slabs_removed_physical_bytes > 0);
    assert!(output.block_slabs_retained_physical_bytes > 0);
    assert_eq!(output.block_slabs_retained_live, 1);
    assert!(output.block_slabs_retained_live_physical_bytes > 0);
    assert_eq!(engine.write_ahead_log_store().stats(1).last_sequence, 3);
    assert_eq!(engine.index_log_store().stats(1).last_sequence, 3);
    assert_eq!(engine.block_store().slab_ids().unwrap(), vec![0, 2]);
}

#[test]
fn operator_gc_retains_slabs_referenced_by_dump_manifest() {
    // The /gc operator RPC must not delete a page slab a durable bucket-dump manifest still
    // references, even when retain_block_slabs_from_id would sweep it and it is no longer in
    // the resident live set. Deleting it makes the manifest uninstallable and loses data on
    // a lagging follower's replay / snapshot-install. The gated storage-manager cycle blocks
    // this via storage_page_gc_dependency_plan; the operator path must mirror the guard.
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
    // Dump: the manifest captures the slab (id 0) holding v1.
    let manifest = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    assert!(
        !manifest.block_slab_ids.is_empty(),
        "dump manifest should reference the slab holding v1"
    );
    // Roll to a new slab and overwrite: slab 0 becomes stale (not live) but is still named
    // by the manifest.
    engine.block_store().roll_slab().unwrap();
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "gc-key".to_string(),
            value: b"v2".to_vec(),
        },
    });
    let live = engine.live_block_slab_ids(1);
    assert!(
        !manifest.block_slab_ids.iter().any(|s| live.contains(s)),
        "manifest slab must be stale (not live) to exercise the guard; live={live:?}"
    );

    let runtime = DataNodeRuntime::new(
        engine.clone(),
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    // Aggressive operator sweep that would delete the stale manifest slab.
    let submitted = runtime.submit_gc(
        GcRequest {
            shard_id: 1,
            retain_wal_from_sequence: None,
            retain_index_log_from_sequence: None,
            retain_block_slabs_from_id: Some(u64::MAX),
            page_gc_delayed_destroy: false,
            page_gc_invalidate_removed_slabs_only: false,
        },
        RequestController { timeout_ms: 1000 },
    );
    let finished = wait_for_job(&runtime, submitted.job_id);
    let Some(DataNodeTaskOutput::Gc(output)) = finished.output else {
        panic!("expected gc output");
    };
    assert!(output.status.ok, "{:?}", output.status);
    let remaining = engine.block_store().slab_ids().unwrap();
    for slab in &manifest.block_slab_ids {
        assert!(
            remaining.contains(slab),
            "operator /gc deleted slab {slab} still referenced by a dump manifest \
             (remaining={remaining:?})"
        );
    }
}

#[test]
fn runtime_cancels_queued_job_before_worker_executes_it() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 8);

    let submitted = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "queued".to_string(),
                value: b"v".to_vec(),
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    let canceled = runtime.cancel_job(submitted.job_id);
    assert_eq!(canceled.status.code, "job_canceled");
    assert_eq!(runtime.stats().canceled_total, 1);
    assert_eq!(runtime.stats().queue_depth, 0);
}

#[test]
fn runtime_cancels_queued_lifecycle_job_before_execution() {
    let runtime = DataNodeRuntime::new_without_workers_for_test(TemporalEngine::default(), 8);

    let submitted = runtime.submit_load(
        crate::control::LoadShardRequest {
            shard_id: 7,
            table_name: "tbl".to_string(),
            shard_uri: "local://tbl/7".to_string(),
            start_routing_bucket: 10,
            end_routing_bucket: 19,
            readonly: false,
            load_version: 42,
            local_node_id: Some(3),
        },
        RequestController { timeout_ms: 1000 },
    );
    assert!(submitted.status.ok, "{submitted:?}");

    let canceled = runtime.cancel_job(submitted.job_id);
    assert_eq!(canceled.status.code, "job_canceled");
    assert_eq!(canceled.kind, DataNodeTaskKind::Load);
    assert_eq!(runtime.stats().canceled_total, 1);
    assert_eq!(runtime.stats().queue_depth, 0);
    assert_eq!(runtime.lifecycle_report().loaded_shard_count, 0);
}

#[test]
fn runtime_marks_inflight_cancellation_requested_before_worker_finishes() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 8);

    let submitted = runtime.submit_dump(
        DumpShardRequest {
            shard_id: 1,
            selected_routing_buckets: Vec::new(),
        },
        RequestController { timeout_ms: 1000 },
    );
    let task = runtime
        .inner
        .queue
        .lock()
        .expect("runtime queue lock poisoned")
        .pop_ready()
        .expect("task should be marked running");
    assert_eq!(task.job_id, submitted.job_id);

    let cancel_requested = runtime.cancel_job(submitted.job_id);
    assert_eq!(cancel_requested.status.code, "job_cancel_requested");
    assert_eq!(
        runtime.job_status(submitted.job_id).unwrap().status.code,
        "job_cancel_requested"
    );
    assert_eq!(runtime.stats().canceled_total, 0);

    let output = execute_task(&runtime.inner, &task);
    let DataNodeTaskOutput::Dump(response) = output else {
        panic!("expected dump output");
    };
    assert_eq!(response.status.code, "job_canceled");
    assert!(take_canceled(&runtime.inner, submitted.job_id));
}

#[test]
fn runtime_honors_inflight_cancellation_before_dump_side_effects() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "dirty".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert!(response.status.ok);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 8);
    mark_dirty(&runtime.inner.dirty, 1, Some("dirty"));
    let task = QueuedTask {
        job_id: 99,
        kind: DataNodeTaskKind::Dump,
        deadline: Instant::now() + Duration::from_secs(60),
        submitted_at_ms: now_ms(),
        request: TaskRequest::Dump(DumpShardRequest {
            shard_id: 1,
            selected_routing_buckets: Vec::new(),
        }),
    };
    runtime
        .inner
        .canceled
        .lock()
        .expect("runtime cancellation lock poisoned")
        .insert(task.job_id);

    let output = execute_task(&runtime.inner, &task);
    let DataNodeTaskOutput::Dump(response) = output else {
        panic!("expected dump output");
    };
    assert_eq!(response.status.code, "job_canceled");
    assert_eq!(response.shard_id, 1);
    assert_eq!(runtime.dirty_objects().len(), 1);
}

#[test]
fn runtime_honors_inflight_cancellation_before_gc_side_effects() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for key in ["a", "b"] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.to_string(),
                value: key.as_bytes().to_vec(),
            },
        });
        assert!(response.status.ok);
    }
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine.clone(), 8);
    mark_dirty(&runtime.inner.dirty, 1, Some("a"));
    let task = QueuedTask {
        job_id: 100,
        kind: DataNodeTaskKind::Gc,
        deadline: Instant::now() + Duration::from_secs(60),
        submitted_at_ms: now_ms(),
        request: TaskRequest::Gc(GcRequest {
            shard_id: 1,
            retain_wal_from_sequence: Some(2),
            retain_index_log_from_sequence: Some(2),
            retain_block_slabs_from_id: None,
            page_gc_delayed_destroy: false,
            page_gc_invalidate_removed_slabs_only: false,
        }),
    };
    runtime
        .inner
        .canceled
        .lock()
        .expect("runtime cancellation lock poisoned")
        .insert(task.job_id);

    let output = execute_task(&runtime.inner, &task);
    let DataNodeTaskOutput::Gc(response) = output else {
        panic!("expected gc output");
    };
    assert_eq!(response.status.code, "job_canceled");
    assert_eq!(response.shard_id, 1);
    assert_eq!(runtime.dirty_objects().len(), 1);
    assert_eq!(engine.write_ahead_log_store().stats(1).last_sequence, 2);
    assert_eq!(engine.index_log_store().stats(1).last_sequence, 2);
    assert_eq!(
        engine
            .write_ahead_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        engine
            .index_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn runtime_queues_are_shard_affine_and_parallel_across_shards() {
    let mut queues = RuntimeQueues::default();
    queues.push(queued_string_set(1, 1, "a"));
    queues.push(queued_string_set(2, 1, "b"));
    queues.push(queued_string_set(3, 2, "c"));

    let first = queues.pop_ready().expect("shard 1 should be ready");
    assert_eq!(first.job_id, 1);
    assert_eq!(queues.queued_total, 2);
    assert!(queues.running_shards.contains(&1));

    let second = queues
        .pop_ready()
        .expect("shard 2 should run while shard 1 is busy");
    assert_eq!(second.job_id, 3);
    assert_eq!(second.request.shard_id(), 2);
    assert!(queues.pop_ready().is_none());

    queues.finish_shard(1);
    let third = queues
        .pop_ready()
        .expect("next shard 1 task should run after lane release");
    assert_eq!(third.job_id, 2);
    queues.finish_shard(2);
    queues.finish_shard(1);
    assert_eq!(queues.queued_total, 0);
    assert!(queues.by_shard.is_empty());
    assert!(queues.running_shards.is_empty());
}

#[test]
fn runtime_scheduler_prioritizes_foreground_over_background() {
    let mut queues = RuntimeQueues::default();
    queues.push(queued_dump(1, 1));
    queues.push(queued_string_set(2, 1, "foreground"));
    queues.push(queued_dump(3, 2));

    let first = queues.pop_ready().expect("foreground work should be ready");
    assert_eq!(first.job_id, 2);
    assert_eq!(first.request.priority(), TaskPriority::Foreground);
    queues.finish_shard(1);

    let second = queues.pop_ready().expect("background shard should run");
    assert_eq!(second.request.priority(), TaskPriority::Background);
    assert_eq!(queues.background_queued_total, 1);
}

#[test]
fn runtime_rejects_background_work_when_background_queue_is_full() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 8,
            max_background_queue_depth: 1,
        },
    );

    let accepted = runtime.submit_dump(
        DumpShardRequest {
            shard_id: 1,
            selected_routing_buckets: Vec::new(),
        },
        RequestController { timeout_ms: 1000 },
    );
    assert!(accepted.status.ok);
    let rejected = runtime.submit_gc(
        GcRequest {
            shard_id: 1,
            retain_wal_from_sequence: None,
            retain_index_log_from_sequence: None,
            retain_block_slabs_from_id: None,
            page_gc_delayed_destroy: false,
            page_gc_invalidate_removed_slabs_only: false,
        },
        RequestController { timeout_ms: 1000 },
    );
    assert_eq!(rejected.status.code, "background_queue_full");
    assert_eq!(runtime.stats().rejected_background_total, 1);
    assert_eq!(runtime.stats().background_queue_depth, 1);
}

// shared-corpus: storage_matrixraft_dump_load_atomicity storage_matrixraft_cache_refill_pressure
#[test]
/// A stage that reports itself as executed has to have executed something.
///
/// The reap used to push "reap_metrics" onto the executed list and stop there, so a cycle said
/// the stage ran while nothing was gathered and nothing published. The WAL counters are the
/// clearest case: incremented on every append and barrier, and read by nothing outside the
/// module that owns them.
#[test]
fn a_metrics_reap_collects_the_counters_it_says_it_did() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    for key in ["reap-a", "reap-b", "reap-c"] {
        assert!(
            runtime
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

    let report = runtime.run_storage_manager_once(
        1,
        StorageManagerOptions {
            enable_metrics_reap: true,
            ..StorageManagerOptions::default()
        },
    );
    assert!(
        report
            .executed_stages
            .iter()
            .any(|stage| stage == "reap_metrics"),
        "the stage should report itself executed: {:?}",
        report.executed_stages
    );
    let reaped = report
        .metrics_reap
        .expect("the stage ran, so the report carries what it read");
    assert!(
        reaped.wal.last_sequence >= 3,
        "the WAL counters have to be THIS shard's, and three writes went in: {reaped:?}"
    );
    assert_eq!(
        reaped.durability_barriers_total,
        reaped.durability_barriers.values().copied().sum::<u64>(),
        "the total has to be the sum of what was attributed"
    );

    // Disabled: nothing collected, and the cycle says which it was.
    let off = runtime.run_storage_manager_once(
        1,
        StorageManagerOptions {
            enable_metrics_reap: false,
            ..StorageManagerOptions::default()
        },
    );
    assert!(off.metrics_reap.is_none());
    assert!(
        off.skipped_stages
            .iter()
            .any(|stage| stage == "reap_metrics_disabled"),
        "{:?}",
        off.skipped_stages
    );
}

#[test]
fn storage_manager_cycle_runs_as_bounded_background_data_node_task() {
    let dir = tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        256,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 2,
        },
    );
    assert!(
        runtime
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 1,
                table_name: "storage-manager".to_string(),
                shard_uri: "local://storage-manager/1".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 16_383,
                readonly: false,
                load_version: 1,
                local_node_id: Some(1),
            })
            .status
            .ok
    );
    assert!(
        runtime
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "storage-manager-background".to_string(),
                    value: b"value".to_vec(),
                },
            })
            .status
            .ok
    );

    let submitted = runtime.submit_storage_manager_cycle(
        StorageManagerCycleRequest {
            shard_id: 1,
            max_dump_buckets_per_round: 8,
            warm_cache: true,
            ..StorageManagerCycleRequest::default()
        },
        RequestController { timeout_ms: 30_000 },
    );
    assert!(submitted.status.ok, "{submitted:?}");

    let finished = wait_for_job(&runtime, submitted.job_id);
    assert!(finished.status.ok, "{finished:?}");
    let Some(DataNodeTaskOutput::StorageManager(response)) = finished.output else {
        panic!("expected storage manager output: {finished:?}");
    };
    assert!(response.status.ok, "{response:?}");
    assert!(response.report.completed, "{:#?}", response.report);
    assert!(
        response.report.production_parity_slice,
        "{:#?}",
        response.report
    );
    assert_eq!(
        response.report.native_stage_order,
        vec![
            "prepare",
            "reclaim_wal",
            "expire",
            "evict",
            "reclaim_page",
            "index_gc",
            "compact",
            "reap_metrics",
        ]
    );
    assert!(response
        .report
        .stages
        .iter()
        .any(|stage| stage.stage == "reclaim_wal" && stage.dumped_bucket_count >= 1));
    assert_eq!(runtime.stats().storage_manager_runs, 1);
    assert_eq!(runtime.stats().background_queue_depth, 0);
}

// shared-corpus: storage_matrixraft_dump_load_atomicity storage_matrixraft_cache_refill_pressure
#[test]
fn storage_manager_scheduler_submits_deduplicated_background_cycles() {
    let dir = tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        512,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 1,
        },
    );
    assert!(
        runtime
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 1,
                table_name: "storage-manager-scheduler".to_string(),
                shard_uri: "local://storage-manager-scheduler/1".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 16_383,
                readonly: false,
                load_version: 1,
                local_node_id: Some(1),
            })
            .status
            .ok
    );
    runtime.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "storage-manager-scheduler".to_string(),
            value: b"value".to_vec(),
        },
    });

    let scheduler = runtime.start_storage_manager_cycle_scheduler(
        Duration::from_millis(5),
        StorageManagerCycleRequest {
            shard_id: 1,
            max_dump_buckets_per_round: 8,
            warm_cache: true,
            ..StorageManagerCycleRequest::default()
        },
        RequestController { timeout_ms: 30_000 },
    );

    wait_until(Duration::from_secs(5), || {
        runtime.stats().storage_manager_runs >= 1
    });
    scheduler.stop();
    assert!(runtime.stats().storage_manager_runs >= 1);
    assert_eq!(runtime.stats().rejected_background_total, 0);
}

// shared-corpus: storage_manager_continuous_background_runtime
#[test]
fn storage_manager_runtime_collects_the_report_of_a_cycle_that_outlived_its_wait() {
    // A completed cycle's report must reach the runtime report even when the cycle takes longer
    // than the short in-loop wait.
    //
    // It did not. The loop tracked the outstanding cycle in a single slot: the top-of-tick poll
    // saw the cycle still running, the cycle finished later in that same tick, `has_pending` then
    // went false so a new cycle was submitted, and submitting OVERWROTE the slot before anything
    // polled the finished job again. Every cycle was abandoned exactly one tick before it
    // completed. Measured on the unfixed code: 432 collection attempts across 45 cycles, 47 of
    // which finished with a valid report, and not one was ever collected.
    //
    // The visible cost is that `last_completed_cycle` stays None forever, and with it every
    // derived figure -- bytes reclaimed, pressure after, the WAL and index-log floors -- reads
    // zero while the storage manager is doing real work. That is the operator's evidence that
    // reclaim ran, and reclaim is the only thing that recovers the disk growth from repeated
    // dumps.
    let dir = tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        256,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 2,
        },
    );
    assert!(
        runtime
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 9,
                table_name: "storage-manager-collect".to_string(),
                shard_uri: "local://storage-manager-collect/9".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 16_383,
                readonly: false,
                load_version: 1,
                local_node_id: Some(1),
            })
            .status
            .ok
    );
    for index in 0..32 {
        runtime.execute(ExecuteRequest {
            shard_id: 9,
            command: Command::StringSet {
                key: format!("collect-{index}"),
                value: vec![b'v'; 64],
            },
        });
    }

    let manager = runtime.start_storage_manager_runtime(StorageManagerRuntimeOptions {
        // A 5ms interval against cycles that take far longer is the case that broke: the wait
        // budget is tied to the interval, so essentially every cycle outlives it.
        interval_ms: 5,
        jitter_percent: 50,
        initial_backoff_ms: 3,
        max_backoff_ms: 40,
        request: StorageManagerCycleRequest {
            shard_id: 9,
            max_dump_buckets_per_round: 3,
            enable_prepare: true,
            enable_wal_reclaim: true,
            enable_evict: true,
            enable_page_reclaim: true,
            enable_index_gc: true,
            ..StorageManagerCycleRequest::default()
        },
        controller: RequestController { timeout_ms: 30_000 },
    });

    wait_until(Duration::from_secs(15), || {
        manager.report().last_completed_cycle.is_some()
    });
    let report = manager.report();
    manager.stop();

    assert!(
        report.rounds_submitted >= 1,
        "expected at least one submitted cycle, got {report:?}"
    );
    assert!(
        report.last_completed_cycle.is_some(),
        "a finished cycle's report was never collected: {report:?}"
    );
    assert_eq!(
        report.submit_failures, 0,
        "cycles should submit cleanly: {report:?}"
    );
}

#[test]
fn storage_manager_runtime_supports_stop_pause_resume_jitter_backoff_and_phase_flags() {
    let dir = tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        256,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 2,
        },
    );
    assert!(
        runtime
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 8,
                table_name: "storage-manager-runtime".to_string(),
                shard_uri: "local://storage-manager-runtime/8".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 16_383,
                readonly: false,
                load_version: 1,
                local_node_id: Some(1),
            })
            .status
            .ok
    );
    runtime.execute(ExecuteRequest {
        shard_id: 8,
        command: Command::StringSet {
            key: "runtime-live".to_string(),
            value: b"value".to_vec(),
        },
    });
    runtime.execute(ExecuteRequest {
        shard_id: 8,
        command: Command::StringSet {
            key: "runtime-live".to_string(),
            value: b"value-newer".repeat(32),
        },
    });
    runtime.execute(ExecuteRequest {
        shard_id: 8,
        command: Command::StringGet {
            key: "runtime-live".to_string(),
        },
    });
    // `runtime-live` is 352 bytes and this engine's memory tier is 256, so reading it back
    // spills straight to disk and leaves the memory cache empty -- the pressure snapshot then
    // reports cache_memory_bytes = 0 no matter how warm the shard actually is. Touch a value
    // that fits, so the assertion below measures a warm memory tier rather than the tier size.
    runtime.execute(ExecuteRequest {
        shard_id: 8,
        command: Command::StringSet {
            key: "runtime-small".to_string(),
            value: b"s".to_vec(),
        },
    });
    runtime.execute(ExecuteRequest {
        shard_id: 8,
        command: Command::StringGet {
            key: "runtime-small".to_string(),
        },
    });

    let manager = runtime.start_storage_manager_runtime(StorageManagerRuntimeOptions {
        interval_ms: 5,
        jitter_percent: 50,
        initial_backoff_ms: 3,
        max_backoff_ms: 40,
        request: StorageManagerCycleRequest {
            shard_id: 8,
            max_dump_buckets_per_round: 3,
            enable_prepare: true,
            enable_wal_reclaim: true,
            enable_expire: false,
            enable_evict: true,
            enable_page_reclaim: true,
            enable_page_compaction: false,
            enable_index_gc: true,
            warm_cache: true,
            follower_replay_cursors: vec![crate::engine::reports::BucketDumpFollowerReplayCursor {
                follower_id: "follower-lagging-runtime".to_string(),
                shard_id: 8,
                wal_sequence: 1,
                index_log_sequence: 1,
            }],
            raft_snapshot_refs: vec![crate::engine::reports::BucketDumpRaftSnapshotRef {
                snapshot_id: "raft-snapshot-runtime".to_string(),
                shard_id: 8,
                last_included_index: 1,
                last_included_term: 1,
                wal_sequence: 1,
                index_log_sequence: 1,
            }],
            page_gc_raft_install_floor_slab_id: Some(1),
            ..StorageManagerCycleRequest::default()
        },
        controller: RequestController { timeout_ms: 30_000 },
    });

    // Wait for a cycle that saw the state these assertions describe, instead of whichever
    // snapshot happened to be first.
    //
    // Two of the conditions pull against each other. The pressure plan is computed at the TOP of
    // a cycle, so the first cycle sees no dump manifest and a replay cursor has nothing to anchor
    // on -- zero retention blockers is the correct answer there, not a defect. But by the time a
    // manifest exists, the writes made before the runtime started have been dumped and nothing is
    // dirty. A shard with a lagging follower AND outstanding work is a shard still being written
    // to, so keep writing while waiting.
    //
    // The predicate SELECTS a cycle; it does not stand in for the assertions. It looks at two
    // fields, and the block below verifies the whole snapshot.
    let mut churn = 0u64;
    wait_until(Duration::from_secs(15), || {
        churn += 1;
        runtime.execute(ExecuteRequest {
            shard_id: 8,
            command: Command::StringSet {
                key: format!("runtime-churn-{churn}"),
                value: b"c".to_vec(),
            },
        });
        let report = manager.report();
        report.rounds_submitted >= 2
            && runtime.stats().storage_manager_runs >= 2
            && report.last_completed_cycle.is_some()
            && report.last_pressure_snapshot.as_ref().is_some_and(|pressure| {
                pressure.dirty_bucket_count >= 1
                    && pressure.follower_cursor_retention_blockers >= 1
            })
    });
    let running = manager.report();
    assert!(running.running);
    assert!(!running.paused);
    assert!(!running.stopped);
    assert_eq!(running.interval_ms, 5);
    assert_eq!(running.jitter_percent, 50);
    assert!(running.last_delay_ms >= 5);
    assert!(running.last_delay_ms <= 7);
    assert_eq!(running.bounded_max_dump_buckets_per_round, 3);
    assert!(running.phase_prepare_enabled);
    assert!(running.phase_wal_reclaim_enabled);
    assert!(!running.phase_expire_enabled);
    assert!(running.phase_evict_enabled);
    assert!(running.phase_page_gc_enabled);
    assert!(!running.phase_compaction_enabled);
    assert!(running.phase_index_gc_enabled);
    assert_eq!(running.configured_follower_cursor_count, 1);
    assert_eq!(running.configured_raft_snapshot_ref_count, 1);
    assert_eq!(
        running.configured_page_gc_raft_install_floor_slab_id,
        Some(1)
    );
    assert!(running.last_job_id.is_some());
    assert!(running.last_status.as_ref().is_some_and(|status| status.ok));
    assert!(running.last_completed_cycle.is_some());
    assert!(running.last_pressure_snapshot.is_some());
    let pressure = running.last_pressure_snapshot.as_ref().unwrap();
    assert!(pressure.dirty_bucket_count >= 1, "{pressure:?}");
    assert!(pressure.undumped_wal_records >= 1, "{pressure:?}");
    assert!(pressure.wal_bytes >= 1, "{pressure:?}");
    assert!(pressure.index_log_bytes >= 1, "{pressure:?}");
    assert!(pressure.cache_memory_bytes >= 1, "{pressure:?}");
    assert!(pressure.memory_cache_pressure_score >= pressure.cache_memory_bytes);
    assert!(pressure.follower_cursor_retention_blockers >= 1, "{pressure:?}");
    assert!(pressure.raft_snapshot_retention_blockers >= 1, "{pressure:?}");
    assert!(pressure.total_pressure_score >= pressure.wal_bytes);
    assert!(running.last_pressure_before >= running.last_pressure_after);
    assert!(!running.last_selected_buckets.is_empty());
    assert!(running.last_bytes_reclaimed >= pressure.cache_disk_bytes);
    // This scenario configures a follower pinned at sequence 1 and a raft snapshot at sequence 1.
    // What a retention cursor imposes is a BOUND on how far reclaim may go, not a refusal; the
    // note below the binding says why, and what is asserted instead.
    let reclaim_wal = running
        .last_phase_reports
        .iter()
        .find(|stage| stage.stage == "reclaim_wal")
        .expect("the cycle ran a reclaim_wal stage");
    // The refusal this asserted was deliberately replaced by a CLAMP. A cursor at sequence 1
    // means everything at or below 1 is behind every reader, so reclaim may take exactly that
    // span and no more; refusing outright let one lagging follower pin the whole log for as long
    // as it lagged, and the log grew without bound underneath it (storage_lifecycle_methods.rs).
    // So the property worth asserting is the BOUND, not a refusal message -- and the bound is the
    // one a retention cursor exists to impose.
    //
    // Both cursors sit at sequence 1, and the wait above already required a retention blocker,
    // which needs 1 < durable_frontier. The floor over the cursors is therefore 1 and the plan
    // must retain from exactly 2. The exact value is the point: >= would also pass if the clamp
    // were dropped and reclaim ran all the way to the durable frontier, which is the regression
    // being guarded here.
    assert!(!reclaim_wal.skipped, "{reclaim_wal:?}");
    assert_eq!(
        reclaim_wal.retain_from_wal_sequence, 2,
        "reclaim must not advance past the slowest cursor: {reclaim_wal:?}"
    );
    assert_eq!(
        reclaim_wal.retain_from_index_log_sequence, 2,
        "reclaim must not advance past the slowest cursor: {reclaim_wal:?}"
    );
    // Clamping instead of refusing did not stop the cursors being counted, nor the pressure
    // signal naming what is holding the logs.
    assert_eq!(reclaim_wal.retention_blockers, 2, "{reclaim_wal:?}");
    assert!(
        reclaim_wal
            .pressure_signal
            .contains("follower_snapshot_retention"),
        "{reclaim_wal:?}"
    );
    assert_eq!(
        running.last_wal_floor_sequence, reclaim_wal.wal_floor_sequence,
        "the manager must report the floor the stage computed: {:?}",
        running.last_skipped_reasons
    );
    assert_eq!(
        running.last_index_log_floor_sequence,
        reclaim_wal.index_log_floor_sequence
    );
    assert!(running.last_retention_blockers >= 1);
    assert!(running
        .last_phase_blockers
        .iter()
        .any(|blocker| blocker.contains("retention_blockers")));
    assert!(running
        .last_phase_reports
        .iter()
        .any(|stage| stage.stage == "prepare"
            && stage.pressure_signal.contains("dirty_slots")
            && stage.pressure_before >= stage.pressure_after));
    // The floor is the CLAMPED frontier, not zero: reclaim ran, and stopped at the slowest cursor.
    // The refusal was asserted TWICE in this test, in two blocks about sixty lines apart, so
    // correcting the first one only moved the failure down here. Both now say the same thing.
    assert!(reclaim_wal.retention_blockers >= 1, "{reclaim_wal:?}");
    assert_eq!(reclaim_wal.wal_floor_sequence, 2, "{reclaim_wal:?}");
    assert_eq!(reclaim_wal.index_log_floor_sequence, 2, "{reclaim_wal:?}");
    assert!(running.last_phase_reports.iter().any(|stage| {
        stage.stage == "reclaim_page"
            && (stage
                .skipped_reason
                .contains("retained dependencies remain")
                || stage.retention_blockers >= 1)
    }));
    assert!(running
        .last_phase_reports
        .iter()
        .any(|stage| stage.pressure_signal.contains("stale_density")
            || stage.pressure_signal.contains("cache_pressure")));
    assert!(
        running
            .last_phase_reports
            .iter()
            .any(|stage| stage.stage == "reclaim_wal"
                && stage.pressure_signal.contains("wal_bytes"))
    );
    assert!(!running.last_phase_reports.is_empty());

    manager.pause();
    let paused_before = manager.report().rounds_submitted;
    // Wait for a skipped round instead of sleeping 25ms and hoping one fits inside it. A round is
    // a delay PLUS a cycle, and a cycle on a busy machine outlasts that sleep -- so the assertion
    // measured how loaded the box was rather than whether pausing skips rounds. The properties
    // being checked are unchanged: while paused, rounds are skipped and none are submitted.
    wait_until(Duration::from_secs(5), || {
        manager.report().rounds_skipped_paused >= 1
    });
    let paused = manager.report();
    assert!(paused.paused);
    assert_eq!(paused.rounds_submitted, paused_before);
    assert!(paused.rounds_skipped_paused >= 1);

    manager.resume();
    runtime.execute(ExecuteRequest {
        shard_id: 8,
        command: Command::StringSet {
            key: "runtime-live-2".to_string(),
            value: b"value-2".to_vec(),
        },
    });
    wait_until(Duration::from_secs(5), || {
        manager.report().rounds_submitted > paused_before
    });
    assert!(!manager.report().paused);

    let stopped = manager.stop();
    assert!(!stopped.running);
    assert!(stopped.stopped);
    assert!(stopped.rounds_submitted >= 2);
    assert_eq!(stopped.submit_failures, 0);
}

// shared-corpus: storage_manager_continuous_background_runtime
#[test]
fn storage_manager_runtime_jitter_and_backoff_are_bounded() {
    let options = StorageManagerRuntimeOptions {
        interval_ms: 10,
        jitter_percent: 50,
        initial_backoff_ms: 4,
        max_backoff_ms: 16,
        request: StorageManagerCycleRequest {
            shard_id: 404,
            max_dump_buckets_per_round: 1,
            ..StorageManagerCycleRequest::default()
        },
        controller: RequestController { timeout_ms: 10 },
    };

    let first_delay = storage_manager_runtime_delay_ms(&options, 1, 4);
    assert!((10..=15).contains(&first_delay));
    assert_eq!(storage_manager_runtime_next_backoff_ms(0, 4, 16), 4);
    assert_eq!(storage_manager_runtime_next_backoff_ms(4, 4, 16), 8);
    assert_eq!(storage_manager_runtime_next_backoff_ms(8, 4, 16), 16);
    assert_eq!(storage_manager_runtime_next_backoff_ms(16, 4, 16), 16);

    let report = storage_manager_runtime_initial_report(&options);
    assert!(report.running);
    assert_eq!(report.current_backoff_ms, 4);
    assert_eq!(report.bounded_max_dump_buckets_per_round, 1);
    assert!(report.phase_prepare_enabled);
    assert!(report.phase_wal_reclaim_enabled);
    assert!(report.phase_expire_enabled);
    assert!(report.phase_evict_enabled);
    assert!(report.phase_page_gc_enabled);
    assert!(report.phase_compaction_enabled);
    assert!(report.phase_index_gc_enabled);
}

fn queued_string_set(job_id: u64, shard_id: ShardId, key: &str) -> QueuedTask {
    QueuedTask {
        job_id,
        kind: DataNodeTaskKind::Execute,
        deadline: Instant::now() + Duration::from_secs(60),
        submitted_at_ms: now_ms(),
        request: TaskRequest::Execute(ExecuteRequest {
            shard_id,
            command: Command::StringSet {
                key: key.to_string(),
                value: b"v".to_vec(),
            },
        }),
    }
}

fn queued_dump(job_id: u64, shard_id: ShardId) -> QueuedTask {
    QueuedTask {
        job_id,
        kind: DataNodeTaskKind::Dump,
        deadline: Instant::now() + Duration::from_secs(60),
        submitted_at_ms: now_ms(),
        request: TaskRequest::Dump(DumpShardRequest {
            shard_id,
            selected_routing_buckets: Vec::new(),
        }),
    }
}

fn wait_for_job(runtime: &DataNodeRuntime, job_id: u64) -> DataNodeTaskStatus {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Some(status) = runtime.job_status(job_id) {
            if status.finished_at_ms.is_some() {
                return status;
            }
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("job {job_id} did not finish");
}

fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        predicate(),
        "condition did not become true within {timeout:?}"
    );
}

#[test]
fn the_slot_index_metric_counts_resident_buckets_not_the_routing_range() {
    // `slot_index_entry_count` was built from `storage.bucket_entries`, which is the routing
    // RANGE. On a default shard that is u32::MAX, and `max(u32::MAX, anything)` is u32::MAX, so
    // the metric exported 4,294,967,295 per shard whatever the index held -- and these are SUMMED
    // across shards. Nothing asserted it, which is why it stayed that way.
    let shard = crate::meta::ServerShardServingState {
        shard_id: 1,
        dirty_bucket_count: 3,
        storage: crate::control::ShardCanonicalStorageStats {
            // the routing range a default shard reports
            bucket_entries: u32::MAX as u64,
            bucket_index_resident_entries: 42,
            ..crate::control::ShardCanonicalStorageStats::default()
        },
        ..crate::meta::ServerShardServingState::default()
    };

    let mut metrics = std::collections::BTreeMap::new();
    super::super::apply_shard_storage_metrics(&mut metrics, std::slice::from_ref(&shard));

    assert_eq!(
        metrics.get("slot_index_entry_count").copied(),
        Some(42),
        "the metric must report resident buckets"
    );
    // The control: the field it used to read is still u32::MAX right here, so a regression to it
    // cannot pass by the numbers happening to agree.
    assert_eq!(shard.storage.bucket_entries, u32::MAX as u64);
}

/// What each maintenance stage costs on a realistic shard, by ablation.
///
///   cargo test --release -p temporalstore-rust --lib what_each_maintenance_stage_costs -- --ignored --nocapture --test-threads=1
///
/// The cycle is timed with everything on, then once per stage with that stage off. The difference
/// is that stage's cost. Ablation rather than instrumentation, so no stage has to be edited to be
/// measured, and a stage that declines for want of pressure shows up as ~0 rather than as absent.
///
/// Compaction was already measured directly at seconds per round
/// (`what_a_whole_shard_compaction_costs`); this is the rest of them, which had no number at all.
#[test]
#[ignore]
fn what_each_maintenance_stage_costs() {
    fn build(objects: usize) -> (tempfile::TempDir, DataNodeRuntime) {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            16 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..objects {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("stage-cost-{index:06}"),
                    value: vec![b'v'; 96],
                },
            });
        }
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        (dir, runtime)
    }

    fn time_once(objects: usize, options: StorageManagerOptions) -> f64 {
        let (_dir, runtime) = build(objects);
        let started = std::time::Instant::now();
        let report = runtime.run_storage_manager_once(1, options);
        let elapsed = started.elapsed().as_secs_f64() * 1000.0;
        let _ = report;
        elapsed
    }

    for objects in [5_000usize, 20_000] {
        let all_on = time_once(objects, StorageManagerOptions::default());
        eprintln!("  [stages] {objects:>6} objects, everything on -> {all_on:>9.1} ms");

        for (name, off) in [
            ("prepare", StorageManagerOptions { enable_prepare: false, ..Default::default() }),
            ("reclaim_wal", StorageManagerOptions { enable_wal_reclaim: false, ..Default::default() }),
            ("reclaim_memory", StorageManagerOptions { enable_memory_reclaim: false, ..Default::default() }),
            ("expire", StorageManagerOptions { enable_expire: false, ..Default::default() }),
            ("reclaim_page", StorageManagerOptions { enable_page_gc: false, ..Default::default() }),
            ("compact_pages", StorageManagerOptions { enable_page_compaction: false, ..Default::default() }),
            ("reclaim_index", StorageManagerOptions { enable_index_gc: false, ..Default::default() }),
            ("reap_metrics", StorageManagerOptions { enable_metrics_reap: false, ..Default::default() }),
        ] {
            let without = time_once(objects, off);
            eprintln!(
                "  [stages] {objects:>6}   without {name:<15} {without:>9.1} ms   (stage costs {:>9.1} ms)",
                all_on - without,
            );
        }
    }
}

#[test]
fn the_periodic_loop_can_evict_when_the_operator_asks() {
    // Until this stage existed the periodic loop -- the one the server actually starts -- relieved
    // memory by invalidating cached pages and nothing else. `apply_storage_eviction`, with
    // dump-before-evict, delete-drop, a batch limit and a pressure threshold, was reachable ONLY
    // from the on-demand cycle, so eviction could not happen on the running loop at ANY setting.
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for index in 0..64 {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("evict-{index:04}"),
                value: vec![b'v'; 128],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );

    // CONTROL: the shipped default must not evict. This is the half that says the change is
    // opt-in rather than a silent change to how every deployment relieves memory.
    let default_report = runtime.run_storage_manager_once(1, StorageManagerOptions::default());
    assert!(
        default_report.skipped_stages.iter().any(|stage| stage == "evict_disabled"),
        "the default must skip eviction, got skipped={:?} executed={:?}",
        default_report.skipped_stages,
        default_report.executed_stages
    );
    assert_eq!(
        runtime.stats().storage_manager_evict_runs,
        0,
        "the default must not have evicted"
    );

    // And with the operator asking, the stage the loop could never reach now runs.
    let enabled_report = runtime.run_storage_manager_once(
        1,
        StorageManagerOptions {
            enable_evict: true,
            eviction_dump_before_evict: true,
            ..StorageManagerOptions::default()
        },
    );
    assert!(
        enabled_report.executed_stages.iter().any(|stage| stage == "evict"),
        "enabling eviction must reach the stage, got executed={:?} skipped={:?}",
        enabled_report.executed_stages,
        enabled_report.skipped_stages
    );
    assert_eq!(
        runtime.stats().storage_manager_evict_runs,
        1,
        "the stage must be counted exactly once"
    );
}

#[test]
fn the_periodic_expire_stage_takes_a_bounded_window_each_round() {
    // The periodic loop passed `ShardExpirySweepRequest::default()`, whose limits are 0 -- and the
    // window code says plainly that "zero limits mean no limit". So every tick walked the WHOLE
    // deadline map, hot and cold, for every loaded shard, while the on-demand cycle passed 128 and
    // carried a cursor. Measured by ablation, that stage was 201.5 ms at 20k objects.
    //
    // This test was originally built around a CURSOR, and that mechanism is gone.
    //
    // The sweep used to walk the deadlines in KEY order, so a window from the start landed on
    // whatever sorted first -- live or not -- and only a resuming cursor let later rounds reach
    // the expired tail. The fixture below still carries a live prefix sorted ahead of the expired
    // keys because of that.
    //
    // Since the deadlines gained a deadline-ordered view, the due keys ARE the front: the live
    // prefix is never examined, the walk stops at the first deadline in the future, and progress
    // needs no cursor because removing a due key shrinks the prefix. The live prefix is kept as a
    // control -- it must still be there afterwards, untouched -- rather than as the obstacle it
    // used to be.
    //
    // What remains worth asserting is the BOUND: one round must take at most its window, and
    // successive rounds must finish the job.
    const LIVE_PREFIX: usize = 20;
    const EXPIRED: usize = 20;
    const WINDOW: usize = 4;

    fn runtime_with_a_live_prefix() -> DataNodeRuntime {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        // Sorted before the expired ones, so they are what a window from the start lands on.
        for index in 0..LIVE_PREFIX {
            let key = format!("aaa-live-{index:04}");
            assert!(engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet { key: key.clone(), value: vec![b'v'; 32] },
                })
                .status
                .ok);
            assert!(engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::CommonExpire { key, ttl_ms: 10 * 60 * 1000 },
                })
                .status
                .ok);
        }
        for index in 0..EXPIRED {
            let key = format!("zzz-dead-{index:04}");
            assert!(engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet { key: key.clone(), value: vec![b'v'; 32] },
                })
                .status
                .ok);
            assert!(engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::CommonExpire { key, ttl_ms: 1 },
                })
                .status
                .ok);
        }
        std::thread::sleep(std::time::Duration::from_millis(30));
        DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        )
    }

    // CONTROL: the shipped default clears this fixture in one round -- because its bound is
    // LARGER than the fixture, not because it is unbounded.
    //
    // This wording is deliberate. When this test was written the default WAS 0, meaning no bound,
    // and the assertion below read as "the default is unbounded". It is not any more: the default
    // is now the cycle's 128 hot / 8 cold. The assertion still passes, but for a different reason
    // than it used to, and a control that passes for a reason it does not state is worth nothing.
    //
    // What it pins now is that the shipped bound comfortably exceeds a small store, so ordinary
    // expiry is not slowed by it -- which is the property that actually matters to a deployment,
    // and which a too-small default would break loudly here.
    let default_bound = StorageManagerOptions::default().max_expire_hot_buckets_per_round;
    assert!(
        default_bound > EXPIRED,
        "this control only means something while the default bound ({default_bound}) exceeds \
         the fixture ({EXPIRED}); raise the fixture or rewrite the control"
    );
    let unbounded = runtime_with_a_live_prefix();
    let cleared = unbounded.run_storage_manager_once(1, StorageManagerOptions::default());
    assert!(
        cleared.executed_stages.iter().any(|stage| stage == "expire"),
        "expire must run: {:?}",
        cleared.executed_stages
    );
    assert_eq!(
        unbounded.stats().expired_records_removed,
        EXPIRED as u64,
        "the shipped default must clear a store smaller than its own per-round bound"
    );

    // Bounded, ONE round: the window is what limits it.
    //
    // The old form of this assertion ran many rounds and required that they had NOT finished,
    // which only held because each round wasted its window on live keys it could not remove.
    // Now a round spends its whole window on due keys, so "not finished yet" is no longer a
    // statement about the bound -- the bound is what ONE round takes.
    let bounded = runtime_with_a_live_prefix();
    let options = StorageManagerOptions {
        max_expire_hot_buckets_per_round: WINDOW,
        max_expire_cold_buckets_per_round: WINDOW,
        ..StorageManagerOptions::default()
    };
    bounded.run_storage_manager_once(1, options.clone());
    let after_one = bounded.stats().expired_records_removed;
    assert!(
        after_one > 0,
        "a bounded round removed nothing, so the window is not reaching the expired keys at all"
    );
    assert!(
        after_one <= WINDOW as u64,
        "one bounded round removed {after_one} with a window of {WINDOW} -- the bound is not \
         binding"
    );

    // And successive rounds finish the job, so the bound paces the work without stalling it.
    let rounds = EXPIRED / WINDOW + 3;
    for _ in 0..rounds {
        bounded.run_storage_manager_once(1, options.clone());
    }
    let removed = bounded.stats().expired_records_removed;
    assert_eq!(
        removed, EXPIRED as u64,
        "after {rounds} further bounded rounds the sweep should have cleared all {EXPIRED} \
         expired keys, but removed {removed}"
    );

    // That equality is also the control on the live prefix: those {LIVE_PREFIX} keys carry no
    // deadline at all, so removing one would be counted here and push the total ABOVE
    // {EXPIRED}. Exactly {EXPIRED} means the sweep took the due keys and nothing else.
}

/// Does the PERIODIC loop -- the one `bin/server.rs` starts -- actually reclaim the logs?
///
///   cargo test -p temporalstore-rust --lib what_the_periodic_loop_reclaims -- --ignored --nocapture
///
/// `bin/server.rs` says the scheduler runs "Dump, WAL and index-log reclaim ... the same phases
/// the cycle endpoint runs", and its banner prints `phases=prepare,reclaim,...`. Reading the call
/// graph says otherwise: `gc_before_sequence` is the only thing that truncates either log, and
/// nothing the periodic loop calls reaches it. This measures rather than argues, and carries the
/// on-demand cycle as a POSITIVE CONTROL so "nothing moved" cannot be mistaken for "the fixture
/// had nothing to reclaim".
#[test]
#[ignore]
fn what_the_periodic_loop_reclaims() {
    fn first_record_offset(engine: &TemporalEngine, shard_id: crate::types::ShardId) -> u64 {
        engine
            .write_ahead_log_store()
            .scan(shard_id, 0, u64::MAX, u64::MAX)
            .expect("scan")
            .first()
            .map(|(offset, _)| *offset)
            .unwrap_or(0)
    }
    fn record_count(engine: &TemporalEngine, shard_id: crate::types::ShardId) -> usize {
        engine
            .write_ahead_log_store()
            .scan(shard_id, 0, u64::MAX, u64::MAX)
            .expect("scan")
            .len()
    }

    fn build(records: usize) -> TemporalEngine {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for index in 0..records {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("reclaim-{index:06}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        engine
    }

    let records = StorageManagerOptions::default().min_undumped_wal_records as usize + 512;

    // ARM A: the periodic loop, run enough times that no threshold can be the explanation.
    let periodic_engine = build(records);
    let before_low = first_record_offset(&periodic_engine, 1);
    let before_count = record_count(&periodic_engine, 1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        periodic_engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    for _ in 0..6 {
        runtime.run_storage_manager_once(1, StorageManagerOptions::default());
    }
    let periodic_engine = runtime.engine();
    let after_low = first_record_offset(&periodic_engine, 1);
    let after_count = record_count(&periodic_engine, 1);

    // ARM B, the POSITIVE CONTROL: the same fixture through the on-demand cycle.
    let cycle_engine = build(records);
    let control_before = record_count(&cycle_engine, 1);
    for _ in 0..6 {
        cycle_engine.run_storage_manager_cycle(crate::engine::reports::StorageManagerCycleRequest {
            shard_id: 1,
            ..crate::engine::reports::StorageManagerCycleRequest::default()
        });
    }
    let control_after = record_count(&cycle_engine, 1);
    let control_low = first_record_offset(&cycle_engine, 1);

    eprintln!("  [periodic]  {before_count:>6} records (first offset {before_low}) -> {after_count:>6} (first offset {after_low})");
    eprintln!("  [cycle   ]  {control_before:>6} records -> {control_after:>6} (first offset {control_low})");
    eprintln!(
        "  periodic reclaimed {} records; the cycle reclaimed {}",
        before_count.saturating_sub(after_count),
        control_before.saturating_sub(control_after)
    );
}

#[test]
fn the_periodic_loop_actually_reclaims_the_write_ahead_log() {
    // The stage was named `reclaim_wal` and reclaimed nothing. `gc_before_sequence` is the only
    // thing that truncates the log, and its production callers were the cycle endpoint, an
    // explicit /gc request, and the embedded proxy's own thread -- none of them this loop. So a
    // server started as shipped grew its log for ever unless something outside asked.
    //
    // Measured before the fix, six rounds on 1,512 records: 0 reclaimed. Six rounds of the
    // on-demand cycle on the same fixture: 1,511.
    fn record_count(engine: &TemporalEngine) -> usize {
        engine
            .write_ahead_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .expect("scan")
            .len()
    }
    fn build(records: usize) -> TemporalEngine {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for index in 0..records {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("reclaim-{index:06}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        engine
    }
    fn run(engine: TemporalEngine, options: StorageManagerOptions) -> TemporalEngine {
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        for _ in 0..4 {
            runtime.run_storage_manager_once(1, options.clone());
        }
        runtime.engine()
    }

    let records = StorageManagerOptions::default().min_undumped_wal_records as usize + 256;

    let engine = build(records);
    let before = record_count(&engine);
    let engine = run(engine, StorageManagerOptions::default());
    let after = record_count(&engine);
    assert!(
        after < before,
        "the periodic loop must reclaim the log it dumps: {before} records before, {after} after"
    );

    // CONTROL: the same rounds with the stage switched off must reclaim NOTHING. Without this,
    // the assertion above would also pass on any unrelated path that happened to shrink the log,
    // and it is the stage under test that has to be responsible.
    let control = build(records);
    let control_before = record_count(&control);
    let control = run(
        control,
        StorageManagerOptions { enable_wal_reclaim: false, ..StorageManagerOptions::default() },
    );
    let control_after = record_count(&control);
    assert_eq!(
        control_after, control_before,
        "with reclaim_wal disabled nothing may shrink the log: {control_before} -> {control_after}"
    );
}

#[test]
fn the_periodic_loop_actually_reclaims_the_index_log() {
    // #1470 gave this loop its write-ahead log reclaim and left the index log open, because
    // `storage_index_gc_report` is engine-internal and takes a cycle request for its thresholds.
    // Until this, the index log was truncated only by the cycle endpoint, an explicit /gc request,
    // or the embedded proxy's own thread -- so a server started as shipped grew it for ever, the
    // same way it grew the write-ahead log.
    //
    // The fixture is large on purpose. The shipped gate needs BOTH triggers, ANDed: at least
    // `DEFAULT_INDEX_GC_INDEX_LOG_BYTES_THRESHOLD` (768 KiB) of index log AND at least 40%
    // removable. Measured, an index-log record is about 59 B, so 8,000 records is 477 KB and the
    // byte gate declines -- correctly. 16,000 reaches 943 KB and it fires. Testing at a tuned-down
    // threshold would exercise a gate no deployment runs.
    fn index_log_records(engine: &TemporalEngine, shard_id: crate::types::ShardId) -> usize {
        engine
            .index_log_store()
            .scan(shard_id, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or(0)
    }

    fn runtime_with_records(records: usize) -> (DataNodeRuntime, usize) {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for index in 0..records {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("index-gc-{index:06}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        let before = index_log_records(&engine, 1);
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        (runtime, before)
    }

    const RECORDS: usize = 16_000;

    let (runtime, before) = runtime_with_records(RECORDS);
    assert!(before > 0, "the fixture must write index-log records, got {before}");
    for _ in 0..3 {
        runtime.run_storage_manager_once(1, StorageManagerOptions::default());
    }
    let runtime_engine = runtime.engine();
    let after = index_log_records(&runtime_engine, 1);
    assert!(
        after < before,
        "the periodic loop must reclaim index-log records: {before} before, {after} after"
    );

    // CONTROL: the same rounds with the stage switched off must reclaim NOTHING. Without it,
    // "the log shrank" could be the dump, the prune or the roll-forward, and the assertion above
    // would pass while proving nothing about index GC.
    let (control, control_before) = runtime_with_records(RECORDS);
    for _ in 0..3 {
        control.run_storage_manager_once(
            1,
            StorageManagerOptions {
                enable_index_gc: false,
                ..StorageManagerOptions::default()
            },
        );
    }
    let control_engine = control.engine();
    let control_after = index_log_records(&control_engine, 1);
    assert_eq!(
        control_after, control_before,
        "with index GC off nothing may truncate the index log: {control_before} before, \
         {control_after} after -- if this moved, the stage under test is not the one responsible"
    );
}


/// Why does the index-log reclaim decline? Prints the gate.
///
///   cargo test -p temporalstore-rust --lib what_the_index_gc_gate_says -- --ignored --nocapture
#[test]
#[ignore]
fn what_the_index_gc_gate_says() {
    for records in [8_000usize, 16_000, 32_000] {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for index in 0..records {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("gate-{index:06}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        // A round first, so the dump has run and the plan reflects it.
        runtime.run_storage_manager_once(1, StorageManagerOptions::default());
        let engine = runtime.engine();
        let wal_plan = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());
        eprintln!(
            "  {records:>6} records -> wal_safe={} blockers={:?}",
            wal_plan.safe_to_reclaim, wal_plan.blocker_reasons
        );
        let report = engine.apply_periodic_index_gc(
            crate::engine::reports::StorageLifecycleRequest {
                shard_id: 1,
                purge_delayed_destroy: true,
                prune_bucket_dump_manifests: true,
                roll_forward_bucket_dump_installs: true,
                ..crate::engine::reports::StorageLifecycleRequest::default()
            },
            None,
            crate::engine::reports::DEFAULT_INDEX_GC_MAX_ENTRIES_PER_ROUND,
        );
        eprintln!(
            "  {records:>6} records -> enabled={} applied={} safe_dirty={} bytes_before={} \
             threshold={} usage={}bp trigger={}bp retain_from={} before={} after={}",
            report.enabled,
            report.applied,
            report.dirty_buckets_committed_before_truncate,
            report.bytes_before,
            report.bytes_threshold,
            report.usage_ratio_basis_points,
            report.usage_ratio_trigger_basis_points,
            report.retain_from_index_log_sequence,
            report.records_before,
            report.records_after,
        );
    }
}

/// What does the index-GC GATE cost when it cannot possibly fire? Prints.
///
///   cargo test --release -p temporalstore-rust --lib what_the_index_gc_gate_costs -- --ignored --nocapture
///
/// The gate needs BOTH triggers, ANDed: at least 768 KiB of index log AND at least 40% removable.
/// It establishes the second by scanning the WHOLE log and decoding every record -- and only then
/// checks the first, which is a file length. So every round below the byte threshold pays a full
/// scan to learn it was never eligible.
#[test]
#[ignore]
fn what_the_index_gc_gate_costs() {
    for records in [4_000usize, 8_000, 16_000] {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for index in 0..records {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("gate-cost-{index:06}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        runtime.run_storage_manager_once(1, StorageManagerOptions::default());
        let engine = runtime.engine();
        let log_bytes = engine.index_log_store().log_len_bytes(1);

        let lifecycle_request = || crate::engine::reports::StorageLifecycleRequest {
            shard_id: 1,
            purge_delayed_destroy: true,
            prune_bucket_dump_manifests: true,
            roll_forward_bucket_dump_installs: true,
            ..crate::engine::reports::StorageLifecycleRequest::default()
        };

        // Attribute the three pieces separately. Timing only the enclosing call cannot say which
        // of them costs, and the first guess -- the index-log scan -- turned out to be wrong.
        let started = std::time::Instant::now();
        for _ in 0..5 {
            let _ = engine.storage_lifecycle_plan(lifecycle_request());
        }
        let plan_ms = started.elapsed().as_micros() as f64 / 5.0 / 1000.0;

        let started = std::time::Instant::now();
        for _ in 0..5 {
            let _ = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());
        }
        let wal_plan_ms = started.elapsed().as_micros() as f64 / 5.0 / 1000.0;

        let started = std::time::Instant::now();
        let mut applied_any = false;
        for _ in 0..5 {
            let report = engine.apply_periodic_index_gc(lifecycle_request(), None, crate::engine::reports::DEFAULT_INDEX_GC_MAX_ENTRIES_PER_ROUND);
            applied_any |= report.applied;
        }
        let per_call = started.elapsed().as_micros() as f64 / 5.0 / 1000.0;

        eprintln!(
            "  [gate] {records:>6} records, log {log_bytes:>8} B -> whole {per_call:>7.1} ms = \
             plan {plan_ms:>7.1} + wal_plan {wal_plan_ms:>6.1} + rest \
             {:>6.1}, applied={applied_any}",
            per_call - plan_ms - wal_plan_ms
        );
    }
}

#[test]
fn the_dump_cap_bounds_a_stage_but_not_a_round() {
    // Two facts, and the second is the surprising one.
    //
    // `reclaim_wal` honours `max_dump_buckets_per_round` (64). `reclaim_index` does NOT, and that
    // is deliberate: `wal_plan.safe_to_reclaim` needs a durable manifest for every live
    // generation, and its whole-dirty-set dump is what produces one. Capping it was tried and
    // stopped index-log reclaim dead -- 16,000 records before a round, 16,000 after.
    //
    // So the option bounds a STAGE, not a round, and a default round still dumps everything. If
    // you came here because you capped that stage and this test failed, that is the reason, and
    // `the_periodic_loop_actually_reclaims_the_index_log` is what breaks next.
    let cap = StorageManagerOptions::default().max_dump_buckets_per_round;
    assert!(cap > 0, "this test is about a non-zero cap");
    let keys = cap * 20;

    fn dirty_after_one_round(keys: usize, options: StorageManagerOptions) -> (usize, usize) {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for index in 0..keys {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("cap-{index:06}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        let first = runtime.run_storage_manager_once(1, options.clone());
        let second = runtime.run_storage_manager_once(1, options);
        (
            first.pressure.dirty_bucket_count,
            second.pressure.dirty_bucket_count,
        )
    }

    // The stage that honours the cap dumps exactly the cap.
    let (before, after) = dirty_after_one_round(
        keys,
        StorageManagerOptions {
            enable_memory_reclaim: false,
            enable_index_gc: false,
            ..StorageManagerOptions::default()
        },
    );
    assert_eq!(
        before.saturating_sub(after),
        cap,
        "reclaim_wal alone must dump exactly its cap: {before} -> {after}"
    );

    // A whole round does not, because reclaim_index dumps the rest on purpose.
    let (before, after) = dirty_after_one_round(keys, StorageManagerOptions::default());
    assert_eq!(
        after, 0,
        "a default round dumps the whole dirty set, because index GC needs the coverage: \
         {before} -> {after}"
    );
}

/// How many O(shard) plans does ONE round build? Prints.
///
///   cargo test -p temporalstore-rust --lib what_one_round_rebuilds -- --ignored --nocapture
///
/// #1496 attributed the index-GC stage: 95% of its 365 ms at 16k records is rebuilding a
/// lifecycle plan and a WAL reclaim plan, not the index-log work it exists to do. This asks the
/// wider question -- how many such plans a whole round builds -- because the answer decides
/// whether reuse is worth the correctness risk.
///
/// COUNTED, not timed. A count does not move when another build is running on the box, and the
/// question is how much duplicated work happens, which is a count. #1496's timings were taken on
/// a quiet box; these do not need one.
#[test]
#[ignore]
fn what_one_round_rebuilds() {
    for keys in [2_000usize, 8_000] {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for index in 0..keys {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("plan-{index:06}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );

        crate::engine::reset_storage_plan_build_counts();
        runtime.run_storage_manager_once(1, StorageManagerOptions::default());
        let (lifecycle, wal) = crate::engine::storage_plan_build_counts();
        eprintln!(
            "  {keys:>6} keys -> one round built {lifecycle} lifecycle plans and {wal} wal \
             reclaim plans"
        );

        // And the same for the stage #1496 measured, on its own.
        crate::engine::reset_storage_plan_build_counts();
        let engine = runtime.engine();
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
        let (lifecycle, wal) = crate::engine::storage_plan_build_counts();
        eprintln!(
            "  {keys:>6} keys -> apply_periodic_index_gc alone built {lifecycle} lifecycle and \
             {wal} wal reclaim plans"
        );
    }
}

/// WHICH periodic stage truncates the logs, and what gates it? Prints.
///
///   cargo test -p temporalstore-rust --lib what_gates_the_periodic_truncation -- --ignored --nocapture
///
/// #1470 said the periodic scheduler reached none of `gc_before_sequence`'s production callers.
/// That was too strong, and this is the measurement that says so. `run_gc_inner` truncates BOTH
/// logs and the page-GC stage calls it -- but only when `stale_page_pressure` holds. #1470's
/// probe wrote 1,512 keys and never deleted, so there were no stale slabs, the gate was false,
/// and the stage never ran. "Six rounds reclaimed 0" was true for a WRITE-ONLY workload and I
/// generalised it into "the loop never truncates".
///
/// Two arms, identical except for the deletes, so the gate is the only difference between them.
#[test]
#[ignore]
fn what_gates_the_periodic_truncation() {
    fn wal_records(engine: &TemporalEngine, shard_id: crate::types::ShardId) -> usize {
        engine
            .write_ahead_log_store()
            .scan(shard_id, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or(0)
    }

    // A 2x2 on the two variables that differed between #1470's fixture and the first attempt
    // here -- record COUNT and value SIZE -- because changing both at once cannot say which one
    // decides. Plus the delete arm, which fails for a different and already-identified reason.
    for (label, delete_every, keys, value_bytes) in [
        ("1256 x 64", 0usize, 1_256usize, 64usize),
        ("1256 x 96", 0, 1_256, 96),
        ("2000 x 64", 0, 2_000, 64),
        ("2000 x 96", 0, 2_000, 96),
        ("2000 x 96 del", 2, 2_000, 96),
    ] {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for index in 0..keys {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("gate-{index:06}"),
                    value: vec![b'v'; value_bytes],
                },
            });
        }
        if delete_every > 0 {
            for index in (0..keys).step_by(delete_every) {
                engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::CommonDelete {
                        key: format!("gate-{index:06}"),
                    },
                });
            }
        }
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        let engine_before = runtime.engine();
        let before = wal_records(&engine_before, 1);
        let mut report = runtime.run_storage_manager_once(1, StorageManagerOptions::default());
        let mut page_gc_rounds = 0;
        if report.executed_stages.iter().any(|stage| stage == "reclaim_page") {
            page_gc_rounds += 1;
        }
        for _ in 0..5 {
            report = runtime.run_storage_manager_once(1, StorageManagerOptions::default());
            if report.executed_stages.iter().any(|stage| stage == "reclaim_page") {
                page_gc_rounds += 1;
            }
        }
        let _ = page_gc_rounds;
        let engine_after = runtime.engine();
        let after = wal_records(&engine_after, 1);
        // Why did (or did not) it reclaim? The plan is the thing that decides.
        let plan = engine_after.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());
        eprintln!(
            "  {label:>13}  plan: safe={} covered={} uncovered={} retain_from={} current={} \
             blockers={:?}",
            plan.safe_to_reclaim,
            plan.covered_bucket_count,
            plan.uncovered_bucket_count,
            plan.retain_from_wal_sequence,
            plan.current_wal_sequence,
            plan.blocker_reasons,
        );
        eprintln!(
            "  {label:>13}  log-resident pages still registered: {}",
            engine_after.wal_resident_page_count(1)
        );
        // The clamp `gc_before_sequence` actually applies. A permissive plan still cannot drop
        // anything above this, so it is the field that explains a safe plan reclaiming nothing.
        eprintln!(
            "  {label:>13}  durable frontier: wal={} index_log={}",
            plan.durable_bucket_generation_frontier_wal_sequence,
            plan.durable_bucket_generation_frontier_index_log_sequence,
        );
        eprintln!(
            "  {label:>12}: stale_slabs={} reclaim_candidates={} reclaimable_bytes={} \
             page_gc_rounds={page_gc_rounds}/6 wal {before} -> {after}",
            report.pressure.stale_block_slab_count,
            report.pressure.reclaim_candidate_count,
            report.pressure.reclaimable_physical_bytes,
        );
    }
}

/// Do BOTH logs stay bounded while ingestion keeps going? Prints a per-round table.
///
///   cargo test -p temporalstore-rust --lib do_both_logs_stay_bounded_under_ingestion \
///       -- --ignored --nocapture --test-threads=1
///
/// Everything this session wired -- #1470 (WAL reclaim on the periodic loop), #1490 (index-log
/// reclaim), #1500 (the dump cap bounds a stage not a round), #1503 (the cycle's round bounds) --
/// was verified one stage at a time on a STATIC store: write everything, then run rounds. That is
/// not how a server runs. The question this answers is the one that actually matters: with writes
/// arriving continuously, do the logs reach a steady state, or do they grow for ever anyway?
///
/// The shape is deliberate. Each round writes a batch and THEN runs one maintenance round, which
/// is the real interleaving -- the dump, the reclaim and the next batch all racing the same shard
/// lock. A test that writes everything first cannot see a reclaim that is always one round behind
/// its own ingest.
#[test]
#[ignore]
fn do_both_logs_stay_bounded_under_ingestion() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    fn wal_records(engine: &TemporalEngine) -> usize {
        engine
            .write_ahead_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or(0)
    }
    fn index_records(engine: &TemporalEngine) -> usize {
        engine
            .index_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or(0)
    }

    const BATCH: usize = 2_000;
    const ROUNDS: usize = 12;

    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );

    eprintln!("  round   written      wal   index    wal_peak  index_peak");
    let mut written = 0usize;
    let mut wal_peak = 0usize;
    let mut index_peak = 0usize;
    for round in 1..=ROUNDS {
        let engine = runtime.engine();
        for index in 0..BATCH {
            let key = format!("soak-{:08}", written + index);
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet { key, value: vec![b'v'; 96] },
            });
            assert!(response.status.ok, "write failed: {:?}", response.status);
        }
        written += BATCH;

        // One maintenance round, exactly as the scheduler runs it.
        runtime.run_storage_manager_once(1, StorageManagerOptions::default());

        let engine = runtime.engine();
        let wal = wal_records(&engine);
        let index = index_records(&engine);
        wal_peak = wal_peak.max(wal);
        index_peak = index_peak.max(index);
        // WHY, per round. A log that never shrinks under ingestion is either being declined by
        // the plan or never reaching a threshold, and those want different fixes.
        let plan = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());
        eprintln!(
            "  {round:>5}   {written:>7}   {wal:>6}  {index:>6}   safe={} cov={} uncov={} \
             retain_wal={} retain_idx={} blockers={:?}",
            plan.safe_to_reclaim,
            plan.covered_bucket_count,
            plan.uncovered_bucket_count,
            plan.retain_from_wal_sequence,
            plan.retain_from_index_log_sequence,
            plan.blocker_reasons,
        );
    }

    let engine = runtime.engine();
    let wal = wal_records(&engine);
    let index = index_records(&engine);
    eprintln!(
        "  ingested {written}, wal ends at {wal} (peak {wal_peak}), index ends at {index} (peak {index_peak})"
    );
    // Deliberately no assertion yet: this prints first so the steady state can be READ off a real
    // interleaving before anything is pinned to a number picked from a static fixture.
}

/// How large must the index-GC round cap be to KEEP UP with ingestion? Prints.
///
///   cargo test -p temporalstore-rust --lib what_index_gc_cap_keeps_up \
///       -- --ignored --nocapture --test-threads=1
///
/// #1516 unfroze the reclaim floor and both logs started reclaiming. The WAL reaches a steady
/// state; the index log does not, because `index_gc_max_entries_per_round` removes 256 records a
/// round while ingest adds 2,000. This sweeps the cap on the same interleaved fixture -- write a
/// batch, run one maintenance round -- and reports whether the log converges or diverges.
///
/// 256 is the CYCLE's default, chosen for an on-demand call. A periodic loop racing live ingest
/// is a different workload, and the question is what value makes the log stop growing rather than
/// what value is tidy.
///
/// MEASURED -- and the answer is that no value does:
///
///      cap   ingested    index_log   verdict
///      256      16000        15488   diverging
///     1024      16000        13952   diverging
///     2048      16000        11904   diverging
///     4096      16000        11904   diverging
///
/// Removal rises with the cap up to 2,048 and then stops dead: 4,096 leaves the log at exactly the
/// same size, so on this workload, above 2,048 the cap is not what binds -- 512 records a round
/// against 2,000 arriving, and raising the constant does not reach it.
///
/// Two limits on how far to read that. This fixture writes 16,000 DISTINCT keys, so almost every
/// index record is the current version of a live key and there is legitimately little to reclaim:
/// "diverging" here is not by itself evidence of a defect. And `what_the_stage_order_buys`, whose
/// fixture overwrites a small key space so records actually become garbage, reclaims 1,765 records
/// a round rather than 64 on the same batch size. So what binds at 512 is specific to insert-only
/// traffic and is NOT identified here -- do not read this as a general ceiling upstream of the
/// knob. What it does establish is that this knob is not the lever on that shape of load, and a
/// future round of tuning should re-run the sweep rather than assume the cap was left too low.
#[test]
#[ignore]
fn what_index_gc_cap_keeps_up() {
    const BATCH: usize = 2_000;
    const ROUNDS: usize = 8;

    fn index_records(engine: &TemporalEngine) -> usize {
        engine
            .index_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or(0)
    }

    eprintln!("     cap   ingested    index_log   verdict");
    for cap in [256usize, 1_024, 2_048, 4_096] {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        let options = StorageManagerOptions {
            index_gc_max_entries_per_round: cap,
            ..StorageManagerOptions::default()
        };
        let mut written = 0usize;
        for _ in 0..ROUNDS {
            let engine = runtime.engine();
            for index in 0..BATCH {
                engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: format!("cap-{:08}", written + index),
                        value: vec![b'v'; 96],
                    },
                });
            }
            written += BATCH;
            runtime.run_storage_manager_once(1, options.clone());
        }
        let engine = runtime.engine();
        let index = index_records(&engine);
        // Converging means the log is a bounded multiple of one batch rather than of the total.
        let verdict = if index < BATCH * 2 {
            "bounded"
        } else if index < written / 2 {
            "lagging"
        } else {
            "diverging"
        };
        eprintln!("  {cap:>6}   {written:>8}   {index:>10}   {verdict}");
    }
}

/// Does reclaiming the index log before rewriting blocks trim more of it per round? Prints.
///
///   cargo test -p temporalstore-rust --lib what_the_stage_order_buys \
///       -- --ignored --nocapture --test-threads=1
///
/// The round-cap sweep established that index-log removal saturates at 512 records a round no
/// matter how large the round cap is -- 2,048 and 4,096 leave the log at exactly the same size --
/// so the cap is not what binds. One candidate for what does: compaction rewrites live records
/// into fresh blocks and appends an index record for each, and it ran BEFORE the reclaim. Those
/// records sit above the floor the reclaim computed, so no round cap can touch them.
///
/// The fixture OVERWRITES a fixed key space rather than appending fresh keys. That matters: an
/// insert-only load leaves no stale pages, so compaction never fires, and a first version of this
/// probe measured a run where `compact_pages` executed zero times out of eight -- a number that
/// looked like a clean result and was actually a measurement of nothing. The assertion below is
/// there so that can never pass silently again.
///
/// MEASURED, both arrangements, compaction firing in all eight rounds of each:
///
///     compact then reclaim (before)   ingested=16000  index_log=1873  per_round=1765
///     reclaim then compact (after)    ingested=16000  index_log=1873  per_round=1765
///
/// Identical. The candidate above is REFUTED: compaction's fresh records are above the floor the
/// reclaim computed, and a record above the floor is protected whichever order the stages run in,
/// so moving the reclaim earlier cannot collect them sooner. The stage order was changed to match
/// the intended sequence, not to buy throughput, and this probe exists so a later change that
/// claims otherwise has to produce a different pair of numbers.
///
/// Note the per-round figure against the round-cap sweep's 64: the difference is the WORKLOAD,
/// not the cap. Here records genuinely become garbage; there, 16,000 distinct keys meant almost
/// every record was the live version of a key and there was little to reclaim.
#[test]
#[ignore]
fn what_the_stage_order_buys() {
    const BATCH: usize = 2_000;
    const ROUNDS: usize = 8;
    // Smaller than BATCH, so every round rewrites keys earlier rounds wrote and the blocks
    // holding the old versions become garbage -- which is what compaction needs to trigger.
    const KEYSPACE: usize = 500;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    let options = StorageManagerOptions::default();
    let mut written = 0usize;
    let mut compacted_rounds = 0usize;
    let mut reclaimed_rounds = 0usize;
    for _ in 0..ROUNDS {
        let engine = runtime.engine();
        for index in 0..BATCH {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("order-{:08}", (written + index) % KEYSPACE),
                    value: vec![b'v'; 96],
                },
            });
        }
        written += BATCH;
        let report = runtime.run_storage_manager_once(1, options.clone());
        if report
            .executed_stages
            .iter()
            .any(|stage| stage == "compact_pages")
        {
            compacted_rounds += 1;
        }
        if report
            .executed_stages
            .iter()
            .any(|stage| stage == "reclaim_index")
        {
            reclaimed_rounds += 1;
        }
    }
    let engine = runtime.engine();
    let index = engine
        .index_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .map(|records| records.len())
        .unwrap_or(0);
    let removed = written.saturating_sub(index);
    eprintln!("  ingested={written} index_log={index} removed={removed} per_round={}",
        removed / ROUNDS);
    eprintln!("  rounds={ROUNDS} compact_pages_ran={compacted_rounds} reclaim_index_ran={reclaimed_rounds}");
    // The positive control. Where compaction never runs, the two stage orders are the same
    // program and the size above says nothing about either.
    assert!(
        compacted_rounds > 0,
        "compaction never ran, so this measures nothing about the stage order"
    );
}

/// Can the page-GC garbage floor ever exclude a slab? Prints.
///
///   cargo test -p temporalstore-rust --lib can_the_page_gc_garbage_floor_bind \
///       -- --ignored --nocapture --test-threads=1
///
/// The floor keeps a slab whose band is less than `min_slab_garbage_basis_points` garbage, and the
/// band's live fraction is summed over the slabs in that band which are NOT collectable. So the
/// floor can only bind where a band holds a MIX -- some collectable slabs, some not. This prints
/// each candidate's band id against its own id, and the utility the floor is compared against.
#[test]
#[ignore]
fn can_the_page_gc_garbage_floor_bind() {
    const BATCH: usize = 400;
    const ROUNDS: usize = 6;
    const KEYSPACE: usize = 100;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    let options = StorageManagerOptions::default();
    let mut written = 0usize;
    for _ in 0..ROUNDS {
        let engine = runtime.engine();
        for index in 0..BATCH {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("floor-{:06}", (written + index) % KEYSPACE),
                    value: vec![b'v'; 96],
                },
            });
        }
        written += BATCH;
        runtime.run_storage_manager_once(1, options.clone());
    }

    let engine = runtime.engine();
    let live = engine.live_block_slab_ids_all_shards();
    let plan = engine
        .block_store()
        .gc_policy_plan(
            u64::MAX,
            live.iter().copied(),
            &crate::block_store::BlockStoreGcPolicy::with_slab_garbage_floor(
                crate::engine::reports::DEFAULT_PAGE_GC_MIN_BAND_GARBAGE_BASIS_POINTS,
                None,
            ),
        )
        .expect("plan");

    eprintln!(
        "  candidates={} selected={} skipped_by_policy={}",
        plan.candidate_count,
        plan.selected_block_slab_ids.len(),
        plan.skipped_by_policy_count
    );
    eprintln!("     slab   total_b    used_b   utility_bp   garbage_bp   floor_keeps_it");
    for candidate in plan.candidates.iter() {
        let garbage = 10_000u64.saturating_sub(candidate.utility_basis_points);
        let kept = garbage < crate::engine::reports::DEFAULT_PAGE_GC_MIN_BAND_GARBAGE_BASIS_POINTS;
        eprintln!(
            "  {:>7}  {:>8}  {:>8}   {:>10}   {:>10}   {}",
            candidate.block_slab_id,
            candidate.total_bytes,
            candidate.used_bytes,
            candidate.utility_basis_points,
            garbage,
            kept
        );
    }
    eprintln!(
        "  VERDICT: the floor excluded {} of {} candidates",
        plan.skipped_by_policy_count, plan.candidate_count
    );
}

/// The reclaim_page stage quarantines the slabs it reclaims instead of unlinking them.
///
/// The stage reached `gc_slabs_before_with_live_refs`, which destroys immediately. The
/// storage-manager cycle has always reached the delayed-destroy entry, so a slab it reclaimed
/// stayed on disk until a later purge and could be recovered if the retain floor turned out to
/// have been computed too generously. The periodic loop had no such second chance.
///
/// Quarantined slabs are still collected -- `DELAYED_DESTROY_MIN_AGE_MS` is an hour, and the
/// prepare stage passes `purge_delayed_destroy` whenever page GC is on -- so this trades prompt
/// space for recoverability, it does not leak.
#[test]
fn the_periodic_reclaim_quarantines_what_it_collects() {
    const BATCH: usize = 400;
    const ROUNDS: usize = 6;
    // Smaller than BATCH so later rounds overwrite earlier keys and whole slabs fall out of use.
    // With insert-only traffic every slab stays live, nothing is reclaimed at all, and the
    // assertions below would be measuring an empty run.
    const KEYSPACE: usize = 100;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    let options = StorageManagerOptions::default();
    let mut written = 0usize;
    let mut removed = 0usize;
    for _ in 0..ROUNDS {
        let engine = runtime.engine();
        for index in 0..BATCH {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("quarantine-{:06}", (written + index) % KEYSPACE),
                    value: vec![b'v'; 96],
                },
            });
        }
        written += BATCH;
        let report = runtime.run_storage_manager_once(1, options.clone());
        if let Some(gc) = report.gc_report.as_ref() {
            removed = removed.saturating_add(gc.block_slabs_removed);
        }
    }

    // The denominator. Nothing below means anything if the stage reclaimed nothing.
    assert!(
        removed > 0,
        "the stage reclaimed no slabs, so this measures nothing about how it disposes of them"
    );

    // Purging with a zero minimum age reports exactly what the stage left in quarantine. The
    // loop's own purge cannot have taken them: it uses the one-hour minimum.
    let quarantined = runtime
        .engine()
        .block_store()
        .purge_delayed_destroy_slabs_older_than(0)
        .map(|report| report.purged_block_slab_ids.len())
        .unwrap_or(0);
    assert!(
        quarantined > 0,
        "reclaimed {removed} slabs and quarantined none of them -- the stage unlinked them outright"
    );
}

/// The maintenance round runs its stages in the intended order.
///
/// Every other assertion about stages in this suite asks whether one RAN -- `.any(|s| s == ..)` --
/// and none of them asks what ran before what, so the round could be reshuffled without a single
/// test noticing. Two stages have already been in the wrong place: the index reclaim ran after
/// compaction, and evict ran last of all, after the metrics reap.
///
/// Evict's position was the more interesting of the two. Expire drops what has died of old age and
/// evict drops what is merely cold; they are two halves of relieving memory and belong together,
/// ahead of the stages that rewrite pages. Running evict last meant it chose victims from a state
/// compaction had just churned, and the memory it freed arrived too late to spare any of those
/// stages work.
///
/// This pins the sequence itself. It asserts relative order and not an exact list, so a new stage
/// can be added without editing it -- only MOVING one of these breaks it.
#[test]
fn the_maintenance_round_runs_its_stages_in_order() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    for index in 0..600usize {
        let engine = runtime.engine();
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("order-guard-{:06}", index % 100),
                value: vec![b'v'; 96],
            },
        });
    }
    // Every stage on, so the round has something to order. Eviction ships DISABLED, so without
    // this the evict stage would be absent and the assertion about it would be vacuous.
    let options = StorageManagerOptions {
        enable_evict: true,
        ..StorageManagerOptions::default()
    };
    let report = runtime.run_storage_manager_once(1, options);
    let stages = report.executed_stages.clone();

    // The order the design intends. Some of these are pressure-gated and legitimately skip a
    // round -- `reclaim_memory` does exactly that on this fixture -- so the check is on the
    // stages that RAN, in the order they ran. Requiring all nine made the guard fail for a
    // reason that had nothing to do with ordering.
    let expected = [
        "prepare",
        "reclaim_wal",
        // Expire and evict come BEFORE the blanket cache invalidation, not after it: the
        // invalidation zeroes the very number eviction's gate reads, so running it first meant
        // eviction never opened its gate at all. See
        // `eviction_opens_its_gate_when_the_cache_is_still_there`.
        "expire",
        "evict",
        "reclaim_memory",
        "reclaim_page",
        "reclaim_index",
        "compact_pages",
        "reap_metrics",
    ];
    let mut previous: Option<(&str, usize)> = None;
    for name in expected {
        let Some(position) = stages.iter().position(|stage| stage == name) else {
            continue;
        };
        if let Some((earlier, earlier_position)) = previous {
            assert!(
                earlier_position < position,
                "{earlier} must run before {name}, but the round executed {stages:?}"
            );
        }
        previous = Some((name, position));
    }

    // The denominator. Skipping absent stages is what makes the loop above robust, and it is
    // also what would let it pass a round that ran nothing worth ordering -- so require the
    // three this change is actually about.
    for required in ["expire", "evict", "reclaim_page"] {
        assert!(
            stages.iter().any(|stage| stage == required),
            "{required} did not run, so the order assertion above proved nothing: {stages:?}"
        );
    }
}


/// Who actually drops the shard cache during a maintenance round? Prints.
///
///   cargo test -p temporalstore-rust --lib what_drops_the_shard_cache \
///       -- --ignored --nocapture --test-threads=1
///
/// `run_gc_inner` opens by calling `invalidate_shard`, which empties the whole shard. But
/// `reclaim_memory` ALSO passes `invalidate_cache: true` and runs earlier in the round, so on any
/// round where that stage fires the cache is already gone by the time page GC looks at it. This
/// prints, per round, which stages ran, how warm the cache was, and what each invalidation
/// actually removed -- so the size of the page-GC drop is measured rather than assumed.
#[test]
#[ignore]
fn what_drops_the_shard_cache() {
    const BATCH: usize = 300;
    const ROUNDS: usize = 8;
    const KEYSPACE: usize = 100;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    let options = StorageManagerOptions::default();

    eprintln!("  round  warm_bytes  mem_ran  gc_ran  gc_cache_removed  slabs_removed");
    let mut written = 0usize;
    for round in 0..ROUNDS {
        let engine = runtime.engine();
        for index in 0..BATCH {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("whodrops-{:06}", (written + index) % KEYSPACE),
                    value: vec![b'v'; 96],
                },
            });
        }
        written += BATCH;
        for key_index in 0..KEYSPACE {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: format!("whodrops-{:06}", key_index),
                },
            });
        }
        let warm = engine.cache().stats().memory_bytes;
        let report = runtime.run_storage_manager_once(1, options.clone());
        let mem_ran = report
            .executed_stages
            .iter()
            .any(|stage| stage == "reclaim_memory");
        let gc_ran = report
            .executed_stages
            .iter()
            .any(|stage| stage == "reclaim_page");
        let gc_removed = report
            .gc_report
            .as_ref()
            .map(|gc| gc.cache_entries_removed)
            .unwrap_or(0);
        let slabs = report
            .gc_report
            .as_ref()
            .map(|gc| gc.block_slabs_removed)
            .unwrap_or(0);
        eprintln!(
            "  {round:>5}  {warm:>10}  {mem_ran:>7}  {gc_ran:>6}  {gc_removed:>16}  {slabs:>13}"
        );
    }

    // Scenario two: reclaim_memory SKIPPED.
    //
    // That stage is pressure-gated. When it fires it invalidates the whole shard itself, which is
    // why page GC's own drop removes nothing above. The question this scenario answers is what
    // page GC does on a round where the earlier stage did NOT run and the cache is still warm.
    eprintln!("  -- with reclaim_memory disabled --");
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    let options = StorageManagerOptions {
        enable_memory_reclaim: false,
        ..StorageManagerOptions::default()
    };
    let mut written = 0usize;
    for round in 0..ROUNDS {
        let engine = runtime.engine();
        for index in 0..BATCH {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("whodrops2-{:06}", (written + index) % KEYSPACE),
                    value: vec![b'v'; 96],
                },
            });
        }
        written += BATCH;
        for key_index in 0..KEYSPACE {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: format!("whodrops2-{:06}", key_index),
                },
            });
        }
        let warm = engine.cache().stats().memory_bytes;
        let report = runtime.run_storage_manager_once(1, options.clone());
        let mem_ran = report
            .executed_stages
            .iter()
            .any(|stage| stage == "reclaim_memory");
        let gc_ran = report
            .executed_stages
            .iter()
            .any(|stage| stage == "reclaim_page");
        let gc_removed = report
            .gc_report
            .as_ref()
            .map(|gc| gc.cache_entries_removed)
            .unwrap_or(0);
        let slabs = report
            .gc_report
            .as_ref()
            .map(|gc| gc.block_slabs_removed)
            .unwrap_or(0);
        eprintln!(
            "  {round:>5}  {warm:>10}  {mem_ran:>7}  {gc_ran:>6}  {gc_removed:>16}  {slabs:>13}"
        );
    }
}

/// A page-GC round invalidates the slabs it reclaimed, not the whole shard's cache.
///
/// `run_gc_inner` opened by calling `invalidate_shard`, which empties every memory, pmem and disk
/// entry the shard holds. It runs BEFORE the collection, because at that point nothing knows what
/// will be reclaimed -- so it takes everything, including on a round that goes on to reclaim
/// nothing at all. The storage-manager cycle has always invalidated per reclaimed slab instead.
///
/// `enable_memory_reclaim: false` is load-bearing, not incidental. `reclaim_memory` passes
/// `invalidate_cache: true` and runs earlier in the round, so when it fires it empties the shard
/// itself and page GC's drop removes nothing -- measured, zero entries on every round. Two earlier
/// versions of this guard passed under mutation for exactly that reason: they were watching a
/// cache that an earlier stage had already emptied. Disabling that stage is what isolates the
/// behaviour under test. `what_drops_the_shard_cache` prints both arrangements.
///
/// The assertion is the invariant that separates them: a round that reclaimed NOTHING must
/// invalidate nothing. Measured, whole-shard drops 200-400 entries on such a round.
#[test]
fn a_page_gc_round_invalidates_only_what_it_reclaimed() {
    const BATCH: usize = 300;
    const ROUNDS: usize = 6;
    const KEYSPACE: usize = 100;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    let options = StorageManagerOptions {
        enable_memory_reclaim: false,
        ..StorageManagerOptions::default()
    };

    let mut written = 0usize;
    let mut checked_rounds = 0usize;
    for _ in 0..ROUNDS {
        let engine = runtime.engine();
        for index in 0..BATCH {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("cachescope-{:06}", (written + index) % KEYSPACE),
                    value: vec![b'v'; 96],
                },
            });
        }
        written += BATCH;
        // Warm the cache before each round, so a whole-shard drop always has something to take.
        for key_index in 0..KEYSPACE {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: format!("cachescope-{:06}", key_index),
                },
            });
        }
        assert!(
            engine.cache().stats().memory_bytes > 0,
            "the cache held nothing after reading every key, so this round proves nothing"
        );

        let report = runtime.run_storage_manager_once(1, options.clone());
        let Some(gc) = report.gc_report.as_ref() else {
            continue;
        };
        if gc.block_slabs_removed == 0 {
            checked_rounds += 1;
            assert_eq!(
                gc.cache_entries_removed, 0,
                "a round that reclaimed no slabs invalidated {} cache entries -- that is the \
                 whole shard being dropped, not what this round collected",
                gc.cache_entries_removed
            );
        }
    }

    // The denominator. The assertion only fires on rounds that reclaimed nothing, so without one
    // of those it checked nothing at all.
    assert!(
        checked_rounds > 0,
        "no round reclaimed zero slabs, so the invariant was never exercised"
    );
}

/// Does a page-GC round's cost grow with the number of slabs the store holds? Prints.
///
///   cargo test -p temporalstore-rust --lib what_a_page_gc_round_walks \
///       -- --ignored --nocapture --test-threads=1
///
/// `gc_slabs_before_with_live_refs` lists every slab file under the block-store root and stats each
/// one, every round, with no bound and no cursor -- the report's retained + removed IS the walk.
/// The design it converges on does one zone per round and resumes an in-progress zone from a stored
/// cursor, so its per-round cost does not grow with the store.
///
/// This prints the walk width and the wall time of the call as slabs accumulate, so the question
/// "does it matter here" is answered with numbers rather than from the shape of the code.
#[test]
#[ignore]
fn what_a_page_gc_round_walks() {
    const BATCH: usize = 400;
    const ROUNDS: usize = 12;
    const KEYSPACE: usize = 200;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    let options = StorageManagerOptions::default();

    eprintln!("  round  slabs_walked  retained  removed  gc_micros");
    let mut written = 0usize;
    for round in 0..ROUNDS {
        let engine = runtime.engine();
        for index in 0..BATCH {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("gcwalk-{:06}", (written + index) % KEYSPACE),
                    value: vec![b'v'; 96],
                },
            });
        }
        written += BATCH;
        runtime.run_storage_manager_once(1, options.clone());

        // Time the walk directly, with a floor that retains everything, so the measurement is the
        // SCAN and not the deletion: retain_from 0 makes every slab ineligible.
        let engine = runtime.engine();
        let started = std::time::Instant::now();
        let report = engine
            .block_store()
            .gc_slabs_before_with_live_refs(0, std::iter::empty())
            .expect("gc");
        let micros = started.elapsed().as_micros();
        let retained = report.retained_block_slab_ids.len();
        let removed = report.removed_block_slab_ids.len();
        let walked = retained + removed;
        eprintln!("  {round:>5}  {walked:>12}  {retained:>8}  {removed:>7}  {micros:>9}");
    }
}

/// How many rounds does eviction need to get back under its memory limit? Prints.
///
///   cargo test -p temporalstore-rust --lib how_long_eviction_takes_to_converge \
///       -- --ignored --nocapture --test-threads=1
///
/// `apply_storage_eviction` selects up to `eviction_batch_limit` victims, evicts them once, and
/// returns -- even when the pressure it just measured is still far above the threshold. It reports
/// `cooldown: pressure_after >= pressure_before`, so it knows when a round freed nothing, and
/// nothing acts on that. The design being followed loops instead, taking batches until usage is
/// back under the limit and stopping on a per-call count budget rather than on the first batch.
///
/// Each arm builds its OWN store. A first version shared one runtime across both arms, so the
/// second arm inherited a cache the first had already drained and reported zeros that meant
/// nothing.
///
/// WHY IT STALLS, which #1555 left open. Not pacing and not selection -- both of those match the
/// design being followed already:
///
///   - pacing: #1555 gave the stage a count budget, so it keeps taking batches while they help.
///   - selection: `evict_sampler` lives on `ShardState`, so the sampler's cursor and pool persist
///     across calls. That is the same persistent-iterator shape their `PolicyLru` uses.
///
/// The ceiling is the MODE. With `eviction_delete_drop` false -- the shipped default, mode
/// `evict_cache` -- a victim is handled by `cache.invalidate_slot(shard_id, routing_bucket)` and
/// nothing else. Eviction can free exactly what is CACHED. Once the cached pages of the eligible
/// buckets are gone, another batch frees nothing, `cooldown` is set, and the loop correctly stops.
///
/// So eviction cannot converge below the cached set by construction, and the bucket node itself is
/// never dropped. That is the same architectural blocker as restore: there is no per-bucket
/// load-back path, so nothing may evict a bucket node and expect to read it again. Raising the
/// budget, changing the sampler, or looping harder cannot move this number -- the fix is a load
/// path, or a deliberate decision to run this stage in `delete_drop`.
#[test]
#[ignore]
fn how_long_eviction_takes_to_converge() {
    const KEYS: usize = 4_000;
    const ROUNDS: usize = 10;

    fn arm(memory_reclaim: bool) {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        {
            let engine = runtime.engine();
            for index in 0..KEYS {
                engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: format!("evictconv-{:08}", index),
                        value: vec![b'v'; 256],
                    },
                });
            }
            for index in 0..KEYS {
                engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: format!("evictconv-{:08}", index),
                    },
                });
            }
        }
        let start = runtime.engine().cache().stats().memory_bytes;
        let threshold = (start / 4).max(1);
        eprintln!(
            "  -- enable_memory_reclaim={memory_reclaim}  start={start}  threshold={threshold} --"
        );
        eprintln!("  round  gate_open  before  after  victims  cooldown  skipped");

        let options = StorageManagerOptions {
            enable_evict: true,
            enable_memory_reclaim: memory_reclaim,
            eviction_memory_pressure_threshold: threshold,
            ..StorageManagerOptions::default()
        };
        let mut converged: Option<usize> = None;
        for round in 0..ROUNDS {
            // Re-warm before each round, so the question is what EVICTION does with a warm cache
            // rather than what an earlier stage left behind.
            let engine = runtime.engine();
            for index in 0..KEYS {
                engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: format!("evictconv-{:08}", index),
                    },
                });
            }
            let report = runtime.run_storage_manager_once(1, options.clone());
            let Some(eviction) = report.eviction.as_ref() else {
                eprintln!("  {round:>5}  (evict stage did not run)");
                continue;
            };
            eprintln!(
                "  {round:>5}  {:>9}  {:>6}  {:>5}  {:>7}  {:>8}  {}",
                eviction.pressure_gate_open,
                eviction.pressure_before,
                eviction.pressure_after,
                eviction.selected_victims.len(),
                eviction.cooldown,
                if eviction.skipped_reason.is_empty() {
                    "-"
                } else {
                    eviction.skipped_reason.as_str()
                },
            );
            if eviction.pressure_after < threshold && converged.is_none() {
                converged = Some(round + 1);
            }
        }
        match converged {
            Some(n) => eprintln!("  VERDICT: under the limit after {n} round(s)"),
            None => eprintln!("  VERDICT: still OVER after {ROUNDS} rounds"),
        }
    }

    arm(true);
    arm(false);
}

/// The round reports what eviction did, including when it declined to do anything.
///
/// Every other stage puts its report on `StorageManagerLoopReport`. The evict stage built a full
/// one -- victims, pressure before and after, `cooldown` for a round that freed nothing, and
/// `skipped_reason` for a round that never started -- and the loop dropped it, keeping only "did
/// it run" for `executed_stages`. So the one stage whose purpose is relieving memory was the one
/// stage whose outcome nobody could see.
///
/// That is not cosmetic. On the shipped configuration this stage NEVER opens its gate:
/// `reclaim_memory` runs earlier in the round and invalidates the whole shard cache, and
/// eviction's threshold is compared against exactly that cache, so it reads 0 and skips with
/// `memory_pressure_below_threshold` every time. Measured over ten rounds in
/// `how_long_eviction_takes_to_converge`. With the report discarded there was no way to learn this
/// from a running server -- `executed_stages` says "evict" either way.
#[test]
fn the_round_reports_what_eviction_did() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    for index in 0..400usize {
        let engine = runtime.engine();
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("evictreport-{:06}", index % 100),
                value: vec![b'v'; 96],
            },
        });
    }

    // Eviction ships disabled, so without this the stage would not run and the assertion would
    // be about a round that never reached it.
    let options = StorageManagerOptions {
        enable_evict: true,
        ..StorageManagerOptions::default()
    };
    let report = runtime.run_storage_manager_once(1, options);

    assert!(
        report
            .executed_stages
            .iter()
            .any(|stage| stage == "evict"),
        "the evict stage did not run, so there is no report to carry: {:?}",
        report.executed_stages
    );
    let eviction = report
        .eviction
        .as_ref()
        .expect("the round ran the evict stage but carried no eviction report");

    // A skipped round must say why. `pressure_gate_open` and `skipped_reason` are the two fields
    // that distinguish "evicted nothing because there was nothing to evict" from "never looked",
    // and on the shipped configuration it is always the latter.
    assert!(
        eviction.pressure_gate_open || !eviction.skipped_reason.is_empty(),
        "eviction neither opened its gate nor said why it skipped -- the report cannot be acted on"
    );
    assert_eq!(
        eviction.shard_id, 1,
        "the report belongs to the shard the round ran for"
    );
}

/// What share of a maintenance round is the index-GC gate now? Prints.
///
///   cargo test -p temporalstore-rust --lib what_share_of_a_round_is_the_index_gate \
///       -- --ignored --nocapture --test-threads=1
///
/// `storage_index_gc_report` opens with `scan(shard, 0, u64::MAX, u64::MAX)` -- the WHOLE index
/// log -- and decodes every record twice to compute the ratio that decides whether GC should run
/// at all. The design being followed estimates the same ratio from a maintained item count and two
/// running averages: O(1), no scan, no decode.
///
/// That was measured once before, at 39 ms for 32,000 records, and DEFERRED -- because WAL reclaim
/// was costing 21,448 ms in the same round and 39 ms against that is noise. #1516 has since fixed
/// the frozen reclaim floor that made WAL reclaim cost what it did, so the denominator that
/// justified deferring is gone. This re-measures the gate as a SHARE of the round rather than in
/// isolation, because the share is what decides whether it is worth an exact counter.
///
/// The fixture deliberately does NOT run a round between writes: the gate's cost only shows on a
/// log that has been allowed to grow, which is the state it exists to detect.
#[test]
#[ignore]
fn what_share_of_a_round_is_the_index_gate() {
    eprintln!("  records  index_gc_on_ms  index_gc_off_ms  gate_share");
    for records in [2_000usize, 8_000, 32_000] {
        // Two stores built identically, so the only difference is whether the stage runs.
        let measure = |enable_index_gc: bool| -> u128 {
            let engine = TemporalEngine::default();
            engine.load_shard(1);
            let runtime = DataNodeRuntime::new_without_workers_with_options(
                engine,
                DataNodeRuntimeOptions {
                    worker_threads: 0,
                    max_queue_depth: 4,
                    max_background_queue_depth: 2,
                },
            );
            {
                let engine = runtime.engine();
                for index in 0..records {
                    engine.execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringSet {
                            key: format!("idxgate-{:08}", index),
                            value: vec![b'v'; 64],
                        },
                    });
                }
            }
            let options = StorageManagerOptions {
                enable_index_gc,
                ..StorageManagerOptions::default()
            };
            let started = Instant::now();
            runtime.run_storage_manager_once(1, options);
            started.elapsed().as_millis()
        };

        let on = measure(true);
        let off = measure(false);
        let share = if on == 0 {
            0.0
        } else {
            100.0 * (on.saturating_sub(off)) as f64 / on as f64
        };
        eprintln!("  {records:>7}  {on:>14}  {off:>15}  {share:>9.1}%");
    }
}

/// Can the reclaim_index dump be capped now that the reclaim floor is no longer frozen? Prints.
///
///   cargo test -p temporalstore-rust --lib can_the_index_dump_be_capped_now \
///       -- --ignored --nocapture --test-threads=1
///
/// The stage dumps the WHOLE dirty set every round, which `what_share_of_a_round_is_the_index_gate`
/// measures at ~68% of a round -- 14,579 ms of a 21,365 ms round on a 32,000-record log.
///
/// It is unbounded because #1500 tried capping it and index-log reclaim stopped dead: 16,000
/// records before a round and 16,000 after. The reason given was coverage --
/// `wal_plan.safe_to_reclaim` needs a durable manifest for every live generation, and the
/// whole-dirty-set dump is what produced one.
///
/// #1516 then fixed the frozen reclaim frontier: a clean bucket no longer pins the floor for ever,
/// and an unset floor no longer erases it. That is the mechanism that made a bounded dump fail to
/// advance anything, so the constraint recorded in #1500 may simply no longer hold. This sweeps
/// the cap and reports BOTH numbers that matter -- what the round costs, and whether the log still
/// reclaims. A cap that makes rounds cheap by not reclaiming is not a win.
#[test]
#[ignore]
fn can_the_index_dump_be_capped_now() {
    const BATCH: usize = 2_000;
    const ROUNDS: usize = 6;
    // Smaller than BATCH so later rounds overwrite earlier keys and index records actually become
    // garbage. A first version wrote 12,000 DISTINCT keys and every arm reported 12,000 records
    // left -- correctly, because with unique keys almost every record is the live version of a
    // key and there is nothing to reclaim. That fixture cannot tell a working cap from a broken
    // one.
    const KEYSPACE: usize = 500;

    fn index_records(engine: &TemporalEngine) -> usize {
        engine
            .index_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or(0)
    }

    eprintln!("     cap   ingested   index_log   round_ms   verdict");
    for cap in [0usize, 64, 256, 1_024] {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        let options = StorageManagerOptions {
            index_gc_max_dump_buckets_per_round: cap,
            ..StorageManagerOptions::default()
        };
        let mut written = 0usize;
        let mut total_round_ms = 0u128;
        for _ in 0..ROUNDS {
            let engine = runtime.engine();
            for index in 0..BATCH {
                engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: format!("dumpcap-{:08}", (written + index) % KEYSPACE),
                        value: vec![b'v'; 64],
                    },
                });
            }
            written += BATCH;
            let started = Instant::now();
            runtime.run_storage_manager_once(1, options.clone());
            total_round_ms += started.elapsed().as_millis();
        }
        let index = index_records(&runtime.engine());
        // Reclaiming means the log is a bounded multiple of one batch, not of the total.
        let verdict = if index < BATCH * 2 {
            "reclaims"
        } else if index < written / 2 {
            "lagging"
        } else {
            "STOPPED"
        };
        let per_round = total_round_ms / ROUNDS as u128;
        eprintln!("  {cap:>6}   {written:>8}   {index:>9}   {per_round:>8}   {verdict}");
    }
}

/// A capped reclaim_index dump still reclaims the index log.
///
/// #1500 capped this dump and index-log reclaim stopped dead -- 16,000 records before a round and
/// 16,000 after -- so the stage was left dumping the WHOLE dirty set with a comment saying not to
/// cap it. That is ~68% of a maintenance round: 14,579 ms of a 21,365 ms round on a 32,000-record
/// log.
///
/// The reason it broke was coverage: `wal_plan.safe_to_reclaim` needs a durable manifest for every
/// live generation, and the whole-dirty-set dump was what produced one. #1516 then fixed the
/// frozen reclaim frontier -- a clean bucket no longer pins the floor for ever, and an unset floor
/// no longer erases it -- which is the mechanism that made a bounded dump fail to advance
/// anything.
///
/// So the constraint recorded in #1500 no longer holds, and this is the guard that says so: a
/// capped dump reclaims as well as an unbounded one.
///
/// What it does NOT do, despite being the obvious thing to claim, is detect a re-frozen frontier.
/// That was checked rather than assumed: reverting #1516's `dirty_object_count > 0` condition
/// leaves this test PASSING. The fixture overwrites a 500-key space, so every bucket is dirty
/// every round, `dirty_object_count > 0` holds for all of them, and the condition being reverted
/// never applies. Catching that regression needs a fixture with CLEAN buckets -- buckets dumped
/// once and not written again -- which is the state where a clean bucket's stale manifest pins the
/// floor. Worth building; not built here, and this guard should not be read as covering it.
#[test]
fn a_capped_index_dump_still_reclaims() {
    const BATCH: usize = 2_000;
    const ROUNDS: usize = 6;
    // Overwrites, so records genuinely become garbage. With distinct keys almost every record is
    // the live version of a key, nothing is reclaimable, and both arms below would report the full
    // count while proving nothing.
    const KEYSPACE: usize = 500;

    fn run(cap: usize) -> (usize, usize) {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        let options = StorageManagerOptions {
            index_gc_max_dump_buckets_per_round: cap,
            ..StorageManagerOptions::default()
        };
        let mut written = 0usize;
        for _ in 0..ROUNDS {
            let engine = runtime.engine();
            for index in 0..BATCH {
                engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: format!("dumpcapguard-{:08}", (written + index) % KEYSPACE),
                        value: vec![b'v'; 64],
                    },
                });
            }
            written += BATCH;
            runtime.run_storage_manager_once(1, options.clone());
        }
        let remaining = runtime
            .engine()
            .index_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or(0);
        (written, remaining)
    }

    let (written, unbounded) = run(0);
    let (_, capped) = run(64);

    // The denominator: the unbounded arm has to reclaim, or "the capped arm matches it" is a
    // statement about two broken runs.
    assert!(
        unbounded < written / 2,
        "the UNBOUNDED arm did not reclaim ({unbounded} of {written} left), so this fixture cannot \
         tell whether the cap is what broke anything"
    );
    assert!(
        capped < written / 2,
        "a capped dump stopped index-log reclaim: {capped} of {written} records left, against \
         {unbounded} with no cap -- the reclaim floor is not advancing on the bounded path"
    );
}

/// A bucket that stopped being written does not pin the reclaim floor for ever.
///
/// #1514 measured both logs growing linearly under continuous ingestion while the plan reported
/// safe=true, full coverage, no blockers -- and a frontier frozen at 2001 for twelve rounds.
/// #1516 fixed it: the floor is a minimum over DIRTY buckets only, and an unset floor means "no
/// constraint" rather than zero. It shipped as 14 lines in one file with NO test, which is why
/// this exists.
///
/// The bug needs CLEAN buckets to show itself -- buckets dumped once and never written again. A
/// dirty-everything fixture cannot reproduce it: every bucket constrains the floor legitimately,
/// so including clean ones changes nothing. That is not hypothetical; the guard beside this one
/// (`a_capped_index_dump_still_reclaims`) overwrites a small key space, and reverting #1516 leaves
/// it passing.
///
/// So: write widely, dump everything, then keep writing to a SMALL subset. The buckets left behind
/// are clean, and their manifests hold sequence numbers from the first phase. Before #1516 those
/// stale manifests pinned the floor and neither log could ever be reclaimed past them.
#[test]
fn a_bucket_that_went_quiet_does_not_pin_the_reclaim_floor() {
    const WIDE_KEYS: usize = 2_000;
    const HOT_KEYS: usize = 20;
    const ROUNDS: usize = 8;
    const BATCH: usize = 1_000;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );

    // Phase one: write widely, so many buckets carry a sequence, then dump them all.
    {
        let engine = runtime.engine();
        for index in 0..WIDE_KEYS {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("quiet-wide-{:08}", index),
                    value: vec![b'v'; 64],
                },
            });
        }
    }
    runtime.run_storage_manager_once(1, StorageManagerOptions::default());

    // Phase two: keep writing, but only to a handful of keys. Every bucket the wide phase touched
    // and this one does not is now CLEAN, holding a manifest from phase one.
    let mut written = 0usize;
    for _ in 0..ROUNDS {
        let engine = runtime.engine();
        for index in 0..BATCH {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("quiet-hot-{:04}", (written + index) % HOT_KEYS),
                    value: vec![b'v'; 64],
                },
            });
        }
        written += BATCH;
        runtime.run_storage_manager_once(1, StorageManagerOptions::default());
    }

    let engine = runtime.engine();
    let wal_records = engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .map(|records| records.len())
        .unwrap_or(0);
    let index_records = engine
        .index_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .map(|records| records.len())
        .unwrap_or(0);

    // The denominator: phase two has to have written enough that a frozen floor is visible as
    // growth rather than lost in the noise of phase one.
    assert!(
        written >= BATCH * ROUNDS,
        "phase two wrote {written} records, too few to tell a frozen floor from a moving one"
    );
    // Neither log may hold everything phase two wrote. A floor pinned by the quiet buckets from
    // phase one cannot advance, and both logs then grow with the total.
    assert!(
        wal_records < written,
        "the write-ahead log holds {wal_records} records against {written} written -- a bucket \
         that went quiet is pinning the reclaim floor"
    );
    assert!(
        index_records < written,
        "the index log holds {index_records} records against {written} written -- a bucket that \
         went quiet is pinning the reclaim floor"
    );
}

/// Eviction opens its gate, because the cache it measures still exists when it looks.
///
/// `apply_storage_eviction` compares `eviction_memory_pressure_threshold` against the shard's
/// cache bytes. The `reclaim_memory` stage is a blanket whole-shard cache invalidation, and it
/// used to run FIRST -- so eviction read 0 and skipped with `memory_pressure_below_threshold`
/// every round, whatever the threshold was. Measured over ten rounds in #1536: the gate never
/// opened once. Eviction had never evicted anything on a default server, and `executed_stages`
/// said "evict" either way.
///
/// The confusing part is the name. The `ReclaimMemory` our stage is named after CONTAINS expire
/// and evict and invalidates no cache at all -- there, those two ARE the memory relief. Our
/// blanket invalidation is a separate mechanism with no counterpart, so there was never an
/// ordering to match, only an input not to destroy.
#[test]
fn eviction_opens_its_gate_when_the_cache_is_still_there() {
    const KEYS: usize = 1_000;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    {
        let engine = runtime.engine();
        for index in 0..KEYS {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("evictgate-{:06}", index),
                    value: vec![b'v'; 128],
                },
            });
        }
        // Read everything back, so there is a populated cache for the gate to measure.
        for index in 0..KEYS {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: format!("evictgate-{:06}", index),
                },
            });
        }
    }

    let warm = runtime.engine().cache().stats().memory_bytes;
    // The denominator. With an empty cache the gate is correct to stay shut and this proves
    // nothing about ordering.
    assert!(
        warm > 0,
        "the cache held nothing after reading every key back, so the gate has nothing to measure"
    );

    // A threshold below what the fixture actually built, so a gate reading the real cache must
    // open and a gate reading a zeroed one cannot.
    let options = StorageManagerOptions {
        enable_evict: true,
        eviction_memory_pressure_threshold: warm / 4,
        ..StorageManagerOptions::default()
    };
    let report = runtime.run_storage_manager_once(1, options);
    let eviction = report
        .eviction
        .as_ref()
        .expect("the round ran the evict stage but carried no eviction report");

    assert!(
        eviction.pressure_gate_open,
        "eviction did not open its gate: it measured {} against a threshold of {} on a cache \
         holding {warm} bytes, and skipped with {:?} -- an earlier stage has zeroed what it reads",
        eviction.pressure_before,
        eviction.memory_pressure_threshold,
        eviction.skipped_reason,
    );
}

/// How long does an expired key survive when the keyspace is large? Prints.
///
///   cargo test -p temporalstore-rust --lib how_long_an_expired_key_survives \
///       -- --ignored --nocapture --test-threads=1
///
/// `expires_at_ms` is a `BTreeMap<String, u64>` -- keyed by KEY, with the deadline as the value --
/// and `expiry_window` walks it in KEY order from a cursor, bounded by a scan budget. The caller
/// then tests `expires_at <= now` on what came back. So a key whose deadline has passed is found
/// only when the cursor happens to reach it, and every key that is NOT due is walked and charged
/// against the same budget on the way.
///
/// That makes time-to-expire a function of how many keys carry deadlines, not of how many are
/// actually due. The design being followed stores TTLs per bucket and keeps a per-bucket MINIMUM,
/// so a whole bucket that cannot contain anything due is skipped in one comparison and never
/// loaded.
///
/// This measures the consequence directly: a handful of already-expired keys hidden in a large
/// keyspace of live ones, and how many maintenance rounds pass before they are actually removed.
/// At 30 s a round, a round count IS a latency.
#[test]
#[ignore]
fn how_long_an_expired_key_survives() {
    const DUE_KEYS: usize = 10;
    const MAX_ROUNDS: usize = 60;

    for live_keys in [1_000usize, 10_000, 40_000] {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        {
            let engine = runtime.engine();
            // Live keys with a long TTL: never due, but every one of them carries a deadline and
            // so sits in the map the sweep walks.
            for index in 0..live_keys {
                engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSetEx {
                        key: format!("live-{:08}", index),
                        value: vec![b'v'; 32],
                        ttl_ms: 3_600_000,
                    },
                });
            }
            // The keys under test, with a TTL that has already passed by the time the first round
            // runs. "zzz" so they sort AFTER the live ones -- the cursor has to walk the whole
            // live set to reach them, which is the cost being measured.
            for index in 0..DUE_KEYS {
                engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSetEx {
                        key: format!("zzz-due-{:04}", index),
                        value: vec![b'v'; 32],
                        ttl_ms: 1,
                    },
                });
            }
        }

        let options = StorageManagerOptions::default();
        // Count what the SWEEP removed, never a read.
        //
        // A `StringGet` on an expired key triggers lazy expiry (`remove_if_expired`), so probing
        // for presence would delete the very keys under measurement and make the sweep look as
        // though it had found them. The stat counts only what the sweep itself removed.
        let before = runtime.stats().expired_records_removed;
        let mut rounds_until_gone: Option<usize> = None;
        for round in 0..MAX_ROUNDS {
            runtime.run_storage_manager_once(1, options.clone());
            let removed = runtime
                .stats()
                .expired_records_removed
                .saturating_sub(before);
            if removed >= DUE_KEYS as u64 {
                rounds_until_gone = Some(round + 1);
                break;
            }
        }
        match rounds_until_gone {
            Some(rounds) => eprintln!(
                "  live_keys={live_keys:>6}  expired after {rounds:>3} round(s)  (~{}s at 30s/round)",
                rounds * 30
            ),
            None => eprintln!(
                "  live_keys={live_keys:>6}  STILL PRESENT after {MAX_ROUNDS} rounds (~{}s)",
                MAX_ROUNDS * 30
            ),
        }
    }
}

/// An expired key is removed in a bounded number of rounds, whatever the keyspace.
///
/// The sweep used to walk `expires_at_ms` in KEY order from a cursor, testing deadlines as it
/// went, so a key that was not due was still walked and charged against the round's scan budget.
/// Time-to-expire was therefore keyspace/scan_budget rounds: ten expired keys behind 10,000 live
/// ones survived more than sixty rounds, and behind 40,000 they survived just as long.
///
/// `expiry_by_deadline` orders the same deadlines by deadline, so the due keys are a PREFIX and
/// the walk stops at the first one in the future. Measured after the change: one round at 1,000,
/// 10,000 and 40,000 live keys alike.
///
/// This guards the property that matters -- that the bound does not depend on the keyspace -- by
/// hiding the due keys behind a large live set and sorting them AFTER it, which is exactly the
/// arrangement the key-ordered scan was worst at.
#[test]
fn an_expired_key_is_removed_in_a_bounded_number_of_rounds() {
    const LIVE_KEYS: usize = 5_000;
    const DUE_KEYS: usize = 5;
    // Generous next to the measured 1, so this fails on a return to keyspace-proportional
    // latency (which would need ~39 rounds at this size) and not on a round of slack.
    const ROUND_BUDGET: usize = 3;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    {
        let engine = runtime.engine();
        for index in 0..LIVE_KEYS {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSetEx {
                    key: format!("live-{index:08}"),
                    value: vec![b'v'; 16],
                    ttl_ms: 3_600_000,
                },
            });
        }
        // "zzz" so the due keys sort AFTER every live one: a key-ordered cursor has to traverse
        // the whole live set to reach them.
        for index in 0..DUE_KEYS {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSetEx {
                    key: format!("zzz-due-{index:04}"),
                    value: vec![b'v'; 16],
                    ttl_ms: 1,
                },
            });
        }
    }

    // Count what the SWEEP removed. A `StringGet` would expire the key lazily on the way through
    // and make the sweep look as though it had found it.
    let before = runtime.stats().expired_records_removed;
    let options = StorageManagerOptions::default();
    let mut rounds_used = 0usize;
    for round in 0..ROUND_BUDGET {
        runtime.run_storage_manager_once(1, options.clone());
        rounds_used = round + 1;
        if runtime
            .stats()
            .expired_records_removed
            .saturating_sub(before)
            >= DUE_KEYS as u64
        {
            break;
        }
    }

    let removed = runtime
        .stats()
        .expired_records_removed
        .saturating_sub(before);
    assert!(
        removed >= DUE_KEYS as u64,
        "the sweep removed {removed} of {DUE_KEYS} expired keys in {rounds_used} round(s) behind \
         {LIVE_KEYS} live ones -- time-to-expire is scaling with the keyspace again"
    );
}

/// Does a compaction round cost the SHARD or the WORK? Prints.
///
///   cargo test -p temporalstore-rust --lib what_a_compaction_round_costs \
///       -- --ignored --nocapture --test-threads=1
///
/// #1465 gave compaction a budget of 2,048 page refs a round, and the round CONTINUES on the same
/// slab rather than re-rolling, so the work each round does is bounded and makes progress. What
/// that does not bound is the SCAN: the relocation walks `shard.hashes`, `shard.zsets`,
/// `shard.lists` and `shard.sets` from the start every round, so a page already sitting on the
/// target slab is still visited before being skipped.
///
/// The design being followed bounds the scan too: it walks `page_compaction_max_slots_per_round`
/// buckets through a PERSISTENT iterator and resumes where it stopped.
///
/// TWO "SKIP THE ROUND" CONDITIONS WERE TRIED AND BOTH REVERTED. Both make this probe read 1 round
/// and 0 refs at every size -- 3,430 ms down to ~650 at 32,000 keys -- and both are wrong. The
/// suite is what refused them, and between them they map out why the check is harder than it looks.
///
///   1. "every live page is already on the newest slab" -- 14 tests fail. It reads slab
///      MEMBERSHIP, so it skips a shard whose single slab is mostly garbage.
///   2. "stale_page_estimate == 0 && live_block_slab_count <= 1" -- 3 tests fail, among them
///      `feature_compaction_rewrites_shared_packed_page_once`. It reads dead space and spread, and
///      still misses shards that are fully live, on one slab, and want REPACKING.
///
/// Compaction turns out to do three things, and a skip has to respect all of them:
///
///   - reclaim dead space inside slabs        (`stale_page_estimate`)
///   - consolidate so older slabs become dead (`live_block_slab_count`)
///   - repack page LAYOUT into shared pages   (neither metric sees this)
///
/// The third is the one that defeats a cheap check: `feature_compaction_rewrites_shared_packed_page_once`
/// appends feature points that share a packed page and expects compaction to rewrite it once, with
/// nothing dead and everything on one slab. Any real fix needs a layout signal -- something like the
/// model layout reports compaction already produces -- not a density number.
///
/// The waste measured above is real and worth fixing. It is not fixable by asking a question about
/// space alone, which is what both attempts did.
///
/// WHERE THE ANSWER ACTUALLY LIVES, found later: the decision is PER OBJECT and belongs to the
/// MODEL, not to the shard. Their compactor asks one per object and skips on an empty answer:
///
///     auto res = ModelManager::CompactPagesHint(model_id, object_pages[object_id]);
///     if (page_indexes.empty()) { continue; }   // no need compaction
///
/// and a single-page model answers empty unconditionally -- their string model's whole
/// implementation is `return {};` with the comment "we keep data in single page, so no need do
/// compaction".
///
/// That is why all three global conditions were refused. There is no shard-wide predicate for
/// "needs compacting", because two objects in the same shard, on the same slab, with the same
/// staleness, get different answers depending on their model. It also explains why
/// `feature_compaction_rewrites_shared_packed_page_once` survives a skip that a string would not:
/// feature objects are packed across pages and genuinely have something to consolidate.
///
/// Ours has no such hint. `compact_shard_pages_with_budgets` walks `strings`, `hashes`, `zsets`,
/// `lists` and `sets` and relocates every live page, so it rewrites single-page objects that
/// cannot benefit. Adding the hint is not a small change -- declining to relocate an object leaves
/// its pages on the old slab, which keeps that slab live and changes what page GC may collect --
/// but it is the shape the answer has to take, and it is not the shape either attempt above tried.
///
/// This measures the difference the only way that separates them -- run compaction until there is
/// nothing left to move, then time one more round. Whatever that round costs is scan, not work. If
/// it grows with the shard, the scan is the cost; if it is flat, the budget already bounds
/// everything that matters and there is nothing to fix here.
#[test]
#[ignore]
fn what_a_compaction_round_costs() {
    eprintln!("  keys   converge_rounds   idle_round_ms   moved_on_idle_round");
    for keys in [2_000usize, 8_000, 32_000] {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for index in 0..keys {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: format!("compactscan-{:08}", index),
                    field: "f".to_string(),
                    value: vec![b'v'; 64],
                },
            });
        }

        // Compact until a round moves nothing: that is convergence, and everything after it is
        // pure scan.
        let mut converge_rounds = 0usize;
        for _ in 0..64 {
            let report = engine
                .compact_shard_pages(1)
                .expect("compaction should succeed");
            converge_rounds += 1;
            if report.rewritten_object_pages == 0 {
                break;
            }
        }

        let started = Instant::now();
        let idle = engine
            .compact_shard_pages(1)
            .expect("compaction should succeed");
        let idle_ms = started.elapsed().as_millis();
        eprintln!(
            "  {keys:>5}   {converge_rounds:>15}   {idle_ms:>13}   {:>19}",
            idle.rewritten_object_pages
        );
    }
}

/// Does compaction keep re-triggering itself on the PERIODIC path? Prints.
///
///   cargo test -p temporalstore-rust --lib does_compaction_retrigger_itself \
///       -- --ignored --nocapture --test-threads=1
///
/// `what_a_compaction_round_costs` shows compaction never reaching a fixed point when called
/// directly: 64 rounds, every one relocating all 2,000 live page refs. The code says why --
/// "a round that relocated everything closes, and the next starts fresh" -- so a completed round
/// drops its continuation state and the next rolls a FRESH slab and moves everything again.
///
/// Called directly that is merely wasteful. The question that decides whether it matters is
/// whether the PERIODIC loop keeps reaching it, because that stage only runs under
/// `stale_page_pressure` -- and rolling a fresh slab is precisely what leaves the previous one
/// stale. If compaction manufactures the pressure that triggers compaction, the loop rewrites the
/// whole live set every round for ever; if the gate shuts after a round or two, the direct-call
/// behaviour is a curiosity and not a defect.
///
/// WRITES NOTHING after the fixture: every round below acts on a store that is not changing, so
/// any repeated work is the loop's own doing.
///
/// MEASURED, and the answer is that this probe CANNOT reach the behaviour: `compact_ran` is false
/// on every round, with an insert-only fixture and with an overwrite-heavy one alike. The gate
/// counts stale SLABS, not stale pages, and a couple of thousand small records fit inside one
/// slab -- so no fixture of this size opens it.
///
/// What that establishes: the no-fixed-point behaviour `what_a_compaction_round_costs` measures is
/// reachable through a DIRECT call -- the on-demand cycle and the operator compact RPC -- and is
/// NOT demonstrated on the periodic loop. The hypothesis that compaction manufactures the
/// stale-page pressure that re-triggers compaction is UNPROVEN, not confirmed: reaching it needs a
/// store spanning several slabs, which this fixture deliberately does not build.
///
/// Left in place because the negative is worth keeping: anyone reading the direct-call numbers
/// will want to know whether the periodic path shares them, and the answer so far is that nobody
/// has shown it does.
#[test]
#[ignore]
fn does_compaction_retrigger_itself() {
    const KEYS: usize = 2_000;
    const ROUNDS: usize = 12;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    {
        let engine = runtime.engine();
        for index in 0..KEYS {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: format!("retrigger-{:08}", index),
                    field: "f".to_string(),
                    value: vec![b'v'; 64],
                },
            });
        }
    }

    eprintln!("  round  compact_ran  rewritten_page_refs  round_ms");
    let options = StorageManagerOptions::default();
    let mut rounds_that_compacted = 0usize;
    let mut total_rewritten = 0usize;
    for round in 0..ROUNDS {
        let started = Instant::now();
        let report = runtime.run_storage_manager_once(1, options.clone());
        let elapsed = started.elapsed().as_millis();
        let ran = report
            .executed_stages
            .iter()
            .any(|stage| stage == "compact_pages");
        let rewritten = report
            .compaction_report
            .as_ref()
            .map(|compaction| compaction.rewritten_object_pages)
            .unwrap_or(0);
        if ran {
            rounds_that_compacted += 1;
        }
        total_rewritten += rewritten;
        eprintln!("  {round:>5}  {ran:>11}  {rewritten:>19}  {elapsed:>8}");
    }
    eprintln!(
        "  VERDICT: compacted on {rounds_that_compacted} of {ROUNDS} rounds, \
         {total_rewritten} page refs rewritten in total, over a store of {KEYS} that never changed"
    );
}

/// The evict stage keeps taking batches while they still help.
///
/// The stage used to take ONE batch of `eviction_batch_limit` and return, however far the pressure
/// still was from the threshold: measured at 16 victims freeing about 4,800 bytes against a
/// 1,775,000-byte overage, roughly 370 rounds at a round every thirty seconds. The design being
/// followed loops until usage is back under the limit and stops on a per-call COUNT budget --
/// `evict_count_limit` 100 against a batch size of 10 -- rather than after the first batch.
///
/// `eviction_count_limit` is that budget. What this asserts is that a round actually spends more
/// than one batch when the pressure warrants it, and that the report describes the STAGE rather
/// than its final batch: returning the last report alone would show only the unproductive batch
/// that stopped the loop and hide every victim taken before it.
#[test]
fn the_evict_stage_keeps_going_while_batches_still_help() {
    const KEYS: usize = 3_000;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    {
        let engine = runtime.engine();
        for index in 0..KEYS {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("evictloop-{index:08}"),
                    value: vec![b'v'; 128],
                },
            });
        }
        for index in 0..KEYS {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: format!("evictloop-{index:08}"),
                },
            });
        }
    }

    let warm = runtime.engine().cache().stats().memory_bytes;
    // The denominator: with an empty cache the gate stays shut and nothing below means anything.
    assert!(
        warm > 0,
        "the cache held nothing after reading every key back, so eviction has nothing to work on"
    );

    let options = StorageManagerOptions {
        enable_evict: true,
        // A threshold far below what the fixture built, so one batch cannot possibly reach it and
        // the loop has a reason to keep going.
        eviction_memory_pressure_threshold: warm / 8,
        ..StorageManagerOptions::default()
    };
    let batch_limit = options.eviction_batch_limit;
    let report = runtime.run_storage_manager_once(1, options);
    let eviction = report
        .eviction
        .as_ref()
        .expect("the round ran the evict stage but carried no eviction report");

    assert!(
        eviction.pressure_gate_open,
        "eviction did not open its gate on a cache of {warm} bytes, so the loop never ran: {:?}",
        eviction.skipped_reason
    );
    assert!(
        eviction.selected_victims.len() > batch_limit,
        "the stage took {} victims with a batch limit of {batch_limit} -- it stopped after one \
         batch while the pressure was still {} against a threshold of {}",
        eviction.selected_victims.len(),
        eviction.pressure_after,
        eviction.memory_pressure_threshold,
    );
}

/// Does the PERIODIC loop reach compaction's wasted work? Prints.
///
///   cargo test -p temporalstore-rust --lib does_the_periodic_loop_reach_compaction \
///       -- --ignored --nocapture --test-threads=1
///
/// #1547 measured compaction never reaching a fixed point when called directly, and left one
/// question open: whether the PERIODIC loop ever gets there. Two fixtures failed to open the
/// stale-page gate and the answer was recorded as UNPROVEN.
///
/// This builds the condition deliberately instead of hoping for it. The gate wants a stale SLAB
/// and `stale_block_slab_pressure` is 1, but a slab is a GiB by default, so no fixture of a
/// reasonable size rolls one by writing. Rolling explicitly and then overwriting everything leaves
/// the first slab holding nothing live -- which is a stale slab, cheaply.
///
/// Then it runs rounds over a store nobody is writing to, and reports whether compaction keeps
/// firing. If it does, the direct-call waste is production behaviour and #1547's caveat can be
/// closed the other way.
#[test]
#[ignore]
fn does_the_periodic_loop_reach_compaction() {
    const KEYS: usize = 400;
    const ROUNDS: usize = 10;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    {
        let engine = runtime.engine();
        for index in 0..KEYS {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: format!("periodiccompact-{index:05}"),
                    field: "f".to_string(),
                    value: vec![b'v'; 64],
                },
            });
        }
        // Force a roll so the writes above sit on a slab that is no longer active, then rewrite
        // every key so that slab holds nothing live at all.
        engine
            .block_store()
            .roll_slab()
            .expect("rolling a slab should succeed");
        for index in 0..KEYS {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: format!("periodiccompact-{index:05}"),
                    field: "f".to_string(),
                    value: vec![b'w'; 72],
                },
            });
        }
    }

    eprintln!("  round  compact_ran  rewritten_page_refs  round_ms");
    let options = StorageManagerOptions::default();
    let mut rounds_that_compacted = 0usize;
    let mut total_rewritten = 0usize;
    for round in 0..ROUNDS {
        let started = Instant::now();
        let report = runtime.run_storage_manager_once(1, options.clone());
        let elapsed = started.elapsed().as_millis();
        let ran = report
            .executed_stages
            .iter()
            .any(|stage| stage == "compact_pages");
        let rewritten = report
            .compaction_report
            .as_ref()
            .map(|compaction| compaction.rewritten_object_pages)
            .unwrap_or(0);
        if ran {
            rounds_that_compacted += 1;
        }
        total_rewritten += rewritten;
        eprintln!("  {round:>5}  {ran:>11}  {rewritten:>19}  {elapsed:>8}");
    }
    // Does the collector take away the slab each round leaves behind? If it did, the stale
    // pressure would clear and the next round would have no reason to run. If the count has
    // climbed with the rounds, compaction is outrunning page GC and that is what sustains it.
    let slabs_at_end = runtime
        .engine()
        .block_store()
        .slab_ids()
        .map(|ids| ids.len())
        .unwrap_or(0);
    eprintln!(
        "  VERDICT: compacted on {rounds_that_compacted} of {ROUNDS} rounds, {total_rewritten} \
         page refs rewritten, over a store that stopped changing before round 0; \
         {slabs_at_end} slabs remain"
    );
    if rounds_that_compacted == 0 {
        eprintln!(
            "  NOTE: the gate still did not open, so this says nothing about the periodic path"
        );
    }
}

/// Does a bucket dump cost the BUCKETS it was asked for, or the whole shard? Prints.
///
///   cargo test -p temporalstore-rust --lib what_a_bucket_dump_costs \
///       -- --ignored --nocapture --test-threads=1
///
/// `create_bucket_dump_manifest` takes a list of buckets, and `max_dump_buckets_per_round` exists
/// to bound how many a round takes. But the manifest is built around `export_index_bytes(shard_id)`
/// -- the WHOLE shard index, serialised and hashed -- whatever that list contains.
///
/// The design being followed dumps per slot: `DumpSlotNotInMemory` reads that slot's marked pages
/// and metas out of the index and writes them without materialising the bucket, so a dump of one
/// slot costs one slot.
///
/// If the cost here is flat in the number of buckets, then bounding the round by bucket count
/// bounds the wrong thing, and it is the same shape as the expiry scan and the compaction scan:
/// the WORK is bounded and the SCAN is not.
#[test]
#[ignore]
fn what_a_bucket_dump_costs() {
    const KEYS: usize = 20_000;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for index in 0..KEYS {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("dumpcost-{index:08}"),
                value: vec![b'v'; 64],
            },
        });
    }

    // Which buckets exist, so the counts below ask for real ones.
    let all_buckets = engine
        .bucket_storage_summaries(1)
        .into_iter()
        .map(|summary| summary.routing_bucket)
        .collect::<Vec<_>>();
    eprintln!("  keys={KEYS} buckets={}", all_buckets.len());
    eprintln!("  buckets_asked   dump_ms");

    for count in [1usize, 8, 64, 512] {
        if count > all_buckets.len() {
            continue;
        }
        let selected = all_buckets.iter().copied().take(count).collect::<Vec<_>>();
        let started = Instant::now();
        let manifest = engine
            .create_bucket_dump_manifest(1, selected)
            .expect("dump should succeed");
        let elapsed = started.elapsed().as_millis();
        eprintln!("  {count:>13}   {elapsed:>7}");
        // Keep the manifest alive so the work is not optimised away.
        assert!(!manifest.manifest_id.is_empty());
    }
}

/// Does an index-GC round cost the LOG or the records it removes? Prints.
///
///   cargo test -p temporalstore-rust --lib what_an_index_gc_round_costs \
///       -- --ignored --nocapture --test-threads=1
///
/// `index_gc_max_entries_per_round` bounds how many index-log records a round REMOVES -- 256 by
/// default, reachable since #1524. What it does not bound is the read: the report opens with
/// `scan(shard_id, 0, u64::MAX, u64::MAX)`, the whole log, every round, and there is no cursor
/// anywhere to resume from.
///
/// The design being followed keeps a persistent `gc_scan_iterator_` and advances it with `Next()`
/// for `index_gc_max_num_per_round` entries, so a round reads what it is budgeted for and the next
/// round continues from there rather than starting over.
///
/// If the cost here grows with the LOG while the removal stays capped, this is the same shape as
/// the expiry walk (#1545), the compaction scan (#1547) and the whole-shard dump (#1559): the work
/// is bounded, the scan is not, and the knob counts the bounded half.
#[test]
#[ignore]
fn what_an_index_gc_round_costs() {
    eprintln!("  index_records   round_ms   removed");
    for writes in [2_000usize, 8_000, 32_000] {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        {
            let engine = runtime.engine();
            // Overwrite a bounded key space so records genuinely become garbage and the stage has
            // something to remove; with distinct keys almost every record is the live version of a
            // key and the removal would be zero for reasons that say nothing about scan cost.
            for index in 0..writes {
                engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: format!("idxscan-{:08}", index % 500),
                        value: vec![b'v'; 64],
                    },
                });
            }
        }
        let before = runtime
            .engine()
            .index_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or(0);

        // Only the index stage, so the number is this stage's and not a whole round's.
        let options = StorageManagerOptions {
            enable_prepare: false,
            enable_wal_reclaim: false,
            enable_memory_reclaim: false,
            enable_expire: false,
            enable_page_gc: false,
            enable_page_compaction: false,
            enable_metrics_reap: false,
            ..StorageManagerOptions::default()
        };
        let started = Instant::now();
        runtime.run_storage_manager_once(1, options);
        let elapsed = started.elapsed().as_millis();

        let after = runtime
            .engine()
            .index_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or(0);
        eprintln!(
            "  {before:>13}   {elapsed:>8}   {:>7}",
            before.saturating_sub(after)
        );
    }
}

/// What does the page-GC retain floor authorise, round by round? Prints.
///
///   cargo test -p temporalstore-rust --lib what_the_retain_floor_authorises \
///       -- --ignored --nocapture --test-threads=1
///
/// #1563 measured eleven slabs left after ten idle rounds: compaction rolls one a round and page
/// GC removes none, so `stale_page_pressure` never closes and compaction re-triggers for ever.
///
/// The floor is the first suspect. The periodic stage computes it as
///
///     stale_block_slab_ids.iter().min().saturating_add(1)
///
/// and `stale_block_slab_ids` is every slab NOT in the live set -- so the floor is derived FROM
/// the stale set. Taking the minimum and adding one authorises deleting slabs strictly below the
/// OLDEST stale slab, which is at most that one slab, however many are stale.
///
/// This prints, per round, how many slabs are stale, what floor that produces, and how many the
/// collector actually removed. If removed stays at zero while stale climbs, the floor is not the
/// whole story; if removed is one a round while compaction adds one, it is a treadmill.
#[test]
#[ignore]
fn what_the_retain_floor_authorises() {
    const KEYS: usize = 400;
    const ROUNDS: usize = 10;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    {
        let engine = runtime.engine();
        for index in 0..KEYS {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: format!("retainfloor-{index:05}"),
                    field: "f".to_string(),
                    value: vec![b'v'; 64],
                },
            });
        }
        engine
            .block_store()
            .roll_slab()
            .expect("rolling a slab should succeed");
        for index in 0..KEYS {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: format!("retainfloor-{index:05}"),
                    field: "f".to_string(),
                    value: vec![b'w'; 72],
                },
            });
        }
    }

    eprintln!("  round  slabs  stale  floor  gc_ran  removed");
    let options = StorageManagerOptions::default();
    for round in 0..ROUNDS {
        let plan = runtime
            .engine()
            .storage_lifecycle_plan(crate::engine::reports::StorageLifecycleRequest {
                shard_id: 1,
                ..Default::default()
            });
        let stale = plan.stale_block_slab_ids.len();
        let floor = plan
            .stale_block_slab_ids
            .iter()
            .min()
            .map(|id| id.saturating_add(1))
            .unwrap_or(0);
        let slabs = runtime
            .engine()
            .block_store()
            .slab_ids()
            .map(|ids| ids.len())
            .unwrap_or(0);

        let report = runtime.run_storage_manager_once(1, options.clone());
        let gc_ran = report
            .executed_stages
            .iter()
            .any(|stage| stage == "reclaim_page");
        let removed = report
            .gc_report
            .as_ref()
            .map(|gc| gc.block_slabs_removed)
            .unwrap_or(0);
        eprintln!("  {round:>5}  {slabs:>5}  {stale:>5}  {floor:>5}  {gc_ran:>6}  {removed:>7}");
    }
}

/// Why does a stale slab below the retain floor survive? Prints.
///
///   cargo test -p temporalstore-rust --lib why_a_stale_slab_below_the_floor_survives \
///       -- --ignored --nocapture --test-threads=1
///
/// #1564 measured the page-GC retain floor freezing at 2 while stale slabs climbed to nine and the
/// collector removed nothing. The floor freezes because it is `min(stale) + 1`, so it cannot rise
/// past the oldest stale slab -- but that only explains the freeze if the oldest stale slab is
/// itself unremovable, and it sits BELOW the floor, which the formula does authorise.
///
/// `gc_slabs_before_with_live_refs_selected` keeps a slab below the floor for exactly two reasons,
/// and the report names both: it is the CURRENT slab, or it is LIVE. And the periodic stage widens
/// "live" beyond pages -- `run_gc_inner` adds every slab named by a bucket dump manifest:
///
///     for manifest in inner.engine.list_bucket_dump_manifests(request.shard_id) {
///         live_block_slab_ids.extend(manifest.block_slab_ids.iter().copied());
///     }
///
/// So a manifest written before compaction relocated the pages still names the OLD slabs. This
/// separates the three: live-by-page, live-by-manifest, and current.
#[test]
#[ignore]
fn why_a_stale_slab_below_the_floor_survives() {
    const KEYS: usize = 400;
    const ROUNDS: usize = 6;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    {
        let engine = runtime.engine();
        for index in 0..KEYS {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: format!("whysurvive-{index:05}"),
                    field: "f".to_string(),
                    value: vec![b'v'; 64],
                },
            });
        }
        engine
            .block_store()
            .roll_slab()
            .expect("rolling a slab should succeed");
        for index in 0..KEYS {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: format!("whysurvive-{index:05}"),
                    field: "f".to_string(),
                    value: vec![b'w'; 72],
                },
            });
        }
    }

    eprintln!("  round  slabs  floor  live_by_page  live_by_manifest  manifests  removed  ret_live  ret_current");
    let options = StorageManagerOptions::default();
    for round in 0..ROUNDS {
        let engine = runtime.engine();
        let plan = engine.storage_lifecycle_plan(crate::engine::reports::StorageLifecycleRequest {
            shard_id: 1,
            ..Default::default()
        });
        let floor = plan
            .stale_block_slab_ids
            .iter()
            .min()
            .map(|id| id.saturating_add(1))
            .unwrap_or(0);
        let live_by_page = engine.live_block_slab_ids_all_shards();
        let manifests = engine.list_bucket_dump_manifests(1);
        let live_by_manifest = manifests
            .iter()
            .flat_map(|manifest| manifest.block_slab_ids.iter().copied())
            .collect::<std::collections::BTreeSet<_>>();
        let slabs = engine
            .block_store()
            .slab_ids()
            .map(|ids| ids.len())
            .unwrap_or(0);

        let report = runtime.run_storage_manager_once(1, options.clone());
        let gc = report.gc_report.as_ref();
        let removed = gc.map(|g| g.block_slabs_removed).unwrap_or(0);
        let ret_live = gc.map(|g| g.block_slabs_retained_live).unwrap_or(0);
        eprintln!(
            "  {round:>5}  {slabs:>5}  {floor:>5}  {:>12}  {:>16}  {:>9}  {removed:>7}  {ret_live:>8}  {:>11}",
            live_by_page.len(),
            live_by_manifest.len(),
            manifests.len(),
            "-"
        );
    }
}

/// An idle shard stops accumulating slabs: the collector keeps pace with compaction.
///
/// The page-GC retain floor is a MIN over the stale slabs, and `run_gc_inner` treats every slab
/// named by a bucket dump manifest as live. So once a dump exists, the slab it names is stale (its
/// pages have moved) and retained (the manifest needs it) -- and `min` sat on that slab for ever.
/// The floor never advanced, every slab compaction rolled afterwards was above it, and the store
/// grew a slab a round on a shard nobody was writing to: measured at eleven slabs after ten rounds,
/// with the collector removing nothing after the first.
///
/// Same shape as #1516, where a reclaim frontier taken as a min over every bucket was pinned by one
/// clean bucket. The floor now takes its min over stale slabs that are NOT manifest-pinned, so it
/// advances past them; they stay protected by the `is_live` check that was already refusing them.
///
/// This asserts the property that failed: an idle shard reaches a steady slab count instead of
/// climbing. It writes NOTHING after the fixture, so any growth is the loop's own doing.
#[test]
fn an_idle_shard_stops_accumulating_slabs() {
    const KEYS: usize = 400;
    const ROUNDS: usize = 10;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    {
        let engine = runtime.engine();
        for index in 0..KEYS {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: format!("idleslabs-{index:05}"),
                    field: "f".to_string(),
                    value: vec![b'v'; 64],
                },
            });
        }
        // Roll, then rewrite everything: the first slab holds nothing live, which is a stale slab
        // for a few hundred records instead of the gigabyte a natural roll would need.
        engine
            .block_store()
            .roll_slab()
            .expect("rolling a slab should succeed");
        for index in 0..KEYS {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: format!("idleslabs-{index:05}"),
                    field: "f".to_string(),
                    value: vec![b'w'; 72],
                },
            });
        }
    }

    let slabs_before = runtime
        .engine()
        .block_store()
        .slab_ids()
        .map(|ids| ids.len())
        .unwrap_or(0);
    let options = StorageManagerOptions::default();
    let mut compacted_rounds = 0usize;
    for _ in 0..ROUNDS {
        let report = runtime.run_storage_manager_once(1, options.clone());
        if report
            .executed_stages
            .iter()
            .any(|stage| stage == "compact_pages")
        {
            compacted_rounds += 1;
        }
    }
    let slabs_after = runtime
        .engine()
        .block_store()
        .slab_ids()
        .map(|ids| ids.len())
        .unwrap_or(0);

    // The denominator: if compaction never ran it rolled no slabs, and a flat count would say
    // nothing about whether the collector can keep up with it.
    assert!(
        compacted_rounds > 0,
        "compaction never ran, so nothing rolled a slab and this measures an idle collector"
    );
    // A slab a round would be {ROUNDS} more. Steady state is a small constant; the bound is
    // deliberately loose so this fails on GROWTH, not on one slab of slack.
    assert!(
        slabs_after <= slabs_before + 2,
        "an idle shard grew from {slabs_before} to {slabs_after} slabs over {ROUNDS} rounds \
         ({compacted_rounds} of them compacting) -- the collector is not keeping pace, so the \
         retain floor has stopped advancing again"
    );
}
