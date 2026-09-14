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
        ..options.clone()
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
        ..options.clone()
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
            purge_delayed_destroy_slab_ids: None,
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

/// A retention cursor handed to the loop a server actually starts must reach all three decisions.
///
/// The engine has honoured these cursors for some time, but only as ARGUMENTS.
/// `StorageManagerCycleRequest` carries the two lists and is reachable over the wire
/// (`POST /storage_manager/cycle`); `StorageManagerOptions` carried neither, and
/// `start_storage_manager_scheduler_for_all_shards` -- the loop server.rs starts unconditionally
/// every 30 s -- takes `StorageManagerOptions`. So the default entry point was the one that could
/// not be told about a reader, and all four of its lifecycle requests plus both of its plan calls
/// passed `Vec::new()` because there was nothing else to pass.
///
/// THE THREE HALVES ARE ASSERTED SEPARATELY, each behind its own denominator, because they are
/// three decisions reached in three files and they are not equally reversible. The write-ahead log
/// floor (`storage_wal_reclaim_plan` -> `gc_before_sequence`) drops records; the index-log floor
/// (`apply_periodic_index_gc` -> `gc_before_sequence_limited`) drops index-log records; the prune
/// (`bucket_dump_manifest_prune_plan_at`) deletes DUMPS, and a deleted dump is the thing a node
/// rebuilt through `POST /server/storage/dumps/install` would have restored from -- unlike a
/// reclaimed log record, nothing regenerates it.
///
/// A count across them reads full on any one, which is exactly the shape that let this sit unfed:
/// the mechanism was whole and well covered at the engine's own entry. The third half is here
/// because the mutation run PROVED it was needed -- with two halves this guard passed with
/// `apply_periodic_index_gc` reverted, one fix live and one silently unguarded.
///
/// Each half carries a CONTROL arm with no cursor, so "the cursor held it" cannot be satisfied by
/// a round that simply did no work.
#[test]
fn a_retention_cursor_reaches_the_loop_a_server_actually_starts() {
    fn build(dir: &std::path::Path, records: usize) -> TemporalEngine {
        let engine = TemporalEngine::with_local_dirs(
            1 << 20,
            dir.join("cache"),
            dir.join("pages"),
            dir.join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..records {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("retention-{index:06}"),
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
    // Sequence 1 in both logs: behind everything this fixture writes, and still above zero, so the
    // plan clamps to it rather than refusing outright (a zero floor reclaims nothing and would
    // make this pass for the wrong reason).
    fn pinned() -> StorageManagerOptions {
        StorageManagerOptions {
            follower_replay_cursors: vec![crate::engine::reports::BucketDumpFollowerReplayCursor {
                follower_id: "reader-held-behind".to_string(),
                shard_id: 1,
                wal_sequence: 1,
                index_log_sequence: 1,
            }],
            ..StorageManagerOptions::default()
        }
    }

    let records = StorageManagerOptions::default().min_undumped_wal_records as usize + 256;

    // ---- HALF ONE: the write-ahead log floor. Its own denominator and its own control. ----
    fn wal_records(engine: &TemporalEngine) -> usize {
        engine
            .write_ahead_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .expect("scan")
            .len()
    }

    let wal_control_dir = tempfile::tempdir().unwrap();
    let wal_control = build(wal_control_dir.path(), records);
    let wal_before = wal_records(&wal_control);
    assert!(
        wal_before > 0,
        "DENOMINATOR: the fixture must leave write-ahead log records to reclaim, got {wal_before}"
    );
    let wal_control = run(wal_control, StorageManagerOptions::default());
    let wal_control_after = wal_records(&wal_control);
    assert!(
        wal_control_after < wal_before,
        "CONTROL: with no cursor the loop must reclaim this log, or the treatment below proves \
         nothing: {wal_before} records before, {wal_control_after} after"
    );

    let wal_pinned_dir = tempfile::tempdir().unwrap();
    let wal_pinned = build(wal_pinned_dir.path(), records);
    let wal_pinned_before = wal_records(&wal_pinned);
    let wal_pinned = run(wal_pinned, pinned());
    let wal_pinned_after = wal_records(&wal_pinned);
    assert!(
        wal_pinned_after > wal_control_after,
        "a cursor at sequence 1 must hold the log the uncursored arm freed: cursored kept \
         {wal_pinned_after} of {wal_pinned_before}, uncursored kept {wal_control_after} of \
         {wal_before}"
    );

    // ---- HALF TWO: the bucket-dump manifest prune. Its own denominator and its own control. ----
    //
    // This half is the one the log counts above cannot see. It is reached through a DIFFERENT
    // request field on a DIFFERENT stage (`reclaim_index`, the only one of the four carrying
    // `prune_bucket_dump_manifests: true`), so half one can pass with this still unfed.
    fn manifest_count(engine: &TemporalEngine) -> usize {
        engine.list_bucket_dump_manifests(1).len()
    }
    fn build_with_dumps(dir: &std::path::Path) -> TemporalEngine {
        let engine = build(dir, 64);
        // Several generations, so there are older dumps for a prune to take. Each write dirties
        // the same bucket again, so each manifest sits above the last.
        for generation in 0..3 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "retention-anchor".to_string(),
                    value: format!("generation-{generation}").into_bytes(),
                },
            });
            engine
                .create_bucket_dump_manifest(1, Vec::new())
                .expect("fixture dump manifest");
        }
        engine
    }

    let prune_control_dir = tempfile::tempdir().unwrap();
    let prune_control = build_with_dumps(prune_control_dir.path());
    let prune_before = manifest_count(&prune_control);
    assert!(
        prune_before >= 2,
        "DENOMINATOR: pruning is vacuous without older dumps to take, got {prune_before}"
    );
    let prune_control = run(prune_control, StorageManagerOptions::default());
    let prune_control_after = manifest_count(&prune_control);
    assert!(
        prune_control_after < prune_before,
        "CONTROL: with no cursor the loop must prune older dumps, or the treatment below proves \
         nothing: {prune_before} manifests before, {prune_control_after} after"
    );

    let prune_pinned_dir = tempfile::tempdir().unwrap();
    let prune_pinned = build_with_dumps(prune_pinned_dir.path());
    let prune_pinned_before = manifest_count(&prune_pinned);
    let prune_pinned = run(prune_pinned, pinned());
    let prune_pinned_after = manifest_count(&prune_pinned);
    assert!(
        prune_pinned_after > prune_control_after,
        "a cursor behind every dump must keep the one it would restore from: cursored kept \
         {prune_pinned_after} of {prune_pinned_before}, uncursored kept {prune_control_after} of \
         {prune_before}"
    );

    // ---- HALF THREE: the index-log floor, which neither count above can see. ----
    //
    // `apply_periodic_index_gc` is handed a `StorageLifecycleRequest` that CARRIES both lists and
    // built its reclaim plan with an empty pair anyway, so a caller that supplied a cursor had it
    // honoured by the prune in the same request and dropped for the truncation. One request, two
    // answers, disagreeing about who is still reading.
    //
    // Asserted on the FLOOR the report carries rather than on bytes removed. The floor is the
    // decision -- `storage_index_gc_report` hands it straight to `gc_before_sequence_limited` --
    // and it is reported even when the shipped 768 KiB byte gate declines, so this needs no
    // 16,000-record fixture to be meaningful. Halves one and two both passed with this reverted,
    // which is why it needs its own arm rather than a shared count.
    let floor_dir = tempfile::tempdir().unwrap();
    let floor_engine = build_with_dumps(floor_dir.path());
    fn index_gc_request(
        cursors: Vec<crate::engine::reports::BucketDumpFollowerReplayCursor>,
    ) -> crate::engine::reports::StorageLifecycleRequest {
        crate::engine::reports::StorageLifecycleRequest {
            shard_id: 1,
            purge_delayed_destroy: true,
            prune_bucket_dump_manifests: true,
            roll_forward_bucket_dump_installs: true,
            follower_replay_cursors: cursors,
            ..crate::engine::reports::StorageLifecycleRequest::default()
        }
    }
    let uncursored_floor = floor_engine
        .apply_periodic_index_gc(
            index_gc_request(Vec::new()),
            None,
            crate::engine::reports::DEFAULT_INDEX_GC_MAX_ENTRIES_PER_ROUND,
        )
        .retain_from_index_log_sequence;
    assert!(
        uncursored_floor > 2,
        "DENOMINATOR: with no cursor the floor must sit above the sequence the cursor below names, \
         or clamping to it cannot show: {uncursored_floor}"
    );
    let cursored_floor = floor_engine
        .apply_periodic_index_gc(
            index_gc_request(vec![
                crate::engine::reports::BucketDumpFollowerReplayCursor {
                    follower_id: "reader-held-behind".to_string(),
                    shard_id: 1,
                    wal_sequence: 1,
                    index_log_sequence: 1,
                },
            ]),
            None,
            crate::engine::reports::DEFAULT_INDEX_GC_MAX_ENTRIES_PER_ROUND,
        )
        .retain_from_index_log_sequence;
    assert!(
        cursored_floor < uncursored_floor,
        "the cursor on the request must clamp the index-log floor it is handed: cursored floor \
         {cursored_floor}, uncursored floor {uncursored_floor}"
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
                purge_delayed_destroy_slab_ids: None,
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
            purge_delayed_destroy_slab_ids: None,
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

/// How many times does ONE maintenance round materialize EVERY live page? Prints.
///
///   cargo test --release -p temporalstore-rust --lib how_many_times_a_round_walks_every_live_page -- --ignored --nocapture
///
/// `collect_live_page_entries` builds a fresh `Vec<LiveBlockEntry>` holding every live page in the
/// shard. It has roughly twenty call sites, and a single periodic round reaches many of them:
/// `storage_wal_reclaim_plan` (via `bucket_storage_summaries`), `storage_lifecycle_plan`, the
/// compaction preamble, eviction victim selection, dump-manifest creation. None of them shares a
/// result with the next.
///
/// This is why the stage timings look the way they do. `what_the_index_gc_gate_costs` attributes
/// the index-GC stage as roughly 60% `storage_wal_reclaim_plan` and 30% `storage_lifecycle_plan`
/// at every corpus size, both scaling linearly with the record count -- 234 ms and 120 ms
/// respectively at 16,000 records, on a loop whose period is 30 s. Neither number is bounded by
/// anything the round was asked to do.
///
/// The multiplier below is the thing to fix, and it is the honest way to size it: not "this stage
/// is slow" but "the round walks the whole shard N times, and N is a property of how the stages
/// are wired rather than of how much work the round was asked to do".
#[test]
#[ignore]
fn how_many_times_a_round_walks_every_live_page() {
    for records in [2_000usize, 8_000] {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for index in 0..records {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("walks-{index:06}"),
                    value: vec![b'v'; 64],
                },
            });
            assert!(response.status.ok, "write {index}: {:?}", response.status);
        }

        // Measured BEFORE the runtime takes the engine, and before the counter is reset, so this
        // report's own walk is not counted against the round.
        let live_pages: u64 = engine
            .bucket_storage_summaries(1)
            .iter()
            .map(|summary| summary.page_ref_count as u64)
            .sum();

        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );

        crate::engine::reset_live_page_scan_entries();
        let started = std::time::Instant::now();
        let report = runtime.run_storage_manager_once(1, StorageManagerOptions::default());
        let round_ms = started.elapsed().as_micros() as f64 / 1000.0;
        let scanned = crate::engine::live_page_scan_entries();

        // Denominators. A shard with no live pages, or a round that walked nothing, would make
        // the ratio below meaningless rather than zero.
        assert!(live_pages > 0, "fixture stored no live pages");
        assert!(
            scanned > 0,
            "the round materialized no live-page entries, so it never reached the stages this measures",
        );

        eprintln!(
            "  [walks] {records:>6} records, {live_pages:>6} live pages -> round {round_ms:>8.1} ms \
materialized {scanned:>8} live-page entries = {:>5.1}x the shard, stages {:?}",
            scanned as f64 / live_pages as f64,
            report.executed_stages,
        );
    }
}

/// WHICH stages do the walking? Attributes the round's live-page scan volume per stage. Prints.
///
///   cargo test --release -p temporalstore-rust --lib which_stages_walk_every_live_page -- --ignored --nocapture
///
/// `how_many_times_a_round_walks_every_live_page` establishes that one round materializes every
/// live page ~35x. That number says a fix is worth doing but not where to apply it. This names the
/// stages, by toggling each one off and diffing the scan counter -- the same subtract-one-stage
/// shape `what_each_maintenance_stage_costs` uses for time, so the two can be read together.
///
/// Read the DELTA (all-on minus without-this-stage), not the without-column. A stage that shares
/// its walks with another stage will under-report here, because switching it off leaves the other
/// one still walking -- so the deltas are a lower bound per stage and need not sum to the total.
/// That is a property worth seeing rather than hiding: where the deltas fall well short of the
/// total, the walking is SHARED, and hoisting one materialization helps more than removing any
/// single stage would.
#[test]
#[ignore]
fn which_stages_walk_every_live_page() {
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
                    key: format!("stage-scan-{index:06}"),
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

    fn scan_once(objects: usize, options: StorageManagerOptions) -> (u64, Vec<String>) {
        let (_dir, runtime) = build(objects);
        crate::engine::reset_live_page_scan_entries();
        let report = runtime.run_storage_manager_once(1, options);
        (crate::engine::live_page_scan_entries(), report.executed_stages)
    }

    for objects in [4_000usize] {
        let (all_on, stages) = scan_once(objects, StorageManagerOptions::default());
        assert!(
            all_on > 0,
            "the round materialized nothing, so this attributes nothing",
        );

        // WHY the two stages below walk so much: they rebuild the same plans. Both counters
        // already exist for exactly this question, and a COUNT is immune to whatever else is
        // running on the box, where a timing would not be.
        let (_dir, runtime) = build(objects);
        crate::engine::reset_storage_plan_build_counts();
        let _ = runtime.run_storage_manager_once(1, StorageManagerOptions::default());
        let (lifecycle_builds, wal_builds) = crate::engine::storage_plan_build_counts();
        eprintln!(
            "  [stage-scan] {objects:>6}   ONE round builds the lifecycle plan {lifecycle_builds} \
time(s) and the WAL reclaim plan {wal_builds} time(s); each walks the shard"
        );
        eprintln!(
            "  [stage-scan] {objects:>6} objects, everything on -> {all_on:>9} live-page entries \
= {:>5.1}x the shard, stages {stages:?}",
            all_on as f64 / objects as f64,
        );

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
            // A stage that did not RUN in the all-on round contributes 0 here, and that zero
            // reads exactly like "this stage does no walking" while meaning "this row measured
            // nothing". Say which it is. `compact_pages` is the one that matters: it does not run
            // on this fixture, yet its preamble is six whole-shard passes when it does.
            let ran = stages.iter().any(|stage| stage == name);
            let (without, _) = scan_once(objects, off);
            eprintln!(
                "  [stage-scan] {objects:>6}   without {name:<15} {without:>9} \
(stage accounts for {:>9} entries, {:>5.1}x the shard){}",
                all_on.saturating_sub(without),
                all_on.saturating_sub(without) as f64 / objects as f64,
                if ran { "" } else { "   <- DID NOT RUN on this fixture; the zero measures nothing" },
            );
        }

        // `enable_evict` is the ONE stage flag that defaults to false (manual `Default` impl, with
        // a comment saying so), so a subtract-one row for it would toggle nothing and report a
        // false zero. Measure it the other way round: what turning it ON adds.
        let (with_evict, evict_stages) = scan_once(
            objects,
            StorageManagerOptions { enable_evict: true, ..Default::default() },
        );
        eprintln!(
            "  [stage-scan] {objects:>6}   evict is OFF by default; ON -> {with_evict:>9} \
(adds {:>9} entries, {:>5.1}x the shard), stages {evict_stages:?}",
            with_evict.saturating_sub(all_on),
            with_evict.saturating_sub(all_on) as f64 / objects as f64,
        );
    }
}

/// FOOTPRINT CADENCE: does every subsystem reach a steady state, or keep growing? Prints.
///
///   cargo test --release -p temporalstore-rust --lib the_footprint_cadence_over_many_rounds -- --ignored --nocapture
///
/// This is the shared measurement spine for the per-subsystem scale work. One fixture, many
/// maintenance rounds, and after EACH round every subsystem's footprint is recorded on one line,
/// so the question "is the cadence good" is answered by reading a column rather than by running
/// ten separate experiments that cannot be compared to each other.
///
/// WHAT A GOOD COLUMN LOOKS LIKE: it rises while the shard fills and then FLATTENS. A column that
/// climbs every round on a fixture that stops writing is the #1565 shape -- an idle shard grew a
/// slab every thirty seconds for ever, eleven after ten rounds, because a `min` over a set with an
/// immovable member froze the reclaim floor. That defect was invisible in any single round and
/// obvious in a column.
///
/// The write phase stops before the rounds begin ON PURPOSE. Growth under continuing writes is
/// expected and says nothing; growth with no writer is the signal.
///
/// Columns, and which subsystem each belongs to:
///
///   wal_bytes / wal_seq   WAL              -- persistent bytes and the sequence, so reclaim shows
///   idx_bytes             index log        -- file length; the collector should flatten it
///   slabs / slab_bytes    page/block store -- slab count is the #1565 signal
///   cache_mem             eviction         -- the only memory the evict gate can see
///   bkt_idx               (memory)         -- bucket index entries: never evicted, expect FLAT
///   manifests             dump             -- retained manifests; the prune policy should bound
///   dirty                 store manager    -- dirty buckets; should fall to 0 with no writer
#[test]
#[ignore]
fn the_footprint_cadence_over_many_rounds() {
    const RECORDS: usize = 8_000;
    const ROUNDS: usize = 12;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for index in 0..RECORDS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("cadence-{index:07}"),
                value: vec![b'v'; 128],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    // Overwrite a third, so there is genuine garbage for the collectors to reclaim. Without this
    // every collector correctly does nothing and a flat column proves only that nothing happened.
    for index in 0..RECORDS / 3 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("cadence-{index:07}"),
                value: vec![b'w'; 192],
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
    let engine = runtime.engine();

    eprintln!(
        "  [cadence] {RECORDS} records written, then NO further writes -- any column that keeps \
climbing below is growth with no writer"
    );
    eprintln!(
        "  [cadence] {:>5} {:>11} {:>9} {:>6} {:>11} {:>10} {:>8} {:>9} {:>6}  stages",
        "round", "wal_bytes", "idx_bytes", "slabs", "slab_bytes", "cache_mem", "bkt_idx",
        "manifests", "dirty",
    );

    let mut first = None;
    let mut last = None;
    for round in 0..ROUNDS {
        let report = runtime.run_storage_manager_once(1, StorageManagerOptions::default());

        let wal = engine.write_ahead_log_store().stats(1);
        let idx_bytes = engine.index_log_store().log_len_bytes(1);
        let slab_ids = engine.page_store().slab_ids().unwrap_or_default();
        let slab_bytes: u64 = engine
            .page_store()
            .slab_block_counts()
            .unwrap_or_default()
            .iter()
            .map(|(_id, physical_bytes, _count)| *physical_bytes)
            .sum();
        let cache_mem = engine.cache().stats().memory_bytes;
        let summaries = engine.bucket_storage_summaries(1);
        let bkt_idx = summaries.len() as u64;
        let dirty = summaries
            .iter()
            .filter(|summary| summary.dirty_object_count > 0)
            .count();
        let manifests = engine.list_bucket_dump_manifests(1).len();

        let row = (
            wal.persistent_bytes,
            idx_bytes,
            slab_ids.len() as u64,
            slab_bytes,
            cache_mem,
            bkt_idx,
            manifests as u64,
        );
        if round == 0 {
            first = Some(row);
        }
        last = Some(row);

        eprintln!(
            "  [cadence] {round:>5} {:>11} {idx_bytes:>9} {:>6} {slab_bytes:>11} {cache_mem:>10} \
{bkt_idx:>8} {manifests:>9} {dirty:>6}  {:?}",
            wal.persistent_bytes,
            slab_ids.len(),
            report.executed_stages,
        );
    }

    let (first, last) = (first.expect("a round ran"), last.expect("a round ran"));
    // Denominator: a fixture where nothing was stored would print zeros in every column and every
    // "did not grow" assertion below would hold vacuously.
    assert!(
        last.5 > 0,
        "the shard holds no buckets, so every column below is trivially flat",
    );

    eprintln!(
        "  [cadence] first -> last:  wal {} -> {}   idx {} -> {}   slabs {} -> {}   cache {} -> {}",
        first.0, last.0, first.1, last.1, first.2, last.2, first.4, last.4,
    );
    eprintln!(
        "  [cadence] READ THE COLUMNS, not this line: a subsystem whose column climbs to the last \
round with no writer is the one to open a thread on."
    );

    // WHICH STAGE GROWS THE INDEX LOG? The column above climbs every round on a shard nobody is
    // writing to. `compact_pages` also runs every round on that shard, and compaction relocates
    // pages and persists the resulting index -- so the obvious suspect is that the index log is
    // growing because compaction keeps rewriting it, not because anything changed.
    //
    // Obvious is not measured. A second fixture, identical except compaction is OFF, settles it:
    // if the growth persists with compaction disabled the suspect is wrong.
    let quiet = TemporalEngine::default();
    quiet.load_shard(1);
    for index in 0..RECORDS {
        quiet.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("cadence-{index:07}"),
                value: vec![b'v'; 128],
            },
        });
    }
    for index in 0..RECORDS / 3 {
        quiet.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("cadence-{index:07}"),
                value: vec![b'w'; 192],
            },
        });
    }
    let quiet_runtime = DataNodeRuntime::new_without_workers_with_options(
        quiet,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    let quiet_engine = quiet_runtime.engine();
    let no_compaction = StorageManagerOptions {
        enable_page_compaction: false,
        ..StorageManagerOptions::default()
    };
    let mut quiet_first = 0u64;
    let mut quiet_last = 0u64;
    for round in 0..ROUNDS {
        let _ = quiet_runtime.run_storage_manager_once(1, no_compaction.clone());
        let idx = quiet_engine.index_log_store().log_len_bytes(1);
        if round == 0 {
            quiet_first = idx;
        }
        quiet_last = idx;
    }

    let with_compaction_growth = last.1.saturating_sub(first.1);
    let without_compaction_growth = quiet_last.saturating_sub(quiet_first);
    eprintln!(
        "  [cadence] index-log growth over {} rounds with NO writer: compaction ON {} bytes, \
compaction OFF {} bytes",
        ROUNDS - 1,
        with_compaction_growth,
        without_compaction_growth,
    );

    // Denominator: if the compaction-on arm did not grow either, this comparison is measuring
    // nothing and the attribution below would be read off two zeros.
    assert!(
        with_compaction_growth > 0,
        "the index log did not grow even with compaction on, so this arm attributes nothing",
    );
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

/// Can the page-GC garbage floor ever exclude a slab? It cannot, and this now SAYS so.
///
///   cargo test -p temporalstore-rust --lib can_the_page_gc_garbage_floor_bind \
///       -- --nocapture --test-threads=1
///
/// It asked the question and printed the answer, but it was `#[ignore]`d, so CI never ran it and
/// the answer was never recorded anywhere that could fail. A shipped default of 4,000 basis
/// points that cannot exclude anything is exactly the shape that gets "fixed" by being raised,
/// which changes nothing and costs someone an afternoon.
///
/// So it still prints the table -- that is the useful part when this eventually changes -- and it
/// now ASSERTS the invariant behind it: every candidate reports 0 used bytes and therefore 10,000
/// basis points of garbage, and the floor excludes none of them. The reason is structural. The
/// floor is compared against a slab's live fraction; a slab's used bytes sum the slabs grouped
/// under its stored id that are NOT collectable; that group is always the slab itself; and
/// a candidate is by definition not current and not live. The candidate filter is the exact
/// negation of the used-bytes filter, so a candidate can never contribute to its own used
/// bytes.
///
/// This is NOT waiting for a stored id to group several slabs. It is waiting for used bytes to mean
/// live PAGE bytes within the slab instead of whole file sizes of neighbouring slabs. When that
/// lands, this test fails -- and that failure is the signal that the knob has become real, which
/// is why the assertions name what they depend on.
#[test]
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
                crate::engine::reports::DEFAULT_PAGE_GC_MIN_SLAB_GARBAGE_BASIS_POINTS,
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
        let kept = garbage < crate::engine::reports::DEFAULT_PAGE_GC_MIN_SLAB_GARBAGE_BASIS_POINTS;
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

    // THE DENOMINATOR FIRST. A floor that excluded nothing because there was nothing to exclude
    // would satisfy every assertion below while saying nothing at all, and that is the failure
    // mode this test spent its whole life in: ignored, so zero candidates and zero exclusions
    // were indistinguishable from a working floor.
    assert!(
        plan.candidate_count > 0,
        "no candidates, so this measures nothing about the floor: {plan:?}"
    );
    assert!(
        crate::engine::reports::DEFAULT_PAGE_GC_MIN_SLAB_GARBAGE_BASIS_POINTS > 0,
        "a floor of zero excludes nothing by definition and would make this vacuous"
    );

    for candidate in plan.candidates.iter() {
        assert_eq!(
            candidate.used_bytes, 0,
            "a candidate's band cannot contribute to its own used bytes -- the candidate filter \
             is the exact negation of the used-bytes filter, and a band holds one slab: \
             {candidate:?}"
        );
        assert_eq!(
            candidate.utility_basis_points, 0,
            "so its live fraction is zero: {candidate:?}"
        );
    }
    assert_eq!(
        plan.skipped_by_policy_count, 0,
        "every candidate is 10,000 bp garbage, so the shipped floor excludes none of them; if \
         this now fails, used bytes have started to mean live page bytes within the slab and the \
         knob has become real: {plan:?}"
    );
    assert_eq!(
        plan.selected_block_slab_ids.len(),
        plan.candidate_count,
        "and every candidate is selected: {plan:?}"
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
/// The ceiling WAS the MODE, and it has moved. With `eviction_delete_drop` false -- the shipped
/// default, mode `evict_cache` -- a victim used to be handled by
/// `cache.invalidate_slot(shard_id, routing_bucket)` and nothing else, so eviction could free
/// exactly what was CACHED. Once the cached pages of the eligible buckets were gone another batch
/// freed nothing, `cooldown` was set, and the loop correctly stopped -- below the cached set, with
/// every bucket node still whole. Raising the budget, changing the sampler or looping harder could
/// not move it, because the missing piece was neither pacing nor selection but a per-bucket
/// load-back path: nothing could drop a bucket node and expect to read it again.
///
/// That path exists now -- `release_bucket_pages` / `reload_released_bucket` -- and `evict_cache`
/// uses it: a victim is dumped, cleared, and has its page list RELEASED, while the node stays
/// routable and the next write through it loads the list back from the model maps. The gate counts
/// the resident bucket index as part of its pressure now, so a round can reduce the thing that
/// actually grows with the corpus. What this measurement is for has changed with it: the question
/// is no longer why it cannot converge but how many rounds it takes to.
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
        // Roll, then rewrite HALF: the first slab is left holed -- some live pages, some dead --
        // which is a stale slab for a few hundred records instead of the gigabyte a natural roll
        // would need, and is a slab compaction can still do something about.
        //
        // It used to rewrite ALL of them, leaving the first slab holding nothing live. That is a
        // slab only the COLLECTOR can act on: no object has a page there, so there is nothing for
        // compaction to relocate off it, and the relocation hint now correctly declines the round.
        // The assertion this test exists for is unchanged; its denominator
        // (`compacted_rounds > 0`) is what stopped being reachable on the old fixture.
        engine
            .block_store()
            .roll_slab()
            .expect("rolling a slab should succeed");
        for index in 0..KEYS / 2 {
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
/// The slab compaction vacated is COLLECTED, so an idle shard's candidate count returns to zero.
///
/// THE DEFECT THIS HOLDS SHUT, as measured. #1627 stopped compaction re-triggering itself and
/// reported this half open: once the drain settles, the emptied slab is never destroyed. In every
/// settled round, at 8,000 records and again at 80,000:
///
///     slabs 2, stale 1, reclaim_candidates 1, relocatable 0
///
/// Nothing grows, which is why it was correctly separated -- but the shard keeps a dead slab's
/// bytes for ever and the candidate count never returns to zero, so any gate keying on it reads a
/// permanently-open condition.
///
/// WHY IT SURVIVED, and it is not the retain floor. The collector RAN in every one of those
/// rounds and refused: `run_gc_inner` holds back every slab a bucket dump manifest names, and the
/// newest manifest still named the slab compaction had emptied. #1565 stopped exactly such a slab
/// freezing the retain FLOOR and said in as many words that it does not make the slab deletable.
/// A manifest is only ever replaced by a DUMP, a dump is selected from DIRTY buckets, and an idle
/// shard has none -- so the pin was permanent. `storage_lifecycle_plan` now asks for that dump,
/// which is the one place both copies of the maintenance round read.
///
/// WHAT THIS ASSERTS, IN ORDER. Both denominators first, because either one makes every later
/// assertion pass for a reason that has nothing to do with the defect: compaction must have run,
/// and a slab must actually have gone stale inside the window. Then the property -- no vacated
/// slab is left standing -- which is what a reverted fix breaks. Then by which mechanism, and
/// WHERE the bytes went: quarantine, not an unlink, which is the delayed-destroy contract the
/// reclaim stage is written to.
///
/// THE PURGE NEEDS ITS AGE INJECTED. Quarantine enforces a ONE HOUR minimum, so a fixture that
/// waits can only ever watch slabs ENTER delayed destroy; a "purged 0" column from a short run
/// says nothing BY CONSTRUCTION. The last section calls the age-parameterised purge with 0 rather
/// than sleeping, which is the same thing an hour later.
///
/// WRITES NOTHING after the fixture, so every round below acts on a store that is not changing.
#[test]
fn an_idle_shard_collects_the_slab_compaction_vacated() {
    const RECORDS: usize = 1_200;
    const ROUNDS: usize = 10;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for index in 0..RECORDS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("vacated-{index:07}"),
                value: vec![b'v'; 128],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    // Overwrite a third, so the slab compaction drains is genuinely holed and the round it spends
    // on it is real work rather than a proof that an empty store compacts cheaply.
    for index in 0..RECORDS / 3 {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("vacated-{index:07}"),
                value: vec![b'w'; 192],
            },
        });
        assert!(response.status.ok, "overwrite {index}: {:?}", response.status);
    }

    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    let engine = runtime.engine();
    let options = StorageManagerOptions::default();

    let mut compacting_rounds = 0_usize;
    let mut went_stale = std::collections::BTreeSet::<u64>::new();
    let mut refresh_rounds = Vec::new();
    let mut stale_by_round = Vec::new();
    let mut candidates_by_round = Vec::new();
    let mut slabs_by_round = Vec::new();
    let mut slab_bytes_by_round = Vec::new();
    for round in 0..ROUNDS {
        let report = runtime.run_storage_manager_once(1, options.clone());
        if report
            .executed_stages
            .iter()
            .any(|stage| stage == "compact_pages")
        {
            compacting_rounds += 1;
        }
        if report
            .lifecycle_plan
            .reasons
            .iter()
            .any(|reason| reason == "slot_dump_refresh_after_relocation")
        {
            refresh_rounds.push(round);
        }
        went_stale.extend(report.lifecycle_plan.stale_block_slab_ids.iter().copied());
        stale_by_round.push(report.pressure.stale_block_slab_count);
        candidates_by_round.push(report.pressure.reclaim_candidate_count);
        slabs_by_round.push(engine.block_store().slab_ids().unwrap_or_default().len());
        slab_bytes_by_round.push(
            engine
                .block_store()
                .slab_block_counts()
                .unwrap_or_default()
                .iter()
                .map(|(_id, physical_bytes, _count)| *physical_bytes)
                .sum::<u64>(),
        );
    }

    // DENOMINATORS, all three, before anything below is read as clean.
    assert!(
        compacting_rounds > 0,
        "compaction never ran over {ROUNDS} rounds, so no slab was ever vacated and the clean \
         counts below are about an idle compactor, not a working collector. slabs per round: \
         {slabs_by_round:?}"
    );
    assert!(
        !went_stale.is_empty(),
        "no slab went stale over {ROUNDS} rounds ({compacting_rounds} of them compacting), so \
         there was nothing for the collector to destroy and a zero here is vacuous. stale per \
         round: {stale_by_round:?}; slabs per round: {slabs_by_round:?}"
    );
    // THE PROPERTY. The slab compaction emptied is gone from the store.
    let slab_ids_after = engine.block_store().slab_ids().unwrap_or_default();
    let still_standing = went_stale
        .iter()
        .copied()
        .filter(|block_slab_id| slab_ids_after.contains(block_slab_id))
        .collect::<Vec<_>>();
    assert!(
        still_standing.is_empty(),
        "slabs {still_standing:?} were vacated by compaction and are STILL in the store after \
         {ROUNDS} rounds with nobody writing. stale per round: {stale_by_round:?}; reclaim \
         candidates per round: {candidates_by_round:?}; slabs per round: {slabs_by_round:?}; slab \
         bytes per round: {slab_bytes_by_round:?}"
    );

    // AND BY WHICH MECHANISM. The property above is the whole requirement; this says the plan
    // is what met it, so a future change that collects the slab some other way reads as a change
    // rather than as this still working.
    assert!(
        !refresh_rounds.is_empty(),
        "the vacated slab was collected, but the plan never asked for the dump that releases a \
         manifest-pinned one -- something else reclaimed it. stale per round: {stale_by_round:?}; \
         reclaim candidates per round: {candidates_by_round:?}"
    );

    // WHERE THE BYTES WENT: quarantine, not an unlink. The reclaim stage asks for delayed
    // destroy so a reader holding a stale address has an hour to stop holding it, and a collector
    // that started deleting outright would pass the assertion above while breaking that.
    let quarantined = engine
        .block_store()
        .delayed_destroy_slab_ids()
        .unwrap_or_default();
    let unaccounted = went_stale
        .iter()
        .copied()
        .filter(|block_slab_id| !quarantined.contains(block_slab_id))
        .collect::<Vec<_>>();
    assert!(
        unaccounted.is_empty(),
        "slabs {unaccounted:?} left the store without passing through delayed destroy \
         (quarantine holds {quarantined:?}) -- they were unlinked instead of quarantined"
    );

    // THE PURGE, WITH ITS AGE INJECTED. Waiting cannot reach this: quarantine enforces an hour.
    let quarantined_bytes = engine
        .block_store()
        .delayed_destroy_slab_reports()
        .unwrap_or_default()
        .iter()
        .map(|report| report.physical_bytes)
        .sum::<u64>();
    assert!(
        quarantined_bytes > 0,
        "quarantine holds {quarantined:?} but zero bytes, so the purge below would release \
         nothing and prove nothing"
    );
    let purge = engine
        .block_store()
        .purge_delayed_destroy_slabs_older_than(0)
        .expect("purging quarantine should succeed");
    assert_eq!(
        purge.purged_physical_bytes, quarantined_bytes,
        "an aged purge released {} of the {quarantined_bytes} bytes quarantine was holding: \
         {purge:?}",
        purge.purged_physical_bytes,
    );

    let plan_after =
        engine.storage_lifecycle_plan(crate::engine::reports::StorageLifecycleRequest {
            shard_id: 1,
            ..Default::default()
        });
    assert!(
        plan_after.stale_block_slab_ids.is_empty(),
        "a settled idle shard still reports stale slabs {:?} after the vacated slab was purged",
        plan_after.stale_block_slab_ids,
    );
    assert!(
        plan_after.reclaim_candidates.is_empty(),
        "a settled idle shard still reports {} reclaim candidates after the vacated slab was \
         purged, so the count a future gate keys on never returns to zero: {:?}",
        plan_after.reclaim_candidates.len(),
        plan_after.reclaim_candidates,
    );
}

/// An idle shard stops growing its index log, because compaction stops running on it.
///
/// THE DEFECT THIS HOLDS SHUT, as measured. A cadence fixture that writes 8,000 records,
/// overwrites a third, STOPS WRITING and runs twelve maintenance rounds. `compact_pages` executed
/// in all twelve, on a shard nobody was touching, and the index log climbed 63 bytes every one of
/// them -- 693 bytes over eleven rounds, about 181 KB a day per idle shard at a thirty-second
/// cadence, unbounded. The same fixture with compaction disabled grew by ZERO bytes, which is what
/// attributes the growth: compaction relocates pages and persists a fresh index record each round,
/// and it kept re-triggering itself because the slab it had just emptied was still a reclaim
/// candidate, and `stale_page_pressure` counts candidates.
///
/// So the index log is the INDEPENDENT witness here, and it is the reason this guard asserts on
/// bytes rather than on the stage list. A stage list can be made to look right by moving the name
/// out of it; the log only stops growing if the work actually stopped.
///
/// WRITES NOTHING after the fixture. Every round below acts on a store that is not changing, so
/// any growth is the loop's own doing.
#[test]
fn an_idle_shard_stops_growing_its_index_log() {
    const RECORDS: usize = 1_200;
    const SETTLE_ROUNDS: usize = 6;
    const IDLE_ROUNDS: usize = 6;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for index in 0..RECORDS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("idle-index-{index:07}"),
                value: vec![b'v'; 128],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    // Overwrite a third, so compaction has genuine garbage to reclaim and the settle rounds below
    // are doing real work rather than proving that an empty store compacts cheaply.
    for index in 0..RECORDS / 3 {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("idle-index-{index:07}"),
                value: vec![b'w'; 192],
            },
        });
        assert!(response.status.ok, "overwrite {index}: {:?}", response.status);
    }

    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    let engine = runtime.engine();
    let options = StorageManagerOptions::default();

    // Let compaction do the work it legitimately has: drain the slab the overwrites left holed.
    // That work is bounded -- a round relocates at most COMPACTION_ROUND_PAGE_REFS refs and this
    // fixture's live set fits inside one round -- so the settle window does not have to be
    // generous, only finite.
    let mut settle_compactions = 0_usize;
    for _ in 0..SETTLE_ROUNDS {
        let report = runtime.run_storage_manager_once(1, options.clone());
        if report
            .executed_stages
            .iter()
            .any(|stage| stage == "compact_pages")
        {
            settle_compactions += 1;
        }
    }

    let index_log_before = engine.index_log_store().log_len_bytes(1);
    let mut idle_rounds_that_compacted = Vec::new();
    let mut index_log_by_round = Vec::new();
    let mut candidates_by_round = Vec::new();
    for round in 0..IDLE_ROUNDS {
        let report = runtime.run_storage_manager_once(1, options.clone());
        if report
            .executed_stages
            .iter()
            .any(|stage| stage == "compact_pages")
        {
            idle_rounds_that_compacted.push(round);
        }
        candidates_by_round.push(report.pressure.reclaim_candidate_count);
        index_log_by_round.push(engine.index_log_store().log_len_bytes(1));
    }
    let index_log_after = engine.index_log_store().log_len_bytes(1);

    // DENOMINATORS. Both of these can make every assertion below hold for a reason that has
    // nothing to do with the defect.
    assert!(
        settle_compactions > 0,
        "compaction never ran even while the shard had a holed slab to drain, so the flat index \
         log below says nothing about compaction stopping -- it says the fixture never started it"
    );
    assert!(
        index_log_before > 0,
        "the index log is empty, so it cannot be observed not to grow"
    );

    assert_eq!(
        index_log_after,
        index_log_before,
        "the index log of an IDLE shard grew {} bytes over {IDLE_ROUNDS} rounds ({index_log_before} \
         -> {index_log_after}, {} a round), with nobody writing to it. Per round: \
         {index_log_by_round:?}; reclaim candidates per round: {candidates_by_round:?}; compaction \
         ran in rounds {idle_rounds_that_compacted:?}",
        index_log_after.saturating_sub(index_log_before),
        index_log_after.saturating_sub(index_log_before) / IDLE_ROUNDS as u64,
    );

    assert!(
        idle_rounds_that_compacted.is_empty(),
        "compaction ran again on an idle shard, in rounds {idle_rounds_that_compacted:?} of \
         {IDLE_ROUNDS}, after {settle_compactions} settling rounds had already drained it -- it is \
         re-triggering on its own residue"
    );
}

/// The hint the maintenance round takes off its plan is the same answer as asking every object.
///
/// The round does not walk the shard to decide whether to compact -- it sums `live_page_refs`
/// over the reclaim candidates it already built, which costs nothing. The NORMATIVE definition is
/// the per-object one: for each object, are any of its pages on a slab someone wants emptied?
/// This holds the cheap form to the normative one, so a change that separates them fails here
/// rather than in a shipped round.
///
/// It also pins the distinction the whole fix rests on. After compaction has drained the holed
/// slab, that slab is still a reclaim candidate -- it is nothing but dead space until the
/// collector destroys it -- so `stale_page_pressure` is still open. The hint is nevertheless
/// zero, because no object has a page left there. That is the difference between "this shard has
/// stale pages" and "there is something for compaction to move", and no shard-wide predicate can
/// express it.
#[test]
fn the_relocation_hint_agrees_with_the_plan_it_is_taken_from() {
    const RECORDS: usize = 400;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for index in 0..RECORDS {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("hint-agree-{index:05}"),
                value: vec![b'v'; 128],
            },
        });
    }
    for index in 0..RECORDS / 2 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("hint-agree-{index:05}"),
                value: vec![b'w'; 192],
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
    let engine = runtime.engine();
    let options = StorageManagerOptions::default();

    let (_pressure, plan) = runtime.storage_manager_pressure_snapshot(1, &options);
    let from_plan = crate::engine::compaction_relocatable_page_refs(&plan.reclaim_candidates);
    let hint = engine.compaction_relocation_hint(1, &plan.reclaim_candidates);
    assert!(
        hint.examined_object_count > 0,
        "the hint saw no objects at all, so agreeing on zero proves nothing: {hint:?}"
    );
    assert!(
        from_plan > 0,
        "the fixture left nothing to relocate before compaction ran, so the two forms would agree \
         on zero for the wrong reason: {plan:?}"
    );
    assert_eq!(
        from_plan, hint.relocatable_page_refs,
        "the round's cheap form and the per-object walk disagree: {from_plan} against {hint:?}"
    );

    // Now drain it, and ask again.
    for _ in 0..4 {
        runtime.run_storage_manager_once(1, options.clone());
    }
    let (pressure_after, plan_after) = runtime.storage_manager_pressure_snapshot(1, &options);
    let from_plan_after =
        crate::engine::compaction_relocatable_page_refs(&plan_after.reclaim_candidates);
    let hint_after = engine.compaction_relocation_hint(1, &plan_after.reclaim_candidates);
    assert_eq!(
        from_plan_after, hint_after.relocatable_page_refs,
        "the two forms disagree once the shard is drained: {from_plan_after} against {hint_after:?}"
    );
    assert_eq!(
        hint_after.relocatable_page_refs, 0,
        "the shard was drained and nobody wrote to it, so no object should have a page on a slab \
         worth emptying: {hint_after:?}"
    );
    assert!(
        hint_after.examined_object_count > 0,
        "the objects vanished, so the zero above is not the one this test is about: {hint_after:?}"
    );
    let _ = pressure_after;
}

/// Does the idle shard settle at a corpus ten times bigger? Prints, and asserts. Release only.
///
///   cargo test --release -p temporalstore-rust --lib an_idle_shard_settles_at_both_corpus_sizes \
///       -- --ignored --nocapture --test-threads=1
///
/// WHY A SECOND SIZE. At 8,000 records a compaction round relocates the whole live set inside
/// COMPACTION_ROUND_PAGE_REFS x 4 rounds, so the drain finishes fast and the shard reaches the
/// state where the self-retrigger is visible. At 80,000 it takes about forty rounds, and a run of
/// thirty-two never gets there: it shows source slab 0 -> destination slab 1 in every round, no
/// fully-stale slab in any round, nothing collected, and slab bytes climbing 60%.
///
/// THAT IS NOT A SECOND DEFECT AND THIS SAYS SO IN COLUMNS. It is ONE bounded relocation still in
/// progress. A round that spends its ref budget stays open and the next resumes onto the same
/// destination slab -- which is exactly why the source and destination do not advance -- and the
/// source slab cannot go stale until the last of its live pages has moved. The peak footprint
/// while that happens is the live set twice over, once on each slab, and it comes back down when
/// the drain completes and the source is collected.
///
/// What the two sizes have in common is the END of the drain, and that is where the defect lives:
/// the round closes, the shard is as compact as this compactor can make it, and the next round
/// rolls a fresh slab and moves everything again. This runs each size until the drain is done and
/// then keeps going with NO WRITER, so the tail is what the assertions read.
#[test]
#[ignore]
fn an_idle_shard_settles_at_both_corpus_sizes() {
    for records in [8_000usize, 80_000usize] {
        settle_one_corpus(records);
    }
}

fn settle_one_corpus(records: usize) {
    // Enough rounds for the drain (records / COMPACTION_ROUND_PAGE_REFS, about 40 at 80,000) plus
    // a tail. The loop stops early once the tail is established, so the cap only has to be big
    // enough not to cut the drain short.
    let max_rounds = records / 1_024 + 24;
    const TAIL: usize = 8;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for index in 0..records {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("settle-{index:07}"),
                value: vec![b'v'; 128],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    for index in 0..records / 3 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("settle-{index:07}"),
                value: vec![b'w'; 192],
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
    let engine = runtime.engine();
    let options = StorageManagerOptions::default();

    eprintln!("  [settle] {records} records written, then NO further writes");
    eprintln!(
        "  [settle] {:>5} {:>7} {:>9} {:>9} {:>6} {:>12} {:>6} {:>11} {:>11} {:>10} {:>9} {:>10} \
{:>9} {:>13}",
        "round", "compact", "rewritten", "src->dst", "slabs", "slab_bytes", "stale", "candidates",
        "relocatable", "idx_bytes", "collected", "quarantine", "manifests", "manifest_slabs",
    );

    let mut rounds_that_compacted = 0usize;
    let mut last_compacting_round: Option<usize> = None;
    let mut index_log_by_round: Vec<u64> = Vec::new();
    let mut slab_bytes_by_round: Vec<u64> = Vec::new();
    let mut went_stale = std::collections::BTreeSet::<u64>::new();
    let mut rounds_run = 0usize;
    for round in 0..max_rounds {
        rounds_run = round + 1;
        let report = runtime.run_storage_manager_once(1, options.clone());
        let compacted = report
            .executed_stages
            .iter()
            .any(|stage| stage == "compact_pages");
        if compacted {
            rounds_that_compacted += 1;
            last_compacting_round = Some(round);
        }
        let (rewritten, source, destination) = report
            .compaction_report
            .as_ref()
            .map(|compaction| {
                (
                    compaction.rewritten_object_pages,
                    compaction.previous_block_slab_id,
                    compaction.compacted_block_slab_id,
                )
            })
            .unwrap_or((0, 0, 0));
        let slab_ids = engine.page_store().slab_ids().unwrap_or_default();
        let slab_bytes: u64 = engine
            .page_store()
            .slab_block_counts()
            .unwrap_or_default()
            .iter()
            .map(|(_id, physical_bytes, _count)| *physical_bytes)
            .sum();
        let relocatable = crate::engine::compaction_relocatable_page_refs(
            &report.lifecycle_plan.reclaim_candidates,
        );
        let index_log_bytes = engine.index_log_store().log_len_bytes(1);
        index_log_by_round.push(index_log_bytes);
        slab_bytes_by_round.push(slab_bytes);
        // What the COLLECTOR did with the slab compaction vacated, beside what compaction did.
        // `collected` is the reclaim stage's own count, `quarantine` is how many slabs are
        // sitting in delayed destroy, and the two manifest columns are what used to hold the
        // slab back: a bucket dump manifest names it, so `run_gc_inner` counts it as live.
        let collected = report
            .gc_report
            .as_ref()
            .map(|gc| gc.block_slabs_removed)
            .unwrap_or(0);
        let quarantined = engine
            .block_store()
            .delayed_destroy_slab_ids()
            .unwrap_or_default();
        let manifests = engine.list_bucket_dump_manifests(1);
        let manifest_slabs = manifests
            .iter()
            .flat_map(|manifest| manifest.block_slab_ids.iter().copied())
            .collect::<std::collections::BTreeSet<_>>();
        went_stale.extend(report.lifecycle_plan.stale_block_slab_ids.iter().copied());
        eprintln!(
            "  [settle] {round:>5} {compacted:>7} {rewritten:>9} {:>9} {:>6} {slab_bytes:>12} \
{:>6} {:>11} {relocatable:>11} {index_log_bytes:>10} {collected:>9} {:>10} {:>9} {:>13}",
            format!("{source}->{destination}"),
            slab_ids.len(),
            report.pressure.stale_block_slab_count,
            report.pressure.reclaim_candidate_count,
            quarantined.len(),
            manifests.len(),
            format!("{manifest_slabs:?}"),
        );
        if round + 1 >= TAIL
            && last_compacting_round
                .map(|last| round.saturating_sub(last) >= TAIL)
                .unwrap_or(false)
        {
            break;
        }
    }

    // DENOMINATOR. A run where compaction never fired says nothing about it settling.
    assert!(
        rounds_that_compacted > 0,
        "compaction never ran at {records} records, so this measures an idle collector and not a \
         settled compactor"
    );
    let last_compacting_round =
        last_compacting_round.expect("a compacting round, since one was counted");
    assert!(
        rounds_run.saturating_sub(last_compacting_round) > TAIL,
        "compaction was still running at round {last_compacting_round} of {rounds_run} at \
         {records} records -- the drain did not finish inside the cap, so the tail below is not a \
         settled shard. index log per round: {index_log_by_round:?}"
    );

    let tail_first_index_log = index_log_by_round[rounds_run - TAIL];
    let tail_last_index_log = index_log_by_round[rounds_run - 1];
    assert_eq!(
        tail_first_index_log,
        tail_last_index_log,
        "the index log of a SETTLED idle shard at {records} records grew {} bytes over the last \
         {TAIL} rounds, with nobody writing: {index_log_by_round:?}",
        tail_last_index_log.saturating_sub(tail_first_index_log),
    );
    let tail_first_slab_bytes = slab_bytes_by_round[rounds_run - TAIL];
    let tail_last_slab_bytes = slab_bytes_by_round[rounds_run - 1];
    assert!(
        tail_last_slab_bytes <= tail_first_slab_bytes,
        "slab bytes of a SETTLED idle shard at {records} records grew from {tail_first_slab_bytes} \
         to {tail_last_slab_bytes} over the last {TAIL} rounds: {slab_bytes_by_round:?}"
    );

    // THE COLLECTOR'S HALF, which #1627 measured open and left open: the slab compaction emptied
    // used to stand for ever, because two retained bucket dump manifests still named it and
    // `run_gc_inner` counts a manifest-named slab as live. Measured before the fix, in EVERY
    // settled round at both sizes: slabs 2, stale 1, candidates 1, collected 0, quarantine 0.
    //
    // Denominator first: a slab has to have gone stale inside the run, or "none left standing"
    // is a statement about a shard that never vacated one.
    assert!(
        !went_stale.is_empty(),
        "no slab went stale at {records} records over {rounds_run} rounds, so the collector had \
         nothing to destroy and the assertion below would hold vacuously"
    );
    let slab_ids_after = engine.block_store().slab_ids().unwrap_or_default();
    let still_standing = went_stale
        .iter()
        .copied()
        .filter(|block_slab_id| slab_ids_after.contains(block_slab_id))
        .collect::<Vec<_>>();
    assert!(
        still_standing.is_empty(),
        "slabs {still_standing:?} were vacated by compaction at {records} records and are STILL \
         in the store after {rounds_run} settled rounds with nobody writing. slab bytes per \
         round: {slab_bytes_by_round:?}"
    );
    // Quarantine holds them for an hour, so purge with the age INJECTED rather than waiting --
    // a short run can otherwise only ever watch slabs ENTER delayed destroy.
    let purge = engine
        .block_store()
        .purge_delayed_destroy_slabs_older_than(0)
        .expect("purging quarantine should succeed");
    let plan_after =
        engine.storage_lifecycle_plan(crate::engine::reports::StorageLifecycleRequest {
            shard_id: 1,
            ..Default::default()
        });
    eprintln!(
        "  [settle] {records} records: compacted in {rounds_that_compacted} rounds, last at \
{last_compacting_round}, then FLAT for {} rounds -- index log {tail_last_index_log} bytes, slab \
bytes {tail_last_slab_bytes}, vacated {went_stale:?}, purged {:?} releasing {} bytes, reclaim \
candidates now {}",
        rounds_run.saturating_sub(last_compacting_round + 1),
        purge.purged_block_slab_ids,
        purge.purged_physical_bytes,
        plan_after.reclaim_candidates.len(),
    );
    assert!(
        plan_after.reclaim_candidates.is_empty(),
        "a settled idle shard at {records} records still reports {} reclaim candidates once the \
         vacated slab is purged, so the count never returns to zero: {:?}",
        plan_after.reclaim_candidates.len(),
        plan_after.reclaim_candidates,
    );
}

/// A shard that has gone quiet must still be able to reclaim its log.
///
/// After the first maintenance round every slot is CLEAN -- the dump captured all of them -- and
/// a clean slot holds no undumped write, so nothing in the shard needs the log retained. The
/// plan must say so.
///
/// It used to say the opposite, and the OVERWRITE in this fixture is what makes it do so. An
/// overwrite gives the slot a new generation, so the manifest the dump just wrote no longer
/// matches its current generation fingerprint and no manifest covers it. That drops the slot into
/// the no-manifest branch, where its `first_dirty_wal_sequence` of 0 -- set to 0 precisely
/// BECAUSE the dump captured everything -- was read as "cannot name its claim", filed under
/// `missing_bucket_generations`, and blocked the whole plan with
/// `slot_generation_without_durable_dump`.
///
/// That is the shape #1516 removed from the manifest branch -- a slot that needs nothing deciding
/// what the log may drop -- surviving one branch over, where it refuses outright rather than
/// merely pinning the floor.
///
/// DENOMINATOR, and it has to be this one. `covered_bucket_count` counts BOTH branches, so it
/// cannot witness which branch ran: a write-only fixture of the same size reaches
/// `safe_to_reclaim` through the MANIFEST branch and passes every assertion below whatever the
/// no-manifest branch does. Measured -- 2,000 records with no overwrite: covered=2000,
/// uncovered=0, safe=true with this fix reverted. `retained_manifest_ids` is the honest witness:
/// the manifest branch records the manifest it matched, the no-manifest branch records nothing,
/// so an EMPTY set plus a non-zero covered count proves every slot took the branch under test.
#[test]
fn a_quiet_shard_with_every_slot_clean_can_reclaim_its_log() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..2_000usize {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("quiet-{index:06}"),
                value: vec![b'v'; 64],
            },
        });
    }
    // The overwrite is load-bearing, not decoration. Without it no slot reaches the branch this
    // test exists for.
    for index in 0..666usize {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("quiet-{index:06}"),
                value: vec![b'w'; 64],
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
    // One round to dump everything, then the writer stops for good.
    runtime.run_storage_manager_once(1, StorageManagerOptions::default());
    let engine = runtime.engine();

    let plan = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());

    // DENOMINATOR, asserted before the property.
    assert!(
        plan.current_wal_sequence > 0,
        "the fixture must leave records in the log, got none: {plan:?}"
    );
    let classified = plan
        .covered_bucket_count
        .saturating_add(plan.uncovered_bucket_count);
    assert!(
        classified > 0,
        "the fixture must leave slots for the plan to classify, got none: {plan:?}"
    );
    assert!(
        plan.retained_manifest_ids.is_empty(),
        "every slot must reach the NO-MANIFEST branch for this test to mean anything, but {} \
         manifest(s) were matched -- the fixture has stopped exercising the branch under test \
         and would pass with the rule reverted: {:?}",
        plan.retained_manifest_ids.len(),
        plan.retained_manifest_ids,
    );

    assert!(
        !plan
            .blocker_reasons
            .iter()
            .any(|reason| reason == "slot_generation_without_durable_dump"),
        "every slot is clean and holds no undumped write, so none of them may be reported as \
         lacking a durable dump: blockers={:?} uncovered={} covered={}",
        plan.blocker_reasons,
        plan.uncovered_bucket_count,
        plan.covered_bucket_count,
    );
    assert_eq!(
        plan.uncovered_bucket_count, 0,
        "a clean slot needs nothing retained on its behalf, so it is not uncovered: {plan:?}"
    );
    assert!(
        plan.safe_to_reclaim,
        "a quiet shard whose every slot is dumped must be reclaimable: {plan:?}"
    );
}

/// `reclaim_wal` must not disappear from the stage list while the log is still reclaimable, and
/// the log must actually shrink.
///
/// The stage list is how an operator reads whether maintenance is alive, so the two cases it must
/// never confuse are "the log is at its floor" and "the stage stopped running". They look
/// identical from outside: flat `persistent_bytes` either way.
///
/// The trigger used to be dump pressure alone -- dirty slots, or undumped records -- which a
/// shard with no writer never has. So the stage ran in round 0 and never again, with the whole
/// log still on disk: measured on 8,000 records over twelve rounds, 0 of 10,666 records freed and
/// `persistent_bytes` flat at 1,344,852 for ever. With both halves fixed the same fixture falls
/// to 33,413 on the first idle round.
///
/// VACUITY. A round whose plan is not reclaimable says nothing about the gate, so the count of
/// reclaimable rounds is asserted before the gate is. Reverting the plan rule alone takes that
/// count to zero and kills this test there; reverting the gate alone kills it below.
#[test]
fn the_log_reclaim_stage_keeps_running_while_the_log_is_still_reclaimable() {
    const IDLE_ROUNDS: usize = 6;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..2_000usize {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("cadence-{index:06}"),
                value: vec![b'v'; 64],
            },
        });
    }
    // Same reason as the test above: without an overwrite the slots keep a matching manifest and
    // the quiet shard reclaims through the other branch entirely.
    for index in 0..666usize {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("cadence-{index:06}"),
                value: vec![b'w'; 64],
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
    // Round 0 dumps. After it the writer is gone and every later round is an idle one.
    let first = runtime.run_storage_manager_once(1, StorageManagerOptions::default());
    assert!(
        first
            .executed_stages
            .iter()
            .any(|stage| stage == "reclaim_wal"),
        "the first round must run the stage at all: {:?}",
        first.executed_stages
    );
    let bytes_after_first = runtime.engine().wal_store().stats(1).persistent_bytes;

    let mut reclaimable_rounds = 0usize;
    let mut rounds_missing_the_stage = Vec::new();
    for round in 0..IDLE_ROUNDS {
        let engine = runtime.engine();
        let reclaimable = engine
            .storage_wal_reclaim_plan(1, Vec::new(), Vec::new())
            .safe_to_reclaim;
        drop(engine);
        let report = runtime.run_storage_manager_once(1, StorageManagerOptions::default());
        if !reclaimable {
            continue;
        }
        reclaimable_rounds += 1;
        if !report
            .executed_stages
            .iter()
            .any(|stage| stage == "reclaim_wal")
        {
            rounds_missing_the_stage.push((round, report.skipped_stages.clone()));
        }
    }
    let bytes_at_end = runtime.engine().wal_store().stats(1).persistent_bytes;

    // DENOMINATOR, asserted before the property. Without it, a plan that never reports the log
    // reclaimable would make every round above vacuous and this test could not fail.
    assert!(
        reclaimable_rounds > 0,
        "no idle round reported a reclaimable log, so this test proves nothing about the gate -- \
         the plan, not the gate, is what broke"
    );
    assert!(
        rounds_missing_the_stage.is_empty(),
        "`reclaim_wal` left the stage list on {} of {reclaimable_rounds} rounds whose log was \
         still reclaimable, which an operator reads as maintenance having died: {:?}",
        rounds_missing_the_stage.len(),
        rounds_missing_the_stage,
    );
    // The stage running is the operator-visible half; the log shrinking is the point of it.
    assert!(
        bytes_at_end < bytes_after_first,
        "the idle rounds ran the stage but freed nothing: {bytes_after_first} bytes after the \
         first round, {bytes_at_end} after {IDLE_ROUNDS} idle ones"
    );
}

// ---------------------------------------------------------------------------------------------
// Eviction relieves the memory it can SEE. The bucket index was in none of the numbers that
// decide whether a shard needs relieving, so the shard that most needed it read as the emptiest.
// ---------------------------------------------------------------------------------------------

/// A shard whose value is INDEX, not cache, is reported as carrying that memory.
///
/// `ShardLoad.memory_bytes` -- the figure the metaserver sums per datanode and sorts placement on
/// -- was `cache.memory_bytes` alone. Drop the cache on a shard holding 400 records and that
/// figure reads ZERO while the index it cannot see is still fully resident. The balancer ranked
/// such a shard as the emptiest node in the fleet and kept sending it work, and the maintenance
/// round never saw a reason to ask it to evict.
///
/// The cache is dropped FIRST and asserted at zero before any claim is made, so the number that
/// moves below cannot be a cache number -- that is the whole point of the measurement.
#[test]
fn a_shard_whose_memory_is_index_not_cache_is_no_longer_reported_as_unloaded() {
    const KEYS: usize = 400;
    let dir = tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..KEYS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("indexload-{index:06}"),
                value: vec![b'v'; 96],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    // Drop the cache FIRST. Whatever is non-zero afterwards is the index.
    let _ = engine.cache().invalidate_shard(1);

    let stats = engine.get_stats(1).stats.expect("stats for a loaded shard");

    // DENOMINATORS, before any claim. Both halves of this test pass trivially on an empty shard:
    // nothing cached and nothing indexed reads exactly like the fix working.
    assert_eq!(
        stats.string_records, KEYS,
        "the fixture wrote {} of {KEYS} records, so the measurement below has no subject",
        stats.string_records
    );
    assert!(
        stats.storage.bucket_index_resident_entries > 0,
        "no resident buckets: {} -- there is no index here to be blind to",
        stats.storage.bucket_index_resident_entries
    );
    assert!(
        stats.storage.bucket_index_resident_bytes > 0,
        "no resident index bytes over {} buckets and {KEYS} records",
        stats.storage.bucket_index_resident_entries
    );

    // THE DEFECT, still visible: the cache-only figure reads zero on a shard holding 400 records.
    assert_eq!(
        stats.cache.memory_bytes, 0,
        "the cache did not drop, so the load figure below could be cache bytes rather than index \
         bytes and this test would prove nothing"
    );

    // THE FIX: the reported load is non-zero, and on this fixture it is exactly the index.
    assert!(
        stats.load_memory_bytes() > 0,
        "the shard still reports as holding no memory: cache={} index={}",
        stats.cache.memory_bytes,
        stats.storage.bucket_index_resident_bytes
    );
    assert_eq!(
        stats.load_memory_bytes(),
        stats.storage.bucket_index_resident_bytes,
        "with the cache at zero the reported load must be the index and nothing else"
    );
    // And it is the same quantity the eviction gate reads, not a second opinion about it.
    assert_eq!(
        stats.storage.bucket_index_resident_bytes,
        engine.bucket_index_resident_bytes(1),
        "the published number and the gate's number disagree, which is how two numbers with one \
         name drift apart"
    );

    // THE FLOOR AND THE PRESSURE READING ARE NOT THE SAME QUANTITY, and must not be.
    //
    // The floor counts NODES, which a release keeps; the pressure reading counts nodes AND the
    // per-page entries, which a release frees. Gate on the floor and eviction appears to free
    // nothing, so the gate re-fires for ever; publish only the pressure reading and an operator
    // loses the figure that does not move under maintenance. If these two are ever equal on a
    // shard with pages, one of them has stopped being what it claims.
    assert!(
        stats.storage.bucket_index_resident_bytes
            > stats.storage.bucket_index_resident_bytes_floor,
        "the node-only floor ({}) and the moving pressure reading ({}) came out equal over {} \
         resident buckets, so they are no longer two different measurements",
        stats.storage.bucket_index_resident_bytes_floor,
        stats.storage.bucket_index_resident_bytes,
        stats.storage.bucket_index_resident_entries
    );

    // The maintenance round's own snapshot tells the same story.
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    let options = StorageManagerOptions::default();
    let (pressure, _) = runtime.storage_manager_pressure_snapshot(1, &options);
    assert_eq!(
        pressure.cache_memory_bytes, 0,
        "the snapshot cache term is not zero, so the assertions below are not about the index"
    );
    assert_eq!(
        pressure.bucket_index_resident_bytes, stats.storage.bucket_index_resident_bytes,
        "the snapshot and the stats path report different index memory for one shard"
    );
    assert!(
        pressure.eviction_memory_pressure_bytes >= pressure.bucket_index_resident_bytes,
        "the eviction pressure figure ({}) does not even contain the index term ({})",
        pressure.eviction_memory_pressure_bytes,
        pressure.bucket_index_resident_bytes
    );
    assert!(
        pressure.total_pressure_score >= pressure.bucket_index_resident_bytes,
        "total pressure ({}) omits the index ({}), which is the term this change exists to add",
        pressure.total_pressure_score,
        pressure.bucket_index_resident_bytes
    );

    // `cache_memory_bytes` stays CACHE-ONLY on purpose. It gates `reclaim_memory`, which relieves
    // memory by invalidating cached pages and cannot touch the index; folding index bytes into it
    // would make that stage fire on a debt it has no way to pay, every round, for ever.
    assert_ne!(
        pressure.cache_memory_bytes, pressure.eviction_memory_pressure_bytes,
        "the cache gate and the eviction gate have become one number; reclaim_memory will now \
         fire on index pressure it cannot relieve"
    );
}

/// The `evict` decision reports the number it gates on, not a copy of its own threshold.
///
/// The signal was built as `signal("cache_memory_bytes", eviction_memory_pressure_threshold, 1)`
/// -- the THRESHOLD passed as the observed value, against a threshold of 1. So it read
/// `over_threshold` on every round for every shard, said nothing about the shard, and said it
/// under a name that was not what it held either. A readout that cannot distinguish a shard under
/// pressure from an empty one is the readiness-report-that-cannot-fail shape.
#[test]
fn the_evict_decision_reports_the_pressure_it_gates_on_not_its_own_threshold() {
    const KEYS: usize = 256;
    const THRESHOLD: u64 = 7_919; // A prime, so a signal echoing it is unmistakable.
    let dir = tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..KEYS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("decision-{index:06}"),
                value: vec![b'v'; 96],
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
    let options = StorageManagerOptions {
        enable_evict: true,
        eviction_memory_pressure_threshold: THRESHOLD,
        ..StorageManagerOptions::default()
    };
    let (pressure, _) = runtime.storage_manager_pressure_snapshot(1, &options);
    // DENOMINATOR: the observed value has to be capable of differing from the threshold, or the
    // assertion below cannot fail whatever the code does.
    assert!(
        pressure.eviction_memory_pressure_bytes != THRESHOLD,
        "the fixture pressure happens to equal the threshold ({THRESHOLD}), so this test cannot \
         tell a reported observation from a reported threshold"
    );

    let report = runtime.run_storage_manager_once(1, options);
    let decision = report
        .pressure_decisions
        .iter()
        .find(|decision| decision.stage == "evict")
        .unwrap_or_else(|| {
            panic!(
                "no evict decision in {:?}",
                report
                    .pressure_decisions
                    .iter()
                    .map(|decision| decision.stage.clone())
                    .collect::<Vec<_>>()
            )
        });
    let signal = decision
        .signals
        .iter()
        .find(|signal| signal.name == "eviction_memory_pressure_bytes")
        .unwrap_or_else(|| {
            panic!(
                "no eviction_memory_pressure_bytes signal in {:?}",
                decision
                    .signals
                    .iter()
                    .map(|signal| signal.name.clone())
                    .collect::<Vec<_>>()
            )
        });
    assert_eq!(
        signal.threshold, THRESHOLD,
        "the evict signal is compared against something other than the eviction threshold"
    );
    assert_ne!(
        signal.observed, THRESHOLD,
        "the evict signal is still reporting its own threshold as the observation"
    );
    assert!(
        signal.observed > 0,
        "the evict signal observed nothing on a shard holding {KEYS} records"
    );
    let index_signal = decision
        .signals
        .iter()
        .find(|signal| signal.name == "bucket_index_resident_bytes")
        .expect("the index term must be reported on the one stage that can release it");
    assert!(
        index_signal.observed > 0,
        "the index term reads zero on a shard holding {KEYS} records"
    );
    assert!(
        signal.observed >= index_signal.observed,
        "the eviction pressure ({}) does not contain the index term ({})",
        signal.observed,
        index_signal.observed
    );
}

/// THE DECISION ON THE DEFAULT: `enable_evict` stays OFF, and this is why, measured.
///
/// The objection is not to eviction. It is to eviction under the defaults that sit beside the
/// flag: `eviction_memory_pressure_threshold` is 0 -- documented as "0 evicts whenever it runs" --
/// and `eviction_dump_before_evict` is false. Flip only `enable_evict` and every maintenance round
/// on every shard evicts unconditionally, with no dump first, on a shard under no memory pressure
/// whatsoever. That is not an eviction policy; it is a policy-shaped hole.
///
/// So this test does not assert a preference. It runs the thing and shows what default-on would
/// mean: the gate opens at pressure the operator never asked to relieve. What would have to be
/// true to flip it: a non-zero DEFAULT threshold chosen against a measured working set, and
/// `eviction_dump_before_evict` defaulted true so an evicted dirty bucket does not keep pinning
/// the log. Both are separate decisions with their own evidence, and neither is made here.
#[test]
fn enable_evict_stays_off_because_its_neighbouring_defaults_would_evict_at_zero_pressure() {
    let defaults = StorageManagerOptions::default();

    // The default is a DECISION, pinned here so a later edit to the `Default` impl has to come
    // past this test and its reasoning rather than sliding through as a tidy-up.
    assert!(
        !defaults.enable_evict,
        "enable_evict now defaults on; the two defaults below must have changed with it"
    );
    assert_eq!(
        defaults.eviction_memory_pressure_threshold, 0,
        "the eviction threshold is no longer 0, which removes the main objection to defaulting \
         enable_evict on -- revisit that decision rather than deleting this assertion"
    );
    assert!(
        !defaults.eviction_dump_before_evict,
        "dump-before-evict now defaults on, which removes the second objection -- revisit the \
         enable_evict default rather than deleting this assertion"
    );

    // THE MEASUREMENT. A shard under no memory pressure anyone would act on.
    const KEYS: usize = 64;
    let dir = tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..KEYS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("zeropressure-{index:06}"),
                value: vec![b'v'; 32],
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

    // CONTROL: the shipped default does not reach the stage at all.
    let shipped = runtime.run_storage_manager_once(1, StorageManagerOptions::default());
    assert!(
        shipped
            .skipped_stages
            .iter()
            .any(|stage| stage == "evict_disabled"),
        "the shipped default already evicts: skipped={:?} executed={:?}",
        shipped.skipped_stages,
        shipped.executed_stages
    );

    // TREATMENT: flip ONLY `enable_evict`, exactly as a "default it on" change would.
    let would_be_default = StorageManagerOptions {
        enable_evict: true,
        ..StorageManagerOptions::default()
    };
    let (pressure, _) = runtime.storage_manager_pressure_snapshot(1, &would_be_default);
    let report = runtime.run_storage_manager_once(1, would_be_default);
    let eviction = report
        .eviction
        .as_ref()
        .expect("the evict stage must report when it is enabled");

    // PROOF THE TREATMENT RAN, before reading anything out of it.
    assert!(
        report.executed_stages.iter().any(|stage| stage == "evict"),
        "the evict stage did not run, so nothing below measures eviction: executed={:?}",
        report.executed_stages
    );
    assert_eq!(
        eviction.memory_pressure_threshold, 0,
        "this round did not use the default threshold, so it is not the round a default-on change \
         would produce"
    );

    // AND THE FINDING. With a threshold of 0 the gate cannot close: the comparison is
    // `pressure_before < 0`, which is false for every shard that has ever existed.
    assert!(
        eviction.pressure_gate_open,
        "the gate declined at threshold 0, which would make this objection moot -- recheck it"
    );
    assert_eq!(
        eviction.skipped_reason, "",
        "the stage skipped for {:?} rather than evicting, so it did not act at zero pressure",
        eviction.skipped_reason
    );
    assert!(
        !eviction.selected_victims.is_empty(),
        "the round took no victims, so default-on would be harmless here and this objection needs \
         a different fixture: pressure={} threshold={}",
        eviction.pressure_before,
        eviction.memory_pressure_threshold
    );
    // And it took them without dumping first, because that default is off too: a dirty bucket
    // evicted this way keeps pinning the log it was never written out of.
    assert!(
        !eviction.dump_before_evict,
        "this round dumped first, so it is not the round the shipped defaults would produce"
    );
    assert!(
        eviction.dump_manifest_ids.is_empty(),
        "a dump manifest appeared without dump-before-evict: {:?}",
        eviction.dump_manifest_ids
    );
    eprintln!(
        "  [evict-default] flipping only enable_evict: threshold={} pressure_before={} \
victims={} dump_manifests={} -- the gate opens on a shard nobody asked to relieve \
(snapshot eviction_memory_pressure_bytes={})",
        eviction.memory_pressure_threshold,
        eviction.pressure_before,
        eviction.selected_victims.len(),
        eviction.dump_manifest_ids.len(),
        pressure.eviction_memory_pressure_bytes,
    );
}

/// What turning it on WOULD buy, and the bar it would have to clear: the periodic loop relieves
/// INDEX memory and still serves every key.
///
/// This is the half of the default decision that is not an objection. The mechanism works: given
/// a threshold below the shard's real pressure and dump-before-evict on, one round of the loop
/// releases buckets, the resident index falls, the node-only floor does NOT, and all 400 keys
/// still read back byte-for-byte through the released buckets.
///
/// The reads are COLD -- the cache is invalidated after the round -- because the first version of
/// the release served every warm read and returned `None` for every cold one. And each value is a
/// function of its key, so a read that resolved through the WRONG page fails here rather than
/// passing on a lucky length.
#[test]
fn the_periodic_loop_relieves_index_memory_and_still_serves_every_key() {
    const KEYS: usize = 400;
    const VALUE_LEN: usize = 96;

    fn keyed_value(index: usize) -> Vec<u8> {
        let mut value = format!("{index:06}:").into_bytes();
        value.resize(VALUE_LEN, b'v');
        value[VALUE_LEN - 1] = (index % 251) as u8;
        value
    }

    let dir = tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..KEYS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("loopevict-{index:06}"),
                value: keyed_value(index),
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
    let engine = runtime.engine();

    // Drop the cache FIRST, so the index bytes below are not cache bytes wearing a new label.
    let _ = engine.cache().invalidate_shard(1);
    let index_before = engine.bucket_index_resident_bytes(1);
    let floor_before = engine
        .get_stats(1)
        .stats
        .expect("stats for a loaded shard")
        .storage
        .bucket_index_resident_bytes_floor;
    // DENOMINATORS.
    assert!(
        index_before > 0,
        "the fixture left no resident index to relieve"
    );
    assert!(floor_before > 0, "the fixture left no resident nodes");
    assert!(
        engine.released_bucket_index_buckets(1).is_empty(),
        "the fixture started with buckets already released, so a release below proves nothing"
    );

    // A threshold BELOW the shard's real pressure, and dump-before-evict on -- the two settings
    // the shipped defaults do not have. This is the configuration the decision above says would
    // have to become the default before `enable_evict` could.
    let options = StorageManagerOptions {
        enable_evict: true,
        eviction_dump_before_evict: true,
        eviction_memory_pressure_threshold: 1,
        ..StorageManagerOptions::default()
    };
    let report = runtime.run_storage_manager_once(1, options);
    assert!(
        report.executed_stages.iter().any(|stage| stage == "evict"),
        "the evict stage did not run: executed={:?} skipped={:?}",
        report.executed_stages,
        report.skipped_stages
    );

    let released = engine.released_bucket_index_buckets(1);
    assert!(
        !released.is_empty(),
        "one round of the loop released no buckets, so no index memory could have been relieved"
    );
    let index_after = engine.bucket_index_resident_bytes(1);
    assert!(
        index_after < index_before,
        "the round freed no INDEX memory: {index_before} -> {index_after} over {} released buckets",
        released.len()
    );

    // THE FLOOR DID NOT MOVE, which is exactly why the pressure reading has to be a different
    // number. Gate on the floor and this round would read as having freed nothing.
    let floor_after = engine
        .get_stats(1)
        .stats
        .expect("stats for a loaded shard")
        .storage
        .bucket_index_resident_bytes_floor;
    assert_eq!(
        floor_after, floor_before,
        "the node-only floor moved on a release, so it is no longer the stable measurement"
    );

    // COLD reads. The warm path served correctly even when the cold one did not.
    let _ = engine.cache().invalidate_shard(1);
    let mut read = 0usize;
    let mut matched = 0usize;
    for index in 0..KEYS {
        read += 1;
        let value = match runtime
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: format!("loopevict-{index:06}"),
                },
            })
            .response
        {
            CommandResponse::Bytes { value } => value,
            other => panic!("a string read answered {other:?}"),
        };
        assert_eq!(
            value,
            Some(keyed_value(index)),
            "key {index} did not read back through a released bucket"
        );
        matched += 1;
    }
    assert_eq!(read, KEYS, "the read-back loop did not run {KEYS} times");
    assert_eq!(matched, KEYS, "only {matched} of {read} keys matched");
    eprintln!(
        "  [evict-loop] released {} buckets; index {index_before} -> {index_after} bytes \
(floor unchanged at {floor_before}); {matched}/{read} keys read back cold",
        released.len()
    );
}



// ---------------------------------------------------------------------------
// A dump manifest must hold back every slab the index it installs will need.
// ---------------------------------------------------------------------------

/// Buckets the slabpin fixture spreads over. Small on purpose, for the same reason #1642's
/// fixture is: picking keys that land in one chosen bucket of the whole u32 routing space costs
/// millions of candidate hashes, and the question here is untouched by how finely the shard is
/// cut.
const SLABPIN_BUCKETS: u32 = 8;

fn slabpin_load(engine: &TemporalEngine, shard_id: ShardId) {
    engine.load_shard_with(LoadShardRequest {
        shard_id,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: SLABPIN_BUCKETS - 1,
        readonly: false,
        table_name: String::new(),
    });
}

/// Big enough that the page lives in a block slab rather than inside its log record. A value small
/// enough to ride in the log is served from the log whatever the slabs hold, which would make
/// every count below say nothing about slabs.
fn slabpin_value(key: &str) -> Vec<u8> {
    let mut value = format!("value-{key}-").into_bytes();
    value.resize(4096, b'v');
    value
}

fn slabpin_write(engine: &TemporalEngine, shard_id: ShardId, key: &str) {
    let response = engine.execute(ExecuteRequest {
        shard_id,
        command: Command::StringSet {
            key: key.to_string(),
            value: slabpin_value(key),
        },
    });
    assert!(response.status.ok, "write {key} failed: {response:?}");
}

fn slabpin_reads_back(engine: &TemporalEngine, shard_id: ShardId, key: &str) -> bool {
    let response = engine.execute(ExecuteRequest {
        shard_id,
        command: Command::StringGet {
            key: key.to_string(),
        },
    });
    response.response
        == (CommandResponse::Bytes {
            value: Some(slabpin_value(key)),
        })
}

/// Install `manifest` into a target holding the shared page slabs and nothing else, and count the
/// two halves separately. NOTHING IS READ BEFORE THE INSTALL: a read answers out of the per-key
/// response cache, so a probe taken while the shard is still empty caches a MISS for that key and
/// every read after the install returns it -- which reads exactly like a loss, on both halves at
/// once. That cost two wrong readings of this fixture.
fn slabpin_restore_counts(
    label: &str,
    dir: &std::path::Path,
    pages_dir: &std::path::Path,
    manifest: &BucketDumpManifest,
    shard_id: ShardId,
    named_keys: &[String],
    outside_keys: &[String],
) -> (bool, usize, usize) {
    let restored = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.join(format!("{label}-cache")),
        pages_dir,
        dir.join(format!("{label}-indexes")),
    );
    assert!(
        restored.list_bucket_dump_manifests(shard_id).is_empty(),
        "the {label} restore target was handed a manifest it was supposed to recover without"
    );
    slabpin_load(&restored, shard_id);
    let installed = restored.install_bucket_dump_manifest(manifest).is_ok();
    let named = named_keys
        .iter()
        .filter(|key| slabpin_reads_back(&restored, shard_id, key))
        .count();
    let outside = outside_keys
        .iter()
        .filter(|key| slabpin_reads_back(&restored, shard_id, key))
        .count();
    (installed, named, outside)
}

/// The collector holds back the slabs a manifest NAMES; the manifest installs a WHOLE-SHARD index.
///
/// THE TWO SCOPES THAT DISAGREE. A bucket dump manifest carries `index_bytes` -- the whole-shard
/// index. #1642 measured that and proved it has to stay whole: install writes the decoded index as
/// THE durable index for the shard, retention keeps the newest manifest and nothing else, and a
/// manifest carrying only its own buckets would install a shard missing every bucket it did not
/// name (measured there: 5/5 named, 0/8 unnamed). It also carries `block_slab_ids`, and that was
/// only the slabs behind the DUMPED buckets' live page refs.
///
/// `block_slab_ids` is the whole of what the collector holds back -- `run_gc_inner` extends its
/// live slab set with it, `storage_page_gc_dependency_plan` blocks on it, and the page-GC retain
/// floor steps over it. So a slab holding nothing but UNNAMED-bucket pages was pinned by nothing.
/// Compaction relocates those pages onto a fresh slab, the old one goes stale, the sweep destroys
/// it, and the manifest's embedded index still points at it. Neither `validate_bucket_dump_manifest`
/// nor the install preflight objects, because both filter what they probe down to `bucket_ids`.
///
/// MEASURED, BEFORE THE FIX: install still returns Ok, the bucket the manifest NAMES restores
/// 5/5, and the buckets it does not name restore 0/8.
///
/// WHAT THIS BUILDS, and every step of it is load-bearing. The unnamed buckets' keys are written
/// FIRST, then the slab is rolled, then the named bucket's keys: that is what puts the two halves
/// on DIFFERENT slabs. Without the roll every slab holds both halves, every slab is named, and the
/// hazard is unreachable -- so the separation is a denominator, asserted before anything is read.
///
/// WHAT IT ASSERTS, IN ORDER. The denominators first, because each one makes every later count
/// pass for a reason that has nothing to do with the defect: the dump named ONE bucket, the shard
/// holds others, the two halves are on disjoint slabs, compaction actually vacated the unnamed
/// half's slabs, and -- the control -- the SAME manifest restores both halves before anything is
/// collected. Then the property, counting the NAMED and the UNNAMED buckets SEPARATELY: #1642 and
/// #1637 both found defects where the named half read full and hid a zero in the other half. Then
/// by which mechanism, which is that the sweep kept the unnamed half's slabs because the manifest
/// now names them.
#[test]
fn a_dump_manifest_holds_back_the_slabs_its_whole_shard_index_will_install() {
    const SHARD: ShardId = 4;
    const IN_NAMED: usize = 5;
    const OUTSIDE: usize = 8;
    const COMPACTION_ROUNDS: usize = 8;

    let dir = tempfile::tempdir().unwrap();
    let pages_dir = dir.path().join("pages");
    let source_index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        &pages_dir,
        &source_index_dir,
    );
    slabpin_load(&engine, SHARD);
    let named_bucket = engine.routing_bucket_for_key(SHARD, "named-0");

    let mut named_keys = Vec::new();
    let mut outside_keys = Vec::new();
    let mut candidate = 0usize;
    while named_keys.len() < IN_NAMED || outside_keys.len() < OUTSIDE {
        let key = format!("named-{candidate}");
        if engine.routing_bucket_for_key(SHARD, &key) == named_bucket {
            if named_keys.len() < IN_NAMED {
                named_keys.push(key);
            }
        } else if outside_keys.len() < OUTSIDE {
            outside_keys.push(key);
        }
        candidate += 1;
    }

    // The unnamed buckets' pages go on the slab the store is filling now.
    for key in outside_keys.iter() {
        slabpin_write(&engine, SHARD, key);
    }
    let outside_slabs = engine
        .live_block_slab_ids(SHARD)
        .into_iter()
        .collect::<BTreeSet<_>>();
    // Roll, so the named bucket's pages land somewhere else. This is what makes a slab holding
    // ONLY unnamed-bucket pages exist at all.
    engine.block_store().roll_slab().expect("roll a fresh slab");
    for key in named_keys.iter() {
        slabpin_write(&engine, SHARD, key);
    }
    let named_slabs = engine
        .live_block_slab_ids(SHARD)
        .into_iter()
        .filter(|slab| !outside_slabs.contains(slab))
        .collect::<BTreeSet<_>>();

    let manifest = engine
        .create_bucket_dump_manifest(SHARD, vec![named_bucket])
        .expect("a dump of one bucket should persist");

    // DENOMINATORS. Each one makes everything below vacuous if it does not hold.
    assert_eq!(
        manifest.bucket_ids,
        vec![named_bucket],
        "the dump was supposed to name ONE bucket and named {:?}; a dump naming every bucket \
         satisfies everything below without testing anything",
        manifest.bucket_ids
    );
    let shard_buckets = engine
        .bucket_storage_summaries(SHARD)
        .into_iter()
        .map(|summary| summary.routing_bucket)
        .collect::<BTreeSet<_>>();
    let unnamed_bucket_count = shard_buckets
        .iter()
        .filter(|bucket| **bucket != named_bucket)
        .count();
    assert!(
        unnamed_bucket_count > 0,
        "every key landed in the dumped bucket, so there are no unnamed buckets to lose (shard \
         holds buckets {shard_buckets:?}, the dump named {named_bucket})"
    );
    assert!(
        !outside_slabs.is_empty() && !named_slabs.is_empty(),
        "the roll did not separate the two halves: the unnamed buckets sit on {outside_slabs:?} \
         and the named bucket on {named_slabs:?}. Sharing a slab makes the manifest name it and \
         the hazard cannot arise"
    );

    // THE CONTROL, taken before anything is compacted or swept: the same manifest, installed into
    // a target holding the shared pages and nothing else, restores BOTH halves. Without it a zero
    // after the sweep says only that this fixture cannot restore, not that the sweep lost data.
    let control = slabpin_restore_counts(
        "control",
        dir.path(),
        &pages_dir,
        &manifest,
        SHARD,
        &named_keys,
        &outside_keys,
    );
    assert_eq!(
        control,
        (true, IN_NAMED, OUTSIDE),
        "the manifest could not restore its own shard before anything was collected \
         (installed, named, unnamed) = {control:?}, so the counts after the sweep would be \
         measuring a broken fixture"
    );

    // COMPACTION, WITH THE ROUNDS COMPUTED FROM THE BUDGET.
    //
    // A round relocates at most COMPACTION_ROUND_PAGE_REFS (2,048) page refs and
    // COMPACTION_ROUND_BYTES (256 MiB), then stops and leaves the rest to the next one. So the
    // rounds this fixture needs is ceil(live_page_refs / 2,048), and at IN_NAMED + OUTSIDE = 13
    // refs that is ONE. This runs COMPACTION_ROUNDS of them -- more than the work needs -- and
    // checks `pages_left_by_budget` per round, which is what actually says a round FINISHED.
    // Waiting for a round to relocate nothing never arrives: each round rolls a fresh slab and
    // moves every live page onto it, so a settled shard still reports a full round's work. That
    // is what the periodic path's relocation hint is for, and calling compaction directly does
    // not consult it.
    let live_page_refs = engine
        .bucket_storage_summaries(SHARD)
        .iter()
        .map(|summary| summary.page_ref_count)
        .sum::<u64>();
    let rounds_needed = live_page_refs.div_ceil(2_048).max(1) as usize;
    assert!(
        COMPACTION_ROUNDS > rounds_needed,
        "the fixture holds {live_page_refs} live page refs, which needs {rounds_needed} \
         compaction round(s); running only {COMPACTION_ROUNDS} would leave pages behind, and a \
         short run looks exactly like a settled one"
    );
    let mut relocated_total = 0usize;
    for round in 0..COMPACTION_ROUNDS {
        let report = engine
            .compact_shard_pages(SHARD)
            .expect("compaction should succeed");
        relocated_total += report.rewritten_page_refs;
        assert_eq!(
            report.pages_left_by_budget, 0,
            "compaction round {round} stopped on its budget, so the shard is half-moved and the \
             sweep below acts on it"
        );
    }
    assert!(
        relocated_total > 0,
        "compaction relocated nothing over {COMPACTION_ROUNDS} rounds, so no slab was vacated and \
         every count below is about an idle compactor"
    );
    let live_after_compaction = engine
        .live_block_slab_ids(SHARD)
        .into_iter()
        .collect::<BTreeSet<_>>();
    let still_live = outside_slabs
        .intersection(&live_after_compaction)
        .count();
    assert_eq!(
        still_live, 0,
        "compaction left {still_live} of the unnamed buckets' slabs {outside_slabs:?} live (live \
         now {live_after_compaction:?}), so nothing went stale and only a pin can be tested when \
         there is something to pin"
    );

    // THE SWEEP. The same aggressive operator sweep the /gc guard earlier in this file pins:
    // `run_gc_inner` keeps the live set plus every slab a durable manifest names, and nothing
    // else -- this is the complement of that guard, on the half it does not cover. The log
    // frontiers stay unset because the restore target below holds no log of its own -- the dump
    // is already the only thing that can rebuild the pre-dump state there, so reclaiming this
    // shard's log would change nothing and only add a variable.
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
            shard_id: SHARD,
            retain_wal_from_sequence: None,
            retain_index_log_from_sequence: None,
            retain_block_slabs_from_id: Some(u64::MAX),
            page_gc_delayed_destroy: false,
            page_gc_invalidate_removed_slabs_only: false,
        },
        RequestController { timeout_ms: 5000 },
    );
    let finished = wait_for_job(&runtime, submitted.job_id);
    let Some(DataNodeTaskOutput::Gc(output)) = finished.output else {
        panic!("expected gc output");
    };
    assert!(output.status.ok, "{:?}", output.status);

    // A CONTROL ON THE SOURCE. It reads every key back -- its own index points at the slabs
    // compaction wrote -- so nothing about the STORE is broken and the restore counts below are
    // about the MANIFEST.
    let source_named = named_keys
        .iter()
        .filter(|key| slabpin_reads_back(&engine, SHARD, key))
        .count();
    let source_outside = outside_keys
        .iter()
        .filter(|key| slabpin_reads_back(&engine, SHARD, key))
        .count();
    assert_eq!(
        (source_named, source_outside),
        (IN_NAMED, OUTSIDE),
        "the source engine lost keys of its own, so the restore below would measure a broken \
         fixture rather than a broken manifest"
    );

    // THE PROPERTY. The same manifest, the same restore, after the collector has run.
    let after = slabpin_restore_counts(
        "after",
        dir.path(),
        &pages_dir,
        &manifest,
        SHARD,
        &named_keys,
        &outside_keys,
    );
    let remaining = engine
        .block_store()
        .slab_ids()
        .unwrap_or_default()
        .into_iter()
        .collect::<BTreeSet<_>>();
    assert_eq!(
        after,
        (true, IN_NAMED, OUTSIDE),
        "restoring from the manifest after the sweep gave (installed, named, unnamed) = {after:?} \
         where the same manifest gave {control:?} before it. The shard's live slabs are now \
         {remaining:?}; the manifest names {:?} and the unnamed buckets' pages are on \
         {outside_slabs:?}. The middle number is the bucket the dump NAMES and the last is the \
         {unnamed_bucket_count} bucket(s) it does not -- install still succeeded, because both \
         validation and the install preflight filter what they probe down to bucket_ids",
        manifest.block_slab_ids
    );

    // AND BY WHICH MECHANISM. The slabs behind the unnamed buckets are still in the store,
    // because the manifest names them. A future change that keeps them some other way reads as a
    // change rather than as this still working.
    let destroyed = outside_slabs
        .iter()
        .copied()
        .filter(|slab| !remaining.contains(slab))
        .collect::<Vec<_>>();
    assert!(
        destroyed.is_empty(),
        "the sweep destroyed {destroyed:?}, slabs holding nothing but unnamed-bucket pages that \
         the manifest's whole-shard index still points at (manifest names {:?}, store holds \
         {remaining:?})",
        manifest.block_slab_ids
    );
}

// =================================================================================================
// LONG RUN: does any subsystem's footprint grow WITHOUT BOUND on a shard nobody writes to?
// =================================================================================================

/// Mirrors `engine::compaction::COMPACTION_ROUND_PAGE_REFS`, which is `pub(super)` and so cannot
/// be named from here.
///
/// It is used for ONE thing: computing how many rounds the relocation still owes before a tail
/// can be read as a trend. A 32-round run of 40 rounds of work looks exactly like a stall, and
/// the denominator assertion below turns that mistake into a failure with the arithmetic in the
/// message instead of a wrong verdict in a report.
const LONG_RUN_COMPACTION_ROUND_PAGE_REFS: usize = 2_048;

/// `StorageManagerRuntimeOptions::default().interval_ms`, the cadence a server actually runs the
/// storage manager at. Every per-day figure printed below is a per-round figure times this, and
/// the cadence is printed beside the figure so the extrapolation can be checked rather than
/// believed.
const LONG_RUN_SCHEDULER_INTERVAL_MS: u64 = 1_000;
const LONG_RUN_ROUNDS_PER_DAY: u64 = 24 * 60 * 60 * 1_000 / LONG_RUN_SCHEDULER_INTERVAL_MS;

/// What a single footprint column did across the tail of a run.
#[derive(Debug, Clone, PartialEq)]
enum FootprintVerdict {
    /// Never moved again after `flat_from`. The ceiling is the value it settled on.
    ///
    /// A column that climbs for the first N rounds and then stops is BOUNDED, not growing, and
    /// `flat_from` is the N.
    Bounded { flat_from: usize, ceiling: u64 },
    /// Fell at least once in the tail: the bytes are reclaimed, not accumulated. `period_rounds`
    /// is tail length over the number of falls, so it is the mean rounds between reclaims.
    Sawtooth {
        teeth: usize,
        period_rounds: f64,
        floor: u64,
        ceiling: u64,
    },
    /// Never fell in the tail and ended higher than it began. This is the #1565 / #1627 shape.
    Growing { per_round: f64, per_day: f64 },
}

impl FootprintVerdict {
    fn is_growing(&self) -> bool {
        matches!(self, FootprintVerdict::Growing { .. })
    }

    fn describe(&self) -> String {
        match self {
            FootprintVerdict::Bounded { flat_from, ceiling } => {
                format!("BOUNDED   flat from tail round {flat_from}, ceiling {ceiling}")
            }
            FootprintVerdict::Sawtooth {
                teeth,
                period_rounds,
                floor,
                ceiling,
            } => format!(
                "SAWTOOTH  {teeth} falls, period {period_rounds:.1} rounds, floor {floor}, \
                 ceiling {ceiling}"
            ),
            FootprintVerdict::Growing { per_round, per_day } => format!(
                "GROWING   {per_round:.1} per round -> {per_day:.0} per day at \
                 {LONG_RUN_SCHEDULER_INTERVAL_MS} ms/round"
            ),
        }
    }
}

/// Classify one column from the rounds AFTER the drain.
///
/// `tail` is already the post-drain slice, so nothing here can mistake a bounded relocation still
/// in progress for a trend -- that is the caller's denominator to establish, and it does.
fn classify_footprint_column(tail: &[u64]) -> FootprintVerdict {
    let ceiling = tail.iter().copied().max().unwrap_or(0);
    let floor = tail.iter().copied().min().unwrap_or(0);
    let mut last_change: Option<usize> = None;
    let mut falls = 0usize;
    for index in 1..tail.len() {
        if tail[index] != tail[index - 1] {
            last_change = Some(index);
        }
        if tail[index] < tail[index - 1] {
            falls += 1;
        }
    }
    let Some(last_change) = last_change else {
        return FootprintVerdict::Bounded {
            flat_from: 0,
            ceiling,
        };
    };
    if falls > 0 {
        return FootprintVerdict::Sawtooth {
            teeth: falls,
            period_rounds: tail.len() as f64 / falls as f64,
            floor,
            ceiling,
        };
    }
    // Rose and then stopped. "Stopped" has to be worth something, so it must hold still for a
    // quarter of the tail (at least four rounds) before this calls it flat rather than slow.
    let settled_margin = (tail.len() / 4).max(4);
    if tail.len().saturating_sub(1).saturating_sub(last_change) >= settled_margin {
        return FootprintVerdict::Bounded {
            flat_from: last_change,
            ceiling,
        };
    }
    let span = tail.len().saturating_sub(1).max(1) as f64;
    let per_round = (tail[tail.len() - 1] as f64 - tail[0] as f64) / span;
    FootprintVerdict::Growing {
        per_round,
        per_day: per_round * LONG_RUN_ROUNDS_PER_DAY as f64,
    }
}

struct LongFootprintRun {
    records: usize,
    rounds_run: usize,
    /// First round of the tail: the point past which the relocation cannot still owe work.
    tail_start: usize,
    columns: Vec<(&'static str, Vec<u64>)>,
    rounds_that_compacted: usize,
    last_compacting_round: Option<usize>,
    budget_drain_rounds: usize,
    buckets_at_end: u64,
    slabs_that_went_stale: usize,
    cumulative_removed: u64,
    cumulative_purged_by_schedule: u64,
    quarantined_at_end: usize,
    /// (slabs backdated, purged by the scheduled round, purged by a direct default-age call)
    injected_age_purge: Option<(usize, usize, usize)>,
}

impl LongFootprintRun {
    fn column(&self, name: &str) -> &[u64] {
        &self
            .columns
            .iter()
            .find(|(column, _)| *column == name)
            .unwrap_or_else(|| panic!("no column named {name}"))
            .1
    }

    fn tail(&self, name: &str) -> &[u64] {
        &self.column(name)[self.tail_start..]
    }

    fn verdict(&self, name: &str) -> FootprintVerdict {
        classify_footprint_column(self.tail(name))
    }
}

/// Write a corpus, stop writing, then run maintenance for a long time and record every
/// subsystem's footprint on every round.
///
/// `tail_rounds` is how many rounds run AFTER the relocation budget can possibly still owe work.
/// The drain itself is not part of the tail: peak footprint during a drain is the live set twice
/// over, once on each slab, and reading that as a trend is the error #1627 documented.
fn drive_long_footprint_run(records: usize, tail_rounds: usize, inject_age: bool) -> LongFootprintRun {
    // How many rounds does the relocation OWE? One round relocates at most
    // COMPACTION_ROUND_PAGE_REFS page refs, and the corpus is about one page ref per record, so
    // the drain needs at least records / budget rounds. Doubled and padded, because a round that
    // also has to dump, reclaim and prune does not spend its whole ref budget on relocation.
    let budget_drain_rounds = records.div_ceil(LONG_RUN_COMPACTION_ROUND_PAGE_REFS);
    let drain_cap = budget_drain_rounds * 2 + 8;
    let max_rounds = drain_cap + tail_rounds;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for index in 0..records {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("longrun-{index:07}"),
                value: vec![b'v'; 128],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    // Overwrite a third, so the collectors have genuine garbage. Without it every collector
    // correctly does nothing and a flat column proves only that nothing happened.
    for index in 0..records / 3 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("longrun-{index:07}"),
                value: vec![b'w'; 192],
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
    let engine = runtime.engine();
    let options = StorageManagerOptions::default();

    // FROM HERE ON NOTHING WRITES. `objects` is carried as a column so that is checkable rather
    // than asserted in prose: a run whose object count moves had a writer, and every "did not
    // grow" verdict below would be about a different experiment.
    eprintln!(
        "  [longrun] {records} records written, then NO further writes. relocation budget \
{LONG_RUN_COMPACTION_ROUND_PAGE_REFS} refs/round -> the drain owes at least \
{budget_drain_rounds} rounds; drain allowance {drain_cap}; tail {tail_rounds}; \
cap {max_rounds}"
    );
    eprintln!(
        "  [longrun] {:>5} {:>11} {:>8} {:>9} {:>6} {:>11} {:>10} {:>8} {:>9} {:>6} {:>8} {:>8} \
{:>8} {:>7}",
        "round",
        "wal_bytes",
        "wal_seq",
        "idx_bytes",
        "slabs",
        "slab_bytes",
        "cache_mem",
        "bkt_idx",
        "manifests",
        "dirty",
        "objects",
        "removed",
        "quarant",
        "purged",
    );

    let mut wal_bytes = Vec::new();
    let mut wal_seq = Vec::new();
    let mut idx_bytes = Vec::new();
    let mut slabs = Vec::new();
    let mut slab_bytes_series = Vec::new();
    let mut cache_mem_series = Vec::new();
    let mut bkt_idx_series = Vec::new();
    let mut manifests_series = Vec::new();
    let mut dirty_series = Vec::new();
    let mut objects_series = Vec::new();
    let mut removed_series = Vec::new();
    let mut quarantine_series = Vec::new();
    let mut purged_series = Vec::new();

    let mut rounds_that_compacted = 0usize;
    let mut last_compacting_round: Option<usize> = None;
    let mut went_stale = std::collections::BTreeSet::<u64>::new();
    let mut cumulative_removed = 0u64;
    let mut cumulative_purged = 0u64;

    for round in 0..max_rounds {
        let report = runtime.run_storage_manager_once(1, options.clone());
        if report
            .executed_stages
            .iter()
            .any(|stage| stage == "compact_pages")
        {
            rounds_that_compacted += 1;
            last_compacting_round = Some(round);
        }
        went_stale.extend(report.lifecycle_plan.stale_block_slab_ids.iter().copied());

        let wal = engine.write_ahead_log_store().stats(1);
        let index_log_bytes = engine.index_log_store().log_len_bytes(1);
        let slab_ids = engine.block_store().slab_ids().unwrap_or_default();
        let slab_bytes: u64 = engine
            .page_store()
            .slab_block_counts()
            .unwrap_or_default()
            .iter()
            .map(|(_id, physical_bytes, _count)| *physical_bytes)
            .sum();
        let cache_mem = engine.cache().stats().memory_bytes;
        let summaries = engine.bucket_storage_summaries(1);
        let bkt_idx = summaries.len() as u64;
        let objects: u64 = summaries.iter().map(|summary| summary.object_count).sum();
        let dirty = summaries
            .iter()
            .filter(|summary| summary.dirty_object_count > 0)
            .count() as u64;
        let manifests = engine.list_bucket_dump_manifests(1).len() as u64;
        let quarantined = engine
            .block_store()
            .delayed_destroy_slab_ids()
            .unwrap_or_default()
            .len() as u64;
        cumulative_removed += report
            .gc_report
            .as_ref()
            .map(|gc| gc.block_slabs_removed as u64)
            .unwrap_or(0);
        cumulative_purged += report
            .lifecycle_report
            .as_ref()
            .map(|lifecycle| lifecycle.delayed_destroy_purged_slabs.len() as u64)
            .unwrap_or(0);

        eprintln!(
            "  [longrun] {round:>5} {:>11} {:>8} {index_log_bytes:>9} {:>6} {slab_bytes:>11} \
{cache_mem:>10} {bkt_idx:>8} {manifests:>9} {dirty:>6} {objects:>8} {cumulative_removed:>8} \
{quarantined:>8} {cumulative_purged:>7}",
            wal.persistent_bytes,
            wal.last_sequence,
            slab_ids.len(),
        );

        wal_bytes.push(wal.persistent_bytes);
        wal_seq.push(wal.last_sequence);
        idx_bytes.push(index_log_bytes);
        slabs.push(slab_ids.len() as u64);
        slab_bytes_series.push(slab_bytes);
        cache_mem_series.push(cache_mem);
        bkt_idx_series.push(bkt_idx);
        manifests_series.push(manifests);
        dirty_series.push(dirty);
        objects_series.push(objects);
        removed_series.push(cumulative_removed);
        quarantine_series.push(quarantined);
        purged_series.push(cumulative_purged);
    }

    // THE HOUR. Quarantine enforces a one-hour minimum age, so every measurement shorter than an
    // hour reports "purged 0" BY CONSTRUCTION and says nothing about the mechanism. Backdating the
    // arrival stamp does NOT shorten the gate -- the purge below still demands the shipped hour --
    // it makes the slab genuinely old by its own clock. That proves the MECHANISM. Proving the
    // SCHEDULE is a different claim and needs a different run; see
    // `a_quarantined_slab_is_purged_after_a_real_hour_of_wall_clock`.
    let injected_age_purge = inject_age.then(|| {
        let backdated = engine
            .block_store()
            .backdate_delayed_destroy_stamps_for_test(2 * 60 * 60 * 1_000)
            .expect("backdating the quarantine stamps");
        let by_round = runtime
            .run_storage_manager_once(1, options.clone())
            .lifecycle_report
            .as_ref()
            .map(|lifecycle| lifecycle.delayed_destroy_purged_slabs.len())
            .unwrap_or(0);
        let directly = engine
            .block_store()
            .purge_delayed_destroy_slabs_with_report()
            .map(|report| report.purged_block_slab_ids.len())
            .unwrap_or(0);
        (backdated, by_round, directly)
    });

    let quarantined_at_end = engine
        .block_store()
        .delayed_destroy_slab_ids()
        .unwrap_or_default()
        .len();
    let buckets_at_end = engine.bucket_storage_summaries(1).len() as u64;

    LongFootprintRun {
        records,
        rounds_run: max_rounds,
        tail_start: drain_cap,
        columns: vec![
            ("wal_bytes", wal_bytes),
            ("wal_seq", wal_seq),
            ("idx_bytes", idx_bytes),
            ("slabs", slabs),
            ("slab_bytes", slab_bytes_series),
            ("cache_mem", cache_mem_series),
            ("bkt_idx", bkt_idx_series),
            ("manifests", manifests_series),
            ("dirty", dirty_series),
            ("objects", objects_series),
            ("removed", removed_series),
            ("quarantine", quarantine_series),
            ("purged", purged_series),
        ],
        rounds_that_compacted,
        last_compacting_round,
        budget_drain_rounds,
        buckets_at_end,
        slabs_that_went_stale: went_stale.len(),
        cumulative_removed,
        cumulative_purged_by_schedule: cumulative_purged,
        quarantined_at_end,
        injected_age_purge,
    }
}

/// Every denominator this run's verdicts depend on, asserted BEFORE any verdict is read.
///
/// A zero from "did not run" and a zero from "ran and did nothing" are different results, and the
/// only way to keep them apart is to assert the run happened first.
fn assert_long_run_denominators(run: &LongFootprintRun) {
    let records = run.records;
    assert!(
        run.rounds_run > run.tail_start,
        "at {records} records the run was {} rounds and the drain allowance alone is {}: there is \
         no tail to read",
        run.rounds_run,
        run.tail_start,
    );
    assert!(
        run.buckets_at_end > 0,
        "the shard holds no buckets at {records} records, so every column is trivially flat and \
         every verdict below is vacuous",
    );
    assert!(
        run.rounds_that_compacted > 0,
        "compaction never ran at {records} records over {} rounds, so this measures an idle \
         collector rather than a settled compactor",
        run.rounds_run,
    );
    assert!(
        run.slabs_that_went_stale > 0,
        "no slab went stale at {records} records over {} rounds, so the collector had nothing to \
         reclaim and the reclaim columns are vacuous",
        run.rounds_run,
    );
    // THE ARITHMETIC THAT KEEPS A DRAIN FROM READING AS A STALL. The relocation owes
    // ceil(records / COMPACTION_ROUND_PAGE_REFS) rounds at minimum; the tail starts past twice
    // that plus sixteen. If compaction is still firing there, the tail is drain and not trend.
    let last_compacting_round = run
        .last_compacting_round
        .expect("a compacting round, since one was counted");
    assert!(
        last_compacting_round < run.tail_start,
        "compaction was still firing at round {last_compacting_round} at {records} records, and \
         the tail starts at {}. The relocation owes at least {} rounds from the \
         {LONG_RUN_COMPACTION_ROUND_PAGE_REFS}-ref budget; raise the allowance rather than \
         reading this tail, because a run shorter than the work is indistinguishable from a stall",
        run.tail_start,
        run.budget_drain_rounds,
    );
    // THE WRITER ACTUALLY STOPPED. Not prose: the object count is a column, and a run where it
    // moved had a writer.
    let objects = run.column("objects");
    let first = objects[0];
    let last = objects[objects.len() - 1];
    assert!(first > 0, "the shard holds no objects at {records} records");
    assert_eq!(
        first, last,
        "the live object count moved from {first} to {last} at {records} records: something WROTE \
         during the rounds, and growth under a writer says nothing",
    );
}

/// Print every column's verdict with the arithmetic behind it.
fn report_long_run_verdicts(run: &LongFootprintRun) {
    eprintln!(
        "  [longrun] ---- {} records, tail = rounds {}..{} ({} rounds), \
{LONG_RUN_ROUNDS_PER_DAY} rounds/day at {LONG_RUN_SCHEDULER_INTERVAL_MS} ms ----",
        run.records,
        run.tail_start,
        run.rounds_run,
        run.rounds_run - run.tail_start,
    );
    for (name, _) in &run.columns {
        let tail = run.tail(name);
        let verdict = classify_footprint_column(tail);
        eprintln!(
            "  [longrun] {name:>11}  tail {:>12} -> {:>12}  delta {:>12}   {}",
            tail[0],
            tail[tail.len() - 1],
            tail[tail.len() - 1] as i128 - tail[0] as i128,
            verdict.describe(),
        );
    }
    eprintln!(
        "  [longrun] cumulative removed {} / quarantined now {} / purged by the schedule {}",
        run.cumulative_removed, run.quarantined_at_end, run.cumulative_purged_by_schedule,
    );
    if let Some((backdated, by_round, directly)) = run.injected_age_purge {
        eprintln!(
            "  [longrun] INJECTED AGE: {backdated} quarantined slabs backdated two hours, then the \
             shipped one-hour purge removed {by_round} in a scheduled round and {directly} on a \
             direct call",
        );
    }
}

/// THE LONG RUN. Release only, driven by hand.
///
///   cargo test --release -p temporalstore-rust --lib the_footprint_stays_bounded_over_a_long_run \
///       -- --ignored --nocapture --test-threads=1
///
/// WHAT THIS ADDS OVER `the_footprint_cadence_over_many_rounds`. That one runs twelve rounds, and
/// twelve rounds is shorter than the relocation drain at 80,000 records (which owes forty from its
/// 2,048-ref budget). So the twelve-round table cannot distinguish "growing" from "still moving
/// the live set", and it cannot reach any time-based threshold at all. This runs past the drain by
/// arithmetic and then keeps going long enough that a per-round slope is a trend rather than
/// noise, at BOTH corpus sizes, because #1623's healthy 8k slab column did not generalise to 80k.
///
/// WHAT IT DOES NOT DO: wait an hour. Quarantine's one-hour minimum age is crossed here by
/// backdating the arrival stamp, which proves the MECHANISM under the shipped gate. The SCHEDULE
/// -- that an unattended node eventually reaches the far side of that hour on its own -- is proved
/// by `a_quarantined_slab_is_purged_after_a_real_hour_of_wall_clock`, and they are different
/// claims. Both are here so neither is mistaken for the other.
#[test]
#[ignore]
fn the_footprint_stays_bounded_over_a_long_run() {
    for records in [8_000usize, 80_000usize] {
        let run = drive_long_footprint_run(records, 150, true);
        assert_long_run_denominators(&run);
        report_long_run_verdicts(&run);

        // The columns that must not grow on a shard with no writer. `slab_bytes` and `slabs` are
        // allowed to sawtooth -- that IS reclaim working -- but not to climb monotonically.
        for column in [
            "wal_bytes",
            "idx_bytes",
            "slabs",
            "slab_bytes",
            "cache_mem",
            "bkt_idx",
            "manifests",
        ] {
            let verdict = run.verdict(column);
            assert!(
                !verdict.is_growing(),
                "{column} at {records} records: {} over tail {:?}",
                verdict.describe(),
                run.tail(column),
            );
        }

        // The quarantine gate, under injected age. The denominator is the backdated count: a
        // purge of zero because nothing was backdated is not the same result as a purge of zero
        // because the gate is stuck.
        let (backdated, by_round, directly) = run
            .injected_age_purge
            .expect("the injected-age arm ran, since it was requested");
        assert!(
            backdated > 0,
            "nothing was in quarantine to backdate at {records} records, so the purge result \
             below is vacuous (cumulative removed {})",
            run.cumulative_removed,
        );
        assert!(
            by_round + directly > 0,
            "{backdated} slabs were two hours old and the shipped one-hour purge removed none of \
             them, by the scheduled round or directly",
        );
    }
}

/// THE SCHEDULE, not the mechanism: left alone for longer than the quarantine minimum age, does a
/// node purge on its own? Takes over an hour of wall clock. Release only, driven by hand.
///
///   cargo test --release -p temporalstore-rust --lib \
///       a_quarantined_slab_is_purged_after_a_real_hour_of_wall_clock \
///       -- --ignored --nocapture --test-threads=1
///
/// Nothing else in this repo has ever run past `DELAYED_DESTROY_MIN_AGE_MS`, so "purged 0" has
/// been the only observation available and it has been uninformative by construction. This waits.
/// The corpus is deliberately small -- the question is a clock, not a scale -- and the rounds are
/// paced so the run is mostly idle rather than mostly CPU.
#[test]
#[ignore]
fn a_quarantined_slab_is_purged_after_a_real_hour_of_wall_clock() {
    const RECORDS: usize = 8_000;
    const PACE: std::time::Duration = std::time::Duration::from_secs(20);
    // Ten minutes past the gate, so a slow round near the boundary does not decide the result.
    let wait_for = std::time::Duration::from_millis(60 * 60 * 1_000 + 10 * 60 * 1_000);

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for index in 0..RECORDS {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("hour-{index:07}"),
                value: vec![b'v'; 128],
            },
        });
    }
    for index in 0..RECORDS / 3 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("hour-{index:07}"),
                value: vec![b'w'; 192],
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
    let engine = runtime.engine();
    let options = StorageManagerOptions::default();

    let started = std::time::Instant::now();
    let mut ever_quarantined = std::collections::BTreeSet::<u64>::new();
    let mut purged = std::collections::BTreeSet::<u64>::new();
    let mut rounds = 0usize;
    eprintln!(
        "  [hour] {RECORDS} records, then NO further writes. Running rounds every {:?} until {:?} \
has passed; the quarantine minimum age is one hour.",
        PACE, wait_for,
    );
    while started.elapsed() < wait_for {
        let report = runtime.run_storage_manager_once(1, options.clone());
        rounds += 1;
        ever_quarantined.extend(
            engine
                .block_store()
                .delayed_destroy_slab_ids()
                .unwrap_or_default(),
        );
        if let Some(lifecycle) = report.lifecycle_report.as_ref() {
            purged.extend(lifecycle.delayed_destroy_purged_slabs.iter().copied());
        }
        if rounds % 15 == 0 {
            eprintln!(
                "  [hour] {:>6.1} min, round {rounds}: quarantine {:?}, ever quarantined {:?}, \
purged {:?}",
                started.elapsed().as_secs_f64() / 60.0,
                engine.block_store().delayed_destroy_slab_ids().unwrap_or_default(),
                ever_quarantined,
                purged,
            );
        }
        std::thread::sleep(PACE);
    }

    // DENOMINATORS. The elapsed time, the rounds, and something to purge -- in that order,
    // because a "nothing was purged" from a run that never reached the hour, a run that ran no
    // rounds, and a run with an empty quarantine are three different findings.
    let elapsed = started.elapsed();
    assert!(
        elapsed.as_millis() as u64 > 60 * 60 * 1_000,
        "the run lasted {elapsed:?}, less than the one-hour quarantine minimum age, so a purge of \
         zero would be uninformative by construction",
    );
    assert!(rounds > 0, "no maintenance round ran in {elapsed:?}");
    assert!(
        !ever_quarantined.is_empty(),
        "no slab ever entered quarantine over {rounds} rounds and {elapsed:?}, so nothing could \
         be purged and the assertion below would hold vacuously",
    );
    eprintln!(
        "  [hour] {rounds} rounds over {:.1} min: ever quarantined {ever_quarantined:?}, purged \
{purged:?}, still in quarantine {:?}",
        elapsed.as_secs_f64() / 60.0,
        engine.block_store().delayed_destroy_slab_ids().unwrap_or_default(),
    );
    assert!(
        !purged.is_empty(),
        "{} slabs sat in quarantine for more than the one-hour minimum age over {rounds} \
         scheduled rounds and NONE was purged: {ever_quarantined:?}",
        ever_quarantined.len(),
    );
}

/// The in-suite form of the long run: same driver, same verdicts, a corpus small enough to run on
/// every commit.
///
/// WHAT THIS CATCHES that the long run cannot: a regression, today, without an operator
/// remembering to spend an hour. WHAT IT CANNOT CATCH: a slope too shallow to show inside a short
/// tail, which is exactly what the ignored long form is for. Both, or neither is worth having.
///
/// The quarantine gate is checked in BOTH directions off the one run, because each direction on
/// its own is passable by a broken store. "A backdated slab is purged" alone still passes if the
/// age check is deleted outright -- a purge that takes everything takes the backdated ones too --
/// and "a fresh slab is not purged" alone still passes if the purge never runs at all.
#[test]
fn the_footprint_columns_stay_bounded_on_an_idle_shard() {
    let run = drive_long_footprint_run(4_096, 12, true);
    assert_long_run_denominators(&run);
    report_long_run_verdicts(&run);

    for column in [
        "wal_bytes",
        "idx_bytes",
        "slabs",
        "slab_bytes",
        "cache_mem",
        "bkt_idx",
        "manifests",
    ] {
        let verdict = run.verdict(column);
        assert!(
            !verdict.is_growing(),
            "{column} on an idle shard: {} over tail {:?}",
            verdict.describe(),
            run.tail(column),
        );
    }

    // The dirty set must reach zero and stay there: that is what "the writer stopped" means to the
    // store manager, and a non-zero dirty column would mean every reclaim above was being taken
    // against a moving target.
    let dirty_tail = run.tail("dirty");
    assert!(
        dirty_tail.iter().all(|dirty| *dirty == 0),
        "buckets stayed dirty with no writer: {dirty_tail:?}",
    );

    // DIRECTION ONE: a slab quarantined moments ago is NOT purged by the scheduled rounds. The
    // denominator is the quarantine column at the last round before any age was injected -- with
    // an empty quarantine the zero below is a statement about a shard that reclaimed nothing.
    let quarantine = run.column("quarantine");
    let quarantined_before_injection = quarantine[quarantine.len() - 1];
    assert!(
        quarantined_before_injection > 0,
        "nothing was in quarantine after {} rounds, so both directions of the age gate are \
         vacuous here (cumulative removed {})",
        run.rounds_run,
        run.cumulative_removed,
    );
    assert_eq!(
        run.cumulative_purged_by_schedule, 0,
        "slabs quarantined moments ago were purged over {} scheduled rounds: the one-hour minimum \
         age is not being enforced, and a reader holding a stale address dangles at a deleted slab",
        run.rounds_run,
    );

    // DIRECTION TWO: the same slab, aged past the shipped hour, IS purged.
    let (backdated, by_round, directly) = run
        .injected_age_purge
        .expect("the injected-age arm ran, since it was requested");
    assert!(
        backdated > 0,
        "nothing was backdated, so the purge result is vacuous (cumulative removed {})",
        run.cumulative_removed,
    );
    assert!(
        by_round + directly > 0,
        "{backdated} slabs were two hours old and the shipped one-hour purge removed none",
    );
    assert_eq!(
        run.quarantined_at_end, 0,
        "quarantine still holds slabs after the aged purge: {backdated} backdated, {by_round} \
         purged by the round, {directly} purged directly",
    );
}

/// The fixture the two rounds above and below are measured on: a shard spanning several slabs
/// with dead space on exactly ONE of them.
///
/// Returned as a runtime because every caller wants the maintenance round's plan, which is where
/// the drain set comes from. `holed_batch` names which batch gets overwritten.
#[cfg(test)]
fn drain_fixture(
    dir: &std::path::Path,
    batches: usize,
    keys_per_batch: usize,
    holed_keys: usize,
) -> (DataNodeRuntime, BTreeSet<u64>) {
    let engine = TemporalEngine::with_local_dirs(
        8 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    for batch in 0..batches {
        for index in 0..keys_per_batch {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("drain-{batch}-{index:05}"),
                    value: vec![b'v'; 256],
                },
            });
            assert!(
                response.status.ok,
                "write {batch}/{index}: {:?}",
                response.status
            );
        }
        // An explicit target rather than the process-wide one: no env var is touched, so no other
        // test in this process inherits a small slab.
        engine
            .page_store()
            .prepare_next_slab_with_target(1)
            .expect("rolling a slab between batches should succeed");
    }
    let slabs = engine
        .live_block_slab_ids(1)
        .into_iter()
        .collect::<BTreeSet<_>>();
    // Hole ONE slab: an overwrite writes a fresh page on the CURRENT slab and leaves the old page
    // on batch 0's slab dead. Nothing else in the shard acquires dead space.
    for index in 0..holed_keys {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("drain-0-{index:05}"),
                value: vec![b'w'; 320],
            },
        });
        assert!(response.status.ok, "overwrite {index}: {:?}", response.status);
    }
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    (runtime, slabs)
}

/// A DRAINING round moves the pages on the slabs it was asked to empty, and leaves the rest.
///
/// This is the round the periodic loop now issues. The direct form above still relocates
/// everything -- an operator instruction is not a suggestion -- and the two are measured on the
/// SAME fixture so the difference is the selection rule and nothing else.
///
/// FOUR HALVES, ASSERTED SEPARATELY, because each of them going to zero looks like success in a
/// combined count: what the round moved, what it declined, what it left for want of budget, and
/// whether the holed slab actually went stale. A round that moved NOTHING would satisfy "moved no
/// more than the drain set" perfectly.
#[test]
fn a_draining_round_moves_only_the_pages_on_the_slabs_it_was_asked_to_empty() {
    const BATCHES: usize = 6;
    const KEYS_PER_BATCH: usize = 200;
    const HOLED_KEYS: usize = 20;

    let dir = tempfile::tempdir().unwrap();
    let (runtime, slabs_before) =
        drain_fixture(dir.path(), BATCHES, KEYS_PER_BATCH, HOLED_KEYS);
    let engine = runtime.engine();
    let options = StorageManagerOptions::default();
    let (_pressure, plan) = runtime.storage_manager_pressure_snapshot(1, &options);
    let drain_slabs = crate::engine::compaction_drain_block_slab_ids(&plan.reclaim_candidates);
    let relocatable = crate::engine::compaction_relocatable_page_refs(&plan.reclaim_candidates);
    let live_page_refs = engine
        .bucket_storage_summaries(1)
        .iter()
        .map(|summary| summary.page_ref_count)
        .sum::<u64>();

    // DENOMINATORS. Each of these makes every count below trivially true for a reason that has
    // nothing to do with the selection rule.
    assert!(
        slabs_before.len() >= BATCHES,
        "the fixture rolled {} slab(s) for {BATCHES} batches ({slabs_before:?}); with one slab \
         the drain set IS the live set and there is nothing to select",
        slabs_before.len()
    );
    assert_eq!(
        drain_slabs.len(),
        1,
        "the overwrites holed {} slabs ({drain_slabs:?}) rather than one",
        drain_slabs.len()
    );
    assert!(
        relocatable > 0,
        "no object holds a page on the holed slab, so the round below has nothing to drain: \
         {plan:?}"
    );
    assert!(
        live_page_refs > relocatable * 2,
        "the shard holds {live_page_refs} live page refs and {relocatable} of them are on the \
         holed slab; without a wide margin, draining and relocating everything are the same act"
    );

    let report = engine
        .compact_shard_pages_draining(1, drain_slabs.clone())
        .expect("a draining round should succeed");

    // HALF ONE: it moved the drain set, all of it, and nothing else.
    assert_eq!(
        report.rewritten_page_refs as u64, relocatable,
        "the draining round moved {} page refs where the plan named {relocatable} on the slabs it \
         was asked to empty ({drain_slabs:?})",
        report.rewritten_page_refs
    );
    // HALF TWO: it actually DECLINED pages. Without this, moving the drain set and moving
    // everything are indistinguishable here.
    assert!(
        report.pages_left_off_drain_set > 0,
        "the round declined no pages at all, so it cannot be shown to have selected anything: it \
         moved {} of {live_page_refs} live refs",
        report.rewritten_page_refs
    );
    assert_eq!(
        report.pages_left_off_drain_set as u64 + report.rewritten_page_refs as u64,
        live_page_refs,
        "the round moved {} and declined {}, which does not account for the shard's \
         {live_page_refs} live refs -- some page was neither considered nor moved",
        report.rewritten_page_refs,
        report.pages_left_off_drain_set
    );
    // HALF THREE: declining is not "unfinished". A round kept open by pages nobody will ever want
    // moved never closes, which is the termination hazard this ordering exists to avoid.
    assert_eq!(
        report.pages_left_by_budget, 0,
        "the round reports {} pages left for want of budget after declining {} off the drain set \
         -- the two are being counted together, and a round that reports unfinished for ever \
         re-runs for ever",
        report.pages_left_by_budget, report.pages_left_off_drain_set
    );

    // HALF FOUR: the point of the whole exercise -- the holed slab is now empty and the dense
    // ones are untouched.
    let slabs_after = engine
        .live_block_slab_ids(1)
        .into_iter()
        .collect::<BTreeSet<_>>();
    for drained in &drain_slabs {
        assert!(
            !slabs_after.contains(drained),
            "slab {drained} was drained but still holds live pages ({slabs_after:?})"
        );
    }
    let untouched = slabs_before
        .iter()
        .filter(|slab| !drain_slabs.contains(slab))
        .filter(|slab| slabs_after.contains(slab))
        .count();
    assert!(
        untouched > 0,
        "every slab of the fixture was vacated, so the round did not leave the dense ones alone: \
         before {slabs_before:?}, drained {drain_slabs:?}, after {slabs_after:?}"
    );

    // AND IT TERMINATES. Nobody wrote to the shard, so a second plan must find nothing left to
    // relocate -- that is the gate the periodic loop reads, and a non-zero here is the
    // self-retrigger coming back in a new shape.
    let (_pressure_after, plan_after) = runtime.storage_manager_pressure_snapshot(1, &options);
    let relocatable_after =
        crate::engine::compaction_relocatable_page_refs(&plan_after.reclaim_candidates);
    assert_eq!(
        relocatable_after, 0,
        "after draining the only holed slab, {relocatable_after} page refs still sit on a slab \
         someone wants emptied, so the loop would compact again on an idle shard: {plan_after:?}"
    );

    // EVERY VALUE STILL READS, on both sides of the selection: the moved half and the declined
    // half. A selection rule that drops a page reads exactly like one that is simply cheaper.
    for index in 0..KEYS_PER_BATCH {
        let expected = if index < HOLED_KEYS {
            vec![b'w'; 320]
        } else {
            vec![b'v'; 256]
        };
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: format!("drain-0-{index:05}"),
            },
        });
        assert_eq!(
            response.response,
            CommandResponse::Bytes {
                value: Some(expected)
            },
            "drain-0-{index:05} did not read back after the draining round"
        );
    }
    for batch in 1..BATCHES {
        for index in 0..KEYS_PER_BATCH {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: format!("drain-{batch}-{index:05}"),
                },
            });
            assert_eq!(
                response.response,
                CommandResponse::Bytes {
                    value: Some(vec![b'v'; 256])
                },
                "drain-{batch}-{index:05} did not read back after the draining round"
            );
        }
    }
}

/// The two selection rules driven to SETTLEMENT at both corpus sizes, in columns. Release only.
///
///   cargo test --release -p temporalstore-rust --lib \
///       what_the_two_selection_rules_move_at_both_corpus_sizes -- --ignored --nocapture \
///       --test-threads=1
///
/// PER ROUND IS THE WRONG UNIT and measuring it that way reads as no difference at all: a round
/// relocates at most COMPACTION_ROUND_PAGE_REFS (2,048) refs whichever rule it uses, so at 8,000
/// records the direct round reports 2,048 and the draining round 980, and the direct arm looks
/// only twice as expensive. It is not twice -- it is 2,048 four times over against 980 once.
/// The comparable quantity is the TOTAL relocated to reach a settled shard, and the round count
/// each needs is arithmetic off the budget, not something to discover by watching for a zero.
///
/// WHY A SECOND SIZE. The amplification grows with the LIVE SET and not with the garbage: a
/// direct settlement rewrites every live page whatever the hole cost, so ten times the corpus is
/// ten times the work to recover the same twenty pages, while the draining settlement is the size
/// of the holed slab either way. An 8,000-record reading would not have generalised.
///
/// Both arms build their OWN fixture, so the direct arm is never measuring a shard the draining
/// arm already drained.
#[test]
#[ignore]
fn what_the_two_selection_rules_move_at_both_corpus_sizes() {
    const BATCHES: usize = 8;
    const HOLED_KEYS: usize = 20;
    const BUDGET: u64 = 2_048;

    println!(
        "{:>8} {:>6} {:>6} {:>8} {:>9} {:>7} {:>9} {:>7} {:>7}",
        "records", "slabs", "drain", "onslab", "directref", "rounds", "drainref", "rounds", "ratio"
    );
    for records in [8_000_usize, 80_000_usize] {
        let keys_per_batch = records / BATCHES;
        let options = StorageManagerOptions::default();

        // ARM ONE: the direct rule, run until a round leaves nothing for want of budget. The
        // rounds it needs is ceil(live refs / budget), computed BEFORE the loop so a short run
        // cannot be mistaken for a settled one.
        let direct_dir = tempfile::tempdir().unwrap();
        let (direct_runtime, direct_slabs) =
            drain_fixture(direct_dir.path(), BATCHES, keys_per_batch, HOLED_KEYS);
        let direct_engine = direct_runtime.engine();
        let (_p, direct_plan) = direct_runtime.storage_manager_pressure_snapshot(1, &options);
        let drain_slabs =
            crate::engine::compaction_drain_block_slab_ids(&direct_plan.reclaim_candidates);
        let relocatable =
            crate::engine::compaction_relocatable_page_refs(&direct_plan.reclaim_candidates);
        let live_page_refs = direct_engine
            .bucket_storage_summaries(1)
            .iter()
            .map(|summary| summary.page_ref_count)
            .sum::<u64>();
        let direct_rounds_needed = live_page_refs.div_ceil(BUDGET).max(1);
        let mut direct_refs = 0_u64;
        let mut direct_rounds = 0_u64;
        loop {
            let round = direct_engine
                .compact_shard_pages(1)
                .expect("a direct round should succeed");
            direct_refs += round.rewritten_page_refs as u64;
            direct_rounds += 1;
            if round.pages_left_by_budget == 0 {
                break;
            }
            assert!(
                direct_rounds <= direct_rounds_needed + 2,
                "{records}: the direct arm ran {direct_rounds} rounds where the budget predicts \
                 {direct_rounds_needed} for {live_page_refs} live refs, so it is not converging"
            );
        }

        // ARM TWO: the draining rule, run until the plan has nothing left on a slab anyone wants
        // emptied. That zero is the gate the periodic loop reads, so reaching it IS settlement --
        // unlike the direct arm, which reports a full round's work on a settled shard for ever.
        let draining_dir = tempfile::tempdir().unwrap();
        let (draining_runtime, _) =
            drain_fixture(draining_dir.path(), BATCHES, keys_per_batch, HOLED_KEYS);
        let draining_engine = draining_runtime.engine();
        let draining_rounds_needed = relocatable.div_ceil(BUDGET).max(1);
        let mut draining_refs = 0_u64;
        let mut draining_rounds = 0_u64;
        loop {
            let (_p2, plan) = draining_runtime.storage_manager_pressure_snapshot(1, &options);
            let left = crate::engine::compaction_relocatable_page_refs(&plan.reclaim_candidates);
            if left == 0 {
                break;
            }
            let set = crate::engine::compaction_drain_block_slab_ids(&plan.reclaim_candidates);
            let round = draining_engine
                .compact_shard_pages_draining(1, set)
                .expect("a draining round should succeed");
            draining_refs += round.rewritten_page_refs as u64;
            draining_rounds += 1;
            assert!(
                draining_rounds <= draining_rounds_needed + 2,
                "{records}: the draining arm ran {draining_rounds} rounds where the budget \
                 predicts {draining_rounds_needed} for {relocatable} refs on the holed slab, so \
                 it is not converging"
            );
        }

        println!(
            "{:>8} {:>6} {:>6} {:>8} {:>9} {:>7} {:>9} {:>7} {:>6.1}x",
            records,
            direct_slabs.len(),
            drain_slabs.len(),
            relocatable,
            direct_refs,
            direct_rounds,
            draining_refs,
            draining_rounds,
            direct_refs as f64 / (draining_refs.max(1) as f64),
        );

        // DENOMINATORS at each size, so a column of zeroes cannot read as a clean result.
        assert_eq!(
            drain_slabs.len(),
            1,
            "{records}: the fixture holed {} slabs rather than one",
            drain_slabs.len()
        );
        assert!(
            relocatable > 0 && direct_refs > 0 && draining_refs > 0,
            "{records}: an arm moved nothing -- on-slab {relocatable}, direct {direct_refs}, \
             draining {draining_refs}"
        );
        // THE CLAIM, at BOTH sizes and asserted as separate halves: the draining settlement costs
        // the size of the HOLE, and the direct settlement costs the size of the STORE.
        assert_eq!(
            draining_refs, relocatable,
            "{records}: draining settled at {draining_refs} refs where the holed slab carried \
             {relocatable}"
        );
        assert!(
            direct_refs >= live_page_refs,
            "{records}: the direct settlement moved {direct_refs} of {live_page_refs} live \
             refs, so it did not rewrite the whole live set and is not the rule being compared"
        );
        assert_eq!(
            direct_rounds, direct_rounds_needed,
            "{records}: the direct arm took {direct_rounds} rounds where the budget predicts \
             {direct_rounds_needed}"
        );
        assert_eq!(
            draining_rounds, draining_rounds_needed,
            "{records}: the draining arm took {draining_rounds} rounds where the budget \
             predicts {draining_rounds_needed}"
        );
    }
}

/// WHAT A DIRECT ROUND MOVES, MEASURED AGAINST WHAT MOVING IT RECOVERS.
///
/// A relocation recovers space only for the slab it VACATES. `compact_page_addresses` copies a
/// page's bytes verbatim and appends them elsewhere, so a page moved off a slab with no dead
/// space comes out byte for byte what it went in as, on a different slab -- and the slab it left
/// is now entirely dead. Same live bytes, one more emptied slab for the collector to destroy.
///
/// The set worth moving is already named: `compaction_drain_block_slab_ids` is the slabs carrying
/// dead space that objects still hold pages on, and `compaction_relocatable_page_refs` counts the
/// pages on them. `compact_shard_pages` -- the operator RPC, the on-demand cycle and the suite --
/// does not consult it and relocates every live page, BY INSTRUCTION. This records that, and it
/// is the control for the draining round below.
///
/// At suite scale the distinction has been invisible, which is why it was never measured: a few
/// thousand small records fit in ONE slab against the 1 GiB target, so "every live page" and "the
/// pages on the holed slab" are the same set. The fixture rolls between batches so the shard
/// spans slabs, and holes exactly one of them.
#[test]
fn a_direct_round_relocates_every_live_page_not_only_the_pages_on_holed_slabs() {
    const BATCHES: usize = 6;
    const KEYS_PER_BATCH: usize = 200;
    const HOLED_KEYS: usize = 20;

    let dir = tempfile::tempdir().unwrap();
    let (runtime, slabs_before) = drain_fixture(dir.path(), BATCHES, KEYS_PER_BATCH, HOLED_KEYS);
    let engine = runtime.engine();
    let options = StorageManagerOptions::default();
    let (_pressure, plan) = runtime.storage_manager_pressure_snapshot(1, &options);
    let drain_slabs = crate::engine::compaction_drain_block_slab_ids(&plan.reclaim_candidates);
    let relocatable = crate::engine::compaction_relocatable_page_refs(&plan.reclaim_candidates);
    let live_page_refs = engine
        .bucket_storage_summaries(1)
        .iter()
        .map(|summary| summary.page_ref_count)
        .sum::<u64>();

    // DENOMINATORS, before any ratio: several slabs, exactly one of them holed, something on it.
    assert!(
        slabs_before.len() >= BATCHES,
        "the fixture rolled {} slab(s) for {BATCHES} batches ({slabs_before:?}), so the shard \
         does not span slabs and the two counts below cannot differ",
        slabs_before.len()
    );
    assert_eq!(
        drain_slabs.len(),
        1,
        "the overwrites holed {} slabs ({drain_slabs:?}) rather than one, so the ratio below is \
         not the one this test is about",
        drain_slabs.len()
    );
    assert!(
        relocatable > 0,
        "no object holds a page on the holed slab, so the round below has nothing to drain and \
         what it reports is about an idle compactor: {plan:?}"
    );
    assert!(
        live_page_refs > relocatable,
        "the shard holds {live_page_refs} live page refs and {relocatable} of them are on the \
         holed slab; with those equal, relocating everything and draining the holed slab are the \
         same act"
    );

    let report = engine
        .compact_shard_pages(1)
        .expect("a compaction round should succeed");
    assert_eq!(
        report.pages_left_by_budget, 0,
        "the round stopped on its budget, so what it moved is a budget artefact rather than its \
         selection rule"
    );

    // THE TWO HALVES, SEPARATELY. One says the round moved the whole live set; the other says the
    // set that moving recovers anything for is far smaller. A single ratio would hide either half
    // going to zero.
    assert!(
        report.rewritten_page_refs as u64 >= live_page_refs,
        "the direct form is documented to relocate every live page: it moved {} of \
         {live_page_refs}",
        report.rewritten_page_refs
    );
    assert_eq!(
        report.pages_left_off_drain_set, 0,
        "a direct round declines nothing -- it was given no drain set -- yet it left {} pages \
         off one",
        report.pages_left_off_drain_set
    );
    assert!(
        report.rewritten_page_refs as u64 > relocatable * 2,
        "the round moved {} page refs where {relocatable} sit on the only slab that carries dead \
         space, across {} slabs. Equal counts would mean it is already moving just the drain set",
        report.rewritten_page_refs,
        slabs_before.len()
    );
}

/// The PERIODIC round is the one that drains. Without this the wiring is untested: both
/// selection rules exist and compile, and nothing says which one the loop actually issues.
///
/// The fixture is the one the two rounds above use, so the expected numbers are the same, and
/// the round here is driven through `run_storage_manager_once` -- the real maintenance path,
/// gate and all -- rather than by calling a compaction entry point directly.
///
/// HALVES SEPARATELY: that compaction RAN (a loop which skipped the stage leaves the dense slabs
/// alone for the wrong reason, and that is exactly what this would otherwise read as success),
/// that the holed slab was drained, and that the dense slabs were left where they were.
#[test]
fn the_periodic_round_drains_the_holed_slab_and_leaves_the_dense_ones() {
    const BATCHES: usize = 6;
    const KEYS_PER_BATCH: usize = 200;
    const HOLED_KEYS: usize = 20;

    let dir = tempfile::tempdir().unwrap();
    let (runtime, slabs_before) = drain_fixture(dir.path(), BATCHES, KEYS_PER_BATCH, HOLED_KEYS);
    let engine = runtime.engine();
    let options = StorageManagerOptions::default();
    let (_pressure, plan) = runtime.storage_manager_pressure_snapshot(1, &options);
    let drain_slabs = crate::engine::compaction_drain_block_slab_ids(&plan.reclaim_candidates);
    let relocatable = crate::engine::compaction_relocatable_page_refs(&plan.reclaim_candidates);
    let live_page_refs = engine
        .bucket_storage_summaries(1)
        .iter()
        .map(|summary| summary.page_ref_count)
        .sum::<u64>();
    assert_eq!(
        drain_slabs.len(),
        1,
        "the fixture holed {} slabs ({drain_slabs:?}) rather than one",
        drain_slabs.len()
    );
    assert!(
        relocatable > 0 && live_page_refs > relocatable * 2,
        "the fixture left {relocatable} refs on the holed slab of {live_page_refs} live, which is \
         not a wide enough margin for the counts below to mean anything"
    );

    let report = runtime.run_storage_manager_once(1, options.clone());

    // DENOMINATOR: the stage ran. A loop that skipped compaction leaves every dense slab alone
    // too, and would satisfy the second half below perfectly.
    assert!(
        report
            .executed_stages
            .iter()
            .any(|stage| stage == "compact_pages"),
        "the maintenance round did not compact, so nothing below is about the selection rule: \
         executed {:?}, skipped {:?}",
        report.executed_stages,
        report.skipped_stages
    );
    let compaction = report
        .compaction_report
        .as_ref()
        .expect("the round reported running compaction, so it must carry its report");
    assert_eq!(
        compaction.compacted_objects as u64, relocatable,
        "the periodic round moved {} page refs where the plan named {relocatable} on the slab it \
         wanted emptied -- the loop is still issuing a whole-shard relocation",
        compaction.compacted_objects
    );

    // The dense slabs are still live; the holed one is not.
    let slabs_after = engine
        .live_block_slab_ids(1)
        .into_iter()
        .collect::<BTreeSet<_>>();
    for drained in &drain_slabs {
        assert!(
            !slabs_after.contains(drained),
            "the periodic round left slab {drained} holding live pages ({slabs_after:?})"
        );
    }
    let untouched = slabs_before
        .iter()
        .filter(|slab| !drain_slabs.contains(slab))
        .filter(|slab| slabs_after.contains(slab))
        .count();
    assert!(
        untouched >= BATCHES - 1,
        "the periodic round vacated slabs it had no reason to: {untouched} of the {} dense slabs \
         are still live (before {slabs_before:?}, drained {drain_slabs:?}, after {slabs_after:?})",
        BATCHES - 1
    );
}

#[test]
// shared-corpus: storage_dump_load_recovery storage_cache_refill;
fn a_maintenance_round_counts_as_a_run_on_the_path_the_server_actually_uses() {
    // The scrape exports ONE number for storage-manager activity:
    // temporalstore_data_node_runtime_jobs_total{kind="storage_manager"}, which reads
    // `storage_manager_runs`. Two paths drive maintenance and they disagreed about that field.
    // The queued path counts a run AND a round; the periodic scheduler counted only a round.
    //
    // A server started as shipped drives maintenance from the scheduler and never queues a
    // storage-manager task, so the exported counter sat at zero while maintenance ran every
    // thirty seconds -- a number an operator cannot act on, because a working loop and a stopped
    // one both read zero.
    //
    // The two fields are asserted APART. Counting them together hid this: the pair moved, and it
    // was only ever the unexported one moving.
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine.clone(), 8);

    for (key, value) in [
        ("run-count-a", b"one".to_vec()),
        ("run-count-a", b"two".to_vec()),
        ("run-count-b", b"three".to_vec()),
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

    // ---- Denominator: nothing has run yet, so a later "it climbed" is not reading a head start.
    let before = runtime.stats();
    assert_eq!(
        before.storage_manager_runs, 0,
        "premise: no run counted before the first round"
    );
    assert_eq!(
        before.storage_manager_loops, 0,
        "premise: no round counted before the first round"
    );

    let options = StorageManagerOptions {
        max_dump_buckets_per_round: 16,
        min_undumped_wal_records: 1,
        dirty_bucket_pressure: 1,
        ..StorageManagerOptions::default()
    };
    let report = runtime.run_storage_manager_once(1, options.clone());

    // ---- Denominator: the round did work, so counting it is counting something.
    assert!(report.status.ok, "{report:?}");
    assert!(
        !report.executed_stages.is_empty(),
        "premise: the round executed at least one stage, got {:?}",
        report.executed_stages
    );

    // ---- Half one: the EXPORTED field. This is the half that stood at zero.
    let after = runtime.stats();
    assert_eq!(
        after.storage_manager_runs, 1,
        "the scheduler ran a maintenance round and {} runs were counted -- this is the field the \
         scrape exports, so the only exported sign of maintenance would stay flat while it ran",
        after.storage_manager_runs
    );

    // ---- Half two: the UNEXPORTED field. This half was always right, and moving alone is what
    // made the surface look alive from inside the process and dead from outside it.
    assert_eq!(
        after.storage_manager_loops, 1,
        "the round counter must still count the round, got {}",
        after.storage_manager_loops
    );

    // ---- The two must not drift apart again across rounds.
    runtime.run_storage_manager_once(1, options.clone());
    runtime.run_storage_manager_once(1, options);
    let later = runtime.stats();
    assert_eq!(later.storage_manager_loops, 3, "three rounds were driven");
    assert_eq!(
        later.storage_manager_runs, later.storage_manager_loops,
        "every round on this path is a run of the storage manager: {} runs against {} rounds",
        later.storage_manager_runs, later.storage_manager_loops
    );

    // ---- And the export still reads the field this test just moved. The renderer lives in the
    // server binary, which this suite does not link, so the link is pinned by reading it. Without
    // this, someone could point the metric at a different field and both halves above would still
    // pass while the scrape went flat again.
    let renderer = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("bin")
        .join("server")
        .join("metrics.rs");
    let text = std::fs::read_to_string(&renderer)
        .unwrap_or_else(|err| panic!("cannot read {}: {err}", renderer.display()));
    assert!(
        text.len() > 1000,
        "premise: read the renderer, not an empty file -- got {} bytes",
        text.len()
    );
    assert!(
        text.contains("temporalstore_data_node_runtime_jobs_total"),
        "premise: this is the file that renders the job counters"
    );
    assert!(
        text.contains("(\"storage_manager\", stats.storage_manager_runs)"),
        "the exported storage_manager job counter must read storage_manager_runs, the field the \
         scheduler increments; if this moved, the scrape no longer reports what this test proved"
    );
}

/// A stub role source, so a test can put the node in any one of the three states by hand.
#[derive(Debug)]
struct FixedShardLeadership(ShardLeadership);

impl ShardLeadershipSource for FixedShardLeadership {
    fn shard_leadership(&self, _shard_id: ShardId) -> ShardLeadership {
        self.0
    }
}

/// One arm of the role guard: a fixture that genuinely compacts, run under one leadership state.
///
/// Returns (rounds, rounds that ran `compact_pages`, page refs relocated, bytes relocated,
/// rounds whose skip list names the role). The denominators are returned rather than folded in,
/// because the two halves of this guard fail in opposite directions and a single combined count
/// can read full for one while the other is zero.
fn run_compaction_rounds_under_role(
    role: Option<ShardLeadership>,
    batch: usize,
    keyspace: usize,
    rounds: usize,
) -> (usize, usize, usize, u64, usize) {
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
    if let Some(role) = role {
        runtime.set_shard_leadership_source(Arc::new(FixedShardLeadership(role)));
    }
    let options = StorageManagerOptions::default();
    let mut written = 0usize;
    let mut compacted_rounds = 0usize;
    let mut relocated_refs = 0usize;
    let mut relocated_bytes = 0u64;
    let mut role_skipped_rounds = 0usize;
    for _ in 0..rounds {
        let engine = runtime.engine();
        for index in 0..batch {
            // Overwrites a fixed key space, so earlier versions become dead pages. An
            // insert-only load leaves nothing stale and compaction never fires, which would
            // make every number below a measurement of nothing.
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("role-{:08}", (written + index) % keyspace),
                    value: vec![b'v'; 96],
                },
            });
        }
        written += batch;
        let report = runtime.run_storage_manager_once(1, options.clone());
        if report
            .executed_stages
            .iter()
            .any(|stage| stage == "compact_pages")
        {
            compacted_rounds += 1;
        }
        if report
            .skipped_stages
            .iter()
            .any(|stage| stage == "compact_pages_not_leading")
        {
            role_skipped_rounds += 1;
        }
        if let Some(compaction) = report.compaction_report.as_ref() {
            // compacted_objects carries the shard report rewritten_page_refs.
            relocated_refs += compaction.compacted_objects;
            relocated_bytes += compaction.relocated_bytes;
        }
    }
    (
        rounds,
        compacted_rounds,
        relocated_refs,
        relocated_bytes,
        role_skipped_rounds,
    )
}

/// A node that does not lead a shard must not rewrite that shard's live set.
///
/// THREE STATES, ASSERTED SEPARATELY, EACH WITH ITS OWN DENOMINATOR. A guard that only checks
/// the follower half cannot tell "skipped because this node follows" from "compaction never ran
/// in this fixture at all" -- both read as zero -- so the leading half is what makes the zero
/// mean something, and the unknown half is what stops the fix from being worse than the waste
/// it removes.
///
///   leading      -> compacts. The positive control.
///   not leading  -> skips, and SAYS why. The behaviour being bought.
///   unknown      -> compacts. The failure mode this change risks: the storage maintenance
///                   scheduler is constructed before consensus is, and on a standalone node
///                   consensus is never constructed at all. A check that read "I cannot tell"
///                   as "I am a follower" would switch page compaction off everywhere, which is
///                   far more damage than the waste.
///
/// Mutation-verified, one mutation per half:
///   * gate loses `leadership_permits_compaction`  -> the not-leading half fails (compacted 8/8).
///   * gate becomes an unconditional skip           -> the leading half fails (compacted 0/8).
///   * `is_known_not_leading` also matches Unknown  -> the unknown half fails (compacted 0/8).
#[test]
fn a_node_that_does_not_lead_a_shard_does_not_rewrite_its_live_set() {
    const BATCH: usize = 2_000;
    const ROUNDS: usize = 8;
    // Smaller than BATCH, so every round overwrites keys earlier rounds wrote.
    const KEYSPACE: usize = 500;

    // No source attached at all is the same state the scheduler sees before consensus exists.
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let bare = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    assert_eq!(
        bare.shard_leadership(1),
        ShardLeadership::Unknown,
        "a runtime with no leadership source must answer Unknown, never NotLeading"
    );

    let (leading_rounds, leading_compacted, leading_refs, leading_bytes, leading_role_skips) =
        run_compaction_rounds_under_role(Some(ShardLeadership::Leading), BATCH, KEYSPACE, ROUNDS);
    let (
        follower_rounds,
        follower_compacted,
        follower_refs,
        follower_bytes,
        follower_role_skips,
    ) = run_compaction_rounds_under_role(
        Some(ShardLeadership::NotLeading),
        BATCH,
        KEYSPACE,
        ROUNDS,
    );
    let (unknown_rounds, unknown_compacted, unknown_refs, unknown_bytes, unknown_role_skips) =
        run_compaction_rounds_under_role(None, BATCH, KEYSPACE, ROUNDS);

    eprintln!(
        "  leading      compacted={leading_compacted}/{leading_rounds} refs={leading_refs} \
         bytes={leading_bytes} role_skips={leading_role_skips}"
    );
    eprintln!(
        "  not_leading  compacted={follower_compacted}/{follower_rounds} refs={follower_refs} \
         bytes={follower_bytes} role_skips={follower_role_skips}"
    );
    eprintln!(
        "  unknown      compacted={unknown_compacted}/{unknown_rounds} refs={unknown_refs} \
         bytes={unknown_bytes} role_skips={unknown_role_skips}"
    );

    // HALF ONE: leading. Without this the follower's zero below is unattributable.
    assert!(
        leading_compacted > 0,
        "a leading node must still compact: ran {leading_compacted} of {leading_rounds} rounds"
    );
    assert!(
        leading_refs > 0 && leading_bytes > 0,
        "a leading node must relocate something: refs={leading_refs} bytes={leading_bytes} over \
         {leading_rounds} rounds"
    );
    assert_eq!(
        leading_role_skips, 0,
        "a leading node must never record the role skip: {leading_role_skips} of \
         {leading_rounds} rounds did"
    );

    // HALF TWO: not leading. Zero work, and a named reason for it.
    assert_eq!(
        follower_compacted, 0,
        "a node that does not lead must not compact: ran {follower_compacted} of \
         {follower_rounds} rounds"
    );
    assert_eq!(
        (follower_refs, follower_bytes),
        (0, 0),
        "a node that does not lead must relocate nothing: refs={follower_refs} \
         bytes={follower_bytes} over {follower_rounds} rounds"
    );
    assert_eq!(
        follower_role_skips, follower_rounds,
        "every round on a non-leading node must name the role as the skip reason: \
         {follower_role_skips} of {follower_rounds} did"
    );

    // HALF THREE: unknown. The state the scheduler is in before consensus exists, and the state
    // every standalone node stays in for ever.
    assert!(
        unknown_compacted > 0,
        "an UNKNOWN role must not disable compaction -- the scheduler starts before consensus \
         does, and a standalone node has none at all: ran {unknown_compacted} of \
         {unknown_rounds} rounds"
    );
    assert!(
        unknown_refs > 0 && unknown_bytes > 0,
        "an UNKNOWN role must still relocate: refs={unknown_refs} bytes={unknown_bytes} over \
         {unknown_rounds} rounds"
    );
    assert_eq!(
        unknown_role_skips, 0,
        "an UNKNOWN role must never record the role skip: {unknown_role_skips} of \
         {unknown_rounds} rounds did"
    );
}

/// What does a node that leads nothing throw away per round? Prints.
///
///   cargo test -p temporalstore-rust --lib what_a_follower_rewrites_per_round \
///       -- --ignored --nocapture --test-threads=1
///
/// Measured against TODAY's compaction, not the one that rewrote the whole live set every round:
/// since #1687 a round relocates only the pages sitting on the slabs the reclaim plan picked, so
/// these are the post-fix numbers and they are still entirely wasted on a node whose clients
/// cannot see the layout.
///
/// MEASURED, 16-core WSL box, load average under 5 throughout, a 500-key live set overwritten
/// by 2,000 writes a round:
///
///     records   rounds   compacted   page refs   bytes relocated   bytes/round
///      8,000       4        4/4         2,000         216,000         54,000
///     80,000      40       40/40       20,000       2,160,000         54,000
///
/// The not-leading arm is the control and reads ZERO on every column at both scales, over
/// denominators of 4 and 40 rounds; without it a zero here would be indistinguishable from a
/// fixture that never generated compaction pressure.
///
/// READ THE THIRD COLUMN FIRST. Compaction fired in EVERY round of both runs, on a shard whose
/// only activity is overwriting the same 500 keys. Since #1687 a round is bounded by the live
/// pages on the slabs the reclaim plan picked, so its cost is flat in the record count -- but
/// it recurs for ever, once per scheduler tick, and at the shipped thirty-second interval that
/// is 54 KB relocated and 500 pages rewritten per shard per tick, about 155 MB a day per idle
/// shard, plus the index record each round persists. None of it is visible to a client of a
/// node that leads nothing.
#[test]
#[ignore]
fn what_a_follower_rewrites_per_round() {
    const BATCH: usize = 2_000;

    // TWO AXES, because one of them alone says the wrong thing.
    //
    // A fixed live set shows the RECURRENCE: the round costs the same every thirty seconds for
    // ever, on a shard nobody is even writing to unusually hard. Growing the record count under
    // a fixed live set does NOT make a round more expensive, and a table that only did that
    // would suggest the waste is bounded -- it is not, it is unbounded in TIME.
    //
    // A live set proportional to the shard shows the other half: what one round costs is the
    // live set sitting on the slabs the reclaim plan picked, so a bigger shard pays more PER
    // ROUND as well as for ever.
    const KEYSPACE: usize = 500;

    eprintln!(
        "  records  role         rounds  compacted  page_refs  relocated_bytes  bytes_per_round"
    );
    for rounds in [4usize, 40usize] {
        let records = BATCH * rounds;
        for (label, role) in [
            ("leading", Some(ShardLeadership::Leading)),
            ("not_leading", Some(ShardLeadership::NotLeading)),
        ] {
            let (denominator, compacted, refs, bytes, _) =
                run_compaction_rounds_under_role(role, BATCH, KEYSPACE, rounds);
            let per_round = if denominator == 0 {
                0
            } else {
                bytes / denominator as u64
            };
            eprintln!(
                "  {records:>7}  {label:<11}  {denominator:>6}  {compacted:>9}  {refs:>9}  \
                 {bytes:>15}  {per_round:>15}"
            );
        }
    }
}
