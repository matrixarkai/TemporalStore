// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Test part 3, split from tests.rs.
#![allow(clippy::all)]
use super::*;
use super::helpers::*;

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
            retain_page_slabs_from_id: None,
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
            stale_page_slab_pressure: 1,
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
    assert_eq!(report.pressure_decisions.len(), 8, "{report:?}");
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
                stale_page_slab_pressure: 1,
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
    assert_eq!(engine.live_page_slab_ids(1), vec![0]);

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
    assert_eq!(output.previous_page_slab_id, 0);
    assert_eq!(output.compacted_page_slab_id, 1);
    assert_eq!(output.stale_page_slab_ids, vec![0]);
    assert_eq!(output.before.total_page_count, 3);
    assert_eq!(output.before.live_page_refs, 2);
    assert_eq!(output.before.stale_page_estimate, 1);
    assert_eq!(output.before.live_ref_density_basis_points, 6_666);
    assert_eq!(output.after.total_page_count, 2);
    assert_eq!(output.after.live_page_refs, 2);
    assert_eq!(output.after.stale_page_estimate, 0);
    assert_eq!(output.after.live_ref_density_basis_points, 10_000);
    assert_eq!(engine.live_page_slab_ids(1), vec![1]);
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
            retain_page_slabs_from_id: Some(2),
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
    assert_eq!(output.page_slabs_removed, 1);
    assert!(output.page_slabs_removed_physical_bytes > 0);
    assert!(output.page_slabs_retained_physical_bytes > 0);
    assert_eq!(output.page_slabs_retained_live, 1);
    assert!(output.page_slabs_retained_live_physical_bytes > 0);
    assert_eq!(engine.write_ahead_log_store().stats(1).last_sequence, 3);
    assert_eq!(engine.index_log_store().stats(1).last_sequence, 3);
    assert_eq!(engine.block_store().slab_ids().unwrap(), vec![0, 2]);
}

#[test]
fn operator_gc_retains_slabs_referenced_by_dump_manifest() {
    // The /gc operator RPC must not delete a page slab a durable bucket-dump manifest still
    // references, even when retain_page_slabs_from_id would sweep it and it is no longer in
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
        !manifest.page_slab_ids.is_empty(),
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
    let live = engine.live_page_slab_ids(1);
    assert!(
        !manifest.page_slab_ids.iter().any(|s| live.contains(s)),
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
            retain_page_slabs_from_id: Some(u64::MAX),
        },
        RequestController { timeout_ms: 1000 },
    );
    let finished = wait_for_job(&runtime, submitted.job_id);
    let Some(DataNodeTaskOutput::Gc(output)) = finished.output else {
        panic!("expected gc output");
    };
    assert!(output.status.ok, "{:?}", output.status);
    let remaining = engine.block_store().slab_ids().unwrap();
    for slab in &manifest.page_slab_ids {
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
            retain_page_slabs_from_id: None,
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
            retain_page_slabs_from_id: None,
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

    wait_until(Duration::from_secs(5), || {
        manager.report().rounds_submitted >= 1
            && runtime.stats().storage_manager_runs >= 1
            && manager.report().last_completed_cycle.is_some()
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
    assert!(pressure.follower_cursor_retention_blockers >= 1);
    assert!(pressure.raft_snapshot_retention_blockers >= 1);
    assert!(pressure.total_pressure_score >= pressure.wal_bytes);
    assert!(running.last_pressure_before >= running.last_pressure_after);
    assert!(!running.last_selected_buckets.is_empty());
    assert!(running.last_bytes_reclaimed >= pressure.cache_disk_bytes);
    assert!(running.last_wal_floor_sequence >= 1);
    assert!(running.last_index_log_floor_sequence >= 1);
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
    assert!(running.last_phase_reports.iter().any(|stage| {
        stage.stage == "reclaim_wal"
            && !stage.selected_buckets.is_empty()
            && stage.wal_floor_sequence >= 1
            && stage.index_log_floor_sequence >= 1
            && stage.retention_blockers >= 1
            && stage.pressure_before >= stage.pressure_after
    }));
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
    thread::sleep(Duration::from_millis(25));
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


