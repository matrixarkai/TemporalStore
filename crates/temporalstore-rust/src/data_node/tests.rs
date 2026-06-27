use super::*;
use tempfile::tempdir;

#[test]
fn runtime_reports_cpp_style_shard_worker_ownership() {
    let runtime = DataNodeRuntime::new(
        TemporalEngine::default(),
        DataNodeRuntimeOptions {
            worker_threads: 4,
            max_queue_depth: 16,
            max_background_queue_depth: 8,
        },
    );

    let stats = runtime.stats();
    assert_eq!(stats.worker_threads, 4);
    assert_eq!(stats.max_queue_depth, 16);
    assert_eq!(stats.max_background_queue_depth, 8);
    assert_eq!(
        runtime.shard_worker_info(7),
        ShardWorkerInfo {
            shard_id: 7,
            worker_index: 3,
            worker_threads: 4,
        }
    );
}

#[test]
fn runtime_lifecycle_report_counts_loaded_readonly_and_preflight() {
    let engine = TemporalEngine::default();
    assert!(
        engine
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 7,
                table_name: "tbl".to_string(),
                shard_uri: "local://tbl/7".to_string(),
                start_routing_slot: 10,
                end_routing_slot: 19,
                readonly: true,
                load_version: 42,
                local_node_id: Some(3),
            })
            .status
            .ok
    );
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );

    let lifecycle = runtime.lifecycle_report();
    assert_eq!(lifecycle.loaded_shard_count, 1);
    assert_eq!(lifecycle.serving_count, 0);
    assert_eq!(lifecycle.readonly_count, 1);
    assert_eq!(lifecycle.queued_count, 0);
    assert_eq!(lifecycle.running_count, 0);
    assert_eq!(lifecycle.unloading_count, 0);
    assert_eq!(lifecycle.failed_count, 0);
    assert_eq!(lifecycle.max_load_version, 42);
    assert_eq!(lifecycle.shards.len(), 1);
    assert_eq!(lifecycle.shards[0].shard_id, 7);
    assert_eq!(lifecycle.shards[0].serving_state, "readonly");
    assert!(lifecycle.shards[0].readonly);
    assert_eq!(lifecycle.shards[0].table_name, "tbl");
    assert_eq!(lifecycle.transitions.len(), 0);

    let preflight = runtime.preflight_report();
    assert_eq!(preflight.lifecycle, lifecycle);
}

#[test]
fn runtime_load_reload_unload_records_lifecycle_transitions() {
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        TemporalEngine::default(),
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );

    let load = runtime.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 7,
        table_name: "tbl".to_string(),
        shard_uri: "local://tbl/7".to_string(),
        start_routing_slot: 10,
        end_routing_slot: 19,
        readonly: false,
        load_version: 42,
        local_node_id: Some(3),
    });
    assert!(load.status.ok, "{load:?}");
    let lifecycle = runtime.lifecycle_report();
    assert_eq!(lifecycle.serving_count, 1);
    assert_eq!(lifecycle.failed_count, 0);
    assert_eq!(lifecycle.transitions.len(), 1);
    assert_eq!(lifecycle.transitions[0].state, "serving");
    assert_eq!(lifecycle.transitions[0].operation, "load");
    assert_eq!(lifecycle.transitions[0].load_version, 42);

    let stale = runtime.reload_shard_with(crate::control::LoadShardRequest {
        shard_id: 7,
        table_name: "stale".to_string(),
        shard_uri: "local://tbl/stale".to_string(),
        start_routing_slot: 20,
        end_routing_slot: 29,
        readonly: true,
        load_version: 41,
        local_node_id: Some(3),
    });
    assert_eq!(stale.status.code, "stale_load_version");
    let failed = runtime.lifecycle_report();
    assert_eq!(failed.failed_count, 1);
    assert_eq!(failed.shards[0].serving_state, "failed");
    assert_eq!(failed.transitions[0].state, "failed");
    assert_eq!(failed.transitions[0].operation, "reload");
    assert_eq!(
        failed.transitions[0].last_status.as_ref().unwrap().code,
        "stale_load_version"
    );

    let reload = runtime.reload_shard_with(crate::control::LoadShardRequest {
        shard_id: 7,
        table_name: "tbl-new".to_string(),
        shard_uri: "local://tbl/7-new".to_string(),
        start_routing_slot: 20,
        end_routing_slot: 29,
        readonly: true,
        load_version: 43,
        local_node_id: Some(4),
    });
    assert!(reload.status.ok, "{reload:?}");
    let readonly = runtime.lifecycle_report();
    assert_eq!(readonly.readonly_count, 1);
    assert_eq!(readonly.failed_count, 0);
    assert_eq!(readonly.transitions[0].state, "readonly");
    assert_eq!(readonly.transitions[0].operation, "reload");
    assert_eq!(readonly.transitions[0].load_version, 43);

    let unload = runtime.unload_shard_with(crate::control::UnloadShardRequest { shard_id: 7 });
    assert!(unload.status.ok, "{unload:?}");
    let unloaded = runtime.lifecycle_report();
    assert_eq!(unloaded.loaded_shard_count, 0);
    assert_eq!(unloaded.transitions[0].state, "unloaded");
    assert_eq!(unloaded.transitions[0].operation, "unload");
}

#[test]
fn data_node_grpc_streaming_contract_exposes_callbacks_and_job_status() {
    let runtime = DataNodeRuntime::new_without_workers_for_test(TemporalEngine::default(), 4);
    let contract = runtime.grpc_streaming_contract();

    assert_eq!(contract.service_name, "temporalstore.v1.DataNodeService");
    assert_eq!(contract.execute_stream_method, "ExecuteStream");
    assert_eq!(
        contract.lifecycle_callback_stream_method,
        "LifecycleCallbacks"
    );
    assert_eq!(contract.job_status_stream_method, "WatchJobStatus");
    assert!(contract.bidirectional_streaming);
    assert!(contract.callback_ack_required);
    assert!(contract.tonic_surface_ready);
}

#[test]
fn distributed_admission_policy_aggregates_peer_counters() {
    let runtime = DataNodeRuntime::new_without_workers_for_test(TemporalEngine::default(), 4);
    let peers = vec![
        DistributedAdmissionPeerSnapshot {
            node_id: "node-a".to_string(),
            shard_id: 7,
            topology_version: 12,
            window_start_ms: 1000,
            read_count: 5,
            write_count: 3,
        },
        DistributedAdmissionPeerSnapshot {
            node_id: "node-b".to_string(),
            shard_id: 7,
            topology_version: 12,
            window_start_ms: 1000,
            read_count: 4,
            write_count: 6,
        },
        DistributedAdmissionPeerSnapshot {
            node_id: "node-c".to_string(),
            shard_id: 8,
            topology_version: 12,
            window_start_ms: 1000,
            read_count: 100,
            write_count: 100,
        },
    ];

    let allowed = runtime.distributed_admission_decision(7, &peers, 10, 10, 12);
    assert!(allowed.status.ok, "{allowed:?}");
    assert_eq!(allowed.participating_nodes, 2);
    assert_eq!(allowed.aggregate_read_count, 9);
    assert_eq!(allowed.aggregate_write_count, 9);
    assert!(allowed.read_allowed);
    assert!(allowed.write_allowed);

    let rejected = runtime.distributed_admission_decision(7, &peers, 9, 9, 12);
    assert_eq!(rejected.status.code, "distributed_admission_rejected");
    assert!(!rejected.read_allowed);
    assert!(!rejected.write_allowed);
    assert!(rejected
        .reasons
        .contains(&"distributed_read_budget_exceeded".to_string()));
    assert!(rejected
        .reasons
        .contains(&"distributed_write_budget_exceeded".to_string()));

    let stale = runtime.distributed_admission_decision(7, &peers, 10, 10, 13);
    assert!(stale
        .reasons
        .contains(&"stale_distributed_admission_topology".to_string()));
}

#[test]
fn multi_process_lifecycle_validation_requires_load_reload_unload_and_restart() {
    let reports = vec![
        DataNodeLifecycleReport {
            loaded_shard_count: 1,
            serving_count: 1,
            readonly_count: 0,
            queued_count: 0,
            running_count: 0,
            unloading_count: 0,
            failed_count: 0,
            max_load_version: 42,
            shards: Vec::new(),
            transitions: vec![
                DataNodeShardLifecycleState {
                    shard_id: 7,
                    state: "serving".to_string(),
                    operation: "load".to_string(),
                    load_version: 42,
                    updated_at_ms: 1,
                    last_status: Some(Status::ok()),
                    scheduler_task_id: Some(1),
                    scheduler_generation: Some(10),
                },
                DataNodeShardLifecycleState {
                    shard_id: 7,
                    state: "readonly".to_string(),
                    operation: "reload".to_string(),
                    load_version: 43,
                    updated_at_ms: 2,
                    last_status: Some(Status::ok()),
                    scheduler_task_id: Some(2),
                    scheduler_generation: Some(11),
                },
            ],
        },
        DataNodeLifecycleReport {
            loaded_shard_count: 0,
            serving_count: 0,
            readonly_count: 0,
            queued_count: 0,
            running_count: 0,
            unloading_count: 0,
            failed_count: 0,
            max_load_version: 43,
            shards: Vec::new(),
            transitions: vec![DataNodeShardLifecycleState {
                shard_id: 7,
                state: "unloaded".to_string(),
                operation: "unload".to_string(),
                load_version: 43,
                updated_at_ms: 3,
                last_status: Some(Status::ok()),
                scheduler_task_id: Some(3),
                scheduler_generation: Some(12),
            }],
        },
    ];
    let persistence = vec![
        DataNodeLifecyclePersistenceReport {
            enabled: true,
            last_restore_status: Some(Status::ok()),
            restore_success_total: 1,
            ..DataNodeLifecyclePersistenceReport::default()
        },
        DataNodeLifecyclePersistenceReport {
            enabled: true,
            last_restore_status: Some(Status::ok()),
            restore_success_total: 1,
            ..DataNodeLifecyclePersistenceReport::default()
        },
    ];

    let validated = validate_multi_process_lifecycle_reports(&reports, &persistence);
    assert!(validated.passed, "{validated:?}");
    assert_eq!(validated.node_count, 2);
    assert!(validated.load_validated);
    assert!(validated.reload_validated);
    assert!(validated.unload_validated);
    assert!(validated.restart_restore_validated);
    assert!(validated.all_nodes_have_persistence);

    let missing_restart = validate_multi_process_lifecycle_reports(&reports, &[]);
    assert!(!missing_restart.passed);
    assert!(missing_restart
        .blockers
        .contains(&"restart_restore_not_validated".to_string()));
}

#[test]
fn runtime_direct_unload_rejects_busy_shard_without_unloading() {
    let engine = TemporalEngine::default();
    engine.load_shard(7);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 8,
            max_background_queue_depth: 4,
        },
    );

    let queued = runtime.submit_dump(
        DumpShardRequest {
            shard_id: 7,
            selected_routing_slots: Vec::new(),
        },
        RequestController { timeout_ms: 1000 },
    );
    assert!(queued.status.ok, "{queued:?}");

    let unload = runtime.unload_shard_with(crate::control::UnloadShardRequest { shard_id: 7 });
    assert_eq!(unload.status.code, "shard_busy");
    let lifecycle = runtime.lifecycle_report();
    assert_eq!(lifecycle.loaded_shard_count, 1);
    assert_eq!(lifecycle.failed_count, 1);
    assert_eq!(lifecycle.transitions[0].state, "failed");
    assert_eq!(lifecycle.transitions[0].operation, "unload");
    assert_eq!(
        lifecycle.transitions[0].last_status.as_ref().unwrap().code,
        "shard_busy"
    );
}

#[test]
fn runtime_queued_unload_waits_for_prior_shard_work() {
    let engine = TemporalEngine::default();
    engine.load_shard(7);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 8);

    let write = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 7,
            command: Command::StringSet {
                key: "before-unload".to_string(),
                value: b"value".to_vec(),
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    let unload = runtime.submit_unload(
        crate::control::UnloadShardRequest { shard_id: 7 },
        RequestController { timeout_ms: 1000 },
    );
    assert!(write.status.ok, "{write:?}");
    assert!(unload.status.ok, "{unload:?}");

    let first = runtime
        .inner
        .queue
        .lock()
        .expect("runtime queue lock poisoned")
        .pop_ready()
        .expect("first shard task should be ready");
    assert_eq!(first.job_id, write.job_id);
    let output = execute_task(&runtime.inner, &first);
    let DataNodeTaskOutput::Execute(response) = output else {
        panic!("expected execute output");
    };
    assert!(response.status.ok, "{response:?}");
    runtime
        .inner
        .queue
        .lock()
        .expect("runtime queue lock poisoned")
        .finish_shard(7);

    let second = runtime
        .inner
        .queue
        .lock()
        .expect("runtime queue lock poisoned")
        .pop_ready()
        .expect("queued unload should be ready after prior work");
    assert_eq!(second.job_id, unload.job_id);
    let output = execute_task(&runtime.inner, &second);
    let DataNodeTaskOutput::Unload(response) = output else {
        panic!("expected unload output");
    };
    assert!(response.status.ok, "{response:?}");
    assert_eq!(runtime.lifecycle_report().loaded_shard_count, 0);
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
        start_routing_slot: 10,
        end_routing_slot: 19,
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
        start_routing_slot: 10,
        end_routing_slot: 19,
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
        start_routing_slot: 10,
        end_routing_slot: 19,
        readonly: false,
        load_version: 42,
        local_node_id: Some(3),
    });
    assert!(load.status.ok, "{load:?}");
    let reload = runtime.reload_shard_with(crate::control::LoadShardRequest {
        shard_id: 7,
        table_name: "tbl".to_string(),
        shard_uri: "local://tbl/7".to_string(),
        start_routing_slot: 10,
        end_routing_slot: 19,
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
        start_routing_slot: 10,
        end_routing_slot: 19,
        readonly: true,
        load_version: 42,
        local_node_id: Some(3),
    });
    assert_eq!(stale_reload.status.code, "lifecycle_token_mismatch");

    let good_reload = restored.reload_shard_with(crate::control::LoadShardRequest {
        shard_id: 7,
        table_name: "tbl-restored".to_string(),
        shard_uri: "local://tbl/restored".to_string(),
        start_routing_slot: 10,
        end_routing_slot: 19,
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
        start_routing_slot: 10,
        end_routing_slot: 19,
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
        start_routing_slot: 10,
        end_routing_slot: 19,
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
            start_routing_slot: 10,
            end_routing_slot: 19,
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
            start_routing_slot: 10,
            end_routing_slot: 19,
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
        start_routing_slot: 10,
        end_routing_slot: 19,
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
        start_routing_slot: 10,
        end_routing_slot: 19,
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
            selected_routing_slots: Vec::new(),
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
fn runtime_dump_can_flush_only_selected_dirty_slots() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let mut key_a = String::new();
    let mut key_b = String::new();
    let mut slot_a = 0;
    for index in 0..128 {
        let key = format!("slot-key-{index}");
        let slot = engine.routing_slot_for_key(1, &key);
        if key_a.is_empty() {
            key_a = key;
            slot_a = slot;
        } else if slot != slot_a {
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
            selected_routing_slots: vec![slot_a],
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
        output.slot_dump_manifest.as_ref().unwrap().slot_ids,
        vec![slot_a]
    );
    let remaining = runtime.dirty_objects();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].key, key_b);
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
fn runtime_builds_cpp_style_server_load_report() {
    let engine = TemporalEngine::default();
    assert!(
        engine
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 7,
                table_name: "tbl".to_string(),
                shard_uri: "local://tbl/7".to_string(),
                start_routing_slot: 10,
                end_routing_slot: 19,
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
            use_cpp_partition_ids: false,
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        }),
        partitions: vec![crate::meta::TablePartition {
            shard_id: 7,
            start_slot: 10,
            end_slot: 19,
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
        partitions: vec![crate::meta::TablePartition {
            shard_id: 7,
            start_slot: 0,
            end_slot: 9,
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
    assert_eq!(shard.oplog_sequence, 1);
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
            selected_dump_slots: Vec::new(),
            max_dump_slots_per_round: 0,
            min_undumped_oplog_records: 0,
            purge_delayed_destroy: false,
            prune_slot_dump_manifests: false,
            roll_forward_slot_dump_installs: false,
            follower_replay_cursors: Vec::new(),
            page_gc_shared_store_cursors: Vec::new(),
            page_gc_raft_snapshot_refs: Vec::new(),
            page_gc_checkpoint_floor_segment_id: None,
            page_gc_raft_install_floor_segment_id: None,
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
fn runtime_storage_manager_loop_runs_cpp_style_pressure_stages() {
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
            max_dump_slots_per_round: 16,
            min_undumped_oplog_records: 1,
            dirty_slot_pressure: 1,
            stale_page_segment_pressure: 1,
            reclaimable_physical_bytes_pressure: 1,
            cache_memory_bytes_pressure: 1,
            cache_disk_bytes_pressure: 1,
            ..StorageManagerOptions::default()
        },
    );

    assert!(report.status.ok, "{report:?}");
    for stage in [
        "prepare",
        "reclaim_oplog",
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
    assert!(report.pressure.dirty_slot_count >= 1);
    assert!(report.pressure.undumped_oplog_records >= 1);
    assert_eq!(report.pressure_decisions.len(), 8, "{report:?}");
    for stage in [
        "prepare",
        "reclaim_oplog",
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
    let reclaim_oplog = report
        .pressure_decisions
        .iter()
        .find(|decision| decision.stage == "reclaim_oplog")
        .unwrap();
    assert!(reclaim_oplog.pressure_active, "{reclaim_oplog:?}");
    assert!(reclaim_oplog
        .signals
        .iter()
        .any(|signal| signal.name == "dirty_slot_count" && signal.over_threshold));
    assert!(reclaim_oplog
        .signals
        .iter()
        .any(|signal| signal.name == "undumped_oplog_records" && signal.over_threshold));
    assert!(reclaim_oplog
        .trigger_reasons
        .iter()
        .any(|reason| reason == "dirty_slot_pressure" || reason == "undumped_oplog_pressure"));
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
    assert_eq!(stats.storage_manager_reclaim_oplog_runs, 1);
    assert_eq!(stats.storage_manager_reclaim_memory_runs, 1);
    assert_eq!(stats.storage_manager_expire_runs, 1);
    assert_eq!(stats.storage_manager_reclaim_page_runs, 1);
    assert_eq!(stats.storage_manager_compact_runs, 1);
    assert_eq!(stats.storage_manager_index_gc_runs, 1);
}

#[test]
// rust-internal: validates periodic runtime scheduling for the StorageManager loop
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
fn runtime_compaction_rewrites_live_pages_and_reports_stale_segments() {
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
    assert_eq!(engine.live_page_segment_ids(1), vec![0]);

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
    assert_eq!(output.previous_page_segment_id, 0);
    assert_eq!(output.compacted_page_segment_id, 1);
    assert_eq!(output.stale_page_segment_ids, vec![0]);
    assert_eq!(output.before.total_page_count, 3);
    assert_eq!(output.before.live_page_refs, 2);
    assert_eq!(output.before.stale_page_estimate, 1);
    assert_eq!(output.before.live_ref_density_basis_points, 6_666);
    assert_eq!(output.after.total_page_count, 2);
    assert_eq!(output.after.live_page_refs, 2);
    assert_eq!(output.after.stale_page_estimate, 0);
    assert_eq!(output.after.live_ref_density_basis_points, 10_000);
    assert_eq!(engine.live_page_segment_ids(1), vec![1]);
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
    engine.page_store().install_segment(1, b"old").unwrap();
    engine.page_store().install_segment(2, b"new").unwrap();

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
            retain_oplog_from_sequence: Some(3),
            retain_index_log_from_sequence: Some(2),
            retain_page_segments_from_id: Some(2),
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
    assert_eq!(output.oplog_records_removed, 2);
    assert_eq!(output.index_log_records_removed, 1);
    assert_eq!(output.page_segments_removed, 1);
    assert!(output.page_segments_removed_physical_bytes > 0);
    assert!(output.page_segments_retained_physical_bytes > 0);
    assert_eq!(output.page_segments_retained_live, 1);
    assert!(output.page_segments_retained_live_physical_bytes > 0);
    assert_eq!(engine.write_ahead_log_store().stats(1).last_sequence, 3);
    assert_eq!(engine.index_log_store().stats(1).last_sequence, 3);
    assert_eq!(engine.page_store().segment_ids().unwrap(), vec![0, 2]);
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
            start_routing_slot: 10,
            end_routing_slot: 19,
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
            selected_routing_slots: Vec::new(),
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
            selected_routing_slots: Vec::new(),
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
            retain_oplog_from_sequence: Some(2),
            retain_index_log_from_sequence: Some(2),
            retain_page_segments_from_id: None,
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
            selected_routing_slots: Vec::new(),
        },
        RequestController { timeout_ms: 1000 },
    );
    assert!(accepted.status.ok);
    let rejected = runtime.submit_gc(
        GcRequest {
            shard_id: 1,
            retain_oplog_from_sequence: None,
            retain_index_log_from_sequence: None,
            retain_page_segments_from_id: None,
        },
        RequestController { timeout_ms: 1000 },
    );
    assert_eq!(rejected.status.code, "background_queue_full");
    assert_eq!(runtime.stats().rejected_background_total, 1);
    assert_eq!(runtime.stats().background_queue_depth, 1);
}

// shared-corpus: storage_byteraft_dump_load_atomicity storage_byteraft_cache_refill_pressure
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
                start_routing_slot: 0,
                end_routing_slot: 16_383,
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
            max_dump_slots_per_round: 8,
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
        response.report.cxx_stage_order,
        vec![
            "prepare",
            "reclaim_oplog",
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
        .any(|stage| stage.stage == "reclaim_oplog" && stage.dumped_slot_count >= 1));
    assert_eq!(runtime.stats().storage_manager_runs, 1);
    assert_eq!(runtime.stats().background_queue_depth, 0);
}

// shared-corpus: storage_byteraft_dump_load_atomicity storage_byteraft_cache_refill_pressure
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
                start_routing_slot: 0,
                end_routing_slot: 16_383,
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

    let scheduler = runtime.start_storage_manager_scheduler(
        Duration::from_millis(5),
        StorageManagerCycleRequest {
            shard_id: 1,
            max_dump_slots_per_round: 8,
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
                start_routing_slot: 0,
                end_routing_slot: 16_383,
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

    let manager = runtime.start_storage_manager_runtime(StorageManagerRuntimeOptions {
        interval_ms: 5,
        jitter_percent: 50,
        initial_backoff_ms: 3,
        max_backoff_ms: 40,
        request: StorageManagerCycleRequest {
            shard_id: 8,
            max_dump_slots_per_round: 3,
            enable_prepare: true,
            enable_oplog_reclaim: true,
            enable_expire: false,
            enable_evict: true,
            enable_page_reclaim: true,
            enable_page_compaction: false,
            enable_index_gc: true,
            warm_cache: true,
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
    assert_eq!(running.bounded_max_dump_slots_per_round, 3);
    assert!(running.phase_prepare_enabled);
    assert!(running.phase_wal_reclaim_enabled);
    assert!(!running.phase_expire_enabled);
    assert!(running.phase_evict_enabled);
    assert!(running.phase_page_gc_enabled);
    assert!(!running.phase_compaction_enabled);
    assert!(running.phase_index_gc_enabled);
    assert!(running.last_job_id.is_some());
    assert!(running.last_status.as_ref().is_some_and(|status| status.ok));
    assert!(running.last_completed_cycle.is_some());
    assert!(running.last_pressure_snapshot.is_some());
    assert!(running.last_pressure_before >= running.last_pressure_after);
    assert!(running
        .last_phase_reports
        .iter()
        .any(|stage| stage.stage == "prepare"
            && stage.pressure_signal.contains("dirty_slots")
            && stage.pressure_before >= stage.pressure_after));
    assert!(
        running
            .last_phase_reports
            .iter()
            .any(|stage| stage.stage == "reclaim_oplog"
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
            max_dump_slots_per_round: 1,
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
    assert_eq!(report.bounded_max_dump_slots_per_round, 1);
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
            selected_routing_slots: Vec::new(),
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
