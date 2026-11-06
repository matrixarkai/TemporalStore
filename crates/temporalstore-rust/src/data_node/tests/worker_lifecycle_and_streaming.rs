// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Test part 1, split from tests.rs.
#![allow(clippy::all)]
use super::*;
use super::helpers::*;
use crate::types::{BatchHashMultiSetRow, BatchHashMultiSetValue};

#[test]
fn compact_batch_write_accepts_writes_and_rejects_reads_before_execution() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 4);

    let written = runtime.batch_write(BatchExecuteRequest {
        shard_id: 1,
        commands: vec![
            Command::StringSet {
                key: "a".to_string(),
                value: b"one".to_vec(),
            },
            Command::StringSet {
                key: "b".to_string(),
                value: b"two".to_vec(),
            },
        ],
    });
    assert!(written.status.ok, "{written:?}");
    assert_eq!(written.applied, 2);
    assert!(written.errors.is_empty());
    assert_eq!(runtime.dirty_shards(), vec![1]);

    let rejected = runtime.batch_write(BatchExecuteRequest {
        shard_id: 1,
        commands: vec![Command::StringGet {
            key: "a".to_string(),
        }],
    });
    assert!(!rejected.status.ok);
    assert_eq!(rejected.status.code, "read_in_write_batch");
    assert_eq!(rejected.applied, 0);
    assert!(rejected.errors.is_empty());

    let stored = runtime.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "a".to_string(),
        },
    });
    assert_eq!(
        stored.response,
        CommandResponse::Bytes {
            value: Some(b"one".to_vec())
        }
    );
}

#[test]
fn compact_batch_write_keeps_shared_wal_mirroring() {
    #[derive(Debug, Default)]
    struct RecordingSink {
        commands: Mutex<Vec<Command>>,
    }

    impl SharedWalSink for RecordingSink {
        fn record_write(&self, _shard_id: ShardId, command: &Command) {
            self.commands
                .lock()
                .expect("recording sink lock poisoned")
                .push(command.clone());
        }
    }

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 4);
    let sink = Arc::new(RecordingSink::default());
    runtime.set_shared_wal_sink(sink.clone());

    let written = runtime.batch_write(BatchExecuteRequest {
        shard_id: 1,
        commands: vec![
            Command::StringSet {
                key: "a".to_string(),
                value: b"one".to_vec(),
            },
            Command::StringSet {
                key: "b".to_string(),
                value: b"two".to_vec(),
            },
        ],
    });

    assert!(written.status.ok, "{written:?}");
    assert_eq!(written.applied, 2);
    assert_eq!(
        sink.commands
            .lock()
            .expect("recording sink lock poisoned")
            .len(),
        2
    );
}

#[test]
fn shared_field_hash_batch_keeps_key_order_and_missing_values() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 4);
    let written = runtime.batch_write(BatchExecuteRequest {
        shard_id: 1,
        commands: vec![Command::HashMultiSet {
            key: "present".to_string(),
            entries: vec![
                ("a".to_string(), b"one".to_vec()),
                ("b".to_string(), b"two".to_vec()),
            ],
        }],
    });
    assert!(written.status.ok, "{written:?}");

    let read = runtime.batch_hash_multi_get(BatchHashMultiGetRequest {
        shard_id: 1,
        keys: vec!["present".to_string(), "missing".to_string()],
        fields: vec!["b".to_string(), "a".to_string()],
    });

    assert!(read.status.ok, "{read:?}");
    assert_eq!(read.responses.len(), 2);
    assert_eq!(
        read.responses[0].response,
        CommandResponse::Values {
            values: vec![Some(b"two".to_vec()), Some(b"one".to_vec())]
        }
    );
    assert_eq!(
        read.responses[1].response,
        CommandResponse::Values {
            values: vec![None, None]
        }
    );
}

#[test]
fn shared_field_hash_write_uses_regular_write_semantics_and_rejects_bad_widths() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 4);

    let written = runtime.batch_hash_multi_set(BatchHashMultiSetRequest {
        shard_id: 1,
        fields: vec!["a".to_string(), "b".to_string()],
        rows: vec![
            BatchHashMultiSetRow(
                "first".to_string(),
                vec![
                    BatchHashMultiSetValue(b"one".to_vec()),
                    BatchHashMultiSetValue(b"two".to_vec()),
                ],
            ),
            BatchHashMultiSetRow(
                "second".to_string(),
                vec![
                    BatchHashMultiSetValue(b"three".to_vec()),
                    BatchHashMultiSetValue(b"four".to_vec()),
                ],
            ),
        ],
    });
    assert!(written.status.ok, "{written:?}");
    assert_eq!(written.applied, 2);
    assert_eq!(runtime.dirty_shards(), vec![1]);

    let read = runtime.batch_hash_multi_get(BatchHashMultiGetRequest {
        shard_id: 1,
        keys: vec!["first".to_string(), "second".to_string()],
        fields: vec!["b".to_string(), "a".to_string()],
    });
    assert_eq!(
        read.responses[0].response,
        CommandResponse::Values {
            values: vec![Some(b"two".to_vec()), Some(b"one".to_vec())]
        }
    );
    assert_eq!(
        read.responses[1].response,
        CommandResponse::Values {
            values: vec![Some(b"four".to_vec()), Some(b"three".to_vec())]
        }
    );

    let rejected = runtime.batch_hash_multi_set(BatchHashMultiSetRequest {
        shard_id: 1,
        fields: vec!["a".to_string(), "b".to_string()],
        rows: vec![BatchHashMultiSetRow(
            "bad".to_string(),
            vec![BatchHashMultiSetValue(b"only-one".to_vec())],
        )],
    });
    assert_eq!(rejected.status.code, "field_value_count_mismatch");
    assert_eq!(rejected.applied, 0);
}

#[test]
fn runtime_reports_style_shard_worker_ownership() {
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
                start_routing_bucket: 10,
                end_routing_bucket: 19,
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
        start_routing_bucket: 10,
        end_routing_bucket: 19,
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
        start_routing_bucket: 20,
        end_routing_bucket: 29,
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
        start_routing_bucket: 20,
        end_routing_bucket: 29,
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

