// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::types::{Command, CommandResponse, ExecuteRequest};

fn seeded(dir: &std::path::Path) -> DataNodeRuntime {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    for i in 0..32 {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("key-{i}"),
                value: vec![i as u8; 96],
            },
        });
        assert!(response.status.ok);
    }
    DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 64,
            max_background_queue_depth: 8,
        },
    )
}

fn options() -> StorageManagerOptions {
    StorageManagerOptions {
        min_undumped_wal_records: 1,
        max_dump_buckets_per_round: 4,
        enable_memory_reclaim: false,
        enable_expire: false,
        enable_block_gc: false,
        enable_block_compaction: false,
        enable_evict: false,
        enable_metrics_reap: false,
        ..Default::default()
    }
}

#[test]
fn timer_execution_preserves_actions_and_counters_without_exposing_partial_diagnostics() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded(dir.path());
    let report = runtime.run_storage_manager_once_with_report_details(1, options(), false);
    assert!(report.status.ok, "{:?}", report.status);
    assert!(report
        .executed_stages
        .iter()
        .any(|stage| stage == "reclaim_wal"));
    assert!(report.lifecycle_report.is_none());
    let stats = runtime.stats();
    assert!(stats.dump_runs > 0);
    assert!(stats.storage_lifecycle_runs > 0);
    assert_eq!(stats.storage_manager_runs, 1);
    for i in 0..32 {
        let response = runtime.engine().execute(ExecuteRequest {
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

#[test]
fn explicit_manager_and_lifecycle_requests_still_return_full_diagnostics() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded(dir.path());
    let report = runtime.run_storage_manager_once(1, options());
    assert!(report.status.ok);
    let lifecycle = report
        .lifecycle_report
        .expect("explicit manager diagnostics");
    assert!(!lifecycle
        .storage_index_snapshot
        .page_index_entry_samples
        .is_empty());
    let response = runtime.apply_storage_lifecycle(StorageLifecycleRequest {
        shard_id: 1,
        ..Default::default()
    });
    assert!(response.status.ok);
    assert!(!response
        .report
        .storage_index_snapshot
        .page_index_entry_samples
        .is_empty());
}
