// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// Prometheus metrics appenders (runtime/storage-manager/ingestion), split from
// server.rs (textual include!, shared flat scope + use-imports; no mod wrapper).

fn append_storage_backend_metric(
    out: &mut String,
    backend: &temporalstore_rust::StorageBackend,
) {
    use temporalstore_rust::StorageBackend;
    let (kind, replication) = match backend {
        StorageBackend::MatrixObject { .. } => ("matrixobject", "shared_store"),
        StorageBackend::SharedPath { .. } => ("shared_path", "shared_store"),
        StorageBackend::RaftReplication => ("raft", "raft"),
    };
    out.push_str(
        "# HELP temporalstore_storage_backend Selected distributed storage backend (1 = active).\n",
    );
    out.push_str("# TYPE temporalstore_storage_backend gauge\n");
    out.push_str(&format!(
        "temporalstore_storage_backend{{backend=\"{kind}\",replication=\"{replication}\"}} 1\n"
    ));
}

fn append_runtime_metrics(out: &mut String, runtime: &DataNodeRuntime) {
    let stats = runtime.stats();
    out.push_str("# HELP temporalstore_data_node_runtime_jobs_total Data node runtime job counters by kind.\n");
    out.push_str("# TYPE temporalstore_data_node_runtime_jobs_total counter\n");
    for (kind, value) in [
        ("submitted", stats.submitted_total),
        ("completed", stats.completed_total),
        ("rejected", stats.rejected_total),
        ("rejected_background", stats.rejected_background_total),
        ("timed_out", stats.timed_out_total),
        ("dump", stats.dump_runs),
        ("compaction", stats.compaction_runs),
        ("gc", stats.gc_runs),
        ("storage_manager", stats.storage_manager_runs),
    ] {
        out.push_str("temporalstore_data_node_runtime_jobs_total{kind=\"");
        out.push_str(kind);
        out.push_str("\"} ");
        out.push_str(&value.to_string());
        out.push('\n');
    }
    out.push_str("# HELP temporalstore_data_node_runtime_queue_depth Current data node runtime queue depth.\n");
    out.push_str("# TYPE temporalstore_data_node_runtime_queue_depth gauge\n");
    out.push_str("temporalstore_data_node_runtime_queue_depth ");
    out.push_str(&stats.queue_depth.to_string());
    out.push('\n');
    out.push_str("# HELP temporalstore_data_node_runtime_background_queue_depth Current background data node queue depth.\n");
    out.push_str("# TYPE temporalstore_data_node_runtime_background_queue_depth gauge\n");
    out.push_str("temporalstore_data_node_runtime_background_queue_depth ");
    out.push_str(&stats.background_queue_depth.to_string());
    out.push('\n');
    out.push_str("# HELP temporalstore_data_node_runtime_queued_shards Current shard queues with pending work.\n");
    out.push_str("# TYPE temporalstore_data_node_runtime_queued_shards gauge\n");
    out.push_str("temporalstore_data_node_runtime_queued_shards ");
    out.push_str(&stats.queued_shard_count.to_string());
    out.push('\n');
    out.push_str("# HELP temporalstore_data_node_runtime_running_shards Current shard lanes executing work.\n");
    out.push_str("# TYPE temporalstore_data_node_runtime_running_shards gauge\n");
    out.push_str("temporalstore_data_node_runtime_running_shards ");
    out.push_str(&stats.running_shard_count.to_string());
    out.push('\n');
    out.push_str("# HELP temporalstore_data_node_dirty_objects Dirty object count.\n");
    out.push_str("# TYPE temporalstore_data_node_dirty_objects gauge\n");
    out.push_str("temporalstore_data_node_dirty_objects ");
    out.push_str(&stats.dirty_object_count.to_string());
    out.push('\n');
    out.push_str("# HELP temporalstore_data_node_dirty_shards Dirty shard count.\n");
    out.push_str("# TYPE temporalstore_data_node_dirty_shards gauge\n");
    out.push_str("temporalstore_data_node_dirty_shards ");
    out.push_str(&stats.dirty_shard_count.to_string());
    out.push('\n');
    let lifecycle_persistence = runtime.lifecycle_persistence_report();
    out.push_str("# HELP temporalstore_data_node_lifecycle_snapshot_enabled Whether automatic lifecycle snapshot persistence is enabled.\n");
    out.push_str("# TYPE temporalstore_data_node_lifecycle_snapshot_enabled gauge\n");
    out.push_str("temporalstore_data_node_lifecycle_snapshot_enabled ");
    out.push_str(if lifecycle_persistence.enabled {
        "1"
    } else {
        "0"
    });
    out.push('\n');
    out.push_str("# HELP temporalstore_data_node_lifecycle_snapshot_events_total Lifecycle snapshot persistence events.\n");
    out.push_str("# TYPE temporalstore_data_node_lifecycle_snapshot_events_total counter\n");
    for (kind, value) in [
        (
            "restore_success",
            lifecycle_persistence.restore_success_total,
        ),
        (
            "restore_failure",
            lifecycle_persistence.restore_failure_total,
        ),
        (
            "persist_success",
            lifecycle_persistence.persist_success_total,
        ),
        (
            "persist_failure",
            lifecycle_persistence.persist_failure_total,
        ),
    ] {
        out.push_str("temporalstore_data_node_lifecycle_snapshot_events_total{kind=\"");
        out.push_str(kind);
        out.push_str("\"} ");
        out.push_str(&value.to_string());
        out.push('\n');
    }
    if let Some(report) = runtime.last_storage_manager_cycle_report() {
        append_storage_manager_cycle_metrics(out, &report);
    }
}

fn append_storage_manager_cycle_metrics(out: &mut String, report: &StorageManagerCycleReport) {
    out.push_str("# HELP temporalstore_storage_manager_pressure Last StorageManager pressure snapshot by shard and signal.\n");
    out.push_str("# TYPE temporalstore_storage_manager_pressure gauge\n");
    let pressure = &report.pressure_snapshot;
    for (signal, value) in [
        ("dirty_slots", pressure.dirty_bucket_count as u64),
        ("undumped_wal_records", pressure.undumped_wal_records),
        ("wal_bytes", pressure.wal_bytes),
        ("index_log_bytes", pressure.index_log_bytes),
        ("stale_page_bytes", pressure.stale_page_bytes),
        ("live_page_bytes", pressure.live_page_bytes),
        (
            "page_segment_stale_density_basis_points",
            pressure.page_slab_stale_density_basis_points,
        ),
        ("memory_cache_bytes", pressure.memory_cache_bytes),
        ("disk_cache_bytes", pressure.disk_cache_bytes),
        (
            "memory_cache_pressure_score",
            pressure.memory_cache_pressure_score,
        ),
        (
            "expired_slot_object_scan_debt",
            pressure.expired_bucket_object_scan_debt as u64,
        ),
        (
            "delayed_destroy_segments",
            pressure.delayed_destroy_slab_count as u64,
        ),
        ("delayed_destroy_bytes", pressure.delayed_destroy_bytes),
        (
            "follower_cursor_retention_blockers",
            pressure.follower_cursor_retention_blockers as u64,
        ),
        (
            "raft_snapshot_retention_blockers",
            pressure.raft_snapshot_retention_blockers as u64,
        ),
        (
            "compaction_debt_models",
            pressure.compaction_debt_model_count as u64,
        ),
        ("compaction_debt_score", pressure.compaction_debt_score),
        ("total_pressure_score", pressure.total_pressure_score),
    ] {
        out.push_str("temporalstore_storage_manager_pressure{shard_id=\"");
        out.push_str(&report.shard_id.to_string());
        out.push_str("\",signal=\"");
        out.push_str(signal);
        out.push_str("\"} ");
        out.push_str(&value.to_string());
        out.push('\n');
    }

    out.push_str("# HELP temporalstore_storage_manager_phase_enabled Last StorageManager phase enabled state.\n");
    out.push_str("# TYPE temporalstore_storage_manager_phase_enabled gauge\n");
    out.push_str("# HELP temporalstore_storage_manager_phase_applied Last StorageManager phase applied state.\n");
    out.push_str("# TYPE temporalstore_storage_manager_phase_applied gauge\n");
    out.push_str("# HELP temporalstore_storage_manager_phase_skipped Last StorageManager phase skipped state.\n");
    out.push_str("# TYPE temporalstore_storage_manager_phase_skipped gauge\n");
    out.push_str("# HELP temporalstore_storage_manager_phase_duration_ms Last StorageManager phase duration.\n");
    out.push_str("# TYPE temporalstore_storage_manager_phase_duration_ms gauge\n");
    out.push_str("# HELP temporalstore_storage_manager_phase_pressure Last StorageManager phase pressure counters by kind.\n");
    out.push_str("# TYPE temporalstore_storage_manager_phase_pressure gauge\n");
    out.push_str("# HELP temporalstore_storage_manager_phase_work Last StorageManager phase work counters by kind.\n");
    out.push_str("# TYPE temporalstore_storage_manager_phase_work gauge\n");
    out.push_str("# HELP temporalstore_storage_manager_phase_bytes Last StorageManager phase bytes by kind.\n");
    out.push_str("# TYPE temporalstore_storage_manager_phase_bytes gauge\n");
    out.push_str("# HELP temporalstore_storage_manager_phase_floors Last StorageManager phase retention floors by kind.\n");
    out.push_str("# TYPE temporalstore_storage_manager_phase_floors gauge\n");
    out.push_str("# HELP temporalstore_storage_manager_phase_errors Last StorageManager phase error count.\n");
    out.push_str("# TYPE temporalstore_storage_manager_phase_errors gauge\n");

    for stage in &report.stages {
        append_storage_manager_phase_bool(
            out,
            "temporalstore_storage_manager_phase_enabled",
            report.shard_id,
            &stage.stage,
            stage.enabled,
        );
        append_storage_manager_phase_bool(
            out,
            "temporalstore_storage_manager_phase_applied",
            report.shard_id,
            &stage.stage,
            stage.applied,
        );
        append_storage_manager_phase_bool(
            out,
            "temporalstore_storage_manager_phase_skipped",
            report.shard_id,
            &stage.stage,
            stage.skipped,
        );
        append_storage_manager_phase_value(
            out,
            "temporalstore_storage_manager_phase_duration_ms",
            report.shard_id,
            &stage.stage,
            stage.duration_ms,
        );
        for (kind, value) in [
            ("score", stage.pressure_score),
            ("threshold", stage.pressure_threshold),
            ("before", stage.pressure_before),
            ("after", stage.pressure_after),
            ("retention_blockers", stage.retention_blockers as u64),
            ("eviction_before", stage.eviction_pressure_before),
            ("eviction_after", stage.eviction_pressure_after),
        ] {
            append_storage_manager_phase_kind_value(
                out,
                "temporalstore_storage_manager_phase_pressure",
                report.shard_id,
                &stage.stage,
                kind,
                value,
            );
        }
        for (kind, value) in [
            ("candidates", stage.candidate_count as u64),
            ("skipped", stage.skipped_count as u64),
            ("selected_slots", stage.selected_buckets.len() as u64),
            (
                "selected_page_segments",
                stage.selected_page_slab_ids.len() as u64,
            ),
            ("dirty_slots", stage.dirty_bucket_count as u64),
            ("dumped_slots", stage.dumped_bucket_count as u64),
            ("wal_records_removed", stage.wal_records_removed as u64),
            (
                "index_log_records_removed",
                stage.index_log_records_removed as u64,
            ),
            (
                "expired_records_removed",
                stage.expired_records_removed as u64,
            ),
            ("cache_entries_removed", stage.cache_entries_removed as u64),
            ("dropped_objects", stage.dropped_object_count as u64),
            (
                "page_segments_reclaimed",
                stage.page_slabs_reclaimed as u64,
            ),
            ("pages_compacted", stage.pages_compacted as u64),
            ("rewritten_page_refs", stage.rewritten_page_refs as u64),
            ("manifest_pruned", stage.manifest_pruned_count as u64),
            ("metrics_slots", stage.metrics_bucket_count as u64),
            ("metrics_page_refs", stage.metrics_page_ref_count),
        ] {
            append_storage_manager_phase_kind_value(
                out,
                "temporalstore_storage_manager_phase_work",
                report.shard_id,
                &stage.stage,
                kind,
                value,
            );
        }
        for (kind, value) in [
            ("before", stage.before_bytes),
            ("after", stage.after_bytes),
            ("live", stage.live_bytes),
            ("stale", stage.stale_bytes),
            ("reclaimed", stage.bytes_reclaimed),
            ("page_reclaimed", stage.page_bytes_reclaimed),
            ("cache_disk_removed", stage.cache_disk_bytes_removed),
        ] {
            append_storage_manager_phase_kind_value(
                out,
                "temporalstore_storage_manager_phase_bytes",
                report.shard_id,
                &stage.stage,
                kind,
                value,
            );
        }
        for (kind, value) in [
            ("wal", stage.wal_floor_sequence),
            ("index_log", stage.index_log_floor_sequence),
            ("retain_from_wal", stage.retain_from_wal_sequence),
            (
                "retain_from_index_log",
                stage.retain_from_index_log_sequence,
            ),
        ] {
            append_storage_manager_phase_kind_value(
                out,
                "temporalstore_storage_manager_phase_floors",
                report.shard_id,
                &stage.stage,
                kind,
                value,
            );
        }
        append_storage_manager_phase_value(
            out,
            "temporalstore_storage_manager_phase_errors",
            report.shard_id,
            &stage.stage,
            stage.errors.len() as u64,
        );
    }
}

fn append_storage_manager_phase_bool(
    out: &mut String,
    name: &str,
    shard_id: u64,
    phase: &str,
    value: bool,
) {
    append_storage_manager_phase_value(out, name, shard_id, phase, u64::from(value));
}

fn append_storage_manager_phase_value(
    out: &mut String,
    name: &str,
    shard_id: u64,
    phase: &str,
    value: u64,
) {
    out.push_str(name);
    out.push_str("{shard_id=\"");
    out.push_str(&shard_id.to_string());
    out.push_str("\",phase=\"");
    out.push_str(phase);
    out.push_str("\"} ");
    out.push_str(&value.to_string());
    out.push('\n');
}

fn append_storage_manager_phase_kind_value(
    out: &mut String,
    name: &str,
    shard_id: u64,
    phase: &str,
    kind: &str,
    value: u64,
) {
    out.push_str(name);
    out.push_str("{shard_id=\"");
    out.push_str(&shard_id.to_string());
    out.push_str("\",phase=\"");
    out.push_str(phase);
    out.push_str("\",kind=\"");
    out.push_str(kind);
    out.push_str("\"} ");
    out.push_str(&value.to_string());
    out.push('\n');
}

fn append_ingestion_metrics(out: &mut String, engine: &TemporalEngine) {
    let report = engine.ingestion_state_report();
    out.push_str(
        "# HELP temporalstore_ingestion_records_total Ingestion record counters by kind.\n",
    );
    out.push_str("# TYPE temporalstore_ingestion_records_total counter\n");
    for (kind, value) in [
        ("accepted", report.stats.accepted_total),
        ("failed", report.stats.failed_total),
        ("duplicate", report.stats.duplicate_total),
        ("dead_letter", report.stats.dead_letter_total),
        ("kafka_committed", report.stats.kafka_committed_total),
    ] {
        out.push_str("temporalstore_ingestion_records_total{kind=\"");
        out.push_str(kind);
        out.push_str("\"} ");
        out.push_str(&value.to_string());
        out.push('\n');
    }
    out.push_str(
        "# HELP temporalstore_ingestion_kafka_max_lag Max observed Kafka ingestion lag.\n",
    );
    out.push_str("# TYPE temporalstore_ingestion_kafka_max_lag gauge\n");
    out.push_str("temporalstore_ingestion_kafka_max_lag ");
    out.push_str(&report.stats.max_kafka_lag.max(0).to_string());
    out.push('\n');
    out.push_str(
        "# HELP temporalstore_ingestion_kafka_ledgers Current Kafka offset ledger entries.\n",
    );
    out.push_str("# TYPE temporalstore_ingestion_kafka_ledgers gauge\n");
    out.push_str("temporalstore_ingestion_kafka_ledgers ");
    out.push_str(&report.kafka_offsets.len().to_string());
    out.push('\n');
    out.push_str("# HELP temporalstore_ingestion_dead_letters Current persisted ingestion dead-letter records.\n");
    out.push_str("# TYPE temporalstore_ingestion_dead_letters gauge\n");
    out.push_str("temporalstore_ingestion_dead_letters ");
    out.push_str(&report.dead_letters.len().to_string());
    out.push('\n');
    out.push_str("# HELP temporalstore_ingestion_flink_checkpoints Current Flink checkpoint states by status.\n");
    out.push_str("# TYPE temporalstore_ingestion_flink_checkpoints gauge\n");
    for status in [
        FlinkCheckpointStatus::Precommitted,
        FlinkCheckpointStatus::Committed,
        FlinkCheckpointStatus::Aborted,
    ] {
        let label = match status {
            FlinkCheckpointStatus::Precommitted => "precommitted",
            FlinkCheckpointStatus::Committed => "committed",
            FlinkCheckpointStatus::Aborted => "aborted",
        };
        let count = report
            .flink_checkpoints
            .iter()
            .filter(|checkpoint| checkpoint.status == status)
            .count();
        out.push_str("temporalstore_ingestion_flink_checkpoints{status=\"");
        out.push_str(label);
        out.push_str("\"} ");
        out.push_str(&count.to_string());
        out.push('\n');
    }
}

fn start_heartbeat_loop(
    engine: TemporalEngine,
    runtime: DataNodeRuntime,
    meta_addr: String,
    server_addr: String,
    binary_version: String,
    interval_ms: u64,
    boot_time_ms: u64,
) {
    if interval_ms == 0 {
        return;
    }
    std::thread::spawn(move || loop {
        let _ = send_heartbeat(
            &engine,
            &runtime,
            &meta_addr,
            &server_addr,
            &binary_version,
            boot_time_ms,
        );
        std::thread::sleep(std::time::Duration::from_millis(interval_ms));
    });
}

