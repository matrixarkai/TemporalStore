// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! DataNodeRuntime reporting/topology methods, extracted from data_node.rs.

use super::*;

impl DataNodeRuntime {
    pub fn stats(&self) -> DataNodeRuntimeStats {
        let stats = self
            .inner
            .stats
            .lock()
            .expect("runtime stats lock poisoned");
        let dirty = self
            .inner
            .dirty
            .lock()
            .expect("dirty tracker lock poisoned");
        let queue = self
            .inner
            .queue
            .lock()
            .expect("runtime queue lock poisoned");
        let queue_depth = queue.queued_total;
        let queued_shard_count = queue.by_shard.len();
        let running_shard_count = queue.running_shards.len();
        let dirty_shard_count = dirty
            .by_key
            .values()
            .map(|info| info.shard_id)
            .collect::<BTreeSet<_>>()
            .len();
        DataNodeRuntimeStats {
            worker_threads: self.inner.options.worker_threads,
            max_queue_depth: self.inner.options.max_queue_depth,
            max_background_queue_depth: self.inner.options.max_background_queue_depth,
            submitted_total: stats.submitted_total,
            completed_total: stats.completed_total,
            rejected_total: stats.rejected_total,
            rejected_background_total: stats.rejected_background_total,
            timed_out_total: stats.timed_out_total,
            canceled_total: stats.canceled_total,
            queue_depth,
            background_queue_depth: queue.background_queued_total,
            queued_shard_count,
            running_shard_count,
            dirty_object_count: dirty.by_key.len(),
            dirty_shard_count,
            expiry_sweeps: stats.expiry_sweeps,
            expired_records_removed: stats.expired_records_removed,
            dump_runs: stats.dump_runs,
            compaction_runs: stats.compaction_runs,
            gc_runs: stats.gc_runs,
            storage_lifecycle_runs: stats.storage_lifecycle_runs,
            storage_manager_runs: stats.storage_manager_runs,
            storage_manager_loops: stats.storage_manager_loops,
            storage_manager_prepare_runs: stats.storage_manager_prepare_runs,
            storage_manager_reclaim_wal_runs: stats.storage_manager_reclaim_wal_runs,
            storage_manager_reclaim_memory_runs: stats.storage_manager_reclaim_memory_runs,
            storage_manager_expire_runs: stats.storage_manager_expire_runs,
            storage_manager_reclaim_page_runs: stats.storage_manager_reclaim_page_runs,
            storage_manager_compact_runs: stats.storage_manager_compact_runs,
            storage_manager_index_gc_runs: stats.storage_manager_index_gc_runs,
        }
    }

    pub fn preflight_report(&self) -> DataNodePreflightReport {
        let stats = self.stats();
        let metaserver = self.metaserver_heartbeat_report();
        let mut degraded_reasons = Vec::new();
        if stats.queue_depth >= stats.max_queue_depth && stats.max_queue_depth > 0 {
            degraded_reasons.push("foreground_queue_full".to_string());
        }
        if stats.background_queue_depth >= stats.max_background_queue_depth
            && stats.max_background_queue_depth > 0
        {
            degraded_reasons.push("background_queue_full".to_string());
        }
        if stats.rejected_total > 0 {
            degraded_reasons.push("rejected_requests".to_string());
        }
        if stats.timed_out_total > 0 {
            degraded_reasons.push("timed_out_requests".to_string());
        }
        if metaserver.last_heartbeat_at_ms > 0 && !metaserver.last_status.ok {
            degraded_reasons.push("metaserver_heartbeat_failed".to_string());
        }
        if metaserver.forbid_auto_register {
            degraded_reasons.push("metaserver_forbid_auto_register".to_string());
        }
        let lifecycle_persistence = self.lifecycle_persistence_report();
        if lifecycle_persistence
            .last_restore_status
            .as_ref()
            .map(|status| !status.ok)
            .unwrap_or(false)
        {
            degraded_reasons.push("lifecycle_snapshot_restore_failed".to_string());
        }
        if lifecycle_persistence
            .last_persist_status
            .as_ref()
            .map(|status| !status.ok)
            .unwrap_or(false)
        {
            degraded_reasons.push("lifecycle_snapshot_persist_failed".to_string());
        }
        let status = if degraded_reasons.is_empty() {
            Status::ok()
        } else {
            Status::error("degraded", degraded_reasons.join(","))
        };
        DataNodePreflightReport {
            status,
            stats,
            lifecycle: self.lifecycle_report(),
            lifecycle_persistence,
            topology_validation: self.topology_validation_report(&metaserver),
            metaserver,
            queued_workers: self.queued_shard_worker_infos(),
            dirty_shards: self.dirty_shards(),
            dirty_objects: self.dirty_objects(),
            degraded_reasons,
        }
    }

    pub fn lifecycle_report(&self) -> DataNodeLifecycleReport {
        let stats = self.stats();
        let mut shards = self.shard_serving_states();
        shards.sort_by_key(|state| state.shard_id);
        let mut transitions = self.lifecycle_states();
        transitions.sort_by_key(|state| state.shard_id);
        let loaded_shard_count = shards.iter().filter(|state| state.loaded).count();
        let serving_count = shards
            .iter()
            .filter(|state| state.serving_state == "serving")
            .count();
        let readonly_count = shards.iter().filter(|state| state.readonly).count();
        let queued_count = shards
            .iter()
            .filter(|state| state.serving_state == "queued")
            .count();
        let running_count = shards
            .iter()
            .filter(|state| state.serving_state == "running")
            .count();
        let unloading_count = shards
            .iter()
            .filter(|state| state.serving_state == "unloading")
            .count();
        let failed_shards = shards
            .iter()
            .filter(|state| state.serving_state == "failed")
            .map(|state| state.shard_id)
            .chain(
                transitions
                    .iter()
                    .filter(|state| state.state == "failed")
                    .map(|state| state.shard_id),
            )
            .collect::<BTreeSet<_>>();
        let failed_count = failed_shards.len();
        let max_load_version = shards
            .iter()
            .map(|state| state.load_version)
            .chain(transitions.iter().map(|state| state.load_version))
            .max()
            .unwrap_or_default();
        let mut storage_lifecycle_metrics = data_node_storage_lifecycle_metrics(&stats);
        apply_shard_storage_metrics(&mut storage_lifecycle_metrics, &shards);
        let (
            storage_write_contract,
            storage_read_contract,
            storage_cold_scan_contract,
            storage_manager_contract,
            storage_index_contract,
            storage_cache_contract,
            storage_reclaim_contract,
        ) = data_node_storage_contracts_from_metrics(&storage_lifecycle_metrics);
        DataNodeLifecycleReport {
            public_storage_contract: PublicStorageContract::default(),
            public_storage_feature_shapes: PublicStorageFeatureShapes::default(),
            effective_storage_tuning: effective_storage_tuning_from_env(),
            storage_lifecycle_metrics: storage_lifecycle_metrics.clone(),
            storage_write_contract,
            storage_read_contract,
            storage_cold_scan_contract,
            storage_manager_contract,
            storage_index_contract,
            storage_cache_contract,
            storage_reclaim_contract,
            storage_safety_snapshot: storage_safety_snapshot_from_metrics(
                &storage_lifecycle_metrics,
            ),
            storage_watermark_snapshot: storage_watermark_snapshot_from_metrics(
                &storage_lifecycle_metrics,
            ),
            storage_gc_snapshot: storage_gc_snapshot_from_metrics(&storage_lifecycle_metrics),
            storage_index_snapshot: storage_index_snapshot_from_metrics(&storage_lifecycle_metrics),
            storage_topology_snapshot: storage_topology_snapshot_from_metrics(
                &storage_lifecycle_metrics,
            ),
            storage_write_sequence: default_storage_write_sequence(),
            storage_read_sequence: default_storage_read_sequence(),
            storage_cold_scan_sequence: default_storage_cold_scan_sequence(),
            storage_lifecycle_phases: default_storage_lifecycle_phases(),
            storage_cache_layers: default_storage_cache_layers(),
            storage_cache_semantics: default_storage_cache_semantics(),
            storage_reclaim_semantics: default_storage_reclaim_semantics(),
            storage_reclaim_scope: default_storage_reclaim_scope(),
            loaded_shard_count,
            serving_count,
            readonly_count,
            queued_count,
            running_count,
            unloading_count,
            failed_count,
            max_load_version,
            shards,
            transitions,
        }
    }

    pub fn grpc_streaming_contract(&self) -> DataNodeGrpcStreamingContract {
        DataNodeGrpcStreamingContract::default()
    }

    pub fn distributed_admission_decision(
        &self,
        shard_id: ShardId,
        peer_snapshots: &[DistributedAdmissionPeerSnapshot],
        read_budget: u64,
        write_budget: u64,
        min_topology_version: u64,
    ) -> DistributedAdmissionDecision {
        distributed_admission_decision(
            shard_id,
            peer_snapshots,
            read_budget,
            write_budget,
            min_topology_version,
        )
    }

    pub fn lifecycle_states(&self) -> Vec<DataNodeShardLifecycleState> {
        self.inner
            .lifecycle
            .lock()
            .expect("runtime lifecycle lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub fn topology_validation_report(
        &self,
        metaserver: &DataNodeMetaHeartbeatReport,
    ) -> DataNodeTopologyValidationReport {
        let mut loaded_shards = self
            .shard_serving_states()
            .into_iter()
            .filter(|state| state.loaded)
            .map(|state| state.shard_id)
            .collect::<Vec<_>>();
        loaded_shards.sort_unstable();
        DataNodeTopologyValidationReport {
            loaded_shard_count: loaded_shards.len(),
            loaded_shards,
            last_meta_topology_version: metaserver.last_topology_version,
            authoritative_topology_version: 0,
            validated_against_metaserver: false,
            mismatch_count: 0,
            missing_in_meta: Vec::new(),
            mismatches: Vec::new(),
            validation_limited: true,
            limitation_reason:
                "metaserver topology partition map is not attached to node preflight".to_string(),
        }
    }

    pub fn validate_topology_against_metaserver(
        &self,
        server_addr: &str,
        topologies: &[TableTopologyResponse],
    ) -> DataNodeTopologyValidationReport {
        let metaserver = self.metaserver_heartbeat_report();
        let loaded_states = self
            .shard_serving_states()
            .into_iter()
            .filter(|state| state.loaded)
            .collect::<Vec<_>>();
        let mut loaded_shards = loaded_states
            .iter()
            .map(|state| state.shard_id)
            .collect::<Vec<_>>();
        loaded_shards.sort_unstable();

        let authoritative_topology_version = topologies
            .iter()
            .filter_map(|topology| topology.table.as_ref().map(|table| table.topology_version))
            .max()
            .unwrap_or_default();
        let mut missing_in_meta = Vec::new();
        let mut mismatches = Vec::new();
        for state in &loaded_states {
            let partition = topologies
                .iter()
                .flat_map(|topology| topology.shards.iter())
                .find(|partition| partition.shard_id == state.shard_id);
            let Some(partition) = partition else {
                missing_in_meta.push(state.shard_id);
                mismatches.push(DataNodeTopologyMismatch {
                    shard_id: state.shard_id,
                    kind: "missing_partition".to_string(),
                    detail: "loaded shard is not present in supplied metaserver topology"
                        .to_string(),
                });
                continue;
            };
            if !state.readonly && partition.primary.as_deref() != Some(server_addr) {
                mismatches.push(DataNodeTopologyMismatch {
                    shard_id: state.shard_id,
                    kind: "primary_mismatch".to_string(),
                    detail: format!(
                        "node serves primary but metaserver primary is {:?}",
                        partition.primary
                    ),
                });
            }
            if state.readonly
                && partition.primary.as_deref() != Some(server_addr)
                && !partition
                    .replicas
                    .iter()
                    .any(|replica| replica == server_addr)
            {
                mismatches.push(DataNodeTopologyMismatch {
                    shard_id: state.shard_id,
                    kind: "replica_mismatch".to_string(),
                    detail: "readonly loaded shard is neither primary nor replica in metaserver topology"
                        .to_string(),
                });
            }
            if u64::from(state.start_routing_bucket) != partition.start_bucket
                || u64::from(state.end_routing_bucket) != partition.end_bucket
            {
                mismatches.push(DataNodeTopologyMismatch {
                    shard_id: state.shard_id,
                    kind: "routing_slot_mismatch".to_string(),
                    detail: format!(
                        "local={}..{}, meta={}..{}",
                        state.start_routing_bucket,
                        state.end_routing_bucket,
                        partition.start_bucket,
                        partition.end_bucket
                    ),
                });
            }
        }
        let mismatch_count = mismatches.len();
        DataNodeTopologyValidationReport {
            loaded_shard_count: loaded_shards.len(),
            loaded_shards,
            last_meta_topology_version: metaserver.last_topology_version,
            authoritative_topology_version,
            validated_against_metaserver: true,
            mismatch_count,
            missing_in_meta,
            mismatches,
            validation_limited: false,
            limitation_reason: String::new(),
        }
    }

    pub fn metaserver_heartbeat_report(&self) -> DataNodeMetaHeartbeatReport {
        self.inner
            .meta_heartbeat
            .lock()
            .expect("meta heartbeat lock poisoned")
            .clone()
    }

    pub fn record_metaserver_heartbeat(&self, response: &ServerHeartbeatResponse) {
        let mut report = self
            .inner
            .meta_heartbeat
            .lock()
            .expect("meta heartbeat lock poisoned");
        report.last_heartbeat_at_ms = now_ms();
        report.last_status = response.status.clone();
        report.last_topology_version = response.topology_version;
        report.last_server_state = response.server_state.clone();
        report.forbid_auto_register = response.forbid_auto_register;
        report.consecutive_failures = if response.status.ok {
            0
        } else {
            report.consecutive_failures.saturating_add(1)
        };
    }

    pub fn server_runtime_load(&self) -> ServerRuntimeLoad {
        let preflight = self.preflight_report();
        let metaserver = preflight.metaserver.clone();
        ServerRuntimeLoad {
            queue_depth: preflight.stats.queue_depth,
            background_queue_depth: preflight.stats.background_queue_depth,
            queued_shard_count: preflight.stats.queued_shard_count,
            running_shard_count: preflight.stats.running_shard_count,
            dirty_object_count: preflight.stats.dirty_object_count,
            dirty_shard_count: preflight.stats.dirty_shard_count,
            rejected_total: preflight.stats.rejected_total,
            rejected_background_total: preflight.stats.rejected_background_total,
            timed_out_total: preflight.stats.timed_out_total,
            canceled_total: preflight.stats.canceled_total,
            dump_runs: preflight.stats.dump_runs,
            compaction_runs: preflight.stats.compaction_runs,
            gc_runs: preflight.stats.gc_runs,
            storage_lifecycle_runs: preflight.stats.storage_lifecycle_runs,
            last_meta_topology_version: metaserver.last_topology_version,
            meta_heartbeat_consecutive_failures: metaserver.consecutive_failures,
            meta_forbid_auto_register: metaserver.forbid_auto_register,
            degraded_reasons: preflight.degraded_reasons,
        }
    }

    pub fn shard_serving_states(&self) -> Vec<ServerShardServingState> {
        let dirty_objects = self.dirty_objects();
        let mut dirty_by_shard = HashMap::<ShardId, usize>::new();
        for dirty in dirty_objects {
            *dirty_by_shard.entry(dirty.shard_id).or_default() += 1;
        }
        let queue = self
            .inner
            .queue
            .lock()
            .expect("runtime queue lock poisoned");
        let queued_shards = queue.by_shard.keys().copied().collect::<BTreeSet<_>>();
        let running_shards = queue.running_shards.clone();
        drop(queue);
        let lifecycle_by_shard = self
            .inner
            .lifecycle
            .lock()
            .expect("runtime lifecycle lock poisoned")
            .clone();

        self.inner
            .engine
            .loaded_shard_stats()
            .into_iter()
            .map(|stats| {
                let lifecycle_state = lifecycle_by_shard
                    .get(&stats.shard_id)
                    .map(|state| state.state.as_str());
                let serving_state = if matches!(
                    lifecycle_state,
                    Some("loading" | "reloading" | "unloading" | "failed")
                ) {
                    lifecycle_state.unwrap()
                } else if running_shards.contains(&stats.shard_id) {
                    "running"
                } else if queued_shards.contains(&stats.shard_id) {
                    "queued"
                } else if stats.readonly {
                    "readonly"
                } else if stats.loaded {
                    "serving"
                } else {
                    "unloaded"
                };
                let worker = self.shard_worker_info(stats.shard_id);
                ServerShardServingState {
                    shard_id: stats.shard_id,
                    serving_state: serving_state.to_string(),
                    worker_index: worker.worker_index,
                    worker_threads: worker.worker_threads,
                    loaded: stats.loaded,
                    readonly: stats.readonly,
                    load_version: stats.load_version,
                    table_name: stats.shard_stat_info.table_name,
                    shard_uri: stats.shard_stat_info.shard_uri,
                    start_routing_bucket: stats.shard_stat_info.start_routing_bucket,
                    end_routing_bucket: stats.shard_stat_info.end_routing_bucket,
                    total_records: stats.total_records,
                    storage_bytes: stats.storage_bytes,
                    cache_memory_bytes: stats.cache.memory_bytes,
                    storage: stats.storage.clone(),
                    block_store_bytes_written: stats.block_store.bytes_written,
                    wal_sequence: stats.write_ahead_log.last_sequence,
                    dirty_object_count: dirty_by_shard
                        .get(&stats.shard_id)
                        .copied()
                        .unwrap_or_default() as u64,
                    dirty_bucket_count: stats.object_manager.dirty_bucket_count as u64,
                }
            })
            .collect()
    }
}
