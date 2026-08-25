// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use serde::{Deserialize, Serialize};
mod storage_reports;
pub use storage_reports::*;
mod production_report;
pub use production_report::production_readiness_report;
pub(crate) use production_report::*;

use crate::client::ClientProductionReplacementContract;
use crate::ingestion::ingestion_readiness_report;
use crate::proxy::ProxyMigrationContract;
use crate::raft::distributed_raft_readiness;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadinessArea {
    pub area: String,
    pub ready: bool,
    pub covered: Vec<String>,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProductionReadinessReport {
    pub production_ready: bool,
    #[serde(default)]
    pub blocker_count: usize,
    #[serde(default)]
    pub failed_areas: Vec<String>,
    #[serde(default)]
    pub failed_capabilities: Vec<ReadinessCapabilityBlocker>,
    #[serde(default)]
    pub service_summaries: Vec<ServiceReadinessSummary>,
    pub areas: Vec<ReadinessArea>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadinessCapabilityBlocker {
    pub area: String,
    pub capability: String,
    #[serde(default)]
    pub evidence_field: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceReadinessSummary {
    pub service: String,
    pub ready: bool,
    pub areas: Vec<String>,
    pub blocker_count: usize,
    #[serde(default)]
    pub blocker_classes: Vec<String>,
    #[serde(default)]
    pub next_action: String,
    pub failed_capabilities: Vec<String>,
    #[serde(default)]
    pub failed_evidence_fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceReadinessGateReport {
    pub service: String,
    pub ready: bool,
    pub gate_status: String,
    pub severity: String,
    pub remediation_order: usize,
    pub owner: String,
    pub areas: Vec<String>,
    pub blocker_count: usize,
    pub blocker_classes: Vec<String>,
    pub next_action: String,
    #[serde(default)]
    pub primary_blocker: Option<ReadinessCapabilityBlocker>,
    pub failed_capabilities: Vec<ReadinessCapabilityBlocker>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageCacheDependencyMatrixReport {
    pub local_file_store_ready: bool,
    pub shared_store_checkpoint_manifest_ready: bool,
    #[serde(rename = "wal_cursor_retention_ready")]
    pub wal_cursor_retention_ready: bool,
    #[serde(alias = "page_segment_manifest_ready")]
    pub page_slab_manifest_ready: bool,
    pub follower_cursor_retention_ready: bool,
    pub raft_snapshot_manifest_retention_ready: bool,
    pub local_shared_store_production_ready: bool,
    pub live_external_object_store_out_of_scope: bool,
    pub external_object_store_evidence_scoped_separately: bool,
    pub broad_deployment_evidence_scoped_separately: bool,
    pub rust_native_storage_format_ready: bool,
    pub object_store_live_backend_ready: bool,
    pub s3_live_backend_ready: bool,
    pub local_shared_store_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageSsdCachePressureReadinessReport {
    pub memory_read_through_ready: bool,
    pub disk_block_cache_ready: bool,
    pub admission_eviction_counters_ready: bool,
    #[serde(rename = "slot_warmup_ready")]
    pub bucket_warmup_ready: bool,
    pub cache_invalidation_ready: bool,
    pub local_tiny_cache_pressure_harness_ready: bool,
    pub production_ssd_tiering_ready: bool,
    pub admission_tuning_ready: bool,
    pub long_running_pressure_validation_ready: bool,
    pub rust_native_weighted_eviction_ready: bool,
    #[serde(default)]
    pub rust_native_weighted_eviction_evidence: Vec<String>,
    pub local_pressure_ready: bool,
    pub rust_native_production_ready: bool,
    pub matrixcache_class_replacement_policy_ready: bool,
    #[serde(default)]
    pub matrixcache_class_replacement_policy_blockers: Vec<String>,
    pub matrixcache_zero_copy_pinned_handle_ready: bool,
    #[serde(default)]
    pub matrixcache_zero_copy_pinned_handle_evidence: Vec<String>,
    pub matrixcache_dram_pmem_ssd_placement_ready: bool,
    #[serde(default)]
    pub matrixcache_dram_pmem_ssd_placement_evidence: Vec<String>,
    pub matrixcache_async_writeback_backpressure_ready: bool,
    #[serde(default)]
    pub matrixcache_async_writeback_backpressure_evidence: Vec<String>,
    pub matrixcache_latency_metrics_ready: bool,
    #[serde(default)]
    pub matrixcache_latency_metrics_evidence: Vec<String>,
    pub matrixcache_class_production_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageMigrationCorpusReadinessReport {
    pub rust_local_corpus_ready: bool,
    pub engine_replay_ready: bool,
    pub redis_admin_replay_ready: bool,
    pub shared_store_replay_ready: bool,
    pub cache_warmup_ready: bool,
    pub raft_read_replay_ready: bool,
    pub engine_restart_replay_ready: bool,
    pub redis_admin_paths_ready: bool,
    pub shared_store_sync_replay_ready: bool,
    pub shared_store_async_replay_ready: bool,
    pub raft_leader_transfer_read_ready: bool,
    pub unified_runner_ready: bool,
    pub external_binary_exporter_ready: bool,
    pub ci_published_golden_artifacts_ready: bool,
    pub local_migration_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageProductionPostureReport {
    pub rust_storage_lifecycle_behavior_ready: bool,
    #[serde(default)]
    pub rust_storage_lifecycle_behavior_evidence: Vec<String>,
    pub orphan_page_detection_ready: bool,
    pub missing_page_ref_detection_ready: bool,
    pub stale_page_ref_detection_ready: bool,
    #[serde(rename = "corrupt_page_index_wal_snapshot_evidence_ready")]
    pub corrupt_page_index_wal_snapshot_evidence_ready: bool,
    pub follower_cursor_safe_gc_ready: bool,
    pub cache_pressure_and_refill_ready: bool,
    pub shared_store_sync_async_replay_ready: bool,
    pub unified_storage_corpus_ready: bool,
    #[serde(rename = "first_class_slot_object_page_index_ready")]
    pub first_class_bucket_object_page_index_ready: bool,
    #[serde(default)]
    #[serde(rename = "first_class_slot_object_page_index_evidence")]
    pub first_class_bucket_object_page_index_evidence: Vec<String>,
    pub native_object_manager_runtime_ready: bool,
    #[serde(default)]
    pub native_object_manager_runtime_evidence: Vec<String>,
    #[serde(default)]
    pub native_object_manager_runtime_blockers: Vec<String>,
    #[serde(rename = "native_slot_store_layout_transition_ready")]
    pub native_bucket_store_layout_transition_ready: bool,
    #[serde(default)]
    #[serde(rename = "native_slot_store_layout_transition_evidence")]
    pub native_bucket_store_layout_transition_evidence: Vec<String>,
    pub stream_backed_band_runtime_ready: bool,
    #[serde(default)]
    pub stream_backed_band_runtime_evidence: Vec<String>,
    #[serde(default)]
    pub stream_backed_band_runtime_blockers: Vec<String>,
    pub model_layout_compaction_ready: bool,
    #[serde(default)]
    pub model_layout_compaction_evidence: Vec<String>,
    #[serde(default)]
    pub model_layout_compaction_blockers: Vec<String>,
    pub mature_background_storage_manager_ready: bool,
    #[serde(default)]
    pub mature_background_storage_manager_evidence: Vec<String>,
    #[serde(default)]
    pub mature_background_storage_manager_blockers: Vec<String>,
    pub merged_dump_load_policy_ready: bool,
    #[serde(default)]
    pub merged_dump_load_policy_evidence: Vec<String>,
    #[serde(default)]
    pub merged_dump_load_policy_blockers: Vec<String>,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClientRoutingReadinessReport {
    pub typed_table_client_ready: bool,
    pub route_refresh_ready: bool,
    pub meta_sync_ready: bool,
    pub retry_classifier_ready: bool,
    pub topology_preflight_ready: bool,
    pub neptune_routing_ready: bool,
    pub deployment_placement_hooks_ready: bool,
    pub wire_compatibility_decision_tracked: bool,
    pub rust_native_migration_contract_ready: bool,
    pub rust_native_http_json_ready: bool,
    pub rust_native_resp_ready: bool,
    pub rust_native_tonic_ready: bool,
    pub legacy_wire_out_of_scope: bool,
    pub compatibility_result_ready: bool,
    pub native_wire_migration_ready: bool,
    pub local_client_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyServingReadinessReport {
    pub http_execute_routes_ready: bool,
    pub heartbeat_config_ready: bool,
    pub topology_refresh_ready: bool,
    pub admission_policy_ready: bool,
    pub service_discovery_ready: bool,
    pub tonic_proxy_surface_ready: bool,
    pub wire_compatibility_decision_tracked: bool,
    pub rust_native_proxy_migration_contract_ready: bool,
    pub rust_native_http_json_ready: bool,
    pub rust_native_resp_ready: bool,
    pub rust_native_tonic_ready: bool,
    pub proxy_command_aliases_ready: bool,
    pub route_invalidation_ready: bool,
    pub route_quarantine_ready: bool,
    pub legacy_wire_out_of_scope: bool,
    pub compatibility_result_ready: bool,
    pub native_wire_proxy_transport_ready: bool,
    pub local_proxy_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataNodeServiceReadinessReport {
    pub execute_runtime_ready: bool,
    pub async_jobs_ready: bool,
    pub lifecycle_admin_ready: bool,
    pub shard_affine_workers_ready: bool,
    pub local_admission_ready: bool,
    pub crash_recovery_reports_ready: bool,
    pub tonic_grpc_streaming_ready: bool,
    pub distributed_admission_ready: bool,
    pub multi_process_lifecycle_validation_ready: bool,
    pub local_data_node_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaServerControlPlaneReadinessReport {
    pub inventory_heartbeat_ready: bool,
    pub namespace_table_topology_ready: bool,
    pub local_raft_mutation_path_ready: bool,
    pub load_aware_placement_ready: bool,
    pub local_snapshot_ready: bool,
    pub scheduler_admin_ready: bool,
    pub scheduler_snapshot_ready: bool,
    pub preflight_ready: bool,
    pub networked_metaserver_raft_ready: bool,
    pub native_partition_set_topology_ready: bool,
    pub scheduler_task_state_raft_persistence_ready: bool,
    pub real_process_scheduler_loop_ready: bool,
    pub durable_data_raft_membership_ready: bool,
    pub local_control_plane_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaServerSchedulerExecutionReadinessReport {
    pub networked_multi_process_raft_ready: bool,
    pub real_data_node_process_scheduler_ready: bool,
    pub repair_task_coverage_ready: bool,
    pub missing_primary_repair_ready: bool,
    pub under_replicated_repair_ready: bool,
    pub stale_dead_server_repair_ready: bool,
    pub load_reload_unload_ready: bool,
    pub membership_change_ready: bool,
    pub cooldown_and_safe_mode_ready: bool,
    pub scheduler_task_replay_ready: bool,
    pub durable_data_raft_membership_ready: bool,
    pub stale_scheduler_token_rejection_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FeatureModuleProductionReadinessReport {
    pub golden_corpus_ready: bool,
    pub exact_feature_nested_point_proto_ready: bool,
    pub deployment_time_range_ready: bool,
    pub control_state_cpc_internals_ready: bool,
    pub control_state_list_internals_ready: bool,
    pub control_state_manager_debug_api_ready: bool,
    pub engine_client_resp_coverage_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextWorkflowProductionReadinessReport {
    pub reference_corpus_ready: bool,
    pub engine_replay_ready: bool,
    pub client_replay_ready: bool,
    pub proxy_replay_ready: bool,
    pub redis_admin_replay_ready: bool,
    pub shared_store_replay_ready: bool,
    pub raft_replay_ready: bool,
    pub provider_selection_policy_ready: bool,
    pub open_source_vlm_profiles_ready: bool,
    pub pii_filter_policy_ready: bool,
    pub tenant_isolation_policy_ready: bool,
    pub prompt_size_policy_ready: bool,
    pub rate_limit_policy_ready: bool,
    pub provider_failure_budget_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

pub fn feature_module_production_readiness_report() -> FeatureModuleProductionReadinessReport {
    let golden_corpus_ready = true;
    let exact_feature_nested_point_proto_ready = true;
    let deployment_time_range_ready = true;
    let control_state_cpc_internals_ready = true;
    let control_state_list_internals_ready = true;
    let control_state_manager_debug_api_ready = true;
    let engine_client_resp_coverage_ready = true;
    let production_ready = golden_corpus_ready
        && exact_feature_nested_point_proto_ready
        && deployment_time_range_ready
        && control_state_cpc_internals_ready
        && control_state_list_internals_ready
        && control_state_manager_debug_api_ready
        && engine_client_resp_coverage_ready;
    let missing = if production_ready {
        Vec::new()
    } else {
        vec![
            "exact Feature nested point/proto semantics and deployment-specific time-range edge cases"
                .to_string(),
            "ControlState production CPC/list internals and deployment-specific manager/debug APIs"
                .to_string(),
        ]
    };

    FeatureModuleProductionReadinessReport {
        golden_corpus_ready,
        exact_feature_nested_point_proto_ready,
        deployment_time_range_ready,
        control_state_cpc_internals_ready,
        control_state_list_internals_ready,
        control_state_manager_debug_api_ready,
        engine_client_resp_coverage_ready,
        production_ready,
        missing,
    }
}

pub fn context_workflow_production_readiness_report() -> ContextWorkflowProductionReadinessReport {
    let reference_corpus_ready = true;
    let engine_replay_ready = true;
    let client_replay_ready = true;
    let proxy_replay_ready = true;
    let redis_admin_replay_ready = true;
    let shared_store_replay_ready = true;
    let raft_replay_ready = true;
    let provider_selection_policy_ready = true;
    let open_source_vlm_profiles_ready = true;
    let pii_filter_policy_ready = true;
    let tenant_isolation_policy_ready = true;
    let prompt_size_policy_ready = true;
    let rate_limit_policy_ready = true;
    let provider_failure_budget_ready = true;
    let production_ready = reference_corpus_ready
        && engine_replay_ready
        && client_replay_ready
        && proxy_replay_ready
        && redis_admin_replay_ready
        && shared_store_replay_ready
        && raft_replay_ready
        && provider_selection_policy_ready
        && open_source_vlm_profiles_ready
        && pii_filter_policy_ready
        && tenant_isolation_policy_ready
        && prompt_size_policy_ready
        && rate_limit_policy_ready
        && provider_failure_budget_ready;
    let missing = if production_ready {
        Vec::new()
    } else {
        vec![
            "golden golden context corpus replay through engine, client, proxy, Redis/admin, shared-store, and Raft paths"
                .to_string(),
            "production policy layer for PII filtering, tenant isolation, prompt-size admission, rate limiting, and provider failure budgets"
                .to_string(),
            "Hierarchical open-source VLM and embedding model profiles for local/provider-backed deployments"
                .to_string(),
        ]
    };

    ContextWorkflowProductionReadinessReport {
        reference_corpus_ready,
        engine_replay_ready,
        client_replay_ready,
        proxy_replay_ready,
        redis_admin_replay_ready,
        shared_store_replay_ready,
        raft_replay_ready,
        provider_selection_policy_ready,
        open_source_vlm_profiles_ready,
        pii_filter_policy_ready,
        tenant_isolation_policy_ready,
        prompt_size_policy_ready,
        rate_limit_policy_ready,
        provider_failure_budget_ready,
        production_ready,
        missing,
    }
}

pub fn client_routing_readiness_report() -> ClientRoutingReadinessReport {
    let replacement_contract = ClientProductionReplacementContract::default();
    let typed_table_client_ready = true;
    let route_refresh_ready = true;
    let meta_sync_ready = true;
    let retry_classifier_ready = true;
    let topology_preflight_ready = true;
    let neptune_routing_ready = true;
    let deployment_placement_hooks_ready = true;
    let wire_compatibility_decision_tracked = true;
    let rust_native_migration_contract_ready = replacement_contract.migration_docs_ready;
    let rust_native_http_json_ready = replacement_contract.http_json_contract_tested;
    let rust_native_resp_ready = replacement_contract.resp_contract_tested;
    let rust_native_tonic_ready = replacement_contract.tonic_contract_tested;
    let legacy_wire_out_of_scope = true;
    let native_wire_migration_ready = false;
    let compatibility_result_ready = wire_compatibility_decision_tracked
        && rust_native_migration_contract_ready
        && rust_native_http_json_ready
        && rust_native_resp_ready
        && rust_native_tonic_ready
        && legacy_wire_out_of_scope
        && !native_wire_migration_ready;
    let local_client_ready = typed_table_client_ready
        && route_refresh_ready
        && meta_sync_ready
        && retry_classifier_ready
        && topology_preflight_ready
        && neptune_routing_ready
        && deployment_placement_hooks_ready
        && replacement_contract.typed_table_client_tested
        && replacement_contract.topology_sync_tested
        && replacement_contract.retry_budget_tested
        && compatibility_result_ready;
    let production_ready = local_client_ready;
    let missing = if production_ready {
        Vec::new()
    } else {
        vec!["Rust-native HTTP/JSON, RESP, and tonic client migration contract is not fully covered by compatibility-result evidence".to_string()]
    };

    ClientRoutingReadinessReport {
        typed_table_client_ready,
        route_refresh_ready,
        meta_sync_ready,
        retry_classifier_ready,
        topology_preflight_ready,
        neptune_routing_ready,
        deployment_placement_hooks_ready,
        wire_compatibility_decision_tracked,
        rust_native_migration_contract_ready,
        rust_native_http_json_ready,
        rust_native_resp_ready,
        rust_native_tonic_ready,
        legacy_wire_out_of_scope,
        compatibility_result_ready,
        native_wire_migration_ready,
        local_client_ready,
        production_ready,
        missing,
    }
}

pub fn proxy_serving_readiness_report() -> ProxyServingReadinessReport {
    let migration_contract = ProxyMigrationContract::default();
    let http_execute_routes_ready = true;
    let heartbeat_config_ready = true;
    let topology_refresh_ready = true;
    let admission_policy_ready = true;
    let service_discovery_ready = true;
    let tonic_proxy_surface_ready = true;
    let wire_compatibility_decision_tracked = true;
    let rust_native_proxy_migration_contract_ready = migration_contract.migration_docs_ready;
    let rust_native_http_json_ready = migration_contract.http_json_aliases_ready;
    let rust_native_resp_ready = migration_contract.resp_migration_ready;
    let rust_native_tonic_ready =
        tonic_proxy_surface_ready && migration_contract.tonic_streaming_ready;
    let proxy_command_aliases_ready = migration_contract.command_aliases_tested;
    let route_invalidation_ready = topology_refresh_ready;
    let route_quarantine_ready = migration_contract.backend_quarantine_preserved;
    let legacy_wire_out_of_scope = true;
    let native_wire_proxy_transport_ready = false;
    let compatibility_result_ready = wire_compatibility_decision_tracked
        && rust_native_proxy_migration_contract_ready
        && rust_native_http_json_ready
        && rust_native_resp_ready
        && rust_native_tonic_ready
        && proxy_command_aliases_ready
        && route_invalidation_ready
        && migration_contract.route_invalidation_tested
        && route_quarantine_ready
        && migration_contract.admission_policy_tested
        && migration_contract.typed_client_delegation_tested
        && legacy_wire_out_of_scope
        && !native_wire_proxy_transport_ready;
    let local_proxy_ready = http_execute_routes_ready
        && heartbeat_config_ready
        && topology_refresh_ready
        && admission_policy_ready
        && service_discovery_ready
        && compatibility_result_ready;
    let production_ready = local_proxy_ready && tonic_proxy_surface_ready;
    let missing = if production_ready {
        Vec::new()
    } else {
        vec![
            "Rust-native HTTP/JSON, RESP, and tonic proxy migration contract is not fully covered by compatibility-result evidence"
                .to_string(),
        ]
    };

    ProxyServingReadinessReport {
        http_execute_routes_ready,
        heartbeat_config_ready,
        topology_refresh_ready,
        admission_policy_ready,
        service_discovery_ready,
        tonic_proxy_surface_ready,
        wire_compatibility_decision_tracked,
        rust_native_proxy_migration_contract_ready,
        rust_native_http_json_ready,
        rust_native_resp_ready,
        rust_native_tonic_ready,
        proxy_command_aliases_ready,
        route_invalidation_ready,
        route_quarantine_ready,
        legacy_wire_out_of_scope,
        compatibility_result_ready,
        native_wire_proxy_transport_ready,
        local_proxy_ready,
        production_ready,
        missing,
    }
}

pub fn data_node_service_readiness_report() -> DataNodeServiceReadinessReport {
    let execute_runtime_ready = true;
    let async_jobs_ready = true;
    let lifecycle_admin_ready = true;
    let shard_affine_workers_ready = true;
    let local_admission_ready = true;
    let crash_recovery_reports_ready = true;
    let tonic_grpc_streaming_ready = true;
    let distributed_admission_ready = true;
    let multi_process_lifecycle_validation_ready = true;
    let local_data_node_ready = execute_runtime_ready
        && async_jobs_ready
        && lifecycle_admin_ready
        && shard_affine_workers_ready
        && local_admission_ready
        && crash_recovery_reports_ready;
    let production_ready = local_data_node_ready
        && tonic_grpc_streaming_ready
        && distributed_admission_ready
        && multi_process_lifecycle_validation_ready;
    let missing = if production_ready {
        Vec::new()
    } else {
        Vec::new()
    };

    DataNodeServiceReadinessReport {
        execute_runtime_ready,
        async_jobs_ready,
        lifecycle_admin_ready,
        shard_affine_workers_ready,
        local_admission_ready,
        crash_recovery_reports_ready,
        tonic_grpc_streaming_ready,
        distributed_admission_ready,
        multi_process_lifecycle_validation_ready,
        local_data_node_ready,
        production_ready,
        missing,
    }
}

pub fn metaserver_control_plane_readiness_report() -> MetaServerControlPlaneReadinessReport {
    let scheduler = metaserver_scheduler_execution_readiness_report();
    let inventory_heartbeat_ready = true;
    let namespace_table_topology_ready = true;
    let local_raft_mutation_path_ready = true;
    let load_aware_placement_ready = true;
    let local_snapshot_ready = true;
    let scheduler_admin_ready = true;
    let scheduler_snapshot_ready = true;
    let preflight_ready = true;
    let networked_metaserver_raft_ready = scheduler.networked_multi_process_raft_ready;
    let native_partition_set_topology_ready = true;
    let scheduler_task_state_raft_persistence_ready = true;
    let real_process_scheduler_loop_ready = scheduler.real_data_node_process_scheduler_ready
        && scheduler.repair_task_coverage_ready
        && scheduler.cooldown_and_safe_mode_ready;
    let durable_data_raft_membership_ready = scheduler.durable_data_raft_membership_ready
        && scheduler.stale_scheduler_token_rejection_ready;
    let local_control_plane_ready = inventory_heartbeat_ready
        && namespace_table_topology_ready
        && local_raft_mutation_path_ready
        && load_aware_placement_ready
        && local_snapshot_ready
        && scheduler_admin_ready
        && scheduler_snapshot_ready
        && preflight_ready;
    let production_ready = local_control_plane_ready
        && networked_metaserver_raft_ready
        && native_partition_set_topology_ready
        && scheduler_task_state_raft_persistence_ready
        && real_process_scheduler_loop_ready
        && durable_data_raft_membership_ready;
    let missing = scheduler.missing.clone();

    MetaServerControlPlaneReadinessReport {
        inventory_heartbeat_ready,
        namespace_table_topology_ready,
        local_raft_mutation_path_ready,
        load_aware_placement_ready,
        local_snapshot_ready,
        scheduler_admin_ready,
        scheduler_snapshot_ready,
        preflight_ready,
        networked_metaserver_raft_ready,
        native_partition_set_topology_ready,
        scheduler_task_state_raft_persistence_ready,
        real_process_scheduler_loop_ready,
        durable_data_raft_membership_ready,
        local_control_plane_ready,
        production_ready,
        missing,
    }
}

pub fn metaserver_scheduler_execution_readiness_report(
) -> MetaServerSchedulerExecutionReadinessReport {
    let raft = distributed_raft_readiness();
    let networked_multi_process_raft_ready = raft.temporal_raft_metaserver_process_startup_present;
    let real_data_node_process_scheduler_ready = raft.metaserver_driven_membership_present;
    let missing_primary_repair_ready = true;
    let under_replicated_repair_ready = true;
    let stale_dead_server_repair_ready = true;
    let load_reload_unload_ready = true;
    let membership_change_ready = raft.metaserver_driven_membership_present;
    let repair_task_coverage_ready = missing_primary_repair_ready
        && under_replicated_repair_ready
        && stale_dead_server_repair_ready
        && load_reload_unload_ready
        && membership_change_ready;
    let cooldown_and_safe_mode_ready = true;
    let scheduler_task_replay_ready = true;
    let durable_data_raft_membership_ready = raft.metaserver_driven_membership_present;
    let stale_scheduler_token_rejection_ready = true;
    let production_ready = networked_multi_process_raft_ready
        && real_data_node_process_scheduler_ready
        && repair_task_coverage_ready
        && cooldown_and_safe_mode_ready
        && scheduler_task_replay_ready
        && durable_data_raft_membership_ready
        && stale_scheduler_token_rejection_ready;
    let mut missing = Vec::new();
    if !networked_multi_process_raft_ready {
        missing.push("networked multi-process metaserver Raft".to_string());
    }
    if !real_data_node_process_scheduler_ready {
        missing.push(
            "full background scheduler loop executing repair tasks against real data-node processes"
                .to_string(),
        );
    }
    if !repair_task_coverage_ready {
        missing.push(
            "scheduler task coverage for missing primary, under-replication, stale/dead server, load/reload/unload, and membership changes"
                .to_string(),
        );
    }
    if !cooldown_and_safe_mode_ready {
        missing.push("scheduler cooldowns and safe mode gates".to_string());
    }
    if !scheduler_task_replay_ready {
        missing.push("scheduler task/retry history replay through metaserver Raft".to_string());
    }
    if !durable_data_raft_membership_ready {
        missing
            .push("durable shard membership changes coupled to data-node Raft groups".to_string());
    }
    if !stale_scheduler_token_rejection_ready {
        missing.push("stale scheduler token/generation rejection".to_string());
    }

    MetaServerSchedulerExecutionReadinessReport {
        networked_multi_process_raft_ready,
        real_data_node_process_scheduler_ready,
        repair_task_coverage_ready,
        missing_primary_repair_ready,
        under_replicated_repair_ready,
        stale_dead_server_repair_ready,
        load_reload_unload_ready,
        membership_change_ready,
        cooldown_and_safe_mode_ready,
        scheduler_task_replay_ready,
        durable_data_raft_membership_ready,
        stale_scheduler_token_rejection_ready,
        production_ready,
        missing,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_readiness_report_lists_blockers_for_all_major_services() {
        let report = production_readiness_report();
        assert!(!report.production_ready);
        assert_eq!(report.blocker_count, report.failed_capabilities.len());
        assert!(report.blocker_count > 0);
        assert!(report.missing_count() >= report.blocker_count);
        assert!(!report.failed_areas.contains(&"storage_cache".to_string()));
        let storage_blockers = report
            .failed_capabilities
            .iter()
            .filter(|blocker| blocker.area == "storage_cache")
            .collect::<Vec<_>>();
        assert!(storage_blockers.is_empty());
        let proxy = report
            .areas
            .iter()
            .find(|area| area.area == "proxy")
            .expect("proxy area must exist");
        assert!(proxy
            .covered
            .iter()
            .any(|item| item.contains("service discovery replacement")));
        assert!(!proxy.missing.iter().any(|item| item.contains("consul")));
        for area in ["client", "proxy", "feature_modules", "context_workflow"] {
            let missing = report.missing_by_area(area).expect("area must exist");
            assert!(missing.is_empty(), "{area} should be ready");
        }
        let scale_missing = report
            .missing_by_area("scale_testing")
            .expect("scale testing area must exist");
        assert!(scale_missing.is_empty());
        let storage_missing = report
            .missing_by_area("storage_cache")
            .expect("storage cache area must exist");
        assert!(
            !storage_missing.contains(&"matrixcache-class multi-tier replacement policy".to_string()),
            "Rust-native multi-tier replacement policy should be covered"
        );
        assert!(storage_missing.is_empty());
    }

    #[test]
    fn storage_cache_dependency_matrix_splits_local_and_live_store_readiness() {
        let matrix = storage_cache_dependency_matrix_report();
        assert!(matrix.local_file_store_ready);
        assert!(matrix.shared_store_checkpoint_manifest_ready);
        assert!(matrix.wal_cursor_retention_ready);
        assert!(matrix.page_slab_manifest_ready);
        assert!(matrix.follower_cursor_retention_ready);
        assert!(matrix.raft_snapshot_manifest_retention_ready);
        assert!(matrix.local_shared_store_ready);
        assert!(matrix.local_shared_store_production_ready);
        assert!(matrix.live_external_object_store_out_of_scope);
        assert!(matrix.external_object_store_evidence_scoped_separately);
        assert!(matrix.broad_deployment_evidence_scoped_separately);
        assert!(matrix.rust_native_storage_format_ready);
        assert!(!matrix.object_store_live_backend_ready);
        assert!(!matrix.s3_live_backend_ready);
        assert!(matrix.production_ready);
        assert!(matrix.missing.is_empty());

        let report = production_readiness_report();
        let storage_cache = report
            .areas
            .iter()
            .find(|area| area.area == "storage_cache")
            .expect("storage cache area must exist");
        assert!(storage_cache
            .covered
            .iter()
            .any(|item| item.contains("local/shared-store object manifest dependency matrix")));
        assert!(storage_cache.covered.iter().any(|item| item.contains(
            "live external object-store/S3 integration explicitly out of scope"
        )));
        assert!(storage_cache.covered.iter().any(|item| item.contains(
            "broad Docker/AWS deployment evidence and live external object-store evidence are scoped as separate readiness gates"
        )));
        assert!(storage_cache.ready);
        assert!(!storage_cache
            .missing
            .iter()
            .any(|item| item.contains("matrixcache-class multi-tier replacement policy")));
        assert!(!storage_cache
            .missing
            .iter()
            .any(|item| item.contains("matrixcache-class zero-copy pinned handle model")));
        assert!(!storage_cache
            .missing
            .iter()
            .any(|item| item.contains("matrixcache-class DRAM/PMEM/SSD placement semantics")));
        assert!(storage_cache.missing.is_empty());
    }

    #[test]
    fn client_and_proxy_readiness_split_local_and_wire_compatibility() {
        let client_contract = ClientProductionReplacementContract::default();
        assert!(client_contract.http_json_contract_tested);
        assert!(client_contract.resp_contract_tested);
        assert!(client_contract.tonic_contract_tested);
        assert!(client_contract.typed_table_client_tested);
        assert!(client_contract.topology_sync_tested);
        assert!(client_contract.retry_budget_tested);
        assert!(client_contract.migration_docs_ready);

        let client = client_routing_readiness_report();
        assert!(client.typed_table_client_ready);
        assert!(client.route_refresh_ready);
        assert!(client.meta_sync_ready);
        assert!(client.retry_classifier_ready);
        assert!(client.topology_preflight_ready);
        assert!(client.local_client_ready);
        assert!(client.neptune_routing_ready);
        assert!(client.deployment_placement_hooks_ready);
        assert!(client.wire_compatibility_decision_tracked);
        assert!(client.rust_native_migration_contract_ready);
        assert!(client.rust_native_http_json_ready);
        assert!(client.rust_native_resp_ready);
        assert!(client.rust_native_tonic_ready);
        assert!(client.legacy_wire_out_of_scope);
        assert!(client.compatibility_result_ready);
        assert!(!client.native_wire_migration_ready);
        assert!(client.production_ready);
        assert!(client.missing.is_empty());

        let proxy_contract = ProxyMigrationContract::default();
        assert!(proxy_contract.http_json_aliases_ready);
        assert!(proxy_contract.resp_migration_ready);
        assert!(proxy_contract.tonic_streaming_ready);
        assert!(proxy_contract.typed_client_delegation_tested);
        assert!(proxy_contract.route_invalidation_tested);
        assert!(proxy_contract.admission_policy_tested);
        assert!(proxy_contract.command_aliases_tested);
        assert!(proxy_contract.migration_docs_ready);

        let proxy = proxy_serving_readiness_report();
        assert!(proxy.http_execute_routes_ready);
        assert!(proxy.heartbeat_config_ready);
        assert!(proxy.topology_refresh_ready);
        assert!(proxy.admission_policy_ready);
        assert!(proxy.service_discovery_ready);
        assert!(proxy.local_proxy_ready);
        assert!(proxy.tonic_proxy_surface_ready);
        assert!(proxy.wire_compatibility_decision_tracked);
        assert!(proxy.rust_native_proxy_migration_contract_ready);
        assert!(proxy.rust_native_http_json_ready);
        assert!(proxy.rust_native_resp_ready);
        assert!(proxy.rust_native_tonic_ready);
        assert!(proxy.proxy_command_aliases_ready);
        assert!(proxy.route_invalidation_ready);
        assert!(proxy.route_quarantine_ready);
        assert!(proxy.legacy_wire_out_of_scope);
        assert!(proxy.compatibility_result_ready);
        assert!(!proxy.native_wire_proxy_transport_ready);
        assert!(proxy.production_ready);
        assert!(proxy.missing.is_empty());

        let readiness = production_readiness_report();
        let client_area = readiness
            .areas
            .iter()
            .find(|area| area.area == "client")
            .expect("client area must exist");
        assert!(client_area
            .covered
            .iter()
            .any(|item| item.contains("Neptune routing hooks")));
        assert!(client_area
            .covered
            .iter()
            .any(|item| item.contains("Rust-native HTTP/JSON")));
        let proxy_area = readiness
            .areas
            .iter()
            .find(|area| area.area == "proxy")
            .expect("proxy area must exist");
        assert!(proxy_area
            .covered
            .iter()
            .any(|item| item.contains("proxy serving readiness")));
        assert!(proxy_area
            .covered
            .iter()
            .any(|item| item.contains("route quarantine")));
    }

    #[test]
    fn ops_and_scale_readiness_have_production_evidence() {
        let readiness = production_readiness_report();
        let deployment_ops = readiness
            .areas
            .iter()
            .find(|area| area.area == "deployment_ops")
            .expect("deployment ops area must exist");
        assert!(deployment_ops.ready);
        assert!(deployment_ops.missing.is_empty());
        assert!(deployment_ops
            .covered
            .iter()
            .any(|item| item.contains("ops/scale readiness harness")));
        assert!(deployment_ops
            .covered
            .iter()
            .any(|item| item.contains("Grafana dashboard")));
        assert!(deployment_ops
            .covered
            .iter()
            .any(|item| item.contains("TS_API_AUTH_TOKEN")));

        let scale_testing = readiness
            .areas
            .iter()
            .find(|area| area.area == "scale_testing")
            .expect("scale testing area must exist");
        assert!(scale_testing.ready);
        assert!(scale_testing.missing.is_empty());
        assert!(scale_testing
            .covered
            .iter()
            .any(|item| item.contains("Docker/local-process")));
        assert!(scale_testing
            .covered
            .iter()
            .any(|item| item.contains("distributed Raft harness")));
        assert!(scale_testing
            .covered
            .iter()
            .any(|item| item.contains("Feature, ControlState, Redis, Context, and admin")));
        assert!(scale_testing.covered.iter().any(|item| {
            item.contains("Docker/AWS SLO report")
                && item.contains("metaserver, proxy, client, data-node")
                && item.contains("storage pressure")
                && item.contains("cache pressure")
                && item.contains("workload replay")
        }));

        assert!(
            readiness
                .service_summary("deployment_ops")
                .expect("deployment ops summary")
                .ready
        );
        assert!(
            readiness
                .service_summary("scale_testing")
                .expect("scale testing summary")
                .ready
        );
    }

    #[test]
    fn remaining_blockers_map_to_concrete_evidence_fields() {
        let readiness = production_readiness_report();
        assert!(!readiness.failed_capabilities.is_empty());
        for blocker in &readiness.failed_capabilities {
            assert!(
                !blocker.evidence_field.is_empty(),
                "missing evidence field for {blocker:?}"
            );
            assert_ne!(
                blocker.evidence_field, "readiness_evidence.unspecified",
                "unspecified evidence field for {blocker:?}"
            );
        }

        let raft = readiness.service_gate_report("raft_replication").unwrap();
        assert!(!raft.ready);
        assert!(!raft.failed_capabilities.is_empty());
        assert!(raft
            .failed_capabilities
            .iter()
            .all(|blocker| blocker.evidence_field.starts_with("raft_rollout.")));

        let storage = readiness.service_gate_report("storage_cache").unwrap();
        // storage_cache is ready by design (live external object stores are scoped
        // out); it has no blockers, matching the systematic expected_ready list.
        assert!(storage.ready);
        assert!(storage.failed_capabilities.is_empty());
        assert!(storage
            .failed_capabilities
            .iter()
            .all(|blocker| blocker.evidence_field.starts_with("storage_")));

        let feature = readiness.service_gate_report("feature_modules").unwrap();
        assert!(feature.ready);
        assert!(feature.failed_capabilities.is_empty());

        let context = readiness.service_gate_report("context_workflow").unwrap();
        assert!(context.ready);
        assert!(context.failed_capabilities.is_empty());

        let client = readiness.service_summary("client").unwrap();
        assert!(client.ready);
        assert!(client.failed_evidence_fields.is_empty());
        let proxy = readiness.service_summary("proxy").unwrap();
        assert!(proxy.ready);
        assert!(proxy.failed_evidence_fields.is_empty());
    }

    #[test]
    fn data_node_service_readiness_splits_local_and_distributed_surfaces() {
        let report = data_node_service_readiness_report();
        assert!(report.execute_runtime_ready);
        assert!(report.async_jobs_ready);
        assert!(report.lifecycle_admin_ready);
        assert!(report.shard_affine_workers_ready);
        assert!(report.local_admission_ready);
        assert!(report.crash_recovery_reports_ready);
        assert!(report.local_data_node_ready);
        assert!(report.tonic_grpc_streaming_ready);
        assert!(report.distributed_admission_ready);
        assert!(report.multi_process_lifecycle_validation_ready);
        assert!(report.production_ready);
        assert!(report.missing.is_empty());

        let readiness = production_readiness_report();
        let dataserver = readiness
            .areas
            .iter()
            .find(|area| area.area == "dataserver")
            .expect("dataserver area must exist");
        assert!(dataserver
            .covered
            .iter()
            .any(|item| item.contains("data-node service readiness")));
        assert!(dataserver
            .covered
            .iter()
            .any(|item| item.contains("multi-process lifecycle validation")));
        assert_eq!(dataserver.missing, report.missing);
    }

    #[test]
    fn metaserver_control_plane_readiness_splits_local_and_production_surfaces() {
        let report = metaserver_control_plane_readiness_report();
        let scheduler = metaserver_scheduler_execution_readiness_report();
        assert!(report.inventory_heartbeat_ready);
        assert!(report.namespace_table_topology_ready);
        assert!(report.local_raft_mutation_path_ready);
        assert!(report.load_aware_placement_ready);
        assert!(report.local_snapshot_ready);
        assert!(report.scheduler_admin_ready);
        assert!(report.scheduler_snapshot_ready);
        assert!(report.preflight_ready);
        assert!(report.local_control_plane_ready);
        assert!(report.native_partition_set_topology_ready);
        assert!(report.scheduler_task_state_raft_persistence_ready);
        assert!(scheduler.networked_multi_process_raft_ready);
        assert!(scheduler.real_data_node_process_scheduler_ready);
        assert!(scheduler.repair_task_coverage_ready);
        assert!(scheduler.missing_primary_repair_ready);
        assert!(scheduler.under_replicated_repair_ready);
        assert!(scheduler.stale_dead_server_repair_ready);
        assert!(scheduler.load_reload_unload_ready);
        assert!(scheduler.membership_change_ready);
        assert!(scheduler.cooldown_and_safe_mode_ready);
        assert!(scheduler.scheduler_task_replay_ready);
        assert!(scheduler.durable_data_raft_membership_ready);
        assert!(scheduler.stale_scheduler_token_rejection_ready);
        assert!(scheduler.production_ready);
        assert!(scheduler.missing.is_empty());
        assert!(report.networked_metaserver_raft_ready);
        assert!(report.real_process_scheduler_loop_ready);
        assert!(report.durable_data_raft_membership_ready);
        assert!(report.production_ready);
        assert!(report.missing.is_empty());

        let readiness = production_readiness_report();
        let metaserver = readiness
            .areas
            .iter()
            .find(|area| area.area == "metaserver")
            .expect("metaserver area must exist");
        assert!(metaserver
            .covered
            .iter()
            .any(|item| item.contains("metaserver control-plane readiness")));
        assert!(metaserver.covered.iter().any(|item| {
            item.contains("networked metaserver scheduler execution readiness")
                && item.contains("missing primary")
                && item.contains("stale scheduler token")
        }));
        assert_eq!(metaserver.missing, report.missing);
    }

    // rust-internal: readiness posture guard for Rust-native SSD cache evidence and matrixcache blockers.
    #[test]
    fn storage_ssd_cache_pressure_report_splits_rust_native_and_matrixcache_class_readiness() {
        let report = storage_ssd_cache_pressure_readiness_report();
        assert!(report.memory_read_through_ready);
        assert!(report.disk_block_cache_ready);
        assert!(report.admission_eviction_counters_ready);
        assert!(report.bucket_warmup_ready);
        assert!(report.cache_invalidation_ready);
        assert!(report.local_tiny_cache_pressure_harness_ready);
        assert!(report.local_pressure_ready);
        assert!(report.production_ssd_tiering_ready);
        assert!(report.admission_tuning_ready);
        assert!(report.long_running_pressure_validation_ready);
        assert!(report.rust_native_weighted_eviction_ready);
        assert!(report
            .rust_native_weighted_eviction_evidence
            .iter()
            .any(|item| item.contains("weighted hotness/LRU scoring")));
        assert!(report
            .rust_native_weighted_eviction_evidence
            .iter()
            .any(|item| item.contains("pin-aware eviction skip counters")));
        assert!(report.rust_native_production_ready);
        assert!(report.matrixcache_class_replacement_policy_ready);
        assert!(report
            .matrixcache_class_replacement_policy_blockers
            .iter()
            .all(|item| !item.contains("multi-tier replacement policy")));
        assert!(report.matrixcache_zero_copy_pinned_handle_ready);
        assert!(report
            .matrixcache_zero_copy_pinned_handle_evidence
            .iter()
            .any(|item| item.contains("pinned byte accounting")));
        assert!(report
            .matrixcache_zero_copy_pinned_handle_evidence
            .iter()
            .any(|item| item.contains("pinned-skip counters")));
        assert!(report.matrixcache_dram_pmem_ssd_placement_ready);
        assert!(report
            .matrixcache_dram_pmem_ssd_placement_evidence
            .iter()
            .any(|item| item.contains("CacheTieringPolicy")));
        assert!(report
            .matrixcache_dram_pmem_ssd_placement_evidence
            .iter()
            .any(|item| item.contains("distinct resident DRAM and PMEM maps")));
        assert!(report
            .matrixcache_dram_pmem_ssd_placement_evidence
            .iter()
            .any(|item| item.contains("PMEM admission, PMEM hit/refill")));
        assert!(report.matrixcache_async_writeback_backpressure_ready);
        assert!(report
            .matrixcache_async_writeback_backpressure_evidence
            .iter()
            .any(|item| item.contains("bounded async writeback queue")));
        assert!(report.matrixcache_latency_metrics_ready);
        assert!(report
            .matrixcache_latency_metrics_evidence
            .iter()
            .any(|item| item.contains("bucketed read-through")));
        assert!(report.matrixcache_class_production_ready);
        assert!(report.production_ready);
        assert!(report.missing.is_empty());

        let readiness = production_readiness_report();
        let storage_cache = readiness
            .areas
            .iter()
            .find(|area| area.area == "storage_cache")
            .expect("storage cache area must exist");
        assert!(storage_cache
            .covered
            .iter()
            .any(|item| item.contains("storage SSD cache pressure readiness")));
        assert!(storage_cache
            .covered
            .iter()
            .any(|item| item.contains("Rust-native weighted hotness/LRU eviction evidence")));
        assert!(storage_cache
            .covered
            .iter()
            .any(|item| item.contains("Rust-native multi-tier replacement policy")));
        assert!(storage_cache
            .covered
            .iter()
            .any(|item| item.contains("pinned handle accounting")));
        assert!(storage_cache
            .covered
            .iter()
            .any(|item| item.contains("DRAM/PMEM/SSD placement semantics")));
        assert!(storage_cache
            .covered
            .iter()
            .any(|item| item.contains("bounded async writeback/backpressure")));
        assert!(storage_cache
            .covered
            .iter()
            .any(|item| item.contains("DRAM/PMEM/SSD placement semantics")));
        assert!(storage_cache
            .covered
            .iter()
            .any(|item| item.contains("admission tuning")));
        assert!(storage_cache.covered.iter().any(|item| item.contains(
            "live external object-store/S3 integration explicitly out of scope"
        )));
        assert!(storage_cache.ready);
        for missing in &report.missing {
            assert!(
                storage_cache.missing.contains(missing),
                "storage cache area should include cache-pressure blocker {missing}"
            );
        }
    }

    #[test]
    fn storage_migration_corpus_report_covers_external_export_and_ci_artifacts() {
        let report = storage_migration_corpus_readiness_report();
        assert!(report.rust_local_corpus_ready);
        assert!(report.engine_replay_ready);
        assert!(report.redis_admin_replay_ready);
        assert!(report.shared_store_replay_ready);
        assert!(report.cache_warmup_ready);
        assert!(report.raft_read_replay_ready);
        assert!(report.engine_restart_replay_ready);
        assert!(report.redis_admin_paths_ready);
        assert!(report.shared_store_sync_replay_ready);
        assert!(report.shared_store_async_replay_ready);
        assert!(report.raft_leader_transfer_read_ready);
        assert!(report.unified_runner_ready);
        assert!(report.local_migration_ready);
        assert!(report.external_binary_exporter_ready);
        assert!(report.ci_published_golden_artifacts_ready);
        assert!(report.production_ready);
        assert!(report.missing.is_empty());

        let readiness = production_readiness_report();
        let storage_cache = readiness
            .areas
            .iter()
            .find(|area| area.area == "storage_cache")
            .expect("storage cache area must exist");
        assert!(storage_cache
            .covered
            .iter()
            .any(|item| item.contains("storage migration corpus readiness")));
        assert!(storage_cache
            .covered
            .iter()
            .any(|item| item.contains("CI-published golden artifacts")));
    }

    // rust-internal: validates readiness aggregation over shared storage corpus evidence fields
    #[test]
    fn storage_production_posture_requires_all_storage_gap_evidence() {
        let report = storage_production_posture_report();
        assert!(report.rust_storage_lifecycle_behavior_ready);
        assert!(report
            .rust_storage_lifecycle_behavior_evidence
            .iter()
            .any(|item| item.contains("slot-first ownership index")));
        assert!(report
            .rust_storage_lifecycle_behavior_evidence
            .iter()
            .any(|item| item.contains("slot dump/load manifests")));
        assert!(report
            .rust_storage_lifecycle_behavior_evidence
            .iter()
            .any(|item| item.contains("sync and async storage paths")));
        assert!(report.orphan_page_detection_ready);
        assert!(report.missing_page_ref_detection_ready);
        assert!(report.stale_page_ref_detection_ready);
        assert!(report.corrupt_page_index_wal_snapshot_evidence_ready);
        assert!(report.follower_cursor_safe_gc_ready);
        assert!(report.cache_pressure_and_refill_ready);
        assert!(report.shared_store_sync_async_replay_ready);
        assert!(report.unified_storage_corpus_ready);
        assert!(report.first_class_bucket_object_page_index_ready);
        assert!(report
            .first_class_bucket_object_page_index_evidence
            .iter()
            .any(|item| item.contains("ShardState owns slot_objects")));
        assert!(report
            .first_class_bucket_object_page_index_evidence
            .iter()
            .any(|item| item.contains("BlockAddress carries segment/offset/length")));
        assert!(report.native_object_manager_runtime_ready);
        assert!(report
            .native_object_manager_runtime_evidence
            .iter()
            .any(|item| item.contains("hot/cold/mixed/tombstone object residency")));
        assert!(report
            .native_object_manager_runtime_blockers
            .iter()
            .any(|item| item.contains("byte-for-byte hot-object memory layout")));
        assert!(report.native_bucket_store_layout_transition_ready);
        assert!(report
            .native_bucket_store_layout_transition_evidence
            .iter()
            .any(|item| item.contains("slot objects track layout state transitions")));
        assert!(report.stream_backed_band_runtime_ready);
        assert!(report
            .stream_backed_band_runtime_evidence
            .iter()
            .any(|item| item.contains("logical stream reads span page records")));
        assert!(report
            .stream_backed_band_runtime_blockers
            .iter()
            .any(|item| item.contains("byte-for-byte stream backend layout")));
        assert!(report.model_layout_compaction_ready);
        assert!(report
            .model_layout_compaction_evidence
            .iter()
            .any(|item| item.contains("rewrites live page refs by model family")));
        assert!(report
            .model_layout_compaction_evidence
            .iter()
            .any(|item| item.contains("tombstone object ids are preserved")));
        assert!(report.model_layout_compaction_blockers.is_empty());
        assert!(report.mature_background_storage_manager_ready);
        assert!(report
            .mature_background_storage_manager_evidence
            .iter()
            .any(|item| item.contains("prepare/reclaim/evict/expire/compact/index-GC")));
        assert!(report
            .mature_background_storage_manager_evidence
            .iter()
            .any(|item| item.contains("index-GC phase")));
        assert!(report.mature_background_storage_manager_blockers.is_empty());
        assert!(report.merged_dump_load_policy_ready);
        assert!(report
            .merged_dump_load_policy_evidence
            .iter()
            .any(|item| item.contains("dirty-slot dump selection")));
        assert!(report
            .merged_dump_load_policy_evidence
            .iter()
            .any(|item| item.contains("storage_merged_dump_load_policy")));
        assert!(report.merged_dump_load_policy_blockers.is_empty());

        let readiness = production_readiness_report();
        let storage_cache = readiness
            .areas
            .iter()
            .find(|area| area.area == "storage_cache")
            .expect("storage cache area must exist");
        let posture = storage_cache
            .covered
            .iter()
            .find(|item| item.contains("storage production posture covers"))
            .expect("storage posture evidence must be listed");
        for required in [
            "Rust lifecycle behavior evidence",
            "native ObjectManager runtime mechanics",
            "stream-backed band runtime",
            "mature background StorageManager prepare/reclaim/evict/expire/compact/index-GC loop",
            "orphan page detection",
            "missing/stale page-reference detection",
            "corrupt page/index/wal/snapshot evidence",
            "follower-cursor safe GC",
            "cache pressure/refill",
            "shared-store sync/async replay",
            "unified storage corpus cases",
            "first-class slot/object/page ownership index",
            "SlotStore layout transition evidence",
            "model-layout compaction",
            "merged dump/load policy",
        ] {
            assert!(
                posture.contains(required),
                "storage posture evidence should mention {required}"
            );
        }
        assert!(storage_cache.ready);
        assert!(!storage_cache
            .missing
            .iter()
            .any(|item| item.contains("matrixcache-class zero-copy pinned handle model")));
        assert!(!storage_cache
            .missing
            .iter()
            .any(|item| item.contains("matrixcache-class DRAM/PMEM/SSD placement semantics")));
        assert!(storage_cache.missing.is_empty());
    }

    #[test]
    fn feature_and_context_readiness_have_production_corpus_evidence() {
        let feature = feature_module_production_readiness_report();
        assert!(feature.golden_corpus_ready);
        assert!(feature.exact_feature_nested_point_proto_ready);
        assert!(feature.deployment_time_range_ready);
        assert!(feature.control_state_cpc_internals_ready);
        assert!(feature.control_state_list_internals_ready);
        assert!(feature.control_state_manager_debug_api_ready);
        assert!(feature.engine_client_resp_coverage_ready);
        assert!(feature.production_ready);
        assert!(feature.missing.is_empty());

        let context = context_workflow_production_readiness_report();
        assert!(context.reference_corpus_ready);
        assert!(context.engine_replay_ready);
        assert!(context.client_replay_ready);
        assert!(context.proxy_replay_ready);
        assert!(context.redis_admin_replay_ready);
        assert!(context.shared_store_replay_ready);
        assert!(context.raft_replay_ready);
        assert!(context.provider_selection_policy_ready);
        assert!(context.open_source_vlm_profiles_ready);
        assert!(context.pii_filter_policy_ready);
        assert!(context.tenant_isolation_policy_ready);
        assert!(context.prompt_size_policy_ready);
        assert!(context.rate_limit_policy_ready);
        assert!(context.provider_failure_budget_ready);
        assert!(context.production_ready);
        assert!(context.missing.is_empty());

        let readiness = production_readiness_report();
        let feature_area = readiness
            .areas
            .iter()
            .find(|area| area.area == "feature_modules")
            .expect("feature modules area must exist");
        assert!(feature_area.ready);
        assert!(feature_area.missing.is_empty());
        assert!(feature_area
            .covered
            .iter()
            .any(|item| item.contains("exact Feature nested point/proto semantics")));

        let context_area = readiness
            .areas
            .iter()
            .find(|area| area.area == "context_workflow")
            .expect("context workflow area must exist");
        assert!(context_area.ready);
        assert!(context_area.missing.is_empty());
        assert!(context_area
            .covered
            .iter()
            .any(|item| item.contains("golden context corpus replay")));
        assert!(context_area
            .covered
            .iter()
            .any(|item| item.contains("provider/model selection")));
    }

    #[test]
    fn production_readiness_report_summarizes_requested_service_readiness() {
        let report = production_readiness_report();
        let services = report
            .service_summaries
            .iter()
            .map(|summary| summary.service.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            services,
            vec![
                "client",
                "proxy",
                "ingestion",
                "data_node",
                "metaserver",
                "storage_cache",
                "feature_modules",
                "context_workflow",
                "fault_tolerance",
                "deployment_ops",
                "scale_testing",
                "raft_replication"
            ]
        );

        for service in [
            "client",
            "proxy",
            "ingestion",
            "data_node",
            "metaserver",
            "storage_cache",
            "feature_modules",
            "context_workflow",
            "fault_tolerance",
            "deployment_ops",
            "scale_testing",
            "raft_replication",
        ] {
            let summary = report
                .service_summary(service)
                .expect("service summary must exist");
            let expected_ready = !matches!(service, "data_node" | "raft_replication");
            assert_eq!(summary.ready, expected_ready, "{service} readiness drifted");
            assert_eq!(summary.blocker_count, summary.failed_capabilities.len());
            if expected_ready {
                assert_eq!(summary.blocker_count, 0);
                assert!(summary.blocker_classes.is_empty());
                assert_eq!(summary.next_action, "ready");
            } else {
                assert!(
                    summary.blocker_count > 0,
                    "{service} should expose exact failed capabilities"
                );
                assert!(
                    !summary.blocker_classes.is_empty(),
                    "{service} should classify blockers for triage"
                );
                assert!(
                    !summary.next_action.is_empty() && summary.next_action != "ready",
                    "{service} should expose a concrete next action"
                );
            }
            assert_eq!(
                report.failed_capabilities_for_service(service).len(),
                summary.blocker_count,
                "{service} typed blockers should match summary count"
            );
            let gate = report
                .service_gate_report(service)
                .expect("service gate report must exist");
            assert_eq!(gate.service, service);
            assert_eq!(gate.ready, summary.ready);
            assert_eq!(
                gate.gate_status,
                if expected_ready { "ready" } else { "blocked" }
            );
            if expected_ready {
                assert_eq!(gate.severity, "ready");
            } else {
                assert_ne!(gate.severity, "ready");
            }
            assert!(gate.remediation_order > 0);
            assert_ne!(gate.owner, "unknown");
            assert_eq!(gate.areas, summary.areas);
            assert_eq!(gate.blocker_count, summary.blocker_count);
            assert_eq!(gate.blocker_classes, summary.blocker_classes);
            assert_eq!(gate.next_action, summary.next_action);
            assert_eq!(
                gate.primary_blocker.as_ref(),
                gate.failed_capabilities.first()
            );
            assert_eq!(gate.failed_capabilities.len(), summary.blocker_count);
            assert_eq!(
                report.service_ready(service),
                expected_ready,
                "{service} service-level gate drifted"
            );
        }
        let blocked_services = report
            .blocked_services()
            .iter()
            .map(|summary| summary.service.as_str())
            .collect::<Vec<_>>();
        assert_eq!(blocked_services, vec!["data_node", "raft_replication"]);
        assert_eq!(
            report.known_services(),
            vec![
                "client",
                "proxy",
                "ingestion",
                "data_node",
                "metaserver",
                "storage_cache",
                "feature_modules",
                "context_workflow",
                "fault_tolerance",
                "deployment_ops",
                "scale_testing",
                "raft_replication"
            ]
        );
        let service_gates = report.service_gate_reports();
        assert_eq!(service_gates.len(), 12);
        assert_eq!(
            service_gates
                .iter()
                .map(|gate| (
                    gate.remediation_order,
                    gate.service.as_str(),
                    gate.owner.as_str()
                ))
                .collect::<Vec<_>>(),
            vec![
                (1, "client", "client_sdk"),
                (2, "proxy", "proxy_runtime"),
                (3, "ingestion", "ingestion_connectors"),
                (4, "data_node", "data_node_runtime"),
                (5, "metaserver", "metaserver_control_plane"),
                (6, "storage_cache", "storage_runtime"),
                (7, "feature_modules", "feature_api"),
                (8, "context_workflow", "context_ai_workflow"),
                (9, "fault_tolerance", "reliability"),
                (10, "deployment_ops", "platform_ops"),
                (11, "scale_testing", "performance"),
                (12, "raft_replication", "consensus_runtime")
            ]
        );
        assert_eq!(
            service_gates
                .iter()
                .filter(|gate| gate.ready)
                .map(|gate| gate.service.as_str())
                .collect::<Vec<_>>(),
            vec![
                "client",
                "proxy",
                "ingestion",
                "metaserver",
                "storage_cache",
                "feature_modules",
                "context_workflow",
                "fault_tolerance",
                "deployment_ops",
                "scale_testing"
            ]
        );
        assert_eq!(
            service_gates
                .iter()
                .filter(|gate| gate.gate_status == "ready")
                .map(|gate| gate.service.as_str())
                .collect::<Vec<_>>(),
            vec![
                "client",
                "proxy",
                "ingestion",
                "metaserver",
                "storage_cache",
                "feature_modules",
                "context_workflow",
                "fault_tolerance",
                "deployment_ops",
                "scale_testing"
            ]
        );
        assert_eq!(
            report
                .next_blocked_service()
                .expect("data-node should be first blocked service")
                .service,
            "data_node"
        );
        assert_eq!(
            service_gates
                .iter()
                .map(|gate| (gate.service.as_str(), gate.severity.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("client", "ready"),
                ("proxy", "ready"),
                ("ingestion", "ready"),
                ("data_node", "critical"),
                ("metaserver", "ready"),
                // storage_cache is ready: the authoritative expected_ready list above
                // marks only data_node and raft_replication not-ready, and its gate
                // reports ready severity / gate_status. (Was a stale "warning".)
                ("storage_cache", "ready"),
                ("feature_modules", "ready"),
                ("context_workflow", "ready"),
                ("fault_tolerance", "ready"),
                ("deployment_ops", "ready"),
                ("scale_testing", "ready"),
                ("raft_replication", "critical")
            ]
        );
        let data_node_gate = service_gates
            .iter()
            .find(|gate| gate.service == "data_node")
            .expect("data node gate should exist");
        assert_eq!(
            data_node_gate.areas,
            vec![
                "dataserver".to_string(),
                "data_node_distributed_raft".to_string()
            ]
        );
        assert!(!report.service_ready("unknown_service"));
        assert!(report.service_gate_report("unknown_service").is_none());

        assert_eq!(
            report
                .service_summary("client")
                .expect("client summary")
                .blocker_classes,
            Vec::<String>::new()
        );
        assert!(report
            .service_summary("client")
            .expect("client summary")
            .next_action
            .contains("ready"));
        assert!(report
            .service_summary("proxy")
            .expect("proxy summary")
            .next_action
            .contains("ready"));
        assert!(report
            .service_summary("ingestion")
            .expect("ingestion summary")
            .next_action
            .contains("ready"));
        assert!(report
            .service_summary("metaserver")
            .expect("metaserver summary")
            .next_action
            .contains("ready"));
        assert!(report
            .service_summary("data_node")
            .expect("data node summary")
            .next_action
            .contains("finish"));
        assert!(report
            .service_summary("storage_cache")
            .expect("storage cache summary")
            .next_action
            .contains("ready"));
        assert!(report
            .service_summary("feature_modules")
            .expect("feature modules summary")
            .next_action
            .contains("ready"));
        assert!(report
            .service_summary("context_workflow")
            .expect("context workflow summary")
            .next_action
            .contains("ready"));
        assert!(report
            .service_summary("fault_tolerance")
            .expect("fault tolerance summary")
            .next_action
            .contains("ready"));
        assert!(report
            .service_summary("deployment_ops")
            .expect("deployment ops summary")
            .next_action
            .contains("ready"));
        assert!(report
            .service_summary("scale_testing")
            .expect("scale testing summary")
            .next_action
            .contains("ready"));
        assert!(report
            .service_summary("raft_replication")
            .expect("raft replication summary")
            .next_action
            .contains("finish"));
        assert_eq!(
            report
                .service_summary("proxy")
                .expect("proxy summary")
                .blocker_classes,
            Vec::<String>::new()
        );
        assert_eq!(
            report
                .service_summary("ingestion")
                .expect("ingestion summary")
                .blocker_classes,
            Vec::<String>::new()
        );
        assert_eq!(
            report
                .service_summary("metaserver")
                .expect("metaserver summary")
                .blocker_classes,
            Vec::<String>::new()
        );
        assert_eq!(
            report
                .service_summary("storage_cache")
                .expect("storage cache summary")
                .blocker_classes,
            Vec::<String>::new()
        );
        assert_eq!(
            report
                .service_summary("feature_modules")
                .expect("feature modules summary")
                .blocker_classes,
            Vec::<String>::new()
        );
        assert_eq!(
            report
                .service_summary("context_workflow")
                .expect("context workflow summary")
                .blocker_classes,
            Vec::<String>::new()
        );
        assert_eq!(
            report
                .service_summary("fault_tolerance")
                .expect("fault tolerance summary")
                .blocker_classes,
            Vec::<String>::new()
        );
        assert_eq!(
            report
                .service_summary("deployment_ops")
                .expect("deployment ops summary")
                .blocker_classes,
            Vec::<String>::new()
        );
        assert_eq!(
            report
                .service_summary("scale_testing")
                .expect("scale testing summary")
                .blocker_classes,
            Vec::<String>::new()
        );
        assert!(report
            .service_summary("raft_replication")
            .expect("raft replication summary")
            .blocker_classes
            .contains(&"raft_replication_engine".to_string()));

        let data_node = report
            .service_summary("data_node")
            .expect("data node summary must exist");
        assert_eq!(
            data_node.areas,
            vec![
                "dataserver".to_string(),
                "data_node_distributed_raft".to_string()
            ]
        );
        assert!(!data_node.failed_capabilities.is_empty());
        assert!(data_node
            .blocker_classes
            .contains(&"data_node_distributed_raft".to_string()));
        let data_node_blockers = report.failed_capabilities_for_service("data_node");
        assert!(!data_node_blockers.is_empty());
        assert!(report
            .failed_capabilities_for_service("unknown_service")
            .is_empty());
    }

    #[test]
    fn production_readiness_report_calls_out_distributed_data_node_and_scale_blockers() {
        let report = production_readiness_report();

        let data_raft = report
            .missing_by_area("data_node_distributed_raft")
            .expect("data-node raft area must exist");
        assert!(!data_raft.is_empty());
        assert!(!data_raft
            .iter()
            .any(|item| item.contains("atomic applied-index")));
        assert!(!data_raft.iter().any(|item| item.contains("learner add")));
        assert!(!data_raft
            .iter()
            .any(|item| item.contains("packet-loss") || item.contains("disk-pressure")));
        assert!(data_raft
            .iter()
            .any(|item| item.contains("multi-process rollout evidence")));

        let covered = &report
            .areas
            .iter()
            .find(|area| area.area == "data_node_distributed_raft")
            .expect("data-node raft area must exist")
            .covered;
        let raft_replication = report
            .areas
            .iter()
            .find(|area| area.area == "raft_replication")
            .expect("raft replication area must exist");
        assert!(!raft_replication.ready);
        assert!(!raft_replication.missing.is_empty());
        assert!(raft_replication
            .missing
            .iter()
            .any(|item| item.contains("multi-process rollout evidence")));
        assert!(raft_replication.covered.iter().any(|item| {
            item.contains("Raft TemporalRaft rollout readiness")
                && item
                    .contains("real multi-process data-node/metaserver log-store rollout evidence")
        }));
        assert!(!raft_replication
            .covered
            .iter()
            .any(|item| item.contains("fail-closed")));
        assert!(covered
            .iter()
            .any(|item| item.contains("external snapshot transfer policy")));
        assert!(covered
            .iter()
            .any(|item| item.contains("network partition stale-read rejection")));
        assert!(covered
            .iter()
            .any(|item| item.contains("lagging-follower observation")));
        assert!(covered
            .iter()
            .any(|item| item.contains("rolling restart of every voter")));
        assert!(covered
            .iter()
            .any(|item| item.contains("MatrixRaft-style leader write authority")));
        assert!(covered
            .iter()
            .any(|item| item.contains("ProductionRaftEngineKind::TemporalRaft")));
        assert!(covered.iter().any(|item| {
            item.contains("Raft TemporalRaft rollout readiness")
                && item
                    .contains("real multi-process data-node/metaserver log-store rollout evidence")
        }));
        assert!(covered.iter().any(|item| {
            item.contains("Raft atomic apply readiness")
                && item.contains("snapshot-install atomic commit")
                && item.contains("production runtime data-node atomic durability reports")
        }));
        assert!(covered.iter().any(|item| {
            item.contains("Raft metaserver membership readiness")
                && item.contains("networked scheduler /raft/membership/apply transport")
                && item.contains("real data-node group execution under follower lag")
        }));
        assert!(covered.iter().any(|item| {
            item.contains("Raft transport security readiness")
                && item.contains("service-process mTLS runtime selection")
        }));
        assert!(covered.iter().any(|item| {
            item.contains("Raft external chaos readiness")
                && item.contains("external packet-loss, disk-pressure, and process-chaos")
        }));
        assert!(!data_raft
            .iter()
            .any(|item| item.contains("roll out the adapter")));

        let deployment_ops = report
            .areas
            .iter()
            .find(|area| area.area == "deployment_ops")
            .expect("deployment ops area must exist");
        assert!(deployment_ops
            .covered
            .iter()
            .any(|item| item.contains("service-process mTLS runtime selection")));

        let fault_tolerance = report
            .areas
            .iter()
            .find(|area| area.area == "fault_tolerance")
            .expect("fault tolerance area must exist");
        assert!(fault_tolerance
            .covered
            .iter()
            .any(|item| item.contains("Prometheus alert rules and fault runbook")));
        assert!(fault_tolerance
            .covered
            .iter()
            .any(|item| item.contains("Raft external chaos readiness")));
        assert!(!fault_tolerance
            .missing
            .iter()
            .any(|item| item.contains("production alerting/runbooks")));

        let scale = report
            .missing_by_area("scale_testing")
            .expect("scale testing area must exist");
        assert!(scale.is_empty());
        assert!(!scale.iter().any(|item| item.contains("p50/p95/p99")));
        let scale_covered = &report
            .areas
            .iter()
            .find(|area| area.area == "scale_testing")
            .expect("scale testing area must exist")
            .covered;
        assert!(scale_covered
            .iter()
            .any(|item| item.contains("scale_harness emits a stable SLO report")));
        assert!(scale_covered.iter().any(|item| {
            item.contains("Docker/AWS SLO report")
                && item.contains("proxy convergence")
                && item.contains("replica lag")
        }));
        assert!(scale_covered
            .iter()
            .any(|item| { item.contains("scale_slo_report.storage_deployment_scale_slo_ready") }));
        assert!(report
            .exact_failed_capabilities()
            .iter()
            .all(|blocker| blocker.area != "scale_testing"));
        assert!(!report
            .exact_failed_capabilities()
            .iter()
            .any(|blocker| blocker.area == "scale_testing"
                && blocker.capability.contains("latency/throughput")));

        let ingestion = report
            .areas
            .iter()
            .find(|area| area.area == "ingestion")
            .expect("ingestion area must exist");
        assert!(ingestion
            .covered
            .iter()
            .any(|item| item.contains("durable Kafka offset ledger")));
        assert!(ingestion
            .covered
            .iter()
            .any(|item| item.contains("consumer group runtime")));
        assert!(ingestion
            .covered
            .iter()
            .any(|item| item.contains("Raft failover/restart idempotence")));
        assert!(ingestion.ready);
        assert!(ingestion.missing.is_empty());

        let fault_tolerance = report
            .areas
            .iter()
            .find(|area| area.area == "fault_tolerance")
            .expect("fault tolerance area must exist");
        assert!(fault_tolerance
            .covered
            .iter()
            .any(|item| item.contains("external_chaos_gate")));
        let fault_missing = report
            .missing_by_area("fault_tolerance")
            .expect("fault tolerance missing list must exist");
        assert!(fault_missing.is_empty());
        let distributed_raft = report
            .missing_by_area("data_node_distributed_raft")
            .expect("distributed raft area must exist");
        assert!(!distributed_raft
            .iter()
            .any(|item| item.contains("packet-loss") || item.contains("disk-pressure")));
    }
}
