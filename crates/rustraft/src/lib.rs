#![forbid(unsafe_code)]
//! RustRaft is the TemporalStore-owned Raft contract and readiness library.
//!
//! The crate intentionally focuses on portable consensus-facing contracts:
//! request/response types, storage and transport traits, safety decisions,
//! metrics names, and fail-closed production readiness reports. It does not run
//! the TemporalStore data-node or metaserver by itself. Those runtimes consume
//! this crate and attach live evidence for pipeline, WAL, snapshot, membership,
//! failover, and process-rollout behavior.
//!
//! Typical integration flow:
//!
//! 1. Build a [`RustRaftReadinessSnapshot`] from the serving runtime.
//! 2. Call [`rustraft_parity_report`] for semantic contract readiness.
//! 3. Attach live runtime evidence to [`RustRaftProductionReadinessInput`].
//! 4. Call [`rustraft_production_readiness_report`] and block production claims
//!    unless the report is ready.
//!
//! The public API is OpenRaft-free by design. Compatibility with existing
//! TemporalStore deployment semantics is expressed through RustRaft-owned types
//! and tests instead of upstream-specific type aliases.

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftRequirementCategory {
    Safety,
    Durability,
    Observability,
    Transport,
    Membership,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftSemanticRequirement {
    pub id: String,
    pub category: RustRaftRequirementCategory,
    pub readiness_field: String,
    pub required_for_production: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftParityContract {
    pub library_name: String,
    pub consensus_backend_boundary: String,
    pub openraft_dependency_removed: bool,
    pub requirements: Vec<RustRaftSemanticRequirement>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftProductionStatus {
    ProductionReady,
    Blocked,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftParityReport {
    pub contract: RustRaftParityContract,
    pub ready: bool,
    pub production_status: RustRaftProductionStatus,
    pub satisfied: Vec<String>,
    pub missing: Vec<String>,
    pub production_blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftProductionReadinessInput {
    pub readiness: RustRaftReadinessSnapshot,
    #[serde(default)]
    pub peer_pipeline: Option<RustRaftPipelineEvidence>,
    #[serde(default)]
    pub snapshot_lifecycle: Option<RustRaftSnapshotLifecycleEvidence>,
    #[serde(default)]
    pub wal_lifecycle: Option<RustRaftWalLifecycleEvidence>,
    #[serde(default)]
    pub data_node_rollout: Option<RustRaftDataNodeProcessRolloutReport>,
    #[serde(default)]
    pub metaserver_rollout: Option<RustRaftMetaProcessRolloutReport>,
    #[serde(default)]
    pub membership_transitions: Vec<RustRaftMembershipTransitionEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftProductionReadinessReport {
    pub parity: RustRaftParityReport,
    pub public_api: RustRaftPublicApiContract,
    pub ready: bool,
    pub production_status: RustRaftProductionStatus,
    pub satisfied: Vec<String>,
    pub missing: Vec<String>,
    pub production_blockers: Vec<String>,
    pub recommended_next_actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadinessEvidence {
    pub requirement_id: String,
    pub readiness_field: String,
    pub present: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadinessSnapshot {
    pub rustraft_leader_write_authority_present: bool,
    pub rustraft_operator_observability_present: bool,
    pub rustraft_rpc_transport_contract_present: bool,
    pub rustraft_log_retention_snapshot_trigger_present: bool,
    pub rustraft_apply_snapshot_fence_present: bool,
    pub raft_storage_apply_fence_present: bool,
    pub rustraft_snapshot_floor_log_matching_present: bool,
    pub rustraft_snapshot_tail_catchup_present: bool,
    pub rustraft_compacted_entry_rejection_present: bool,
    pub rustraft_metaserver_snapshot_floor_election_present: bool,
    pub learner_catchup_promotion_present: bool,
    pub metaserver_membership_workflow_present: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftPublicApiContract {
    pub storage_trait: String,
    pub transport_trait: String,
    pub rpc_messages: Vec<String>,
    pub safety_helpers: Vec<String>,
    pub metrics: RustRaftMetricNames,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftMetricNames {
    pub ready: String,
    pub append_latency_ms: String,
    pub vote_latency_ms: String,
    pub read_index_latency_ms: String,
    pub snapshot_install_latency_ms: String,
    pub peer_append_queue_depth: String,
    pub peer_reorder_queue_depth: String,
    pub peer_snapshot_installed_index: String,
    pub wal_segment_count: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftPeerPipelineStatus {
    pub peer_id: u64,
    pub match_index: u64,
    pub next_index: u64,
    pub append_requests: u64,
    pub append_accepted: u64,
    pub append_rejected: u64,
    pub inflight_entries: u64,
    pub inflight_bytes: u64,
    pub append_queue_depth: u64,
    pub append_queue_limit: u64,
    pub append_queue_max_depth: u64,
    pub inflight_bytes_limit: u64,
    pub apply_inflight_tasks: u64,
    pub apply_inflight_limit: u64,
    pub apply_queue_depth: u64,
    pub apply_queue_max_depth: u64,
    pub apply_batch_bytes_limit: u64,
    pub apply_backpressure_rejections: u64,
    pub memory_backpressure_rejections: u64,
    pub oversized_log_rejections: u64,
    pub reorder_queue_depth: u64,
    pub out_of_order_append_rejections: u64,
    pub reorder_entries_rejected: u64,
    pub reorder_entry_timeouts: u64,
    pub reorder_dropped_packages: u64,
    #[serde(default)]
    pub stale_term_rejections: u64,
    pub snapshot_sending: bool,
    pub snapshot_installing: bool,
    pub snapshot_installed_index: u64,
    pub snapshot_send_attempts: u64,
    pub snapshot_install_total_chunks: u64,
    pub snapshot_install_progress_per_mille: u64,
    pub snapshot_backpressure_rejections: u64,
    pub snapshot_rate_limit_rejections: u64,
    pub snapshot_install_rolled_back: u64,
    #[serde(default)]
    pub snapshot_chunk_retry_count: u64,
    #[serde(default)]
    pub snapshot_send_timeouts: u64,
    pub snapshot_during_membership_change: bool,
    pub snapshot_rejoin_after_compacted_log: bool,
    pub transfer_leader_target: bool,
    pub transfer_leader_timeouts: u64,
    pub pre_vote_rejections: u64,
    pub election_rejections: u64,
    pub offline_timeout_reached: bool,
    pub offline_timeout_rejections: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftPipelineLimits {
    pub max_inflights_replicate: u64,
    pub max_memory_replicate_log_bytes: u64,
    pub max_inflights_apply_task: u64,
    pub max_apply_batch_bytes: u64,
    pub enable_reorder_queue: bool,
    pub reorder_window_size: u64,
    pub reorder_timeout_us: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftPipelineEvidence {
    pub per_peer_pipeline_state_present: bool,
    pub append_backpressure_enforced: bool,
    pub apply_backpressure_enforced: bool,
    pub memory_replicate_bytes_enforced: bool,
    pub oversized_log_rejection_present: bool,
    pub out_of_order_append_handling_present: bool,
    pub reorder_timeout_drop_present: bool,
    pub stale_term_rejection_present: bool,
    pub reorder_queue_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftSnapshotLifecycleEvidence {
    pub sender_lifecycle_present: bool,
    pub downloader_lifecycle_present: bool,
    pub retry_backpressure_present: bool,
    pub chunk_retry_present: bool,
    pub send_timeout_present: bool,
    pub rate_limit_present: bool,
    pub install_progress_present: bool,
    pub install_rollback_present: bool,
    pub membership_change_present: bool,
    pub rejoin_after_compacted_log_present: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftWalLifecycleStatus {
    pub segment_count: u64,
    pub active_segment_id: u64,
    pub first_retained_segment_id: u64,
    pub last_retained_segment_id: u64,
    pub total_bytes: u64,
    pub active_segment_bytes: u64,
    pub total_records: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub first_log_index: u64,
    pub last_log_index: u64,
    pub released_segment_count: u64,
    pub slow_fsync_backpressure_observed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftWalLifecycleEvidence {
    pub segment_lifecycle_present: bool,
    pub retained_range_present: bool,
    pub sequence_range_present: bool,
    pub log_index_range_present: bool,
    pub compaction_observed: bool,
    pub slow_fsync_backpressure_observed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftProcessNodeEvidence {
    pub node_id: u64,
    pub addr: String,
    pub wal_dir: String,
    #[serde(default)]
    pub snapshot_dir: String,
    pub commit_index: u64,
    pub applied_index: u64,
    pub snapshot_id: Option<String>,
    pub restarted: bool,
    pub log_store_validated: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftProcessOperationalSemanticsEvidence {
    #[serde(default)]
    pub api_presence_only_rejected: bool,
    #[serde(default)]
    pub process_path_validated: bool,
    #[serde(default)]
    pub read_index_validated: bool,
    #[serde(default)]
    pub leader_lease_validated: bool,
    #[serde(default)]
    pub stale_leader_lease_rejection_observed: bool,
    #[serde(default)]
    pub follower_lease_expiration_observed: bool,
    #[serde(default)]
    pub lagging_follower_read_rejected: bool,
    #[serde(default)]
    pub bounded_stale_read_acceptance_observed: bool,
    #[serde(default)]
    pub bounded_stale_read_rejection_observed: bool,
    #[serde(default)]
    pub minority_partition_read_rejection_observed: bool,
    #[serde(default)]
    pub healed_follower_catchup_observed: bool,
    #[serde(default)]
    pub stale_follower_write_rejected: bool,
    #[serde(default)]
    pub leader_transfer_exact_once_validated: bool,
    #[serde(default)]
    pub leader_transfer_under_load_validated: bool,
    #[serde(default)]
    pub snapshot_bootstrap_validated: bool,
    #[serde(default)]
    pub snapshot_install_restart_validated: bool,
    #[serde(default)]
    pub membership_rescale_validated: bool,
    #[serde(default)]
    pub membership_add_promote_remove_validated: bool,
    #[serde(default)]
    pub follower_rejoin_after_compaction_validated: bool,
    #[serde(default)]
    pub secondary_read_eligibility_validated: bool,
    #[serde(default)]
    pub apply_pipeline_converged: bool,
    #[serde(default)]
    pub wal_persistence_observed: bool,
    #[serde(default)]
    pub fsm_apply_idempotent_replay_observed: bool,
    #[serde(default)]
    pub storage_mutation_wal_fence_atomicity_observed: bool,
    #[serde(default)]
    pub snapshot_install_apply_fence_atomicity_observed: bool,
    #[serde(default)]
    pub process_restart_after_apply_crash_recovered: bool,
    #[serde(default)]
    pub ready: bool,
    #[serde(default)]
    pub blockers: Vec<String>,
}

impl RustRaftProcessOperationalSemanticsEvidence {
    pub fn proves_runtime_semantics(&self) -> bool {
        self.ready
            && self.blockers.is_empty()
            && self.api_presence_only_rejected
            && self.process_path_validated
            && self.read_index_validated
            && self.leader_lease_validated
            && self.stale_leader_lease_rejection_observed
            && self.follower_lease_expiration_observed
            && self.lagging_follower_read_rejected
            && self.bounded_stale_read_acceptance_observed
            && self.bounded_stale_read_rejection_observed
            && self.minority_partition_read_rejection_observed
            && self.healed_follower_catchup_observed
            && self.stale_follower_write_rejected
            && self.leader_transfer_exact_once_validated
            && self.leader_transfer_under_load_validated
            && self.snapshot_bootstrap_validated
            && self.snapshot_install_restart_validated
            && self.membership_rescale_validated
            && self.membership_add_promote_remove_validated
            && self.follower_rejoin_after_compaction_validated
            && self.secondary_read_eligibility_validated
            && self.apply_pipeline_converged
            && self.wal_persistence_observed
            && self.fsm_apply_idempotent_replay_observed
            && self.storage_mutation_wal_fence_atomicity_observed
            && self.snapshot_install_apply_fence_atomicity_observed
            && self.process_restart_after_apply_crash_recovered
    }

    pub fn missing_requirements(&self) -> Vec<String> {
        let mut missing = Vec::new();
        for (present, requirement) in [
            (self.ready, "operational_semantics_ready"),
            (
                self.api_presence_only_rejected,
                "api_presence_only_rejected",
            ),
            (self.process_path_validated, "process_path_validated"),
            (self.read_index_validated, "read_index_validated"),
            (self.leader_lease_validated, "leader_lease_validated"),
            (
                self.stale_leader_lease_rejection_observed,
                "stale_leader_lease_rejection_observed",
            ),
            (
                self.follower_lease_expiration_observed,
                "follower_lease_expiration_observed",
            ),
            (
                self.lagging_follower_read_rejected,
                "lagging_follower_read_rejected",
            ),
            (
                self.bounded_stale_read_acceptance_observed,
                "bounded_stale_read_acceptance_observed",
            ),
            (
                self.bounded_stale_read_rejection_observed,
                "bounded_stale_read_rejection_observed",
            ),
            (
                self.minority_partition_read_rejection_observed,
                "minority_partition_read_rejection_observed",
            ),
            (
                self.healed_follower_catchup_observed,
                "healed_follower_catchup_observed",
            ),
            (
                self.stale_follower_write_rejected,
                "stale_follower_write_rejected",
            ),
            (
                self.leader_transfer_exact_once_validated,
                "leader_transfer_exact_once_validated",
            ),
            (
                self.leader_transfer_under_load_validated,
                "leader_transfer_under_load_validated",
            ),
            (
                self.snapshot_bootstrap_validated,
                "snapshot_bootstrap_validated",
            ),
            (
                self.snapshot_install_restart_validated,
                "snapshot_install_restart_validated",
            ),
            (
                self.membership_rescale_validated,
                "membership_rescale_validated",
            ),
            (
                self.membership_add_promote_remove_validated,
                "membership_add_promote_remove_validated",
            ),
            (
                self.follower_rejoin_after_compaction_validated,
                "follower_rejoin_after_compaction_validated",
            ),
            (
                self.secondary_read_eligibility_validated,
                "secondary_read_eligibility_validated",
            ),
            (self.apply_pipeline_converged, "apply_pipeline_converged"),
            (self.wal_persistence_observed, "wal_persistence_observed"),
            (
                self.fsm_apply_idempotent_replay_observed,
                "fsm_apply_idempotent_replay_observed",
            ),
            (
                self.storage_mutation_wal_fence_atomicity_observed,
                "storage_mutation_wal_fence_atomicity_observed",
            ),
            (
                self.snapshot_install_apply_fence_atomicity_observed,
                "snapshot_install_apply_fence_atomicity_observed",
            ),
            (
                self.process_restart_after_apply_crash_recovered,
                "process_restart_after_apply_crash_recovered",
            ),
        ] {
            if !present {
                missing.push(requirement.to_string());
            }
        }
        missing.extend(
            self.blockers
                .iter()
                .map(|blocker| format!("blocker:{blocker}")),
        );
        missing
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftDataNodeProcessRolloutReport {
    pub shard_id: u64,
    #[serde(default)]
    pub voters: Vec<u64>,
    #[serde(default)]
    pub learners: Vec<u64>,
    pub nodes: Vec<RustRaftProcessNodeEvidence>,
    #[serde(default)]
    pub spawned_process_count: u64,
    #[serde(default)]
    pub independent_wal_dirs: bool,
    #[serde(default)]
    pub independent_snapshot_dirs: bool,
    #[serde(default)]
    pub observed_process_requests: u64,
    #[serde(default)]
    pub read_index_responses_observed: u64,
    #[serde(default)]
    pub restarted_node_count: u64,
    #[serde(default)]
    pub per_node_log_store_inspection_count: u64,
    pub write_proposed_through_process_api: bool,
    #[serde(default)]
    pub leader_transfer_validated: bool,
    #[serde(default)]
    pub failover_validated: bool,
    #[serde(default)]
    pub secondary_lag_observed: bool,
    #[serde(default)]
    pub lagging_follower_read_rejection_observed: bool,
    #[serde(default)]
    pub stale_follower_write_rejection_observed: bool,
    #[serde(default)]
    pub catchup_read_eligibility_observed: bool,
    #[serde(default)]
    pub minority_partition_rejection_observed: bool,
    #[serde(default)]
    pub bounded_stale_read_eligibility_observed: bool,
    #[serde(default)]
    pub healed_follower_catchup_observed: bool,
    #[serde(default)]
    pub lagging_follower_observed_lag: u64,
    #[serde(default)]
    pub membership_change_validated: bool,
    #[serde(default)]
    pub follower_lag_validated: bool,
    #[serde(default)]
    pub secondary_read_validated: bool,
    pub recovered_after_restart: bool,
    #[serde(default)]
    pub restart_recovery_validated: bool,
    pub snapshot_install_validated: bool,
    pub applied_fence_validated: bool,
    #[serde(default)]
    pub crash_after_storage_mutation_recovered: bool,
    #[serde(default)]
    pub crash_after_wal_persist_recovered: bool,
    #[serde(default)]
    pub crash_during_snapshot_install_recovered: bool,
    #[serde(default)]
    pub apply_fence_recovered_after_restart: bool,
    pub multi_process_log_store_validated: bool,
    #[serde(default)]
    pub operational_semantics: RustRaftProcessOperationalSemanticsEvidence,
    pub ready: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftMetaProcessRolloutReport {
    #[serde(default)]
    pub voters: Vec<u64>,
    #[serde(default)]
    pub learners: Vec<u64>,
    pub nodes: Vec<RustRaftProcessNodeEvidence>,
    #[serde(default)]
    pub spawned_process_count: u64,
    #[serde(default)]
    pub independent_wal_dirs: bool,
    #[serde(default)]
    pub independent_snapshot_dirs: bool,
    #[serde(default)]
    pub observed_process_requests: u64,
    #[serde(default)]
    pub read_index_responses_observed: u64,
    #[serde(default)]
    pub restarted_node_count: u64,
    #[serde(default)]
    pub per_node_log_store_inspection_count: u64,
    pub mutation_proposed_through_process_api: bool,
    #[serde(default)]
    pub applied_raft_mutations: u64,
    #[serde(default)]
    pub generated_scheduler_tasks: u64,
    #[serde(default)]
    pub scheduler_retries: u64,
    #[serde(default)]
    pub stale_scheduler_token_rejected: bool,
    #[serde(default)]
    pub data_node_membership_results_ready: bool,
    #[serde(default)]
    pub scheduler_mutations_proposed_through_process_api: bool,
    #[serde(default)]
    pub scheduler_task_replay_from_raft_log_observed: bool,
    #[serde(default)]
    pub membership_mutations_proposed_through_process_api: bool,
    #[serde(default)]
    pub data_node_membership_workflow_report_attached: bool,
    #[serde(default)]
    pub data_node_raft_group_results_observed: bool,
    #[serde(default)]
    pub failover_validated: bool,
    #[serde(default)]
    pub membership_change_validated: bool,
    #[serde(default)]
    pub follower_lag_validated: bool,
    #[serde(default)]
    pub secondary_read_validated: bool,
    pub read_index_validated: bool,
    pub snapshot_install_validated: bool,
    pub recovered_after_restart: bool,
    pub scheduler_task_replay_validated: bool,
    #[serde(default)]
    pub crash_after_meta_mutation_recovered: bool,
    #[serde(default)]
    pub crash_after_meta_wal_persist_recovered: bool,
    #[serde(default)]
    pub crash_during_meta_snapshot_install_recovered: bool,
    #[serde(default)]
    pub meta_apply_fence_recovered_after_restart: bool,
    pub multi_process_log_store_validated: bool,
    #[serde(default)]
    pub operational_semantics: RustRaftProcessOperationalSemanticsEvidence,
    pub ready: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftMembershipScope {
    Metaserver,
    DataNode,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftMembershipTransitionKind {
    Failover,
    ScaleUp,
    ScaleDown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftMembershipTransitionEvidence {
    pub scope: RustRaftMembershipScope,
    pub transition: RustRaftMembershipTransitionKind,
    #[serde(default)]
    pub before_voters: Vec<u64>,
    #[serde(default)]
    pub after_voters: Vec<u64>,
    #[serde(default)]
    pub before_learners: Vec<u64>,
    #[serde(default)]
    pub after_learners: Vec<u64>,
    pub leader_before: Option<u64>,
    pub leader_after: Option<u64>,
    #[serde(default)]
    pub failed_or_removed_nodes: Vec<u64>,
    #[serde(default)]
    pub added_nodes: Vec<u64>,
    #[serde(default)]
    pub caught_up_nodes: Vec<u64>,
    pub commit_index_before: u64,
    pub commit_index_after: u64,
    pub applied_index_after: u64,
    pub joint_consensus_used: bool,
    pub old_majority_preserved: bool,
    pub new_majority_reached: bool,
    pub stale_leader_rejected: bool,
    pub read_index_validated_after: bool,
    pub write_validated_after: bool,
    pub snapshot_floor_preserved: bool,
    pub secondary_replication_visible: bool,
    #[serde(default)]
    pub scheduler_generation_advanced: bool,
    #[serde(default)]
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftMembershipTransitionDecision {
    pub scope: RustRaftMembershipScope,
    pub transition: RustRaftMembershipTransitionKind,
    pub ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftMembershipReadinessReport {
    pub ready: bool,
    pub satisfied: Vec<String>,
    pub missing: Vec<String>,
    pub decisions: Vec<RustRaftMembershipTransitionDecision>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftRole {
    Leader,
    Follower,
    Candidate,
    Learner,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftLogId {
    pub term: u64,
    pub index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftLogEntry {
    pub log_id: RustRaftLogId,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftHardState {
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub committed: Option<RustRaftLogId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftPeerStatus {
    pub node_id: u64,
    pub matched: u64,
    pub next_index: u64,
    pub learner: bool,
    pub healthy: bool,
    pub lag: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftStatusSnapshot {
    pub group_id: u64,
    pub node_id: u64,
    pub role: RustRaftRole,
    pub term: u64,
    pub leader_id: Option<u64>,
    pub commit_index: u64,
    pub applied_index: u64,
    pub last_log_index: u64,
    pub last_snapshot_index: u64,
    pub peers: Vec<RustRaftPeerStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftAppendEntriesRequest {
    pub group_id: u64,
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_id: Option<RustRaftLogId>,
    pub entries: Vec<RustRaftLogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftAppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub match_index: u64,
    pub rejection_hint: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftVoteRequest {
    pub group_id: u64,
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_id: Option<RustRaftLogId>,
    pub pre_vote: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftSnapshotMeta {
    pub snapshot_id: String,
    pub last_log_id: RustRaftLogId,
    pub membership: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftSnapshotChunk {
    pub meta: RustRaftSnapshotMeta,
    pub offset: u64,
    pub data: Vec<u8>,
    pub done: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftInstallSnapshotRequest {
    pub group_id: u64,
    pub term: u64,
    pub leader_id: u64,
    pub chunk: RustRaftSnapshotChunk,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftInstallSnapshotResponse {
    pub term: u64,
    pub accepted: bool,
    pub next_offset: u64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadIndexRequest {
    pub group_id: u64,
    pub requester_id: u64,
    pub min_commit_index: u64,
    pub allow_lease_read: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadIndexResponse {
    pub safe: bool,
    pub read_index: u64,
    pub lease_read: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadSafetyDecision {
    pub safe: bool,
    pub read_index: u64,
    pub lease_read: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftLearnerPromotionDecision {
    pub promotable: bool,
    pub learner_id: u64,
    pub learner_match_index: u64,
    pub required_match_index: u64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftAppendSafetyDecision {
    pub accepted: bool,
    pub rejected_compacted_entry: bool,
    pub reason: String,
}

#[derive(Debug, Error)]
pub enum RustRaftError {
    #[error("transport error: {0}")]
    Transport(String),
    #[error("storage error: {0}")]
    Storage(String),
}

pub trait RustRaftStorage {
    fn append_entries(&mut self, entries: &[RustRaftLogEntry]) -> Result<(), RustRaftError>;
    fn read_entries(&self, start: u64, end: u64) -> Result<Vec<RustRaftLogEntry>, RustRaftError>;
    fn hard_state(&self) -> Result<RustRaftHardState, RustRaftError>;
    fn install_snapshot(&mut self, chunk: RustRaftSnapshotChunk) -> Result<(), RustRaftError>;
}

pub trait RustRaftTransport {
    fn append_entries(
        &self,
        target: u64,
        request: RustRaftAppendEntriesRequest,
    ) -> Result<RustRaftAppendEntriesResponse, RustRaftError>;
    fn vote(
        &self,
        target: u64,
        request: RustRaftVoteRequest,
    ) -> Result<RustRaftVoteResponse, RustRaftError>;
    fn install_snapshot(
        &self,
        target: u64,
        request: RustRaftInstallSnapshotRequest,
    ) -> Result<RustRaftInstallSnapshotResponse, RustRaftError>;
    fn read_index(
        &self,
        target: u64,
        request: RustRaftReadIndexRequest,
    ) -> Result<RustRaftReadIndexResponse, RustRaftError>;
}

pub fn rustraft_parity_contract() -> RustRaftParityContract {
    RustRaftParityContract {
        library_name: "rustraft".to_string(),
        consensus_backend_boundary: "temporalstore_rust::raft::DataRaftConsensusBackend"
            .to_string(),
        openraft_dependency_removed: true,
        requirements: rustraft_requirements(),
    }
}

pub fn rustraft_public_api_contract() -> RustRaftPublicApiContract {
    RustRaftPublicApiContract {
        storage_trait: "RustRaftStorage".to_string(),
        transport_trait: "RustRaftTransport".to_string(),
        rpc_messages: vec![
            "RustRaftAppendEntriesRequest".to_string(),
            "RustRaftVoteRequest".to_string(),
            "RustRaftInstallSnapshotRequest".to_string(),
            "RustRaftReadIndexRequest".to_string(),
        ],
        safety_helpers: vec![
            "rustraft_read_safety_decision".to_string(),
            "rustraft_append_safety_decision".to_string(),
            "rustraft_learner_promotion_decision".to_string(),
        ],
        metrics: rustraft_metric_names(),
    }
}

pub fn rustraft_metric_names() -> RustRaftMetricNames {
    RustRaftMetricNames {
        ready: "rustraft_ready".to_string(),
        append_latency_ms: "rustraft_append_latency_ms".to_string(),
        vote_latency_ms: "rustraft_vote_latency_ms".to_string(),
        read_index_latency_ms: "rustraft_read_index_latency_ms".to_string(),
        snapshot_install_latency_ms: "rustraft_snapshot_install_latency_ms".to_string(),
        peer_append_queue_depth: "rustraft_peer_append_queue_depth".to_string(),
        peer_reorder_queue_depth: "rustraft_peer_reorder_queue_depth".to_string(),
        peer_snapshot_installed_index: "rustraft_peer_snapshot_installed_index".to_string(),
        wal_segment_count: "rustraft_wal_segment_count".to_string(),
    }
}

pub fn rustraft_parity_report(snapshot: &RustRaftReadinessSnapshot) -> RustRaftParityReport {
    let contract = rustraft_parity_contract();
    let evidence = rustraft_readiness_evidence(snapshot);
    let satisfied = evidence
        .iter()
        .filter(|item| item.present)
        .map(|item| item.requirement_id.clone())
        .collect::<Vec<_>>();
    let missing = evidence
        .iter()
        .filter(|item| !item.present)
        .map(|item| item.requirement_id.clone())
        .collect::<Vec<_>>();
    let production_blockers = contract
        .requirements
        .iter()
        .filter(|requirement| {
            requirement.required_for_production && missing.iter().any(|id| id == &requirement.id)
        })
        .map(|requirement| format!("{:?}:{}", requirement.category, requirement.id).to_lowercase())
        .collect::<Vec<_>>();
    let ready = missing.is_empty() && production_blockers.is_empty();
    RustRaftParityReport {
        contract,
        ready,
        production_status: if ready {
            RustRaftProductionStatus::ProductionReady
        } else {
            RustRaftProductionStatus::Blocked
        },
        satisfied,
        missing,
        production_blockers,
    }
}

pub fn rustraft_production_readiness_report(
    input: &RustRaftProductionReadinessInput,
) -> RustRaftProductionReadinessReport {
    let parity = rustraft_parity_report(&input.readiness);
    let mut satisfied = parity
        .satisfied
        .iter()
        .map(|id| format!("contract:{id}"))
        .collect::<Vec<_>>();
    let mut missing = parity
        .missing
        .iter()
        .map(|id| format!("contract:{id}"))
        .collect::<Vec<_>>();
    let mut production_blockers = parity.production_blockers.clone();
    let mut recommended_next_actions = Vec::new();

    if parity.ready {
        satisfied.push("contract:all_required_semantics".to_string());
    } else {
        recommended_next_actions.push(
            "fix RustRaft semantic contract/readiness gaps before production rollout".to_string(),
        );
    }

    require_option(
        "pipeline:evidence_present",
        input.peer_pipeline.as_ref(),
        &mut satisfied,
        &mut missing,
        &mut production_blockers,
        &mut recommended_next_actions,
        "attach per-peer pipeline evidence from the running RustRaft group",
    );
    if let Some(pipeline) = &input.peer_pipeline {
        for (present, id, action) in [
            (
                pipeline.per_peer_pipeline_state_present,
                "pipeline:per_peer_state",
                "export per-peer replication/apply pipeline state",
            ),
            (
                pipeline.append_backpressure_enforced,
                "pipeline:append_backpressure",
                "prove append queue backpressure under load",
            ),
            (
                pipeline.apply_backpressure_enforced,
                "pipeline:apply_backpressure",
                "prove apply queue backpressure under load",
            ),
            (
                pipeline.memory_replicate_bytes_enforced,
                "pipeline:memory_replicate_bytes",
                "prove max_memory_replicate_log_bytes enforcement",
            ),
            (
                pipeline.oversized_log_rejection_present,
                "pipeline:oversized_log_rejection",
                "prove oversized log entry rejection",
            ),
            (
                pipeline.out_of_order_append_handling_present,
                "pipeline:out_of_order_append_handling",
                "prove out-of-order append handling/rejection",
            ),
            (
                pipeline.reorder_queue_enabled,
                "pipeline:reorder_queue",
                "enable and prove reorder queue behavior",
            ),
        ] {
            require_bool(
                present,
                id,
                &mut satisfied,
                &mut missing,
                &mut production_blockers,
                &mut recommended_next_actions,
                action,
            );
        }
    }

    require_option(
        "snapshot:evidence_present",
        input.snapshot_lifecycle.as_ref(),
        &mut satisfied,
        &mut missing,
        &mut production_blockers,
        &mut recommended_next_actions,
        "attach snapshot send/install lifecycle evidence",
    );
    if let Some(snapshot) = &input.snapshot_lifecycle {
        for (present, id, action) in [
            (
                snapshot.sender_lifecycle_present,
                "snapshot:sender_lifecycle",
                "prove snapshot sender lifecycle",
            ),
            (
                snapshot.downloader_lifecycle_present,
                "snapshot:downloader_lifecycle",
                "prove snapshot downloader/install lifecycle",
            ),
            (
                snapshot.retry_backpressure_present,
                "snapshot:retry_backpressure",
                "prove snapshot retry/backpressure behavior",
            ),
            (
                snapshot.chunk_retry_present,
                "snapshot:chunk_retry",
                "prove snapshot chunk retry behavior",
            ),
            (
                snapshot.send_timeout_present,
                "snapshot:send_timeout",
                "prove snapshot send timeout behavior",
            ),
            (
                snapshot.rate_limit_present,
                "snapshot:rate_limit",
                "prove snapshot rate limiting",
            ),
            (
                snapshot.install_progress_present,
                "snapshot:install_progress",
                "export snapshot install progress",
            ),
            (
                snapshot.install_rollback_present,
                "snapshot:install_rollback",
                "prove snapshot install rollback",
            ),
            (
                snapshot.membership_change_present,
                "snapshot:membership_change",
                "prove snapshot behavior during membership change",
            ),
            (
                snapshot.rejoin_after_compacted_log_present,
                "snapshot:rejoin_after_compacted_log",
                "prove rejoin after compacted log",
            ),
        ] {
            require_bool(
                present,
                id,
                &mut satisfied,
                &mut missing,
                &mut production_blockers,
                &mut recommended_next_actions,
                action,
            );
        }
    }

    require_option(
        "wal:evidence_present",
        input.wal_lifecycle.as_ref(),
        &mut satisfied,
        &mut missing,
        &mut production_blockers,
        &mut recommended_next_actions,
        "attach WAL segment/range/backpressure evidence",
    );
    if let Some(wal) = &input.wal_lifecycle {
        for (present, id, action) in [
            (
                wal.segment_lifecycle_present,
                "wal:segment_lifecycle",
                "prove WAL segment lifecycle",
            ),
            (
                wal.retained_range_present,
                "wal:retained_range",
                "prove retained WAL range reporting",
            ),
            (
                wal.sequence_range_present,
                "wal:sequence_range",
                "prove WAL sequence range reporting",
            ),
            (
                wal.log_index_range_present,
                "wal:log_index_range",
                "prove WAL log-index range reporting",
            ),
            (
                wal.compaction_observed,
                "wal:compaction",
                "prove WAL compaction/released segment behavior",
            ),
            (
                wal.slow_fsync_backpressure_observed,
                "wal:slow_fsync_backpressure",
                "prove slow fsync backpressure behavior",
            ),
        ] {
            require_bool(
                present,
                id,
                &mut satisfied,
                &mut missing,
                &mut production_blockers,
                &mut recommended_next_actions,
                action,
            );
        }
    }

    require_data_node_rollout(
        input.data_node_rollout.as_ref(),
        &mut satisfied,
        &mut missing,
        &mut production_blockers,
        &mut recommended_next_actions,
    );
    require_meta_rollout(
        input.metaserver_rollout.as_ref(),
        &mut satisfied,
        &mut missing,
        &mut production_blockers,
        &mut recommended_next_actions,
    );
    require_membership_transitions(
        &input.membership_transitions,
        &mut satisfied,
        &mut missing,
        &mut production_blockers,
        &mut recommended_next_actions,
    );

    let ready = missing.is_empty() && production_blockers.is_empty();
    RustRaftProductionReadinessReport {
        parity,
        public_api: rustraft_public_api_contract(),
        ready,
        production_status: if ready {
            RustRaftProductionStatus::ProductionReady
        } else {
            RustRaftProductionStatus::Blocked
        },
        satisfied,
        missing,
        production_blockers,
        recommended_next_actions,
    }
}

pub fn rustraft_membership_readiness_report(
    transitions: &[RustRaftMembershipTransitionEvidence],
) -> RustRaftMembershipReadinessReport {
    let required = [
        (
            RustRaftMembershipScope::Metaserver,
            RustRaftMembershipTransitionKind::Failover,
        ),
        (
            RustRaftMembershipScope::Metaserver,
            RustRaftMembershipTransitionKind::ScaleUp,
        ),
        (
            RustRaftMembershipScope::Metaserver,
            RustRaftMembershipTransitionKind::ScaleDown,
        ),
        (
            RustRaftMembershipScope::DataNode,
            RustRaftMembershipTransitionKind::Failover,
        ),
        (
            RustRaftMembershipScope::DataNode,
            RustRaftMembershipTransitionKind::ScaleUp,
        ),
        (
            RustRaftMembershipScope::DataNode,
            RustRaftMembershipTransitionKind::ScaleDown,
        ),
    ];
    let mut satisfied = Vec::new();
    let mut missing = Vec::new();
    let mut decisions = Vec::new();

    for (scope, transition) in required {
        let id = membership_transition_id(scope, transition);
        let Some(evidence) = transitions
            .iter()
            .find(|item| item.scope == scope && item.transition == transition)
        else {
            missing.push(format!("{id}:evidence_present"));
            decisions.push(RustRaftMembershipTransitionDecision {
                scope,
                transition,
                ready: false,
                missing: vec!["evidence_present".to_string()],
            });
            continue;
        };
        let transition_missing = rustraft_membership_transition_missing(evidence);
        if transition_missing.is_empty() {
            satisfied.push(id);
            decisions.push(RustRaftMembershipTransitionDecision {
                scope,
                transition,
                ready: true,
                missing: Vec::new(),
            });
        } else {
            missing.extend(
                transition_missing
                    .iter()
                    .map(|requirement| format!("{id}:{requirement}")),
            );
            decisions.push(RustRaftMembershipTransitionDecision {
                scope,
                transition,
                ready: false,
                missing: transition_missing,
            });
        }
    }

    RustRaftMembershipReadinessReport {
        ready: missing.is_empty(),
        satisfied,
        missing,
        decisions,
    }
}

pub fn rustraft_membership_transition_missing(
    evidence: &RustRaftMembershipTransitionEvidence,
) -> Vec<String> {
    let mut missing = Vec::new();
    let before_majority = majority_size(evidence.before_voters.len());
    let after_majority = majority_size(evidence.after_voters.len());
    if evidence.before_voters.len() < 3 {
        missing.push("before_voter_quorum_size".to_string());
    }
    if evidence.after_voters.len() < 3 {
        missing.push("after_voter_quorum_size".to_string());
    }
    if evidence.commit_index_after < evidence.commit_index_before {
        missing.push("monotonic_commit_index".to_string());
    }
    if evidence.applied_index_after < evidence.commit_index_after {
        missing.push("apply_catches_commit".to_string());
    }
    if !evidence.old_majority_preserved {
        missing.push(format!("old_majority_preserved_{before_majority}"));
    }
    if !evidence.new_majority_reached {
        missing.push(format!("new_majority_reached_{after_majority}"));
    }
    if !evidence.stale_leader_rejected {
        missing.push("stale_leader_rejected".to_string());
    }
    if !evidence.read_index_validated_after {
        missing.push("read_index_after_transition".to_string());
    }
    if !evidence.write_validated_after {
        missing.push("write_after_transition".to_string());
    }
    if !evidence.snapshot_floor_preserved {
        missing.push("snapshot_floor_preserved".to_string());
    }
    if !evidence.secondary_replication_visible {
        missing.push("secondary_replication_visible".to_string());
    }
    if matches!(evidence.scope, RustRaftMembershipScope::Metaserver)
        && !evidence.scheduler_generation_advanced
    {
        missing.push("scheduler_generation_advanced".to_string());
    }
    match evidence.transition {
        RustRaftMembershipTransitionKind::Failover => {
            if evidence.leader_before.is_none() || evidence.leader_after.is_none() {
                missing.push("leader_before_after_present".to_string());
            }
            if evidence.leader_before == evidence.leader_after {
                missing.push("leader_changed_after_failover".to_string());
            }
            if evidence.failed_or_removed_nodes.is_empty() {
                missing.push("failed_node_recorded".to_string());
            }
        }
        RustRaftMembershipTransitionKind::ScaleUp => {
            if !evidence.joint_consensus_used {
                missing.push("joint_consensus_used".to_string());
            }
            if evidence.added_nodes.is_empty() {
                missing.push("added_node_recorded".to_string());
            }
            if evidence.after_voters.len() <= evidence.before_voters.len() {
                missing.push("voter_count_increased".to_string());
            }
            if evidence.caught_up_nodes.is_empty() {
                missing.push("learner_catchup_observed".to_string());
            }
        }
        RustRaftMembershipTransitionKind::ScaleDown => {
            if !evidence.joint_consensus_used {
                missing.push("joint_consensus_used".to_string());
            }
            if evidence.failed_or_removed_nodes.is_empty() {
                missing.push("removed_node_recorded".to_string());
            }
            if evidence.after_voters.len() >= evidence.before_voters.len() {
                missing.push("voter_count_decreased".to_string());
            }
        }
    }
    missing.extend(
        evidence
            .blockers
            .iter()
            .map(|blocker| format!("blocker:{blocker}")),
    );
    missing
}

pub fn rustraft_pipeline_evidence(
    peers: &[RustRaftPeerPipelineStatus],
    limits: RustRaftPipelineLimits,
) -> RustRaftPipelineEvidence {
    RustRaftPipelineEvidence {
        per_peer_pipeline_state_present: !peers.is_empty(),
        append_backpressure_enforced: peers.iter().any(|peer| {
            peer.append_queue_limit == limits.max_inflights_replicate
                && (peer.append_queue_max_depth >= peer.append_queue_limit
                    || peer.append_queue_depth >= peer.append_queue_limit)
        }),
        apply_backpressure_enforced: peers.iter().any(|peer| {
            peer.apply_inflight_limit == limits.max_inflights_apply_task
                && (peer.apply_backpressure_rejections > 0
                    || peer.apply_queue_max_depth >= peer.apply_inflight_limit)
        }),
        memory_replicate_bytes_enforced: peers.iter().any(|peer| {
            peer.inflight_bytes_limit == limits.max_memory_replicate_log_bytes
                && peer.memory_backpressure_rejections > 0
        }),
        oversized_log_rejection_present: peers.iter().any(|peer| peer.oversized_log_rejections > 0),
        out_of_order_append_handling_present: peers.iter().any(|peer| {
            peer.out_of_order_append_rejections > 0
                || peer.reorder_entries_rejected > 0
                || peer.reorder_entry_timeouts > 0
                || peer.reorder_dropped_packages > 0
        }),
        reorder_timeout_drop_present: peers
            .iter()
            .any(|peer| peer.reorder_entry_timeouts > 0 && peer.reorder_dropped_packages > 0),
        stale_term_rejection_present: peers.iter().any(|peer| peer.stale_term_rejections > 0),
        reorder_queue_enabled: limits.enable_reorder_queue
            && limits.reorder_window_size > 0
            && limits.reorder_timeout_us > 0
            && peers.iter().any(|peer| peer.reorder_queue_depth > 0),
    }
}

pub fn rustraft_snapshot_lifecycle_evidence(
    peers: &[RustRaftPeerPipelineStatus],
    send_snapshot_timeout_ms: u64,
    max_inflights_replicate: u64,
) -> RustRaftSnapshotLifecycleEvidence {
    RustRaftSnapshotLifecycleEvidence {
        sender_lifecycle_present: send_snapshot_timeout_ms > 0
            && peers
                .iter()
                .any(|peer| peer.snapshot_sending || peer.snapshot_send_attempts > 0),
        downloader_lifecycle_present: peers
            .iter()
            .any(|peer| peer.snapshot_installing || peer.snapshot_install_total_chunks > 0),
        retry_backpressure_present: peers.iter().any(|peer| {
            peer.snapshot_backpressure_rejections > 0
                || (max_inflights_replicate > 0
                    && peer.snapshot_send_attempts > max_inflights_replicate)
        }),
        chunk_retry_present: peers.iter().any(|peer| peer.snapshot_chunk_retry_count > 0),
        send_timeout_present: peers.iter().any(|peer| peer.snapshot_send_timeouts > 0),
        rate_limit_present: peers
            .iter()
            .any(|peer| peer.snapshot_rate_limit_rejections > 0),
        install_progress_present: peers.iter().any(|peer| {
            peer.snapshot_installed_index > 0 || peer.snapshot_install_progress_per_mille > 0
        }),
        install_rollback_present: peers
            .iter()
            .any(|peer| peer.snapshot_install_rolled_back > 0),
        membership_change_present: peers
            .iter()
            .any(|peer| peer.snapshot_during_membership_change),
        rejoin_after_compacted_log_present: peers
            .iter()
            .any(|peer| peer.snapshot_rejoin_after_compacted_log),
    }
}

pub fn rustraft_wal_lifecycle_evidence(
    status: &RustRaftWalLifecycleStatus,
) -> RustRaftWalLifecycleEvidence {
    RustRaftWalLifecycleEvidence {
        segment_lifecycle_present: status.segment_count > 0
            && status.active_segment_id >= status.first_retained_segment_id
            && status.last_retained_segment_id >= status.first_retained_segment_id,
        retained_range_present: status.first_retained_segment_id <= status.last_retained_segment_id,
        sequence_range_present: status.first_sequence <= status.last_sequence
            && status.total_records > 0,
        log_index_range_present: status.first_log_index <= status.last_log_index
            && status.last_log_index > 0,
        compaction_observed: status.released_segment_count > 0,
        slow_fsync_backpressure_observed: status.slow_fsync_backpressure_observed,
    }
}

pub fn rustraft_readiness_evidence(
    snapshot: &RustRaftReadinessSnapshot,
) -> Vec<RustRaftReadinessEvidence> {
    rustraft_requirements()
        .into_iter()
        .map(|requirement| RustRaftReadinessEvidence {
            present: readiness_field_present(snapshot, &requirement.readiness_field),
            requirement_id: requirement.id,
            readiness_field: requirement.readiness_field,
        })
        .collect()
}

pub fn rustraft_read_safety_decision(
    status: &RustRaftStatusSnapshot,
    request: &RustRaftReadIndexRequest,
) -> RustRaftReadSafetyDecision {
    if status.group_id != request.group_id {
        return RustRaftReadSafetyDecision {
            safe: false,
            read_index: status.commit_index,
            lease_read: false,
            reason: "group_mismatch".to_string(),
        };
    }
    if !matches!(status.role, RustRaftRole::Leader) {
        return RustRaftReadSafetyDecision {
            safe: false,
            read_index: status.commit_index,
            lease_read: false,
            reason: "not_leader".to_string(),
        };
    }
    if status.applied_index < request.min_commit_index {
        return RustRaftReadSafetyDecision {
            safe: false,
            read_index: status.commit_index,
            lease_read: false,
            reason: "apply_lag".to_string(),
        };
    }
    RustRaftReadSafetyDecision {
        safe: true,
        read_index: status.commit_index,
        lease_read: request.allow_lease_read,
        reason: "safe".to_string(),
    }
}

pub fn rustraft_learner_promotion_decision(
    status: &RustRaftStatusSnapshot,
    learner_id: u64,
    max_lag: u64,
) -> RustRaftLearnerPromotionDecision {
    let Some(peer) = status.peers.iter().find(|peer| peer.node_id == learner_id) else {
        return RustRaftLearnerPromotionDecision {
            promotable: false,
            learner_id,
            learner_match_index: 0,
            required_match_index: status.commit_index.saturating_sub(max_lag),
            reason: "learner_missing".to_string(),
        };
    };
    let required_match_index = status.commit_index.saturating_sub(max_lag);
    let promotable = peer.learner && peer.healthy && peer.matched >= required_match_index;
    RustRaftLearnerPromotionDecision {
        promotable,
        learner_id,
        learner_match_index: peer.matched,
        required_match_index,
        reason: if promotable {
            "caught_up".to_string()
        } else {
            "not_caught_up".to_string()
        },
    }
}

pub fn rustraft_append_safety_decision(
    first_retained_log_index: u64,
    snapshot_index: u64,
    request: &RustRaftAppendEntriesRequest,
) -> RustRaftAppendSafetyDecision {
    let prev_index = request.prev_log_id.as_ref().map(|id| id.index).unwrap_or(0);
    if prev_index > 0 && prev_index < first_retained_log_index && prev_index <= snapshot_index {
        return RustRaftAppendSafetyDecision {
            accepted: false,
            rejected_compacted_entry: true,
            reason: "prev_log_compacted".to_string(),
        };
    }
    if request
        .entries
        .iter()
        .any(|entry| entry.log_id.index < first_retained_log_index)
    {
        return RustRaftAppendSafetyDecision {
            accepted: false,
            rejected_compacted_entry: true,
            reason: "entry_compacted".to_string(),
        };
    }
    RustRaftAppendSafetyDecision {
        accepted: true,
        rejected_compacted_entry: false,
        reason: "accepted".to_string(),
    }
}

fn rustraft_requirements() -> Vec<RustRaftSemanticRequirement> {
    use RustRaftRequirementCategory::*;
    [
        (
            "leader_write_authority",
            Safety,
            "rustraft_leader_write_authority_present",
        ),
        (
            "operator_observability",
            Observability,
            "rustraft_operator_observability_present",
        ),
        (
            "rpc_transport_contract",
            Transport,
            "rustraft_rpc_transport_contract_present",
        ),
        (
            "snapshot_trigger",
            Durability,
            "rustraft_log_retention_snapshot_trigger_present",
        ),
        (
            "apply_snapshot_fence",
            Durability,
            "rustraft_apply_snapshot_fence_present",
        ),
        (
            "storage_apply_fence",
            Durability,
            "raft_storage_apply_fence_present",
        ),
        (
            "snapshot_floor_log_matching",
            Durability,
            "rustraft_snapshot_floor_log_matching_present",
        ),
        (
            "snapshot_tail_catchup",
            Durability,
            "rustraft_snapshot_tail_catchup_present",
        ),
        (
            "compacted_entry_rejection",
            Safety,
            "rustraft_compacted_entry_rejection_present",
        ),
        (
            "metaserver_snapshot_floor_election",
            Safety,
            "rustraft_metaserver_snapshot_floor_election_present",
        ),
        (
            "learner_catchup_promotion",
            Membership,
            "learner_catchup_promotion_present",
        ),
        (
            "metaserver_membership_workflow",
            Membership,
            "metaserver_membership_workflow_present",
        ),
    ]
    .into_iter()
    .map(
        |(id, category, readiness_field)| RustRaftSemanticRequirement {
            id: id.to_string(),
            category,
            readiness_field: readiness_field.to_string(),
            required_for_production: true,
        },
    )
    .collect()
}

fn require_option<T>(
    id: &str,
    value: Option<&T>,
    satisfied: &mut Vec<String>,
    missing: &mut Vec<String>,
    blockers: &mut Vec<String>,
    actions: &mut Vec<String>,
    action: &str,
) {
    require_bool(
        value.is_some(),
        id,
        satisfied,
        missing,
        blockers,
        actions,
        action,
    );
}

fn require_bool(
    present: bool,
    id: &str,
    satisfied: &mut Vec<String>,
    missing: &mut Vec<String>,
    blockers: &mut Vec<String>,
    actions: &mut Vec<String>,
    action: &str,
) {
    if present {
        satisfied.push(id.to_string());
    } else {
        missing.push(id.to_string());
        blockers.push(id.to_string());
        actions.push(action.to_string());
    }
}

fn require_data_node_rollout(
    rollout: Option<&RustRaftDataNodeProcessRolloutReport>,
    satisfied: &mut Vec<String>,
    missing: &mut Vec<String>,
    blockers: &mut Vec<String>,
    actions: &mut Vec<String>,
) {
    require_option(
        "data_node:evidence_present",
        rollout,
        satisfied,
        missing,
        blockers,
        actions,
        "attach data-node process rollout evidence",
    );
    let Some(rollout) = rollout else {
        return;
    };
    for (present, id, action) in [
        (
            rollout.ready,
            "data_node:ready",
            "make data-node rollout ready",
        ),
        (
            rollout.blockers.is_empty(),
            "data_node:no_blockers",
            "clear data-node rollout blockers",
        ),
        (
            !rollout.nodes.is_empty()
                && rollout.spawned_process_count as usize >= rollout.nodes.len(),
            "data_node:processes_spawned",
            "spawn and observe all data-node RustRaft processes",
        ),
        (
            !rollout.voters.is_empty(),
            "data_node:voters_present",
            "run data-node RustRaft with voter membership",
        ),
        (
            rollout.independent_wal_dirs,
            "data_node:independent_wal_dirs",
            "use independent WAL dirs per data-node process",
        ),
        (
            rollout.independent_snapshot_dirs,
            "data_node:independent_snapshot_dirs",
            "use independent snapshot dirs per data-node process",
        ),
        (
            rollout.write_proposed_through_process_api,
            "data_node:process_write_path",
            "prove writes enter through the process API",
        ),
        (
            rollout.read_index_responses_observed > 0,
            "data_node:read_index",
            "observe data-node read-index responses",
        ),
        (
            rollout.leader_transfer_validated,
            "data_node:leader_transfer",
            "validate data-node leader transfer",
        ),
        (
            rollout.failover_validated,
            "data_node:failover",
            "validate data-node failover",
        ),
        (
            rollout.membership_change_validated,
            "data_node:membership_change",
            "validate data-node membership add/promote/remove",
        ),
        (
            rollout.follower_lag_validated,
            "data_node:follower_lag",
            "validate data-node follower lag handling",
        ),
        (
            rollout.secondary_read_validated,
            "data_node:secondary_read",
            "validate data-node secondary read eligibility",
        ),
        (
            rollout.recovered_after_restart && rollout.restart_recovery_validated,
            "data_node:restart_recovery",
            "validate data-node restart recovery",
        ),
        (
            rollout.snapshot_install_validated,
            "data_node:snapshot_install",
            "validate data-node snapshot install",
        ),
        (
            rollout.applied_fence_validated,
            "data_node:apply_fence",
            "validate data-node apply fence",
        ),
        (
            rollout.multi_process_log_store_validated,
            "data_node:multi_process_log_store",
            "validate independent multi-process log stores",
        ),
        (
            rollout.operational_semantics.proves_runtime_semantics(),
            "data_node:operational_semantics",
            "prove data-node runtime semantics, not only API presence",
        ),
    ] {
        require_bool(present, id, satisfied, missing, blockers, actions, action);
    }
    for missing_requirement in rollout.operational_semantics.missing_requirements() {
        require_bool(
            false,
            &format!("data_node:semantics:{missing_requirement}"),
            satisfied,
            missing,
            blockers,
            actions,
            "complete data-node operational semantics evidence",
        );
    }
    for blocker in &rollout.blockers {
        require_bool(
            false,
            &format!("data_node:blocker:{blocker}"),
            satisfied,
            missing,
            blockers,
            actions,
            "clear data-node rollout blocker",
        );
    }
}

fn require_meta_rollout(
    rollout: Option<&RustRaftMetaProcessRolloutReport>,
    satisfied: &mut Vec<String>,
    missing: &mut Vec<String>,
    blockers: &mut Vec<String>,
    actions: &mut Vec<String>,
) {
    require_option(
        "metaserver:evidence_present",
        rollout,
        satisfied,
        missing,
        blockers,
        actions,
        "attach metaserver process rollout evidence",
    );
    let Some(rollout) = rollout else {
        return;
    };
    for (present, id, action) in [
        (
            rollout.ready,
            "metaserver:ready",
            "make metaserver rollout ready",
        ),
        (
            rollout.blockers.is_empty(),
            "metaserver:no_blockers",
            "clear metaserver rollout blockers",
        ),
        (
            !rollout.nodes.is_empty()
                && rollout.spawned_process_count as usize >= rollout.nodes.len(),
            "metaserver:processes_spawned",
            "spawn and observe all metaserver RustRaft processes",
        ),
        (
            !rollout.voters.is_empty(),
            "metaserver:voters_present",
            "run metaserver RustRaft with voter membership",
        ),
        (
            rollout.independent_wal_dirs,
            "metaserver:independent_wal_dirs",
            "use independent WAL dirs per metaserver process",
        ),
        (
            rollout.independent_snapshot_dirs,
            "metaserver:independent_snapshot_dirs",
            "use independent snapshot dirs per metaserver process",
        ),
        (
            rollout.mutation_proposed_through_process_api,
            "metaserver:process_mutation_path",
            "prove metaserver mutations enter through the process API",
        ),
        (
            rollout.read_index_responses_observed > 0 && rollout.read_index_validated,
            "metaserver:read_index",
            "validate metaserver read-index responses",
        ),
        (
            rollout.applied_raft_mutations > 0,
            "metaserver:applied_mutations",
            "observe applied metaserver RustRaft mutations",
        ),
        (
            rollout.scheduler_task_replay_validated,
            "metaserver:scheduler_replay",
            "validate scheduler task replay from RustRaft log",
        ),
        (
            rollout.data_node_membership_results_ready
                && rollout.data_node_membership_workflow_report_attached
                && rollout.data_node_raft_group_results_observed,
            "metaserver:data_node_membership_workflow",
            "validate data-node membership workflow through metaserver RustRaft",
        ),
        (
            rollout.failover_validated,
            "metaserver:failover",
            "validate metaserver failover",
        ),
        (
            rollout.membership_change_validated,
            "metaserver:membership_change",
            "validate metaserver membership change",
        ),
        (
            rollout.follower_lag_validated,
            "metaserver:follower_lag",
            "validate metaserver follower lag handling",
        ),
        (
            rollout.secondary_read_validated,
            "metaserver:secondary_read",
            "validate metaserver secondary read eligibility",
        ),
        (
            rollout.recovered_after_restart,
            "metaserver:restart_recovery",
            "validate metaserver restart recovery",
        ),
        (
            rollout.snapshot_install_validated,
            "metaserver:snapshot_install",
            "validate metaserver snapshot install",
        ),
        (
            rollout.multi_process_log_store_validated,
            "metaserver:multi_process_log_store",
            "validate independent metaserver log stores",
        ),
        (
            rollout.operational_semantics.proves_runtime_semantics(),
            "metaserver:operational_semantics",
            "prove metaserver runtime semantics, not only API presence",
        ),
    ] {
        require_bool(present, id, satisfied, missing, blockers, actions, action);
    }
    for missing_requirement in rollout.operational_semantics.missing_requirements() {
        require_bool(
            false,
            &format!("metaserver:semantics:{missing_requirement}"),
            satisfied,
            missing,
            blockers,
            actions,
            "complete metaserver operational semantics evidence",
        );
    }
    for blocker in &rollout.blockers {
        require_bool(
            false,
            &format!("metaserver:blocker:{blocker}"),
            satisfied,
            missing,
            blockers,
            actions,
            "clear metaserver rollout blocker",
        );
    }
}

fn require_membership_transitions(
    transitions: &[RustRaftMembershipTransitionEvidence],
    satisfied: &mut Vec<String>,
    missing: &mut Vec<String>,
    blockers: &mut Vec<String>,
    actions: &mut Vec<String>,
) {
    let report = rustraft_membership_readiness_report(transitions);
    if report.ready {
        satisfied.push("membership:all_required_transitions".to_string());
    } else {
        missing.extend(report.missing.iter().map(|id| format!("membership:{id}")));
        blockers.extend(report.missing.iter().map(|id| format!("membership:{id}")));
        actions.push(
            "run metaserver and data-node RustRaft failover, scale-up, and scale-down transitions"
                .to_string(),
        );
    }
    for id in report.satisfied {
        satisfied.push(format!("membership:{id}"));
    }
}

fn membership_transition_id(
    scope: RustRaftMembershipScope,
    transition: RustRaftMembershipTransitionKind,
) -> String {
    format!("{scope:?}:{transition:?}").to_lowercase()
}

fn majority_size(voters: usize) -> usize {
    voters / 2 + 1
}

fn readiness_field_present(snapshot: &RustRaftReadinessSnapshot, field: &str) -> bool {
    match field {
        "rustraft_leader_write_authority_present" => {
            snapshot.rustraft_leader_write_authority_present
        }
        "rustraft_operator_observability_present" => {
            snapshot.rustraft_operator_observability_present
        }
        "rustraft_rpc_transport_contract_present" => {
            snapshot.rustraft_rpc_transport_contract_present
        }
        "rustraft_log_retention_snapshot_trigger_present" => {
            snapshot.rustraft_log_retention_snapshot_trigger_present
        }
        "rustraft_apply_snapshot_fence_present" => snapshot.rustraft_apply_snapshot_fence_present,
        "raft_storage_apply_fence_present" => snapshot.raft_storage_apply_fence_present,
        "rustraft_snapshot_floor_log_matching_present" => {
            snapshot.rustraft_snapshot_floor_log_matching_present
        }
        "rustraft_snapshot_tail_catchup_present" => snapshot.rustraft_snapshot_tail_catchup_present,
        "rustraft_compacted_entry_rejection_present" => {
            snapshot.rustraft_compacted_entry_rejection_present
        }
        "rustraft_metaserver_snapshot_floor_election_present" => {
            snapshot.rustraft_metaserver_snapshot_floor_election_present
        }
        "learner_catchup_promotion_present" => snapshot.learner_catchup_promotion_present,
        "metaserver_membership_workflow_present" => snapshot.metaserver_membership_workflow_present,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ready_snapshot() -> RustRaftReadinessSnapshot {
        RustRaftReadinessSnapshot {
            rustraft_leader_write_authority_present: true,
            rustraft_operator_observability_present: true,
            rustraft_rpc_transport_contract_present: true,
            rustraft_log_retention_snapshot_trigger_present: true,
            rustraft_apply_snapshot_fence_present: true,
            raft_storage_apply_fence_present: true,
            rustraft_snapshot_floor_log_matching_present: true,
            rustraft_snapshot_tail_catchup_present: true,
            rustraft_compacted_entry_rejection_present: true,
            rustraft_metaserver_snapshot_floor_election_present: true,
            learner_catchup_promotion_present: true,
            metaserver_membership_workflow_present: true,
        }
    }

    fn ready_operational_semantics() -> RustRaftProcessOperationalSemanticsEvidence {
        RustRaftProcessOperationalSemanticsEvidence {
            api_presence_only_rejected: true,
            process_path_validated: true,
            read_index_validated: true,
            leader_lease_validated: true,
            stale_leader_lease_rejection_observed: true,
            follower_lease_expiration_observed: true,
            lagging_follower_read_rejected: true,
            bounded_stale_read_acceptance_observed: true,
            bounded_stale_read_rejection_observed: true,
            minority_partition_read_rejection_observed: true,
            healed_follower_catchup_observed: true,
            stale_follower_write_rejected: true,
            leader_transfer_exact_once_validated: true,
            leader_transfer_under_load_validated: true,
            snapshot_bootstrap_validated: true,
            snapshot_install_restart_validated: true,
            membership_rescale_validated: true,
            membership_add_promote_remove_validated: true,
            follower_rejoin_after_compaction_validated: true,
            secondary_read_eligibility_validated: true,
            apply_pipeline_converged: true,
            wal_persistence_observed: true,
            fsm_apply_idempotent_replay_observed: true,
            storage_mutation_wal_fence_atomicity_observed: true,
            snapshot_install_apply_fence_atomicity_observed: true,
            process_restart_after_apply_crash_recovered: true,
            ready: true,
            blockers: Vec::new(),
        }
    }

    fn ready_process_nodes() -> Vec<RustRaftProcessNodeEvidence> {
        vec![
            RustRaftProcessNodeEvidence {
                node_id: 1,
                addr: "127.0.0.1:19001".to_string(),
                wal_dir: "/tmp/rustraft/node1/wal".to_string(),
                snapshot_dir: "/tmp/rustraft/node1/snapshots".to_string(),
                commit_index: 42,
                applied_index: 42,
                snapshot_id: Some("snap-40".to_string()),
                restarted: true,
                log_store_validated: true,
            },
            RustRaftProcessNodeEvidence {
                node_id: 2,
                addr: "127.0.0.1:19002".to_string(),
                wal_dir: "/tmp/rustraft/node2/wal".to_string(),
                snapshot_dir: "/tmp/rustraft/node2/snapshots".to_string(),
                commit_index: 42,
                applied_index: 42,
                snapshot_id: Some("snap-40".to_string()),
                restarted: true,
                log_store_validated: true,
            },
        ]
    }

    fn ready_data_node_rollout() -> RustRaftDataNodeProcessRolloutReport {
        RustRaftDataNodeProcessRolloutReport {
            shard_id: 7,
            voters: vec![1, 2, 3],
            learners: vec![4],
            nodes: ready_process_nodes(),
            spawned_process_count: 2,
            independent_wal_dirs: true,
            independent_snapshot_dirs: true,
            observed_process_requests: 16,
            read_index_responses_observed: 8,
            restarted_node_count: 2,
            per_node_log_store_inspection_count: 2,
            write_proposed_through_process_api: true,
            leader_transfer_validated: true,
            failover_validated: true,
            secondary_lag_observed: true,
            lagging_follower_read_rejection_observed: true,
            stale_follower_write_rejection_observed: true,
            catchup_read_eligibility_observed: true,
            minority_partition_rejection_observed: true,
            bounded_stale_read_eligibility_observed: true,
            healed_follower_catchup_observed: true,
            lagging_follower_observed_lag: 3,
            membership_change_validated: true,
            follower_lag_validated: true,
            secondary_read_validated: true,
            recovered_after_restart: true,
            restart_recovery_validated: true,
            snapshot_install_validated: true,
            applied_fence_validated: true,
            crash_after_storage_mutation_recovered: true,
            crash_after_wal_persist_recovered: true,
            crash_during_snapshot_install_recovered: true,
            apply_fence_recovered_after_restart: true,
            multi_process_log_store_validated: true,
            operational_semantics: ready_operational_semantics(),
            ready: true,
            blockers: Vec::new(),
        }
    }

    fn ready_meta_rollout() -> RustRaftMetaProcessRolloutReport {
        RustRaftMetaProcessRolloutReport {
            voters: vec![1, 2, 3],
            learners: vec![4],
            nodes: ready_process_nodes(),
            spawned_process_count: 2,
            independent_wal_dirs: true,
            independent_snapshot_dirs: true,
            observed_process_requests: 20,
            read_index_responses_observed: 10,
            restarted_node_count: 2,
            per_node_log_store_inspection_count: 2,
            mutation_proposed_through_process_api: true,
            applied_raft_mutations: 12,
            generated_scheduler_tasks: 4,
            scheduler_retries: 1,
            stale_scheduler_token_rejected: true,
            data_node_membership_results_ready: true,
            scheduler_mutations_proposed_through_process_api: true,
            scheduler_task_replay_from_raft_log_observed: true,
            membership_mutations_proposed_through_process_api: true,
            data_node_membership_workflow_report_attached: true,
            data_node_raft_group_results_observed: true,
            failover_validated: true,
            membership_change_validated: true,
            follower_lag_validated: true,
            secondary_read_validated: true,
            read_index_validated: true,
            snapshot_install_validated: true,
            recovered_after_restart: true,
            scheduler_task_replay_validated: true,
            crash_after_meta_mutation_recovered: true,
            crash_after_meta_wal_persist_recovered: true,
            crash_during_meta_snapshot_install_recovered: true,
            meta_apply_fence_recovered_after_restart: true,
            multi_process_log_store_validated: true,
            operational_semantics: ready_operational_semantics(),
            ready: true,
            blockers: Vec::new(),
        }
    }

    fn membership_transition(
        scope: RustRaftMembershipScope,
        transition: RustRaftMembershipTransitionKind,
    ) -> RustRaftMembershipTransitionEvidence {
        match transition {
            RustRaftMembershipTransitionKind::Failover => RustRaftMembershipTransitionEvidence {
                scope,
                transition,
                before_voters: vec![1, 2, 3],
                after_voters: vec![1, 2, 3],
                before_learners: Vec::new(),
                after_learners: Vec::new(),
                leader_before: Some(1),
                leader_after: Some(2),
                failed_or_removed_nodes: vec![1],
                added_nodes: Vec::new(),
                caught_up_nodes: vec![2, 3],
                commit_index_before: 100,
                commit_index_after: 104,
                applied_index_after: 104,
                joint_consensus_used: false,
                old_majority_preserved: true,
                new_majority_reached: true,
                stale_leader_rejected: true,
                read_index_validated_after: true,
                write_validated_after: true,
                snapshot_floor_preserved: true,
                secondary_replication_visible: true,
                scheduler_generation_advanced: matches!(scope, RustRaftMembershipScope::Metaserver),
                blockers: Vec::new(),
            },
            RustRaftMembershipTransitionKind::ScaleUp => RustRaftMembershipTransitionEvidence {
                scope,
                transition,
                before_voters: vec![1, 2, 3],
                after_voters: vec![1, 2, 3, 4],
                before_learners: vec![4],
                after_learners: Vec::new(),
                leader_before: Some(1),
                leader_after: Some(1),
                failed_or_removed_nodes: Vec::new(),
                added_nodes: vec![4],
                caught_up_nodes: vec![4],
                commit_index_before: 100,
                commit_index_after: 108,
                applied_index_after: 108,
                joint_consensus_used: true,
                old_majority_preserved: true,
                new_majority_reached: true,
                stale_leader_rejected: true,
                read_index_validated_after: true,
                write_validated_after: true,
                snapshot_floor_preserved: true,
                secondary_replication_visible: true,
                scheduler_generation_advanced: matches!(scope, RustRaftMembershipScope::Metaserver),
                blockers: Vec::new(),
            },
            RustRaftMembershipTransitionKind::ScaleDown => RustRaftMembershipTransitionEvidence {
                scope,
                transition,
                before_voters: vec![1, 2, 3, 4],
                after_voters: vec![1, 2, 3],
                before_learners: Vec::new(),
                after_learners: Vec::new(),
                leader_before: Some(1),
                leader_after: Some(1),
                failed_or_removed_nodes: vec![4],
                added_nodes: Vec::new(),
                caught_up_nodes: vec![1, 2, 3],
                commit_index_before: 108,
                commit_index_after: 112,
                applied_index_after: 112,
                joint_consensus_used: true,
                old_majority_preserved: true,
                new_majority_reached: true,
                stale_leader_rejected: true,
                read_index_validated_after: true,
                write_validated_after: true,
                snapshot_floor_preserved: true,
                secondary_replication_visible: true,
                scheduler_generation_advanced: matches!(scope, RustRaftMembershipScope::Metaserver),
                blockers: Vec::new(),
            },
        }
    }

    fn ready_membership_transitions() -> Vec<RustRaftMembershipTransitionEvidence> {
        [
            RustRaftMembershipScope::Metaserver,
            RustRaftMembershipScope::DataNode,
        ]
        .into_iter()
        .flat_map(|scope| {
            [
                RustRaftMembershipTransitionKind::Failover,
                RustRaftMembershipTransitionKind::ScaleUp,
                RustRaftMembershipTransitionKind::ScaleDown,
            ]
            .into_iter()
            .map(move |transition| membership_transition(scope, transition))
        })
        .collect()
    }

    fn ready_production_input() -> RustRaftProductionReadinessInput {
        RustRaftProductionReadinessInput {
            readiness: ready_snapshot(),
            peer_pipeline: Some(RustRaftPipelineEvidence {
                per_peer_pipeline_state_present: true,
                append_backpressure_enforced: true,
                apply_backpressure_enforced: true,
                memory_replicate_bytes_enforced: true,
                oversized_log_rejection_present: true,
                out_of_order_append_handling_present: true,
                reorder_timeout_drop_present: true,
                stale_term_rejection_present: true,
                reorder_queue_enabled: true,
            }),
            snapshot_lifecycle: Some(RustRaftSnapshotLifecycleEvidence {
                sender_lifecycle_present: true,
                downloader_lifecycle_present: true,
                retry_backpressure_present: true,
                chunk_retry_present: true,
                send_timeout_present: true,
                rate_limit_present: true,
                install_progress_present: true,
                install_rollback_present: true,
                membership_change_present: true,
                rejoin_after_compacted_log_present: true,
            }),
            wal_lifecycle: Some(RustRaftWalLifecycleEvidence {
                segment_lifecycle_present: true,
                retained_range_present: true,
                sequence_range_present: true,
                log_index_range_present: true,
                compaction_observed: true,
                slow_fsync_backpressure_observed: true,
            }),
            data_node_rollout: Some(ready_data_node_rollout()),
            metaserver_rollout: Some(ready_meta_rollout()),
            membership_transitions: ready_membership_transitions(),
        }
    }

    #[test]
    fn contract_is_openraft_free_and_complete() {
        let contract = rustraft_parity_contract();
        assert!(contract.openraft_dependency_removed);
        assert_eq!(contract.requirements.len(), 12);
    }

    #[test]
    fn crate_readme_documents_open_source_contract_surface() {
        let readme = include_str!("../README.md");
        assert!(readme.contains("RustRaft"));
        assert!(readme.contains("rustraft_parity_report"));
        assert!(readme.contains("rustraft_production_readiness_report"));
        assert!(readme.contains("OpenRaft-free"));
        assert!(readme.contains("Apache-2.0"));
    }

    #[test]
    fn report_fails_closed() {
        let mut snapshot = ready_snapshot();
        snapshot.raft_storage_apply_fence_present = false;
        let report = rustraft_parity_report(&snapshot);
        assert!(!report.ready);
        assert_eq!(report.production_status, RustRaftProductionStatus::Blocked);
        assert_eq!(report.missing, vec!["storage_apply_fence".to_string()]);
    }

    #[test]
    fn production_readiness_gate_accepts_complete_evidence() {
        let report = rustraft_production_readiness_report(&ready_production_input());
        assert!(report.ready, "{report:#?}");
        assert_eq!(
            report.production_status,
            RustRaftProductionStatus::ProductionReady
        );
        assert!(report.missing.is_empty());
        assert!(report.production_blockers.is_empty());
        assert_eq!(report.public_api.storage_trait, "RustRaftStorage");
    }

    #[test]
    fn production_readiness_gate_fails_closed_without_runtime_evidence() {
        let report = rustraft_production_readiness_report(&RustRaftProductionReadinessInput {
            readiness: ready_snapshot(),
            peer_pipeline: None,
            snapshot_lifecycle: None,
            wal_lifecycle: None,
            data_node_rollout: None,
            metaserver_rollout: None,
            membership_transitions: Vec::new(),
        });
        assert!(!report.ready);
        assert_eq!(report.production_status, RustRaftProductionStatus::Blocked);
        assert!(report
            .missing
            .contains(&"pipeline:evidence_present".to_string()));
        assert!(report
            .missing
            .contains(&"snapshot:evidence_present".to_string()));
        assert!(report.missing.contains(&"wal:evidence_present".to_string()));
        assert!(report
            .missing
            .contains(&"data_node:evidence_present".to_string()));
        assert!(report
            .missing
            .contains(&"metaserver:evidence_present".to_string()));
        assert!(report
            .missing
            .iter()
            .any(|item| item == "membership:datanode:scaledown:evidence_present"));
    }

    #[test]
    fn production_readiness_gate_reports_specific_wal_blocker() {
        let mut input = ready_production_input();
        input.wal_lifecycle = Some(RustRaftWalLifecycleEvidence {
            segment_lifecycle_present: true,
            retained_range_present: true,
            sequence_range_present: true,
            log_index_range_present: true,
            compaction_observed: false,
            slow_fsync_backpressure_observed: true,
        });
        let report = rustraft_production_readiness_report(&input);
        assert!(!report.ready);
        assert!(report.missing.contains(&"wal:compaction".to_string()));
        assert!(report
            .recommended_next_actions
            .contains(&"prove WAL compaction/released segment behavior".to_string()));
    }

    #[test]
    fn safety_helpers_accept_healthy_state() {
        let status = RustRaftStatusSnapshot {
            group_id: 1,
            node_id: 1,
            role: RustRaftRole::Leader,
            term: 2,
            leader_id: Some(1),
            commit_index: 10,
            applied_index: 10,
            last_log_index: 10,
            last_snapshot_index: 4,
            peers: vec![RustRaftPeerStatus {
                node_id: 2,
                matched: 10,
                next_index: 11,
                learner: true,
                healthy: true,
                lag: 0,
            }],
        };
        assert!(
            rustraft_read_safety_decision(
                &status,
                &RustRaftReadIndexRequest {
                    group_id: 1,
                    requester_id: 1,
                    min_commit_index: 10,
                    allow_lease_read: true,
                },
            )
            .safe
        );
        assert!(rustraft_learner_promotion_decision(&status, 2, 0).promotable);
    }

    #[test]
    fn membership_readiness_requires_failover_scale_up_and_scale_down_for_meta_and_data_nodes() {
        let report = rustraft_membership_readiness_report(&ready_membership_transitions());
        assert!(report.ready, "{report:#?}");
        assert!(report
            .satisfied
            .contains(&"metaserver:failover".to_string()));
        assert!(report.satisfied.contains(&"metaserver:scaleup".to_string()));
        assert!(report
            .satisfied
            .contains(&"metaserver:scaledown".to_string()));
        assert!(report.satisfied.contains(&"datanode:failover".to_string()));
        assert!(report.satisfied.contains(&"datanode:scaleup".to_string()));
        assert!(report.satisfied.contains(&"datanode:scaledown".to_string()));
    }

    #[test]
    fn membership_readiness_fails_closed_when_transition_evidence_is_missing() {
        let transitions = ready_membership_transitions()
            .into_iter()
            .filter(|item| {
                !(item.scope == RustRaftMembershipScope::DataNode
                    && item.transition == RustRaftMembershipTransitionKind::ScaleDown)
            })
            .collect::<Vec<_>>();
        let report = rustraft_membership_readiness_report(&transitions);
        assert!(!report.ready);
        assert!(report
            .missing
            .contains(&"datanode:scaledown:evidence_present".to_string()));
    }

    #[test]
    fn membership_readiness_rejects_unsafe_scale_up_without_joint_consensus() {
        let mut transition = membership_transition(
            RustRaftMembershipScope::Metaserver,
            RustRaftMembershipTransitionKind::ScaleUp,
        );
        transition.joint_consensus_used = false;
        let missing = rustraft_membership_transition_missing(&transition);
        assert!(missing.contains(&"joint_consensus_used".to_string()));
    }
}
