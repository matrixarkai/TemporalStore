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
    pub snapshot_sending: bool,
    pub snapshot_installing: bool,
    pub snapshot_installed_index: u64,
    pub snapshot_send_attempts: u64,
    pub snapshot_install_total_chunks: u64,
    pub snapshot_install_progress_per_mille: u64,
    pub snapshot_backpressure_rejections: u64,
    pub snapshot_rate_limit_rejections: u64,
    pub snapshot_install_rolled_back: u64,
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
    pub reorder_queue_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftSnapshotLifecycleEvidence {
    pub sender_lifecycle_present: bool,
    pub downloader_lifecycle_present: bool,
    pub retry_backpressure_present: bool,
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

    #[test]
    fn contract_is_openraft_free_and_complete() {
        let contract = rustraft_parity_contract();
        assert!(contract.openraft_dependency_removed);
        assert_eq!(contract.requirements.len(), 12);
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
}
