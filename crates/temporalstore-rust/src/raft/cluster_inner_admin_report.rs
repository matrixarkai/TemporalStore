//! RaftClusterInner::matrixraft_runtime_admin_report, split from raft.rs.
use super::*;

impl RaftClusterInner {
    pub(super) fn matrixraft_runtime_admin_report(&self) -> MatrixRaftRuntimeAdminReport {
        let status = self.status();
        let leader_log = self
            .nodes
            .get(&self.leader_id)
            .map(|node| node.log.as_slice())
            .unwrap_or_default();
        let peer_pipeline_states = self
            .nodes
            .values()
            .map(|node| {
                let mut pipeline = node.pipeline_state.clone();
                pipeline.snapshot_installing = pipeline.snapshot_installing
                    || self
                        .pending_snapshots
                        .keys()
                        .any(|(target_id, _)| *target_id == node.id);
                if pipeline.next_index == 0 {
                    pipeline.next_index = node_next_log_index(node);
                }
                if pipeline.inflight_entries == 0 && node.commit_index < status.commit_index {
                    pipeline.inflight_entries =
                        status.commit_index.saturating_sub(node.commit_index);
                    pipeline.inflight_bytes = leader_log
                        .iter()
                        .filter(|entry| entry.index > node.commit_index)
                        .map(|entry| command_size_bytes(&entry.command))
                        .sum();
                    pipeline.append_queue_depth = pipeline.inflight_entries;
                }
                if node.alive {
                    pipeline.offline_since_ms = None;
                    pipeline.offline_elapsed_ms = 0;
                    pipeline.offline_timeout_reached = false;
                } else if let Some(offline_since_ms) = pipeline.offline_since_ms {
                    pipeline.offline_elapsed_ms =
                        self.logical_time_ms.saturating_sub(offline_since_ms);
                    let reached = self.config.offline_timeout_tick > 0
                        && pipeline.offline_elapsed_ms >= self.config.offline_timeout_tick;
                    pipeline.offline_timeout_reached = reached;
                }
                if pipeline.transfer_leader_target {
                    if let Some(started_ms) = pipeline.transfer_leader_started_ms {
                        pipeline.transfer_leader_elapsed_ms =
                            self.logical_time_ms.saturating_sub(started_ms);
                    }
                } else {
                    pipeline.transfer_leader_elapsed_ms = 0;
                }
                MatrixRaftPeerPipelineState {
                    peer_id: node.id,
                    role: node.role,
                    replica_role: node.replica_role,
                    match_index: pipeline.match_index,
                    next_index: pipeline.next_index,
                    append_requests: pipeline.append_requests,
                    append_accepted: pipeline.append_accepted,
                    append_rejected: pipeline.append_rejected,
                    inflight_entries: pipeline.inflight_entries,
                    inflight_bytes: pipeline.inflight_bytes,
                    append_queue_depth: pipeline.append_queue_depth,
                    append_queue_limit: self.config.max_inflights_replicate,
                    inflight_bytes_limit: self.config.max_memory_replicate_log_bytes,
                    apply_inflight_tasks: pipeline.apply_inflight_tasks,
                    apply_inflight_limit: self.config.max_inflights_apply_task,
                    apply_queue_depth: pipeline.apply_queue_depth,
                    apply_queue_max_depth: pipeline.apply_queue_max_depth,
                    apply_batch_bytes_limit: self.config.max_apply_batch_bytes,
                    apply_backpressure_rejections: pipeline.apply_backpressure_rejections,
                    memory_backpressure_rejections: pipeline.memory_backpressure_rejections,
                    oversized_log_rejections: pipeline.oversized_log_rejections,
                    append_queue_max_depth: pipeline.append_queue_max_depth,
                    reorder_queue_depth: pipeline.reorder_queue_depth,
                    out_of_order_append_rejections: pipeline.out_of_order_append_rejections,
                    reorder_entries_accepted: pipeline.reorder_entries_accepted,
                    reorder_entries_released: pipeline.reorder_entries_released,
                    reorder_entries_rejected: pipeline.reorder_entries_rejected,
                    reorder_entry_timeouts: pipeline.reorder_entry_timeouts,
                    reorder_dropped_packages: pipeline.reorder_dropped_packages,
                    stale_term_rejections: pipeline.stale_term_rejections,
                    snapshot_sending: pipeline.snapshot_sending,
                    snapshot_installing: pipeline.snapshot_installing,
                    snapshot_installed_index: pipeline.snapshot_installed_index,
                    snapshot_send_attempts: pipeline.snapshot_send_attempts,
                    snapshot_send_completed: pipeline.snapshot_send_completed,
                    snapshot_send_failed: pipeline.snapshot_send_failed,
                    snapshot_install_started: pipeline.snapshot_install_started,
                    snapshot_install_completed: pipeline.snapshot_install_completed,
                    snapshot_install_rejected: pipeline.snapshot_install_rejected,
                    snapshot_install_rolled_back: pipeline.snapshot_install_rolled_back,
                    snapshot_install_received_chunks: pipeline.snapshot_install_received_chunks,
                    snapshot_install_total_chunks: pipeline.snapshot_install_total_chunks,
                    snapshot_install_progress_per_mille: pipeline
                        .snapshot_install_progress_per_mille,
                    snapshot_retry_count: pipeline.snapshot_retry_count,
                    snapshot_chunk_retry_count: pipeline.snapshot_chunk_retry_count,
                    snapshot_backpressure_rejections: pipeline.snapshot_backpressure_rejections,
                    snapshot_rate_limit_rejections: pipeline.snapshot_rate_limit_rejections,
                    snapshot_send_elapsed_ms: pipeline.snapshot_send_elapsed_ms,
                    snapshot_send_timeouts: pipeline.snapshot_send_timeouts,
                    snapshot_during_membership_change: pipeline.snapshot_during_membership_change,
                    snapshot_rejoin_after_compacted_log: pipeline
                        .snapshot_rejoin_after_compacted_log,
                    transfer_leader_target: pipeline.transfer_leader_target,
                    transfer_leader_requests: pipeline.transfer_leader_requests,
                    transfer_leader_accepted: pipeline.transfer_leader_accepted,
                    transfer_leader_rejected: pipeline.transfer_leader_rejected,
                    transfer_leader_completed: pipeline.transfer_leader_completed,
                    transfer_leader_elapsed_ms: pipeline.transfer_leader_elapsed_ms,
                    transfer_leader_timeouts: pipeline.transfer_leader_timeouts,
                    pre_vote_rejections: pipeline.pre_vote_rejections,
                    election_rejections: pipeline.election_rejections,
                    offline_elapsed_ms: pipeline.offline_elapsed_ms,
                    offline_timeout_reached: pipeline.offline_timeout_reached,
                    offline_timeout_rejections: pipeline.offline_timeout_rejections,
                    auto_promoted_from_learner: pipeline.auto_promoted_from_learner,
                }
            })
            .collect::<Vec<_>>();

        let stale_follower_read_rejected = status.nodes.iter().any(|node| {
            node.node_id != self.leader_id
                && node.replica_role.can_serve_data()
                && node.lag > 0
                && node.alive
        }) || self.read_safety_state.read_index_rejected > 0;
        let stale_follower_write_rejected = status
            .nodes
            .iter()
            .any(|node| node.node_id != self.leader_id && node.alive)
            && self.read_safety_state.stale_follower_write_rejected > 0;
        let stale_leader_lease_rejected = self.read_safety_state.stale_leader_lease_rejected > 0;
        let lagging_follower_read_rejected =
            self.read_safety_state.lagging_follower_read_rejected > 0;
        let bounded_stale_read_accepted = self.read_safety_state.bounded_stale_read_accepted > 0;
        let bounded_stale_read_rejected = self.read_safety_state.bounded_stale_read_rejected > 0;
        let minority_partition_rejected_reads =
            self.read_safety_state.minority_partition_read_rejected > 0;
        let minority_partition_rejected_writes =
            self.read_safety_state.minority_partition_write_rejected > 0;
        let healed_follower_caught_up = self.read_safety_state.healed_follower_catchup_observed > 0;
        let witness_membership_present = self
            .nodes
            .values()
            .any(|node| matches!(node.replica_role, RaftReplicaRole::Witness));
        let witness_role_behavior_present = self.nodes.values().any(|node| {
            matches!(node.replica_role, RaftReplicaRole::Witness)
                && node.replica_role.participates_in_quorum()
                && !node.replica_role.can_serve_data()
                && !node.replica_role.can_be_leader()
        });
        let learner_auto_promote_present = self
            .nodes
            .values()
            .any(|node| node.pipeline_state.auto_promoted_from_learner);
        let pending_joint_consensus_present = self.joint_membership.is_some();
        let membership_evidence = self.membership_evidence.clone();
        let learner_add_present = membership_evidence.learner_add_count > 0;
        let learner_catchup_present = membership_evidence.learner_catchup_count > 0;
        let learner_promote_present = membership_evidence.learner_promote_count > 0;
        let voter_remove_present = membership_evidence.voter_remove_count > 0;
        let unique_leader_transfer_commit_ids = membership_evidence
            .leader_transfer_exact_once_commit_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len() as u64;
        let leader_transfer_exact_once_present = membership_evidence.leader_transfer_write_count
            > 0
            && membership_evidence.leader_transfer_exact_once_commit_count
                >= membership_evidence.leader_transfer_write_count
            && unique_leader_transfer_commit_ids >= membership_evidence.leader_transfer_write_count
            && unique_leader_transfer_commit_ids
                == membership_evidence
                    .leader_transfer_exact_once_commit_ids
                    .len() as u64;
        let pending_joint_consensus_restart_present =
            membership_evidence.pending_joint_consensus_persist_count > 0
                && membership_evidence.pending_joint_consensus_restore_count > 0;
        let read_index_validated = status.leader_lease_valid && status.has_majority;
        let lease_read_validated =
            self.config.lease_duration_ms > 0 || self.config.assume_lease_when_start;
        let matrixraft_peer_pipeline_states = peer_pipeline_states
            .iter()
            .map(MatrixRaftPeerPipelineState::to_matrixraft_peer_pipeline_status)
            .collect::<Vec<_>>();
        let matrixraft_pipeline_evidence = matrixraft_pipeline_evidence(
            &matrixraft_peer_pipeline_states,
            MatrixRaftPipelineLimits {
                max_inflights_replicate: self.config.max_inflights_replicate,
                max_memory_replicate_log_bytes: self.config.max_memory_replicate_log_bytes,
                max_inflights_apply_task: self.config.max_inflights_apply_task,
                max_apply_batch_bytes: self.config.max_apply_batch_bytes,
                enable_reorder_queue: self.config.enable_reorder_queue,
                reorder_window_size: self.config.reorder_window_size,
                reorder_timeout_us: self.config.reorder_timeout_us,
            },
        );
        let matrixraft_snapshot_evidence = matrixraft_snapshot_lifecycle_evidence(
            &matrixraft_peer_pipeline_states,
            self.config.send_snapshot_timeout_ms,
            self.config.max_inflights_replicate,
        );
        let append_backpressure_enforced =
            matrixraft_pipeline_evidence.append_backpressure_enforced;
        let apply_backpressure_enforced = matrixraft_pipeline_evidence.apply_backpressure_enforced;
        let memory_replicate_bytes_enforced =
            matrixraft_pipeline_evidence.memory_replicate_bytes_enforced;
        let oversized_log_rejection_present =
            matrixraft_pipeline_evidence.oversized_log_rejection_present;
        let out_of_order_append_handling_present =
            matrixraft_pipeline_evidence.out_of_order_append_handling_present;
        let reorder_timeout_drop_present =
            matrixraft_pipeline_evidence.reorder_timeout_drop_present;
        let stale_term_rejection_present =
            matrixraft_pipeline_evidence.stale_term_rejection_present;
        let reorder_queue_enabled = matrixraft_pipeline_evidence.reorder_queue_enabled;
        let snapshot_sender_lifecycle_present =
            matrixraft_snapshot_evidence.sender_lifecycle_present;
        let snapshot_downloader_lifecycle_present =
            matrixraft_snapshot_evidence.downloader_lifecycle_present;
        let snapshot_retry_backpressure_present =
            matrixraft_snapshot_evidence.retry_backpressure_present;
        let snapshot_chunk_retry_present = matrixraft_snapshot_evidence.chunk_retry_present;
        let snapshot_send_timeout_present = matrixraft_snapshot_evidence.send_timeout_present;
        let snapshot_rate_limit_present = matrixraft_snapshot_evidence.rate_limit_present;
        let snapshot_install_progress_present =
            matrixraft_snapshot_evidence.install_progress_present;
        let snapshot_install_rollback_present =
            matrixraft_snapshot_evidence.install_rollback_present;
        let snapshot_membership_change_present =
            matrixraft_snapshot_evidence.membership_change_present;
        let snapshot_rejoin_after_compacted_log_present =
            matrixraft_snapshot_evidence.rejoin_after_compacted_log_present;
        let (
            wal_segment_count,
            wal_active_segment_id,
            wal_first_retained_segment_id,
            wal_last_retained_segment_id,
            wal_total_bytes,
            wal_active_segment_bytes,
            wal_total_records,
            wal_first_sequence,
            wal_last_sequence,
            wal_first_log_index,
            wal_last_log_index,
            wal_released_segment_count,
            wal_slow_fsync_backpressure_observed,
        ) = self
            .wal
            .as_ref()
            .and_then(|wal| wal.segment_report(self.shard_id, self.leader_id).ok())
            .map(|report| {
                let first = report
                    .segments
                    .first()
                    .map(|segment| segment.segment_id)
                    .unwrap_or_default();
                let last = report
                    .segments
                    .last()
                    .map(|segment| segment.segment_id)
                    .unwrap_or_default();
                let total_bytes = report.segments.iter().map(|segment| segment.bytes).sum();
                let active_bytes = report
                    .segments
                    .iter()
                    .find(|segment| segment.segment_id == report.active_segment_id)
                    .map(|segment| segment.bytes)
                    .unwrap_or_default();
                let total_records = report
                    .segments
                    .iter()
                    .map(|segment| segment.record_count)
                    .sum();
                let first_sequence = report
                    .segments
                    .iter()
                    .find_map(|segment| {
                        (segment.first_sequence > 0).then_some(segment.first_sequence)
                    })
                    .unwrap_or_default();
                let last_sequence = report
                    .segments
                    .iter()
                    .rev()
                    .find_map(|segment| {
                        (segment.last_sequence > 0).then_some(segment.last_sequence)
                    })
                    .unwrap_or_default();
                (
                    report.segments.len() as u64,
                    report.active_segment_id,
                    first,
                    last,
                    total_bytes,
                    active_bytes,
                    total_records,
                    first_sequence,
                    last_sequence,
                    report.first_retained_log_index,
                    report.last_retained_log_index,
                    report.released_segment_count,
                    report.slow_fsync_backpressure_observed,
                )
            })
            .unwrap_or((0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false));
        let matrixraft_wal_evidence =
            matrixraft_wal_lifecycle_evidence(&MatrixRaftWalLifecycleStatus {
                segment_count: wal_segment_count,
                active_segment_id: wal_active_segment_id,
                first_retained_segment_id: wal_first_retained_segment_id,
                last_retained_segment_id: wal_last_retained_segment_id,
                total_bytes: wal_total_bytes,
                active_segment_bytes: wal_active_segment_bytes,
                total_records: wal_total_records,
                first_sequence: wal_first_sequence,
                last_sequence: wal_last_sequence,
                first_log_index: wal_first_log_index,
                last_log_index: wal_last_log_index,
                released_segment_count: wal_released_segment_count,
                slow_fsync_backpressure_observed: wal_slow_fsync_backpressure_observed,
                slow_fsync_threshold_ms: 0,
                slow_fsync_count: 0,
                consecutive_slow_fsync_count: 0,
                max_fsync_elapsed_ms: 0,
                compacted_after_slow_fsync_count: 0,
            });
        let wal_segment_lifecycle_present = matrixraft_wal_evidence.segment_lifecycle_present;
        let pre_vote_enforced = self.config.enable_pre_vote;
        let pre_vote_process_evidence_observed = self.config.enable_pre_vote
            && self.read_safety_state.pre_vote_requests > 0
            && (self.read_safety_state.pre_vote_accepted > 0
                || self.read_safety_state.pre_vote_rejected > 0)
            && peer_pipeline_states
                .iter()
                .any(|peer| peer.pre_vote_rejections > 0);
        let election_prohibition_observed = self.config.prohibits_election
            && peer_pipeline_states
                .iter()
                .any(|peer| peer.election_rejections > 0);
        let offline_timeout_observed = self.config.offline_timeout_tick > 0
            && peer_pipeline_states
                .iter()
                .any(|peer| peer.offline_timeout_reached || peer.offline_timeout_rejections > 0);
        let transfer_timeout_observed = self.config.transfer_timeout_tick > 0
            && peer_pipeline_states
                .iter()
                .any(|peer| peer.transfer_leader_timeouts > 0);
        let election_controls_enforced = pre_vote_process_evidence_observed
            && election_prohibition_observed
            && offline_timeout_observed
            && transfer_timeout_observed;
        let quorum_peer_ids = peer_pipeline_states
            .iter()
            .filter(|peer| peer.replica_role.participates_in_quorum())
            .map(|peer| peer.peer_id)
            .collect::<Vec<_>>();
        let admin_status_surface_evidence =
            matrixraft_admin_status_surface_evidence(&MatrixRaftAdminStatusSurfaceInput {
                commit_index: status.commit_index,
                max_observed_node_commit_index: status
                    .nodes
                    .iter()
                    .map(|node| node.commit_index)
                    .max()
                    .unwrap_or_default(),
                quorum_size: status.majority as u64,
                quorum_peer_ids,
                peer_pipeline: matrixraft_peer_pipeline_states.clone(),
                wal_last_log_index,
                wal_segment_lifecycle_present,
            });
        let quorum_peer_progress_observed =
            admin_status_surface_evidence.quorum_peer_progress_observed;
        let peer_pipeline_runtime_activity_observed =
            admin_status_surface_evidence.peer_pipeline_runtime_activity_observed;
        let peer_pipeline_limits_observed =
            admin_status_surface_evidence.peer_pipeline_limits_observed;
        let admin_status_surface_complete = admin_status_surface_evidence.complete;
        let per_peer_pipeline_state_present =
            matrixraft_pipeline_evidence.per_peer_pipeline_state_present;
        let capability_matrix = vec![
            MatrixRaftCapabilityEvidence {
                capability: "per_peer_replication_pipeline_state".to_string(),
                ready: per_peer_pipeline_state_present
                    && append_backpressure_enforced
                    && apply_backpressure_enforced
                    && memory_replicate_bytes_enforced
                    && oversized_log_rejection_present,
                evidence_field: "peer_pipeline_states[*].{match_index,next_index,inflight_bytes,inflight_bytes_limit,append_queue_depth,append_queue_limit,append_queue_max_depth,append_*,apply_inflight_limit,apply_queue_depth,apply_queue_max_depth,apply_batch_bytes_limit,apply_backpressure_rejections,memory_backpressure_rejections,oversized_log_rejections}".to_string(),
                detail: format!(
                    "{} peers reported; append_backpressure={append_backpressure_enforced}; apply_backpressure={apply_backpressure_enforced}; memory_bytes={memory_replicate_bytes_enforced}; oversized={oversized_log_rejection_present}",
                    peer_pipeline_states.len()
                ),
            },
            MatrixRaftCapabilityEvidence {
                capability: "reorder_queue_runtime".to_string(),
                ready: reorder_queue_enabled
                    && out_of_order_append_handling_present
                    && reorder_timeout_drop_present
                    && stale_term_rejection_present
                    && peer_pipeline_states
                        .iter()
                        .all(|peer| peer.reorder_queue_depth <= self.config.reorder_window_size),
                evidence_field:
                    "peer_pipeline_states[*].{reorder_queue_depth,out_of_order_append_rejections,reorder_entries_*,reorder_entry_timeouts,reorder_dropped_packages,stale_term_rejections}".to_string(),
                detail: format!(
                    "enabled={reorder_queue_enabled}; out_of_order={out_of_order_append_handling_present}; timeout_drop={reorder_timeout_drop_present}; stale_term={stale_term_rejection_present}; window={}; timeout_us={}",
                    self.config.reorder_window_size,
                    self.config.reorder_timeout_us
                ),
            },
            MatrixRaftCapabilityEvidence {
                capability: "snapshot_sender_downloader_lifecycle".to_string(),
                ready: snapshot_sender_lifecycle_present
                    && snapshot_downloader_lifecycle_present
                    && snapshot_retry_backpressure_present
                    && snapshot_chunk_retry_present
                    && snapshot_send_timeout_present
                    && snapshot_rate_limit_present
                    && snapshot_install_progress_present
                    && snapshot_install_rollback_present
                    && snapshot_membership_change_present
                    && snapshot_rejoin_after_compacted_log_present,
                evidence_field: "peer_pipeline_states[*].{snapshot_sending,snapshot_installing,snapshot_progress,snapshot_retry,snapshot_chunk_retry,snapshot_send_timeout,snapshot_rate_limit,snapshot_rollback,snapshot_membership_change,snapshot_rejoin_after_compacted_log}".to_string(),
                detail: format!(
                    "sender={snapshot_sender_lifecycle_present}; downloader={snapshot_downloader_lifecycle_present}; retry_backpressure={snapshot_retry_backpressure_present}; chunk_retry={snapshot_chunk_retry_present}; send_timeout={snapshot_send_timeout_present}; rate_limit={snapshot_rate_limit_present}; progress={snapshot_install_progress_present}; rollback={snapshot_install_rollback_present}; membership={snapshot_membership_change_present}; rejoin_compacted={snapshot_rejoin_after_compacted_log_present}"
                ),
            },
            MatrixRaftCapabilityEvidence {
                capability: "lease_read_index_pre_vote_semantics".to_string(),
                ready: read_index_validated
                    && lease_read_validated
                    && stale_follower_read_rejected
                    && stale_follower_write_rejected
                    && stale_leader_lease_rejected
                    && lagging_follower_read_rejected
                    && bounded_stale_read_accepted
                    && bounded_stale_read_rejected
                    && minority_partition_rejected_reads
                    && minority_partition_rejected_writes
                    && healed_follower_caught_up
                    && pre_vote_enforced
                    && pre_vote_process_evidence_observed,
                evidence_field: "read_index_*; lease_read_*; stale_leader_lease_rejection_count; lagging_follower_read_rejection_count; bounded_stale_read_*; minority_partition_*_rejection_count; stale_follower_write_rejection_count; healed_follower_catchup_count; pre_vote_*; peer_pipeline_states[*].pre_vote_rejections"
                    .to_string(),
                detail: format!(
                    "read_index={read_index_validated}; lease={lease_read_validated}; stale_lease={stale_leader_lease_rejected}; lagging_read={lagging_follower_read_rejected}; bounded_accept={bounded_stale_read_accepted}; bounded_reject={bounded_stale_read_rejected}; stale_read_rejected={stale_follower_read_rejected}; stale_write_rejected={stale_follower_write_rejected}; minority_read_rejected={minority_partition_rejected_reads}; minority_write_rejected={minority_partition_rejected_writes}; healed_catchup={healed_follower_caught_up}; pre_vote={pre_vote_enforced}; pre_vote_observed={pre_vote_process_evidence_observed}"
                ),
            },
            MatrixRaftCapabilityEvidence {
                capability: "pre_vote_election_transfer_controls".to_string(),
                ready: election_controls_enforced,
                evidence_field: "pre_vote_process_evidence_observed; election_prohibition_observed; offline_timeout_observed; transfer_timeout_observed; peer_pipeline_states[*].{pre_vote_rejections,election_rejections,offline_timeout_*,transfer_leader_timeouts}".to_string(),
                detail: format!(
                    "pre_vote_observed={pre_vote_process_evidence_observed}; election_prohibited={election_prohibition_observed}; offline_timeout={offline_timeout_observed}; transfer_timeout={transfer_timeout_observed}"
                ),
            },
            MatrixRaftCapabilityEvidence {
                capability: "wal_segment_lifecycle".to_string(),
                ready: wal_segment_lifecycle_present,
                evidence_field: "wal_{segment_count,active_segment_id,first_retained_segment_id,last_retained_segment_id,total_bytes,total_records,first_sequence,last_sequence,first_log_index,last_log_index,released_segment_count,slow_fsync_backpressure_observed}".to_string(),
                detail: format!(
                    "segments={wal_segment_count}; bytes={wal_total_bytes}; records={wal_total_records}; seq={wal_first_sequence}..{wal_last_sequence}; log_index={wal_first_log_index}..{wal_last_log_index}; released={wal_released_segment_count}; slow_fsync={wal_slow_fsync_backpressure_observed}"
                ),
            },
            MatrixRaftCapabilityEvidence {
                capability: "admin_status_surface".to_string(),
                ready: admin_status_surface_complete,
                evidence_field: "admin_status_surface_complete; quorum_peer_progress_observed; peer_pipeline_runtime_activity_observed; peer_pipeline_limits_observed; /raft/control/matrixraft_runtime_admin; prometheus matrixraft metrics".to_string(),
                detail: format!(
                    "majority={}; commit_index={}; peer_rows={}; quorum_progress={quorum_peer_progress_observed}; runtime_activity={peer_pipeline_runtime_activity_observed}; limits={peer_pipeline_limits_observed}",
                    status.majority,
                    status.commit_index,
                    peer_pipeline_states.len()
                ),
            },
            MatrixRaftCapabilityEvidence {
                capability: "membership_role_semantics".to_string(),
                ready: witness_membership_present
                    && witness_role_behavior_present
                    && learner_add_present
                    && learner_catchup_present
                    && learner_promote_present
                    && voter_remove_present
                    && learner_auto_promote_present
                    && leader_transfer_exact_once_present
                    && pending_joint_consensus_present
                    && pending_joint_consensus_restart_present,
                evidence_field: "membership_evidence.{learner_add_count,learner_catchup_count,learner_promote_count,voter_remove_count,witness_add_count,auto_promote_count,leader_transfer_write_count,leader_transfer_exact_once_commit_count,leader_transfer_exact_once_commit_ids,pending_joint_consensus_persist_count,pending_joint_consensus_restore_count}; witness_membership_present; witness_role_behavior_present; learner_auto_promote_present; pending_joint_consensus_present".to_string(),
                detail: format!(
                    "learner_add={learner_add_present}; catchup={learner_catchup_present}; promote={learner_promote_present}; remove={voter_remove_present}; witness={witness_membership_present}; witness_behavior={witness_role_behavior_present}; auto_promote={learner_auto_promote_present}; transfer_exact_once={leader_transfer_exact_once_present}; pending_joint_consensus={pending_joint_consensus_present}; restart={pending_joint_consensus_restart_present}"
                ),
            },
        ];

        let mut blockers = Vec::new();
        if !read_index_validated {
            blockers.push("read_index_not_validated".to_string());
        }
        if !lease_read_validated {
            blockers.push("lease_read_not_validated".to_string());
        }
        if !stale_follower_read_rejected {
            blockers.push("stale_follower_read_rejection_missing".to_string());
        }
        if !stale_follower_write_rejected {
            blockers.push("stale_follower_write_rejection_missing".to_string());
        }
        if !stale_leader_lease_rejected {
            blockers.push("stale_leader_lease_rejection_missing".to_string());
        }
        if !lagging_follower_read_rejected {
            blockers.push("lagging_follower_read_rejection_missing".to_string());
        }
        if !bounded_stale_read_accepted {
            blockers.push("bounded_stale_read_acceptance_missing".to_string());
        }
        if !bounded_stale_read_rejected {
            blockers.push("bounded_stale_read_rejection_missing".to_string());
        }
        if !minority_partition_rejected_reads {
            blockers.push("minority_partition_read_rejection_missing".to_string());
        }
        if !minority_partition_rejected_writes {
            blockers.push("minority_partition_write_rejection_missing".to_string());
        }
        if !healed_follower_caught_up {
            blockers.push("healed_follower_catchup_missing".to_string());
        }
        if !append_backpressure_enforced {
            blockers.push("append_backpressure_not_enforced".to_string());
        }
        if !apply_backpressure_enforced {
            blockers.push("apply_backpressure_not_enforced".to_string());
        }
        if !memory_replicate_bytes_enforced {
            blockers.push("memory_replicate_bytes_not_enforced".to_string());
        }
        if !oversized_log_rejection_present {
            blockers.push("oversized_log_rejection_missing".to_string());
        }
        if !reorder_queue_enabled {
            blockers.push("reorder_queue_not_enabled".to_string());
        }
        if !out_of_order_append_handling_present {
            blockers.push("out_of_order_append_handling_missing".to_string());
        }
        if !reorder_timeout_drop_present {
            blockers.push("reorder_timeout_drop_missing".to_string());
        }
        if !stale_term_rejection_present {
            blockers.push("stale_term_rejection_missing".to_string());
        }
        if !snapshot_sender_lifecycle_present {
            blockers.push("snapshot_sender_lifecycle_missing".to_string());
        }
        if !snapshot_downloader_lifecycle_present {
            blockers.push("snapshot_downloader_lifecycle_missing".to_string());
        }
        if !snapshot_retry_backpressure_present {
            blockers.push("snapshot_retry_backpressure_missing".to_string());
        }
        if !snapshot_chunk_retry_present {
            blockers.push("snapshot_chunk_retry_missing".to_string());
        }
        if !snapshot_send_timeout_present {
            blockers.push("snapshot_send_timeout_missing".to_string());
        }
        if !snapshot_rate_limit_present {
            blockers.push("snapshot_rate_limit_missing".to_string());
        }
        if !snapshot_install_progress_present {
            blockers.push("snapshot_install_progress_missing".to_string());
        }
        if !snapshot_install_rollback_present {
            blockers.push("snapshot_install_rollback_missing".to_string());
        }
        if !snapshot_membership_change_present {
            blockers.push("snapshot_membership_change_missing".to_string());
        }
        if !snapshot_rejoin_after_compacted_log_present {
            blockers.push("snapshot_rejoin_after_compacted_log_missing".to_string());
        }
        if !wal_segment_lifecycle_present {
            blockers.push("wal_segment_lifecycle_missing".to_string());
        }
        if !witness_membership_present {
            blockers.push("witness_membership_missing".to_string());
        }
        if !witness_role_behavior_present {
            blockers.push("witness_role_behavior_missing".to_string());
        }
        if !learner_add_present {
            blockers.push("learner_add_evidence_missing".to_string());
        }
        if !learner_catchup_present {
            blockers.push("learner_catchup_evidence_missing".to_string());
        }
        if !learner_promote_present {
            blockers.push("learner_promote_evidence_missing".to_string());
        }
        if !voter_remove_present {
            blockers.push("voter_remove_evidence_missing".to_string());
        }
        if !learner_auto_promote_present {
            blockers.push("learner_auto_promote_missing".to_string());
        }
        if !leader_transfer_exact_once_present {
            blockers.push("leader_transfer_exact_once_evidence_missing".to_string());
        }
        if !pending_joint_consensus_present {
            blockers.push("pending_joint_consensus_evidence_missing".to_string());
        }
        if !pending_joint_consensus_restart_present {
            blockers.push("pending_joint_consensus_restart_evidence_missing".to_string());
        }
        if !pre_vote_enforced {
            blockers.push("pre_vote_not_enforced".to_string());
        }
        if !pre_vote_process_evidence_observed {
            blockers.push("pre_vote_process_evidence_missing".to_string());
        }
        if !election_prohibition_observed {
            blockers.push("election_prohibition_evidence_missing".to_string());
        }
        if !offline_timeout_observed {
            blockers.push("offline_timeout_evidence_missing".to_string());
        }
        if !transfer_timeout_observed {
            blockers.push("transfer_timeout_evidence_missing".to_string());
        }
        if !election_controls_enforced {
            blockers.push("election_controls_not_enforced".to_string());
        }
        if !admin_status_surface_complete {
            blockers.push("admin_status_surface_incomplete".to_string());
        }
        if !quorum_peer_progress_observed {
            blockers.push("quorum_peer_progress_evidence_missing".to_string());
        }
        if !peer_pipeline_runtime_activity_observed {
            blockers.push("peer_pipeline_runtime_activity_missing".to_string());
        }
        if !peer_pipeline_limits_observed {
            blockers.push("peer_pipeline_limits_missing".to_string());
        }

        MatrixRaftRuntimeAdminReport {
            shard_id: self.shard_id,
            leader_id: self.leader_id,
            commit_index: status.commit_index,
            leader_lease_valid: status.leader_lease_valid,
            read_index_validated,
            lease_read_validated,
            stale_follower_read_rejected,
            stale_follower_write_rejected,
            stale_leader_lease_rejected,
            lagging_follower_read_rejected,
            bounded_stale_read_accepted,
            bounded_stale_read_rejected,
            minority_partition_rejected_reads,
            minority_partition_rejected_writes,
            healed_follower_caught_up,
            witness_membership_present,
            witness_role_behavior_present,
            learner_auto_promote_present,
            pending_joint_consensus_present,
            learner_add_present,
            learner_catchup_present,
            learner_promote_present,
            voter_remove_present,
            leader_transfer_exact_once_present,
            pending_joint_consensus_restart_present,
            membership_evidence,
            peer_pipeline_states,
            append_backpressure_enforced,
            apply_backpressure_enforced,
            memory_replicate_bytes_enforced,
            oversized_log_rejection_present,
            out_of_order_append_handling_present,
            reorder_timeout_drop_present,
            stale_term_rejection_present,
            reorder_queue_enabled,
            snapshot_sender_lifecycle_present,
            snapshot_downloader_lifecycle_present,
            snapshot_retry_backpressure_present,
            snapshot_chunk_retry_present,
            snapshot_send_timeout_present,
            snapshot_rate_limit_present,
            snapshot_install_progress_present,
            snapshot_install_rollback_present,
            snapshot_membership_change_present,
            snapshot_rejoin_after_compacted_log_present,
            wal_segment_lifecycle_present,
            wal_segment_count,
            wal_active_segment_id,
            wal_first_retained_segment_id,
            wal_last_retained_segment_id,
            wal_total_bytes,
            wal_active_segment_bytes,
            wal_total_records,
            wal_first_sequence,
            wal_last_sequence,
            wal_first_log_index,
            wal_last_log_index,
            wal_released_segment_count,
            wal_slow_fsync_backpressure_observed,
            pre_vote_enforced,
            election_controls_enforced,
            pre_vote_process_evidence_observed,
            election_prohibition_observed,
            offline_timeout_observed,
            transfer_timeout_observed,
            read_index_requests: self.read_safety_state.read_index_requests,
            read_index_accepted: self.read_safety_state.read_index_accepted,
            read_index_rejected: self.read_safety_state.read_index_rejected,
            lease_read_requests: self.read_safety_state.lease_read_requests,
            lease_read_accepted: self.read_safety_state.lease_read_accepted,
            lease_read_rejected: self.read_safety_state.lease_read_rejected,
            stale_leader_lease_rejection_count: self.read_safety_state.stale_leader_lease_rejected,
            lagging_follower_read_rejection_count: self
                .read_safety_state
                .lagging_follower_read_rejected,
            bounded_stale_read_requests: self.read_safety_state.bounded_stale_read_requests,
            bounded_stale_read_accepted_count: self.read_safety_state.bounded_stale_read_accepted,
            bounded_stale_read_rejected_count: self.read_safety_state.bounded_stale_read_rejected,
            minority_partition_read_rejection_count: self
                .read_safety_state
                .minority_partition_read_rejected,
            minority_partition_write_rejection_count: self
                .read_safety_state
                .minority_partition_write_rejected,
            stale_follower_write_rejection_count: self
                .read_safety_state
                .stale_follower_write_rejected,
            healed_follower_catchup_count: self.read_safety_state.healed_follower_catchup_observed,
            pre_vote_requests: self.read_safety_state.pre_vote_requests,
            pre_vote_accepted: self.read_safety_state.pre_vote_accepted,
            pre_vote_rejected: self.read_safety_state.pre_vote_rejected,
            quorum_peer_progress_observed,
            peer_pipeline_runtime_activity_observed,
            peer_pipeline_limits_observed,
            admin_status_surface_complete,
            capability_matrix,
            ready: blockers.is_empty(),
            blockers,
        }
    }
}
