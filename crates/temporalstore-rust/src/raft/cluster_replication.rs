//! RaftCluster append-entries / vote / WAL replication methods, split from raft.rs.
use super::*;

impl RaftCluster {
    pub fn wait_for_applied_index(
        &self,
        node_id: RaftNodeId,
        index: u64,
        timeout_ms: u64,
    ) -> Result<(), RaftError> {
        let deadline = InstantCompat::now();
        loop {
            let applied_index = {
                let inner = self.inner.read().expect("raft cluster lock poisoned");
                inner
                    .nodes
                    .get(&node_id)
                    .ok_or(RaftError::NodeNotFound(node_id))?
                    .applied_index
            };
            if applied_index >= index {
                return Ok(());
            }
            if deadline.elapsed() >= Duration::from_millis(timeout_ms) {
                return Err(RaftError::AppliedIndexTimeout {
                    node_id,
                    applied_index,
                    target_index: index,
                    timeout_ms,
                });
            }
            thread::sleep(Duration::from_millis(1));
        }
    }

    pub fn hard_state(&self, node_id: RaftNodeId) -> Result<RaftHardState, RaftError> {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        let node = inner
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        Ok(RaftHardState {
            current_term: node.current_term,
            voted_for: node.voted_for,
            commit_index: node.commit_index,
        })
    }

    pub fn membership(&self) -> RaftMembership {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        RaftMembership {
            shard_id: inner.shard_id,
            voters: inner.voting_node_ids(),
            leader_id: inner.leader_id,
        }
    }

    pub fn wal_records(&self) -> Vec<(RaftNodeId, RaftWalRecord)> {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        let membership = RaftMembership {
            shard_id: inner.shard_id,
            voters: inner.voting_node_ids(),
            leader_id: inner.leader_id,
        };
        inner
            .nodes
            .iter()
            .map(|(node_id, node)| {
                (
                    *node_id,
                    RaftWalRecord {
                        hard_state: RaftHardState {
                            current_term: node.current_term,
                            voted_for: node.voted_for,
                            commit_index: node.commit_index,
                        },
                        membership: membership.clone(),
                        replica_role: node.replica_role,
                        joint_membership: inner.joint_membership.clone(),
                        latest_external_snapshot_ref: inner.latest_external_snapshot_ref.clone(),
                        installed_snapshot: node.installed_snapshot.clone(),
                        apply_snapshot_fence: raft_apply_snapshot_fence(node),
                        storage_apply_fence: raft_storage_apply_fence(inner.shard_id, node),
                        pipeline_state: node.pipeline_state.clone(),
                        read_safety_state: inner.read_safety_state.clone(),
                        membership_evidence: inner.membership_evidence.clone(),
                        entries: node.log.clone(),
                    },
                )
            })
            .collect()
    }

    pub fn persist_wal(&self, root: impl AsRef<Path>) -> io::Result<()> {
        let wal = LocalRaftWal::new(root.as_ref().to_path_buf());
        let records = self.wal_records();
        for (node_id, record) in records {
            wal.persist_node(record.membership.shard_id, node_id, &record)?;
        }
        Ok(())
    }

    pub fn build_append_entries_request(
        &self,
        target_id: RaftNodeId,
    ) -> Result<AppendEntriesRequest, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let entry_limit = inner.config.max_inflights_replicate.max(1);
        let byte_limit = inner.config.max_memory_replicate_log_bytes.max(1);
        let mut current_inflight_entries = 0;
        let mut current_inflight_bytes = 0;
        let leader_commit_for_drain = inner
            .nodes
            .get(&inner.leader_id)
            .map(|leader| leader.commit_index)
            .unwrap_or_default();
        if let Some(target) = inner.nodes.get_mut(&target_id) {
            if target.commit_index >= leader_commit_for_drain
                && (target.pipeline_state.inflight_entries > 0
                    || target.pipeline_state.inflight_bytes > 0)
            {
                target.pipeline_state.inflight_entries = 0;
                target.pipeline_state.inflight_bytes = 0;
                target.pipeline_state.append_queue_depth = 0;
            }
            target.pipeline_state.append_requests =
                target.pipeline_state.append_requests.saturating_add(1);
            current_inflight_entries = target.pipeline_state.inflight_entries;
            current_inflight_bytes = target.pipeline_state.inflight_bytes;
            if target.pipeline_state.inflight_entries >= entry_limit
                || target.pipeline_state.inflight_bytes >= byte_limit
            {
                target.pipeline_state.append_rejected =
                    target.pipeline_state.append_rejected.saturating_add(1);
                target.pipeline_state.memory_backpressure_rejections = target
                    .pipeline_state
                    .memory_backpressure_rejections
                    .saturating_add(u64::from(
                        target.pipeline_state.inflight_bytes >= byte_limit,
                    ));
                let inflight_entries = target.pipeline_state.inflight_entries;
                let inflight_bytes = target.pipeline_state.inflight_bytes;
                inner.persist_configured_wal()?;
                return Err(RaftError::AppendBackpressure {
                    node_id: target_id,
                    inflight_entries,
                    inflight_bytes,
                    entry_limit,
                    byte_limit,
                });
            }
        }
        let available_entries = entry_limit.saturating_sub(current_inflight_entries);
        let available_bytes = byte_limit.saturating_sub(current_inflight_bytes);
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let leader_id = inner.leader_id;
        let leader_term = leader.current_term;
        let shard_id = inner.shard_id;
        let leader_commit = leader.commit_index;
        let target = inner
            .nodes
            .get(&target_id)
            .ok_or(RaftError::NodeNotFound(target_id))?;
        let enable_reorder_queue = inner.config.enable_reorder_queue;
        let target_last_index = node_last_log_or_snapshot_index(target);
        let prev_log_index = target_last_index.min(node_last_log_or_snapshot_index(leader));
        let prev_log_term =
            node_term_at_log_or_snapshot_index(leader, prev_log_index).unwrap_or_default();
        let mut entries = Vec::new();
        let mut inflight_bytes = 0u64;
        for entry in leader
            .log
            .iter()
            .filter(|entry| entry.index > prev_log_index)
        {
            if entries.len() as u64 >= available_entries {
                break;
            }
            let entry_bytes = command_size_bytes(&entry.command);
            if !entries.is_empty() && inflight_bytes.saturating_add(entry_bytes) > available_bytes {
                break;
            }
            if entries.is_empty() && entry_bytes > available_bytes {
                if let Some(target) = inner.nodes.get_mut(&target_id) {
                    target.pipeline_state.append_rejected =
                        target.pipeline_state.append_rejected.saturating_add(1);
                    target.pipeline_state.memory_backpressure_rejections = target
                        .pipeline_state
                        .memory_backpressure_rejections
                        .saturating_add(1);
                }
                inner.persist_configured_wal()?;
                return Err(RaftError::AppendBackpressure {
                    node_id: target_id,
                    inflight_entries: 0,
                    inflight_bytes: entry_bytes,
                    entry_limit,
                    byte_limit,
                });
            }
            inflight_bytes = inflight_bytes.saturating_add(entry_bytes);
            entries.push(entry.clone());
        }
        let inflight_bytes = entries
            .iter()
            .map(|entry| command_size_bytes(&entry.command))
            .sum();
        if let Some(target) = inner.nodes.get_mut(&target_id) {
            target.pipeline_state.append_accepted =
                target.pipeline_state.append_accepted.saturating_add(1);
            target.pipeline_state.match_index = target.commit_index;
            target.pipeline_state.next_index = prev_log_index.saturating_add(1);
            target.pipeline_state.inflight_entries =
                current_inflight_entries.saturating_add(entries.len() as u64);
            target.pipeline_state.inflight_bytes =
                current_inflight_bytes.saturating_add(inflight_bytes);
            target.pipeline_state.append_queue_depth = target.pipeline_state.inflight_entries;
            target.pipeline_state.append_queue_max_depth = target
                .pipeline_state
                .append_queue_max_depth
                .max(target.pipeline_state.append_queue_depth);
            if enable_reorder_queue {
                target.pipeline_state.reorder_queue_depth =
                    target.commit_index.saturating_sub(target.applied_index);
            }
        }
        inner.persist_configured_wal()?;
        Ok(AppendEntriesRequest {
            rpc: None,
            shard_id,
            term: leader_term,
            leader_id,
            target_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        })
    }

    pub fn record_append_entries_response(
        &self,
        target_id: RaftNodeId,
        response: &AppendEntriesResponse,
    ) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let target = inner
            .nodes
            .get_mut(&target_id)
            .ok_or(RaftError::NodeNotFound(target_id))?;
        target.pipeline_state.inflight_entries = 0;
        target.pipeline_state.inflight_bytes = 0;
        target.pipeline_state.append_queue_depth = 0;
        if response.success {
            target.pipeline_state.match_index =
                target.pipeline_state.match_index.max(response.match_index);
            target.pipeline_state.next_index = response.match_index.saturating_add(1);
        } else {
            target.pipeline_state.append_rejected =
                target.pipeline_state.append_rejected.saturating_add(1);
            target.pipeline_state.next_index =
                target.pipeline_state.next_index.saturating_sub(1).max(1);
        }
        inner.persist_configured_wal()
    }

    pub fn receive_append_entries(
        &self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        if request.shard_id != inner.shard_id {
            return Ok(AppendEntriesResponse {
                term: 0,
                success: false,
                match_index: 0,
                reject_reason: Some("shard_mismatch".to_string()),
            });
        }
        let entries = request.entries;
        let target_id = request.target_id;
        let leader_id = request.leader_id;
        let term = request.term;
        let leader_commit = request.leader_commit;
        let received_entries = entries.len() as u64;
        let received_bytes = entries
            .iter()
            .map(|entry| command_size_bytes(&entry.command))
            .sum::<u64>();
        let enable_reorder_queue = inner.config.enable_reorder_queue;
        let reorder_window_size = inner.config.reorder_window_size;
        let max_apply_batch_bytes = inner.config.max_apply_batch_bytes;
        let max_inflights_apply_task = inner.config.max_inflights_apply_task.max(1);
        let (term, last_index) = {
            let node = inner
                .nodes
                .get_mut(&target_id)
                .ok_or(RaftError::NodeNotFound(target_id))?;
            if term < node.current_term {
                node.pipeline_state.append_rejected =
                    node.pipeline_state.append_rejected.saturating_add(1);
                node.pipeline_state.stale_term_rejections =
                    node.pipeline_state.stale_term_rejections.saturating_add(1);
                node.pipeline_state.reorder_entries_rejected = node
                    .pipeline_state
                    .reorder_entries_rejected
                    .saturating_add(received_entries.max(1));
                return Ok(AppendEntriesResponse {
                    term: node.current_term,
                    success: false,
                    match_index: node_last_log_or_snapshot_index(node),
                    reject_reason: Some("stale_term".to_string()),
                });
            }
            if request.prev_log_index > 0 {
                let prev_term = node_term_at_log_or_snapshot_index(node, request.prev_log_index);
                if prev_term != Some(request.prev_log_term) {
                    let last_index = node_last_log_or_snapshot_index(node);
                    let missing_gap = request.prev_log_index.saturating_sub(last_index);
                    node.pipeline_state.out_of_order_append_rejections = node
                        .pipeline_state
                        .out_of_order_append_rejections
                        .saturating_add(1);
                    node.pipeline_state.reorder_entries_rejected = node
                        .pipeline_state
                        .reorder_entries_rejected
                        .saturating_add(received_entries.max(1));
                    if enable_reorder_queue && missing_gap > 0 {
                        let reject_reason = if missing_gap > reorder_window_size {
                            node.pipeline_state.reorder_entry_timeouts =
                                node.pipeline_state.reorder_entry_timeouts.saturating_add(1);
                            node.pipeline_state.reorder_dropped_packages = node
                                .pipeline_state
                                .reorder_dropped_packages
                                .saturating_add(1);
                            "reorder_window_timeout"
                        } else {
                            node.pipeline_state.reorder_queue_depth = node
                                .pipeline_state
                                .reorder_queue_depth
                                .saturating_add(received_entries.max(1));
                            "out_of_order_append_queued"
                        };
                        let response = AppendEntriesResponse {
                            term: node.current_term,
                            success: false,
                            match_index: last_index,
                            reject_reason: Some(reject_reason.to_string()),
                        };
                        inner.persist_configured_wal()?;
                        return Ok(response);
                    }
                    return Ok(AppendEntriesResponse {
                        term: node.current_term,
                        success: false,
                        match_index: node_last_log_or_snapshot_index(node),
                        reject_reason: Some("log_mismatch".to_string()),
                    });
                }
            }
            if received_bytes > max_apply_batch_bytes {
                node.pipeline_state.apply_queue_depth = received_entries.max(1);
                node.pipeline_state.apply_queue_max_depth = node
                    .pipeline_state
                    .apply_queue_max_depth
                    .max(node.pipeline_state.apply_queue_depth);
                node.pipeline_state.apply_backpressure_rejections = node
                    .pipeline_state
                    .apply_backpressure_rejections
                    .saturating_add(1);
                let response = AppendEntriesResponse {
                    term: node.current_term,
                    success: false,
                    match_index: node_last_log_or_snapshot_index(node),
                    reject_reason: Some("apply_batch_backpressure".to_string()),
                };
                inner.persist_configured_wal()?;
                return Ok(response);
            }
            if received_entries > max_inflights_apply_task {
                node.pipeline_state.apply_queue_depth = received_entries;
                node.pipeline_state.apply_queue_max_depth = node
                    .pipeline_state
                    .apply_queue_max_depth
                    .max(node.pipeline_state.apply_queue_depth);
                node.pipeline_state.apply_backpressure_rejections = node
                    .pipeline_state
                    .apply_backpressure_rejections
                    .saturating_add(1);
                let response = AppendEntriesResponse {
                    term: node.current_term,
                    success: false,
                    match_index: node_last_log_or_snapshot_index(node),
                    reject_reason: Some("apply_inflight_backpressure".to_string()),
                };
                inner.persist_configured_wal()?;
                return Ok(response);
            }
            if enable_reorder_queue
                && received_entries > 0
                && node
                    .pipeline_state
                    .reorder_queue_depth
                    .saturating_add(received_entries)
                    > reorder_window_size
            {
                node.pipeline_state.reorder_entries_rejected = node
                    .pipeline_state
                    .reorder_entries_rejected
                    .saturating_add(received_entries);
                node.pipeline_state.reorder_entry_timeouts =
                    node.pipeline_state.reorder_entry_timeouts.saturating_add(1);
                node.pipeline_state.reorder_dropped_packages = node
                    .pipeline_state
                    .reorder_dropped_packages
                    .saturating_add(1);
                let response = AppendEntriesResponse {
                    term: node.current_term,
                    success: false,
                    match_index: node_last_log_or_snapshot_index(node),
                    reject_reason: Some("reorder_window_exceeded".to_string()),
                };
                inner.persist_configured_wal()?;
                return Ok(response);
            }
            node.current_term = term;
            node.role = RaftRole::Follower;
            let before_reorder_depth = node.pipeline_state.reorder_queue_depth;
            for entry in entries.iter().cloned() {
                append_entry(node, entry);
            }
            let last_index = node_last_log_or_snapshot_index(node);
            node.commit_index = leader_commit.min(last_index);
            if node.replica_role.can_serve_data() {
                node.pipeline_state.apply_inflight_tasks =
                    node.pipeline_state.apply_inflight_tasks.saturating_add(1);
                node.pipeline_state.apply_queue_depth = received_entries;
                node.pipeline_state.apply_queue_max_depth = node
                    .pipeline_state
                    .apply_queue_max_depth
                    .max(node.pipeline_state.apply_queue_depth);
                apply_committed(node);
                node.pipeline_state.apply_inflight_tasks = 0;
                node.pipeline_state.apply_queue_depth = 0;
            }
            node.pipeline_state.match_index = node.commit_index;
            node.pipeline_state.next_index = node_next_log_index(node);
            node.pipeline_state.inflight_entries = 0;
            node.pipeline_state.inflight_bytes = 0;
            node.pipeline_state.append_queue_depth = 0;
            node.pipeline_state.reorder_queue_depth = if enable_reorder_queue {
                node.commit_index.saturating_sub(node.applied_index)
            } else {
                0
            };
            if enable_reorder_queue {
                node.pipeline_state.reorder_entries_accepted = node
                    .pipeline_state
                    .reorder_entries_accepted
                    .saturating_add(received_entries);
                let released = before_reorder_depth
                    .saturating_add(received_entries)
                    .saturating_sub(node.pipeline_state.reorder_queue_depth);
                node.pipeline_state.reorder_entries_released = node
                    .pipeline_state
                    .reorder_entries_released
                    .saturating_add(released);
            }
            node.pipeline_state.snapshot_installing = false;
            node.pipeline_state.pre_vote_rejections = node
                .pipeline_state
                .pre_vote_rejections
                .saturating_add(u64::from(received_entries == 0 && received_bytes == 0));
            (node.current_term, last_index)
        };
        inner.leader_id = leader_id;
        for (node_id, peer) in inner.nodes.iter_mut() {
            if *node_id != leader_id && peer.role == RaftRole::Leader {
                peer.role = RaftRole::Follower;
            }
        }
        if leader_id != target_id {
            if let Some(leader) = inner.nodes.get_mut(&leader_id) {
                leader.alive = true;
                leader.role = RaftRole::Leader;
                leader.current_term = leader.current_term.max(term);
                for entry in entries {
                    append_entry(leader, entry);
                }
                let leader_last_index = node_last_log_or_snapshot_index(leader);
                leader.commit_index = leader
                    .commit_index
                    .max(leader_commit.min(leader_last_index));
            }
        }
        let config = inner.config.clone();
        refresh_all_pipeline_states(&mut inner.nodes, leader_id, &config);
        inner.renew_leader_lease();
        inner.persist_configured_wal()?;
        Ok(AppendEntriesResponse {
            term,
            success: true,
            match_index: last_index,
            reject_reason: None,
        })
    }

    pub fn build_vote_request(
        &self,
        candidate_id: RaftNodeId,
        target_id: RaftNodeId,
    ) -> Result<VoteRequest, RaftError> {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        let candidate = inner
            .nodes
            .get(&candidate_id)
            .ok_or(RaftError::NodeNotFound(candidate_id))?;
        let last_log_index = candidate
            .log
            .last()
            .map(|entry| entry.index)
            .unwrap_or_default();
        let last_log_term = candidate
            .log
            .last()
            .map(|entry| entry.term)
            .unwrap_or_default();
        Ok(VoteRequest {
            rpc: None,
            shard_id: inner.shard_id,
            term: candidate.current_term + 1,
            candidate_id,
            target_id,
            last_log_index,
            last_log_term,
        })
    }

    pub fn receive_vote_request(&self, request: VoteRequest) -> Result<VoteResponse, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        if request.shard_id != inner.shard_id {
            return Ok(VoteResponse {
                term: 0,
                vote_granted: false,
                reject_reason: Some("shard_mismatch".to_string()),
            });
        }
        if !inner
            .nodes
            .get(&request.candidate_id)
            .map(|candidate| candidate.replica_role.can_be_leader())
            .unwrap_or(false)
        {
            return Ok(VoteResponse {
                term: 0,
                vote_granted: false,
                reject_reason: Some("candidate_not_voter".to_string()),
            });
        }
        let node = inner
            .nodes
            .get_mut(&request.target_id)
            .ok_or(RaftError::NodeNotFound(request.target_id))?;
        if !node.replica_role.participates_in_quorum() {
            node.pipeline_state.election_rejections =
                node.pipeline_state.election_rejections.saturating_add(1);
            return Ok(VoteResponse {
                term: node.current_term,
                vote_granted: false,
                reject_reason: Some("target_not_voter".to_string()),
            });
        }
        if request.term < node.current_term {
            node.pipeline_state.pre_vote_rejections =
                node.pipeline_state.pre_vote_rejections.saturating_add(1);
            return Ok(VoteResponse {
                term: node.current_term,
                vote_granted: false,
                reject_reason: Some("stale_term".to_string()),
            });
        }
        if request.term > node.current_term {
            node.current_term = request.term;
            node.voted_for = None;
            node.role = RaftRole::Follower;
        }
        let local_last_index = node.log.last().map(|entry| entry.index).unwrap_or_default();
        let local_last_term = node.log.last().map(|entry| entry.term).unwrap_or_default();
        let log_up_to_date =
            (request.last_log_term, request.last_log_index) >= (local_last_term, local_last_index);
        if !log_up_to_date {
            let term = node.current_term;
            node.pipeline_state.pre_vote_rejections =
                node.pipeline_state.pre_vote_rejections.saturating_add(1);
            inner.persist_configured_wal()?;
            return Ok(VoteResponse {
                term,
                vote_granted: false,
                reject_reason: Some("candidate_log_behind".to_string()),
            });
        }
        if node.voted_for.is_some() && node.voted_for != Some(request.candidate_id) {
            let term = node.current_term;
            node.pipeline_state.election_rejections =
                node.pipeline_state.election_rejections.saturating_add(1);
            inner.persist_configured_wal()?;
            return Ok(VoteResponse {
                term,
                vote_granted: false,
                reject_reason: Some("already_voted".to_string()),
            });
        }
        node.current_term = request.term;
        node.voted_for = Some(request.candidate_id);
        node.role = RaftRole::Follower;
        let term = node.current_term;
        inner.persist_configured_wal()?;
        Ok(VoteResponse {
            term,
            vote_granted: true,
            reject_reason: None,
        })
    }
}
