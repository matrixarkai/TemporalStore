// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

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
        let mut probe_only = false;
        let leader_commit_for_drain = inner
            .nodes
            .get(&inner.leader_id)
            .map(|leader| leader.commit_index)
            .unwrap_or_default();
        let leader_last_index = inner
            .nodes
            .get(&inner.leader_id)
            .map(node_last_log_or_snapshot_index)
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
            // A request with nothing to send buffers nothing, so the in-flight limits
            // do not apply to it. This is the heartbeat -- and the probe a newly promoted
            // leader sends to find out where a follower really is. Blocking it leaves the
            // new leader unable to contact anyone, which reads as a leaderless shard.
            let carries_entries = target.pipeline_state.next_index <= leader_last_index;
            if carries_entries
                && (target.pipeline_state.inflight_entries >= entry_limit
                    || target.pipeline_state.inflight_bytes >= byte_limit)
            {
                // A charged-up window used to REFUSE the append outright. For a peer that is
                // behind, that is a deadlock, not backpressure: the refusal blocks the very
                // entries whose acknowledgements would drain the window, the lag-derived
                // refresh keeps re-charging it while the leader commits with the remaining
                // quorum, and the peer is cut off for good. Measured on a 30k-write corpus:
                // one election put a follower past the 128-entry window, it was never sent
                // another entry, the second follower followed at the next election, writes
                // froze on NoMajority, and the log grew unbounded because compaction held for
                // a "catching up" follower that was never allowed to catch up. Degrade to a
                // PROBE instead -- one entry, whatever its size. Its acknowledgement moves the
                // match forward, the refresh re-derives a smaller charge, and the window
                // reopens by itself. The rejection counters still record the pressure.
                target.pipeline_state.append_rejected =
                    target.pipeline_state.append_rejected.saturating_add(1);
                target.pipeline_state.memory_backpressure_rejections = target
                    .pipeline_state
                    .memory_backpressure_rejections
                    .saturating_add(u64::from(
                        target.pipeline_state.inflight_bytes >= byte_limit,
                    ));
                probe_only = true;
            }
        }
        // Never build a batch larger than a receiver will accept in one request: a follower
        // refuses anything over its apply limit, so a bigger batch is not throughput, it is a
        // request that can only be rejected.
        let apply_limit = inner.config.max_inflights_apply_task.max(1);
        let (available_entries, available_bytes) = if probe_only {
            // One entry regardless of the charge: the probe is what drains the window.
            (1, byte_limit)
        } else {
            (
                entry_limit
                    .saturating_sub(current_inflight_entries)
                    .min(apply_limit),
                byte_limit.saturating_sub(current_inflight_bytes),
            )
        };
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
        let leader_last_index = node_last_log_or_snapshot_index(leader);
        // Where to resume from. `next_index` is the peer's own answer -- lowered by every
        // rejection, set from the peer's match index on success -- so it is what makes a mismatch
        // converge. Falling back to the local shadow of the peer is only for a peer that has not
        // answered yet, because the shadow does not move when a peer rejects, and asking about the
        // same index forever is exactly how a diverged follower stays stuck.
        let target_next_index = target.pipeline_state.next_index;
        let prev_log_index = if target_next_index > 0 {
            target_next_index.saturating_sub(1).min(leader_last_index)
        } else {
            node_last_log_or_snapshot_index(target).min(leader_last_index)
        };
        let prev_log_term =
            node_term_at_log_or_snapshot_index(leader, prev_log_index).unwrap_or_default();
        let mut entries = Vec::new();
        let mut inflight_bytes = 0u64;
        // A raft log is in ascending index order, so everything this peer still needs is a
        // SUFFIX of it. Scanning from the front and filtering costs the whole log on every
        // AppendEntries to every peer, which makes a write more expensive the more history sits
        // in front of it -- appending one entry should cost the same at index 10 and 10,000.
        let first_unsent = leader
            .log
            .partition_point(|entry| entry.index <= prev_log_index);
        crate::durability_metrics::record_scan(
            "replication_entries_examined",
            (leader.log.len() - first_unsent) as u64,
        );
        for entry in leader.log[first_unsent..].iter() {
            if entries.len() as u64 >= available_entries {
                break;
            }
            let entry_bytes = command_size_bytes(&entry.command);
            if !entries.is_empty() && inflight_bytes.saturating_add(entry_bytes) > available_bytes {
                break;
            }
            if entries.is_empty()
                && entry_bytes > available_bytes
                && current_inflight_bytes > 0
                && !probe_only
            {
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
                    inflight_entries: current_inflight_entries,
                    inflight_bytes: current_inflight_bytes,
                    entry_bytes,
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
        // A reply from a newer term means this leader is stale -- step down rather than keep
        // sending requests that can only be refused. A peer that was isolated returns with a term
        // well ahead of ours, and without this it rejects every append forever while we stay
        // leader at the old term, so it can never rejoin.
        let local_leader_id = inner.leader_id;
        let stepped_down = inner
            .nodes
            .get_mut(&local_leader_id)
            .map(|leader| {
                if response.term > leader.current_term {
                    leader.current_term = response.term;
                    leader.voted_for = None;
                    leader.role = RaftRole::Follower;
                    true
                } else {
                    false
                }
            })
            .unwrap_or(false);
        let target = inner
            .nodes
            .get_mut(&target_id)
            .ok_or(RaftError::NodeNotFound(target_id))?;
        target.pipeline_state.inflight_entries = 0;
        target.pipeline_state.inflight_bytes = 0;
        target.pipeline_state.append_queue_depth = 0;
        if stepped_down {
            target.current_term = target.current_term.max(response.term);
            inner.persist_configured_wal()?;
            return Ok(());
        }
        if response.success {
            target.pipeline_state.match_index =
                target.pipeline_state.match_index.max(response.match_index);
            target.pipeline_state.next_index = response.match_index.saturating_add(1);
        } else {
            target.pipeline_state.append_rejected =
                target.pipeline_state.append_rejected.saturating_add(1);
            // Retreat only when the peer is telling us the logs disagree. A peer that refused
            // because the request was too big to apply at once has the prefix we sent; going
            // further back would only make the next request bigger and refused again.
            if Self::rejection_means_logs_disagree(response.reject_reason.as_deref()) {
                target.pipeline_state.next_index =
                    target.pipeline_state.next_index.saturating_sub(1).max(1);
            }
        }
        inner.persist_configured_wal()
    }

    /// Whether a rejection means the two logs disagree about a prefix, which is the only thing
    /// retreating to an earlier entry can repair.
    ///
    /// The others are refusals of THIS request, not of the position: too many entries to apply at
    /// once, too many bytes, a stale term, the wrong shard. Retreating on those sends more next
    /// time, so it turns a transient refusal into a permanent one. An unrecognised reason retreats,
    /// which is the conservative reading and what the code did for every reason before.
    fn rejection_means_logs_disagree(reject_reason: Option<&str>) -> bool {
        !matches!(
            reject_reason,
            Some("apply_inflight_backpressure")
                | Some("apply_batch_backpressure")
                | Some("stale_term")
                | Some("shard_mismatch")
        )
    }

    /// Release what a request reserved when the send failed outright.
    ///
    /// A response -- success or rejection -- clears the reservation. A send that never reaches the
    /// peer produces no response, so without this the reservation is held forever, and once enough
    /// have leaked the peer is refused by its own backpressure limit on every future attempt. It
    /// cannot catch up (nothing is sent to it) and so cannot drain the reservation (which only
    /// drains on catching up), which makes the exclusion permanent.
    pub fn record_append_entries_send_failure(
        &self,
        target_id: RaftNodeId,
    ) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let target = inner
            .nodes
            .get_mut(&target_id)
            .ok_or(RaftError::NodeNotFound(target_id))?;
        target.pipeline_state.inflight_entries = 0;
        target.pipeline_state.inflight_bytes = 0;
        target.pipeline_state.append_queue_depth = 0;
        target.pipeline_state.append_send_failures = target
            .pipeline_state
            .append_send_failures
            .saturating_add(1);
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
        // Record what the LEADER says its commit index is, on every request -- including the ones
        // this node is about to reject. That is the whole point: a rejecting follower keeps
        // hearing from the leader, so this stays fresh while the node's own log does not move,
        // and the two together say "behind, and not catching up".
        {
            let logical_now = inner.logical_time_ms;
            if let Some(node) = inner.nodes.get_mut(&target_id) {
                node.pipeline_state.leader_reported_commit_index = leader_commit;
                if node.pipeline_state.last_accepted_append_ms == 0 {
                    // Never accepted anything yet: start the clock here rather than at zero, so a
                    // node that just joined does not report the age of the process as a stall.
                    node.pipeline_state.last_accepted_append_ms = logical_now;
                }
            }
        }
        // Contact from the leader proves it is alive, whatever we go on to decide about its
        // entries. A follower marks its leader down when its election timer expires, and until
        // now only an ACCEPTED append marked it back up -- so a follower that is merely behind,
        // which rejects appends while it catches up, held a healthy leader as down and refused
        // every operation that needs one.
        let heard_from_the_leader = inner
            .nodes
            .get(&target_id)
            .map(|node| term >= node.current_term)
            .unwrap_or(false);
        if heard_from_the_leader && leader_id != target_id {
            if let Some(leader) = inner.nodes.get_mut(&leader_id) {
                leader.alive = true;
            }
        }
        let received_entries = entries.len() as u64;
        let received_bytes = entries
            .iter()
            .map(|entry| command_size_bytes(&entry.command))
            .sum::<u64>();
        // A leader that is merely out of sync with this follower's log is still ALIVE.
        // Only a stale-term request (from a superseded leader) leaves the election timer
        // running. Counting only ACCEPTED appends as contact meant a follower being caught
        // up kept timing out and campaigning, which bumped the term, which made the
        // leader's in-flight appends stale, which got them rejected -- catch-up and
        // election fighting each other instead of converging.
        let local_current_term = inner
            .nodes
            .get(&target_id)
            .map(|node| node.current_term)
            .unwrap_or_default();
        if term >= local_current_term {
            inner.leader_contact_epoch = inner.leader_contact_epoch.saturating_add(1);
        }
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
            // votedFor is scoped to a single term (Raft Fig-2): observing a higher term in
            // any RPC -- including AppendEntries -- must clear it, else a stale vote from the
            // old term wrongly suppresses this node's vote in the new term (split-vote
            // liveness bug). The vote and snapshot-install paths already do this.
            if term > node.current_term {
                node.voted_for = None;
            }
            node.current_term = term;
            node.role = RaftRole::Follower;
            let before_reorder_depth = node.pipeline_state.reorder_queue_depth;
            for entry in entries.iter().cloned() {
                append_entry(node, entry);
            }
            // Raft Figure 2: commitIndex = min(leaderCommit, index of the last NEW entry
            // in THIS AppendEntries) -- NOT the whole-log tail. A divergent UNCOMMITTED
            // suffix that sits beyond this batch (e.g. an entry from a failed prior-term
            // leader) must not be committed just because it is present in the log. Using
            // node_last_log_or_snapshot_index here would commit/apply such an entry the
            // cluster never committed. commitIndex also stays monotonic (max with current).
            let last_new_entry_index = entries
                .iter()
                .map(|entry| entry.index)
                .max()
                .unwrap_or(request.prev_log_index);
            node.commit_index = node
                .commit_index
                .max(leader_commit.min(last_new_entry_index));
            // Reported match_index reflects the follower's WHOLE log tail (how far its log
            // now matches the leader after this append), independent of the commit clamp.
            let last_index = node_last_log_or_snapshot_index(node);
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
        // Proof of life from the leader. A follower's timer loop diffs this to decide
        // whether the leader has gone quiet and it should stand for election.
        inner.leader_contact_epoch = inner.leader_contact_epoch.saturating_add(1);
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
        let local_node_id_for_refresh = inner.local_node_id;
        // This append was accepted, so progress happened: stamp it before anything can fail.
        let accepted_at = inner.logical_time_ms;
        if let Some(node) = inner.nodes.get_mut(&target_id) {
            node.pipeline_state.last_accepted_append_ms = accepted_at;
        }
        refresh_all_pipeline_states(&mut inner.nodes, leader_id, local_node_id_for_refresh, &config);
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
        // R9: a candidate durably persists its incremented term and a self-vote BEFORE it
        // advertises the RequestVote. Without that ordering a crash-restart could re-issue the
        // same term without remembering it already voted for itself, and grant a second vote in
        // that term -- two leaders for one term. Take the write lock, bump `current_term`, record
        // `voted_for = self`, and fsync the WAL before returning the request.
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let shard_id = inner.shard_id;
        let (election_term, last_log_index, last_log_term) = {
            let candidate = inner
                .nodes
                .get_mut(&candidate_id)
                .ok_or(RaftError::NodeNotFound(candidate_id))?;
            candidate.current_term = candidate.current_term.saturating_add(1);
            candidate.voted_for = Some(candidate_id);
            // Advertise the snapshot-aware tail: a fully-snapshotted candidate has an empty
            // `log`, so raw `log.last()` would advertise (0,0) and lose every election it
            // should win. Use the same helper as the AppendEntries / meta-raft paths.
            let last_log_index = node_last_log_or_snapshot_index(candidate);
            let last_log_term =
                node_term_at_log_or_snapshot_index(candidate, last_log_index).unwrap_or_default();
            (candidate.current_term, last_log_index, last_log_term)
        };
        inner.persist_configured_wal()?;
        Ok(VoteRequest {
            pre_vote: false,
            rpc: None,
            shard_id,
            term: election_term,
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
        if request.pre_vote {
            // Answer, and change nothing: no term adopted, no vote recorded, nothing persisted.
            // That is what makes a pre-vote safe to lose, and what stops a node that cannot reach
            // anyone from walking its term up on a timer.
            let local_last_index = node_last_log_or_snapshot_index(node);
            let local_last_term =
                node_term_at_log_or_snapshot_index(node, local_last_index).unwrap_or_default();
            let would_grant = request.term > node.current_term
                && (request.last_log_term, request.last_log_index)
                    >= (local_last_term, local_last_index);
            if !would_grant {
                node.pipeline_state.pre_vote_rejections =
                    node.pipeline_state.pre_vote_rejections.saturating_add(1);
            }
            return Ok(VoteResponse {
                term: node.current_term,
                vote_granted: would_grant,
                reject_reason: (!would_grant).then(|| "pre_vote_declined".to_string()),
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
        // Snapshot-aware voter tail: a fully-snapshotted voter has an empty `log`; comparing
        // raw `log.last()` (-> (0,0)) would grant a vote to a candidate missing committed
        // entries this voter already holds in its snapshot (leader-completeness violation).
        let local_last_index = node_last_log_or_snapshot_index(node);
        let local_last_term =
            node_term_at_log_or_snapshot_index(node, local_last_index).unwrap_or_default();
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
        // Raft resets the election timer when it grants a vote: granting one means someone
        // else is already standing, and campaigning against them in the same window is what
        // turns a split vote into a livelock the cluster never escapes.
        inner.leader_contact_epoch = inner.leader_contact_epoch.saturating_add(1);
        inner.persist_configured_wal()?;
        Ok(VoteResponse {
            term,
            vote_granted: true,
            reject_reason: None,
        })
    }
}
