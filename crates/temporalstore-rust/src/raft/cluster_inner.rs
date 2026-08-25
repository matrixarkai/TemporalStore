// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! RaftClusterInner core helpers (timeouts, candidates, election, membership, WAL, status), split from raft.rs.
use super::*;

impl RaftClusterInner {
    pub(super) fn refresh_offline_timeout_states(&mut self) {
        for node in self.nodes.values_mut() {
            if node.alive {
                node.pipeline_state.offline_since_ms = None;
                node.pipeline_state.offline_elapsed_ms = 0;
                node.pipeline_state.offline_timeout_reached = false;
                continue;
            }
            let offline_since_ms = node
                .pipeline_state
                .offline_since_ms
                .get_or_insert(self.logical_time_ms);
            node.pipeline_state.offline_elapsed_ms =
                self.logical_time_ms.saturating_sub(*offline_since_ms);
            let reached = self.config.offline_timeout_tick > 0
                && node.pipeline_state.offline_elapsed_ms >= self.config.offline_timeout_tick;
            if reached && !node.pipeline_state.offline_timeout_reached {
                node.pipeline_state.offline_timeout_rejections = node
                    .pipeline_state
                    .offline_timeout_rejections
                    .saturating_add(1);
            }
            node.pipeline_state.offline_timeout_reached = reached;
        }
    }

    pub(super) fn refresh_snapshot_send_timeouts(&mut self) {
        if self.config.send_snapshot_timeout_ms == 0 {
            return;
        }
        for node in self.nodes.values_mut() {
            // Either flag makes the guard reject a proposal to this peer, so either has to be
            // able to time out. Watching only the sending flag left a peer stuck installing to
            // reject proposals forever: the refresh skipped it and reset its clock every pass.
            let in_transfer = node.pipeline_state.snapshot_sending
                || node.pipeline_state.snapshot_installing;
            if !in_transfer {
                node.pipeline_state.snapshot_send_started_ms = None;
                node.pipeline_state.snapshot_send_elapsed_ms = 0;
                node.pipeline_state.snapshot_send_progress_mark = 0;
                continue;
            }
            // Progress resets the clock. Measuring from the start of the send instead would
            // abort a large transfer that is moving along perfectly well, which is the reason
            // the timeout had to be set long enough to be useless against a real stall.
            let progressed = node.pipeline_state.snapshot_install_received_chunks
                != node.pipeline_state.snapshot_send_progress_mark;
            if progressed {
                node.pipeline_state.snapshot_send_progress_mark =
                    node.pipeline_state.snapshot_install_received_chunks;
                node.pipeline_state.snapshot_send_started_ms = Some(self.logical_time_ms);
                node.pipeline_state.snapshot_send_elapsed_ms = 0;
                continue;
            }
            let started_ms = node
                .pipeline_state
                .snapshot_send_started_ms
                .get_or_insert(self.logical_time_ms);
            node.pipeline_state.snapshot_send_elapsed_ms =
                self.logical_time_ms.saturating_sub(*started_ms);
            if node.pipeline_state.snapshot_send_elapsed_ms >= self.config.send_snapshot_timeout_ms
            {
                node.pipeline_state.snapshot_sending = false;
                node.pipeline_state.snapshot_installing = false;
                node.pipeline_state.snapshot_send_started_ms = None;
                node.pipeline_state.snapshot_send_progress_mark = 0;
                node.pipeline_state.snapshot_send_timeouts =
                    node.pipeline_state.snapshot_send_timeouts.saturating_add(1);
                node.pipeline_state.snapshot_retry_count =
                    node.pipeline_state.snapshot_retry_count.saturating_add(1);
                node.pipeline_state.snapshot_send_failed =
                    node.pipeline_state.snapshot_send_failed.saturating_add(1);
                node.pipeline_state.snapshot_backpressure_rejections = node
                    .pipeline_state
                    .snapshot_backpressure_rejections
                    .saturating_add(1);
            }
        }
    }

    pub(super) fn refresh_leader_transfer_timeouts(&mut self) {
        if self.config.transfer_timeout_tick == 0 {
            return;
        }
        for node in self.nodes.values_mut() {
            if !node.pipeline_state.transfer_leader_target {
                node.pipeline_state.transfer_leader_started_ms = None;
                node.pipeline_state.transfer_leader_elapsed_ms = 0;
                continue;
            }
            let started_ms = node
                .pipeline_state
                .transfer_leader_started_ms
                .get_or_insert(self.logical_time_ms);
            node.pipeline_state.transfer_leader_elapsed_ms =
                self.logical_time_ms.saturating_sub(*started_ms);
            if node.pipeline_state.transfer_leader_elapsed_ms >= self.config.transfer_timeout_tick {
                node.pipeline_state.transfer_leader_target = false;
                node.pipeline_state.transfer_leader_started_ms = None;
                node.pipeline_state.transfer_leader_timeouts = node
                    .pipeline_state
                    .transfer_leader_timeouts
                    .saturating_add(1);
                node.pipeline_state.transfer_leader_rejected = node
                    .pipeline_state
                    .transfer_leader_rejected
                    .saturating_add(1);
            }
        }
    }

    pub(super) fn ensure_live_leader(&mut self) -> Result<(), RaftError> {
        if self
            .nodes
            .get(&self.leader_id)
            .map(|node| node.alive && node.role == RaftRole::Leader)
            .unwrap_or(false)
        {
            return Ok(());
        }
        self.promote_best_live_follower()
    }

    pub(super) fn promote_best_live_follower(&mut self) -> Result<(), RaftError> {
        let candidate = self.best_live_candidate()?;
        self.elect_leader(candidate)
    }

    pub(super) fn best_live_candidate(&self) -> Result<RaftNodeId, RaftError> {
        self.nodes
            .values()
            .filter(|node| node.alive && node.replica_role.can_be_leader())
            .min_by_key(|node| {
                (
                    std::cmp::Reverse(node.commit_index),
                    std::cmp::Reverse(node_last_log_or_snapshot_index(node)),
                    node.id,
                )
            })
            .map(|node| node.id)
            .ok_or(RaftError::LeaderUnavailable)
    }

    pub(super) fn best_live_candidate_in(
        &self,
        allowed: &BTreeSet<RaftNodeId>,
    ) -> Result<RaftNodeId, RaftError> {
        self.nodes
            .values()
            .filter(|node| {
                allowed.contains(&node.id) && node.alive && node.replica_role.can_be_leader()
            })
            .min_by_key(|node| {
                (
                    std::cmp::Reverse(node.commit_index),
                    std::cmp::Reverse(node_last_log_or_snapshot_index(node)),
                    node.id,
                )
            })
            .map(|node| node.id)
            .ok_or(RaftError::LeaderUnavailable)
    }

    pub(super) fn catch_up_live_followers(&mut self) -> Result<Vec<RaftNodeId>, RaftError> {
        self.ensure_live_leader()?;
        let leader_id = self.leader_id;
        let leader = self
            .nodes
            .get(&leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let leader_log = leader.log.clone();
        let leader_commit_index = leader.commit_index;
        let leader_snapshot = leader.installed_snapshot.clone();
        let mut caught_up = Vec::new();
        for node in self
            .nodes
            .values_mut()
            .filter(|node| node.alive && node.id != leader_id)
        {
            if node.commit_index < leader_commit_index
                || node_last_log_or_snapshot_index(node)
                    < leader_log
                        .last()
                        .map(|entry| entry.index)
                        .unwrap_or_default()
            {
                install_leader_snapshot_tail(
                    node,
                    leader_snapshot.clone(),
                    leader_log.clone(),
                    leader_commit_index,
                );
            }
            if node.commit_index >= leader_commit_index {
                caught_up.push(node.id);
            }
        }
        Ok(caught_up)
    }

    pub(super) fn catch_up_live_followers_bounded(
        &mut self,
        max_entries_per_follower: u64,
    ) -> Result<u64, RaftError> {
        self.ensure_live_leader()?;
        let leader_id = self.leader_id;
        let leader = self
            .nodes
            .get(&leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let leader_log = leader.log.clone();
        let leader_commit_index = leader.commit_index;
        let leader_snapshot = leader.installed_snapshot.clone();
        let mut replayed_log_entries = 0u64;
        for node in self
            .nodes
            .values_mut()
            .filter(|node| node.alive && node.id != leader_id)
        {
            if node.commit_index >= leader_commit_index {
                continue;
            }
            let before = node.commit_index;
            let snapshot_floor = leader_snapshot
                .as_ref()
                .map(|snapshot| snapshot.last_included_index)
                .unwrap_or_default();
            let target_commit_index = leader_commit_index
                .min(node.commit_index + max_entries_per_follower)
                .max(snapshot_floor.min(leader_commit_index));
            if let Some(snapshot) = leader_snapshot.clone() {
                if node_last_log_or_snapshot_index(node) < snapshot.last_included_index {
                    install_snapshot_state_for_role(node, snapshot);
                }
            }
            node.log = leader_log
                .iter()
                .filter(|entry| entry.index <= target_commit_index)
                .cloned()
                .collect();
            node.commit_index = target_commit_index;
            if node.replica_role.can_serve_data() {
                apply_committed(node);
            }
            replayed_log_entries += node.commit_index.saturating_sub(before);
        }
        Ok(replayed_log_entries)
    }

    pub(super) fn remove_node_safely(&mut self, node_id: RaftNodeId) -> Result<(), RaftError> {
        if self.voting_node_ids().len() == 1
            && self
                .nodes
                .get(&node_id)
                .map(|node| node.replica_role.participates_in_quorum())
                .unwrap_or(false)
        {
            return Err(RaftError::CannotRemoveLastNode);
        }
        if !self.nodes.contains_key(&node_id) {
            return Err(RaftError::NodeNotFound(node_id));
        }
        let remaining = self
            .voting_node_ids()
            .into_iter()
            .filter(|id| *id != node_id)
            .collect::<BTreeSet<_>>();
        let required_after = majority(remaining.len());
        let live_after = remaining
            .iter()
            .filter(|id| self.nodes.get(id).map(|node| node.alive).unwrap_or(false))
            .count();
        if live_after < required_after {
            return Err(RaftError::NoMajority {
                live: live_after,
                required: required_after,
            });
        }

        if self.leader_id == node_id {
            let leader_commit_index = self.leader_commit_index();
            let candidate_id = self.best_live_candidate_in(&remaining)?;
            let candidate = self
                .nodes
                .get(&candidate_id)
                .ok_or(RaftError::NodeNotFound(candidate_id))?;
            if candidate.commit_index < leader_commit_index {
                return Err(RaftError::ReplicaLagging {
                    replica_id: candidate_id,
                    replica_commit_index: candidate.commit_index,
                    leader_commit_index,
                });
            }
            self.nodes.remove(&node_id);
            self.elect_leader(candidate_id)?;
        } else {
            self.nodes.remove(&node_id);
        }
        Ok(())
    }

    pub(super) fn plan_membership_change(
        &self,
        new_voters: impl IntoIterator<Item = RaftNodeId>,
    ) -> Result<RaftMembershipChangePlan, RaftError> {
        if self.joint_membership.is_some() {
            return Err(RaftError::JointConsensusInProgress);
        }
        if !self
            .nodes
            .get(&self.leader_id)
            .map(|node| node.alive && node.role == RaftRole::Leader)
            .unwrap_or(false)
        {
            return Err(RaftError::LeaderUnavailable);
        }
        let old_voters = self.voting_node_ids().into_iter().collect::<BTreeSet<_>>();
        let new_voters = new_voters.into_iter().collect::<BTreeSet<_>>();
        if new_voters.is_empty() {
            return Err(RaftError::CannotRemoveLastNode);
        }
        if old_voters == new_voters {
            return Err(RaftError::InvalidConfig(
                "membership change must add or remove at least one voter".to_string(),
            ));
        }

        let add_voters = new_voters
            .difference(&old_voters)
            .copied()
            .collect::<Vec<_>>();
        let remove_voters = old_voters
            .difference(&new_voters)
            .copied()
            .collect::<Vec<_>>();
        let kind = match (add_voters.is_empty(), remove_voters.is_empty()) {
            (false, true) => RaftMembershipChangeKind::AddVoter,
            (true, false) => RaftMembershipChangeKind::RemoveVoter,
            (false, false) => RaftMembershipChangeKind::ReplaceVoter,
            (true, true) => unreachable!("old_voters != new_voters was checked"),
        };

        let live_new_voters = new_voters
            .iter()
            .filter(|node_id| {
                self.nodes
                    .get(node_id)
                    .map(|node| node.alive)
                    .unwrap_or(true)
            })
            .count();
        let required_new_majority = majority(new_voters.len());
        if live_new_voters < required_new_majority {
            return Err(RaftError::NoMajority {
                live: live_new_voters,
                required: required_new_majority,
            });
        }

        Ok(RaftMembershipChangePlan {
            shard_id: self.shard_id,
            kind,
            old_voters: old_voters.into_iter().collect(),
            new_voters: new_voters.into_iter().collect(),
            add_voters,
            remove_voters,
        })
    }

    pub(super) fn scale_change_report(&self) -> RaftScaleChangeReport {
        let status = self.status();
        RaftScaleChangeReport {
            leader_id: status.leader_id,
            voters: self.voting_node_ids(),
            live_voters: status.live_voters,
            majority: status.majority,
            caught_up_voters: status
                .nodes
                .into_iter()
                .filter(|node| {
                    node.alive && node.lag == 0 && node.replica_role.participates_in_quorum()
                })
                .map(|node| node.node_id)
                .collect(),
        }
    }

    pub(super) fn failover_report(&self, old_leader_id: RaftNodeId) -> RaftFailoverReport {
        let status = self.status();
        let term = status.current_term;
        RaftFailoverReport {
            old_leader_id,
            new_leader_id: status.leader_id,
            term,
            commit_index: status.commit_index,
            caught_up_voters: status
                .nodes
                .into_iter()
                .filter(|node| {
                    node.alive && node.lag == 0 && node.replica_role.participates_in_quorum()
                })
                .map(|node| node.node_id)
                .collect(),
        }
    }

    pub(super) fn pre_vote_would_win(&self, candidate_id: RaftNodeId) -> Result<bool, RaftError> {
        self.candidate_log_would_win(candidate_id)
    }

    pub(super) fn candidate_log_would_win(&self, candidate_id: RaftNodeId) -> Result<bool, RaftError> {
        let candidate = self
            .nodes
            .get(&candidate_id)
            .ok_or(RaftError::NodeNotFound(candidate_id))?;
        // Snapshot-aware tail: after log compaction the last committed entry lives in the
        // installed snapshot, not `log`. Comparing raw `log.last()` (which is None -> (0,0)
        // for a fully-snapshotted node) would let a lagging candidate look "up to date"
        // against a caught-up replica (safety) and stop the healthiest node from winning
        // (liveness). Mirrors the meta-raft election path.
        let candidate_last_index = node_last_log_or_snapshot_index(candidate);
        let candidate_last_term =
            node_term_at_log_or_snapshot_index(candidate, candidate_last_index).unwrap_or_default();
        let votes = self
            .nodes
            .values()
            .filter(|node| node.alive)
            .filter(|node| node.replica_role.participates_in_quorum())
            .filter(|node| {
                let local_last_index = node_last_log_or_snapshot_index(node);
                let local_last_term =
                    node_term_at_log_or_snapshot_index(node, local_last_index).unwrap_or_default();
                (candidate_last_term, candidate_last_index) >= (local_last_term, local_last_index)
            })
            .count();
        if let Some(membership) = &self.joint_membership {
            let old_votes = self.up_to_date_votes_in(candidate_id, &membership.old_voters)?;
            let new_votes = self.up_to_date_votes_in(candidate_id, &membership.new_voters)?;
            return Ok(old_votes >= majority(membership.old_voters.len())
                && new_votes >= majority(membership.new_voters.len()));
        }
        Ok(votes >= majority(self.voting_node_ids().len()))
    }

    pub(super) fn up_to_date_votes_in(
        &self,
        candidate_id: RaftNodeId,
        voters: &[RaftNodeId],
    ) -> Result<usize, RaftError> {
        let candidate = self
            .nodes
            .get(&candidate_id)
            .ok_or(RaftError::NodeNotFound(candidate_id))?;
        // Snapshot-aware tail: after log compaction the last committed entry lives in the
        // installed snapshot, not `log`. Comparing raw `log.last()` (which is None -> (0,0)
        // for a fully-snapshotted node) would let a lagging candidate look "up to date"
        // against a caught-up replica (safety) and stop the healthiest node from winning
        // (liveness). Mirrors the meta-raft election path.
        let candidate_last_index = node_last_log_or_snapshot_index(candidate);
        let candidate_last_term =
            node_term_at_log_or_snapshot_index(candidate, candidate_last_index).unwrap_or_default();
        Ok(voters
            .iter()
            .filter_map(|node_id| self.nodes.get(node_id))
            .filter(|node| node.alive)
            .filter(|node| node.replica_role.participates_in_quorum())
            .filter(|node| {
                let local_last_index = node_last_log_or_snapshot_index(node);
                let local_last_term =
                    node_term_at_log_or_snapshot_index(node, local_last_index).unwrap_or_default();
                (candidate_last_term, candidate_last_index) >= (local_last_term, local_last_index)
            })
            .count())
    }

    /// R5: promote `node_id` only after a DURABLE per-term quorum RequestVote round grants it a
    /// majority. This mirrors the receiver rules
    /// already implemented in `receive_vote_request` (single vote per term via `voted_for`,
    /// leader-completeness log-freshness), but drives them across every voter and gates
    /// promotion on the collected grant count. A partitioned candidate whose granting voters
    /// fall short of majority cannot elect, so two disjoint partitions can never each promote a
    /// leader for the same term.
    pub(super) fn elect_leader(&mut self, node_id: RaftNodeId) -> Result<(), RaftError> {
        if self.config.prohibits_election {
            for node in self.nodes.values_mut() {
                node.pipeline_state.election_rejections =
                    node.pipeline_state.election_rejections.saturating_add(1);
            }
            return Err(RaftError::ElectionProhibited);
        }
        if let Some((live, required)) = self.joint_majority_failure() {
            return Err(RaftError::NoMajority { live, required });
        }
        if !self
            .nodes
            .get(&node_id)
            .map(|node| node.alive && node.replica_role.can_be_leader())
            .unwrap_or(false)
        {
            return Err(RaftError::NodeNotFound(node_id));
        }

        let election_term = self
            .nodes
            .values()
            .map(|node| node.current_term)
            .max()
            .unwrap_or_default()
            .saturating_add(1);
        let (candidate_last_index, candidate_last_term) = {
            let candidate = self
                .nodes
                .get(&node_id)
                .ok_or(RaftError::NodeNotFound(node_id))?;
            let last_index = node_last_log_or_snapshot_index(candidate);
            let last_term =
                node_term_at_log_or_snapshot_index(candidate, last_index).unwrap_or_default();
            (last_index, last_term)
        };

        // Candidate durably records its own term bump + self-vote first.
        {
            let candidate = self
                .nodes
                .get_mut(&node_id)
                .ok_or(RaftError::NodeNotFound(node_id))?;
            candidate.current_term = election_term;
            candidate.voted_for = Some(node_id);
        }
        let required = self.required_majority();
        let mut grants = 1usize; // self-vote

        let voter_ids: Vec<RaftNodeId> = self
            .nodes
            .values()
            .filter(|node| node.replica_role.participates_in_quorum() && node.id != node_id)
            .map(|node| node.id)
            .collect();
        for voter_id in voter_ids {
            let node = self
                .nodes
                .get_mut(&voter_id)
                .ok_or(RaftError::NodeNotFound(voter_id))?;
            // A partitioned / unreachable voter cannot respond to the RequestVote — it observes
            // the higher term (so it cannot later grant a stale one) but does not grant here.
            if !node.alive {
                node.current_term = node.current_term.max(election_term);
                continue;
            }
            let already_voted_other = node.current_term == election_term
                && node.voted_for.is_some()
                && node.voted_for != Some(node_id);
            let voter_last_index = node_last_log_or_snapshot_index(node);
            let voter_last_term =
                node_term_at_log_or_snapshot_index(node, voter_last_index).unwrap_or_default();
            let candidate_log_up_to_date = (candidate_last_term, candidate_last_index)
                >= (voter_last_term, voter_last_index);
            if !already_voted_other && candidate_log_up_to_date {
                node.current_term = election_term;
                node.voted_for = Some(node_id);
                node.role = RaftRole::Follower;
                grants += 1;
            } else {
                node.current_term = node.current_term.max(election_term);
                node.pipeline_state.election_rejections =
                    node.pipeline_state.election_rejections.saturating_add(1);
            }
        }

        if grants < required {
            // Did not collect a durable quorum — do NOT promote. Leave the cluster leaderless
            // for this attempt rather than installing a minority "leader".
            if let Some(candidate) = self.nodes.get_mut(&node_id) {
                candidate.role = RaftRole::Follower;
            }
            return Err(RaftError::NoMajority {
                live: grants,
                required,
            });
        }

        self.leader_id = node_id;
        for node in self.nodes.values_mut() {
            node.role = if node.id == node_id {
                RaftRole::Leader
            } else {
                RaftRole::Follower
            };
        }
        self.election_elapsed_tick = 0;
        self.renew_leader_lease();
        Ok(())
    }

    /// Install `node_id` as leader at `term` after it won a networked election.
    /// Unlike `elect_leader` this does not synthesise peer votes from local shadow
    /// state -- the grants were already collected over the wire.
    pub(super) fn promote_elected_leader(
        &mut self,
        node_id: RaftNodeId,
        term: u64,
    ) -> Result<(), RaftError> {
        if !self.nodes.contains_key(&node_id) {
            return Err(RaftError::NodeNotFound(node_id));
        }
        self.leader_id = node_id;
        for node in self.nodes.values_mut() {
            node.role = if node.id == node_id {
                RaftRole::Leader
            } else {
                RaftRole::Follower
            };
            node.current_term = node.current_term.max(term);
        }
        if let Some(leader) = self.nodes.get_mut(&node_id) {
            leader.alive = true;
            leader.voted_for = Some(node_id);
        }
        self.election_elapsed_tick = 0;
        self.renew_leader_lease();
        // Raft's fresh-leader state: optimistic `next_index`, unknown `match_index`, and
        // nothing in flight. The first AppendEntries is then a probe that discovers where
        // each follower really is, and rejections walk `next_index` back. Re-deriving the
        // pipeline from shadow peer state here instead would immediately re-inflate the
        // in-flight window and reject that probe.
        let next_index = self
            .nodes
            .get(&node_id)
            .map(node_next_log_index)
            .unwrap_or(1);
        for node in self.nodes.values_mut() {
            if node.id == node_id {
                continue;
            }
            node.pipeline_state.next_index = next_index;
            node.pipeline_state.inflight_entries = 0;
            node.pipeline_state.inflight_bytes = 0;
            node.pipeline_state.append_queue_depth = 0;
        }
        Ok(())
    }

    /// Advance the commit index to the highest entry a quorum has matched, applying what that
    /// commits on the leader. The classic counting rule applies: only an entry of the current
    /// term commits by counting replicas; older entries commit with it.
    pub(super) fn maybe_advance_quorum_commit(&mut self) -> bool {
        let leader_id = self.leader_id;
        let required = self.required_majority();
        let Some(leader) = self.nodes.get(&leader_id) else {
            return false;
        };
        if leader.role != RaftRole::Leader || !leader.alive {
            return false;
        }
        let current_term = leader.current_term;
        let leader_last = node_next_log_index(leader).saturating_sub(1);
        let mut matched: Vec<u64> = Vec::new();
        for (id, node) in &self.nodes {
            if !node.replica_role.participates_in_quorum() {
                continue;
            }
            matched.push(if *id == leader_id {
                leader_last
            } else {
                node.pipeline_state.match_index
            });
        }
        if matched.len() < required {
            return false;
        }
        matched.sort_unstable_by(|left, right| right.cmp(left));
        let candidate = matched[required - 1];
        let Some(leader) = self.nodes.get(&leader_id) else {
            return false;
        };
        if candidate <= leader.commit_index {
            return false;
        }
        match node_term_at_log_or_snapshot_index(leader, candidate) {
            Some(term) if term == current_term => {}
            _ => return false,
        }
        let waiters = std::mem::take(&mut self.response_waiters);
        let pending = &mut self.pending_responses;
        let leader = self
            .nodes
            .get_mut(&leader_id)
            .expect("leader present: fetched above");
        leader.commit_index = candidate;
        if leader.replica_role.can_serve_data() {
            apply_committed_recording(leader, &waiters, pending);
        }
        self.response_waiters = waiters;
        self.renew_leader_lease();
        true
    }

    pub(super) fn persist_configured_wal(&mut self) -> Result<(), RaftError> {
        let staged = self.stage_configured_wal()?;
        self.finish_staged_wal(staged)
    }

    /// Take the barriers a set of staged appends owes.
    ///
    /// Safe to call with the propose lock released: the writes are already ordered, and a barrier
    /// makes every byte already in the file durable regardless of who wrote it.
    pub(super) fn finish_staged_wal(
        &self,
        staged: Vec<local_wal::StagedWalAppend>,
    ) -> Result<(), RaftError> {
        let Some(wal) = &self.wal else {
            return Ok(());
        };
        for append in staged {
            wal.finish_staged(append)
                .map_err(|err| RaftError::Wal(err.to_string()))?;
        }
        Ok(())
    }

    /// Write what this node must persist, WITHOUT taking the barrier for it.
    pub(super) fn stage_configured_wal(&mut self) -> Result<Vec<local_wal::StagedWalAppend>, RaftError> {
        // P4: this thread owes a barrier rather than skipping one -- `flush_deferred_persist`
        // takes it before the propose acks. Only the thread that opened the deferral defers, so
        // a vote grant or heartbeat racing this propose still fsyncs before it answers.
        if self.persist_deferred_owner == Some(std::thread::current().id()) {
            self.persist_dirty = true;
            return Ok(Vec::new());
        }
        // Clone the WAL handle (cheap -- it shares the cursor cache via an Arc) so `self` is free
        // to be borrowed mutably below for the coalescing fingerprint map.
        let wal = match &self.wal {
            Some(wal) => wal.clone(),
            None => return Ok(Vec::new()),
        };
        let shard_id = self.shard_id;
        let max_segment_bytes = self.config.max_segment_bytes;
        let min_keep_segment_num = self.config.min_keep_segment_num as usize;
        let coalesce = raft_wal_coalesce_on();
        // P3: a deployed process owns exactly one node but keeps a full cluster view, so
        // `wal_records()` emits a record per peer and this loop fsyncs every one of them. A
        // leader owes its peers no durability: each persists its own hard state before answering
        // an RPC, and re-learns the rest from AppendEntries on restart. Persist only our own
        // record. Unset -- the in-process test cluster -- keeps persisting every node.
        let local_only = self.local_node_id;
        // Build ONLY the record we are going to persist. Building every node's record and then
        // skipping all but one clones the whole log once per node and discards the rest, which
        // is O(log length) of pure waste on every persist.
        let records = match local_only {
            Some(local) => self.wal_record_for(local).into_iter().collect::<Vec<_>>(),
            None => self.wal_records(),
        };
        let mut staged = Vec::new();
        for (node_id, record) in records {
            // P1: when coalescing is enabled, skip the fdatasync for a node whose durability-
            // relevant state is byte-identical to what we last persisted. A false "changed" only
            // costs a redundant (safe) fsync; the skip fires ONLY when nothing Raft must persist
            // has changed, so it can never drop a committed entry or an un-fsynced hard-state.
            if coalesce {
                let fingerprint = raft_durable_fingerprint(&record);
                if self.last_durable_fingerprint.get(&node_id) == Some(&fingerprint) {
                    continue;
                }
                staged.push(
                    wal.stage_node_segmented(
                        shard_id,
                        node_id,
                        &record,
                        max_segment_bytes,
                        min_keep_segment_num,
                    )
                    .map_err(|err| RaftError::Wal(err.to_string()))?,
                );
                self.last_durable_fingerprint.insert(node_id, fingerprint);
            } else {
                staged.push(
                    wal.stage_node_segmented(
                        shard_id,
                        node_id,
                        &record,
                        max_segment_bytes,
                        min_keep_segment_num,
                    )
                    .map_err(|err| RaftError::Wal(err.to_string()))?,
                );
            }
        }
        Ok(staged)
    }

    /// P4: start owing barriers on THIS thread instead of taking them. The caller must hold the
    /// propose lock, which is what makes this thread the only proposer for the span.
    pub(super) fn begin_deferred_persist(&mut self) {
        self.persist_deferred_owner = Some(std::thread::current().id());
        self.persist_dirty = false;
    }

    /// P4: take the one real barrier covering everything deferred since `begin_deferred_persist`.
    /// Called before the propose acks, on the success and the failure path alike.
    pub(super) fn flush_deferred_persist(&mut self) -> Result<(), RaftError> {
        let staged = self.stage_deferred_persist()?;
        self.finish_staged_wal(staged)
    }

    /// End the deferral WITHOUT writing what it owes.
    ///
    /// Used for the commit index once the entry itself is already durable. Raft treats the commit
    /// index as recoverable -- a restarting node learns it from its leader, and a leader
    /// re-establishes it by committing an entry of its own term -- so it does not have to be made
    /// durable before a write is acknowledged. The next record written carries the current value
    /// anyway.
    pub(super) fn discard_deferred_persist(&mut self) {
        self.persist_deferred_owner = None;
        self.persist_dirty = false;
    }

    /// End the deferral by WRITING what is owed, leaving the barrier to the caller.
    ///
    /// This is what lets the propose path drop its lock before flushing: the write happens while
    /// the lock still orders it, and the barrier -- which every concurrent proposer can share --
    /// happens after.
    pub(super) fn stage_deferred_persist(
        &mut self,
    ) -> Result<Vec<local_wal::StagedWalAppend>, RaftError> {
        self.persist_deferred_owner = None;
        if !self.persist_dirty {
            return Ok(Vec::new());
        }
        self.persist_dirty = false;
        self.stage_configured_wal()
    }

    pub(super) fn wal_records(&self) -> Vec<(RaftNodeId, RaftWalRecord)> {
        self.nodes
            .keys()
            .copied()
            .filter_map(|node_id| self.wal_record_for(node_id))
            .collect()
    }

    /// Build the WAL record for ONE node. Cloning a node's log is O(log length), so a caller
    /// that will persist a single node must not pay for the others.
    pub(super) fn wal_record_for(
        &self,
        node_id: RaftNodeId,
    ) -> Option<(RaftNodeId, RaftWalRecord)> {
        let membership = RaftMembership {
            shard_id: self.shard_id,
            voters: self.voting_node_ids(),
            leader_id: self.leader_id,
        };
        self.nodes
            .get(&node_id)
            .map(|node| {
                (
                    node_id,
                    RaftWalRecord {
                        hard_state: RaftHardState {
                            current_term: node.current_term,
                            voted_for: node.voted_for,
                            commit_index: node.commit_index,
                        },
                        membership: membership.clone(),
                        replica_role: node.replica_role,
                        joint_membership: self.joint_membership.clone(),
                        latest_external_snapshot_ref: self.latest_external_snapshot_ref.clone(),
                        installed_snapshot: node.installed_snapshot.clone(),
                        apply_snapshot_fence: raft_apply_snapshot_fence(node),
                        storage_apply_fence: raft_storage_apply_fence(self.shard_id, node),
                        pipeline_state: node.pipeline_state.clone(),
                        read_safety_state: self.read_safety_state.clone(),
                        membership_evidence: self.membership_evidence.clone(),
                        entries: node.log.clone(),
                    },
                )
            })
    }

    pub(super) fn leader_commit_index(&self) -> u64 {
        self.nodes
            .get(&self.leader_id)
            .map(|node| node.commit_index)
            .unwrap_or_default()
    }

    /// R4 leader-ready barrier: has the current leader committed an entry in its OWN term? Until
    /// it has, a freshly elected leader must not serve read-index/lease reads, because it cannot
    /// yet know the true commit point of its predecessors' terms (Raft §5.4.2 — the reason a new
    /// leader commits a no-op). A committed current-term entry (in the log or folded into an
    /// installed snapshot) discharges the barrier.
    pub(super) fn leader_has_committed_current_term(&self) -> bool {
        let Some(leader) = self.nodes.get(&self.leader_id) else {
            return false;
        };
        let term = leader.current_term;
        let committed_in_log = leader
            .log
            .iter()
            .any(|entry| entry.term == term && entry.index <= leader.commit_index);
        let committed_in_snapshot = leader
            .installed_snapshot
            .as_ref()
            .map(|snapshot| {
                snapshot.last_included_term == term
                    && snapshot.last_included_index <= leader.commit_index
            })
            .unwrap_or(false);
        committed_in_log || committed_in_snapshot
    }

    pub(super) fn voting_node_ids(&self) -> Vec<RaftNodeId> {
        self.nodes
            .values()
            .filter(|node| node.replica_role.participates_in_quorum())
            .map(|node| node.id)
            .collect()
    }

    pub(super) fn live_quorum_participants(&self) -> usize {
        self.nodes
            .values()
            .filter(|node| node.alive && node.replica_role.participates_in_quorum())
            .count()
    }

    pub(super) fn required_majority(&self) -> usize {
        self.joint_membership
            .as_ref()
            .map(|membership| {
                majority(membership.old_voters.len()).max(majority(membership.new_voters.len()))
            })
            .unwrap_or_else(|| majority(self.voting_node_ids().len()))
    }

    pub(super) fn joint_majority_failure(&self) -> Option<(usize, usize)> {
        self.joint_membership
            .as_ref()
            .and_then(|membership| joint_majority_failure(&self.nodes, membership))
    }

    pub(super) fn renew_leader_lease(&mut self) {
        self.leader_lease_deadline_ms = if self.config.lease_duration_ms == 0 {
            u64::MAX
        } else {
            self.logical_time_ms
                .saturating_add(self.config.lease_duration_ms)
        };
    }

    pub(super) fn leader_lease_valid(&self) -> bool {
        self.config.lease_duration_ms == 0 || self.logical_time_ms <= self.leader_lease_deadline_ms
    }

    pub(super) fn status(&self) -> RaftClusterStatus {
        let commit_index = self.leader_commit_index();
        let current_term = self
            .nodes
            .get(&self.leader_id)
            .map(|node| node.current_term)
            .unwrap_or_default();
        let majority = self.required_majority();
        let live_voters = self.live_quorum_participants();
        let leader_lease_valid = self
            .nodes
            .get(&self.leader_id)
            .map(|node| {
                node.alive && node.role == RaftRole::Leader && node.replica_role.can_be_leader()
            })
            .unwrap_or(false)
            && self.leader_lease_valid()
            && live_voters >= majority
            && self.joint_majority_failure().is_none();
        RaftClusterStatus {
            leader_id: self.leader_id,
            current_term,
            commit_index,
            majority,
            live_voters,
            has_majority: live_voters >= majority,
            leader_lease_valid,
            nodes: self
                .nodes
                .values()
                .map(|node| node_status(node, commit_index))
                .collect(),
        }
    }

}
