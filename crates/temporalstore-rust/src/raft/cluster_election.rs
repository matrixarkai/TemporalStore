// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! RaftCluster leader election / transfer / failover methods, split from raft.rs.
use super::*;

impl RaftCluster {
    /// Monotonic counter of AppendEntries accepted from a leader. A follower that sees
    /// it stall for an election timeout concludes the leader is gone.
    pub fn leader_contact_epoch(&self) -> u64 {
        self.inner
            .read()
            .expect("raft cluster lock poisoned")
            .leader_contact_epoch
    }

    /// True only when this node both believes it is the leader AND still holds the
    /// leader role. A restarted node initialises `leader_id` to the lowest node id, so
    /// `leader_id == me` on its own is not evidence that this node still leads.
    pub fn is_local_leader(&self, node_id: RaftNodeId) -> bool {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        inner.leader_id == node_id
            && inner
                .nodes
                .get(&node_id)
                .map(|node| node.role == RaftRole::Leader)
                .unwrap_or(false)
    }

    /// Give up leadership locally. Used when a leader can no longer reach a quorum, or
    /// learns a peer has moved to a newer term: it must stop acting as leader before it
    /// can serve another write.
    pub fn step_down_local(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let stepped = {
            let node = inner
                .nodes
                .get_mut(&node_id)
                .ok_or(RaftError::NodeNotFound(node_id))?;
            if node.role == RaftRole::Leader {
                node.role = RaftRole::Follower;
                true
            } else {
                false
            }
        };
        if stepped {
            inner.leader_lease_deadline_ms = 0;
            inner.persist_configured_wal()?;
        }
        Ok(())
    }

    /// Production runtimes clear this: leadership there is decided by real RequestVote
    /// RPCs, so `tick_election` must not promote a node from local shadow state.
    pub fn set_local_shadow_election(&self, enabled: bool) {
        self.inner
            .write()
            .expect("raft cluster lock poisoned")
            .local_shadow_election = enabled;
    }

    /// Step one of a networked election: bump this node's term, record its self-vote,
    /// and get both durable BEFORE any vote is requested. Returns the RequestVote to
    /// send to each peer (`target_id` is filled in per peer by the caller).
    pub fn prepare_campaign(&self, candidate_id: RaftNodeId) -> Result<VoteRequest, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        if inner.config.prohibits_election {
            return Err(RaftError::ElectionProhibited);
        }
        if !inner
            .nodes
            .get(&candidate_id)
            .map(|node| node.alive && node.replica_role.can_be_leader())
            .unwrap_or(false)
        {
            return Err(RaftError::NodeNotFound(candidate_id));
        }
        let shard_id = inner.shard_id;
        let term = inner
            .nodes
            .values()
            .map(|node| node.current_term)
            .max()
            .unwrap_or_default()
            .saturating_add(1);
        let (last_log_index, last_log_term) = {
            let candidate = inner
                .nodes
                .get(&candidate_id)
                .ok_or(RaftError::NodeNotFound(candidate_id))?;
            let index = node_last_log_or_snapshot_index(candidate);
            (
                index,
                node_term_at_log_or_snapshot_index(candidate, index).unwrap_or_default(),
            )
        };
        {
            let candidate = inner
                .nodes
                .get_mut(&candidate_id)
                .ok_or(RaftError::NodeNotFound(candidate_id))?;
            candidate.current_term = term;
            candidate.voted_for = Some(candidate_id);
            candidate.role = RaftRole::Follower;
        }
        inner.election_elapsed_tick = 0;
        inner.persist_configured_wal()?;
        Ok(VoteRequest {
            rpc: None,
            shard_id,
            term,
            candidate_id,
            target_id: candidate_id,
            last_log_index,
            last_log_term,
        })
    }

    /// Step two: promote locally iff a majority of voters granted `term`. `grants`
    /// includes the durable self-vote taken in `prepare_campaign`.
    pub fn conclude_campaign(
        &self,
        candidate_id: RaftNodeId,
        term: u64,
        grants: usize,
    ) -> Result<bool, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        // Votes are collected over the wire, so a newer term can land between
        // `prepare_campaign` and here. The grants would then be for a term this node has
        // already abandoned, and installing them would resurrect a superseded leader.
        let campaign_still_live = inner
            .nodes
            .get(&candidate_id)
            .map(|node| node.current_term == term && node.voted_for == Some(candidate_id))
            .unwrap_or(false);
        if !campaign_still_live {
            return Ok(false);
        }
        let required = inner.required_majority();
        if grants < required {
            if let Some(candidate) = inner.nodes.get_mut(&candidate_id) {
                candidate.role = RaftRole::Follower;
            }
            inner.persist_configured_wal()?;
            return Ok(false);
        }
        inner.promote_elected_leader(candidate_id, term)?;
        inner.persist_configured_wal()?;
        Ok(true)
    }

    /// A peer answered with a newer term: abandon the campaign and step down, so this
    /// node cannot go on to install itself as a stale-term leader.
    pub fn observe_higher_term(&self, node_id: RaftNodeId, term: u64) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let bumped = {
            let node = inner
                .nodes
                .get_mut(&node_id)
                .ok_or(RaftError::NodeNotFound(node_id))?;
            if term > node.current_term {
                node.current_term = term;
                node.voted_for = None;
                // Dropping the role (not just the term) is what actually stops the timer
                // loop from continuing to act as leader on a superseded term.
                node.role = RaftRole::Follower;
                true
            } else {
                false
            }
        };
        if bumped {
            inner.persist_configured_wal()?;
        }
        Ok(())
    }

    pub fn elect_leader(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        inner.elect_leader(node_id)?;
        inner.persist_configured_wal()
    }

    pub fn begin_leader_transfer(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        inner.ensure_live_leader()?;
        let leader_commit_index = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?
            .commit_index;
        let logical_time_ms = inner.logical_time_ms;
        let candidate = inner
            .nodes
            .get_mut(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        candidate.pipeline_state.transfer_leader_requests = candidate
            .pipeline_state
            .transfer_leader_requests
            .saturating_add(1);
        if !candidate.alive {
            candidate.pipeline_state.transfer_leader_rejected = candidate
                .pipeline_state
                .transfer_leader_rejected
                .saturating_add(1);
            inner.persist_configured_wal()?;
            return Err(RaftError::NodeNotFound(node_id));
        }
        if candidate.commit_index < leader_commit_index {
            let replica_commit_index = candidate.commit_index;
            candidate.pipeline_state.transfer_leader_rejected = candidate
                .pipeline_state
                .transfer_leader_rejected
                .saturating_add(1);
            inner.persist_configured_wal()?;
            return Err(RaftError::ReplicaLagging {
                replica_id: node_id,
                replica_commit_index,
                leader_commit_index,
            });
        }
        candidate.pipeline_state.transfer_leader_target = true;
        candidate.pipeline_state.transfer_leader_started_ms = Some(logical_time_ms);
        candidate.pipeline_state.transfer_leader_elapsed_ms = 0;
        candidate.pipeline_state.transfer_leader_accepted = candidate
            .pipeline_state
            .transfer_leader_accepted
            .saturating_add(1);
        inner.persist_configured_wal()
    }

    pub fn transfer_leader(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        inner.ensure_live_leader()?;
        let leader_commit_index = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?
            .commit_index;
        let logical_time_ms = inner.logical_time_ms;
        let candidate = inner
            .nodes
            .get_mut(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        candidate.pipeline_state.transfer_leader_requests = candidate
            .pipeline_state
            .transfer_leader_requests
            .saturating_add(1);
        if !candidate.alive {
            candidate.pipeline_state.transfer_leader_rejected = candidate
                .pipeline_state
                .transfer_leader_rejected
                .saturating_add(1);
            inner.persist_configured_wal()?;
            return Err(RaftError::NodeNotFound(node_id));
        }
        if candidate.commit_index < leader_commit_index {
            let replica_commit_index = candidate.commit_index;
            candidate.pipeline_state.transfer_leader_rejected = candidate
                .pipeline_state
                .transfer_leader_rejected
                .saturating_add(1);
            inner.persist_configured_wal()?;
            return Err(RaftError::ReplicaLagging {
                replica_id: node_id,
                replica_commit_index,
                leader_commit_index,
            });
        }
        candidate.pipeline_state.transfer_leader_target = true;
        candidate.pipeline_state.transfer_leader_started_ms = Some(logical_time_ms);
        candidate.pipeline_state.transfer_leader_elapsed_ms = 0;
        candidate.pipeline_state.transfer_leader_accepted = candidate
            .pipeline_state
            .transfer_leader_accepted
            .saturating_add(1);
        inner.elect_leader(node_id)?;
        if let Some(target) = inner.nodes.get_mut(&node_id) {
            target.pipeline_state.transfer_leader_completed = target
                .pipeline_state
                .transfer_leader_completed
                .saturating_add(1);
            target.pipeline_state.transfer_leader_target = false;
            target.pipeline_state.transfer_leader_started_ms = None;
            target.pipeline_state.transfer_leader_elapsed_ms = 0;
        }
        inner.persist_configured_wal()
    }

    pub fn promote_if_leader_down(&self) -> Result<RaftNodeId, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        if inner
            .nodes
            .get(&inner.leader_id)
            .map(|node| node.alive)
            .unwrap_or(false)
        {
            return Ok(inner.leader_id);
        }
        inner.promote_best_live_follower()?;
        inner.persist_configured_wal()?;
        Ok(inner.leader_id)
    }

    pub fn failover_primary(&self) -> Result<RaftFailoverReport, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let old_leader_id = inner.leader_id;
        if inner
            .nodes
            .get(&old_leader_id)
            .map(|node| node.alive && node.role == RaftRole::Leader)
            .unwrap_or(false)
        {
            return Ok(inner.failover_report(old_leader_id));
        }
        inner.promote_best_live_follower()?;
        inner.persist_configured_wal()?;
        Ok(inner.failover_report(old_leader_id))
    }

    pub fn tick_election(&self) -> Result<RaftTickOutcome, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        if inner
            .nodes
            .get(&inner.leader_id)
            .map(|node| node.alive && node.role == RaftRole::Leader)
            .unwrap_or(false)
        {
            inner.election_elapsed_tick = 0;
            inner.renew_leader_lease();
            return Ok(RaftTickOutcome::LeaderAlive {
                leader_id: inner.leader_id,
            });
        }

        inner.election_elapsed_tick += 1;
        let timeout_tick = u64::from(inner.config.election_cycle_tick);
        if inner.election_elapsed_tick < timeout_tick || !inner.local_shadow_election {
            // With shadow election off, the runtime's own campaign owns promotion; all
            // this tick still does is age the election clock.
            return Ok(RaftTickOutcome::ElectionPending {
                elapsed_tick: inner.election_elapsed_tick,
                timeout_tick,
            });
        }

        let candidate_id = inner.best_live_candidate()?;
        if inner.config.enable_pre_vote {
            inner.read_safety_state.pre_vote_requests =
                inner.read_safety_state.pre_vote_requests.saturating_add(1);
            if !inner.pre_vote_would_win(candidate_id)? {
                inner.read_safety_state.pre_vote_rejected =
                    inner.read_safety_state.pre_vote_rejected.saturating_add(1);
                if let Some(candidate) = inner.nodes.get_mut(&candidate_id) {
                    candidate.pipeline_state.pre_vote_rejections = candidate
                        .pipeline_state
                        .pre_vote_rejections
                        .saturating_add(1);
                }
                inner.election_elapsed_tick = 0;
                inner.persist_configured_wal()?;
                return Ok(RaftTickOutcome::PreVoteRejected { candidate_id });
            }
            inner.read_safety_state.pre_vote_accepted =
                inner.read_safety_state.pre_vote_accepted.saturating_add(1);
        }
        inner.elect_leader(candidate_id)?;
        inner.election_elapsed_tick = 0;
        inner.persist_configured_wal()?;
        let term = inner
            .nodes
            .get(&candidate_id)
            .map(|node| node.current_term)
            .unwrap_or_default();
        Ok(RaftTickOutcome::LeaderElected {
            leader_id: candidate_id,
            term,
        })
    }
}
