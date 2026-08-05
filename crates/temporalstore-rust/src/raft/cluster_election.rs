//! RaftCluster leader election / transfer / failover methods, split from raft.rs.
use super::*;

impl RaftCluster {
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
        if inner.election_elapsed_tick < timeout_tick {
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
