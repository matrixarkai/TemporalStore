//! RaftCluster membership-change / joint-consensus / catch-up methods, split from raft.rs.
use super::*;

impl RaftCluster {
    pub fn add_node(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        self.add_node_with_role(node_id, RaftReplicaRole::Voter)
    }

    pub fn add_node_with_role(
        &self,
        node_id: RaftNodeId,
        replica_role: RaftReplicaRole,
    ) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        if inner.nodes.contains_key(&node_id) {
            return Err(RaftError::NodeAlreadyExists(node_id));
        }
        inner.ensure_live_leader()?;
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let mut node = new_node(node_id, RaftRole::Follower, inner.shard_id);
        node.replica_role = replica_role;
        node.current_term = leader.current_term;
        install_leader_snapshot_tail(
            &mut node,
            leader.installed_snapshot.clone(),
            leader.log.clone(),
            leader.commit_index,
        );
        inner.nodes.insert(node_id, node);
        match replica_role {
            RaftReplicaRole::Learner => {
                inner.membership_evidence.learner_add_count = inner
                    .membership_evidence
                    .learner_add_count
                    .saturating_add(1);
            }
            RaftReplicaRole::Witness => {
                inner.membership_evidence.witness_add_count = inner
                    .membership_evidence
                    .witness_add_count
                    .saturating_add(1);
            }
            RaftReplicaRole::Voter => {}
        }
        inner.persist_configured_wal()?;
        Ok(())
    }

    pub fn add_learner_with_auto_promote(
        &self,
        node_id: RaftNodeId,
        auto_promote: bool,
    ) -> Result<(), RaftError> {
        self.add_node_with_role(node_id, RaftReplicaRole::Learner)?;
        if auto_promote {
            self.catch_up(node_id)?;
            self.promote_learner_to_voter(node_id)?;
        }
        Ok(())
    }

    pub fn add_node_safely(&self, node_id: RaftNodeId) -> Result<RaftScaleChangeReport, RaftError> {
        self.add_node(node_id)?;
        self.catch_up_live_followers()?;
        Ok(self.scale_change_report())
    }

    pub fn promote_learner_to_voter(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let node = inner
            .nodes
            .get_mut(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        match node.replica_role {
            RaftReplicaRole::Learner => {
                node.pipeline_state.auto_promoted_from_learner = true;
                node.replica_role = RaftReplicaRole::Voter;
                inner.membership_evidence.learner_promote_count = inner
                    .membership_evidence
                    .learner_promote_count
                    .saturating_add(1);
                inner.membership_evidence.auto_promote_count = inner
                    .membership_evidence
                    .auto_promote_count
                    .saturating_add(1);
                inner.persist_configured_wal()
            }
            RaftReplicaRole::Voter => Ok(()),
            RaftReplicaRole::Witness => Err(RaftError::InvalidConfig(
                "witness replicas cannot be promoted to voter through learner promotion"
                    .to_string(),
            )),
        }
    }

    pub fn remove_node(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        if inner.voting_node_ids().len() == 1
            && inner
                .nodes
                .get(&node_id)
                .map(|node| node.replica_role.participates_in_quorum())
                .unwrap_or(false)
        {
            return Err(RaftError::CannotRemoveLastNode);
        }
        inner
            .nodes
            .remove(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        inner.membership_evidence.voter_remove_count = inner
            .membership_evidence
            .voter_remove_count
            .saturating_add(1);
        if inner.leader_id == node_id {
            inner.promote_best_live_follower()?;
        }
        inner.persist_configured_wal()?;
        Ok(())
    }

    pub fn remove_node_safely(
        &self,
        node_id: RaftNodeId,
    ) -> Result<RaftScaleChangeReport, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        inner.remove_node_safely(node_id)?;
        inner.persist_configured_wal()?;
        Ok(inner.scale_change_report())
    }

    pub fn plan_membership_change(
        &self,
        new_voters: impl IntoIterator<Item = RaftNodeId>,
    ) -> Result<RaftMembershipChangePlan, RaftError> {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        inner.plan_membership_change(new_voters)
    }

    pub fn apply_membership_change_safely(
        &self,
        new_voters: impl IntoIterator<Item = RaftNodeId>,
    ) -> Result<RaftMembershipChangeReport, RaftError> {
        let plan = self.plan_membership_change(new_voters)?;
        let joint_membership = self.begin_joint_consensus(plan.new_voters.clone())?;
        let caught_up_voters = match self.catch_up_live_followers() {
            Ok(caught_up_voters) => caught_up_voters,
            Err(err) => {
                let _ = self.abort_joint_consensus();
                return Err(err);
            }
        };
        let committed_membership = match self.commit_joint_consensus() {
            Ok(membership) => membership,
            Err(err) => {
                let _ = self.abort_joint_consensus();
                return Err(err);
            }
        };
        let status = self.status();
        Ok(RaftMembershipChangeReport {
            plan,
            joint_membership,
            committed_membership,
            caught_up_voters,
            leader_id: status.leader_id,
            commit_index: status.commit_index,
        })
    }

    pub fn begin_joint_consensus(
        &self,
        new_voters: impl IntoIterator<Item = RaftNodeId>,
    ) -> Result<JointConsensusMembership, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        if inner.joint_membership.is_some() {
            return Err(RaftError::JointConsensusInProgress);
        }
        inner.ensure_live_leader()?;
        let mut old_voters = inner.voting_node_ids();
        old_voters.sort_unstable();
        let mut new_voters = new_voters.into_iter().collect::<Vec<_>>();
        new_voters.sort_unstable();
        new_voters.dedup();
        if new_voters.is_empty() {
            return Err(RaftError::CannotRemoveLastNode);
        }
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let leader_term = leader.current_term;
        let leader_log = leader.log.clone();
        let leader_commit_index = leader.commit_index;
        let shard_id = inner.shard_id;
        for node_id in &new_voters {
            if !inner.nodes.contains_key(node_id) {
                let mut node = new_node(*node_id, RaftRole::Follower, shard_id);
                node.replica_role = RaftReplicaRole::Voter;
                node.current_term = leader_term;
                node.log = leader_log.clone();
                node.commit_index = leader_commit_index;
                apply_committed(&mut node);
                inner.nodes.insert(*node_id, node);
            }
        }
        let membership = JointConsensusMembership {
            old_voters,
            new_voters,
        };
        inner.joint_membership = Some(membership.clone());
        inner
            .membership_evidence
            .pending_joint_consensus_persist_count = inner
            .membership_evidence
            .pending_joint_consensus_persist_count
            .saturating_add(1);
        inner.persist_configured_wal()?;
        Ok(membership)
    }

    pub fn commit_joint_consensus(&self) -> Result<RaftMembership, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let membership = inner
            .joint_membership
            .take()
            .ok_or(RaftError::NoJointConsensus)?;
        if let Some((live, required)) = joint_majority_failure(&inner.nodes, &membership) {
            inner.joint_membership = Some(membership);
            return Err(RaftError::NoMajority { live, required });
        }
        let new_voters = membership
            .new_voters
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        inner
            .nodes
            .retain(|node_id, _| new_voters.contains(node_id));
        if !inner.nodes.contains_key(&inner.leader_id) {
            inner.promote_best_live_follower()?;
        }
        let raft_membership = RaftMembership {
            shard_id: inner.shard_id,
            voters: inner.voting_node_ids(),
            leader_id: inner.leader_id,
        };
        inner.persist_configured_wal()?;
        Ok(raft_membership)
    }

    pub fn abort_joint_consensus(&self) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let membership = inner
            .joint_membership
            .take()
            .ok_or(RaftError::NoJointConsensus)?;
        let old_voters = membership
            .old_voters
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        inner
            .nodes
            .retain(|node_id, _| old_voters.contains(node_id));
        if !inner.nodes.contains_key(&inner.leader_id) {
            inner.promote_best_live_follower()?;
        }
        inner.persist_configured_wal()?;
        Ok(())
    }

    pub fn joint_membership(&self) -> Option<JointConsensusMembership> {
        self.inner
            .read()
            .expect("raft cluster lock poisoned")
            .joint_membership
            .clone()
    }

    pub fn set_alive(&self, node_id: RaftNodeId, alive: bool) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let logical_time_ms = inner.logical_time_ms;
        let node = inner
            .nodes
            .get_mut(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        node.alive = alive;
        if alive {
            node.pipeline_state.offline_since_ms = None;
            node.pipeline_state.offline_elapsed_ms = 0;
            node.pipeline_state.offline_timeout_reached = false;
            node.pipeline_state.inflight_entries = 0;
            node.pipeline_state.inflight_bytes = 0;
            node.pipeline_state.append_queue_depth = 0;
            node.pipeline_state.apply_inflight_tasks = 0;
            node.pipeline_state.apply_queue_depth = 0;
        } else if node.pipeline_state.offline_since_ms.is_none() {
            node.pipeline_state.offline_since_ms = Some(logical_time_ms);
        }
        inner.persist_configured_wal()?;
        Ok(())
    }

    pub fn catch_up(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let leader_id = inner.leader_id;
        let leader = inner
            .nodes
            .get(&leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let leader_log = leader.log.clone();
        let leader_commit_index = leader.commit_index;
        let node = inner
            .nodes
            .get_mut(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        node.log = leader_log;
        node.commit_index = leader_commit_index;
        if node.replica_role.can_serve_data() {
            apply_committed(node);
        }
        inner.membership_evidence.learner_catchup_count = inner
            .membership_evidence
            .learner_catchup_count
            .saturating_add(1);
        let config = inner.config.clone();
        refresh_all_pipeline_states(&mut inner.nodes, leader_id, &config);
        inner.persist_configured_wal()?;
        Ok(())
    }

    pub fn catch_up_live_followers(&self) -> Result<Vec<RaftNodeId>, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let caught_up = inner.catch_up_live_followers()?;
        inner.persist_configured_wal()?;
        Ok(caught_up)
    }

    pub fn catch_up_live_followers_bounded(
        &self,
        max_entries_per_follower: u64,
    ) -> Result<RaftCatchUpReport, RaftError> {
        if max_entries_per_follower == 0 {
            return Err(RaftError::InvalidConfig(
                "max_entries_per_follower must be greater than zero".to_string(),
            ));
        }
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let replayed_log_entries =
            inner.catch_up_live_followers_bounded(max_entries_per_follower)?;
        inner.persist_configured_wal()?;
        let status = inner.status();
        let health = replication_health_from_status(status.clone(), 0);
        Ok(RaftCatchUpReport {
            leader_id: status.leader_id,
            leader_commit_index: status.commit_index,
            max_entries_per_follower,
            replayed_log_entries,
            caught_up_voters: health.caught_up_voters,
            lagging_voters: health.lagging_voters,
        })
    }
}
