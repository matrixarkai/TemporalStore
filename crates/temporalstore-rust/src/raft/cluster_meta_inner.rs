//! MetaRaftClusterInner methods, split from raft.rs.
use super::*;

impl MetaRaftClusterInner {
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
        let candidate = self
            .nodes
            .values()
            .filter(|node| node.alive)
            .min_by_key(|node| {
                (
                    std::cmp::Reverse(node.commit_index),
                    std::cmp::Reverse(meta_node_last_log_or_snapshot_index(node)),
                    node.id,
                )
            })
            .map(|node| node.id)
            .ok_or(RaftError::LeaderUnavailable)?;
        self.elect_leader(candidate)
    }

    pub(super) fn best_live_candidate_in(
        &self,
        allowed: &BTreeSet<RaftNodeId>,
    ) -> Result<RaftNodeId, RaftError> {
        self.nodes
            .values()
            .filter(|node| allowed.contains(&node.id) && node.alive)
            .min_by_key(|node| {
                (
                    std::cmp::Reverse(node.commit_index),
                    std::cmp::Reverse(meta_node_last_log_or_snapshot_index(node)),
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
        let leader_state = leader.state.clone();
        let leader_snapshot_index = leader.installed_snapshot_index;
        let leader_snapshot_term = leader.installed_snapshot_term;
        let mut caught_up = Vec::new();
        for node in self
            .nodes
            .values_mut()
            .filter(|node| node.alive && node.id != leader_id)
        {
            if node.commit_index < leader_commit_index
                || node.log.last().map(|entry| entry.index).unwrap_or_default()
                    < leader_log
                        .last()
                        .map(|entry| entry.index)
                        .unwrap_or_default()
            {
                install_meta_leader_snapshot_tail(
                    node,
                    leader_snapshot_index,
                    leader_snapshot_term,
                    leader_log.clone(),
                    leader_commit_index,
                    leader_state.clone(),
                );
            }
            if node.commit_index >= leader_commit_index {
                caught_up.push(node.id);
            }
        }
        Ok(caught_up)
    }

    pub(super) fn remove_node_safely(&mut self, node_id: RaftNodeId) -> Result<(), RaftError> {
        if self.nodes.len() == 1 {
            return Err(RaftError::CannotRemoveLastNode);
        }
        if !self.nodes.contains_key(&node_id) {
            return Err(RaftError::NodeNotFound(node_id));
        }
        let remaining = self
            .nodes
            .keys()
            .copied()
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
        if !self
            .nodes
            .get(&self.leader_id)
            .map(|node| node.alive && node.role == RaftRole::Leader)
            .unwrap_or(false)
        {
            return Err(RaftError::LeaderUnavailable);
        }
        let old_voters = self.nodes.keys().copied().collect::<BTreeSet<_>>();
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
            shard_id: 0,
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
            voters: self.nodes.keys().copied().collect(),
            live_voters: status.live_voters,
            majority: status.majority,
            caught_up_voters: status
                .nodes
                .into_iter()
                .filter(|node| node.alive && node.lag == 0)
                .map(|node| node.node_id)
                .collect(),
        }
    }

    pub(super) fn failover_report(&self, old_leader_id: RaftNodeId) -> RaftFailoverReport {
        let status = self.status();
        RaftFailoverReport {
            old_leader_id,
            new_leader_id: status.leader_id,
            term: status.current_term,
            commit_index: status.commit_index,
            caught_up_voters: status
                .nodes
                .into_iter()
                .filter(|node| node.alive && node.lag == 0)
                .map(|node| node.node_id)
                .collect(),
        }
    }

    pub(super) fn elect_leader(&mut self, node_id: RaftNodeId) -> Result<(), RaftError> {
        if self.config.prohibits_election {
            return Err(RaftError::ElectionProhibited);
        }
        let required = majority(self.nodes.len());
        let live = self.nodes.values().filter(|node| node.alive).count();
        if live < required {
            return Err(RaftError::NoMajority { live, required });
        }
        if !self
            .nodes
            .get(&node_id)
            .map(|node| node.alive)
            .unwrap_or(false)
        {
            return Err(RaftError::NodeNotFound(node_id));
        }
        if !self.candidate_log_would_win(node_id)? {
            let candidate_commit_index = self
                .nodes
                .get(&node_id)
                .map(|node| node.commit_index)
                .unwrap_or_default();
            return Err(RaftError::ReplicaLagging {
                replica_id: node_id,
                replica_commit_index: candidate_commit_index,
                leader_commit_index: self.leader_commit_index(),
            });
        }
        self.leader_id = node_id;
        let next_term = self
            .nodes
            .values()
            .map(|node| node.current_term)
            .max()
            .unwrap_or_default()
            + 1;
        for node in self.nodes.values_mut() {
            node.role = if node.id == node_id {
                RaftRole::Leader
            } else {
                RaftRole::Follower
            };
            node.current_term = next_term;
        }
        Ok(())
    }

    pub(super) fn candidate_log_would_win(&self, candidate_id: RaftNodeId) -> Result<bool, RaftError> {
        let candidate = self
            .nodes
            .get(&candidate_id)
            .ok_or(RaftError::NodeNotFound(candidate_id))?;
        let candidate_last_index = meta_node_last_log_or_snapshot_index(candidate);
        let candidate_last_term = meta_node_last_log_or_snapshot_term(candidate);
        let votes = self
            .nodes
            .values()
            .filter(|node| node.alive)
            .filter(|node| {
                let local_last_index = meta_node_last_log_or_snapshot_index(node);
                let local_last_term = meta_node_last_log_or_snapshot_term(node);
                (candidate_last_term, candidate_last_index) >= (local_last_term, local_last_index)
            })
            .count();
        Ok(votes >= majority(self.nodes.len()))
    }

    pub(super) fn leader_commit_index(&self) -> u64 {
        self.nodes
            .get(&self.leader_id)
            .map(|node| node.commit_index)
            .unwrap_or_default()
    }

    pub(super) fn status(&self) -> RaftClusterStatus {
        let commit_index = self.leader_commit_index();
        let current_term = self
            .nodes
            .get(&self.leader_id)
            .map(|node| node.current_term)
            .unwrap_or_default();
        let majority = majority(self.nodes.len());
        let live_voters = self.nodes.values().filter(|node| node.alive).count();
        let leader_lease_valid = self
            .nodes
            .get(&self.leader_id)
            .map(|node| node.alive && node.role == RaftRole::Leader)
            .unwrap_or(false)
            && live_voters >= majority;
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
                .map(|node| meta_node_status(node, commit_index))
                .collect(),
        }
    }
}
