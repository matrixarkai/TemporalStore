use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::raft::RaftNodeId;
use crate::types::ShardId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardRole {
    Primary,
    Secondary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardReplicaState {
    Creating,
    Loading,
    Normal,
    Freezing,
    Frozen,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardReplica {
    pub shard_id: ShardId,
    pub replica_id: u64,
    pub node_id: RaftNodeId,
    pub role: ShardRole,
    pub state: ShardReplicaState,
    pub load_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardMovePlan {
    pub shard_id: ShardId,
    pub source_replica_id: u64,
    pub source_node_id: RaftNodeId,
    pub target_replica_id: u64,
    pub target_node_id: RaftNodeId,
    pub load_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RebalanceStep {
    LoadTarget {
        shard_id: ShardId,
        replica_id: u64,
        node_id: RaftNodeId,
        load_version: u64,
    },
    UpdateMembership {
        shard_id: ShardId,
        active_replica_ids: Vec<u64>,
        primary_replica_id: u64,
        membership_version: u64,
    },
    FreezeSource {
        shard_id: ShardId,
        replica_id: u64,
        node_id: RaftNodeId,
    },
    UnloadSource {
        shard_id: ShardId,
        replica_id: u64,
        node_id: RaftNodeId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RebalanceOptions {
    pub max_moves_per_round: usize,
    pub partition_count_safe_gap: usize,
}

impl Default for RebalanceOptions {
    fn default() -> Self {
        Self {
            max_moves_per_round: 10,
            partition_count_safe_gap: 0,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RebalanceController {
    replicas: BTreeMap<u64, ShardReplica>,
    membership_version: u64,
    next_replica_id: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RebalanceError {
    #[error("replica not found: {0}")]
    ReplicaNotFound(u64),
    #[error("cannot safely move primary replica {0} without a normal secondary")]
    CannotMovePrimary(u64),
    #[error("target node already hosts shard {shard_id}: node={node_id}")]
    TargetAlreadyHostsShard {
        shard_id: ShardId,
        node_id: RaftNodeId,
    },
    #[error("no target node is available")]
    NoTargetNode,
    #[error("move target is not loading: {0}")]
    TargetNotLoading(u64),
}

impl RebalanceController {
    pub fn with_replicas(replicas: impl IntoIterator<Item = ShardReplica>) -> Self {
        let mut max_replica_id = 0;
        let mut map = BTreeMap::new();
        for replica in replicas {
            max_replica_id = max_replica_id.max(replica.replica_id);
            map.insert(replica.replica_id, replica);
        }
        Self {
            replicas: map,
            membership_version: 1,
            next_replica_id: max_replica_id + 1,
        }
    }

    pub fn replicas(&self) -> Vec<ShardReplica> {
        self.replicas.values().cloned().collect()
    }

    pub fn membership_version(&self) -> u64 {
        self.membership_version
    }

    pub fn node_loads(&self) -> BTreeMap<RaftNodeId, usize> {
        let mut loads = BTreeMap::new();
        for replica in self
            .replicas
            .values()
            .filter(|replica| matches!(replica.state, ShardReplicaState::Normal))
        {
            *loads.entry(replica.node_id).or_default() += 1;
        }
        loads
    }

    pub fn plan_rebalance_round(
        &self,
        all_node_ids: impl IntoIterator<Item = RaftNodeId>,
        options: RebalanceOptions,
    ) -> Vec<ShardMovePlan> {
        let all_nodes = all_node_ids.into_iter().collect::<BTreeSet<_>>();
        if all_nodes.len() < 2 || options.max_moves_per_round == 0 {
            return Vec::new();
        }

        let mut simulated_loads = self.node_loads();
        for node_id in &all_nodes {
            simulated_loads.entry(*node_id).or_default();
        }
        let total_normal = simulated_loads.values().sum::<usize>();
        if total_normal == 0 {
            return Vec::new();
        }
        let safe_line = total_normal.div_ceil(all_nodes.len()) + options.partition_count_safe_gap;
        let mut plans = Vec::new();
        let mut next_replica_id = self.next_replica_id;
        let mut candidates = self
            .replicas
            .values()
            .filter(|replica| replica.state == ShardReplicaState::Normal)
            .cloned()
            .collect::<Vec<_>>();
        candidates.sort_by_key(|replica| (replica.node_id, replica.shard_id, replica.replica_id));

        for source in candidates {
            if plans.len() >= options.max_moves_per_round {
                break;
            }
            if simulated_loads
                .get(&source.node_id)
                .copied()
                .unwrap_or_default()
                <= safe_line
            {
                continue;
            }
            if source.role == ShardRole::Primary && !self.has_normal_secondary(source.shard_id) {
                continue;
            }
            let Some(target_node_id) = least_loaded_target(
                &simulated_loads,
                &all_nodes,
                source.shard_id,
                &self.replicas,
            ) else {
                continue;
            };
            if target_node_id == source.node_id {
                continue;
            }
            plans.push(ShardMovePlan {
                shard_id: source.shard_id,
                source_replica_id: source.replica_id,
                source_node_id: source.node_id,
                target_replica_id: next_replica_id,
                target_node_id,
                load_version: source.load_version + 1,
            });
            next_replica_id += 1;
            *simulated_loads.entry(source.node_id).or_default() -= 1;
            *simulated_loads.entry(target_node_id).or_default() += 1;
        }

        plans
    }

    pub fn begin_move(
        &mut self,
        plan: &ShardMovePlan,
    ) -> Result<Vec<RebalanceStep>, RebalanceError> {
        let source = self
            .replicas
            .get(&plan.source_replica_id)
            .ok_or(RebalanceError::ReplicaNotFound(plan.source_replica_id))?;
        if source.role == ShardRole::Primary && !self.has_normal_secondary(source.shard_id) {
            return Err(RebalanceError::CannotMovePrimary(source.replica_id));
        }
        if self.replicas.values().any(|replica| {
            replica.shard_id == plan.shard_id
                && replica.node_id == plan.target_node_id
                && replica.state != ShardReplicaState::Frozen
        }) {
            return Err(RebalanceError::TargetAlreadyHostsShard {
                shard_id: plan.shard_id,
                node_id: plan.target_node_id,
            });
        }

        self.replicas.insert(
            plan.target_replica_id,
            ShardReplica {
                shard_id: plan.shard_id,
                replica_id: plan.target_replica_id,
                node_id: plan.target_node_id,
                role: ShardRole::Secondary,
                state: ShardReplicaState::Loading,
                load_version: plan.load_version,
            },
        );
        self.next_replica_id = self.next_replica_id.max(plan.target_replica_id + 1);

        Ok(vec![RebalanceStep::LoadTarget {
            shard_id: plan.shard_id,
            replica_id: plan.target_replica_id,
            node_id: plan.target_node_id,
            load_version: plan.load_version,
        }])
    }

    pub fn finish_target_load(
        &mut self,
        target_replica_id: u64,
    ) -> Result<Vec<RebalanceStep>, RebalanceError> {
        let (shard_id, source_replica_id, source_node_id, primary_replica_id) = {
            let target = self
                .replicas
                .get(&target_replica_id)
                .ok_or(RebalanceError::ReplicaNotFound(target_replica_id))?;
            if target.state != ShardReplicaState::Loading {
                return Err(RebalanceError::TargetNotLoading(target_replica_id));
            }
            let source = self
                .replicas
                .values()
                .find(|replica| {
                    replica.shard_id == target.shard_id
                        && replica.state == ShardReplicaState::Normal
                        && replica.node_id != target.node_id
                })
                .ok_or(RebalanceError::NoTargetNode)?;
            let primary_replica_id = self
                .primary_replica_id(target.shard_id)
                .unwrap_or(target_replica_id);
            (
                target.shard_id,
                source.replica_id,
                source.node_id,
                primary_replica_id,
            )
        };

        self.replicas
            .get_mut(&target_replica_id)
            .expect("checked target")
            .state = ShardReplicaState::Normal;
        self.replicas
            .get_mut(&source_replica_id)
            .expect("checked source")
            .state = ShardReplicaState::Freezing;
        self.membership_version += 1;
        let active_replica_ids = self.active_replica_ids(shard_id);

        Ok(vec![
            RebalanceStep::UpdateMembership {
                shard_id,
                active_replica_ids,
                primary_replica_id,
                membership_version: self.membership_version,
            },
            RebalanceStep::FreezeSource {
                shard_id,
                replica_id: source_replica_id,
                node_id: source_node_id,
            },
        ])
    }

    pub fn finish_source_freeze(
        &mut self,
        source_replica_id: u64,
    ) -> Result<Vec<RebalanceStep>, RebalanceError> {
        let source = self
            .replicas
            .get_mut(&source_replica_id)
            .ok_or(RebalanceError::ReplicaNotFound(source_replica_id))?;
        source.state = ShardReplicaState::Frozen;
        self.membership_version += 1;
        Ok(vec![RebalanceStep::UnloadSource {
            shard_id: source.shard_id,
            replica_id: source.replica_id,
            node_id: source.node_id,
        }])
    }

    pub fn rollback_move(&mut self, target_replica_id: u64) -> Result<(), RebalanceError> {
        let target = self
            .replicas
            .get(&target_replica_id)
            .ok_or(RebalanceError::ReplicaNotFound(target_replica_id))?;
        if target.state == ShardReplicaState::Loading || target.state == ShardReplicaState::Creating
        {
            self.replicas.remove(&target_replica_id);
        }
        Ok(())
    }

    fn has_normal_secondary(&self, shard_id: ShardId) -> bool {
        self.replicas.values().any(|replica| {
            replica.shard_id == shard_id
                && replica.role == ShardRole::Secondary
                && replica.state == ShardReplicaState::Normal
        })
    }

    fn primary_replica_id(&self, shard_id: ShardId) -> Option<u64> {
        self.replicas
            .values()
            .find(|replica| replica.shard_id == shard_id && replica.role == ShardRole::Primary)
            .map(|replica| replica.replica_id)
    }

    fn active_replica_ids(&self, shard_id: ShardId) -> Vec<u64> {
        self.replicas
            .values()
            .filter(|replica| {
                replica.shard_id == shard_id
                    && matches!(
                        replica.state,
                        ShardReplicaState::Normal | ShardReplicaState::Freezing
                    )
            })
            .map(|replica| replica.replica_id)
            .collect()
    }
}

fn least_loaded_target(
    loads: &BTreeMap<RaftNodeId, usize>,
    all_nodes: &BTreeSet<RaftNodeId>,
    shard_id: ShardId,
    replicas: &BTreeMap<u64, ShardReplica>,
) -> Option<RaftNodeId> {
    all_nodes
        .iter()
        .filter(|node_id| {
            !replicas.values().any(|replica| {
                replica.shard_id == shard_id
                    && replica.node_id == **node_id
                    && replica.state != ShardReplicaState::Frozen
            })
        })
        .min_by_key(|node_id| (loads.get(node_id).copied().unwrap_or_default(), **node_id))
        .copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replica(
        replica_id: u64,
        shard_id: ShardId,
        node_id: RaftNodeId,
        role: ShardRole,
    ) -> ShardReplica {
        ShardReplica {
            shard_id,
            replica_id,
            node_id,
            role,
            state: ShardReplicaState::Normal,
            load_version: 1,
        }
    }

    #[test]
    fn rebalance_moves_from_overloaded_node_to_low_load_node() {
        let mut controller = RebalanceController::with_replicas([
            replica(1, 10, 1, ShardRole::Primary),
            replica(2, 10, 2, ShardRole::Secondary),
            replica(3, 20, 1, ShardRole::Primary),
            replica(4, 20, 2, ShardRole::Secondary),
            replica(5, 30, 1, ShardRole::Primary),
            replica(6, 30, 2, ShardRole::Secondary),
        ]);

        let plans = controller.plan_rebalance_round(
            [1, 2, 3],
            RebalanceOptions {
                max_moves_per_round: 1,
                partition_count_safe_gap: 0,
            },
        );
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].source_node_id, 1);
        assert_eq!(plans[0].target_node_id, 3);

        assert!(matches!(
            controller.begin_move(&plans[0]).unwrap().as_slice(),
            [RebalanceStep::LoadTarget { .. }]
        ));
        let steps = controller
            .finish_target_load(plans[0].target_replica_id)
            .unwrap();
        assert!(matches!(steps[0], RebalanceStep::UpdateMembership { .. }));
        assert!(matches!(steps[1], RebalanceStep::FreezeSource { .. }));
        let unload = controller
            .finish_source_freeze(plans[0].source_replica_id)
            .unwrap();
        assert!(matches!(unload[0], RebalanceStep::UnloadSource { .. }));
        assert_eq!(
            controller
                .replicas()
                .into_iter()
                .find(|replica| replica.replica_id == plans[0].source_replica_id)
                .unwrap()
                .state,
            ShardReplicaState::Frozen
        );
    }

    #[test]
    fn rebalance_does_not_move_lonely_primary() {
        let controller = RebalanceController::with_replicas([
            replica(1, 10, 1, ShardRole::Primary),
            replica(2, 20, 1, ShardRole::Primary),
        ]);

        assert!(controller
            .plan_rebalance_round([1, 2], RebalanceOptions::default())
            .is_empty());
    }

    #[test]
    fn begin_move_rejects_target_that_already_hosts_same_shard() {
        let mut controller = RebalanceController::with_replicas([
            replica(1, 10, 1, ShardRole::Primary),
            replica(2, 10, 2, ShardRole::Secondary),
        ]);
        let err = controller
            .begin_move(&ShardMovePlan {
                shard_id: 10,
                source_replica_id: 1,
                source_node_id: 1,
                target_replica_id: 3,
                target_node_id: 2,
                load_version: 2,
            })
            .unwrap_err();
        assert_eq!(
            err,
            RebalanceError::TargetAlreadyHostsShard {
                shard_id: 10,
                node_id: 2,
            }
        );
    }
}
