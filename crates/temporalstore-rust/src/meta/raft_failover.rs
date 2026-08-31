// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Metaserver-driven raft leader failover planning.
//!
//! The metaserver's failure detector ([`SingleNodeMeta::freeze_stale_resources`]
//! in `meta/lifecycle.rs`) marks a datanode whose heartbeats have gone stale as
//! [`MetaEntityState::Frozen`]. On its own that freeze is inert for a
//! raft-replicated shard: the surviving replicas are never told their leader is
//! gone, so their raft view keeps the dead node marked alive, `tick_election`
//! (`raft/cluster_election.rs`) never observes a dead leader, and writes to that
//! group stall indefinitely. That stall is the "freeze" this feature fixes.
//!
//! This module adds the missing detection->trigger bridge. Given the current
//! server membership, [`compute_raft_failover_triggers`] produces, for every
//! frozen server, the set of still-live (Normal) servers the metaserver should
//! notify so raft can re-elect. The metaserver binary drives the plan by POSTing
//! the dead node's liveness (`node_id`, `alive = false`) to each surviving
//! replica and then asking it to run its native failover
//! (`/raft/admin/failover` -> `RaftCluster::failover_primary`). That native path
//! is guarded by raft's own safety checks in `elect_leader`
//! (`raft/cluster_inner.rs`): a live majority is required (a minority partition
//! cannot elect -> no split-brain) and the promoted candidate's log must be
//! up to date (`candidate_log_would_win` -> no committed-write loss). The
//! metaserver never selects a leader itself.
//!
//! The planner is pure and deterministic (frozen nodes ordered by node id then
//! address, live targets ordered by address) so it is fully unit-testable.

use super::*;

/// A single planned raft failover trigger: one dead (frozen) server whose raft
/// leadership must move, plus the live servers to notify so a surviving replica
/// re-elects through its own native, safety-guarded election path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftFailoverTrigger {
    /// Raft node id of the dead/frozen server, marked not-alive on the replicas.
    pub dead_node_id: u64,
    /// Address of the dead/frozen server (for logging and per-episode dedup).
    pub dead_server_addr: String,
    /// Live (Normal) server addresses to drive the native raft failover on.
    pub live_targets: Vec<String>,
}

/// Pure planner: for every frozen server produce a failover trigger listing the
/// live (Normal) servers to notify. Deterministic — triggers ordered by dead
/// node id then address; targets ordered by address. Returns an empty plan when
/// there are no frozen servers or no live targets (nothing safe to drive).
/// A server as the failover planner sees it.
///
/// The planner needs to know which node is which and whether it is serving. A
/// server record also carries one shard load and one serving state per shard
/// the node holds, and cloning those to read three fields cost 4.7ms a tick on
/// 32 nodes holding 1000 shards each.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailoverMember {
    pub server_addr: String,
    pub node_id: u64,
    pub state: MetaEntityState,
}

impl FailoverMember {
    pub fn of(server: &ServerMetaInfo) -> Self {
        Self {
            server_addr: server.server_addr.clone(),
            node_id: server.node_id,
            state: server.state,
        }
    }
}

pub fn compute_raft_failover_triggers(servers: &[FailoverMember]) -> Vec<RaftFailoverTrigger> {
    let mut live_targets: Vec<String> = servers
        .iter()
        .filter(|server| server.state == MetaEntityState::Normal)
        .map(|server| server.server_addr.clone())
        .collect();
    live_targets.sort();
    live_targets.dedup();
    if live_targets.is_empty() {
        // No surviving replica to elect onto — leave the group as-is rather than
        // fabricate a leader (mirrors compute_auto_rebalance's empty-live guard).
        return Vec::new();
    }

    let mut dead: Vec<&FailoverMember> = servers
        .iter()
        .filter(|server| server.state == MetaEntityState::Frozen)
        .collect();
    dead.sort_by(|a, b| {
        a.node_id
            .cmp(&b.node_id)
            .then_with(|| a.server_addr.cmp(&b.server_addr))
    });

    dead.into_iter()
        .map(|server| RaftFailoverTrigger {
            dead_node_id: server.node_id,
            dead_server_addr: server.server_addr.clone(),
            live_targets: live_targets.clone(),
        })
        .collect()
}

impl SingleNodeMeta {
    /// Compute the raft failover plan for the current membership: every frozen
    /// server maps to the live (Normal) servers the metaserver should drive to
    /// re-elect. Pure read of the shared meta state.
    pub fn plan_raft_failover(&self) -> Vec<RaftFailoverTrigger> {
        let state = self.inner.read().expect("meta lock poisoned");
        let servers = state
            .servers
            .values()
            .map(FailoverMember::of)
            .collect::<Vec<_>>();
        compute_raft_failover_triggers(&servers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server(addr: &str, node_id: u64, state: MetaEntityState) -> FailoverMember {
        FailoverMember::of(&server_record(addr, node_id, state))
    }

    fn server_record(addr: &str, node_id: u64, state: MetaEntityState) -> ServerMetaInfo {
        ServerMetaInfo {
            registered_at_ms: 0,
            reported_record_count: 0,
            reported_storage_bytes: 0,
            numa_nodes: Vec::new(),
            load_key_count: 0,
            load_memory_bytes: 0,
            worst_shard_state_penalty: 0,
            freeze_reason: FreezeReason::Unspecified,
            server_addr: addr.to_string(),
            node_id,
            location: "zone-a".to_string(),
            state,
            last_heartbeat_ms: 1,
            frozen_since_ms: 0,
            freeze_cooldown_until_ms: 0,
            boot_time_ms: 1,
            reported_boot_time_ms: 0,
            reboot_detected: false,
            reports_shard_states: false,
            binary_version: "test".to_string(),
            shard_loads: Vec::new(),
            shard_stat_loads: Vec::new(),
            runtime_load: ServerRuntimeLoad::default(),
            shard_states: Vec::new(),
        }
    }

    #[test]
    fn frozen_leader_yields_a_trigger_targeting_every_live_server() {
        let triggers = compute_raft_failover_triggers(&[
            server("node-a", 1, MetaEntityState::Frozen),
            server("node-b", 2, MetaEntityState::Normal),
            server("node-c", 3, MetaEntityState::Normal),
        ]);
        assert_eq!(triggers.len(), 1);
        assert_eq!(triggers[0].dead_node_id, 1);
        assert_eq!(triggers[0].dead_server_addr, "node-a");
        // Live targets are the surviving Normal servers, sorted by address.
        assert_eq!(triggers[0].live_targets, vec!["node-b", "node-c"]);
    }

    #[test]
    fn no_frozen_servers_yields_no_plan() {
        let triggers = compute_raft_failover_triggers(&[
            server("node-a", 1, MetaEntityState::Normal),
            server("node-b", 2, MetaEntityState::Normal),
        ]);
        assert!(triggers.is_empty());
    }

    #[test]
    fn no_live_servers_yields_no_plan() {
        // Everything frozen: there is nowhere safe to elect, so no trigger.
        let triggers = compute_raft_failover_triggers(&[
            server("node-a", 1, MetaEntityState::Frozen),
            server("node-b", 2, MetaEntityState::Frozen),
        ]);
        assert!(triggers.is_empty());
    }

    #[test]
    fn dropped_servers_are_neither_targets_nor_triggers() {
        let triggers = compute_raft_failover_triggers(&[
            server("node-a", 1, MetaEntityState::Frozen),
            server("node-b", 2, MetaEntityState::Normal),
            server("node-c", 3, MetaEntityState::Dropped),
        ]);
        assert_eq!(triggers.len(), 1);
        // node-c (Dropped) is not a valid election target.
        assert_eq!(triggers[0].live_targets, vec!["node-b"]);
    }

    #[test]
    fn multiple_frozen_servers_are_ordered_deterministically_by_node_id() {
        let triggers = compute_raft_failover_triggers(&[
            server("node-c", 3, MetaEntityState::Frozen),
            server("node-a", 1, MetaEntityState::Frozen),
            server("node-b", 2, MetaEntityState::Normal),
        ]);
        let dead_ids = triggers
            .iter()
            .map(|trigger| trigger.dead_node_id)
            .collect::<Vec<_>>();
        assert_eq!(dead_ids, vec![1, 3]);
        for trigger in &triggers {
            assert_eq!(trigger.live_targets, vec!["node-b"]);
        }
    }
}
