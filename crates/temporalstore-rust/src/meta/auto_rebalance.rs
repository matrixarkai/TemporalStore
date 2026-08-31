// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Metaserver-driven automatic shard placement.
//!
//! The metaserver owns a plain shard→owner map ([`ShardLocation`]). On its own
//! that map is only ever rewritten by a datanode self-registering a shard, so a
//! membership change (a node going down, or a fresh node joining) never moves a
//! shard: an orphaned shard keeps pointing at a dead owner, and a new node is
//! never given any load.
//!
//! This module adds the missing placement brain. Given the current shard→owner
//! map and the set of live (Normal-state) servers, [`compute_auto_rebalance`]
//! produces the reassignments needed to (1) evacuate every shard whose owner is
//! no longer serving onto a live server, and (2) spread the shards roughly
//! evenly so a freshly-joined server actually receives load. The metaserver
//! binary drives the resulting plan — telling the target node to load the shard
//! and rewriting the owner map — so clients stop resolving to a dead node.
//!
//! The planner is pure and deterministic (ties break on the lowest server
//! address / shard id) so it is fully unit-testable and produces the same plan
//! on every node.

use std::collections::BTreeSet;

use super::*;

/// Why a shard is being reassigned to a new owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShardReassignmentReason {
    /// The shard's current owner is not a live (Normal) server — it was frozen,
    /// dropped, or is absent — so the shard must move to a live server.
    OwnerUnavailable,
    /// The shard's owner is live, but load is uneven; the shard moves to spread
    /// shards more evenly across live servers (e.g. onto a newly-joined node).
    Rebalance,
    /// The shard's owner is live and healthy but is not serving this shard, so
    /// every read routed to it misses. Produced by [`ShardChecker`]; evacuation
    /// cannot catch it because the owner itself is perfectly available -- it is
    /// the shard that is gone, not the server.
    OwnerDiverged,
    /// The shard's owner is live but sits outside the location its table asked
    /// for, so the shard moves back into that location. Only produced by
    /// [`compute_placement_aware_rebalance`]; evacuation cannot catch this case
    /// because the owner is healthy.
    LocationViolation,
}

impl ShardReassignmentReason {
    /// Stable label for metrics and logs. Matches the serde representation, so
    /// a dashboard and a JSON payload name the same cause the same way.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OwnerUnavailable => "owner_unavailable",
            Self::Rebalance => "rebalance",
            Self::LocationViolation => "location_violation",
            Self::OwnerDiverged => "owner_diverged",
        }
    }
}

/// A single planned shard ownership change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardReassignment {
    pub shard_id: ShardId,
    /// The previous owner address, if the shard was registered before.
    #[serde(default)]
    pub from_server: Option<String>,
    /// The chosen new owner (always a live/Normal server).
    pub to_server: String,
    pub reason: ShardReassignmentReason,
}

/// Options controlling how aggressively the planner rebalances.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutoRebalanceOptions {
    /// Cap on the number of reassignments produced in one planning round.
    pub max_moves: usize,
    /// Enable the load-balancing pass (spread shards onto under-loaded live
    /// servers). Evacuation of unavailable owners always runs regardless.
    pub balance_load: bool,
    /// Confine placement to the location the shard's table prefers, and pull
    /// back shards that have drifted out of it. Read only by
    /// [`compute_placement_aware_rebalance`]; the flat planner has no notion of
    /// location and ignores this.
    pub location_scoped: bool,
    /// Balance each table across its eligible servers separately, instead of
    /// balancing total shard counts. Without it a table can be entirely homed on
    /// one node while global counts look even. Read only by
    /// [`compute_placement_aware_rebalance`].
    pub per_table_balance: bool,
    /// How many shards above its fair share a server may hold before it sheds
    /// load. Damps churn: without it the planner chases a one-shard imbalance
    /// and moves data every round. Read only by
    /// [`compute_placement_aware_rebalance`].
    pub balance_safe_gap: usize,
}

impl Default for AutoRebalanceOptions {
    fn default() -> Self {
        Self {
            max_moves: 64,
            balance_load: true,
            location_scoped: true,
            per_table_balance: true,
            balance_safe_gap: 0,
        }
    }
}

/// Pure planner: compute the reassignments that bring `shard_owners` onto the
/// set of `live_servers`, evacuating unavailable owners first and then (if
/// enabled) balancing load. `shard_owners` maps shard id → current owner addr
/// (owner may be a dead/frozen server, not present in `live_servers`).
pub fn compute_auto_rebalance(
    shard_owners: &BTreeMap<ShardId, String>,
    live_servers: &BTreeSet<String>,
    options: AutoRebalanceOptions,
) -> Vec<ShardReassignment> {
    let mut plans = Vec::new();
    if live_servers.is_empty() {
        // Nowhere to place shards; leave the map untouched rather than drop routes.
        return plans;
    }

    // Working owner map + per-live-server load, seeded from the current state.
    let mut owner: BTreeMap<ShardId, Option<String>> = BTreeMap::new();
    let mut load: BTreeMap<String, usize> =
        live_servers.iter().map(|addr| (addr.clone(), 0)).collect();
    for (shard_id, current) in shard_owners {
        if live_servers.contains(current) {
            *load.get_mut(current).expect("seeded") += 1;
            owner.insert(*shard_id, Some(current.clone()));
        } else {
            owner.insert(*shard_id, None);
        }
    }

    // Pass 1 — evacuate every shard whose owner is unavailable onto the
    // least-loaded live server (ties: lowest address).
    for (shard_id, current) in shard_owners {
        if plans.len() >= options.max_moves {
            return plans;
        }
        if live_servers.contains(current) {
            continue;
        }
        let target = least_loaded_server(&load);
        *load.get_mut(&target).expect("target is live") += 1;
        owner.insert(*shard_id, Some(target.clone()));
        plans.push(ShardReassignment {
            shard_id: *shard_id,
            from_server: Some(current.clone()),
            to_server: target,
            reason: ShardReassignmentReason::OwnerUnavailable,
        });
    }

    // Pass 2 — spread load: while the busiest live server has more than one
    // shard beyond the idlest, move a shard from busiest → idlest. Deterministic
    // and guaranteed to terminate (each move strictly reduces the spread).
    if options.balance_load {
        while plans.len() < options.max_moves {
            let Some((busy_addr, busy_load)) = most_loaded_server(&load) else {
                break;
            };
            let idle_addr = least_loaded_server(&load);
            let idle_load = load.get(&idle_addr).copied().unwrap_or_default();
            if busy_load <= idle_load + 1 {
                break;
            }
            // Pick a shard currently on the busy server (highest id, for
            // determinism) to move to the idle server.
            let Some(shard_id) = owner
                .iter()
                .filter(|(_, owner_addr)| owner_addr.as_deref() == Some(busy_addr.as_str()))
                .map(|(shard_id, _)| *shard_id)
                .max()
            else {
                break;
            };
            let from = busy_addr.clone();
            owner.insert(shard_id, Some(idle_addr.clone()));
            *load.get_mut(&from).expect("busy is live") -= 1;
            *load.get_mut(&idle_addr).expect("idle is live") += 1;
            plans.push(ShardReassignment {
                shard_id,
                from_server: Some(from),
                to_server: idle_addr,
                reason: ShardReassignmentReason::Rebalance,
            });
        }
    }

    plans
}

fn least_loaded_server(load: &BTreeMap<String, usize>) -> String {
    load.iter()
        .min_by(|(addr_a, load_a), (addr_b, load_b)| {
            load_a.cmp(load_b).then_with(|| addr_a.cmp(addr_b))
        })
        .map(|(addr, _)| addr.clone())
        .expect("load map is non-empty when live servers exist")
}

fn most_loaded_server(load: &BTreeMap<String, usize>) -> Option<(String, usize)> {
    load.iter()
        .max_by(|(addr_a, load_a), (addr_b, load_b)| {
            load_a.cmp(load_b).then_with(|| addr_b.cmp(addr_a))
        })
        .map(|(addr, count)| (addr.clone(), *count))
}

impl SingleNodeMeta {
    /// Snapshot of the current shard→owner locations.
    pub fn shard_locations(&self) -> Vec<ShardLocation> {
        let state = self.inner.read().expect("meta lock poisoned");
        let mut locations = state.shards.values().cloned().collect::<Vec<_>>();
        locations.sort_by_key(|location| location.shard_id);
        locations
    }

    /// Compute the auto-rebalance plan for the current membership using default
    /// options. Only Normal-state servers are eligible placement targets.
    pub fn plan_auto_rebalance(&self) -> Vec<ShardReassignment> {
        self.plan_auto_rebalance_with_options(AutoRebalanceOptions::default())
    }

    pub fn plan_auto_rebalance_with_options(
        &self,
        options: AutoRebalanceOptions,
    ) -> Vec<ShardReassignment> {
        let state = self.inner.read().expect("meta lock poisoned");
        let live_servers = state
            .servers
            .values()
            .filter(|server| server.state == MetaEntityState::Normal)
            .map(|server| server.server_addr.clone())
            .collect::<BTreeSet<_>>();
        let shard_owners = serving_shard_owners(&state);
        compute_auto_rebalance(&shard_owners, &live_servers, options)
    }

    /// Apply a single reassignment to the shard→owner map, preserving any
    /// recorded snapshot ref and emitting a topology event so clients observe
    /// the new owner. This is the metaserver-driven counterpart to a datanode
    /// self-registering a shard.
    pub fn reassign_shard(&self, shard_id: ShardId, to_server: &str) -> AckResponse {
        self.reassign_shard_with_reason(shard_id, to_server, "unspecified")
    }

    /// Reassign a shard, recording why it moved so `/metrics` can distinguish a
    /// rebalance from an evacuation or a divergence repair.
    pub fn reassign_shard_with_reason(
        &self,
        shard_id: ShardId,
        to_server: &str,
        reason: &str,
    ) -> AckResponse {
        // Checked before the metric: a refused reassignment did not happen, so
        // counting it would overstate what the rebalancer actually did.
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
        self.metrics.record_reassignment(reason);
        self.record_mutation(MetaMutation::RegisterShard(RegisterShardRequest {
            registered_at_ms: 0,
            shard_id,
            server_addr: to_server.to_string(),
        }));
        let mut state = self.inner.write().expect("meta lock poisoned");
        let latest_snapshot = state
            .shards
            .get(&shard_id)
            .and_then(|location| location.latest_snapshot.clone());
        state.shards.insert(
            shard_id,
            ShardLocation {
                registered_at_ms: 0,
                preferred_location: String::new(),
                state: MetaEntityState::Normal,
                shard_id,
                server_addr: to_server.to_string(),
                latest_snapshot,
            },
        );
        ensure_server(&mut state, to_server);
        record_topology_event(
            &mut state,
            "auto_reassign_shard",
            format!("shard:{shard_id}"),
            format!("server_addr={to_server}"),
        );
        AckResponse {
            status: Status::ok(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owners(pairs: &[(ShardId, &str)]) -> BTreeMap<ShardId, String> {
        pairs
            .iter()
            .map(|(shard_id, addr)| (*shard_id, addr.to_string()))
            .collect()
    }

    fn live(addrs: &[&str]) -> BTreeSet<String> {
        addrs.iter().map(|addr| addr.to_string()).collect()
    }

    #[test]
    fn evacuates_shard_off_a_dead_owner_onto_a_live_server() {
        // shard 2's owner (node-b) is not live -> must move to the one live node.
        let plans = compute_auto_rebalance(
            &owners(&[(1, "node-a"), (2, "node-b")]),
            &live(&["node-a"]),
            AutoRebalanceOptions::default(),
        );
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].shard_id, 2);
        assert_eq!(plans[0].from_server.as_deref(), Some("node-b"));
        assert_eq!(plans[0].to_server, "node-a");
        assert_eq!(plans[0].reason, ShardReassignmentReason::OwnerUnavailable);
    }

    #[test]
    fn no_moves_when_every_owner_is_live_and_balanced() {
        let plans = compute_auto_rebalance(
            &owners(&[(1, "node-a"), (2, "node-b")]),
            &live(&["node-a", "node-b"]),
            AutoRebalanceOptions::default(),
        );
        assert!(plans.is_empty());
    }

    #[test]
    fn places_load_onto_a_freshly_joined_idle_server() {
        // node-a holds both shards; node-c just joined empty -> one shard moves.
        let plans = compute_auto_rebalance(
            &owners(&[(1, "node-a"), (2, "node-a")]),
            &live(&["node-a", "node-c"]),
            AutoRebalanceOptions::default(),
        );
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].to_server, "node-c");
        assert_eq!(plans[0].reason, ShardReassignmentReason::Rebalance);
        // Deterministic: the highest-id shard on the busy node is chosen.
        assert_eq!(plans[0].shard_id, 2);
    }

    #[test]
    fn evacuation_prefers_the_least_loaded_live_server() {
        // node-a already holds shard 1; shards 2 and 3 are orphaned. They should
        // spread: first onto the idle node-c, second onto whichever is idlest.
        let plans = compute_auto_rebalance(
            &owners(&[(1, "node-a"), (2, "dead"), (3, "dead")]),
            &live(&["node-a", "node-c"]),
            AutoRebalanceOptions::default(),
        );
        let targets = plans
            .iter()
            .map(|plan| plan.to_server.clone())
            .collect::<Vec<_>>();
        // Both orphans placed, and node-c (idle) receives the first one.
        assert_eq!(plans.iter().filter(|p| p.reason == ShardReassignmentReason::OwnerUnavailable).count(), 2);
        assert!(targets.contains(&"node-c".to_string()));
    }

    #[test]
    fn no_live_servers_yields_no_plan() {
        let plans = compute_auto_rebalance(
            &owners(&[(1, "dead-a"), (2, "dead-b")]),
            &live(&[]),
            AutoRebalanceOptions::default(),
        );
        assert!(plans.is_empty());
    }

    #[test]
    fn respects_max_moves_cap() {
        let plans = compute_auto_rebalance(
            &owners(&[(1, "dead"), (2, "dead"), (3, "dead")]),
            &live(&["node-a", "node-b"]),
            AutoRebalanceOptions {
                max_moves: 1,
                balance_load: true,
                ..AutoRebalanceOptions::default()
            },
        );
        assert_eq!(plans.len(), 1);
    }

    #[test]
    fn balance_disabled_only_evacuates() {
        // node-a holds two shards, node-c idle, but balancing is off: no move.
        let plans = compute_auto_rebalance(
            &owners(&[(1, "node-a"), (2, "node-a")]),
            &live(&["node-a", "node-c"]),
            AutoRebalanceOptions {
                max_moves: 64,
                balance_load: false,
                ..AutoRebalanceOptions::default()
            },
        );
        assert!(plans.is_empty());
    }
}
