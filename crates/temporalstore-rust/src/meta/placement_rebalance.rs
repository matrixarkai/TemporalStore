// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Placement-rule-aware shard rebalancing.
//!
//! Two parts of the metaserver decide where a shard lives, and today they
//! disagree.
//!
//! [`build_shards`] (`meta/partitioning.rs`) builds the topology a client reads.
//! It is placement-aware: it spreads a shard's replicas across distinct
//! locations and distinct hosts, because two replicas sharing a rack or a
//! machine are one fault away from being zero replicas.
//!
//! [`compute_auto_rebalance`] is the part that actually *moves* shards, and it
//! knows none of that. It sees a flat shard-to-owner map and a flat set of live
//! addresses, so it will happily:
//!
//! * evacuate a shard out of the location its table asked for, because a server
//!   in some other location happened to hold fewer shards;
//! * leave a shard sitting in a location its table no longer prefers, because
//!   the owner is live and that is the only thing evacuation checks;
//! * pile every shard of one table onto one node while the *total* shard counts
//!   look perfectly balanced, so losing that single node takes the whole table
//!   down -- which is exactly the outcome replica placement exists to prevent.
//!
//! This module supplies the placement rules the mover is missing:
//!
//! 1. **Location scoping.** A shard is only placed on a server in the location
//!    its table prefers, whenever that location has any live server. A shard
//!    already sitting outside it is moved back
//!    ([`ShardReassignmentReason::LocationViolation`]).
//! 2. **Per-table balancing.** Balance is computed per table rather than over
//!    total shard counts, so each table spreads across the nodes available to it
//!    instead of being allowed to single-home while the global picture looks
//!    even.
//! 3. **A safe gap.** A server sheds load only once it is more than
//!    [`AutoRebalanceOptions::balance_safe_gap`] shards above its domain's fair
//!    share, which stops the planner from chasing a one-shard imbalance and
//!    moving data on every round.
//!
//! Placement decisions are grouped into *balance domains*: shards that share
//! both the same set of eligible servers and (when per-table balancing is on)
//! the same table. Each domain is balanced independently, which is what makes
//! "spread this table across its own rack" expressible at all.
//!
//! Like [`compute_auto_rebalance`], the planner is pure and deterministic --
//! domains are visited in sorted order and every tie breaks on the lowest server
//! address or shard id -- so it is fully unit-testable and produces the same
//! plan on every node.

use std::collections::BTreeSet;

use super::*;

/// Eligible-set key for a shard with no location constraint.
const ANY_LOCATION: &str = "*";

/// A live placement target: an address plus the location it sits in.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PlacementTarget {
    pub server_addr: String,
    /// Location tag, or empty when the server declares none.
    pub location: String,
}

/// What the planner needs to know about one shard beyond its current owner.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardPlacement {
    /// Namespace-qualified table the shard belongs to. Balancing groups by this
    /// so no single table ends up homed on one node.
    pub table_key: String,
    /// The location the shard's table prefers, or empty for "anywhere". Honoured
    /// only while that location still has a live server: a preference is not
    /// allowed to strand a shard.
    pub preferred_location: String,
}

/// A balance domain: the shards sharing an eligible server set and (when
/// per-table balancing is on) a table, plus that eligible set.
#[derive(Debug)]
struct BalanceDomain {
    eligible: BTreeSet<String>,
    shards: BTreeSet<ShardId>,
}

/// Everything the passes need, resolved once up front so the planner is plain
/// map lookups rather than repeated placement reasoning.
struct PlacementIndex {
    /// Eligible server set per key (`ANY_LOCATION`, or a location tag).
    eligible_sets: BTreeMap<String, BTreeSet<String>>,
    /// Which eligible set each shard resolves to.
    shard_eligible: BTreeMap<ShardId, String>,
    /// Which table each shard belongs to (empty when unknown).
    shard_table: BTreeMap<ShardId, String>,
}

impl PlacementIndex {
    fn eligible(&self, shard_id: ShardId) -> &BTreeSet<String> {
        let key = self
            .shard_eligible
            .get(&shard_id)
            .map(String::as_str)
            .unwrap_or(ANY_LOCATION);
        self.eligible_sets
            .get(key)
            .expect("every resolved key was inserted")
    }

    fn table(&self, shard_id: ShardId) -> &str {
        self.shard_table
            .get(&shard_id)
            .map(String::as_str)
            .unwrap_or("")
    }
}

/// Working state threaded through the passes: who owns what, and the two load
/// views target selection reads.
struct PlacementState {
    owner: BTreeMap<ShardId, Option<String>>,
    /// Total shards per server.
    load: BTreeMap<String, usize>,
    /// Shards of one table per server, keyed `(table, server)`.
    table_load: BTreeMap<(String, String), usize>,
}

impl PlacementState {
    fn place(&mut self, shard_id: ShardId, table: &str, from: Option<&str>, to: &str) {
        if let Some(from_addr) = from {
            if let Some(count) = self.load.get_mut(from_addr) {
                *count = count.saturating_sub(1);
            }
            if let Some(count) = self
                .table_load
                .get_mut(&(table.to_string(), from_addr.to_string()))
            {
                *count = count.saturating_sub(1);
            }
        }
        *self.load.entry(to.to_string()).or_default() += 1;
        *self
            .table_load
            .entry((table.to_string(), to.to_string()))
            .or_default() += 1;
        self.owner.insert(shard_id, Some(to.to_string()));
    }

    /// Pick the best placement target from `eligible`: fewest shards of this
    /// table first, then fewest shards overall, then the lowest address.
    ///
    /// Ordering on the table count first is what keeps one table from stacking
    /// onto a node that happens to be globally idle -- the same reason replicas
    /// are spread rather than packed.
    fn best_target(&self, eligible: &BTreeSet<String>, table: &str) -> String {
        eligible
            .iter()
            .min_by(|addr_a, addr_b| {
                let table_a = self
                    .table_load
                    .get(&(table.to_string(), (*addr_a).clone()))
                    .copied()
                    .unwrap_or_default();
                let table_b = self
                    .table_load
                    .get(&(table.to_string(), (*addr_b).clone()))
                    .copied()
                    .unwrap_or_default();
                let total_a = self.load.get(*addr_a).copied().unwrap_or_default();
                let total_b = self.load.get(*addr_b).copied().unwrap_or_default();
                table_a
                    .cmp(&table_b)
                    .then_with(|| total_a.cmp(&total_b))
                    .then_with(|| addr_a.cmp(addr_b))
            })
            .cloned()
            .expect("eligible set is non-empty")
    }
}

/// Pure planner: bring `shard_owners` onto `live_servers` while honouring the
/// placement rules in `shard_placement`.
///
/// Runs in three passes, all sharing [`AutoRebalanceOptions::max_moves`]:
/// evacuate shards whose owner is gone, pull back shards sitting outside their
/// table's location, then balance each domain. Shards with no entry in
/// `shard_placement` are unconstrained and share one domain, which is exactly
/// [`compute_auto_rebalance`]'s behaviour -- so a cluster that declares no
/// placement at all plans the same way.
pub fn compute_placement_aware_rebalance(
    shard_owners: &BTreeMap<ShardId, String>,
    shard_placement: &BTreeMap<ShardId, ShardPlacement>,
    live_servers: &[PlacementTarget],
    options: AutoRebalanceOptions,
) -> Vec<ShardReassignment> {
    let mut plans = Vec::new();
    if live_servers.is_empty() {
        // Nowhere to place shards; leave the map untouched rather than drop routes.
        return plans;
    }

    let index = build_index(shard_owners, shard_placement, live_servers, options);
    let all_servers = index
        .eligible_sets
        .get(ANY_LOCATION)
        .expect("the unconstrained set is always present");

    // Seed the working state from the current map. A shard on a dead owner is
    // recorded as unplaced so it contributes to nobody's load.
    let mut state = PlacementState {
        owner: BTreeMap::new(),
        load: all_servers.iter().map(|addr| (addr.clone(), 0)).collect(),
        table_load: BTreeMap::new(),
    };
    for (shard_id, current) in shard_owners {
        if all_servers.contains(current) {
            *state.load.get_mut(current).expect("seeded") += 1;
            *state
                .table_load
                .entry((index.table(*shard_id).to_string(), current.clone()))
                .or_default() += 1;
            state.owner.insert(*shard_id, Some(current.clone()));
        } else {
            state.owner.insert(*shard_id, None);
        }
    }

    // Pass 1 -- evacuate every shard whose owner is no longer live.
    for (shard_id, current) in shard_owners {
        if plans.len() >= options.max_moves {
            return plans;
        }
        if all_servers.contains(current) {
            continue;
        }
        let table = index.table(*shard_id).to_string();
        let target = state.best_target(index.eligible(*shard_id), &table);
        state.place(*shard_id, &table, None, &target);
        plans.push(ShardReassignment {
            shard_id: *shard_id,
            from_server: Some(current.clone()),
            to_server: target,
            reason: ShardReassignmentReason::OwnerUnavailable,
        });
    }

    // Pass 2 -- pull back shards whose live owner sits outside their table's
    // location. Evacuation never catches these: the owner is perfectly healthy,
    // it is just in the wrong place.
    if options.location_scoped {
        for (shard_id, current) in shard_owners {
            if plans.len() >= options.max_moves {
                return plans;
            }
            if !all_servers.contains(current) {
                continue; // already handled by pass 1
            }
            let eligible = index.eligible(*shard_id);
            if eligible.contains(current) {
                continue;
            }
            let table = index.table(*shard_id).to_string();
            let target = state.best_target(eligible, &table);
            state.place(*shard_id, &table, Some(current), &target);
            plans.push(ShardReassignment {
                shard_id: *shard_id,
                from_server: Some(current.clone()),
                to_server: target,
                reason: ShardReassignmentReason::LocationViolation,
            });
        }
    }

    // Pass 3 -- balance each domain independently.
    if options.balance_load {
        for domain in build_domains(&index, &state, options).values() {
            if plans.len() >= options.max_moves {
                return plans;
            }
            balance_domain(domain, &index, &mut state, &mut plans, options);
        }
    }

    plans
}

/// Resolve every shard's eligible server set and table once.
fn build_index(
    shard_owners: &BTreeMap<ShardId, String>,
    shard_placement: &BTreeMap<ShardId, ShardPlacement>,
    live_servers: &[PlacementTarget],
    options: AutoRebalanceOptions,
) -> PlacementIndex {
    let mut eligible_sets: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let all = eligible_sets.entry(ANY_LOCATION.to_string()).or_default();
    for target in live_servers {
        all.insert(target.server_addr.clone());
    }
    // A preference is matched by hierarchical containment rather than string
    // equality, so a table pinned to `dc1` accepts any server beneath it -- with
    // exact matching, only a single rack could ever be named.
    let parsed_targets = live_servers
        .iter()
        .map(|target| (target, Location::parse(&target.location)))
        .collect::<Vec<_>>();
    let mut wanted_locations = BTreeSet::new();
    for placement in shard_placement.values() {
        if !placement.preferred_location.is_empty() {
            wanted_locations.insert(placement.preferred_location.clone());
        }
    }
    for wanted in wanted_locations {
        let pattern = Location::parse(&wanted);
        let matching = parsed_targets
            .iter()
            .filter(|(_, location)| location.belongs_to(&pattern))
            .map(|(target, _)| target.server_addr.clone())
            .collect::<BTreeSet<_>>();
        eligible_sets.insert(wanted, matching);
    }

    let mut shard_eligible = BTreeMap::new();
    let mut shard_table = BTreeMap::new();
    for shard_id in shard_owners.keys() {
        let placement = shard_placement.get(shard_id);
        if let Some(placement) = placement {
            shard_table.insert(*shard_id, placement.table_key.clone());
        }
        // A preference is honoured only while its location still has capacity;
        // otherwise the shard falls back to anywhere rather than being stranded.
        let key = match placement {
            Some(placement)
                if options.location_scoped
                    && !placement.preferred_location.is_empty()
                    && eligible_sets
                        .get(&placement.preferred_location)
                        .is_some_and(|servers| !servers.is_empty()) =>
            {
                placement.preferred_location.clone()
            }
            _ => ANY_LOCATION.to_string(),
        };
        shard_eligible.insert(*shard_id, key);
    }

    PlacementIndex {
        eligible_sets,
        shard_eligible,
        shard_table,
    }
}

/// Group the shards into balance domains, keyed for deterministic iteration.
/// The eligible set is part of the key: two tables pinned to different racks
/// must never be balanced against each other.
fn build_domains(
    index: &PlacementIndex,
    state: &PlacementState,
    options: AutoRebalanceOptions,
) -> BTreeMap<(String, String), BalanceDomain> {
    let mut domains: BTreeMap<(String, String), BalanceDomain> = BTreeMap::new();
    for shard_id in state.owner.keys() {
        let eligible_key = index
            .shard_eligible
            .get(shard_id)
            .cloned()
            .unwrap_or_else(|| ANY_LOCATION.to_string());
        let table_key = if options.per_table_balance {
            index.table(*shard_id).to_string()
        } else {
            String::new()
        };
        domains
            .entry((eligible_key, table_key))
            .or_insert_with(|| BalanceDomain {
                eligible: index.eligible(*shard_id).clone(),
                shards: BTreeSet::new(),
            })
            .shards
            .insert(*shard_id);
    }
    domains
}

/// Move shards off the busiest servers in one domain until nobody sits more than
/// `balance_safe_gap` above the domain's fair share. Each move strictly reduces
/// the spread, so this terminates.
fn balance_domain(
    domain: &BalanceDomain,
    index: &PlacementIndex,
    state: &mut PlacementState,
    plans: &mut Vec<ShardReassignment>,
    options: AutoRebalanceOptions,
) {
    if domain.eligible.is_empty() {
        return;
    }
    // Domain-local counts: only this domain's shards, only on its servers.
    let mut counts: BTreeMap<String, usize> = domain
        .eligible
        .iter()
        .map(|addr| (addr.clone(), 0))
        .collect();
    let mut placed = 0_usize;
    for shard_id in &domain.shards {
        if let Some(Some(addr)) = state.owner.get(shard_id) {
            if let Some(count) = counts.get_mut(addr) {
                *count += 1;
                placed += 1;
            }
        }
    }
    if placed == 0 {
        return;
    }
    // Ceiling share: with 5 shards over 2 servers the fair share is 3, not 2, so
    // a server holding 3 is not treated as overloaded.
    let fair_share = placed.div_ceil(domain.eligible.len());
    let safe_line = fair_share + options.balance_safe_gap;

    while plans.len() < options.max_moves {
        let Some((busy_addr, busy_count)) = counts
            .iter()
            .max_by(|(addr_a, count_a), (addr_b, count_b)| {
                count_a.cmp(count_b).then_with(|| addr_b.cmp(addr_a))
            })
            .map(|(addr, count)| (addr.clone(), *count))
        else {
            return;
        };
        if busy_count <= safe_line {
            return; // nobody is above their fair share plus the gap
        }
        let Some((idle_addr, idle_count)) = counts
            .iter()
            .min_by(|(addr_a, count_a), (addr_b, count_b)| {
                count_a.cmp(count_b).then_with(|| addr_a.cmp(addr_b))
            })
            .map(|(addr, count)| (addr.clone(), *count))
        else {
            return;
        };
        if busy_count <= idle_count + 1 {
            return; // moving would only invert the imbalance
        }
        // Highest shard id on the busy server, for a deterministic choice.
        let Some(shard_id) = domain
            .shards
            .iter()
            .filter(|shard_id| {
                state.owner.get(*shard_id).and_then(|addr| addr.as_deref())
                    == Some(busy_addr.as_str())
            })
            .max()
            .copied()
        else {
            return;
        };

        let table = index.table(shard_id).to_string();
        state.place(shard_id, &table, Some(&busy_addr), &idle_addr);
        *counts.get_mut(&busy_addr).expect("busy is in domain") -= 1;
        *counts.get_mut(&idle_addr).expect("idle is in domain") += 1;
        plans.push(ShardReassignment {
            shard_id,
            from_server: Some(busy_addr),
            to_server: idle_addr,
            reason: ShardReassignmentReason::Rebalance,
        });
    }
}

impl SingleNodeMeta {
    /// Live placement targets: Normal-state servers with the location they
    /// declared at registration.
    pub fn placement_targets(&self) -> Vec<PlacementTarget> {
        let state = self.inner.read().expect("meta lock poisoned");
        let mut targets = state
            .servers
            .values()
            .filter(|server| server.state == MetaEntityState::Normal)
            .map(|server| PlacementTarget {
                server_addr: server.server_addr.clone(),
                location: server.location.clone(),
            })
            .collect::<Vec<_>>();
        targets.sort();
        targets
    }

    /// Placement constraints for every registered shard, derived from the table
    /// that owns it. A shard whose table is missing, dropped or frozen carries
    /// no constraint and is planned as unconstrained.
    pub fn shard_placements(&self) -> BTreeMap<ShardId, ShardPlacement> {
        let state = self.inner.read().expect("meta lock poisoned");
        shard_owning_tables(&state)
            .into_iter()
            .filter_map(|(shard_id, table)| {
                if table.info.state != MetaEntityState::Normal {
                    return None;
                }
                // The shard's own pin wins over the table's, and an empty pin
                // means it has none -- which is what every shard means until
                // somebody says otherwise.
                let pinned = state
                    .shards
                    .get(shard_id)
                    .map(|location| location.preferred_location.clone())
                    .unwrap_or_default();
                let preferred_location = if pinned.is_empty() {
                    table.info.serving_options.preferred_location.clone()
                } else {
                    pinned
                };
                Some((
                    shard_id,
                    ShardPlacement {
                        table_key: table_key(&table.info.namespace, &table.info.table_name),
                        preferred_location,
                    },
                ))
            })
            .collect()
    }

    /// Compute the placement-aware rebalance plan for the current membership.
    /// This is the placement-rule-honouring counterpart to
    /// [`Self::plan_auto_rebalance_with_options`].
    pub fn plan_placement_aware_rebalance(
        &self,
        options: AutoRebalanceOptions,
    ) -> Vec<ShardReassignment> {
        let live_servers = self.placement_targets();
        let shard_placement = self.shard_placements();
        let shard_owners = {
            let state = self.inner.read().expect("meta lock poisoned");
            serving_shard_owners(&state)
        };
        compute_placement_aware_rebalance(&shard_owners, &shard_placement, &live_servers, options)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owners(pairs: &[(ShardId, &str)]) -> BTreeMap<ShardId, String> {
        pairs
            .iter()
            .map(|(shard_id, addr)| (*shard_id, (*addr).to_string()))
            .collect()
    }

    fn targets(pairs: &[(&str, &str)]) -> Vec<PlacementTarget> {
        pairs
            .iter()
            .map(|(addr, location)| PlacementTarget {
                server_addr: (*addr).to_string(),
                location: (*location).to_string(),
            })
            .collect()
    }

    /// Every shard `ids` belongs to `table`, pinned to `location` ("" = anywhere).
    fn placement(entries: &[(ShardId, &str, &str)]) -> BTreeMap<ShardId, ShardPlacement> {
        entries
            .iter()
            .map(|(shard_id, table, location)| {
                (
                    *shard_id,
                    ShardPlacement {
                        table_key: (*table).to_string(),
                        preferred_location: (*location).to_string(),
                    },
                )
            })
            .collect()
    }

    fn moves(plans: &[ShardReassignment]) -> Vec<(ShardId, &str, ShardReassignmentReason)> {
        plans
            .iter()
            .map(|plan| (plan.shard_id, plan.to_server.as_str(), plan.reason))
            .collect()
    }

    #[test]
    fn evacuation_stays_inside_the_tables_location() {
        // The idlest server in the cluster is in the wrong rack. Placement-blind
        // planning would evacuate onto it and silently break the table's
        // location contract; here the shard lands on its own rack instead.
        let plans = compute_placement_aware_rebalance(
            &owners(&[(1, "dead"), (2, "a1"), (3, "a1")]),
            &placement(&[
                (1, "ns.orders", "rack-a"),
                (2, "ns.orders", "rack-a"),
                (3, "ns.orders", "rack-a"),
            ]),
            &targets(&[("a1", "rack-a"), ("a2", "rack-a"), ("b1", "rack-b")]),
            AutoRebalanceOptions {
                balance_load: false,
                ..AutoRebalanceOptions::default()
            },
        );
        assert_eq!(
            moves(&plans),
            vec![(1, "a2", ShardReassignmentReason::OwnerUnavailable)]
        );
    }

    #[test]
    fn a_preference_can_name_a_whole_datacenter() {
        // With exact string matching this is impossible to express: a table
        // could only ever be pinned to one rack. Any server beneath dc1 now
        // qualifies, and one outside it does not.
        let plans = compute_placement_aware_rebalance(
            &owners(&[(1, "dead")]),
            &placement(&[(1, "ns.orders", "us-east/dc1")]),
            &targets(&[
                ("a1", "us-east/dc1/az1/rack1"),
                ("a2", "us-east/dc1/az2/rack9"),
                ("b1", "us-east/dc2/az1/rack1"),
            ]),
            AutoRebalanceOptions {
                balance_load: false,
                ..AutoRebalanceOptions::default()
            },
        );
        assert_eq!(plans.len(), 1);
        assert!(
            plans[0].to_server == "a1" || plans[0].to_server == "a2",
            "must land inside dc1, got {}",
            plans[0].to_server
        );
    }

    #[test]
    fn a_shard_outside_its_preferred_datacenter_is_pulled_back_into_it() {
        let plans = compute_placement_aware_rebalance(
            &owners(&[(1, "b1")]),
            &placement(&[(1, "ns.orders", "us-east/dc1")]),
            &targets(&[
                ("a1", "us-east/dc1/az1/rack1"),
                ("b1", "us-east/dc2/az1/rack1"),
            ]),
            AutoRebalanceOptions {
                balance_load: false,
                ..AutoRebalanceOptions::default()
            },
        );
        assert_eq!(
            moves(&plans),
            vec![(1, "a1", ShardReassignmentReason::LocationViolation)]
        );
    }

    #[test]
    fn a_deeper_preference_still_pins_to_one_rack() {
        // The narrow case keeps working: naming every level pins exactly.
        let plans = compute_placement_aware_rebalance(
            &owners(&[(1, "dead")]),
            &placement(&[(1, "ns.orders", "us-east/dc1/az2/rack9")]),
            &targets(&[
                ("a1", "us-east/dc1/az1/rack1"),
                ("a2", "us-east/dc1/az2/rack9"),
            ]),
            AutoRebalanceOptions {
                balance_load: false,
                ..AutoRebalanceOptions::default()
            },
        );
        assert_eq!(
            moves(&plans),
            vec![(1, "a2", ShardReassignmentReason::OwnerUnavailable)]
        );
    }

    #[test]
    fn a_preference_no_live_server_matches_falls_back_to_anywhere() {
        // Same guard as before, now evaluated hierarchically: dc9 is empty, so
        // the shard is placed rather than stranded.
        let plans = compute_placement_aware_rebalance(
            &owners(&[(1, "dead")]),
            &placement(&[(1, "ns.orders", "us-east/dc9")]),
            &targets(&[("a1", "us-east/dc1/az1/rack1")]),
            AutoRebalanceOptions::default(),
        );
        assert_eq!(
            moves(&plans),
            vec![(1, "a1", ShardReassignmentReason::OwnerUnavailable)]
        );
    }

    #[test]
    fn a_shard_on_a_live_server_in_the_wrong_location_is_pulled_back() {
        // Evacuation cannot catch this: the owner is perfectly healthy, it is
        // just in a location the table no longer prefers.
        let plans = compute_placement_aware_rebalance(
            &owners(&[(1, "b1")]),
            &placement(&[(1, "ns.orders", "rack-a")]),
            &targets(&[("a1", "rack-a"), ("b1", "rack-b")]),
            AutoRebalanceOptions {
                balance_load: false,
                ..AutoRebalanceOptions::default()
            },
        );
        assert_eq!(
            moves(&plans),
            vec![(1, "a1", ShardReassignmentReason::LocationViolation)]
        );
        assert_eq!(plans[0].from_server.as_deref(), Some("b1"));
    }

    #[test]
    fn a_preference_with_no_live_server_falls_back_rather_than_stranding_the_shard() {
        // rack-a is entirely gone. Honouring the preference literally would mean
        // placing the shard nowhere, which loses the route outright.
        let plans = compute_placement_aware_rebalance(
            &owners(&[(1, "dead")]),
            &placement(&[(1, "ns.orders", "rack-a")]),
            &targets(&[("b1", "rack-b"), ("b2", "rack-b")]),
            AutoRebalanceOptions::default(),
        );
        assert_eq!(
            moves(&plans),
            vec![(1, "b1", ShardReassignmentReason::OwnerUnavailable)]
        );
    }

    #[test]
    fn a_table_is_spread_even_when_total_shard_counts_look_balanced() {
        // Four shards of each table, two servers, four shards each: the global
        // counts are perfectly even, so total-count balancing sees nothing to
        // do. But each table is single-homed, and losing one node takes a whole
        // table down. Per-table balancing is what notices.
        let shard_owners = owners(&[
            (1, "s1"),
            (2, "s1"),
            (3, "s1"),
            (4, "s1"),
            (5, "s2"),
            (6, "s2"),
            (7, "s2"),
            (8, "s2"),
        ]);
        let shard_placement = placement(&[
            (1, "ns.orders", ""),
            (2, "ns.orders", ""),
            (3, "ns.orders", ""),
            (4, "ns.orders", ""),
            (5, "ns.users", ""),
            (6, "ns.users", ""),
            (7, "ns.users", ""),
            (8, "ns.users", ""),
        ]);
        let live = targets(&[("s1", ""), ("s2", "")]);

        // Without per-table grouping the imbalance is invisible.
        let blind = compute_placement_aware_rebalance(
            &shard_owners,
            &shard_placement,
            &live,
            AutoRebalanceOptions {
                per_table_balance: false,
                ..AutoRebalanceOptions::default()
            },
        );
        assert!(blind.is_empty());

        // With it, each table ends up spanning both servers.
        let plans = compute_placement_aware_rebalance(
            &shard_owners,
            &shard_placement,
            &live,
            AutoRebalanceOptions::default(),
        );
        let mut final_owner = shard_owners.clone();
        for plan in &plans {
            final_owner.insert(plan.shard_id, plan.to_server.clone());
        }
        for table in [[1, 2, 3, 4], [5, 6, 7, 8]] {
            let hosts = table
                .iter()
                .map(|shard_id| final_owner[shard_id].clone())
                .collect::<BTreeSet<_>>();
            assert_eq!(
                hosts.len(),
                2,
                "table {table:?} should span both servers, got {hosts:?}"
            );
        }
    }

    #[test]
    fn tables_pinned_to_different_locations_are_not_balanced_against_each_other() {
        // rack-a holds four shards of one table and rack-b one shard of another.
        // A planner that pooled them would try to "balance" across the racks and
        // break both location contracts; each rack must balance on its own.
        let plans = compute_placement_aware_rebalance(
            &owners(&[(1, "a1"), (2, "a1"), (3, "a1"), (4, "a1"), (5, "b1")]),
            &placement(&[
                (1, "ns.orders", "rack-a"),
                (2, "ns.orders", "rack-a"),
                (3, "ns.orders", "rack-a"),
                (4, "ns.orders", "rack-a"),
                (5, "ns.users", "rack-b"),
            ]),
            &targets(&[
                ("a1", "rack-a"),
                ("a2", "rack-a"),
                ("b1", "rack-b"),
                ("b2", "rack-b"),
            ]),
            AutoRebalanceOptions::default(),
        );
        assert!(!plans.is_empty(), "rack-a is lopsided and should rebalance");
        for plan in &plans {
            assert!(
                plan.to_server.starts_with('a'),
                "orders must stay in rack-a, got {}",
                plan.to_server
            );
            assert!(plan.shard_id <= 4, "the rack-b shard must not move");
        }
    }

    #[test]
    fn the_safe_gap_damps_churn() {
        // Four shards on one server, none on the other. With no gap the planner
        // levels them; with a gap of two the server is inside its allowance and
        // the data stays put.
        let shard_owners = owners(&[(1, "s1"), (2, "s1"), (3, "s1"), (4, "s1")]);
        let shard_placement = placement(&[
            (1, "ns.orders", ""),
            (2, "ns.orders", ""),
            (3, "ns.orders", ""),
            (4, "ns.orders", ""),
        ]);
        let live = targets(&[("s1", ""), ("s2", "")]);

        let levelled = compute_placement_aware_rebalance(
            &shard_owners,
            &shard_placement,
            &live,
            AutoRebalanceOptions::default(),
        );
        assert_eq!(levelled.len(), 2);

        let damped = compute_placement_aware_rebalance(
            &shard_owners,
            &shard_placement,
            &live,
            AutoRebalanceOptions {
                balance_safe_gap: 2,
                ..AutoRebalanceOptions::default()
            },
        );
        assert!(damped.is_empty());
    }

    #[test]
    fn placement_falls_back_to_flat_planning_when_nothing_declares_a_location() {
        // A cluster that sets no placement at all must plan exactly as the flat
        // planner does, so enabling the gate is a no-op for those deployments.
        let shard_owners = owners(&[(1, "dead"), (2, "s1"), (3, "s1")]);
        let live = targets(&[("s1", ""), ("s2", "")]);
        let flat = compute_auto_rebalance(
            &shard_owners,
            &live
                .iter()
                .map(|target| target.server_addr.clone())
                .collect::<BTreeSet<_>>(),
            AutoRebalanceOptions::default(),
        );
        let placement_aware = compute_placement_aware_rebalance(
            &shard_owners,
            &BTreeMap::new(),
            &live,
            AutoRebalanceOptions::default(),
        );
        assert_eq!(moves(&flat), moves(&placement_aware));
    }

    #[test]
    fn a_freshly_joined_server_in_the_right_location_receives_load() {
        let plans = compute_placement_aware_rebalance(
            &owners(&[(1, "a1"), (2, "a1"), (3, "a1"), (4, "a1")]),
            &placement(&[
                (1, "ns.orders", "rack-a"),
                (2, "ns.orders", "rack-a"),
                (3, "ns.orders", "rack-a"),
                (4, "ns.orders", "rack-a"),
            ]),
            &targets(&[("a1", "rack-a"), ("a2", "rack-a")]),
            AutoRebalanceOptions::default(),
        );
        assert_eq!(
            moves(&plans),
            vec![
                (4, "a2", ShardReassignmentReason::Rebalance),
                (3, "a2", ShardReassignmentReason::Rebalance),
            ]
        );
    }

    #[test]
    fn no_live_servers_yields_no_plan() {
        let plans = compute_placement_aware_rebalance(
            &owners(&[(1, "dead")]),
            &placement(&[(1, "ns.orders", "rack-a")]),
            &[],
            AutoRebalanceOptions::default(),
        );
        assert!(plans.is_empty());
    }

    #[test]
    fn max_moves_caps_every_pass() {
        let plans = compute_placement_aware_rebalance(
            &owners(&[(1, "dead"), (2, "dead"), (3, "dead"), (4, "dead")]),
            &placement(&[
                (1, "ns.orders", "rack-a"),
                (2, "ns.orders", "rack-a"),
                (3, "ns.orders", "rack-a"),
                (4, "ns.orders", "rack-a"),
            ]),
            &targets(&[("a1", "rack-a"), ("a2", "rack-a")]),
            AutoRebalanceOptions {
                max_moves: 2,
                ..AutoRebalanceOptions::default()
            },
        );
        assert_eq!(plans.len(), 2);
    }

    #[test]
    fn location_scoping_can_be_turned_off() {
        // With scoping off the planner ignores the preference and behaves flatly,
        // so an operator can fall back without reverting the whole feature.
        let plans = compute_placement_aware_rebalance(
            &owners(&[(1, "b1")]),
            &placement(&[(1, "ns.orders", "rack-a")]),
            &targets(&[("a1", "rack-a"), ("b1", "rack-b")]),
            AutoRebalanceOptions {
                location_scoped: false,
                balance_load: false,
                ..AutoRebalanceOptions::default()
            },
        );
        assert!(plans.is_empty());
    }

    #[test]
    fn evacuation_prefers_the_server_holding_least_of_that_table() {
        // b1 is globally idler, but it already holds two shards of this table
        // while b2 holds none of it. Spreading the table wins over the raw count.
        let plans = compute_placement_aware_rebalance(
            &owners(&[
                (1, "dead"),
                (2, "b1"),
                (3, "b1"),
                (4, "b2"),
                (5, "b2"),
                (6, "b2"),
            ]),
            &placement(&[
                (1, "ns.orders", ""),
                (2, "ns.orders", ""),
                (3, "ns.orders", ""),
                (4, "ns.users", ""),
                (5, "ns.users", ""),
                (6, "ns.users", ""),
            ]),
            &targets(&[("b1", ""), ("b2", "")]),
            AutoRebalanceOptions {
                balance_load: false,
                ..AutoRebalanceOptions::default()
            },
        );
        assert_eq!(
            moves(&plans),
            vec![(1, "b2", ShardReassignmentReason::OwnerUnavailable)]
        );
    }
}
