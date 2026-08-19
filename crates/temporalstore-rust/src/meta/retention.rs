// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Retention and garbage collection for dropped meta resources.
//!
//! Dropping a server, proxy or table sets its state to
//! [`MetaEntityState::Dropped`] and stops there — `apply_set_server_state` and
//! its siblings carry an explicit `MetaEntityState::Dropped => {}` arm. The
//! entry itself is never removed.
//!
//! So the metaserver's state only ever grows. Every node ever decommissioned,
//! every proxy ever retired, every table ever deleted stays in [`MetaState`] for
//! the lifetime of the cluster. That is not a tidiness problem:
//!
//! * [`MetaSnapshot`] carries all of it. Snapshots are exported and installed
//!   wholesale, including onto raft peers, so every dropped resource is
//!   re-serialised and re-shipped on every snapshot for as long as the cluster
//!   lives.
//! * `list_servers` and `list_tables` return the tombstones alongside the live
//!   resources, so an operator's view fills with things that no longer exist.
//! * The shard routes of a dropped table stay in the owner map even though
//!   `get_table_topology` already refuses to serve them: pure ballast that
//!   nothing can ever use.
//!
//! There was also nothing to age against, because dropping recorded no
//! timestamp — "dropped long enough ago to forget" was not expressible.
//!
//! This module adds both halves. `MetaState::dropped_since_ms` records when each
//! resource was dropped (kept beside the resources rather than inside them, so
//! the wire types are unchanged, and carried through snapshots so a peer keeps
//! ageing the tombstones it inherits). [`plan_meta_retention`] then decides,
//! purely, which tombstones are old enough to purge.
//!
//! Two guards keep collection from destroying information:
//!
//! * **A dropped server that still owns a shard is never purged.** The owner map
//!   would be left naming an address the metaserver knows nothing about, which
//!   is strictly worse than the tombstone. Such servers are reported as blocked;
//!   once rebalancing has evacuated the shard, the next round collects them.
//! * **Purges are capped per round.** Decommissioning a rack retires many
//!   resources at once, and a pass that rewrote most of the meta state in one
//!   step would stall every other meta operation behind the write lock.
//!
//! A dropped table's shard routes are purged *with* the table, since they are
//! already unroutable and keeping them would leave entries pointing at a table
//! that no longer exists.

use std::collections::BTreeSet;

use super::*;

/// Tuning for [`plan_meta_retention`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaRetentionOptions {
    /// How long a dropped server's tombstone is kept.
    pub server_retention_ms: u64,
    /// How long a dropped proxy's tombstone is kept.
    pub proxy_retention_ms: u64,
    /// How long a dropped table's tombstone is kept.
    pub table_retention_ms: u64,
    /// Most resources purged in one round, so a mass decommission does not
    /// rewrite the whole meta state under a single write lock.
    pub max_purges_per_round: usize,
}

impl Default for MetaRetentionOptions {
    fn default() -> Self {
        Self {
            server_retention_ms: 24 * 60 * 60 * 1_000,
            proxy_retention_ms: 24 * 60 * 60 * 1_000,
            table_retention_ms: 24 * 60 * 60 * 1_000,
            max_purges_per_round: 20,
        }
    }
}

/// One dropped resource the planner may collect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionCandidate {
    /// Address for a server or proxy, namespace-qualified key for a table.
    pub id: String,
    /// When it was dropped, or 0 if unknown (a tombstone predating the field).
    pub dropped_since_ms: u64,
}

/// What one retention round should remove.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaRetentionPlan {
    /// Dropped servers old enough to forget, ordered by address.
    pub servers: Vec<String>,
    /// Dropped proxies old enough to forget, ordered by address.
    pub proxies: Vec<String>,
    /// Dropped tables old enough to forget, ordered by key.
    pub tables: Vec<String>,
    /// Shard routes belonging to the purged tables, ordered by shard id.
    pub shards: Vec<ShardId>,
    /// Dropped servers held back because they still own a shard, ordered by
    /// address.
    pub blocked_servers: Vec<String>,
    /// How many otherwise-eligible resources the per-round cap held back.
    pub capped: usize,
}

impl MetaRetentionPlan {
    /// True when this round has nothing to do.
    pub fn is_empty(&self) -> bool {
        self.servers.is_empty()
            && self.proxies.is_empty()
            && self.tables.is_empty()
            && self.shards.is_empty()
    }

    /// Number of resources (not shard routes) this round purges.
    pub fn purge_count(&self) -> usize {
        self.servers.len() + self.proxies.len() + self.tables.len()
    }
}

/// What one retention round actually removed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaRetentionReport {
    pub status: Status,
    pub plan: MetaRetentionPlan,
}

/// Pure planner: decide which dropped resources are old enough to forget.
///
/// `shard_owners` maps shard id to its recorded owner address and `shard_tables`
/// maps shard id to the key of the table that owns it; both are needed for the
/// guards. Deterministic — every output vector is sorted, and the per-round cap
/// is spent in a fixed order (proxies, then tables, then servers), so the same
/// state always yields the same plan.
pub fn plan_meta_retention(
    servers: &[RetentionCandidate],
    proxies: &[RetentionCandidate],
    tables: &[RetentionCandidate],
    shard_owners: &BTreeMap<ShardId, String>,
    shard_tables: &BTreeMap<ShardId, String>,
    now_ms: u64,
    options: MetaRetentionOptions,
) -> MetaRetentionPlan {
    let mut plan = MetaRetentionPlan::default();

    // A dropped server whose address still appears in the owner map is load
    // bearing: the route names it, so forgetting the server strands the route.
    let owning_addrs = shard_owners.values().collect::<BTreeSet<_>>();

    // A tombstone with no timestamp predates this feature. It is left alone
    // rather than treated as infinitely old, which would purge the entire
    // history on the first round after an upgrade.
    let expired = |candidate: &RetentionCandidate, retention_ms: u64| -> bool {
        candidate.dropped_since_ms != 0
            && now_ms.saturating_sub(candidate.dropped_since_ms) >= retention_ms
    };

    let mut eligible_proxies = proxies
        .iter()
        .filter(|candidate| expired(candidate, options.proxy_retention_ms))
        .map(|candidate| candidate.id.clone())
        .collect::<Vec<_>>();
    eligible_proxies.sort();

    let mut eligible_tables = tables
        .iter()
        .filter(|candidate| expired(candidate, options.table_retention_ms))
        .map(|candidate| candidate.id.clone())
        .collect::<Vec<_>>();
    eligible_tables.sort();

    let mut eligible_servers = Vec::new();
    for candidate in servers {
        if !expired(candidate, options.server_retention_ms) {
            continue;
        }
        if owning_addrs.contains(&candidate.id) {
            plan.blocked_servers.push(candidate.id.clone());
            continue;
        }
        eligible_servers.push(candidate.id.clone());
    }
    eligible_servers.sort();
    plan.blocked_servers.sort();

    // Spend the round's budget in a fixed order: proxies first because they are
    // the cheapest and have no dependents, servers last because they are the
    // ones a route can still be attached to.
    let total_eligible = eligible_proxies.len() + eligible_tables.len() + eligible_servers.len();
    let mut budget = options.max_purges_per_round;
    for (source, sink) in [
        (eligible_proxies, &mut plan.proxies),
        (eligible_tables, &mut plan.tables),
        (eligible_servers, &mut plan.servers),
    ] {
        for item in source {
            if budget == 0 {
                break;
            }
            budget -= 1;
            sink.push(item);
        }
    }
    plan.capped = total_eligible.saturating_sub(plan.purge_count());

    // A purged table's shard routes go with it: they already resolve to nothing.
    let purged_tables = plan.tables.iter().cloned().collect::<BTreeSet<_>>();
    plan.shards = shard_tables
        .iter()
        .filter(|(_, table_key)| purged_tables.contains(*table_key))
        .map(|(shard_id, _)| *shard_id)
        .collect();
    plan.shards.sort();
    plan
}

impl SingleNodeMeta {
    /// Compute the retention plan for the current state without applying it.
    pub fn plan_meta_retention_now(&self, options: MetaRetentionOptions) -> MetaRetentionPlan {
        let now = now_ms();
        let state = self.inner.read().expect("meta lock poisoned");
        let dropped_at = |kind: &str, id: &str| -> u64 {
            state
                .dropped_since_ms
                .get(&dropped_key(kind, id))
                .copied()
                .unwrap_or_default()
        };
        let servers = state
            .servers
            .values()
            .filter(|server| server.state == MetaEntityState::Dropped)
            .map(|server| RetentionCandidate {
                id: server.server_addr.clone(),
                dropped_since_ms: dropped_at("server", &server.server_addr),
            })
            .collect::<Vec<_>>();
        let proxies = state
            .proxies
            .values()
            .filter(|proxy| proxy.state == MetaEntityState::Dropped)
            .map(|proxy| RetentionCandidate {
                id: proxy.proxy_addr.clone(),
                dropped_since_ms: dropped_at("proxy", &proxy.proxy_addr),
            })
            .collect::<Vec<_>>();
        let tables = state
            .tables
            .iter()
            .filter(|(_, table)| table.info.state == MetaEntityState::Dropped)
            .map(|(key, _)| RetentionCandidate {
                id: key.clone(),
                dropped_since_ms: dropped_at("table", key),
            })
            .collect::<Vec<_>>();
        let shard_owners = state
            .shards
            .values()
            .map(|location| (location.shard_id, location.server_addr.clone()))
            .collect::<BTreeMap<_, _>>();
        let shard_tables = state
            .shards
            .keys()
            .filter_map(|shard_id| {
                let table = table_for_shard(&state, *shard_id)?;
                Some((
                    *shard_id,
                    table_key(&table.info.namespace, &table.info.table_name),
                ))
            })
            .collect::<BTreeMap<_, _>>();
        plan_meta_retention(
            &servers,
            &proxies,
            &tables,
            &shard_owners,
            &shard_tables,
            now,
            options,
        )
    }

    /// Compute and apply one retention round: forget the dropped resources whose
    /// tombstones have aged out, along with the shard routes of any purged
    /// table.
    pub fn purge_expired_meta(&self, options: MetaRetentionOptions) -> MetaRetentionReport {
        let plan = self.plan_meta_retention_now(options);
        if plan.is_empty() {
            return MetaRetentionReport {
                status: Status::ok(),
                plan,
            };
        }
        let mut state = self.inner.write().expect("meta lock poisoned");
        for addr in &plan.proxies {
            state.proxies.remove(addr);
            state.dropped_since_ms.remove(&dropped_key("proxy", addr));
            record_topology_event(
                &mut state,
                "proxy_purged",
                format!("proxy:{addr}"),
                "reason=retention",
            );
        }
        for shard_id in &plan.shards {
            state.shards.remove(shard_id);
        }
        for key in &plan.tables {
            state.tables.remove(key);
            state.dropped_since_ms.remove(&dropped_key("table", key));
            record_topology_event(
                &mut state,
                "table_purged",
                format!("table:{key}"),
                "reason=retention",
            );
        }
        for addr in &plan.servers {
            state.servers.remove(addr);
            state.dropped_since_ms.remove(&dropped_key("server", addr));
            record_topology_event(
                &mut state,
                "server_purged",
                format!("server:{addr}"),
                "reason=retention",
            );
        }
        MetaRetentionReport {
            status: Status::ok(),
            plan,
        }
    }

    /// Background loop running [`Self::purge_expired_meta`] on an interval.
    pub fn start_meta_retention_loop(
        &self,
        options: MetaRetentionOptions,
        interval_ms: u64,
    ) -> thread::JoinHandle<()> {
        let meta = self.clone();
        let interval = Duration::from_millis(interval_ms.max(1));
        thread::spawn(move || loop {
            let _ = meta.purge_expired_meta(options);
            thread::sleep(interval);
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 30 * 24 * 60 * 60 * 1_000;
    const DAY: u64 = 24 * 60 * 60 * 1_000;

    fn candidates(pairs: &[(&str, u64)]) -> Vec<RetentionCandidate> {
        pairs
            .iter()
            .map(|(id, dropped_since_ms)| RetentionCandidate {
                id: (*id).to_string(),
                dropped_since_ms: *dropped_since_ms,
            })
            .collect()
    }

    fn map(pairs: &[(ShardId, &str)]) -> BTreeMap<ShardId, String> {
        pairs
            .iter()
            .map(|(shard_id, value)| (*shard_id, (*value).to_string()))
            .collect()
    }

    fn plan(
        servers: &[RetentionCandidate],
        proxies: &[RetentionCandidate],
        tables: &[RetentionCandidate],
        shard_owners: &BTreeMap<ShardId, String>,
        shard_tables: &BTreeMap<ShardId, String>,
        options: MetaRetentionOptions,
    ) -> MetaRetentionPlan {
        plan_meta_retention(
            servers,
            proxies,
            tables,
            shard_owners,
            shard_tables,
            NOW,
            options,
        )
    }

    #[test]
    fn tombstones_past_their_retention_are_collected() {
        let result = plan(
            &candidates(&[("old-server", NOW - 2 * DAY)]),
            &candidates(&[("old-proxy", NOW - 2 * DAY)]),
            &candidates(&[("ns.old-table", NOW - 2 * DAY)]),
            &BTreeMap::new(),
            &BTreeMap::new(),
            MetaRetentionOptions::default(),
        );
        assert_eq!(result.servers, vec!["old-server"]);
        assert_eq!(result.proxies, vec!["old-proxy"]);
        assert_eq!(result.tables, vec!["ns.old-table"]);
        assert_eq!(result.capped, 0);
    }

    #[test]
    fn a_recent_tombstone_is_kept() {
        // Retention exists so an operator can still see what was decommissioned
        // an hour ago.
        let result = plan(
            &candidates(&[("recent", NOW - 60_000)]),
            &candidates(&[("recent-proxy", NOW - 60_000)]),
            &candidates(&[("ns.recent", NOW - 60_000)]),
            &BTreeMap::new(),
            &BTreeMap::new(),
            MetaRetentionOptions::default(),
        );
        assert!(result.is_empty());
    }

    #[test]
    fn a_dropped_server_that_still_owns_a_shard_is_held_back() {
        // Forgetting it would leave the owner map naming an address the
        // metaserver knows nothing about, which is worse than the tombstone.
        let result = plan(
            &candidates(&[("owner", NOW - 2 * DAY)]),
            &[],
            &[],
            &map(&[(1, "owner")]),
            &BTreeMap::new(),
            MetaRetentionOptions::default(),
        );
        assert!(result.servers.is_empty());
        assert_eq!(result.blocked_servers, vec!["owner"]);
    }

    #[test]
    fn the_server_is_collected_once_its_shard_has_moved_away() {
        let result = plan(
            &candidates(&[("owner", NOW - 2 * DAY)]),
            &[],
            &[],
            &map(&[(1, "somebody-else")]),
            &BTreeMap::new(),
            MetaRetentionOptions::default(),
        );
        assert_eq!(result.servers, vec!["owner"]);
        assert!(result.blocked_servers.is_empty());
    }

    #[test]
    fn a_purged_tables_shard_routes_go_with_it() {
        // They already resolve to nothing: get_table_topology refuses a dropped
        // table. Leaving them behind orphans them permanently.
        let result = plan(
            &[],
            &[],
            &candidates(&[("ns.gone", NOW - 2 * DAY)]),
            &map(&[(1, "s1"), (2, "s1"), (3, "s1")]),
            &map(&[(1, "ns.gone"), (2, "ns.gone"), (3, "ns.stays")]),
            MetaRetentionOptions::default(),
        );
        assert_eq!(result.tables, vec!["ns.gone"]);
        assert_eq!(result.shards, vec![1, 2]);
    }

    #[test]
    fn a_tombstone_with_no_timestamp_is_left_alone() {
        // Pre-existing tombstones carry no drop time. Treating a missing
        // timestamp as "infinitely old" would purge the entire history on the
        // first round after an upgrade.
        let result = plan(
            &candidates(&[("legacy", 0)]),
            &candidates(&[("legacy-proxy", 0)]),
            &candidates(&[("ns.legacy", 0)]),
            &BTreeMap::new(),
            &BTreeMap::new(),
            MetaRetentionOptions::default(),
        );
        assert!(result.is_empty());
    }

    #[test]
    fn the_per_round_cap_bounds_how_much_one_pass_rewrites() {
        // Decommissioning a rack retires many resources at once; rewriting most
        // of the meta state in one pass would stall every other operation behind
        // the write lock.
        let servers = (0..10)
            .map(|index| (format!("s{index}"), NOW - 2 * DAY))
            .collect::<Vec<_>>();
        let servers = servers
            .iter()
            .map(|(id, dropped)| RetentionCandidate {
                id: id.clone(),
                dropped_since_ms: *dropped,
            })
            .collect::<Vec<_>>();
        let result = plan(
            &servers,
            &[],
            &[],
            &BTreeMap::new(),
            &BTreeMap::new(),
            MetaRetentionOptions {
                max_purges_per_round: 3,
                ..MetaRetentionOptions::default()
            },
        );
        assert_eq!(result.purge_count(), 3);
        assert_eq!(result.capped, 7);
        // Deterministic: the lowest addresses go first.
        assert_eq!(result.servers, vec!["s0", "s1", "s2"]);
    }

    #[test]
    fn the_cap_spends_on_proxies_before_servers() {
        // Proxies have no dependents, so they are the cheapest thing to forget.
        let result = plan(
            &candidates(&[("s0", NOW - 2 * DAY)]),
            &candidates(&[("p0", NOW - 2 * DAY)]),
            &[],
            &BTreeMap::new(),
            &BTreeMap::new(),
            MetaRetentionOptions {
                max_purges_per_round: 1,
                ..MetaRetentionOptions::default()
            },
        );
        assert_eq!(result.proxies, vec!["p0"]);
        assert!(result.servers.is_empty());
        assert_eq!(result.capped, 1);
    }

    #[test]
    fn each_resource_kind_ages_on_its_own_clock() {
        let result = plan(
            &candidates(&[("s0", NOW - 2 * DAY)]),
            &candidates(&[("p0", NOW - 2 * DAY)]),
            &candidates(&[("ns.t0", NOW - 2 * DAY)]),
            &BTreeMap::new(),
            &BTreeMap::new(),
            MetaRetentionOptions {
                server_retention_ms: DAY,
                proxy_retention_ms: 60_000,
                table_retention_ms: 7 * DAY,
                ..MetaRetentionOptions::default()
            },
        );
        assert_eq!(result.servers, vec!["s0"]);
        assert_eq!(result.proxies, vec!["p0"]);
        // The table is only two days old against a seven-day retention.
        assert!(result.tables.is_empty());
    }

    #[test]
    fn purging_removes_the_resources_from_meta_state_and_from_snapshots() {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
        });
        meta.register_proxy(RegisterProxyRequest {
            proxy_addr: "proxy-a".to_string(),
            namespace: "ns".to_string(),
            location: "rack-1".to_string(),
            config_version: 1,
            binary_version: "v1".to_string(),
        });
        assert!(meta
            .drop_server(StateChangeRequest {
                endpoint: "node-a".to_string(),
                freeze_cooldown_ms: 0,
            })
            .status
            .ok);
        assert!(meta
            .drop_proxy(StateChangeRequest {
                endpoint: "proxy-a".to_string(),
                freeze_cooldown_ms: 0,
            })
            .status
            .ok);

        // The tombstones are visible until they age out.
        assert_eq!(meta.list_servers().servers.len(), 1);
        assert_eq!(meta.list_proxies().proxies.len(), 1);
        assert!(meta
            .purge_expired_meta(MetaRetentionOptions::default())
            .plan
            .is_empty());

        let report = meta.purge_expired_meta(MetaRetentionOptions {
            server_retention_ms: 0,
            proxy_retention_ms: 0,
            table_retention_ms: 0,
            max_purges_per_round: 20,
        });
        assert!(report.status.ok);
        assert_eq!(report.plan.servers, vec!["node-a"]);
        assert_eq!(report.plan.proxies, vec!["proxy-a"]);
        assert!(meta.list_servers().servers.is_empty());
        assert!(meta.list_proxies().proxies.is_empty());
        // And they are gone from the snapshot that raft peers install.
        let snapshot = meta.export_snapshot();
        assert!(snapshot.servers.is_empty());
        assert!(snapshot.proxies.is_empty());
    }

    #[test]
    fn a_resource_that_comes_back_loses_its_tombstone_clock() {
        // Re-registering a dropped server must not leave it scheduled for
        // collection while it is serving.
        let meta = SingleNodeMeta::default();
        let register = || {
            meta.register_server(RegisterServerRequest {
                server_addr: "node-a".to_string(),
                node_id: 1,
                location: "rack-1".to_string(),
                binary_version: "v1".to_string(),
            })
        };
        register();
        meta.drop_server(StateChangeRequest {
            endpoint: "node-a".to_string(),
            freeze_cooldown_ms: 0,
        });
        register();
        let report = meta.purge_expired_meta(MetaRetentionOptions {
            server_retention_ms: 0,
            proxy_retention_ms: 0,
            table_retention_ms: 0,
            max_purges_per_round: 20,
        });
        assert!(report.plan.is_empty());
        assert_eq!(meta.list_servers().servers.len(), 1);
    }

    #[test]
    fn drop_timestamps_survive_a_snapshot_round_trip() {
        // A peer installing a snapshot must keep ageing the tombstones it
        // inherits rather than restarting every one of their clocks.
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
        });
        meta.drop_server(StateChangeRequest {
            endpoint: "node-a".to_string(),
            freeze_cooldown_ms: 0,
        });
        let snapshot = meta.export_snapshot();
        assert_eq!(snapshot.dropped_since_ms.len(), 1);

        let restored = SingleNodeMeta::default();
        assert!(restored.install_snapshot(snapshot).status.ok);
        let report = restored.purge_expired_meta(MetaRetentionOptions {
            server_retention_ms: 0,
            proxy_retention_ms: 0,
            table_retention_ms: 0,
            max_purges_per_round: 20,
        });
        assert_eq!(report.plan.servers, vec!["node-a"]);
    }
}
