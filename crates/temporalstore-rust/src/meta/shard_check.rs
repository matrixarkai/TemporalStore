// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Reconciliation between what the metaserver routes and what datanodes serve.
//!
//! Every datanode heartbeat carries `shard_states`: the shards that node is
//! actually serving right now. The metaserver keeps its own `shard -> owner`
//! map. Nothing has ever compared the two.
//!
//! So when the two drift apart the metaserver never finds out. A shard can
//! disappear from a node -- it was unloaded by an operator, it failed to reload
//! after a restart, a load was rolled back, a disk went read-only -- and the
//! owner map keeps pointing at that node indefinitely. The node is healthy, its
//! heartbeats never stop, so neither the stale-heartbeat detector nor reboot
//! detection has anything to say about it. Every read for that shard is routed
//! to a server that will miss on all of them, and the only signal is client-side
//! errors.
//!
//! [`compute_auto_rebalance`] cannot see it either: it evacuates shards whose
//! *owner* is unavailable, and this owner is perfectly available. It is the
//! shard that is gone, not the server.
//!
//! This module closes that loop. [`ShardChecker::check`] compares the owner map
//! against what each server reports and produces one
//! [`ShardReassignmentReason::OwnerDiverged`] reassignment per shard that its
//! recorded owner is not serving, so the existing rebalance loop re-places it
//! onto a node that will.
//!
//! Three guards keep the check from doing more harm than the drift:
//!
//! * **Reboot grace.** A node that restarted seconds ago has not finished
//!   reloading, so its shards are legitimately missing for a while. Servers
//!   inside [`ShardCheckOptions::reboot_grace_ms`] of their boot time are
//!   skipped -- otherwise every restart would trigger a full re-placement of
//!   everything that node owned.
//! * **Report capability.** A server is only judged once it has been *seen* to
//!   report shard states at least once
//!   ([`ServerMetaInfo::reports_shard_states`]). A node that never reports them
//!   is indistinguishable from one serving nothing, and treating silence as
//!   "serving nothing" would re-place the entire cluster.
//! * **Settle grace.** A shard the owner reports but has not finished loading
//!   is *in progress*, not missing. `serving_state` distinguishes them:
//!   `loading`, `reloading`, `queued`, `running` and `unloading` are transient,
//!   and acting on them would cancel work already underway and re-place a shard
//!   that was about to serve — thrashing the very load it is waiting on. Those
//!   states become actionable only after
//!   [`ShardCheckOptions::settle_grace_ms`] of being continuously reported that
//!   way. `serving` and `readonly` are healthy and never diverge; `failed` and
//!   `unloaded` are terminal and act immediately, as does a shard absent from
//!   the report entirely.
//! * **Rate limit.** At most [`ShardCheckOptions::max_moves_per_window`]
//!   divergences are acted on per window. A correlated fault can make many
//!   shards look missing at once, and reacting to all of them would move more
//!   data than the fault did.
//!
//! The comparison itself is pure; only the rate-limit window is stateful.

use std::collections::BTreeSet;

use super::*;

/// Tuning for [`ShardChecker`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardCheckOptions {
    /// Skip a server this recently booted; it is still reloading its shards.
    pub reboot_grace_ms: u64,
    /// How long a shard may sit in a transient serving state before the
    /// metaserver stops waiting for it and treats it as diverged.
    pub settle_grace_ms: u64,
    /// Most divergences acted on per window.
    pub max_moves_per_window: usize,
    /// Length of the rate-limit window.
    pub window_ms: u64,
}

impl Default for ShardCheckOptions {
    fn default() -> Self {
        Self {
            reboot_grace_ms: 30_000,
            settle_grace_ms: 120_000,
            max_moves_per_window: 10,
            window_ms: 60_000,
        }
    }
}

/// One shard the metaserver routes to a server that is not serving it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardDivergence {
    pub shard_id: ShardId,
    /// The recorded owner, which is live but does not report this shard.
    pub server_addr: String,
    /// True when the owner reports the shard but is not serving it -- a weaker
    /// signal than the shard being absent from the report entirely.
    #[serde(default)]
    pub reported_unloaded: bool,
    /// The `serving_state` the owner reported, or empty when the shard was
    /// absent from the report altogether.
    #[serde(default)]
    pub serving_state: String,
}

/// What one reconciliation round found.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardCheckReport {
    /// Divergences observed, ordered by shard id. Includes ones the rate limit
    /// declined to act on, so the full picture is always reportable.
    pub diverged: Vec<ShardDivergence>,
    /// Servers skipped because they booted too recently, ordered by address.
    pub skipped_in_reboot_grace: Vec<String>,
    /// Servers skipped because they have never reported shard states, ordered
    /// by address.
    pub skipped_without_shard_reports: Vec<String>,
    /// How many divergences the rate limit held back this round.
    pub rate_limited: usize,
    /// Shards being waited on: reported in a transient state that has not yet
    /// outlasted the settle grace. Not divergences, but worth surfacing --
    /// a shard that never leaves this list is a load that never finishes.
    #[serde(default)]
    pub settling: Vec<ShardId>,
}

/// How a datanode describes one shard it is holding, reduced to what the
/// reconciler needs. Ordered so the healthiest verdict wins when two workers
/// report the same shard mid-handoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ShardHealth {
    /// The node has stopped trying (`failed`), or is holding it unloaded.
    Broken(&'static str),
    /// Work is underway: the node is loading, reloading, queued, running or
    /// unloading it. Not a divergence until it outlasts the settle grace.
    Transient(&'static str),
    /// Serving reads, in read-write or read-only mode.
    Serving(&'static str),
}

impl ShardHealth {
    fn classify(state: &ServerShardServingState) -> Self {
        match state.serving_state.as_str() {
            // `readonly` is a serving mode, not a fault: reads resolve there.
            "serving" | "readonly" => Self::Serving("serving"),
            "loading" | "reloading" | "queued" | "running" => Self::Transient("loading"),
            "unloading" => Self::Transient("unloading"),
            "failed" => Self::Broken("failed"),
            "unloaded" => Self::Broken("unloaded"),
            // An unrecognised state from a newer datanode: fall back to the
            // `loaded` flag rather than guessing it is broken.
            _ if state.loaded => Self::Serving("serving"),
            _ => Self::Broken("unloaded"),
        }
    }
}

/// The shards this server reports itself as actually serving.
///
/// Uses the same classification the divergence check does, so "serving" means
/// the same thing to both: a shard mid-load or one the node has given up on is
/// not being served, and `readonly` is.
pub fn serving_shards(server: &ServerMetaInfo) -> BTreeSet<ShardId> {
    server
        .shard_states
        .iter()
        .filter(|state| matches!(ShardHealth::classify(state), ShardHealth::Serving(_)))
        .map(|state| state.shard_id)
        .collect()
}

/// Stateful reconciler: pure comparison plus a rate-limit window.
#[derive(Debug, Clone)]
pub struct ShardChecker {
    options: ShardCheckOptions,
    window_start_ms: u64,
    moves_this_window: usize,
    /// When each (shard, owner) pair was first seen in a transient state, so the
    /// settle grace is measured from the first observation rather than from a
    /// load start time the metaserver does not know.
    settling_since_ms: BTreeMap<(ShardId, String), u64>,
}

impl Default for ShardChecker {
    fn default() -> Self {
        Self::new(ShardCheckOptions::default())
    }
}

impl ShardChecker {
    pub fn new(options: ShardCheckOptions) -> Self {
        Self {
            options,
            window_start_ms: 0,
            moves_this_window: 0,
            settling_since_ms: BTreeMap::new(),
        }
    }

    pub fn options(&self) -> ShardCheckOptions {
        self.options
    }

    /// Divergences acted on in the current window.
    pub fn moves_this_window(&self) -> usize {
        self.moves_this_window
    }

    /// Compare the owner map against what each server reports serving.
    ///
    /// Pure apart from the rate-limit window: the same inputs produce the same
    /// report, and every output vector is sorted.
    pub fn check(
        &mut self,
        shard_owners: &BTreeMap<ShardId, String>,
        servers: &[ServerMetaInfo],
        now_ms: u64,
    ) -> ShardCheckReport {
        self.roll_window(now_ms);

        let mut report = ShardCheckReport::default();
        // Which servers may be judged this round, and what each one serves.
        let mut served: BTreeMap<&str, BTreeMap<ShardId, ShardHealth>> = BTreeMap::new();
        let mut judgeable: BTreeSet<&str> = BTreeSet::new();
        for server in servers {
            if server.state != MetaEntityState::Normal {
                // A frozen or dropped server is already being handled elsewhere;
                // its shards are not "diverged", they are unavailable.
                continue;
            }
            if !server.reports_shard_states {
                report
                    .skipped_without_shard_reports
                    .push(server.server_addr.clone());
                continue;
            }
            if self.in_reboot_grace(server, now_ms) {
                report.skipped_in_reboot_grace.push(server.server_addr.clone());
                continue;
            }
            judgeable.insert(server.server_addr.as_str());
            let entry = served.entry(server.server_addr.as_str()).or_default();
            for state in &server.shard_states {
                let health = ShardHealth::classify(state);
                // A shard reported twice (two workers mid-handoff) keeps the
                // healthiest verdict: one worker still serving it is enough.
                let health = match entry.get(&state.shard_id) {
                    Some(existing) => (*existing).max(health),
                    None => health,
                };
                entry.insert(state.shard_id, health);
            }
        }
        report.skipped_in_reboot_grace.sort();
        report.skipped_without_shard_reports.sort();

        let mut still_settling = BTreeMap::new();
        for (shard_id, owner_addr) in shard_owners {
            if !judgeable.contains(owner_addr.as_str()) {
                continue;
            }
            let key = (*shard_id, owner_addr.clone());
            let health = served
                .get(owner_addr.as_str())
                .and_then(|shards| shards.get(shard_id))
                .copied();
            let (diverged, serving_state) = match health {
                // Serving, in either mode: nothing to reconcile.
                Some(ShardHealth::Serving(_)) => (false, None),
                Some(ShardHealth::Transient(state)) => {
                    // The node is working on it. Start (or continue) the clock,
                    // and only give up once the grace has elapsed -- acting
                    // sooner would cancel a load that was about to finish.
                    let since = self
                        .settling_since_ms
                        .get(&key)
                        .copied()
                        .unwrap_or(now_ms);
                    still_settling.insert(key.clone(), since);
                    if now_ms.saturating_sub(since) >= self.options.settle_grace_ms {
                        (true, Some(state))
                    } else {
                        report.settling.push(*shard_id);
                        (false, None)
                    }
                }
                // The node has stopped trying, or never had it.
                Some(ShardHealth::Broken(state)) => (true, Some(state)),
                None => (true, None),
            };
            if diverged {
                report.diverged.push(ShardDivergence {
                    shard_id: *shard_id,
                    server_addr: owner_addr.clone(),
                    reported_unloaded: serving_state.is_some(),
                    serving_state: serving_state.unwrap_or_default().to_string(),
                });
            }
        }
        // Forget clocks for shards that recovered or moved: a later stall must
        // start its own grace rather than inherit an expired one.
        self.settling_since_ms = still_settling;
        report.settling.sort();
        report.settling.dedup();
        report
    }

    /// Turn a report into reassignments, spending the window's budget. Each
    /// diverged shard moves to the least loaded live server that is not its
    /// current owner; a shard with nowhere else to go is left alone rather than
    /// pointed at the same node again.
    pub fn plan_moves(
        &mut self,
        report: &mut ShardCheckReport,
        shard_owners: &BTreeMap<ShardId, String>,
        live_servers: &BTreeSet<String>,
    ) -> Vec<ShardReassignment> {
        let mut moves = Vec::new();
        if live_servers.is_empty() {
            return moves;
        }
        let mut load: BTreeMap<String, usize> =
            live_servers.iter().map(|addr| (addr.clone(), 0)).collect();
        for owner_addr in shard_owners.values() {
            if let Some(count) = load.get_mut(owner_addr) {
                *count += 1;
            }
        }

        let mut rate_limited = 0_usize;
        let diverged = report.diverged.clone();
        for divergence in &diverged {
            if self.moves_this_window >= self.options.max_moves_per_window {
                rate_limited += 1;
                continue;
            }
            let Some(target) = load
                .iter()
                .filter(|(addr, _)| *addr != &divergence.server_addr)
                .min_by(|(addr_a, load_a), (addr_b, load_b)| {
                    load_a.cmp(load_b).then_with(|| addr_a.cmp(addr_b))
                })
                .map(|(addr, _)| addr.clone())
            else {
                // The diverging owner is the only live server. Re-placing onto
                // itself would achieve nothing; leave the route and let the
                // node reload.
                continue;
            };
            if let Some(count) = load.get_mut(&divergence.server_addr) {
                *count = count.saturating_sub(1);
            }
            *load.entry(target.clone()).or_default() += 1;
            self.moves_this_window += 1;
            moves.push(ShardReassignment {
                shard_id: divergence.shard_id,
                from_server: Some(divergence.server_addr.clone()),
                to_server: target,
                reason: ShardReassignmentReason::OwnerDiverged,
            });
        }
        report.rate_limited = rate_limited;
        moves
    }

    fn roll_window(&mut self, now_ms: u64) {
        let window = self.options.window_ms.max(1);
        if self.window_start_ms == 0 || now_ms.saturating_sub(self.window_start_ms) >= window {
            self.window_start_ms = now_ms;
            self.moves_this_window = 0;
        }
    }

    /// True while the server is still inside its post-boot reload window. A zero
    /// or future-dated boot time yields false: an unknown boot time must not
    /// grant an unbounded grace.
    fn in_reboot_grace(&self, server: &ServerMetaInfo, now_ms: u64) -> bool {
        if self.options.reboot_grace_ms == 0 || server.boot_time_ms == 0 {
            return false;
        }
        now_ms
            .checked_sub(server.boot_time_ms)
            .is_some_and(|age| age < self.options.reboot_grace_ms)
    }
}

impl SingleNodeMeta {
    /// Run one reconciliation round against the current state and return both
    /// the findings and the reassignments to drive.
    pub fn check_shard_divergence(
        &self,
        checker: &mut ShardChecker,
    ) -> (ShardCheckReport, Vec<ShardReassignment>) {
        let now = now_ms();
        let (shard_owners, servers, live_servers) = {
            let state = self.inner.read().expect("meta lock poisoned");
            // A frozen shard is out of service on purpose, so its owner not
            // serving it is expected rather than divergence.
            let shard_owners = serving_shard_owners(&state);
            let servers = state.servers.values().cloned().collect::<Vec<_>>();
            let live_servers = state
                .servers
                .values()
                .filter(|server| server.state == MetaEntityState::Normal)
                .map(|server| server.server_addr.clone())
                .collect::<BTreeSet<_>>();
            (shard_owners, servers, live_servers)
        };
        let mut report = checker.check(&shard_owners, &servers, now);
        let moves = checker.plan_moves(&mut report, &shard_owners, &live_servers);
        self.metrics.record_divergence(&report);
        if !report.diverged.is_empty() {
            let mut state = self.inner.write().expect("meta lock poisoned");
            for divergence in &report.diverged {
                record_topology_event(
                    &mut state,
                    "shard_divergence_detected",
                    format!("shard:{}", divergence.shard_id),
                    format!(
                        "owner={},reported_unloaded={}",
                        divergence.server_addr, divergence.reported_unloaded
                    ),
                );
            }
        }
        (report, moves)
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn a_server_the_check_skipped_is_not_reported_as_consistent() {
        // The check skips a server that has never reported shard states -- it
        // `continue`s, so that server's shards are never examined. Without this
        // reported, `diverged 0` means either "nothing diverged" or "the servers
        // that mattered were never looked at", and nothing tells them apart.
        let meta = SingleNodeMeta::default();
        assert!(meta
            .register_server(RegisterServerRequest {
                numa_nodes: Vec::new(),
                server_addr: "quiet".to_string(),
                node_id: 1,
                location: "rack-1".to_string(),
                binary_version: "v1".to_string(),
            })
            .status
            .ok);

        let mut checker = ShardChecker::new(ShardCheckOptions::default());
        let (report, _) = meta.check_shard_divergence(&mut checker);
        assert!(report.diverged.is_empty());
        assert_eq!(report.skipped_without_shard_reports.len(), 1, "{report:?}");

        let exported = meta.subsystem_metrics().prometheus();
        assert!(
            exported.contains(
                "temporalstore_meta_divergence_skipped{reason=\"no_shard_reports\"} 1"
            ),
            "a server the check never examined was not reported:\n{exported}"
        );
        // And the clean reason stays at zero, so the two are distinguishable.
        assert!(
            exported
                .contains("temporalstore_meta_divergence_skipped{reason=\"reboot_grace\"} 0"),
            "{exported}"
        );
    }

    use super::*;

    fn owners(pairs: &[(ShardId, &str)]) -> BTreeMap<ShardId, String> {
        pairs
            .iter()
            .map(|(shard_id, addr)| (*shard_id, (*addr).to_string()))
            .collect()
    }

    fn live(addrs: &[&str]) -> BTreeSet<String> {
        addrs.iter().map(|addr| (*addr).to_string()).collect()
    }

    fn serving_state(shard_id: ShardId, state: &str) -> ServerShardServingState {
        ServerShardServingState {
            shard_id,
            serving_state: state.to_string(),
            worker_index: 0,
            worker_threads: 1,
            loaded: matches!(state, "serving" | "readonly"),
            readonly: state == "readonly",
            load_version: 1,
            table_name: "ns.orders".to_string(),
            shard_uri: String::new(),
            start_routing_bucket: 0,
            end_routing_bucket: u32::MAX,
            total_records: 0,
            storage_bytes: 0,
            cache_memory_bytes: 0,
            storage: ShardCanonicalStorageStats::default(),
            block_store_bytes_written: 0,
            wal_sequence: 0,
            dirty_object_count: 0,
            dirty_bucket_count: 0,
        }
    }

    /// A healthy, reporting server that boots long before any test clock.
    fn server(addr: &str, serving: &[(ShardId, &str)]) -> ServerMetaInfo {
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
            node_id: 1,
            location: "rack-1".to_string(),
            state: MetaEntityState::Normal,
            last_heartbeat_ms: 1_000_000,
            frozen_since_ms: 0,
            freeze_cooldown_until_ms: 0,
            boot_time_ms: 1,
            reported_boot_time_ms: 1,
            reboot_detected: false,
            reports_shard_states: true,
            binary_version: "test".to_string(),
            shard_loads: Vec::new(),
            shard_stat_loads: Vec::new(),
            runtime_load: ServerRuntimeLoad::default(),
            shard_states: serving
                .iter()
                .map(|(shard_id, state)| serving_state(*shard_id, *state))
                .collect(),
        }
    }

    const NOW: u64 = 1_000_000;

    #[test]
    fn a_shard_the_owner_no_longer_serves_is_detected_and_re_placed() {
        // The owner is healthy and heartbeating; it simply stopped serving
        // shard 2. Nothing else in the metaserver can see this.
        let mut checker = ShardChecker::default();
        let shard_owners = owners(&[(1, "s1"), (2, "s1"), (3, "s2")]);
        let servers = vec![
            server("s1", &[(1, "serving")]),
            server("s2", &[(3, "serving")]),
        ];
        let mut report = checker.check(&shard_owners, &servers, NOW);
        assert_eq!(report.diverged.len(), 1);
        assert_eq!(report.diverged[0].shard_id, 2);
        assert_eq!(report.diverged[0].server_addr, "s1");
        assert!(!report.diverged[0].reported_unloaded);

        let moves = checker.plan_moves(&mut report, &shard_owners, &live(&["s1", "s2"]));
        assert_eq!(moves.len(), 1);
        assert_eq!(moves[0].shard_id, 2);
        assert_eq!(moves[0].to_server, "s2");
        assert_eq!(moves[0].reason, ShardReassignmentReason::OwnerDiverged);
    }

    #[test]
    fn a_shard_the_node_gave_up_on_diverges_immediately() {
        // `failed` is terminal: the node is not going to serve this by waiting.
        let mut checker = ShardChecker::default();
        let report = checker.check(&owners(&[(1, "s1")]), &[server("s1", &[(1, "failed")])], NOW);
        assert_eq!(report.diverged.len(), 1);
        assert!(report.diverged[0].reported_unloaded);
        assert_eq!(report.diverged[0].serving_state, "failed");
        assert!(report.settling.is_empty());
    }

    #[test]
    fn an_unloaded_shard_diverges_immediately() {
        let mut checker = ShardChecker::default();
        let report = checker.check(&owners(&[(1, "s1")]), &[server("s1", &[(1, "unloaded")])], NOW);
        assert_eq!(report.diverged.len(), 1);
        assert_eq!(report.diverged[0].serving_state, "unloaded");
    }

    #[test]
    fn a_shard_that_is_still_loading_is_not_yanked_out_from_under_the_load() {
        // The node is mid-load. Re-placing it now cancels work already underway
        // and thrashes the very load the metaserver is waiting on.
        for state in ["loading", "reloading", "queued", "running", "unloading"] {
            let mut checker = ShardChecker::default();
            let report =
                checker.check(&owners(&[(1, "s1")]), &[server("s1", &[(1, state)])], NOW);
            assert!(
                report.diverged.is_empty(),
                "{state} must not diverge on sight"
            );
            assert_eq!(report.settling, vec![1], "{state} should be waited on");
        }
    }

    #[test]
    fn a_readonly_shard_is_serving_and_never_diverges() {
        // Read-only is a serving mode, not a fault: reads resolve there.
        let mut checker = ShardChecker::default();
        let report = checker.check(&owners(&[(1, "s1")]), &[server("s1", &[(1, "readonly")])], NOW);
        assert!(report.diverged.is_empty());
        assert!(report.settling.is_empty());
    }

    #[test]
    fn a_load_that_never_finishes_eventually_diverges() {
        let mut checker = ShardChecker::default();
        let shard_owners = owners(&[(1, "s1")]);
        let servers = [server("s1", &[(1, "loading")])];

        // Inside the grace the metaserver keeps waiting.
        assert!(checker.check(&shard_owners, &servers, NOW).diverged.is_empty());
        let grace = ShardCheckOptions::default().settle_grace_ms;
        assert!(checker
            .check(&shard_owners, &servers, NOW + grace - 1)
            .diverged
            .is_empty());

        // Past it, the load is not coming and the shard is re-placed.
        let report = checker.check(&shard_owners, &servers, NOW + grace);
        assert_eq!(report.diverged.len(), 1);
        assert_eq!(report.diverged[0].serving_state, "loading");
        assert!(report.settling.is_empty());
    }

    #[test]
    fn the_settle_clock_restarts_after_the_shard_recovers() {
        // A shard that loaded successfully and later stalls again gets a fresh
        // grace, rather than inheriting an already-expired one.
        let mut checker = ShardChecker::default();
        let shard_owners = owners(&[(1, "s1")]);
        let grace = ShardCheckOptions::default().settle_grace_ms;

        checker.check(&shard_owners, &[server("s1", &[(1, "loading")])], NOW);
        checker.check(&shard_owners, &[server("s1", &[(1, "serving")])], NOW + 1_000);
        // Stalls again well past the original clock; the grace starts over.
        let report = checker.check(
            &shard_owners,
            &[server("s1", &[(1, "loading")])],
            NOW + grace + 2_000,
        );
        assert!(report.diverged.is_empty());
        assert_eq!(report.settling, vec![1]);
    }

    #[test]
    fn a_shard_two_workers_disagree_about_keeps_the_healthier_verdict() {
        // Mid-handoff one worker can report `loading` while another serves it.
        // One worker serving is enough.
        let mut checker = ShardChecker::default();
        let mut handoff = server("s1", &[(1, "loading")]);
        handoff.shard_states.push(serving_state(1, "serving"));
        let report = checker.check(&owners(&[(1, "s1")]), &[handoff], NOW);
        assert!(report.diverged.is_empty());
        assert!(report.settling.is_empty());
    }

    #[test]
    fn a_fully_consistent_cluster_reports_nothing() {
        let mut checker = ShardChecker::default();
        let report = checker.check(
            &owners(&[(1, "s1"), (2, "s2")]),
            &[server("s1", &[(1, "serving")]), server("s2", &[(2, "serving")])],
            NOW,
        );
        assert!(report.diverged.is_empty());
        assert!(report.skipped_in_reboot_grace.is_empty());
        assert!(report.skipped_without_shard_reports.is_empty());
    }

    #[test]
    fn a_server_that_just_booted_is_left_alone_to_reload() {
        // Every shard is missing because the node has not finished loading yet.
        // Acting on that would re-place the node's entire holding on every
        // restart -- far more damage than the restart itself.
        let mut checker = ShardChecker::default();
        let mut booting = server("s1", &[]);
        booting.boot_time_ms = NOW - 5_000; // 5s old, grace is 30s
        let report = checker.check(&owners(&[(1, "s1"), (2, "s1")]), &[booting], NOW);
        assert!(report.diverged.is_empty());
        assert_eq!(report.skipped_in_reboot_grace, vec!["s1"]);
    }

    #[test]
    fn the_reboot_grace_expires() {
        let mut checker = ShardChecker::default();
        let mut booted = server("s1", &[]);
        booted.boot_time_ms = NOW - 31_000; // past the 30s grace
        let report = checker.check(&owners(&[(1, "s1")]), &[booted], NOW);
        assert!(report.skipped_in_reboot_grace.is_empty());
        assert_eq!(report.diverged.len(), 1);
    }

    #[test]
    fn a_server_that_never_reports_shard_states_is_never_judged() {
        // An old build reporting nothing is indistinguishable from one serving
        // nothing. Guessing would re-place the whole cluster on upgrade.
        let mut checker = ShardChecker::default();
        let mut silent = server("s1", &[]);
        silent.reports_shard_states = false;
        let report = checker.check(&owners(&[(1, "s1"), (2, "s1")]), &[silent], NOW);
        assert!(report.diverged.is_empty());
        assert_eq!(report.skipped_without_shard_reports, vec!["s1"]);
    }

    #[test]
    fn a_reporting_server_that_now_serves_nothing_is_judged() {
        // Once a server has been seen reporting shard states, an empty report is
        // real information: it dropped everything.
        let mut checker = ShardChecker::default();
        let report = checker.check(&owners(&[(1, "s1"), (2, "s1")]), &[server("s1", &[])], NOW);
        assert_eq!(report.diverged.len(), 2);
    }

    #[test]
    fn a_frozen_owner_is_not_a_divergence() {
        // The freeze path already owns this server; its shards are unavailable,
        // not diverged, and evacuation will move them.
        let mut checker = ShardChecker::default();
        let mut frozen = server("s1", &[]);
        frozen.state = MetaEntityState::Frozen;
        let report = checker.check(&owners(&[(1, "s1")]), &[frozen], NOW);
        assert!(report.diverged.is_empty());
        assert!(report.skipped_without_shard_reports.is_empty());
    }

    #[test]
    fn an_unregistered_owner_is_not_a_divergence() {
        // Nothing is known about this address, so nothing can be concluded.
        let mut checker = ShardChecker::default();
        let report = checker.check(&owners(&[(1, "ghost")]), &[server("s1", &[])], NOW);
        assert!(report.diverged.is_empty());
    }

    #[test]
    fn the_rate_limit_bounds_how_much_moves_per_window() {
        // A correlated fault can make many shards look missing at once. Reacting
        // to all of them would move more data than the fault did.
        let mut checker = ShardChecker::new(ShardCheckOptions {
            max_moves_per_window: 2,
            ..ShardCheckOptions::default()
        });
        let shard_owners = owners(&[(1, "s1"), (2, "s1"), (3, "s1"), (4, "s1"), (5, "s1")]);
        let servers = vec![server("s1", &[]), server("s2", &[])];
        let mut report = checker.check(&shard_owners, &servers, NOW);
        // The report is complete even though the action is capped.
        assert_eq!(report.diverged.len(), 5);
        let moves = checker.plan_moves(&mut report, &shard_owners, &live(&["s1", "s2"]));
        assert_eq!(moves.len(), 2);
        assert_eq!(report.rate_limited, 3);

        // Still inside the same window: the budget stays spent.
        let mut again = checker.check(&shard_owners, &servers, NOW + 1_000);
        assert!(checker
            .plan_moves(&mut again, &shard_owners, &live(&["s1", "s2"]))
            .is_empty());

        // A new window refills it.
        let mut next = checker.check(&shard_owners, &servers, NOW + 61_000);
        assert_eq!(
            checker
                .plan_moves(&mut next, &shard_owners, &live(&["s1", "s2"]))
                .len(),
            2
        );
    }

    #[test]
    fn a_divergence_with_nowhere_else_to_go_is_reported_but_not_moved() {
        // Re-placing a shard onto the same node that just lost it achieves
        // nothing; better to leave the route and let the node reload.
        let mut checker = ShardChecker::default();
        let shard_owners = owners(&[(1, "s1")]);
        let mut report = checker.check(&shard_owners, &[server("s1", &[])], NOW);
        assert_eq!(report.diverged.len(), 1);
        assert!(checker
            .plan_moves(&mut report, &shard_owners, &live(&["s1"]))
            .is_empty());
    }

    #[test]
    fn moves_spread_across_targets_rather_than_stacking_on_one() {
        let mut checker = ShardChecker::default();
        let shard_owners = owners(&[(1, "s1"), (2, "s1"), (3, "s1"), (4, "s1")]);
        let servers = vec![server("s1", &[]), server("s2", &[]), server("s3", &[])];
        let mut report = checker.check(&shard_owners, &servers, NOW);
        let moves = checker.plan_moves(&mut report, &shard_owners, &live(&["s1", "s2", "s3"]));
        assert_eq!(moves.len(), 4);
        let targets = moves
            .iter()
            .map(|plan| plan.to_server.clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(targets.len(), 2, "both spare servers should take load");
    }
}
