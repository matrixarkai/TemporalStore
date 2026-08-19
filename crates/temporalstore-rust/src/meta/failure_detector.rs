// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Adaptive heartbeat failure detection and correlated-failure conviction gating.
//!
//! The metaserver decides that a datanode is dead in
//! [`SingleNodeMeta::freeze_stale_resources_with_policy`] (`meta/lifecycle.rs`):
//! any server whose last heartbeat is older than a fixed `stale_after_ms` is
//! frozen. That single fixed threshold has three failure modes this module
//! addresses, all of which turn a small incident into a cluster-wide one:
//!
//! 1. **It is not adaptive.** One threshold is applied to every server no matter
//!    how often that server actually heartbeats. Tuned tight enough to detect a
//!    1s-interval node quickly, it false-positives on a node with a longer
//!    interval or a slower link; tuned loose enough to be safe for the slow
//!    node, detection of the fast node is needlessly late.
//! 2. **A metaserver stall convicts the whole fleet.** `stale_after_ms` is
//!    measured against wall clock, so if the metaserver itself pauses (GC,
//!    scheduling starvation, a slow snapshot install) every server crosses the
//!    threshold at once. The first round after the stall then freezes every
//!    datanode, even though nothing was ever wrong with them.
//! 3. **A correlated failure is treated as N independent ones.** Losing a rack
//!    or a switch makes every server behind it look stale simultaneously.
//!    Freezing them all drains the topology, and the freeze is what
//!    [`compute_auto_rebalance`] and [`compute_raft_failover_triggers`] act on,
//!    so the metaserver amplifies a partial outage into a total one.
//!
//! This module supplies the two mechanisms the fixed threshold lacks:
//!
//! * [`MetaFailureDetector`] is a phi-accrual detector. It learns each server's
//!   own heartbeat interval distribution and reports suspicion as
//!   `phi = -log10(P(still alive | silence so far))` under an exponential
//!   arrival model, so one [`FailureDetectorOptions::phi_failure_threshold`]
//!   means the same confidence for a fast node and a slow one. It also holds a
//!   stall guard: if the detector itself did not run for a while it reports
//!   [`Diagnosis::Unknown`] and suppresses conviction for a grace window rather
//!   than blaming the fleet for its own pause (failure mode 2).
//! * [`plan_conviction`] is a correlated-failure gate. Servers are grouped by
//!   [`ServerMetaInfo::location`], and if the abnormal fraction of a location
//!   exceeds a warning/critical ratio the whole location enters *safe mode*: no
//!   server in it is convicted this round, and the severity is reported so an
//!   operator can act. An isolated failure still convicts immediately, which is
//!   what keeps failure mode 3 from costing detection speed.
//!
//! Both are pure and deterministic given an injected clock, so the whole
//! decision is unit-testable without a running cluster.
//! [`MetaFailureDetector::plan_round`] wires them together into the single call
//! the metaserver loop makes.
//!
//! The detector is fed by polling [`ServerMetaInfo::last_heartbeat_ms`] rather
//! than by hooking the heartbeat handler: that field already carries the true
//! arrival timestamp, so the learned interval distribution is identical, while
//! the heartbeat hot path takes no extra lock.

use std::collections::BTreeSet;

use super::*;

/// log10(e). Converts the exponential-model silence ratio into a phi value, so
/// phi reads as "how many nines of confidence that the node is gone".
const LOG10_E: f64 = std::f64::consts::LOG10_E;

/// Tuning for [`MetaFailureDetector`].
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FailureDetectorOptions {
    /// How many recent heartbeat intervals per server feed the mean.
    pub sample_capacity: usize,
    /// Interval assumed for a server's very first heartbeat, before any real
    /// interval has been observed. Deliberately large: it biases early phi
    /// downward, so a freshly registered server is never convicted on the
    /// strength of a single sample.
    pub initial_interval_ms: u64,
    /// Intervals longer than this are dropped instead of folded into the mean.
    /// They record a stall or a clock artifact, not the node's cadence, and
    /// admitting them would inflate the mean and blind the detector.
    pub max_interval_ms: u64,
    /// Convict when phi exceeds this. 5.0 means roughly "the odds of this much
    /// silence from a live node are about 1 in 10^5".
    pub phi_failure_threshold: f64,
    /// If the detector itself did not run for longer than this, suspicion is
    /// suppressed for the same duration again (the stall guard).
    pub max_round_pause_ms: u64,
}

impl Default for FailureDetectorOptions {
    fn default() -> Self {
        Self {
            sample_capacity: 1000,
            initial_interval_ms: 30_000,
            max_interval_ms: 60_000,
            phi_failure_threshold: 5.0,
            max_round_pause_ms: 5_000,
        }
    }
}

/// What the detector believes about one server right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Diagnosis {
    /// Suspicion is not trustworthy this round (the detector itself stalled, or
    /// is still inside the grace window after a stall). Never convict.
    Unknown,
    /// No heartbeat has ever been observed for this server, so there is no
    /// interval distribution to judge it against. Never convict.
    NotObserved,
    /// Silence so far is consistent with this server's own heartbeat cadence.
    Healthy,
    /// Silence exceeds [`FailureDetectorOptions::phi_failure_threshold`].
    Failed,
}

/// How badly one location is damaged, from the fraction of its servers that are
/// abnormal. Ordered: `Normal` < `Warning` < `Critical`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DamageSeverity {
    Normal,
    Warning,
    Critical,
}

/// Tuning for the correlated-failure gate in [`plan_conviction`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConvictionPolicy {
    /// Abnormal percentage of a location above which it is `Warning`.
    pub warning_ratio_percent: u64,
    /// Abnormal percentage of a location above which it is `Critical`.
    pub critical_ratio_percent: u64,
    /// A location needs at least this many abnormal servers before safe mode can
    /// engage at all. Safe mode exists to catch *correlated* failure; without
    /// this floor a percentage alone would put a small cluster into safe mode on
    /// its first single dead node and then never convict anything.
    pub min_abnormal_for_safe_mode: usize,
    /// Master switch for the correlated-failure gate. With it off, every failed
    /// server is convicted regardless of how many of its neighbours also failed.
    pub safe_mode_enabled: bool,
    /// Master switch for conviction itself. With it off the plan is still
    /// computed and reportable but no server is named for freezing (dry run).
    pub convict_enabled: bool,
    /// Treat a detected reboot as a failure. A restarted datanode has dropped
    /// every shard the metaserver still believes it is serving, so leaving it in
    /// the topology means routing reads to a node that will miss on all of them.
    /// With this off a reboot is reported but not acted on.
    pub convict_on_reboot: bool,
}

impl Default for ConvictionPolicy {
    fn default() -> Self {
        Self {
            warning_ratio_percent: 5,
            critical_ratio_percent: 8,
            min_abnormal_for_safe_mode: 2,
            safe_mode_enabled: true,
            convict_enabled: true,
            convict_on_reboot: true,
        }
    }
}

/// One server as seen by [`plan_conviction`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConvictionCandidate {
    pub server_addr: String,
    /// Location tag used to group servers for the correlated-failure gate.
    /// Servers with an empty location share one implicit group.
    pub location: String,
    /// The server is not currently serving (already frozen). It counts toward
    /// its location's damage but is not convicted again.
    pub abnormal: bool,
    /// The detector diagnosed this serving server as [`Diagnosis::Failed`].
    pub failed: bool,
    /// The server restarted in place: it heartbeated a boot time different from
    /// the one the metaserver anchored on. Unlike `failed` this is not inferred
    /// from silence, so it is trustworthy even while the detector is paused.
    #[serde(default)]
    pub rebooted: bool,
}

/// The damage assessment for one location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocationDamage {
    pub location: String,
    pub severity: DamageSeverity,
    pub total_servers: usize,
    pub abnormal_servers: usize,
    /// True when this location is held back from conviction this round.
    pub safe_mode: bool,
}

/// What the metaserver should do this round.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConvictionPlan {
    /// Servers to freeze, ordered by address.
    pub convict: Vec<String>,
    /// Servers the detector called failed but that safe mode held back, ordered
    /// by address. These are the ones an operator needs to look at.
    pub held_by_safe_mode: Vec<String>,
    /// Per-location damage, ordered by location.
    pub damage: Vec<LocationDamage>,
    /// Serving servers observed to have restarted in place, ordered by address.
    /// Reported whether or not the policy converts a reboot into a conviction,
    /// so the restart is visible even when it is not acted on.
    #[serde(default)]
    pub rebooted: Vec<String>,
}

impl ConvictionPlan {
    /// The worst severity across every location this round.
    pub fn worst_severity(&self) -> DamageSeverity {
        self.damage
            .iter()
            .map(|entry| entry.severity)
            .max()
            .unwrap_or(DamageSeverity::Normal)
    }
}

/// Pure correlated-failure gate: group `candidates` by location, assess each
/// location's damage, and convict only the failed servers in locations that are
/// not in safe mode. Deterministic; every output vector is sorted.
pub fn plan_conviction(
    candidates: &[ConvictionCandidate],
    policy: ConvictionPolicy,
) -> ConvictionPlan {
    let mut by_location: BTreeMap<&str, Vec<&ConvictionCandidate>> = BTreeMap::new();
    for candidate in candidates {
        by_location
            .entry(candidate.location.as_str())
            .or_default()
            .push(candidate);
    }

    let mut plan = ConvictionPlan::default();
    for (location, members) in by_location {
        let total = members.len();
        // A server is a conviction target when the detector called it failed, or
        // when it restarted in place and the policy treats that as a failure.
        let is_target = |member: &ConvictionCandidate| {
            member.failed || (member.rebooted && policy.convict_on_reboot)
        };
        // A targeted server counts as damage too: it is about to stop serving.
        let abnormal = members
            .iter()
            .filter(|member| member.abnormal || is_target(member))
            .count();
        let severity = assess_severity(total, abnormal, policy);
        let safe_mode = policy.safe_mode_enabled && severity != DamageSeverity::Normal;

        plan.rebooted.extend(
            members
                .iter()
                .filter(|member| member.rebooted && !member.abnormal)
                .map(|member| member.server_addr.clone()),
        );

        let mut failed = members
            .iter()
            .filter(|member| is_target(member) && !member.abnormal)
            .map(|member| member.server_addr.clone())
            .collect::<Vec<_>>();
        failed.sort();
        if safe_mode || !policy.convict_enabled {
            plan.held_by_safe_mode.extend(failed);
        } else {
            plan.convict.extend(failed);
        }

        plan.damage.push(LocationDamage {
            location: location.to_string(),
            severity,
            total_servers: total,
            abnormal_servers: abnormal,
            safe_mode,
        });
    }
    plan.convict.sort();
    plan.held_by_safe_mode.sort();
    plan.rebooted.sort();
    plan
}

/// Damage severity for one location. Ratios are compared without integer
/// division so a small location is not rounded straight into safe mode, and the
/// `min_abnormal_for_safe_mode` floor keeps an isolated failure convictable.
fn assess_severity(total: usize, abnormal: usize, policy: ConvictionPolicy) -> DamageSeverity {
    if total == 0 || abnormal < policy.min_abnormal_for_safe_mode.max(1) {
        return DamageSeverity::Normal;
    }
    let abnormal_scaled = abnormal as u128 * 100;
    let total = total as u128;
    if abnormal_scaled > total * policy.critical_ratio_percent as u128 {
        DamageSeverity::Critical
    } else if abnormal_scaled > total * policy.warning_ratio_percent as u128 {
        DamageSeverity::Warning
    } else {
        DamageSeverity::Normal
    }
}

/// Sliding window of one server's recent heartbeat intervals.
#[derive(Debug, Clone)]
struct ArrivalWindow {
    intervals: VecDeque<u64>,
    sum: u64,
    last_arrival_ms: u64,
}

impl ArrivalWindow {
    fn new(first_arrival_ms: u64, options: &FailureDetectorOptions) -> Self {
        let mut window = Self {
            intervals: VecDeque::with_capacity(options.sample_capacity.max(1)),
            sum: 0,
            last_arrival_ms: first_arrival_ms,
        };
        // Seed with the conservative initial interval so the mean is defined
        // from the first observation and starts out biased against conviction.
        window.push(options.initial_interval_ms.max(1), options);
        window
    }

    fn push(&mut self, interval_ms: u64, options: &FailureDetectorOptions) {
        let capacity = options.sample_capacity.max(1);
        while self.intervals.len() >= capacity {
            if let Some(evicted) = self.intervals.pop_front() {
                self.sum -= evicted;
            }
        }
        self.intervals.push_back(interval_ms);
        self.sum += interval_ms;
    }

    fn observe(&mut self, arrival_ms: u64, options: &FailureDetectorOptions) {
        if arrival_ms <= self.last_arrival_ms {
            // Not a new heartbeat (or the clock went backwards): nothing to learn.
            return;
        }
        let interval = arrival_ms - self.last_arrival_ms;
        self.last_arrival_ms = arrival_ms;
        if interval <= options.max_interval_ms {
            self.push(interval, options);
        }
    }

    fn mean_ms(&self) -> f64 {
        if self.intervals.is_empty() {
            return 0.0;
        }
        self.sum as f64 / self.intervals.len() as f64
    }

    /// Suspicion that this server is gone, given how long it has been silent
    /// relative to its own learned cadence.
    fn phi(&self, now_ms: u64) -> f64 {
        let mean = self.mean_ms();
        if mean <= 0.0 || now_ms <= self.last_arrival_ms {
            return 0.0;
        }
        LOG10_E * (now_ms - self.last_arrival_ms) as f64 / mean
    }
}

/// Per-server phi-accrual heartbeat failure detector with a stall guard.
///
/// Feed it one round at a time: [`MetaFailureDetector::begin_round`], then
/// [`MetaFailureDetector::observe`] for every server's latest heartbeat
/// timestamp, then [`MetaFailureDetector::diagnose`] for each. Or call
/// [`MetaFailureDetector::plan_round`], which does all of that and applies the
/// correlated-failure gate.
#[derive(Debug, Clone)]
pub struct MetaFailureDetector {
    options: FailureDetectorOptions,
    windows: BTreeMap<String, ArrivalWindow>,
    last_round_ms: u64,
    /// Suspicion stays suppressed until this timestamp after a detector stall.
    paused_until_ms: u64,
}

impl Default for MetaFailureDetector {
    fn default() -> Self {
        Self::new(FailureDetectorOptions::default())
    }
}

impl MetaFailureDetector {
    pub fn new(options: FailureDetectorOptions) -> Self {
        Self {
            options,
            windows: BTreeMap::new(),
            last_round_ms: 0,
            paused_until_ms: 0,
        }
    }

    pub fn options(&self) -> FailureDetectorOptions {
        self.options
    }

    /// Number of servers with a learned interval distribution.
    pub fn tracked_servers(&self) -> usize {
        self.windows.len()
    }

    /// Open a detection round. Returns `false` when suspicion is not trustworthy
    /// this round: either the detector just started, or it did not run for
    /// longer than [`FailureDetectorOptions::max_round_pause_ms`], in which case
    /// every server looks silent through no fault of its own. A stall re-arms
    /// the grace window, so conviction stays suppressed until the fleet has had
    /// a full pause window to be heard from again.
    pub fn begin_round(&mut self, now_ms: u64) -> bool {
        let previous = self.last_round_ms;
        self.last_round_ms = now_ms;
        if previous == 0 {
            // First round ever: nothing was being watched before it, so silence
            // carries no information yet.
            self.paused_until_ms = now_ms.saturating_add(self.options.max_round_pause_ms);
            return false;
        }
        if now_ms.saturating_sub(previous) > self.options.max_round_pause_ms {
            self.paused_until_ms = now_ms.saturating_add(self.options.max_round_pause_ms);
            return false;
        }
        now_ms >= self.paused_until_ms
    }

    /// True when the current round's suspicion is trustworthy.
    pub fn is_active(&self, now_ms: u64) -> bool {
        self.last_round_ms != 0 && now_ms >= self.paused_until_ms
    }

    /// Record that `server_addr`'s newest heartbeat arrived at
    /// `last_heartbeat_ms`. Repeated calls with an unchanged timestamp are
    /// no-ops, so this is safe to call every round for every server.
    pub fn observe(&mut self, server_addr: &str, last_heartbeat_ms: u64) {
        if last_heartbeat_ms == 0 {
            return;
        }
        match self.windows.get_mut(server_addr) {
            Some(window) => window.observe(last_heartbeat_ms, &self.options),
            None => {
                self.windows.insert(
                    server_addr.to_string(),
                    ArrivalWindow::new(last_heartbeat_ms, &self.options),
                );
            }
        }
    }

    /// Stop tracking a server (it left the cluster).
    pub fn forget(&mut self, server_addr: &str) {
        self.windows.remove(server_addr);
    }

    /// Drop every server that is no longer present, so a long-lived detector
    /// does not accumulate windows for departed nodes.
    pub fn retain_only(&mut self, present: &BTreeSet<String>) {
        self.windows.retain(|addr, _| present.contains(addr));
    }

    /// Current suspicion for one server, or `None` if it was never observed.
    pub fn phi(&self, server_addr: &str, now_ms: u64) -> Option<f64> {
        self.windows.get(server_addr).map(|window| window.phi(now_ms))
    }

    /// The server's learned mean heartbeat interval, or `None` if unobserved.
    pub fn mean_interval_ms(&self, server_addr: &str) -> Option<f64> {
        self.windows.get(server_addr).map(|window| window.mean_ms())
    }

    /// Diagnose one server. Returns [`Diagnosis::Unknown`] whenever the detector
    /// is paused, so a stalled metaserver can never convict anybody.
    pub fn diagnose(&self, server_addr: &str, now_ms: u64) -> Diagnosis {
        if !self.is_active(now_ms) {
            return Diagnosis::Unknown;
        }
        let Some(window) = self.windows.get(server_addr) else {
            return Diagnosis::NotObserved;
        };
        if window.phi(now_ms) > self.options.phi_failure_threshold {
            Diagnosis::Failed
        } else {
            Diagnosis::Healthy
        }
    }

    /// Run one full detection round over the current server list: learn from
    /// every heartbeat, diagnose every serving server, then apply the
    /// correlated-failure gate. The returned plan names exactly the servers the
    /// metaserver should freeze.
    pub fn plan_round(
        &mut self,
        servers: &[ServerMetaInfo],
        now_ms: u64,
        policy: ConvictionPolicy,
    ) -> ConvictionPlan {
        let active = self.begin_round(now_ms);
        let mut present = BTreeSet::new();
        for server in servers {
            present.insert(server.server_addr.clone());
            self.observe(&server.server_addr, server.last_heartbeat_ms);
        }
        self.retain_only(&present);

        let candidates = servers
            .iter()
            // A dropped server has left the cluster: it is neither damage nor a
            // conviction candidate.
            .filter(|server| server.state != MetaEntityState::Dropped)
            .map(|server| {
                let serving = server.state == MetaEntityState::Normal;
                ConvictionCandidate {
                    server_addr: server.server_addr.clone(),
                    location: server.location.clone(),
                    abnormal: !serving,
                    failed: active
                        && serving
                        && self.diagnose(&server.server_addr, now_ms) == Diagnosis::Failed,
                    // Not gated on `active`: a changed boot time is direct
                    // evidence the process restarted, not an inference drawn
                    // from silence, so the detector's stall guard does not
                    // apply to it.
                    rebooted: serving && server.reboot_detected,
                }
            })
            .collect::<Vec<_>>();
        plan_conviction(&candidates, policy)
    }
}

/// Outcome of one adaptive detection round.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveConvictionReport {
    pub status: Status,
    /// Servers frozen this round.
    pub frozen_servers: Vec<String>,
    /// Servers the detector called failed but that safe mode held back. Nothing
    /// is wrong with the detection; the metaserver is declining to act on it
    /// because acting would widen the outage.
    pub held_by_safe_mode: Vec<String>,
    /// Per-location damage assessment for this round.
    pub damage: Vec<LocationDamage>,
    /// Serving servers observed to have restarted in place this round.
    #[serde(default)]
    pub rebooted: Vec<String>,
    /// True when the detector was paused this round (it had only just started,
    /// or it stalled), so no server could be convicted whatever its silence.
    pub detector_paused: bool,
}

impl SingleNodeMeta {
    /// Run one adaptive detection round and freeze exactly the servers the
    /// resulting [`ConvictionPlan`] names.
    ///
    /// This is the adaptive counterpart to
    /// [`Self::freeze_stale_resources_with_policy`]: instead of freezing every
    /// server past one fixed age, it judges each server against its own learned
    /// heartbeat cadence, declines to act at all if the detector itself stalled,
    /// and holds back conviction in any location that is losing too many servers
    /// at once. Proxies are not covered here and stay on the fixed threshold
    /// (see [`Self::freeze_stale_proxies`]).
    ///
    /// `detector` carries the learned per-server distributions across rounds, so
    /// the caller must keep the same instance for the life of the loop.
    pub fn convict_stale_servers_adaptive(
        &self,
        detector: &mut MetaFailureDetector,
        policy: ConvictionPolicy,
        safe_mode: SafeModePolicy,
    ) -> AdaptiveConvictionReport {
        let now = now_ms();
        let servers = {
            let state = self.inner.read().expect("meta lock poisoned");
            state.servers.values().cloned().collect::<Vec<_>>()
        };
        let plan = detector.plan_round(&servers, now, policy);
        let detector_paused = !detector.is_active(now);

        let mut frozen_servers = Vec::new();
        for endpoint in &plan.convict {
            let response = self.freeze_server(StateChangeRequest {
                endpoint: endpoint.clone(),
                freeze_cooldown_ms: safe_mode.server_freeze_cooldown_ms,
            });
            if !response.status.ok {
                return AdaptiveConvictionReport {
                    status: response.status,
                    frozen_servers,
                    held_by_safe_mode: plan.held_by_safe_mode,
                    damage: plan.damage,
                    rebooted: plan.rebooted,
                    detector_paused,
                };
            }
            frozen_servers.push(endpoint.clone());
        }

        AdaptiveConvictionReport {
            status: Status::ok(),
            frozen_servers,
            held_by_safe_mode: plan.held_by_safe_mode,
            damage: plan.damage,
            rebooted: plan.rebooted,
            detector_paused,
        }
    }

    /// Background loop that replaces [`Self::start_failure_detector_loop`] when
    /// adaptive detection is enabled: adaptive conviction for datanodes, and the
    /// existing fixed threshold for proxies.
    ///
    /// The detector is owned by the loop so its learned distributions survive
    /// across rounds; a restart of the loop starts detection over, which is why
    /// [`MetaFailureDetector::begin_round`] treats its first round as untrusted.
    pub fn start_adaptive_failure_detector_loop(
        &self,
        options: FailureDetectorOptions,
        policy: ConvictionPolicy,
        safe_mode: SafeModePolicy,
        proxy_stale_after_ms: u64,
        interval_ms: u64,
    ) -> thread::JoinHandle<()> {
        let meta = self.clone();
        let interval = Duration::from_millis(interval_ms.max(1));
        let mut detector = MetaFailureDetector::new(options);
        thread::spawn(move || loop {
            let _ = meta.convict_stale_servers_adaptive(&mut detector, policy, safe_mode.clone());
            let _ = meta.freeze_stale_proxies(proxy_stale_after_ms, safe_mode.clone());
            thread::sleep(interval);
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options() -> FailureDetectorOptions {
        FailureDetectorOptions {
            // A tiny seed keeps the tests readable: the learned mean converges to
            // the injected cadence within a couple of beats.
            initial_interval_ms: 1_000,
            ..FailureDetectorOptions::default()
        }
    }

    fn server(addr: &str, location: &str, state: MetaEntityState, heartbeat_ms: u64) -> ServerMetaInfo {
        ServerMetaInfo {
            server_addr: addr.to_string(),
            node_id: 0,
            location: location.to_string(),
            state,
            last_heartbeat_ms: heartbeat_ms,
            frozen_since_ms: 0,
            freeze_cooldown_until_ms: 0,
            boot_time_ms: 1,
            reported_boot_time_ms: 0,
            reboot_detected: false,
            binary_version: "test".to_string(),
            shard_loads: Vec::new(),
            shard_stat_loads: Vec::new(),
            runtime_load: ServerRuntimeLoad::default(),
            shard_states: Vec::new(),
        }
    }

    fn candidate(addr: &str, location: &str, abnormal: bool, failed: bool) -> ConvictionCandidate {
        ConvictionCandidate {
            server_addr: addr.to_string(),
            location: location.to_string(),
            abnormal,
            failed,
            rebooted: false,
        }
    }

    fn rebooted_candidate(addr: &str, location: &str) -> ConvictionCandidate {
        ConvictionCandidate {
            rebooted: true,
            ..candidate(addr, location, false, false)
        }
    }

    /// Drive a detector through `beats` heartbeats spaced `interval_ms` apart so
    /// its learned mean matches that cadence, and return the last arrival time.
    fn train(detector: &mut MetaFailureDetector, addr: &str, interval_ms: u64, beats: u64) -> u64 {
        let mut now = 10_000;
        for _ in 0..beats {
            detector.begin_round(now);
            detector.observe(addr, now);
            now += interval_ms;
        }
        now - interval_ms
    }

    #[test]
    fn phi_is_scaled_by_the_servers_own_heartbeat_interval() {
        // Two servers, same silence, ten-fold different cadence: the fast one is
        // far more suspicious. A fixed stale_after_ms cannot express this.
        let mut fast = MetaFailureDetector::new(options());
        let last_fast = train(&mut fast, "fast", 1_000, 40);
        let mut slow = MetaFailureDetector::new(options());
        let last_slow = train(&mut slow, "slow", 10_000, 40);

        let silence = 20_000;
        let phi_fast = fast.phi("fast", last_fast + silence).expect("observed");
        let phi_slow = slow.phi("slow", last_slow + silence).expect("observed");
        assert!(
            phi_fast > phi_slow * 5.0,
            "fast node should be far more suspicious: fast={phi_fast} slow={phi_slow}"
        );
    }

    #[test]
    fn a_server_beating_on_cadence_is_healthy_and_a_silent_one_fails() {
        let mut detector = MetaFailureDetector::new(options());
        let last = train(&mut detector, "node-a", 1_000, 40);
        // One interval later is entirely normal.
        detector.begin_round(last + 1_000);
        assert_eq!(detector.diagnose("node-a", last + 1_000), Diagnosis::Healthy);
        // Silence far beyond the learned cadence is not.
        detector.begin_round(last + 2_000);
        detector.begin_round(last + 4_000);
        detector.begin_round(last + 6_000);
        detector.begin_round(last + 8_000);
        detector.begin_round(last + 10_000);
        detector.begin_round(last + 12_000);
        assert_eq!(detector.diagnose("node-a", last + 12_000), Diagnosis::Failed);
    }

    #[test]
    fn an_unobserved_server_is_never_convicted() {
        let mut detector = MetaFailureDetector::new(options());
        let last = train(&mut detector, "node-a", 1_000, 40);
        detector.begin_round(last + 1_000);
        assert_eq!(
            detector.diagnose("ghost", last + 1_000),
            Diagnosis::NotObserved
        );
    }

    #[test]
    fn the_first_round_never_convicts() {
        // Nothing was being watched before the first round, so the silence
        // preceding it carries no information.
        let mut detector = MetaFailureDetector::new(options());
        assert!(!detector.begin_round(1_000_000));
        detector.observe("node-a", 1);
        assert_eq!(detector.diagnose("node-a", 1_000_000), Diagnosis::Unknown);
    }

    #[test]
    fn a_detector_stall_suppresses_conviction_for_a_grace_window() {
        // The stall guard: the metaserver, not the fleet, was the thing that
        // stopped. Every node looks silent, and none may be convicted for it.
        let mut detector = MetaFailureDetector::new(options());
        let last = train(&mut detector, "node-a", 1_000, 40);

        let stalled_at = last + 600_000; // detector itself was gone for 10 minutes
        assert!(!detector.begin_round(stalled_at));
        assert_eq!(detector.diagnose("node-a", stalled_at), Diagnosis::Unknown);

        // Still inside the grace window: suspicion stays suppressed.
        let during_grace = stalled_at + options().max_round_pause_ms / 2;
        assert!(!detector.begin_round(during_grace));
        assert_eq!(detector.diagnose("node-a", during_grace), Diagnosis::Unknown);

        // Past the grace window, a server that came back is judged normally
        // again, and one that stayed silent is finally convictable.
        let after_grace = stalled_at + options().max_round_pause_ms + 1;
        detector.observe("node-a", after_grace);
        assert!(detector.begin_round(after_grace));
        assert_eq!(detector.diagnose("node-a", after_grace), Diagnosis::Healthy);
    }

    #[test]
    fn a_restarted_server_is_convicted_without_waiting_for_silence() {
        // The heartbeats never stopped, so no amount of phi would ever flag this
        // node. What changed is that it dropped every shard it was serving.
        let plan = plan_conviction(
            &[
                rebooted_candidate("node-a", "rack-1"),
                candidate("node-b", "rack-1", false, false),
                candidate("node-c", "rack-1", false, false),
                candidate("node-d", "rack-1", false, false),
            ],
            ConvictionPolicy::default(),
        );
        assert_eq!(plan.convict, vec!["node-a"]);
        assert_eq!(plan.rebooted, vec!["node-a"]);
    }

    #[test]
    fn a_reboot_is_still_subject_to_safe_mode() {
        // A rolling restart that takes out half a rack is as damaging as a rack
        // fault, and the same guard has to hold.
        let plan = plan_conviction(
            &[
                rebooted_candidate("node-a", "rack-1"),
                rebooted_candidate("node-b", "rack-1"),
                candidate("node-c", "rack-1", false, false),
                candidate("node-d", "rack-1", false, false),
            ],
            ConvictionPolicy::default(),
        );
        assert!(plan.convict.is_empty());
        assert_eq!(plan.held_by_safe_mode, vec!["node-a", "node-b"]);
        assert_eq!(plan.rebooted, vec!["node-a", "node-b"]);
        assert_eq!(plan.worst_severity(), DamageSeverity::Critical);
    }

    #[test]
    fn reboot_conviction_can_be_turned_off_while_still_being_reported() {
        let policy = ConvictionPolicy {
            convict_on_reboot: false,
            ..ConvictionPolicy::default()
        };
        let plan = plan_conviction(
            &[
                rebooted_candidate("node-a", "rack-1"),
                candidate("node-b", "rack-1", false, false),
            ],
            policy,
        );
        assert!(plan.convict.is_empty());
        assert!(plan.held_by_safe_mode.is_empty());
        // Still surfaced: an operator needs to see the restart either way.
        assert_eq!(plan.rebooted, vec!["node-a"]);
        assert_eq!(plan.worst_severity(), DamageSeverity::Normal);
    }

    #[test]
    fn a_reboot_is_trusted_even_while_the_detector_is_paused() {
        // The stall guard exists because silence is ambiguous after a detector
        // pause. A changed boot time is not an inference from silence, so it
        // stays actionable.
        let mut detector = MetaFailureDetector::new(options());
        let policy = ConvictionPolicy::default();
        let mut rebooted_server = server("a", "rack-1", MetaEntityState::Normal, 10_000);
        rebooted_server.reboot_detected = true;
        let servers = vec![
            rebooted_server,
            server("b", "rack-1", MetaEntityState::Normal, 10_000),
            server("c", "rack-1", MetaEntityState::Normal, 10_000),
            server("d", "rack-1", MetaEntityState::Normal, 10_000),
        ];
        // First round ever: the detector is paused, so phi convicts nobody.
        let plan = detector.plan_round(&servers, 10_000, policy);
        assert!(plan.damage.iter().all(|entry| entry.total_servers == 4));
        assert_eq!(plan.convict, vec!["a"]);
    }

    #[test]
    fn a_frozen_server_that_rebooted_is_not_reconvicted() {
        let plan = plan_conviction(
            &[
                ConvictionCandidate {
                    abnormal: true,
                    rebooted: true,
                    ..candidate("node-a", "rack-1", true, false)
                },
                candidate("node-b", "rack-1", false, false),
            ],
            ConvictionPolicy::default(),
        );
        assert!(plan.convict.is_empty());
        assert!(plan.rebooted.is_empty());
    }

    #[test]
    fn an_isolated_failure_is_convicted() {
        let plan = plan_conviction(
            &[
                candidate("node-a", "rack-1", false, true),
                candidate("node-b", "rack-1", false, false),
                candidate("node-c", "rack-1", false, false),
                candidate("node-d", "rack-1", false, false),
            ],
            ConvictionPolicy::default(),
        );
        assert_eq!(plan.convict, vec!["node-a"]);
        assert!(plan.held_by_safe_mode.is_empty());
        assert_eq!(plan.worst_severity(), DamageSeverity::Normal);
    }

    #[test]
    fn a_correlated_failure_puts_the_location_into_safe_mode_instead() {
        // Half a rack going silent at once is a rack problem, not four node
        // problems. Freezing all four would drain the topology.
        let plan = plan_conviction(
            &[
                candidate("node-a", "rack-1", false, true),
                candidate("node-b", "rack-1", false, true),
                candidate("node-c", "rack-1", false, false),
                candidate("node-d", "rack-1", false, false),
            ],
            ConvictionPolicy::default(),
        );
        assert!(plan.convict.is_empty());
        assert_eq!(plan.held_by_safe_mode, vec!["node-a", "node-b"]);
        assert_eq!(plan.worst_severity(), DamageSeverity::Critical);
        assert_eq!(plan.damage.len(), 1);
        assert!(plan.damage[0].safe_mode);
        assert_eq!(plan.damage[0].abnormal_servers, 2);
        assert_eq!(plan.damage[0].total_servers, 4);
    }

    #[test]
    fn safe_mode_is_scoped_to_the_damaged_location() {
        // rack-1 is in trouble; rack-2's unrelated single failure still converts.
        let plan = plan_conviction(
            &[
                candidate("a1", "rack-1", false, true),
                candidate("a2", "rack-1", false, true),
                candidate("a3", "rack-1", false, false),
                candidate("b1", "rack-2", false, true),
                candidate("b2", "rack-2", false, false),
                candidate("b3", "rack-2", false, false),
                candidate("b4", "rack-2", false, false),
            ],
            ConvictionPolicy::default(),
        );
        assert_eq!(plan.convict, vec!["b1"]);
        assert_eq!(plan.held_by_safe_mode, vec!["a1", "a2"]);
        let rack1 = plan
            .damage
            .iter()
            .find(|entry| entry.location == "rack-1")
            .expect("rack-1 reported");
        let rack2 = plan
            .damage
            .iter()
            .find(|entry| entry.location == "rack-2")
            .expect("rack-2 reported");
        assert!(rack1.safe_mode);
        assert!(!rack2.safe_mode);
    }

    #[test]
    fn already_frozen_servers_count_as_damage_but_are_not_reconvicted() {
        // The frozen node is what tips the location over, so the newly failed
        // one is held rather than compounding the outage.
        let plan = plan_conviction(
            &[
                candidate("node-a", "rack-1", true, false),
                candidate("node-b", "rack-1", false, true),
                candidate("node-c", "rack-1", false, false),
                candidate("node-d", "rack-1", false, false),
            ],
            ConvictionPolicy::default(),
        );
        assert!(plan.convict.is_empty());
        assert_eq!(plan.held_by_safe_mode, vec!["node-b"]);
        assert_eq!(plan.damage[0].abnormal_servers, 2);
    }

    #[test]
    fn disabling_safe_mode_convicts_every_failure() {
        let policy = ConvictionPolicy {
            safe_mode_enabled: false,
            ..ConvictionPolicy::default()
        };
        let plan = plan_conviction(
            &[
                candidate("node-a", "rack-1", false, true),
                candidate("node-b", "rack-1", false, true),
                candidate("node-c", "rack-1", false, false),
            ],
            policy,
        );
        assert_eq!(plan.convict, vec!["node-a", "node-b"]);
        assert!(plan.held_by_safe_mode.is_empty());
        // The severity is still reported even though it no longer gates.
        assert_eq!(plan.worst_severity(), DamageSeverity::Critical);
    }

    #[test]
    fn disabling_conviction_reports_the_plan_without_naming_anyone_to_freeze() {
        let policy = ConvictionPolicy {
            convict_enabled: false,
            ..ConvictionPolicy::default()
        };
        let plan = plan_conviction(&[candidate("node-a", "rack-1", false, true)], policy);
        assert!(plan.convict.is_empty());
        assert_eq!(plan.held_by_safe_mode, vec!["node-a"]);
    }

    #[test]
    fn plan_round_convicts_the_one_node_that_stopped_beating() {
        let mut detector = MetaFailureDetector::new(options());
        let policy = ConvictionPolicy::default();
        let mut now = 10_000;
        // Four nodes beat together for a while.
        for _ in 0..40 {
            let servers = ["a", "b", "c", "d"]
                .iter()
                .map(|addr| server(addr, "rack-1", MetaEntityState::Normal, now))
                .collect::<Vec<_>>();
            let plan = detector.plan_round(&servers, now, policy);
            assert!(plan.convict.is_empty(), "healthy fleet must not be convicted");
            now += 1_000;
        }
        // Then "a" goes quiet while the others keep beating.
        let dead_since = now - 1_000;
        let mut convicted = Vec::new();
        for _ in 0..20 {
            let servers = vec![
                server("a", "rack-1", MetaEntityState::Normal, dead_since),
                server("b", "rack-1", MetaEntityState::Normal, now),
                server("c", "rack-1", MetaEntityState::Normal, now),
                server("d", "rack-1", MetaEntityState::Normal, now),
            ];
            convicted = detector.plan_round(&servers, now, policy).convict;
            if !convicted.is_empty() {
                break;
            }
            now += 1_000;
        }
        assert_eq!(convicted, vec!["a"]);
    }

    #[test]
    fn plan_round_holds_back_when_the_whole_rack_goes_quiet() {
        let mut detector = MetaFailureDetector::new(options());
        let policy = ConvictionPolicy::default();
        let mut now = 10_000;
        for _ in 0..40 {
            let servers = ["a", "b", "c", "d"]
                .iter()
                .map(|addr| server(addr, "rack-1", MetaEntityState::Normal, now))
                .collect::<Vec<_>>();
            detector.plan_round(&servers, now, policy);
            now += 1_000;
        }
        // The whole rack stops at once. Nothing may be frozen: this is exactly
        // the case where freezing everything turns a rack outage into a total one.
        let dead_since = now - 1_000;
        let mut plan = ConvictionPlan::default();
        for _ in 0..20 {
            let servers = ["a", "b", "c", "d"]
                .iter()
                .map(|addr| server(addr, "rack-1", MetaEntityState::Normal, dead_since))
                .collect::<Vec<_>>();
            plan = detector.plan_round(&servers, now, policy);
            assert!(plan.convict.is_empty(), "a whole-rack outage must not be convicted");
            now += 1_000;
        }
        assert_eq!(plan.held_by_safe_mode, vec!["a", "b", "c", "d"]);
        assert_eq!(plan.worst_severity(), DamageSeverity::Critical);
    }

    #[test]
    fn plan_round_forgets_servers_that_left_the_cluster() {
        let mut detector = MetaFailureDetector::new(options());
        let policy = ConvictionPolicy::default();
        let servers = vec![
            server("a", "rack-1", MetaEntityState::Normal, 10_000),
            server("b", "rack-1", MetaEntityState::Normal, 10_000),
        ];
        detector.plan_round(&servers, 10_000, policy);
        assert_eq!(detector.tracked_servers(), 2);

        let servers = vec![server("a", "rack-1", MetaEntityState::Normal, 11_000)];
        detector.plan_round(&servers, 11_000, policy);
        assert_eq!(detector.tracked_servers(), 1);
        assert!(detector.phi("b", 11_000).is_none());
    }

    #[test]
    fn dropped_servers_are_neither_damage_nor_candidates() {
        let mut detector = MetaFailureDetector::new(options());
        let policy = ConvictionPolicy::default();
        let servers = vec![
            server("a", "rack-1", MetaEntityState::Normal, 10_000),
            server("gone", "rack-1", MetaEntityState::Dropped, 0),
        ];
        detector.begin_round(9_000);
        let plan = detector.plan_round(&servers, 10_000, policy);
        assert_eq!(plan.damage.len(), 1);
        assert_eq!(plan.damage[0].total_servers, 1);
        assert_eq!(plan.damage[0].abnormal_servers, 0);
    }

    #[test]
    fn a_stalled_interval_is_not_folded_into_the_learned_cadence() {
        // A one-off gap longer than max_interval_ms is a stall, not the node's
        // cadence; admitting it would inflate the mean and blind the detector.
        let mut detector = MetaFailureDetector::new(options());
        let last = train(&mut detector, "node-a", 1_000, 40);
        let mean_before = detector.mean_interval_ms("node-a").expect("observed");

        let after_stall = last + options().max_interval_ms + 60_000;
        detector.observe("node-a", after_stall);
        let mean_after = detector.mean_interval_ms("node-a").expect("observed");
        assert!(
            (mean_after - mean_before).abs() < 1.0,
            "outsized gap must not move the mean: before={mean_before} after={mean_after}"
        );
        // The arrival itself still counts, so once the detector has run out its
        // post-stall grace window the node reads as freshly heard from -- judged
        // against the cadence it had before the gap, not against the gap.
        let mut now = after_stall;
        let settled = after_stall + options().max_round_pause_ms + 1_000;
        while now <= settled {
            detector.begin_round(now);
            now += 1_000;
        }
        assert_eq!(detector.diagnose("node-a", settled), Diagnosis::Healthy);
    }
}
