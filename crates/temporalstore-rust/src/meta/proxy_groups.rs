// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Proxy capacity: named groups, and the calibration that keeps them filled.
//!
//! A proxy declares its own `namespace` when it registers, and that is the whole
//! of the story today. Nothing else about the routing tier is expressible:
//!
//! * **A namespace cannot say how much proxy capacity it wants.** There is no
//!   target to compare against, so "this namespace should have four proxies in
//!   this datacenter" lives in a deployment tool, or in somebody's head.
//! * **There is no spare pool.** A proxy that is running and healthy but not
//!   claimed by anything is indistinguishable from one that is serving, because
//!   the only thing that assigns work is the proxy's own configuration.
//! * **Nothing refills a namespace that loses proxies.** When the failure
//!   detector freezes two of a namespace's four proxies, the namespace runs at
//!   half capacity until a human notices and reconfigures a replacement, even
//!   when idle proxies are sitting in the same location.
//!
//! A [`ProxyGroupInfo`] is the missing declaration: a named pool that wants
//! `instance_num` proxies for one namespace, within one location. Proxies are
//! attached to a group by the metaserver rather than by their own config, so the
//! assignment is something the cluster owns and can repair.
//!
//! [`plan_proxy_calibration`] is the repair. Each round it compares every
//! group's target against what is actually attached and serving, attaches idle
//! proxies to make up a shortfall, and detaches surplus. It is pure and
//! deterministic — groups are visited in name order and every tie breaks on the
//! proxy address — so the same state always produces the same plan.
//!
//! Location is matched **hierarchically**, so a group placed at `us-east/dc1`
//! draws from any proxy beneath it rather than from one exact rack. An empty
//! location means "anywhere".

use std::collections::BTreeMap;

use super::*;

/// A named pool of proxies serving one namespace within one location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyGroupInfo {
    /// Unique name, and the key proxies are attached by.
    pub group: String,
    /// The namespace proxies in this group serve.
    pub namespace: String,
    /// Where the group draws proxies from. Matched hierarchically, so a
    /// datacenter-level placement accepts any rack beneath it. Empty means
    /// anywhere.
    #[serde(default)]
    pub location: String,
    /// How many serving proxies the group wants.
    pub instance_num: u64,
    /// Bumped whenever the served configuration changes, so an attached proxy
    /// can tell from its heartbeat that it must re-read.
    #[serde(default)]
    pub config_version: u64,
    /// What percentage of traffic attached proxies should shed, 0 to 100.
    ///
    /// The proxy has always implemented this -- it reads the figure from its
    /// heartbeat, applies it, and exports it as a metric -- but the metaserver
    /// sent a hard-coded zero in every response, so the lever could only be
    /// pulled by restarting each proxy with different configuration, during
    /// exactly the incident where restarting proxies is least welcome.
    #[serde(default)]
    pub drop_percent: u8,
    pub state: MetaEntityState,
}

/// What an attached proxy should be serving, as resolved from its group.
pub(super) struct ProxyServedConfig {
    pub changed: bool,
    pub namespace: String,
    pub config_version: u64,
    pub drop_percent: u8,
}

/// Create a group, or update the target and placement of an existing one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PutProxyGroupRequest {
    pub group: String,
    pub namespace: String,
    #[serde(default)]
    pub location: String,
    pub instance_num: u64,
    /// Percentage of traffic attached proxies should shed. Omitted means none,
    /// which is what every existing group means.
    #[serde(default)]
    pub drop_percent: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DropProxyGroupRequest {
    pub group: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListProxyGroupsResponse {
    pub status: Status,
    pub groups: Vec<ProxyGroupInfo>,
}

/// One proxy joining or leaving a group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyAttachment {
    pub proxy_addr: String,
    /// The group being joined, or the one being left.
    pub group: String,
}

/// A group that could not be filled this round, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyGroupShortfall {
    pub group: String,
    pub wanted: u64,
    pub attached: u64,
    /// Idle proxies eligible for this group at the time of planning. Zero here
    /// with a shortfall means the cluster has no spare capacity in that
    /// location, which is an operator problem rather than a metaserver one.
    pub available: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyCalibrationOptions {
    /// Most attach/detach decisions applied in one round, so a large
    /// reconfiguration lands gradually rather than moving the whole routing tier
    /// at once.
    pub max_changes_per_round: usize,
}

impl Default for ProxyCalibrationOptions {
    fn default() -> Self {
        Self {
            max_changes_per_round: 16,
        }
    }
}

/// What one calibration round should change.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyCalibrationPlan {
    /// Idle proxies to attach, ordered by proxy address.
    pub attach: Vec<ProxyAttachment>,
    /// Proxies to release back to the idle pool, ordered by proxy address.
    pub detach: Vec<ProxyAttachment>,
    /// Groups still short of their target after this round, ordered by name.
    pub shortfalls: Vec<ProxyGroupShortfall>,
    /// Changes the per-round cap held back.
    pub capped: usize,
}

impl ProxyCalibrationPlan {
    pub fn is_empty(&self) -> bool {
        self.attach.is_empty() && self.detach.is_empty()
    }

    pub fn change_count(&self) -> usize {
        self.attach.len() + self.detach.len()
    }
}

/// What one calibration round actually did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyCalibrationReport {
    pub status: Status,
    pub plan: ProxyCalibrationPlan,
}

/// Is this proxy a candidate for attachment right now?
///
/// It must be serving, unattached, and have been heard from — a proxy that has
/// never heartbeated is a registration, not capacity.
fn is_idle_candidate(proxy: &ProxyMetaInfo) -> bool {
    proxy.state == MetaEntityState::Normal
        && proxy.group.is_empty()
        // Counted heartbeats, not the timestamp. Registration stamps `last_heartbeat_ms`, so
        // testing it against zero asked a question every registered proxy answered the same
        // way -- the rule this line exists to enforce was not in force.
        && proxy.heartbeats_total != 0
}

/// Pure planner: bring every group to its declared capacity.
///
/// Detachments are decided before attachments, so a proxy released by a shrunk
/// or dropped group is not also counted as available to fill another group in
/// the same round — that would attach a proxy the caller has not detached yet.
/// It becomes available on the next round instead, which keeps each round's
/// plan independently applicable.
pub fn plan_proxy_calibration(
    groups: &[ProxyGroupInfo],
    proxies: &[ProxyMetaInfo],
    options: ProxyCalibrationOptions,
) -> ProxyCalibrationPlan {
    let mut plan = ProxyCalibrationPlan::default();
    let live_groups = groups
        .iter()
        .filter(|group| group.state == MetaEntityState::Normal)
        .map(|group| (group.group.as_str(), group))
        .collect::<BTreeMap<_, _>>();

    let mut sorted_proxies = proxies.iter().collect::<Vec<_>>();
    sorted_proxies.sort_by(|left, right| left.proxy_addr.cmp(&right.proxy_addr));

    // 1. Release anything attached to a group that is gone, frozen or dropped.
    //    Its capacity no longer exists, so holding the proxy strands it.
    let mut wanted_detach = Vec::new();
    for proxy in &sorted_proxies {
        if proxy.group.is_empty() {
            continue;
        }
        if !live_groups.contains_key(proxy.group.as_str()) {
            wanted_detach.push(ProxyAttachment {
                proxy_addr: proxy.proxy_addr.clone(),
                group: proxy.group.clone(),
            });
        }
    }

    // Everything below is asked once per group, and none of it changes with
    // the group being asked.
    //
    // Each proxy's location was parsed, and allocated, again for every group in
    // the tier. Its group membership was found by walking every proxy again for
    // every group. Both are properties of the proxy.
    let proxy_locations = sorted_proxies
        .iter()
        .map(|proxy| Location::parse(&proxy.location))
        .collect::<Vec<_>>();
    let mut attached_by_group: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for proxy in &sorted_proxies {
        // A frozen proxy is attached but not serving, so it does not count
        // toward the target -- otherwise a group silently runs short every time
        // the failure detector freezes one of its members.
        if proxy.group.is_empty() || proxy.state != MetaEntityState::Normal {
            continue;
        }
        attached_by_group
            .entry(proxy.group.as_str())
            .or_default()
            .push(proxy.proxy_addr.clone());
    }

    // 2. Per group, compare the target against what is actually serving.
    let mut wanted_attach = Vec::new();
    // Which proxies this round has already promised to a group. Asked once per
    // eligible proxy per group, which is why it is a set and not a scan of the
    // attachments planned so far.
    let mut claimed: BTreeSet<String> = BTreeSet::new();
    for (name, group) in &live_groups {
        let mut attached = attached_by_group
            .get(*name)
            .cloned()
            .unwrap_or_default();
        attached.sort();

        let target = group.instance_num as usize;
        if attached.len() > target {
            // Shed the surplus from the end, so the longest-standing members by
            // address ordering are the ones that stay.
            for addr in attached.iter().skip(target).rev() {
                wanted_detach.push(ProxyAttachment {
                    proxy_addr: addr.clone(),
                    group: (*name).to_string(),
                });
            }
            continue;
        }

        let short = target - attached.len();
        if short == 0 {
            continue;
        }
        let pattern = Location::parse(&group.location);
        let eligible = sorted_proxies
            .iter()
            .enumerate()
            .filter(|(_, proxy)| is_idle_candidate(proxy))
            .filter(|(index, _)| proxy_locations[*index].belongs_to(&pattern))
            .filter(|(_, proxy)| !claimed.contains(proxy.proxy_addr.as_str()))
            .map(|(_, proxy)| proxy.proxy_addr.clone())
            .collect::<Vec<_>>();

        for addr in eligible.iter().take(short) {
            claimed.insert(addr.clone());
            wanted_attach.push(ProxyAttachment {
                proxy_addr: addr.clone(),
                group: (*name).to_string(),
            });
        }
        if eligible.len() < short {
            plan.shortfalls.push(ProxyGroupShortfall {
                group: (*name).to_string(),
                wanted: group.instance_num,
                attached: attached.len() as u64,
                available: eligible.len() as u64,
            });
        }
    }

    wanted_detach.sort_by(|left, right| left.proxy_addr.cmp(&right.proxy_addr));
    wanted_detach.dedup_by(|left, right| left.proxy_addr == right.proxy_addr);
    wanted_attach.sort_by(|left, right| left.proxy_addr.cmp(&right.proxy_addr));

    // 3. Spend the round's budget, detachments first: releasing a stranded proxy
    //    is always safe, while an attachment commits capacity.
    let total = wanted_detach.len() + wanted_attach.len();
    let mut budget = options.max_changes_per_round;
    for item in wanted_detach {
        if budget == 0 {
            break;
        }
        budget -= 1;
        plan.detach.push(item);
    }
    for item in wanted_attach {
        if budget == 0 {
            break;
        }
        budget -= 1;
        plan.attach.push(item);
    }
    plan.capped = total.saturating_sub(plan.change_count());
    plan
}

impl SingleNodeMeta {
    /// Create a proxy group, or update an existing one's target and placement.
    ///
    /// Changing what a group serves bumps `config_version`, which is how an
    /// attached proxy learns from its next heartbeat that it must re-read.
    pub fn put_proxy_group(&self, request: PutProxyGroupRequest) -> AckResponse {
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
        // Through the same judgement the propose path uses, so the two
        // cannot drift apart again.
        if let Some(status) =
            self.admission_refusal(&MetaMutation::PutProxyGroup(request.clone()))
        {
            return AckResponse { status };
        }
        self.record_mutation(MetaMutation::PutProxyGroup(request.clone()));
        self.apply_put_proxy_group(request)
    }

    pub(super) fn apply_put_proxy_group(&self, request: PutProxyGroupRequest) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        // Clamped once, before anything compares or records it. What is stored
        // has always been clamped; comparing the raw request against it meant a
        // group asking for more than 100 never matched itself, so every re-put
        // bumped config_version and told every attached proxy its config had
        // changed. The change history had the same split, reporting a shed rate
        // the group was not using.
        let drop_percent = request.drop_percent.min(100);
        let existing = state.proxy_groups.get(&request.group).cloned();
        let config_version = match &existing {
            // Only a change proxies must act on bumps the version; re-putting an
            // identical group leaves attached proxies undisturbed.
            Some(previous)
                if previous.namespace == request.namespace
                    && previous.location == request.location
                    && previous.drop_percent == drop_percent
                    && previous.state == MetaEntityState::Normal =>
            {
                previous.config_version
            }
            Some(previous) => previous.config_version.saturating_add(1),
            None => 1,
        };
        let info = ProxyGroupInfo {
            drop_percent,
            group: request.group.clone(),
            namespace: request.namespace,
            location: request.location,
            instance_num: request.instance_num,
            config_version,
            state: MetaEntityState::Normal,
        };
        state.proxy_groups.insert(request.group.clone(), info);
        record_topology_event(
            &mut state,
            "put_proxy_group",
            format!("proxy_group:{}", request.group),
            format!(
                "instance_num={},drop_percent={},config_version={config_version}",
                request.instance_num, drop_percent
            ),
        );
        AckResponse {
            status: Status::ok(),
        }
    }

    /// Drop a proxy group. Its proxies are released by the next calibration
    /// round rather than here, so releasing them goes through the same recorded
    /// path every other attachment change does.
    pub fn drop_proxy_group(&self, request: DropProxyGroupRequest) -> AckResponse {
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
        self.record_mutation(MetaMutation::DropProxyGroup(request.clone()));
        self.apply_drop_proxy_group(request)
    }

    pub(super) fn apply_drop_proxy_group(&self, request: DropProxyGroupRequest) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        let Some(group) = state.proxy_groups.get_mut(&request.group) else {
            return AckResponse {
                status: Status::error("proxy_group_not_found", "proxy group not found"),
            };
        };
        if group.state == MetaEntityState::Dropped {
            return AckResponse {
                status: Status::error("proxy_group_not_found", "proxy group is dropped"),
            };
        }
        group.state = MetaEntityState::Dropped;
        record_topology_event(
            &mut state,
            "drop_proxy_group",
            format!("proxy_group:{}", request.group),
            "state=dropped",
        );
        AckResponse {
            status: Status::ok(),
        }
    }

    pub fn list_proxy_groups(&self) -> ListProxyGroupsResponse {
        let state = self.inner.read().expect("meta lock poisoned");
        ListProxyGroupsResponse {
            status: Status::ok(),
            groups: state.proxy_groups.values().cloned().collect(),
        }
    }

    /// Attach a proxy to a group, or release it when `group` is empty.
    pub fn set_proxy_group(&self, request: ProxyAttachment) -> AckResponse {
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
        self.record_mutation(MetaMutation::SetProxyGroup(request.clone()));
        self.apply_set_proxy_group(request)
    }

    pub(super) fn apply_set_proxy_group(&self, request: ProxyAttachment) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        let namespace = state
            .proxy_groups
            .get(&request.group)
            .map(|group| group.namespace.clone());
        let Some(proxy) = state.proxies.get_mut(&request.proxy_addr) else {
            return AckResponse {
                status: Status::error("not_found", "proxy not found"),
            };
        };
        proxy.group = request.group.clone();
        // The group is the authority on what an attached proxy serves; a
        // released proxy keeps serving nothing until something claims it.
        proxy.namespace = if request.group.is_empty() {
            String::new()
        } else {
            namespace.unwrap_or_default()
        };
        record_topology_event(
            &mut state,
            "set_proxy_group",
            format!("proxy:{}", request.proxy_addr),
            format!("group={}", request.group),
        );
        AckResponse {
            status: Status::ok(),
        }
    }

    /// Compute the calibration plan for the current state without applying it.
    pub fn plan_proxy_calibration_now(
        &self,
        options: ProxyCalibrationOptions,
    ) -> ProxyCalibrationPlan {
        let state = self.inner.read().expect("meta lock poisoned");
        let groups = state.proxy_groups.values().cloned().collect::<Vec<_>>();
        let proxies = state.proxies.values().cloned().collect::<Vec<_>>();
        plan_proxy_calibration(&groups, &proxies, options)
    }

    /// Run one calibration round: fill every group to its target from the idle
    /// pool, and release proxies whose group shrank or went away.
    pub fn calibrate_proxy_groups(
        &self,
        options: ProxyCalibrationOptions,
    ) -> ProxyCalibrationReport {
        let plan = self.plan_proxy_calibration_now(options);
        // What the round actually changed, which stops matching the plan as
        // soon as one change is refused: it returns there and leaves the rest
        // of the plan standing.
        let mut applied = ProxyCalibrationPlan::default();
        for item in plan.detach.iter() {
            let response = self.set_proxy_group(ProxyAttachment {
                proxy_addr: item.proxy_addr.clone(),
                group: String::new(),
            });
            if !response.status.ok {
                self.metrics.record_calibration(&plan, &applied);
                return ProxyCalibrationReport {
                    status: response.status,
                    plan,
                };
            }
            applied.detach.push(item.clone());
        }
        for item in plan.attach.iter() {
            let response = self.set_proxy_group(item.clone());
            if !response.status.ok {
                self.metrics.record_calibration(&plan, &applied);
                return ProxyCalibrationReport {
                    status: response.status,
                    plan,
                };
            }
            applied.attach.push(item.clone());
        }
        self.metrics.record_calibration(&plan, &applied);
        ProxyCalibrationReport {
            status: Status::ok(),
            plan,
        }
    }

    /// Background loop that keeps every group at its declared capacity.
    pub fn start_proxy_calibration_loop(
        &self,
        options: ProxyCalibrationOptions,
        interval_ms: u64,
    ) -> thread::JoinHandle<()> {
        let meta = self.clone();
        let interval = Duration::from_millis(interval_ms.max(1));
        thread::spawn(move || loop {
            if !meta.is_meta_change_muted() {
                let _ = meta.calibrate_proxy_groups(options);
            }
            thread::sleep(interval);
        })
    }

    /// What an attached proxy should be told on its next heartbeat, if anything.
    ///
    /// Returns the namespace and config version it should be serving, and
    /// whether that differs from what it reported. A proxy with no group is told
    /// to serve nothing, which is how a released proxy learns it is idle.
    pub(super) fn proxy_group_config(
        state: &MetaState,
        proxy_addr: &str,
        reported_namespace: &str,
        reported_config_version: u64,
    ) -> ProxyServedConfig {
        let attached = state
            .proxies
            .get(proxy_addr)
            .map(|proxy| proxy.group.clone())
            .unwrap_or_default();
        let group = if attached.is_empty() {
            None
        } else {
            state
                .proxy_groups
                .get(&attached)
                .filter(|group| group.state == MetaEntityState::Normal)
        };
        match group {
            Some(group) => {
                let changed = group.namespace != reported_namespace
                    || group.config_version > reported_config_version;
                ProxyServedConfig {
                    changed,
                    namespace: group.namespace.clone(),
                    config_version: group.config_version,
                    drop_percent: group.drop_percent,
                }
            }
            // Unattached: it must stop serving whatever it thinks it serves,
            // and shedding a share of nothing is meaningless.
            None => ProxyServedConfig {
                changed: !reported_namespace.is_empty(),
                namespace: String::new(),
                config_version: 0,
                drop_percent: 0,
            },
        }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn a_group_short_of_proxies_is_reported() {
        // Calibration attaches and detaches through `set_proxy_group`, so those
        // land in the change history. A group left short did not: it existed
        // only in the returned plan, which the loop dropped. That is the number
        // an operator needs -- the shortfall type says as much about itself.
        let meta = SingleNodeMeta::default();
        assert!(meta
            .put_proxy_group(PutProxyGroupRequest {
                group: "g1".to_string(),
                namespace: "ns".to_string(),
                location: "rack-1".to_string(),
                instance_num: 3,
                drop_percent: 0,
            })
            .status
            .ok);
        // One idle proxy for a group that wants three.
        assert!(meta
            .register_proxy(RegisterProxyRequest {
                proxy_addr: "p1".to_string(),
                namespace: "ns".to_string(),
                location: "rack-1".to_string(),
                config_version: 1,
                binary_version: "v1".to_string(),
            })
            .status
            .ok);
        assert!(meta
            .proxy_heartbeat(ProxyHeartbeatRequest {
                proxy_addr: "p1".to_string(),
                namespace: "ns".to_string(),
                config_version: 1,
                binary_version: "v1".to_string(),
                boot_time_ms: 0,
            })
            .status
            .ok);

        let report = meta.calibrate_proxy_groups(ProxyCalibrationOptions::default());
        assert!(report.status.ok, "{report:?}");
        assert_eq!(report.plan.shortfalls.len(), 1, "{:?}", report.plan);

        let exported = meta.subsystem_metrics().prometheus();
        assert!(
            exported.contains("temporalstore_meta_calibration_shortfall_groups 1"),
            "a group short of capacity was not reported:\n{exported}"
        );
        assert!(
            exported.contains("temporalstore_meta_calibration_shortfall_proxies 2"),
            "wanted 3 and attached 1, so two are missing:\n{exported}"
        );
    }

    #[test]
    fn a_group_at_its_target_reports_no_shortfall() {
        // The guard: a healthy round must not look like a capacity problem.
        let meta = SingleNodeMeta::default();
        assert!(meta
            .put_proxy_group(PutProxyGroupRequest {
                group: "g1".to_string(),
                namespace: "ns".to_string(),
                location: "rack-1".to_string(),
                instance_num: 1,
                drop_percent: 0,
            })
            .status
            .ok);
        assert!(meta
            .register_proxy(RegisterProxyRequest {
                proxy_addr: "p1".to_string(),
                namespace: "ns".to_string(),
                location: "rack-1".to_string(),
                config_version: 1,
                binary_version: "v1".to_string(),
            })
            .status
            .ok);
        assert!(meta
            .proxy_heartbeat(ProxyHeartbeatRequest {
                proxy_addr: "p1".to_string(),
                namespace: "ns".to_string(),
                config_version: 1,
                binary_version: "v1".to_string(),
                boot_time_ms: 0,
            })
            .status
            .ok);

        assert!(meta
            .calibrate_proxy_groups(ProxyCalibrationOptions::default())
            .status
            .ok);
        let exported = meta.subsystem_metrics().prometheus();
        assert!(
            exported.contains("temporalstore_meta_calibration_shortfall_groups 0"),
            "a satisfied group was reported as short:\n{exported}"
        );
    }

    use super::*;

    fn group(name: &str, ns: &str, location: &str, want: u64) -> ProxyGroupInfo {
        ProxyGroupInfo {
            drop_percent: 0,
            group: name.to_string(),
            namespace: ns.to_string(),
            location: location.to_string(),
            instance_num: want,
            config_version: 1,
            state: MetaEntityState::Normal,
        }
    }

    fn proxy(addr: &str, location: &str, attached: &str) -> ProxyMetaInfo {
        ProxyMetaInfo {
            registered_at_ms: 0,
            // These helpers build proxies that HAVE reported in; the tests that want a silent
            // one set this back to zero themselves.
            heartbeats_total: 1,
            proxy_addr: addr.to_string(),
            namespace: String::new(),
            group: attached.to_string(),
            location: location.to_string(),
            state: MetaEntityState::Normal,
            config_version: 1,
            last_heartbeat_ms: 1_000,
            frozen_since_ms: 0,
            freeze_cooldown_until_ms: 0,
            freeze_reason: FreezeReason::Unspecified,
            binary_version: "v1".to_string(),
            boot_time_ms: 1,
            restart_count: 0,
        }
    }

    fn attached(plan: &ProxyCalibrationPlan) -> Vec<(&str, &str)> {
        plan.attach
            .iter()
            .map(|a| (a.proxy_addr.as_str(), a.group.as_str()))
            .collect()
    }

    fn released(plan: &ProxyCalibrationPlan) -> Vec<&str> {
        plan.detach.iter().map(|a| a.proxy_addr.as_str()).collect()
    }

    #[test]
    fn a_group_below_target_draws_from_the_idle_pool() {
        let plan = plan_proxy_calibration(
            &[group("orders", "ns", "", 2)],
            &[
                proxy("p1", "rack-1", ""),
                proxy("p2", "rack-1", ""),
                proxy("p3", "rack-1", ""),
            ],
            ProxyCalibrationOptions::default(),
        );
        assert_eq!(attached(&plan), vec![("p1", "orders"), ("p2", "orders")]);
        assert!(plan.detach.is_empty());
        assert!(plan.shortfalls.is_empty());
    }

    #[test]
    fn a_group_at_target_is_left_alone() {
        let plan = plan_proxy_calibration(
            &[group("orders", "ns", "", 2)],
            &[
                proxy("p1", "rack-1", "orders"),
                proxy("p2", "rack-1", "orders"),
                proxy("p3", "rack-1", ""),
            ],
            ProxyCalibrationOptions::default(),
        );
        assert!(plan.is_empty());
    }

    #[test]
    fn shrinking_a_group_releases_the_surplus() {
        let plan = plan_proxy_calibration(
            &[group("orders", "ns", "", 1)],
            &[
                proxy("p1", "rack-1", "orders"),
                proxy("p2", "rack-1", "orders"),
                proxy("p3", "rack-1", "orders"),
            ],
            ProxyCalibrationOptions::default(),
        );
        assert_eq!(released(&plan), vec!["p2", "p3"]);
        assert!(plan.attach.is_empty());
    }

    #[test]
    fn a_frozen_member_does_not_count_toward_the_target() {
        // The case the whole thing exists for: the detector freezes a member and
        // the namespace quietly runs under capacity until somebody notices.
        let mut frozen = proxy("p1", "rack-1", "orders");
        frozen.state = MetaEntityState::Frozen;
        let plan = plan_proxy_calibration(
            &[group("orders", "ns", "", 2)],
            &[
                frozen,
                proxy("p2", "rack-1", "orders"),
                proxy("p3", "rack-1", ""),
            ],
            ProxyCalibrationOptions::default(),
        );
        assert_eq!(attached(&plan), vec![("p3", "orders")]);
    }

    #[test]
    fn a_group_only_draws_from_its_own_location() {
        // Matched hierarchically, so a datacenter-level placement accepts any
        // rack beneath it and nothing outside it.
        let plan = plan_proxy_calibration(
            &[group("orders", "ns", "us-east/dc1", 2)],
            &[
                proxy("a1", "us-east/dc1/az1/rack1", ""),
                proxy("a2", "us-east/dc1/az2/rack9", ""),
                proxy("b1", "us-east/dc2/az1/rack1", ""),
            ],
            ProxyCalibrationOptions::default(),
        );
        assert_eq!(attached(&plan), vec![("a1", "orders"), ("a2", "orders")]);
    }

    #[test]
    fn a_group_with_no_eligible_proxies_reports_a_shortfall() {
        // Nothing the metaserver can do about it, so say so rather than
        // silently running under capacity.
        let plan = plan_proxy_calibration(
            &[group("orders", "ns", "us-east/dc1", 2)],
            &[proxy("b1", "us-east/dc2/az1/rack1", "")],
            ProxyCalibrationOptions::default(),
        );
        assert!(plan.attach.is_empty());
        assert_eq!(plan.shortfalls.len(), 1);
        assert_eq!(plan.shortfalls[0].group, "orders");
        assert_eq!(plan.shortfalls[0].wanted, 2);
        assert_eq!(plan.shortfalls[0].attached, 0);
        assert_eq!(plan.shortfalls[0].available, 0);
    }

    #[test]
    fn a_dropped_group_releases_everything_it_held() {
        let mut dropped = group("orders", "ns", "", 2);
        dropped.state = MetaEntityState::Dropped;
        let plan = plan_proxy_calibration(
            &[dropped],
            &[
                proxy("p1", "rack-1", "orders"),
                proxy("p2", "rack-1", "orders"),
            ],
            ProxyCalibrationOptions::default(),
        );
        assert_eq!(released(&plan), vec!["p1", "p2"]);
    }

    #[test]
    fn two_groups_never_claim_the_same_proxy() {
        let plan = plan_proxy_calibration(
            &[group("alpha", "ns", "", 2), group("beta", "ns", "", 2)],
            &[proxy("p1", "rack-1", ""), proxy("p2", "rack-1", "")],
            ProxyCalibrationOptions::default(),
        );
        let claimed = attached(&plan);
        assert_eq!(claimed.len(), 2);
        // alpha sorts first, so it fills before beta gets a look in.
        assert_eq!(claimed, vec![("p1", "alpha"), ("p2", "alpha")]);
        assert_eq!(plan.shortfalls.len(), 1);
        assert_eq!(plan.shortfalls[0].group, "beta");
    }

    #[test]
    fn a_proxy_released_this_round_is_not_also_reattached() {
        // Detach and attach in one round would hand out a proxy the caller has
        // not released yet; it becomes available next round instead.
        let mut dropped = group("old", "ns", "", 1);
        dropped.state = MetaEntityState::Dropped;
        let plan = plan_proxy_calibration(
            &[dropped, group("new", "ns", "", 1)],
            &[proxy("p1", "rack-1", "old")],
            ProxyCalibrationOptions::default(),
        );
        assert_eq!(released(&plan), vec!["p1"]);
        assert!(plan.attach.is_empty());
        assert_eq!(plan.shortfalls[0].group, "new");
    }

    #[test]
    fn a_proxy_that_has_never_heartbeated_is_not_capacity() {
        // The shape a registration actually has: a heartbeat TIMESTAMP, because registration
        // stamps one, and no heartbeats counted. This test used to zero the timestamp instead,
        // which no registered proxy ever does -- so it asserted the right rule against a state
        // production cannot reach, and the guard it was checking passed everything through.
        let mut silent = proxy("p1", "rack-1", "");
        silent.heartbeats_total = 0;
        let plan = plan_proxy_calibration(
            &[group("orders", "ns", "", 1)],
            &[silent],
            ProxyCalibrationOptions::default(),
        );
        assert!(plan.attach.is_empty());
        assert_eq!(plan.shortfalls[0].available, 0);
    }

    #[test]
    fn the_per_round_cap_bounds_how_much_the_routing_tier_moves() {
        let proxies = (0..10)
            .map(|i| proxy(&format!("p{i}"), "rack-1", ""))
            .collect::<Vec<_>>();
        let plan = plan_proxy_calibration(
            &[group("orders", "ns", "", 10)],
            &proxies,
            ProxyCalibrationOptions {
                max_changes_per_round: 3,
            },
        );
        assert_eq!(plan.change_count(), 3);
        assert_eq!(plan.capped, 7);
    }

    #[test]
    fn calibration_attaches_and_the_heartbeat_carries_the_namespace() {
        // End to end: declare capacity, register an idle proxy, calibrate, and
        // the proxy learns what it serves from its next heartbeat.
        let meta = SingleNodeMeta::default();
        assert!(meta
            .register_proxy(RegisterProxyRequest {
                registered_at_ms: 0,
                proxy_addr: "p1".to_string(),
                namespace: String::new(),
                location: "us-east/dc1/az1".to_string(),
                config_version: 0,
                binary_version: "v1".to_string(),
            })
            .status
            .ok);
        assert!(meta
            .proxy_heartbeat(ProxyHeartbeatRequest {
                proxy_addr: "p1".to_string(),
                namespace: String::new(),
                config_version: 0,
                binary_version: "v1".to_string(),
                boot_time_ms: 1,
            })
            .status
            .ok);
        assert!(meta
            .put_proxy_group(PutProxyGroupRequest {
                drop_percent: 0,
                group: "orders".to_string(),
                namespace: "ns".to_string(),
                location: "us-east/dc1".to_string(),
                instance_num: 1,
            })
            .status
            .ok);

        let report = meta.calibrate_proxy_groups(ProxyCalibrationOptions::default());
        assert!(report.status.ok);
        assert_eq!(attached(&report.plan), vec![("p1", "orders")]);

        // The proxy still thinks it serves nothing; the heartbeat tells it.
        let beat = meta.proxy_heartbeat(ProxyHeartbeatRequest {
            proxy_addr: "p1".to_string(),
            namespace: String::new(),
            config_version: 0,
            binary_version: "v1".to_string(),
            boot_time_ms: 1,
        });
        assert!(beat.status.ok);
        assert!(beat.config_changed);
        assert_eq!(beat.namespace, "ns");

        // Once it agrees, nothing more is signalled.
        let settled = meta.proxy_heartbeat(ProxyHeartbeatRequest {
            proxy_addr: "p1".to_string(),
            namespace: "ns".to_string(),
            config_version: beat.config_version,
            binary_version: "v1".to_string(),
            boot_time_ms: 1,
        });
        assert!(!settled.config_changed);
    }

    /// Build a namespace with one proxy group serving it and one proxy attached.
    fn serving_namespace() -> SingleNodeMeta {
        let meta = SingleNodeMeta::default();
        assert!(meta
            .add_namespace(AddNamespaceRequest {
                namespace: "ns".to_string()
            })
            .status
            .ok);
        assert!(meta
            .register_proxy(RegisterProxyRequest {
                proxy_addr: "p1".to_string(),
                namespace: String::new(),
                location: "us-east/dc1/az1".to_string(),
                config_version: 0,
                binary_version: "v1".to_string(),
            })
            .status
            .ok);
        assert!(meta
            .proxy_heartbeat(ProxyHeartbeatRequest {
                proxy_addr: "p1".to_string(),
                namespace: String::new(),
                config_version: 0,
                binary_version: "v1".to_string(),
                boot_time_ms: 1,
            })
            .status
            .ok);
        assert!(meta
            .put_proxy_group(PutProxyGroupRequest {
                group: "orders".to_string(),
                namespace: "ns".to_string(),
                location: "us-east/dc1".to_string(),
                instance_num: 1,
                drop_percent: 0,
            })
            .status
            .ok);
        let first = meta.calibrate_proxy_groups(ProxyCalibrationOptions::default());
        assert_eq!(attached(&first.plan), vec![("p1", "orders")]);
        meta
    }

    #[test]
    fn a_namespace_a_proxy_group_still_routes_to_is_not_dropped() {
        // Dropping the namespace under a serving group used to succeed. The
        // group stayed Normal, its proxy stayed attached, and the very next
        // heartbeat still handed that proxy the dropped namespace to serve --
        // with nothing anywhere reporting the contradiction. A live table
        // already blocks the drop; routing is the other live dependent.
        let meta = serving_namespace();

        let refused = meta.drop_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        assert_eq!(
            refused.status.code, "namespace_still_routed",
            "a namespace with a proxy group routing to it was dropped"
        );

        // And nothing moved: refusing must leave the tier exactly as it was.
        let group = meta
            .list_proxy_groups()
            .groups
            .into_iter()
            .find(|g| g.group == "orders")
            .expect("group missing");
        assert_eq!(group.state, MetaEntityState::Normal);
        let beat = meta.proxy_heartbeat(ProxyHeartbeatRequest {
            proxy_addr: "p1".to_string(),
            namespace: String::new(),
            config_version: 0,
            binary_version: "v1".to_string(),
            boot_time_ms: 1,
        });
        assert_eq!(beat.namespace, "ns");
    }

    #[test]
    fn dropping_the_group_first_lets_the_namespace_go() {
        // The guard must let go, or it would have bought consistency by making
        // a routed namespace undroppable forever. A dropped group is not a live
        // dependent, so the namespace goes once the group does.
        let meta = serving_namespace();
        assert!(meta
            .drop_proxy_group(DropProxyGroupRequest {
                group: "orders".to_string()
            })
            .status
            .ok);
        assert!(
            meta.drop_namespace(AddNamespaceRequest {
                namespace: "ns".to_string()
            })
            .status
            .ok,
            "namespace stayed undroppable after its group was dropped"
        );
    }

    #[test]
    fn routing_does_not_block_freezing_a_namespace() {
        // The guard is for Dropped only. Freeze and unfreeze share this path
        // and must keep working while a group routes to the namespace.
        let meta = serving_namespace();
        assert!(meta
            .freeze_namespace(AddNamespaceRequest {
                namespace: "ns".to_string()
            })
            .status
            .ok);
        assert!(meta
            .unfreeze_namespace(AddNamespaceRequest {
                namespace: "ns".to_string()
            })
            .status
            .ok);
    }

    #[test]
    fn a_round_that_stops_early_does_not_count_attachments_it_never_made() {
        // Two things this pins down.
        //
        // The counts came off the plan, before a single `set_proxy_group` had
        // run -- and calibration returns on the first one that is refused, so
        // the round could report attaching proxies it never touched.
        //
        // They also went onto `shards_reassigned_total`, which is documented as
        // "Shard ownership changes, by cause". A proxy joining a group is not a
        // shard moving, and folding it in there inflates the number an operator
        // watches for rebalance churn.
        let meta = SingleNodeMeta::default();
        assert!(meta
            .register_proxy(RegisterProxyRequest {
                proxy_addr: "p1".to_string(),
                namespace: String::new(),
                location: "rack-1".to_string(),
                config_version: 0,
                binary_version: "v1".to_string(),
            })
            .status
            .ok);
        assert!(meta
            .proxy_heartbeat(ProxyHeartbeatRequest {
                proxy_addr: "p1".to_string(),
                namespace: String::new(),
                config_version: 0,
                binary_version: "v1".to_string(),
                boot_time_ms: 1,
            })
            .status
            .ok);
        assert!(meta
            .put_proxy_group(PutProxyGroupRequest {
                group: "orders".to_string(),
                namespace: "ns".to_string(),
                location: "rack-1".to_string(),
                instance_num: 1,
                drop_percent: 0,
            })
            .status
            .ok);

        // The round would attach p1 -- but every change it attempts is refused.
        assert!(meta.set_meta_change_muted(true).status.ok);
        let report = meta.calibrate_proxy_groups(ProxyCalibrationOptions::default());
        assert!(
            !report.status.ok,
            "the round should have been refused: {report:?}"
        );
        assert_eq!(attached(&report.plan), vec![("p1", "orders")], "it planned one");

        let exported = meta.subsystem_metrics().prometheus();
        assert!(
            exported.contains("temporalstore_meta_proxy_attachments_total{kind=\"attach\"} 0"),
            "counted an attachment the round never made:\n{exported}"
        );
        assert!(
            !exported.contains("temporalstore_meta_shards_reassigned_total{reason=\"proxy_attach\"}"),
            "a proxy attaching is still counted as a shard moving:\n{exported}"
        );
        // The round, its shortfall and its cap are planning facts and stay.
        assert!(
            exported.contains(
                "temporalstore_meta_detector_rounds_total{subsystem=\"proxy_calibration\"} 1"
            ),
            "{exported}"
        );
    }

    #[test]
    fn a_round_that_finishes_counts_every_attachment() {
        // The other side of it: when the round runs to the end the count is the
        // whole plan, so the fix cannot have simply stopped counting.
        let meta = SingleNodeMeta::default();
        for addr in ["p1", "p2"] {
            assert!(meta
                .register_proxy(RegisterProxyRequest {
                    proxy_addr: addr.to_string(),
                    namespace: String::new(),
                    location: "rack-1".to_string(),
                    config_version: 0,
                    binary_version: "v1".to_string(),
                })
                .status
                .ok);
            assert!(meta
                .proxy_heartbeat(ProxyHeartbeatRequest {
                    proxy_addr: addr.to_string(),
                    namespace: String::new(),
                    config_version: 0,
                    binary_version: "v1".to_string(),
                    boot_time_ms: 1,
                })
                .status
                .ok);
        }
        assert!(meta
            .put_proxy_group(PutProxyGroupRequest {
                group: "orders".to_string(),
                namespace: "ns".to_string(),
                location: "rack-1".to_string(),
                instance_num: 2,
                drop_percent: 0,
            })
            .status
            .ok);

        let report = meta.calibrate_proxy_groups(ProxyCalibrationOptions::default());
        assert!(report.status.ok);
        assert_eq!(report.plan.attach.len(), 2);

        let exported = meta.subsystem_metrics().prometheus();
        assert!(
            exported.contains("temporalstore_meta_proxy_attachments_total{kind=\"attach\"} 2"),
            "a completed round undercounted:\n{exported}"
        );
    }

    #[test]
    fn dropping_a_group_releases_its_proxy_back_to_idle() {
        let meta = SingleNodeMeta::default();
        meta.register_proxy(RegisterProxyRequest {
            registered_at_ms: 0,
            proxy_addr: "p1".to_string(),
            namespace: String::new(),
            location: "rack-1".to_string(),
            config_version: 0,
            binary_version: "v1".to_string(),
        });
        meta.proxy_heartbeat(ProxyHeartbeatRequest {
            proxy_addr: "p1".to_string(),
            namespace: String::new(),
            config_version: 0,
            binary_version: "v1".to_string(),
            boot_time_ms: 1,
        });
        meta.put_proxy_group(PutProxyGroupRequest {
            drop_percent: 0,
            group: "orders".to_string(),
            namespace: "ns".to_string(),
            location: String::new(),
            instance_num: 1,
        });
        meta.calibrate_proxy_groups(ProxyCalibrationOptions::default());

        assert!(meta
            .drop_proxy_group(DropProxyGroupRequest {
                group: "orders".to_string(),
            })
            .status
            .ok);
        let report = meta.calibrate_proxy_groups(ProxyCalibrationOptions::default());
        assert_eq!(released(&report.plan), vec!["p1"]);

        let idle = meta
            .list_proxies()
            .proxies
            .into_iter()
            .find(|p| p.proxy_addr == "p1")
            .expect("registered");
        assert!(idle.group.is_empty());
        assert!(idle.namespace.is_empty());
    }

    #[test]
    fn groups_survive_a_snapshot_round_trip_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("proxy-group-mutations.jsonl");
        {
            let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
            assert!(meta
                .put_proxy_group(PutProxyGroupRequest {
                    drop_percent: 0,
                    group: "orders".to_string(),
                    namespace: "ns".to_string(),
                    location: "us-east/dc1".to_string(),
                    instance_num: 3,
                })
                .status
                .ok);
            let snapshot = meta.export_snapshot();
            assert_eq!(snapshot.proxy_groups.len(), 1);
            let restored = SingleNodeMeta::default();
            assert!(restored.install_snapshot(snapshot).status.ok);
            assert_eq!(restored.list_proxy_groups().groups[0].instance_num, 3);
        }
        let recovered = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        let groups = recovered.list_proxy_groups().groups;
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].namespace, "ns");
        assert_eq!(groups[0].instance_num, 3);
    }

    #[test]
    fn a_clamped_drop_percent_still_counts_as_unchanged() {
        // The stored value is clamped to 100; the comparison that decides
        // "nothing changed" was made against the raw request. So a group asking
        // for more than 100 never matched itself: every re-put bumped
        // config_version, and every attached proxy saw config_changed on its
        // next heartbeat, for as long as the group was re-applied.
        let meta = SingleNodeMeta::default();
        let put = || {
            meta.put_proxy_group(PutProxyGroupRequest {
                group: "orders".to_string(),
                namespace: "ns".to_string(),
                location: "rack-1".to_string(),
                instance_num: 1,
                drop_percent: 250,
            })
        };
        assert!(put().status.ok);
        let first = meta.list_proxy_groups().groups[0].clone();
        assert_eq!(first.drop_percent, 100, "the stored value is clamped");

        assert!(put().status.ok);
        let second = meta.list_proxy_groups().groups[0].clone();
        assert_eq!(
            second.config_version, first.config_version,
            "re-putting the same group moved config_version, so every attached \
             proxy is told its config changed"
        );

        // And a real change still bumps it, so this does not just freeze the version.
        assert!(meta
            .put_proxy_group(PutProxyGroupRequest {
                group: "orders".to_string(),
                namespace: "ns".to_string(),
                location: "rack-1".to_string(),
                instance_num: 1,
                drop_percent: 10,
            })
            .status
            .ok);
        let third = meta.list_proxy_groups().groups[0].clone();
        assert!(
            third.config_version > second.config_version,
            "a genuine change stopped bumping config_version"
        );
        assert_eq!(third.drop_percent, 10);
    }

    #[test]
    fn the_change_history_records_the_drop_percent_that_took_effect() {
        // The event carried the requested value while the state carried the
        // clamped one, so the history said 250 for a group shedding 100.
        let meta = SingleNodeMeta::default();
        assert!(meta
            .put_proxy_group(PutProxyGroupRequest {
                group: "orders".to_string(),
                namespace: "ns".to_string(),
                location: "rack-1".to_string(),
                instance_num: 1,
                drop_percent: 250,
            })
            .status
            .ok);
        let events = meta.topology_events(TopologyEventsRequest::default());
        let put = events
            .events
            .iter()
            .find(|event| event.kind == "put_proxy_group")
            .expect("no put_proxy_group event");
        assert!(
            put.detail.contains("drop_percent=100"),
            "the history reports a value the group is not using: {:?}",
            put.detail
        );
    }

    #[test]
    fn re_putting_an_identical_group_does_not_disturb_its_proxies() {
        // A config reconciler running this on a loop must not bump the version
        // every pass and make every attached proxy re-read.
        let meta = SingleNodeMeta::default();
        let put = || {
            meta.put_proxy_group(PutProxyGroupRequest {
                drop_percent: 0,
                group: "orders".to_string(),
                namespace: "ns".to_string(),
                location: "rack-1".to_string(),
                instance_num: 2,
            })
        };
        assert!(put().status.ok);
        let first = meta.list_proxy_groups().groups[0].config_version;
        assert!(put().status.ok);
        assert_eq!(meta.list_proxy_groups().groups[0].config_version, first);

        // Changing what it serves does bump it.
        assert!(meta
            .put_proxy_group(PutProxyGroupRequest {
                drop_percent: 0,
                group: "orders".to_string(),
                namespace: "other".to_string(),
                location: "rack-1".to_string(),
                instance_num: 2,
            })
            .status
            .ok);
        assert!(meta.list_proxy_groups().groups[0].config_version > first);
    }
}
