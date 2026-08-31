// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! SingleNodeMeta reporting methods (conformance/info/stats/preflight/topology_version), extracted from meta.rs.

use super::*;

impl SingleNodeMeta {
    pub fn control_plane_parity_report(&self) -> MetaControlPlaneParityReport {
        let state = self.inner.read().expect("meta lock poisoned");
        let table_topology_ready = !state.tables.is_empty()
            && state
                .tables
                .values()
                .all(|table| table.info.shard_count > 0 && table.info.replica_count > 0);
        let transitional_state_model_ready = state
            .tables
            .values()
            .any(|table| table.info.state != MetaEntityState::Normal)
            || state
                .servers
                .values()
                .any(|server| server.state != MetaEntityState::Normal)
            || state
                .proxies
                .values()
                .any(|proxy| proxy.state != MetaEntityState::Normal)
            || state.topology_events.iter().any(|event| {
                matches!(
                    event.kind.as_str(),
                    "table_state" | "server_state" | "proxy_state"
                )
            });
        let topology_history_ready =
            state.topology_version > 0 && !state.topology_events.is_empty();
        let scheduler_owned_finish_load_ready = !state.scheduler_finish_generations.is_empty();
        let scheduler_generation_check_ready = scheduler_owned_finish_load_ready;
        let durable_replay_ready = self.mutation_log.is_some();
        let real_data_node_coordination_ready = state.servers.values().any(|server| {
            !server.shard_states.is_empty() || !server.runtime_load.degraded_reasons.is_empty()
        });

        let mut blockers = Vec::new();
        if !table_topology_ready {
            blockers.push("table/shard topology evidence missing".to_string());
        }
        if !transitional_state_model_ready {
            blockers.push("transitional table/server/proxy state evidence missing".to_string());
        }
        if !topology_history_ready {
            blockers.push("topology history evidence missing".to_string());
        }
        if !scheduler_owned_finish_load_ready {
            blockers.push("scheduler-owned finish_load token evidence missing".to_string());
        }
        if !durable_replay_ready {
            blockers.push("durable mutation log replay evidence missing".to_string());
        }
        if !real_data_node_coordination_ready {
            blockers.push(
                "real data-node heartbeat/lifecycle coordination evidence missing".to_string(),
            );
        }

        MetaControlPlaneParityReport {
            status: if blockers.is_empty() {
                Status::ok()
            } else {
                Status::error(
                    "metaserver_control_plane_parity_blocked",
                    blockers.join("; "),
                )
            },
            table_topology_ready,
            transitional_state_model_ready,
            topology_history_ready,
            scheduler_owned_finish_load_ready,
            scheduler_generation_check_ready,
            durable_replay_ready,
            real_data_node_coordination_ready,
            scheduler_finish_generation_count: state.scheduler_finish_generations.len(),
            topology_event_count: state.topology_events.len(),
            topology_version: state.topology_version,
            blockers,
        }
    }

    pub fn info(&self) -> MetaInfo {
        let meta_change_muted = self.is_meta_change_muted();
        MetaInfo {
            meta_change_muted,
            status: Status::ok(),
            stats: self.stats(),
            boot_time_ms: self.boot_time_ms,
            durable_mutation_log: self.mutation_log.is_some(),
        }
    }

    pub fn stats(&self) -> MetaStats {
        let state = self.inner.read().expect("meta lock poisoned");
        stats_from_state(&state, &self.counters)
    }

    /// Count the tables, namespaces and proxy groups by state.
    ///
    /// A scrape wants these counts and nothing else about those resources, and
    /// listing them to count them meant cloning every one: 648.5us for 4096
    /// tables and 292.3us for their namespaces, out of a 1093.3us scrape.
    pub fn resource_tallies(&self) -> ResourceTalliesResponse {
        let state = self.inner.read().expect("meta lock poisoned");
        let mut tallies = ResourceTalliesResponse {
            status: Status::ok(),
            tables: StateTally::default(),
            namespaces: StateTally::default(),
            proxy_groups: StateTally::default(),
        };
        for table in state.tables.values() {
            tallies.tables.record(table.info.state);
        }
        for namespace_state in state.namespaces.values() {
            tallies.namespaces.record(*namespace_state);
        }
        for group in state.proxy_groups.values() {
            tallies.proxy_groups.record(group.state);
        }
        tallies
    }

    pub fn preflight_report(&self) -> MetaPreflightReport {
        let state = self.inner.read().expect("meta lock poisoned");
        let normal_servers = state
            .servers
            .values()
            .filter(|server| server.state == MetaEntityState::Normal)
            .count();
        let frozen_servers = state
            .servers
            .values()
            .filter(|server| server.state == MetaEntityState::Frozen)
            .count();
        let normal_proxies = state
            .proxies
            .values()
            .filter(|proxy| proxy.state == MetaEntityState::Normal)
            .count();
        let frozen_proxies = state
            .proxies
            .values()
            .filter(|proxy| proxy.state == MetaEntityState::Frozen)
            .count();
        let dropped_tables = state
            .tables
            .values()
            .filter(|table| table.info.state == MetaEntityState::Dropped)
            .count();
        let mut degraded_reasons = Vec::new();
        if frozen_servers > 0 {
            degraded_reasons.push("frozen_servers".to_string());
        }
        if frozen_proxies > 0 {
            degraded_reasons.push("frozen_proxies".to_string());
        }
        if normal_servers == 0 && !state.shards.is_empty() {
            degraded_reasons.push("no_normal_servers_for_registered_shards".to_string());
        }
        // The same question asked of the routing tier. `frozen_proxies` covers a
        // tier that was frozen; a tier that was dropped instead leaves both
        // counts at zero, so the report came back ok with nothing left to route
        // through. Guarded on the tier existing at all, because a
        // direct-to-datanode deployment has no proxies and is not degraded.
        if normal_proxies == 0 && !state.proxies.is_empty() {
            degraded_reasons.push("no_normal_proxies_for_registered_proxies".to_string());
        }
        let status = if degraded_reasons.is_empty() {
            Status::ok()
        } else {
            Status::error("degraded", degraded_reasons.join(","))
        };
        MetaPreflightReport {
            status,
            stats: stats_from_state(&state, &self.counters),
            normal_servers,
            frozen_servers,
            normal_proxies,
            frozen_proxies,
            dropped_tables,
            shard_routes: state.shards.len(),
            degraded_reasons,
        }
    }

    pub fn topology_version_report(
        &self,
        request: TopologyVersionRequest,
    ) -> TopologyVersionReport {
        let state = self.inner.read().expect("meta lock poisoned");
        topology_version_report_from_state(&state, request.old_topology_version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proxy(addr: &str) -> RegisterProxyRequest {
        RegisterProxyRequest {
            proxy_addr: addr.to_string(),
            namespace: "ns".to_string(),
            location: "rack-1".to_string(),
            config_version: 1,
            binary_version: "v1".to_string(),
        }
    }

    #[test]
    fn a_routing_tier_with_nothing_serving_is_degraded() {
        // frozen_proxies catches a tier that was frozen. A tier that was
        // dropped instead left both counts at zero and the report came back ok
        // with nothing to route through.
        let meta = SingleNodeMeta::default();
        assert!(meta.register_proxy(proxy("p1")).status.ok);
        assert!(meta
            .drop_proxy(StateChangeRequest {
                endpoint: "p1".to_string(),
                reason: FreezeReason::Operator,
                freeze_cooldown_ms: 0,
            })
            .status
            .ok);

        let report = meta.preflight_report();
        assert_eq!(report.normal_proxies, 0);
        assert_eq!(report.frozen_proxies, 0, "dropped, not frozen: {report:?}");
        assert!(
            report
                .degraded_reasons
                .iter()
                .any(|reason| reason == "no_normal_proxies_for_registered_proxies"),
            "a routing tier with nothing serving reported healthy: {report:?}"
        );
        assert!(!report.status.ok);
    }

    #[test]
    fn a_deployment_with_no_proxies_at_all_is_not_degraded() {
        // The guard. A direct-to-datanode deployment has no routing tier by
        // design, and calling that degraded would make the report cry wolf.
        let meta = SingleNodeMeta::default();
        let report = meta.preflight_report();
        assert_eq!(report.normal_proxies, 0);
        assert!(
            !report
                .degraded_reasons
                .iter()
                .any(|reason| reason == "no_normal_proxies_for_registered_proxies"),
            "a deployment that never had proxies was called degraded: {report:?}"
        );
    }

    #[test]
    fn a_serving_proxy_is_not_degraded() {
        let meta = SingleNodeMeta::default();
        assert!(meta.register_proxy(proxy("p1")).status.ok);
        let report = meta.preflight_report();
        assert_eq!(report.normal_proxies, 1);
        assert!(report.degraded_reasons.is_empty(), "{report:?}");
    }
}
