// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! SingleNodeMeta reporting methods (parity/info/stats/preflight/topology_version), extracted from meta.rs.

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
        MetaInfo {
            status: Status::ok(),
            stats: self.stats(),
            boot_time_ms: self.boot_time_ms,
            durable_mutation_log: self.mutation_log.is_some(),
        }
    }

    pub fn stats(&self) -> MetaStats {
        let state = self.inner.read().expect("meta lock poisoned");
        stats_from_state(&state)
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
        let status = if degraded_reasons.is_empty() {
            Status::ok()
        } else {
            Status::error("degraded", degraded_reasons.join(","))
        };
        MetaPreflightReport {
            status,
            stats: stats_from_state(&state),
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
