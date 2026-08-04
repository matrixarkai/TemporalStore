//! Topology / placement / stats helpers, extracted from meta.rs.

use super::*;
use std::collections::BTreeSet;
use crate::types::{ShardId, Status};
use crate::partition_id::{PartitionId, PartitionIdError};

pub(super) fn table_shard_id(
    table: &TableMetaInfo,
    offset: u64,
) -> Result<ShardId, crate::partition_id::PartitionIdError> {
    if !table.use_cpp_partition_ids {
        return Ok(table.first_shard_id + offset);
    }
    PartitionId::new(table.table_id, offset, 0, table.partition_version as u64).map(PartitionId::id)
}

pub(super) fn table_for_shard<'a>(state: &'a MetaState, shard_id: ShardId) -> Option<&'a TableRecord> {
    state.tables.values().find(|table| {
        (0..table.info.shard_count).any(|offset| {
            table_shard_id(&table.info, offset)
                .map(|candidate| candidate == shard_id)
                .unwrap_or(false)
        })
    })
}

pub(super) fn push_replica(
    state: &MetaState,
    replicas: &mut Vec<String>,
    seen_replicas: &mut BTreeSet<String>,
    used_locations: &mut BTreeSet<String>,
    used_hosts: &mut BTreeSet<String>,
    server_addr: &str,
) {
    if !seen_replicas.insert(server_addr.to_string()) {
        return;
    }
    if let Some(server) = state.servers.get(server_addr) {
        if !server.location.is_empty() {
            used_locations.insert(server.location.clone());
        }
    }
    let host = server_host(server_addr);
    if !host.is_empty() {
        used_hosts.insert(host);
    }
    replicas.push(server_addr.to_string());
}

pub(super) fn placement_shard_state_penalty(serving_state: &str) -> u8 {
    match serving_state {
        "serving" | "readonly" => 0,
        "queued" | "running" | "loading" => 1,
        "freezing" | "unloading" => 2,
        "failed" => 3,
        _ => 1,
    }
}

pub(super) fn server_host(server_addr: &str) -> String {
    if let Some(stripped) = server_addr.strip_prefix('[') {
        return stripped
            .split_once(']')
            .map(|(host, _)| host.to_string())
            .unwrap_or_else(|| server_addr.to_string());
    }
    server_addr
        .rsplit_once(':')
        .map(|(host, port)| {
            if port.chars().all(|ch| ch.is_ascii_digit()) {
                host.to_string()
            } else {
                server_addr.to_string()
            }
        })
        .unwrap_or_else(|| server_addr.to_string())
}

pub(super) fn server_endpoint(state: &MetaState, server_addr: &str) -> ServerEndpoint {
    ServerEndpoint {
        server_addr: server_addr.to_string(),
        location: state
            .servers
            .get(server_addr)
            .map(|server| server.location.clone())
            .unwrap_or_default(),
    }
}

pub(super) fn ensure_server(state: &mut MetaState, server_addr: &str) {
    state
        .servers
        .entry(server_addr.to_string())
        .or_insert_with(|| ServerMetaInfo {
            server_addr: server_addr.to_string(),
            node_id: 0,
            location: String::new(),
            state: MetaEntityState::Normal,
            last_heartbeat_ms: now_ms(),
            frozen_since_ms: 0,
            freeze_cooldown_until_ms: 0,
            boot_time_ms: 0,
            binary_version: String::new(),
            shard_loads: Vec::new(),
            partition_loads: Vec::new(),
            runtime_load: ServerRuntimeLoad::default(),
            shard_states: Vec::new(),
        });
}

pub(super) fn stats_from_state(state: &MetaState) -> MetaStats {
    MetaStats {
        register_shard_total: state.counters.register_shard_total,
        get_shard_total: state.counters.get_shard_total,
        server_register_total: state.counters.server_register_total,
        server_heartbeat_total: state.counters.server_heartbeat_total,
        proxy_register_total: state.counters.proxy_register_total,
        proxy_heartbeat_total: state.counters.proxy_heartbeat_total,
        namespace_create_total: state.counters.namespace_create_total,
        table_create_total: state.counters.table_create_total,
        topology_query_total: state.counters.topology_query_total,
        load_finish_total: state.counters.load_finish_total,
        topology_version: state.topology_version,
        server_count: state.servers.len(),
        proxy_count: state.proxies.len(),
        namespace_count: state.namespaces.len(),
        table_count: state.tables.len(),
        shard_count: state.shards.len(),
    }
}

const TOPOLOGY_EVENT_HISTORY_LIMIT: usize = 256;

pub(super) fn topology_version_report_from_state(
    state: &MetaState,
    old_topology_version: u64,
) -> TopologyVersionReport {
    let changed_tables = state
        .tables
        .values()
        .filter(|table| table.info.topology_version > old_topology_version)
        .map(|table| table.info.clone())
        .collect::<Vec<_>>();
    let events = state
        .topology_events
        .iter()
        .filter(|event| event.topology_version > old_topology_version)
        .cloned()
        .collect::<Vec<_>>();
    let event_history_truncated = old_topology_version < state.topology_version
        && state
            .topology_events
            .front()
            .is_some_and(|event| old_topology_version < event.topology_version.saturating_sub(1));
    TopologyVersionReport {
        status: Status::ok(),
        current_topology_version: state.topology_version,
        old_topology_version,
        unchanged: old_topology_version >= state.topology_version,
        server_count: state.servers.len(),
        proxy_count: state.proxies.len(),
        table_count: state.tables.len(),
        shard_route_count: state.shards.len(),
        normal_servers: state
            .servers
            .values()
            .filter(|server| server.state == MetaEntityState::Normal)
            .count(),
        frozen_servers: state
            .servers
            .values()
            .filter(|server| server.state == MetaEntityState::Frozen)
            .count(),
        dropped_servers: state
            .servers
            .values()
            .filter(|server| server.state == MetaEntityState::Dropped)
            .count(),
        normal_proxies: state
            .proxies
            .values()
            .filter(|proxy| proxy.state == MetaEntityState::Normal)
            .count(),
        frozen_proxies: state
            .proxies
            .values()
            .filter(|proxy| proxy.state == MetaEntityState::Frozen)
            .count(),
        dropped_proxies: state
            .proxies
            .values()
            .filter(|proxy| proxy.state == MetaEntityState::Dropped)
            .count(),
        normal_tables: state
            .tables
            .values()
            .filter(|table| table.info.state == MetaEntityState::Normal)
            .count(),
        frozen_tables: state
            .tables
            .values()
            .filter(|table| table.info.state == MetaEntityState::Frozen)
            .count(),
        dropped_tables: state
            .tables
            .values()
            .filter(|table| table.info.state == MetaEntityState::Dropped)
            .count(),
        changed_tables,
        events,
        event_history_truncated,
    }
}

pub(super) fn record_topology_event(
    state: &mut MetaState,
    kind: impl Into<String>,
    resource: impl Into<String>,
    detail: impl Into<String>,
) -> u64 {
    state.topology_version += 1;
    state.topology_events.push_back(TopologyChangeEvent {
        topology_version: state.topology_version,
        timestamp_ms: now_ms(),
        kind: kind.into(),
        resource: resource.into(),
        detail: detail.into(),
    });
    while state.topology_events.len() > TOPOLOGY_EVENT_HISTORY_LIMIT {
        state.topology_events.pop_front();
    }
    state.topology_version
}

pub(super) fn counters_from_stats(stats: &MetaStats) -> MetaCounters {
    MetaCounters {
        register_shard_total: stats.register_shard_total,
        get_shard_total: stats.get_shard_total,
        server_register_total: stats.server_register_total,
        server_heartbeat_total: stats.server_heartbeat_total,
        proxy_register_total: stats.proxy_register_total,
        proxy_heartbeat_total: stats.proxy_heartbeat_total,
        namespace_create_total: stats.namespace_create_total,
        table_create_total: stats.table_create_total,
        topology_query_total: stats.topology_query_total,
        load_finish_total: stats.load_finish_total,
    }
}
