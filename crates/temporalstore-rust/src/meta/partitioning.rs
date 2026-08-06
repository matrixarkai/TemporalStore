//! Table partition/placement construction, extracted from meta.rs.

use super::*;
use std::collections::BTreeSet;

pub(super) fn build_shards(state: &MetaState, table: &TableMetaInfo) -> Vec<TableShard> {
    #[derive(Debug)]
    struct PlacementCandidate {
        server_addr: String,
        location: String,
        degraded: bool,
        queue_depth: usize,
        background_queue_depth: usize,
        running_shard_count: usize,
        dirty_object_count: usize,
        dirty_shard_count: usize,
        shard_state_penalty: u8,
        key_count: u64,
        memory_bytes: u64,
    }

    let mut normal_servers = state
        .servers
        .values()
        .filter(|server| server.state == MetaEntityState::Normal)
        .map(|server| {
            let key_count = server
                .shard_loads
                .iter()
                .map(|load| load.key_count)
                .sum::<u64>();
            let memory_bytes = server
                .shard_loads
                .iter()
                .map(|load| load.memory_bytes)
                .sum::<u64>();
            let shard_state_penalty = server
                .shard_states
                .iter()
                .map(|state| placement_shard_state_penalty(&state.serving_state))
                .max()
                .unwrap_or_default();
            PlacementCandidate {
                server_addr: server.server_addr.clone(),
                location: server.location.clone(),
                degraded: !server.runtime_load.degraded_reasons.is_empty(),
                queue_depth: server.runtime_load.queue_depth,
                background_queue_depth: server.runtime_load.background_queue_depth,
                running_shard_count: server.runtime_load.running_shard_count,
                dirty_object_count: server.runtime_load.dirty_object_count,
                dirty_shard_count: server.runtime_load.dirty_shard_count,
                shard_state_penalty,
                key_count,
                memory_bytes,
            }
        })
        .collect::<Vec<_>>();
    normal_servers.sort_by(|left, right| {
        (
            left.degraded,
            left.shard_state_penalty,
            left.queue_depth,
            left.background_queue_depth,
            left.running_shard_count,
            left.dirty_object_count,
            left.dirty_shard_count,
            left.key_count,
            left.memory_bytes,
            &left.server_addr,
        )
            .cmp(&(
                right.degraded,
                right.shard_state_penalty,
                right.queue_depth,
                right.background_queue_depth,
                right.running_shard_count,
                right.dirty_object_count,
                right.dirty_shard_count,
                right.key_count,
                right.memory_bytes,
                &right.server_addr,
            ))
    });
    let slot_count = 1_u64 << 30;
    let mut shards = Vec::new();
    for offset in 0..table.shard_count {
        let shard_id = table_shard_id(table, offset).unwrap_or(table.first_shard_id + offset);
        let start_slot = slot_count * offset / table.shard_count;
        let end_slot = (slot_count * (offset + 1) / table.shard_count).saturating_sub(1);
        let mut replicas = Vec::new();
        let mut seen_replicas = BTreeSet::new();
        let mut used_locations = BTreeSet::new();
        let mut used_hosts = BTreeSet::new();
        if let Some(location) = state.shards.get(&shard_id) {
            push_replica(
                state,
                &mut replicas,
                &mut seen_replicas,
                &mut used_locations,
                &mut used_hosts,
                &location.server_addr,
            );
        }
        for candidate in &normal_servers {
            if replicas.len() >= table.replica_count as usize {
                break;
            }
            if seen_replicas.contains(&candidate.server_addr) {
                continue;
            }
            if !candidate.location.is_empty() && used_locations.contains(&candidate.location) {
                continue;
            }
            let host = server_host(&candidate.server_addr);
            if !host.is_empty() && used_hosts.contains(&host) {
                continue;
            }
            push_replica(
                state,
                &mut replicas,
                &mut seen_replicas,
                &mut used_locations,
                &mut used_hosts,
                &candidate.server_addr,
            );
        }
        for candidate in &normal_servers {
            if replicas.len() >= table.replica_count as usize {
                break;
            }
            push_replica(
                state,
                &mut replicas,
                &mut seen_replicas,
                &mut used_locations,
                &mut used_hosts,
                &candidate.server_addr,
            );
        }
        let primary = state
            .shards
            .get(&shard_id)
            .map(|location| location.server_addr.clone())
            .or_else(|| replicas.first().cloned());
        let primary_endpoint = primary
            .as_ref()
            .map(|server_addr| server_endpoint(state, server_addr));
        let replica_endpoints = replicas
            .iter()
            .map(|server_addr| server_endpoint(state, server_addr))
            .collect();
        shards.push(TableShard {
            shard_id,
            start_slot,
            end_slot,
            primary,
            replicas,
            primary_endpoint,
            replica_endpoints,
        });
    }
    shards
}


