// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Table partition/placement construction, extracted from meta.rs.

use super::*;
use std::collections::BTreeSet;

/// Whether the server a shard is registered to is in service.
///
/// An unknown address counts as serving: a route can outlive the server record
/// it names, and treating that as out of service would silently unroute shards
/// the metaserver simply has not heard about yet.
fn owner_is_serving(state: &MetaState, server_addr: &str) -> bool {
    state
        .servers
        .get(server_addr)
        .map(|server| server.state == MetaEntityState::Normal)
        .unwrap_or(true)
}

pub(super) fn build_shards(
    state: &MetaState,
    table: &TableMetaInfo,
    client_location: &str,
) -> Vec<TableShard> {
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
    // Every live server's parsed location, used to size the separation ladder.
    let candidate_locations = normal_servers
        .iter()
        .map(|candidate| Location::parse(&candidate.location))
        .collect::<Vec<_>>();
    let caller = Location::parse(client_location);
    let bucket_count = 1_u64 << 30;
    let mut shards = Vec::new();
    for offset in 0..table.shard_count {
        let shard_id = table_shard_id(table, offset).unwrap_or(table.first_shard_id + offset);
        let start_bucket = bucket_count * offset / table.shard_count;
        let end_bucket = (bucket_count * (offset + 1) / table.shard_count).saturating_sub(1);
        let mut replicas = Vec::new();
        let mut seen_replicas = BTreeSet::new();
        let mut used_locations = BTreeSet::new();
        let mut used_hosts = BTreeSet::new();
        let mut placed_locations: Vec<Location> = Vec::new();
        // The owner is the one entry that reaches the replica list without
        // passing the Normal filter the candidate scan applies. A server that
        // was frozen or dropped is not serving, so naming it is telling a client
        // to read from somewhere that is deliberately out of service.
        let owner = state
            .shards
            .get(&shard_id)
            .filter(|location| owner_is_serving(state, &location.server_addr));
        if let Some(location) = owner {
            if let Some(server) = state.servers.get(&location.server_addr) {
                placed_locations.push(Location::parse(&server.location));
            }
            push_replica(
                state,
                &mut replicas,
                &mut seen_replicas,
                &mut used_locations,
                &mut used_hosts,
                &location.server_addr,
            );
        }
        // Spread replicas as far apart as the topology allows: try the widest
        // separation first (a different top-level domain) and only narrow when
        // nothing qualifies. Comparing whole location strings, as this used to,
        // treats two racks in one availability unit as "different locations" and
        // happily puts both replicas of a shard inside it.
        for separation in separation_ladder(&candidate_locations) {
            if replicas.len() >= table.replica_count as usize {
                break;
            }
            for (index, candidate) in normal_servers.iter().enumerate() {
                if replicas.len() >= table.replica_count as usize {
                    break;
                }
                if seen_replicas.contains(&candidate.server_addr) {
                    continue;
                }
                // Parsed once, above, into candidate_locations. This scan runs
                // per shard and per rung of the separation ladder, so re-parsing
                // here cost one parse and allocation per server per shard per
                // rung on a call every client and proxy makes.
                let candidate_location = &candidate_locations[index];
                if !separated_from(&placed_locations, candidate_location, separation) {
                    continue;
                }
                let host = server_host(&candidate.server_addr);
                if !host.is_empty() && used_hosts.contains(&host) {
                    continue;
                }
                if !candidate_location.is_empty() {
                    placed_locations.push(candidate_location.clone());
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
        let primary = match state.shards.get(&shard_id) {
            // A recorded owner that is not serving leaves the shard with no
            // primary. Falling through to the candidate scan would nominate a
            // server that never loaded this shard, and a client that followed
            // the nomination would read an empty shard and believe it.
            Some(location) => {
                owner_is_serving(state, &location.server_addr).then(|| location.server_addr.clone())
            }
            // No owner recorded yet: propose a placement, which is how a new
            // table gets one before anything registers.
            None => replicas.first().cloned(),
        };
        // Replicas are deliberately spread as far apart as the topology
        // allows, so most of a shard's replicas are far from any given caller
        // by construction. Ordering them nearest-first is what lets a caller
        // that has a replica in its own location read from it rather than
        // crossing the fabric to whichever server happened to sort first on
        // load. Only the order changes: the same servers are returned, and the
        // primary -- which is where the shard is actually owned -- is untouched.
        if !caller.is_empty() {
            // Each replica's distance is worked out once. `sort_by_key` calls
            // its key function O(n log n) times, and this one parsed a location
            // on every call, so the distance is computed up front and the sort
            // then only compares integers.
            //
            // Still stable: ties fall back to the position the scan above
            // produced, so servers equally close to the caller keep their
            // load-ordered sequence.
            let mut ranked = replicas
                .iter()
                .enumerate()
                .map(|(position, server_addr)| {
                    let distance = state
                        .servers
                        .get(server_addr)
                        .map(|server| caller.shared_prefix_len(&Location::parse(&server.location)))
                        .unwrap_or(0);
                    (distance, position, server_addr.clone())
                })
                .collect::<Vec<_>>();
            ranked.sort_by(|left, right| {
                right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1))
            });
            replicas = ranked
                .into_iter()
                .map(|(_, _, server_addr)| server_addr)
                .collect();
        }
        let primary_endpoint = primary
            .as_ref()
            .map(|server_addr| server_endpoint(state, server_addr));
        let replica_endpoints = replicas
            .iter()
            .map(|server_addr| server_endpoint(state, server_addr))
            .collect();
        shards.push(TableShard {
            shard_id,
            start_bucket,
            end_bucket,
            primary,
            replicas,
            primary_endpoint,
            replica_endpoints,
        });
    }
    shards
}


