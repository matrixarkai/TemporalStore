// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Table partition/placement construction, extracted from meta.rs.

use super::*;
use std::collections::BTreeSet;

/// The `load_version` the PRIMARY reports for this shard, or 0 when nothing is known.
///
/// This is the same value `validate_load_version` compares against on the data node, because it
/// is the server's own report of what it has loaded. Taking the max is deliberate: a server
/// mid-handoff can briefly report two entries for one shard, and the newer one is the one a write
/// should be fenced against.
///
/// 0 on every unknown -- no such server, no entry, not loaded -- so an unknown never produces a
/// token that would fence a client out.
fn reported_load_version(state: &MetaState, server_addr: &str, shard_id: ShardId) -> u64 {
    state
        .servers
        .get(server_addr)
        .and_then(|server| {
            server
                .shard_states
                .iter()
                .filter(|shard_state| shard_state.shard_id == shard_id && shard_state.loaded)
                .map(|shard_state| shard_state.load_version)
                .max()
        })
        .unwrap_or(0)
}

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

fn data_raft_route(
    state: &MetaState,
    table: &TableMetaInfo,
    shard_id: ShardId,
) -> Option<(DataRaftRoute, u64)> {
    let reporters = state.data_raft_reporters.get(&shard_id)?;
    let owning_table = table_for_shard(state, shard_id)?;
    if owning_table.info.table_id != table.table_id {
        return None;
    }
    let mut newest = None;
    let mut leader = None;
    let mut conflict = false;
    for addr in reporters {
        let Some(server) = state.servers.get(addr) else { continue };
        for shard in server.shard_states.iter().filter(|shard| {
            shard.shard_id == shard_id && shard.loaded
                && (shard.table_name.is_empty() || shard.table_name == table.table_name)
        }) {
            let Some(role) = shard.raft_role else { continue };
            let epoch = (shard.load_version, role.term);
            if newest.is_none_or(|current| epoch > current) {
                newest = Some(epoch);
                leader = None;
                conflict = false;
            }
            if newest != Some(epoch) || !role.is_leader || shard.readonly
                || server.state != MetaEntityState::Normal || server.reboot_detected {
                continue;
            }
            conflict |= leader.is_some_and(|current| current != addr);
            leader = Some(addr);
        }
    }
    let (load_version, term) = newest.unwrap_or_default();
    let endpoint = leader.filter(|_| !conflict).map(|addr| server_endpoint(state, addr));
    Some((DataRaftRoute { endpoint, load_version }, term))
}

/// Split a table's budget across its shard range. Only the currently routed
/// Raft leader receives a budget for the load and term it reported.
pub(super) fn cache_budgets_for_server(
    state: &MetaState,
    server_addr: &str,
) -> Vec<ShardCacheBudget> {
    let Some(server) = state.servers.get(server_addr) else {
        return Vec::new();
    };
    server
        .shard_states
        .iter()
        .filter_map(|shard| {
            let role = shard.raft_role.filter(|role| role.is_leader)?;
            if !shard.loaded || shard.readonly {
                return None;
            }
            let table = &table_for_shard(state, shard.shard_id)?.info;
            let options = &table.serving_options;
            if !options.cache_class
                || options.cache_budget_bytes == 0
                || table.shard_count == 0
                || table.state != MetaEntityState::Normal
                || state
                    .namespaces
                    .get(&table.namespace)
                    .is_some_and(|status| *status != MetaEntityState::Normal)
                || (!shard.table_name.is_empty() && shard.table_name != table.table_name)
            {
                return None;
            }
            if state.table_ranges_overlap
                && state.tables.values().any(|other| {
                    let other = &other.info;
                    let end = other.first_shard_id.saturating_add(other.shard_count);
                    other.table_id != table.table_id
                        && (other.first_shard_id..end).contains(&shard.shard_id)
                })
            {
                return None;
            }
            let (route, term) = data_raft_route(state, table, shard.shard_id)?;
            if term != role.term
                || route.load_version != shard.load_version
                || route.endpoint?.server_addr != server_addr
            {
                return None;
            }
            let offset = shard.shard_id - table.first_shard_id;
            Some(ShardCacheBudget {
                shard_id: shard.shard_id,
                table_id: table.table_id,
                load_version: shard.load_version,
                raft_term: role.term,
                bytes: options.cache_budget_bytes / table.shard_count
                    + u64::from(offset < options.cache_budget_bytes % table.shard_count),
            })
        })
        .collect()
}

pub(super) fn build_shards(
    state: &MetaState,
    table: &TableMetaInfo,
    client_location: &str,
) -> Vec<TableShard> {
    /// A server being considered for a shard, borrowed from the metadata
    /// rather than copied out of it.
    ///
    /// These used to own their two strings, which is two heap allocations per
    /// live server on every topology request, for values dropped when the
    /// request ends. The state is read-locked for the whole of this function,
    /// so borrowing is sound and the copies bought nothing.
    #[derive(Debug)]
    struct PlacementCandidate<'a> {
        server_addr: &'a str,
        location: &'a str,
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
            PlacementCandidate {
                server_addr: server.server_addr.as_str(),
                location: server.location.as_str(),
                degraded: !server.runtime_load.degraded_reasons.is_empty(),
                queue_depth: server.runtime_load.queue_depth,
                background_queue_depth: server.runtime_load.background_queue_depth,
                running_shard_count: server.runtime_load.running_shard_count,
                dirty_object_count: server.runtime_load.dirty_object_count,
                dirty_shard_count: server.runtime_load.dirty_shard_count,
                shard_state_penalty: server.worst_shard_state_penalty,
                key_count: server.load_key_count,
                memory_bytes: server.load_memory_bytes,
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
            left.server_addr,
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
                right.server_addr,
            ))
    });
    // Every live server's parsed location, used to size the separation ladder.
    let candidate_locations = normal_servers
        .iter()
        .map(|candidate| Location::parse(candidate.location))
        .collect::<Vec<_>>();
    let caller = Location::parse(client_location);
    // The locations above, reachable by address, so ranking a shard's replicas
    // does not parse them again for every shard.
    let location_of = normal_servers
        .iter()
        .map(|candidate| candidate.server_addr)
        .zip(candidate_locations.iter())
        .collect::<BTreeMap<&str, &Location>>();
    // Derived from the candidate locations alone, so it is the same for every
    // shard of this table. It was being rebuilt per shard.
    let ladder = separation_ladder(&candidate_locations);
    let bucket_count = 1_u64 << 30;
    let mut shards = Vec::new();
    for offset in 0..table.shard_count {
        let shard_id = table_shard_id(table, offset).unwrap_or(table.first_shard_id + offset);
        let start_bucket = bucket_count * offset / table.shard_count;
        let end_bucket = (bucket_count * (offset + 1) / table.shard_count).saturating_sub(1);
        let mut replicas = Vec::new();
        let mut seen_replicas = BTreeSet::new();
        let mut used_hosts = BTreeSet::new();
        let mut placed_locations: Vec<Location> = Vec::new();
        // A shard that is not serving gets no placement at all. Skipping only
        // its recorded owner is not enough: the candidate scan below would pick
        // a primary for it anyway, and it would stay routable.
        let serving = state
            .shards
            .get(&shard_id)
            .map(|location| location.state == MetaEntityState::Normal)
            .unwrap_or(true);
        if !serving {
            shards.push(TableShard {
                raft_route: None,
                shard_id,
                start_bucket,
                end_bucket,
                primary: None,
                replicas: Vec::new(),
                primary_endpoint: None,
                replica_endpoints: Vec::new(),
                // Not serving, so there is no primary to be fenced against.
                load_version: 0,
            });
            continue;
        }
        // A serving shard can still be owned by a server that is not serving.
        // That is a separate question from the one above, and the owner is the
        // one entry that reaches the replica list without passing the Normal
        // filter the candidate scan applies.
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
                &mut used_hosts,
                &location.server_addr,
            );
        }
        // Spread replicas as far apart as the topology allows: try the widest
        // separation first (a different top-level domain) and only narrow when
        // nothing qualifies. Comparing whole location strings, as this used to,
        // treats two racks in one availability unit as "different locations" and
        // happily puts both replicas of a shard inside it.
        for &separation in &ladder {
            if replicas.len() >= table.replica_count as usize {
                break;
            }
            for (index, candidate) in normal_servers.iter().enumerate() {
                if replicas.len() >= table.replica_count as usize {
                    break;
                }
                if seen_replicas.contains(candidate.server_addr) {
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
                let host = server_host(candidate.server_addr);
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
                    &mut used_hosts,
                    candidate.server_addr,
                );
            }
        }
        // Whatever the ladder could not place is filled here. Two passes, not
        // one: a host already holding a replica of this shard is taken only
        // when nothing else is left.
        //
        // This used to be a single unconditional pass. `push_replica` records
        // `used_hosts` but does not enforce it -- only the ladder above checked
        // it -- so when every server shares a location the ladder rejects them
        // all, every replica after the first came from here, and a shard could
        // put two replicas on one host while another host sat idle. It reported
        // the full replica count and lost all of them to one host failure.
        for allow_used_host in [false, true] {
            for candidate in &normal_servers {
                if replicas.len() >= table.replica_count as usize {
                    break;
                }
                if !allow_used_host {
                    let host = server_host(candidate.server_addr);
                    if !host.is_empty() && used_hosts.contains(&host) {
                        continue;
                    }
                }
                push_replica(
                    state,
                    &mut replicas,
                    &mut seen_replicas,
                    &mut used_hosts,
                    candidate.server_addr,
                );
            }
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
                    // A replica is normally one of the candidates above, whose
                    // location is already parsed. A shard whose recorded owner
                    // is not among them -- a route naming a server the
                    // metaserver has not heard about -- still has to be parsed.
                    let distance = match location_of.get(server_addr.as_str()) {
                        Some(location) => caller.shared_prefix_len(location),
                        None => state
                            .servers
                            .get(server_addr)
                            .map(|server| {
                                caller.shared_prefix_len(&Location::parse(&server.location))
                            })
                            .unwrap_or(0),
                    };
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
        let load_version = primary
            .as_deref()
            .map(|server_addr| reported_load_version(state, server_addr, shard_id))
            .unwrap_or(0);
        shards.push(TableShard {
            raft_route: data_raft_route(state, table, shard_id).map(|(route, _)| route),
            shard_id,
            start_bucket,
            end_bucket,
            primary,
            replicas,
            primary_endpoint,
            replica_endpoints,
            load_version,
        });
    }
    shards
}

#[cfg(test)]
mod raft_route_tests {
    use super::*;

    #[test]
    fn elected_leader_route_preserves_placement_and_rejects_old_terms() {
        let meta = SingleNodeMeta::default();
        assert!(meta.add_table(AddTableRequest {
            namespace: "kvcache".into(), table_name: "model".into(),
            first_shard_id: 100, shard_count: 3, replica_count: 3,
            partition_version: 0,
            serving_options: TableServingOptions {
                cache_class: true,
                cache_budget_bytes: 10,
                ..Default::default()
            },
        }).status.ok);
        for (node_id, host) in [(1, "a"), (2, "b"), (3, "c")] {
            assert!(meta.register_server(RegisterServerRequest {
                server_addr: format!("{host}:1000"), grpc_addr: Some(format!("{host}:2000")),
                node_id, location: String::new(), binary_version: String::new(),
                numa_nodes: Vec::new(), registered_at_ms: 0,
            }).status.ok);
        }
        assert!(meta.register(RegisterShardRequest {
            shard_id: 100, server_addr: "a:1000".into(), registered_at_ms: 0,
        }).status.ok);
        let report = |host: &str, term, is_leader| {
            let response = meta.server_heartbeat(ServerHeartbeatRequest {
                server_addr: format!("{host}:1000"),
                boot_time_ms: 1, binary_version: String::new(),
                shard_loads: Vec::new(), shard_stat_loads: Vec::new(),
                runtime_load: Default::default(),
                shard_states: vec![ServerShardServingState {
                    shard_id: 100, loaded: true, load_version: 7, table_name: "model".into(),
                    raft_role: Some(DataRaftShardRole { term, is_leader }),
                    ..Default::default()
                }],
            });
            assert!(response.status.ok);
            response.cache_budgets
        };
        let topology = |meta: &SingleNodeMeta, old_topology_version| {
            meta.get_table_topology(GetTableTopologyRequest {
                namespace: "kvcache".into(), table_name: "model".into(),
                client_location: String::new(), old_topology_version,
            })
        };

        let budgets = report("a", 1, true);
        assert_eq!(budgets.len(), 1);
        assert_eq!((budgets[0].bytes, budgets[0].load_version, budgets[0].raft_term), (4, 7, 1));
        let first = topology(&meta, 0);
        let table = first.table.unwrap();
        assert_eq!(budgets[0].table_id, table.table_id);
        let version = table.topology_version;
        let route = first.shards[0].raft_route.as_ref().unwrap();
        assert_eq!(route.endpoint.as_ref().unwrap().grpc_addr.as_deref(), Some("a:2000"));

        assert!(report("b", 2, false).is_empty());
        let interim = topology(&meta, version);
        assert!(!interim.unchanged);
        assert!(interim.shards[0].raft_route.as_ref().unwrap().endpoint.is_none());

        let budgets = report("b", 2, true);
        assert_eq!(budgets.len(), 1);
        assert_eq!(budgets[0].raft_term, 2);
        assert!(report("a", 1, true).is_empty());
        let current = topology(&meta, 0);
        assert_eq!(current.shards[0].primary.as_deref(), Some("a:1000"));
        let route = current.shards[0].raft_route.as_ref().unwrap();
        assert_eq!(route.endpoint.as_ref().unwrap().grpc_addr.as_deref(), Some("b:2000"));
        assert_eq!(route.load_version, 7);
        let current_version = current.table.as_ref().unwrap().topology_version;
        report("b", 1, false);
        assert!(topology(&meta, current_version).unchanged);
        assert_eq!(topology(&meta, 0).shards, current.shards);
        report("b", 2, true);
        assert!(topology(&meta, current_version).unchanged);

        // Neither claimant can reclaim cache data while the term has two reported leaders.
        assert!(report("a", 2, true).is_empty());
        assert!(report("b", 2, true).is_empty());
        let conflicted = topology(&meta, current_version);
        assert!(!conflicted.unchanged);
        assert!(conflicted.shards[0].raft_route.as_ref().unwrap().endpoint.is_none());
        assert!(report("a", 2, false).is_empty());
        let recovered = report("b", 2, true);
        assert_eq!(recovered, budgets);

        let restored = SingleNodeMeta::default();
        assert!(restored.install_snapshot(meta.export_snapshot()).status.ok);
        assert_eq!(topology(&restored, 0).shards, current.shards);

        assert!(meta.update_table(UpdateTableRequest {
            namespace: "kvcache".into(),
            table_name: "model".into(),
            shard_count: None,
            replica_count: None,
            first_shard_id: None,
            partition_version: None,
            serving_options: Some(TableServingOptionsPatch {
                cache_class: Some(false),
                ..Default::default()
            }),
        }).status.ok);
        assert!(report("b", 2, true).is_empty());
    }
}
