// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! SingleNodeMeta topology/listing/freeze/finish-load lifecycle methods, extracted from meta.rs.

use super::*;

/// One round of the simple failure detector, in the shape the metrics recorder
/// takes.
///
/// The fields left empty belong to the adaptive detector: safe-mode holds,
/// per-location damage, reboot detection, the orphan guard. This detector has
/// none of them, so empty is what actually happened rather than a placeholder.
fn simple_conviction_round(
    frozen_servers: Vec<String>,
    frozen_proxies: Vec<String>,
) -> AdaptiveConvictionReport {
    AdaptiveConvictionReport {
        status: Status::ok(),
        frozen_servers,
        frozen_proxies,
        held_by_safe_mode: Vec::new(),
        damage: Vec::new(),
        rebooted: Vec::new(),
        detector_paused: false,
        held_by_orphan_guard: Vec::new(),
        orphaned_shards: Vec::new(),
    }
}

impl SingleNodeMeta {
    pub fn get_table_topology(&self, request: GetTableTopologyRequest) -> TableTopologyResponse {
        self.counters.topology_query_total.fetch_add(1, Ordering::Relaxed);
        // Resolving a topology only reads. It took the exclusive lock solely to
        // count, which serialised every client's and every proxy's hottest call
        // against each other and against all metadata writes.
        let state = self.inner.read().expect("meta lock poisoned");
        let Some(table) = state
            .tables
            .get(&table_key(&request.namespace, &request.table_name))
        else {
            return TableTopologyResponse {
                status: Status::error("table_not_found", "table not found"),
                table: None,
                shards: Vec::new(),
                unchanged: false,
            };
        };
        if table.info.state == MetaEntityState::Dropped {
            return TableTopologyResponse {
                status: Status::error("table_not_found", "table is dropped"),
                table: Some(table.info.clone()),
                shards: Vec::new(),
                unchanged: false,
            };
        }
        if table.info.state == MetaEntityState::Frozen {
            return TableTopologyResponse {
                status: Status::error("resource_frozen", "table is frozen"),
                table: Some(table.info.clone()),
                shards: Vec::new(),
                unchanged: false,
            };
        }
        // A namespace freeze covers every table in it, including tables
        // created after the freeze -- checking the table alone would let one
        // slip through.
        match state.namespaces.get(&request.namespace).copied() {
            Some(MetaEntityState::Dropped) => {
                return TableTopologyResponse {
                    status: Status::error("table_not_found", "namespace is dropped"),
                    table: Some(table.info.clone()),
                    shards: Vec::new(),
                    unchanged: false,
                };
            }
            Some(MetaEntityState::Frozen) => {
                return TableTopologyResponse {
                    status: Status::error("resource_frozen", "namespace is frozen"),
                    table: Some(table.info.clone()),
                    shards: Vec::new(),
                    unchanged: false,
                };
            }
            _ => {}
        }
        if request.old_topology_version >= table.info.topology_version {
            return TableTopologyResponse {
                status: Status::ok(),
                table: Some(table.info.clone()),
                shards: Vec::new(),
                unchanged: true,
            };
        }
        let shards = build_shards(&state, &table.info, &request.client_location);
        TableTopologyResponse {
            status: Status::ok(),
            table: Some(table.info.clone()),
            shards,
            unchanged: false,
        }
    }

    pub fn list_namespaces(&self) -> ListNamespacesResponse {
        let state = self.inner.read().expect("meta lock poisoned");
        // Tally every namespace in one pass. Counting each namespace's tables
        // by filtering the whole table set cost namespaces times tables, which
        // a metrics scrape pays on every tick: 151.8us at 32 namespaces of 32
        // tables, against 0.4us at 4 of 4.
        let mut table_counts: BTreeMap<&str, usize> = BTreeMap::new();
        for table in state.tables.values() {
            if table.info.state != MetaEntityState::Dropped {
                *table_counts
                    .entry(table.info.namespace.as_str())
                    .or_default() += 1;
            }
        }
        let namespaces = state
            .namespaces
            .iter()
            .map(|(namespace, state_value)| NamespaceMetaInfo {
                namespace: namespace.clone(),
                // A namespace holding no table is absent from the tally, not
                // zero, so it still reports zero here.
                table_count: table_counts
                    .get(namespace.as_str())
                    .copied()
                    .unwrap_or_default(),
                state: *state_value,
            })
            .collect();
        ListNamespacesResponse {
            status: Status::ok(),
            namespaces,
        }
    }

    pub fn list_tables(&self) -> ListTablesResponse {
        let state = self.inner.read().expect("meta lock poisoned");
        ListTablesResponse {
            status: Status::ok(),
            tables: state
                .tables
                .values()
                .map(|table| table.info.clone())
                .collect(),
        }
    }

    /// List shard placement, ordered by shard id and returned a page at a time.
    ///
    /// Servers, proxies, proxy groups, namespaces and tables could all be
    /// listed; shards -- the thing the metaserver exists to place -- could only
    /// be fetched one id at a time, so answering "what is on this node?" or
    /// "which shards has nothing claimed?" meant knowing every id up front.
    pub fn list_shards(&self, request: ListShardsRequest) -> ListShardsResponse {
        let state = self.inner.read().expect("meta lock poisoned");
        let limit = if request.limit == 0 {
            LIST_SHARDS_DEFAULT_LIMIT
        } else {
            request.limit.min(LIST_SHARDS_DEFAULT_LIMIT)
        };

        // One pass over the tables to build shard id -> owning table, rather
        // than asking per shard: the per-shard lookup scans every table, so
        // doing it inside the loop would be quadratic in a large deployment.
        let mut shard_tables = BTreeMap::new();
        for table in state.tables.values() {
            for offset in 0..table.info.shard_count {
                if let Ok(shard_id) = table_shard_id(&table.info, offset) {
                    shard_tables.insert(
                        shard_id,
                        (table.info.namespace.clone(), table.info.table_name.clone()),
                    );
                }
            }
        }

        let mut ids = state
            .shards
            .keys()
            .copied()
            .filter(|shard_id| *shard_id > request.after_shard_id)
            .collect::<Vec<_>>();
        ids.sort_unstable();

        let mut shards = Vec::new();
        let mut next_after_shard_id = None;
        for shard_id in ids {
            let Some(location) = state.shards.get(&shard_id) else {
                continue;
            };
            if !request.server_addr.is_empty() && location.server_addr != request.server_addr {
                continue;
            }
            if shards.len() == limit {
                // A full page and at least one more match: hand back where to
                // resume rather than silently truncating.
                next_after_shard_id = shards.last().map(|entry: &ShardListEntry| entry.shard_id);
                break;
            }
            let (namespace, table_name) = shard_tables.get(&shard_id).cloned().unwrap_or_default();
            // What the owner says about this shard, when it says anything at
            // all. A server that has never reported its shard states cannot be
            // read as reporting an empty set.
            let owner_reports_loaded = state
                .servers
                .get(&location.server_addr)
                .filter(|server| server.reports_shard_states)
                .map(|server| {
                    server
                        .shard_states
                        .iter()
                        .any(|reported| reported.shard_id == shard_id && reported.loaded)
                });
            shards.push(ShardListEntry {
                shard_id,
                server_addr: location.server_addr.clone(),
                namespace,
                table_name,
                latest_snapshot: location.latest_snapshot.clone(),
                state: location.state,
                owner_reports_loaded,
            });
        }

        ListShardsResponse {
            status: Status::ok(),
            shards,
            next_after_shard_id,
        }
    }

    pub fn list_servers(&self) -> ListServersResponse {
        let state = self.inner.read().expect("meta lock poisoned");
        ListServersResponse {
            status: Status::ok(),
            servers: state.servers.values().cloned().collect(),
        }
    }

    pub fn freeze_stale_servers(&self, stale_after_ms: u64) -> StaleServerReport {
        let report = self.freeze_stale_resources(stale_after_ms);
        StaleServerReport {
            status: report.status,
            frozen_servers: report.frozen_servers,
        }
    }

    pub fn freeze_stale_resources(&self, stale_after_ms: u64) -> StaleResourceReport {
        self.freeze_stale_resources_with_policy(
            stale_after_ms,
            SafeModePolicy {
                server_freeze_cooldown_ms: 0,
                proxy_freeze_cooldown_ms: 0,
            },
        )
    }

    pub fn freeze_stale_resources_with_policy(
        &self,
        stale_after_ms: u64,
        policy: SafeModePolicy,
    ) -> StaleResourceReport {
        let now = now_ms();
        let stale_servers = {
            let state = self.inner.read().expect("meta lock poisoned");
            state
                .servers
                .values()
                .filter(|server| {
                    server.state == MetaEntityState::Normal
                        && now.saturating_sub(server.last_heartbeat_ms) > stale_after_ms
                })
                .map(|server| server.server_addr.clone())
                .collect::<Vec<_>>()
        };

        let mut frozen_servers = Vec::new();
        for endpoint in stale_servers {
            let response = self.freeze_server(StateChangeRequest {
                endpoint: endpoint.clone(),
                freeze_cooldown_ms: policy.server_freeze_cooldown_ms,
                // The metaserver decided this, not an operator.
                reason: FreezeReason::Unresponsive,
            });
            if !response.status.ok {
                return StaleResourceReport {
                    status: response.status,
                    frozen_servers,
                    frozen_proxies: Vec::new(),
                };
            }
            frozen_servers.push(endpoint);
        }

        let proxy_report = self.freeze_stale_proxies(stale_after_ms, policy);
        // Recorded per tier, the way the adaptive detector records its own
        // rounds. `record_conviction` counts both lists, so one call carrying
        // both would double every freeze; the adaptive path calls it once per
        // tier with the other list empty, and this matches it.
        //
        // Without this the counter is exported unconditionally and sits at zero
        // while this detector -- the default one, since
        // TS_META_ADAPTIVE_FAILURE_DETECTOR is off unless asked for -- freezes
        // servers and proxies. A confident wrong number reads worse than an
        // absent series: `absent()` catches a missing one, nothing catches a
        // zero.
        self.metrics.record_conviction(
            TIER_SERVER,
            &simple_conviction_round(frozen_servers.clone(), Vec::new()),
        );
        self.metrics.record_conviction(
            TIER_PROXY,
            &simple_conviction_round(Vec::new(), proxy_report.frozen_proxies.clone()),
        );
        StaleResourceReport {
            status: proxy_report.status,
            frozen_servers,
            frozen_proxies: proxy_report.frozen_proxies,
        }
    }

    /// Freeze every proxy whose heartbeat is older than `stale_after_ms`.
    ///
    /// Split out of [`Self::freeze_stale_resources_with_policy`] so the adaptive
    /// server detector ([`Self::convict_stale_servers_adaptive`]) can keep
    /// sweeping proxies on the fixed threshold: proxies are stateless routers,
    /// so freezing one is cheap and does not move data, and the correlated
    /// -failure reasoning that applies to datanodes does not apply to them.
    pub fn freeze_stale_proxies(
        &self,
        stale_after_ms: u64,
        policy: SafeModePolicy,
    ) -> StaleResourceReport {
        let now = now_ms();
        let stale_proxies = {
            let state = self.inner.read().expect("meta lock poisoned");
            state
                .proxies
                .values()
                .filter(|proxy| {
                    proxy.state == MetaEntityState::Normal
                        && now.saturating_sub(proxy.last_heartbeat_ms) > stale_after_ms
                })
                .map(|proxy| proxy.proxy_addr.clone())
                .collect::<Vec<_>>()
        };

        let mut frozen_proxies = Vec::new();
        for endpoint in stale_proxies {
            let response = self.freeze_proxy(StateChangeRequest {
                endpoint: endpoint.clone(),
                freeze_cooldown_ms: policy.proxy_freeze_cooldown_ms,
                reason: FreezeReason::Unresponsive,
            });
            if !response.status.ok {
                return StaleResourceReport {
                    status: response.status,
                    frozen_servers: Vec::new(),
                    frozen_proxies,
                };
            }
            frozen_proxies.push(endpoint);
        }

        StaleResourceReport {
            status: Status::ok(),
            frozen_servers: Vec::new(),
            frozen_proxies,
        }
    }

    pub fn safe_mode_report(&self) -> SafeModeReport {
        let state = self.inner.read().expect("meta lock poisoned");
        let now = now_ms();
        let blocked_servers = state
            .servers
            .values()
            .filter(|server| {
                server.state == MetaEntityState::Frozen && server.freeze_cooldown_until_ms > now
            })
            .map(|server| server.server_addr.clone())
            .collect::<Vec<_>>();
        let blocked_proxies = state
            .proxies
            .values()
            .filter(|proxy| {
                proxy.state == MetaEntityState::Frozen && proxy.freeze_cooldown_until_ms > now
            })
            .map(|proxy| proxy.proxy_addr.clone())
            .collect::<Vec<_>>();
        SafeModeReport {
            status: Status::ok(),
            blocked_servers,
            blocked_proxies,
            server_count: state.servers.len(),
            proxy_count: state.proxies.len(),
        }
    }

    pub fn start_failure_detector_loop(
        &self,
        stale_after_ms: u64,
        interval_ms: u64,
    ) -> thread::JoinHandle<()> {
        let meta = self.clone();
        let interval = Duration::from_millis(interval_ms.max(1));
        thread::spawn(move || loop {
            let _ = meta.freeze_stale_resources(stale_after_ms);
            thread::sleep(interval);
        })
    }

    pub fn list_proxies(&self) -> ListProxiesResponse {
        let state = self.inner.read().expect("meta lock poisoned");
        ListProxiesResponse {
            status: Status::ok(),
            proxies: state.proxies.values().cloned().collect(),
        }
    }

    pub fn freeze_server(&self, request: StateChangeRequest) -> AckResponse {
        self.set_server_state(request, MetaEntityState::Frozen)
    }

    /// Return a frozen server to service.
    ///
    /// Until now the only way out of a freeze was for the server to re-register,
    /// which meant an operator had no lever at all and a convicted node cleared
    /// its own conviction. This is that lever: it is always available, whatever
    /// the freeze reason, because an operator must be able to overrule the
    /// metaserver.
    pub fn unfreeze_server(&self, request: StateChangeRequest) -> AckResponse {
        self.set_server_state(request, MetaEntityState::Normal)
    }

    /// Take a server out of service because it announced its own shutdown.
    ///
    /// Without this a clean shutdown is indistinguishable from a crash: the node
    /// simply stops heartbeating, and the metaserver waits out the whole
    /// detection window before reacting. Every read routed to that node during
    /// the window fails, and a rolling deploy pays that cost once per node.
    ///
    /// Idempotent: a server that is already out of service reports
    /// `not_modified` rather than failing, so a shutdown hook that retries or
    /// races the failure detector does not produce spurious errors.
    pub fn notify_server_stop(&self, request: NotifyStopRequest) -> AckResponse {
        {
            let state = self.inner.read().expect("meta lock poisoned");
            let Some(server) = state.servers.get(&request.endpoint) else {
                return AckResponse {
                    status: Status::error("server_not_found", "server not found"),
                };
            };
            if server.state != MetaEntityState::Normal {
                return AckResponse {
                    status: Status::error("not_modified", "server is already out of service"),
                };
            }
        }
        self.freeze_server(StateChangeRequest {
            endpoint: request.endpoint,
            freeze_cooldown_ms: 0,
            // No cooldown and not a conviction: this node is expected back.
            reason: FreezeReason::Stopping,
        })
    }

    /// Take a proxy out of service because it announced its own shutdown.
    ///
    /// A proxy is dropped rather than frozen, matching the reference. It holds
    /// no data, so there is nothing to preserve by keeping a tombstone in the
    /// routing set -- and leaving it frozen would keep it in the damage
    /// accounting the conviction gate reads.
    pub fn notify_proxy_stop(&self, request: NotifyStopRequest) -> AckResponse {
        {
            let state = self.inner.read().expect("meta lock poisoned");
            let Some(proxy) = state.proxies.get(&request.endpoint) else {
                return AckResponse {
                    status: Status::error("proxy_not_found", "proxy not found"),
                };
            };
            if proxy.state != MetaEntityState::Normal {
                return AckResponse {
                    status: Status::error("not_modified", "proxy is already out of service"),
                };
            }
        }
        self.drop_proxy(StateChangeRequest {
            endpoint: request.endpoint,
            freeze_cooldown_ms: 0,
            reason: FreezeReason::Stopping,
        })
    }

    pub fn drop_server(&self, request: StateChangeRequest) -> AckResponse {
        self.set_server_state(request, MetaEntityState::Dropped)
    }

    pub fn freeze_proxy(&self, request: StateChangeRequest) -> AckResponse {
        self.set_proxy_state(request, MetaEntityState::Frozen)
    }

    /// Return a frozen proxy to service. See [`Self::unfreeze_server`].
    pub fn unfreeze_proxy(&self, request: StateChangeRequest) -> AckResponse {
        self.set_proxy_state(request, MetaEntityState::Normal)
    }

    pub fn drop_proxy(&self, request: StateChangeRequest) -> AckResponse {
        self.set_proxy_state(request, MetaEntityState::Dropped)
    }

    pub fn finish_load(&self, request: LoadFinishRequest) -> AckResponse {
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
        self.record_mutation(MetaMutation::FinishLoad(request.clone()));
        self.apply_finish_load(request)
    }

    pub(super) fn apply_finish_load(&self, request: LoadFinishRequest) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        self.counters.load_finish_total.fetch_add(1, Ordering::Relaxed);
        if !request.status.ok {
            return AckResponse {
                status: request.status,
            };
        }
        let Some(server) = state.servers.get(&request.server_addr) else {
            return AckResponse {
                status: Status::error(
                    "server_not_found",
                    "server must register before finish_load",
                ),
            };
        };
        if server.state != MetaEntityState::Normal {
            return AckResponse {
                status: Status::error("resource_frozen", "server is not serving"),
            };
        }
        if let Some(newer_state) = server
            .shard_states
            .iter()
            .filter(|state| state.shard_id == request.shard_id)
            .map(|state| state.load_version)
            .max()
            .filter(|load_version| *load_version > request.load_version)
        {
            return AckResponse {
                status: Status::error(
                    "stale_load_version",
                    format!(
                        "finish_load version {} is older than server-reported version {newer_state}",
                        request.load_version
                    ),
                ),
            };
        }
        if request.scheduler_task_id.is_some() && request.scheduler_generation.is_none() {
            return AckResponse {
                status: Status::error(
                    "scheduler_generation_required",
                    "scheduler-owned finish_load must include scheduler_generation",
                ),
            };
        }
        if let Some(generation) = request.scheduler_generation {
            let generation_key =
                scheduler_finish_generation_key(request.shard_id, &request.server_addr);
            if let Some(previous) = state.scheduler_finish_generations.get(&generation_key) {
                if generation < *previous {
                    return AckResponse {
                        status: Status::error(
                            "stale_scheduler_generation",
                            format!(
                                "finish_load scheduler_generation {generation} is older than accepted generation {previous}"
                            ),
                        ),
                    };
                }
            }
        }
        if let Some(table) = table_for_shard(&state, request.shard_id) {
            if table.info.state == MetaEntityState::Dropped {
                return AckResponse {
                    status: Status::error("table_not_found", "table is dropped"),
                };
            }
            if table.info.state == MetaEntityState::Frozen {
                return AckResponse {
                    status: Status::error("resource_frozen", "table is frozen"),
                };
            }
        }
        let latest_snapshot = state
            .shards
            .get(&request.shard_id)
            .and_then(|location| location.latest_snapshot.clone());
        let server_addr = request.server_addr.clone();
        state.shards.insert(
            request.shard_id,
            ShardLocation {
<<<<<<< HEAD
                registered_at_ms: 0,
||||||| a7277311
=======
                preferred_location: String::new(),
>>>>>>> matrixark/main
                state: MetaEntityState::Normal,
                shard_id: request.shard_id,
                server_addr: server_addr.clone(),
                latest_snapshot,
            },
        );
        if let Some(generation) = request.scheduler_generation {
            let generation_key = scheduler_finish_generation_key(request.shard_id, &server_addr);
            state
                .scheduler_finish_generations
                .entry(generation_key)
                .and_modify(|previous| *previous = (*previous).max(generation))
                .or_insert(generation);
        }
        record_topology_event(
            &mut state,
            "finish_load",
            format!("shard:{}", request.shard_id),
            format!("server_addr={server_addr}"),
        );
        AckResponse {
            status: Status::ok(),
        }
    }


}
