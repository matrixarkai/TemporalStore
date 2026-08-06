//! SingleNodeMeta topology/listing/freeze/finish-load lifecycle methods, extracted from meta.rs.

use super::*;

impl SingleNodeMeta {
    pub fn get_table_topology(&self, request: GetTableTopologyRequest) -> TableTopologyResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        state.counters.topology_query_total += 1;
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
        if request.old_topology_version >= table.info.topology_version {
            return TableTopologyResponse {
                status: Status::ok(),
                table: Some(table.info.clone()),
                shards: Vec::new(),
                unchanged: true,
            };
        }
        let shards = build_shards(&state, &table.info);
        TableTopologyResponse {
            status: Status::ok(),
            table: Some(table.info.clone()),
            shards,
            unchanged: false,
        }
    }

    pub fn list_namespaces(&self) -> ListNamespacesResponse {
        let state = self.inner.read().expect("meta lock poisoned");
        let namespaces = state
            .namespaces
            .iter()
            .map(|(namespace, state_value)| NamespaceMetaInfo {
                namespace: namespace.clone(),
                table_count: state
                    .tables
                    .values()
                    .filter(|table| {
                        table.info.namespace == *namespace
                            && table.info.state != MetaEntityState::Dropped
                    })
                    .count(),
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
        let (stale_servers, stale_proxies) = {
            let state = self.inner.read().expect("meta lock poisoned");
            let stale_servers = state
                .servers
                .values()
                .filter(|server| {
                    server.state == MetaEntityState::Normal
                        && now.saturating_sub(server.last_heartbeat_ms) > stale_after_ms
                })
                .map(|server| server.server_addr.clone())
                .collect::<Vec<_>>();
            let stale_proxies = state
                .proxies
                .values()
                .filter(|proxy| {
                    proxy.state == MetaEntityState::Normal
                        && now.saturating_sub(proxy.last_heartbeat_ms) > stale_after_ms
                })
                .map(|proxy| proxy.proxy_addr.clone())
                .collect::<Vec<_>>();
            (stale_servers, stale_proxies)
        };

        let mut frozen_servers = Vec::new();
        for endpoint in stale_servers {
            let response = self.freeze_server(StateChangeRequest {
                endpoint: endpoint.clone(),
                freeze_cooldown_ms: policy.server_freeze_cooldown_ms,
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

        let mut frozen_proxies = Vec::new();
        for endpoint in stale_proxies {
            let response = self.freeze_proxy(StateChangeRequest {
                endpoint: endpoint.clone(),
                freeze_cooldown_ms: policy.proxy_freeze_cooldown_ms,
            });
            if !response.status.ok {
                return StaleResourceReport {
                    status: response.status,
                    frozen_servers,
                    frozen_proxies,
                };
            }
            frozen_proxies.push(endpoint);
        }

        StaleResourceReport {
            status: Status::ok(),
            frozen_servers,
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

    pub fn drop_server(&self, request: StateChangeRequest) -> AckResponse {
        self.set_server_state(request, MetaEntityState::Dropped)
    }

    pub fn freeze_proxy(&self, request: StateChangeRequest) -> AckResponse {
        self.set_proxy_state(request, MetaEntityState::Frozen)
    }

    pub fn drop_proxy(&self, request: StateChangeRequest) -> AckResponse {
        self.set_proxy_state(request, MetaEntityState::Dropped)
    }

    pub fn finish_load(&self, request: LoadFinishRequest) -> AckResponse {
        self.record_mutation(MetaMutation::FinishLoad(request.clone()));
        self.apply_finish_load(request)
    }

    pub(super) fn apply_finish_load(&self, request: LoadFinishRequest) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        state.counters.load_finish_total += 1;
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
