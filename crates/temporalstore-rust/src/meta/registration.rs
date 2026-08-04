//! SingleNodeMeta server/proxy/namespace registration + heartbeat, extracted from meta.rs.

use super::*;

impl SingleNodeMeta {
    pub fn register_server(&self, request: RegisterServerRequest) -> AckResponse {
        self.record_mutation(MetaMutation::RegisterServer(request.clone()));
        self.apply_register_server(request)
    }

    pub(super) fn apply_register_server(&self, request: RegisterServerRequest) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        state.counters.server_register_total += 1;
        if let Some(existing) = state.servers.get(&request.server_addr) {
            let now = now_ms();
            if existing.state == MetaEntityState::Frozen && existing.freeze_cooldown_until_ms > now
            {
                return AckResponse {
                    status: Status::error("resource_frozen", "server is in freeze cooldown"),
                };
            }
        }
        let now = now_ms();
        let server_addr = request.server_addr.clone();
        state.servers.insert(
            server_addr.clone(),
            ServerMetaInfo {
                server_addr: request.server_addr,
                node_id: request.node_id,
                location: request.location,
                state: MetaEntityState::Normal,
                last_heartbeat_ms: now,
                frozen_since_ms: 0,
                freeze_cooldown_until_ms: 0,
                boot_time_ms: 0,
                binary_version: request.binary_version,
                shard_loads: Vec::new(),
                partition_loads: Vec::new(),
                runtime_load: ServerRuntimeLoad::default(),
                shard_states: Vec::new(),
            },
        );
        record_topology_event(
            &mut state,
            "register_server",
            format!("server:{server_addr}"),
            "state=normal",
        );
        AckResponse {
            status: Status::ok(),
        }
    }

    pub fn server_heartbeat(&self, request: ServerHeartbeatRequest) -> ServerHeartbeatResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        state.counters.server_heartbeat_total += 1;
        let topology_version = state.topology_version;
        let Some(server) = state.servers.get_mut(&request.server_addr) else {
            return ServerHeartbeatResponse {
                status: Status::error("not_found", "server not found"),
                forbid_auto_register: false,
                topology_version,
                server_state: "unknown".to_string(),
            };
        };
        if server.state == MetaEntityState::Frozen {
            return ServerHeartbeatResponse {
                status: Status::error("resource_frozen", "server frozen"),
                forbid_auto_register: true,
                topology_version,
                server_state: MetaEntityState::Frozen.as_str().to_string(),
            };
        }
        server.last_heartbeat_ms = now_ms();
        server.boot_time_ms = request.boot_time_ms;
        if !request.binary_version.is_empty() {
            server.binary_version = request.binary_version;
        }
        server.shard_loads = request.shard_loads;
        server.partition_loads = request.partition_loads;
        server.runtime_load = request.runtime_load;
        server.shard_states = request.shard_states;
        ServerHeartbeatResponse {
            status: Status::ok(),
            forbid_auto_register: false,
            topology_version,
            server_state: server.state.as_str().to_string(),
        }
    }

    pub fn register_proxy(&self, request: RegisterProxyRequest) -> AckResponse {
        self.record_mutation(MetaMutation::RegisterProxy(request.clone()));
        self.apply_register_proxy(request)
    }

    pub(super) fn apply_register_proxy(&self, request: RegisterProxyRequest) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        state.counters.proxy_register_total += 1;
        if let Some(existing) = state.proxies.get(&request.proxy_addr) {
            let now = now_ms();
            if existing.state == MetaEntityState::Frozen && existing.freeze_cooldown_until_ms > now
            {
                return AckResponse {
                    status: Status::error("resource_frozen", "proxy is in freeze cooldown"),
                };
            }
        }
        state.proxies.insert(
            request.proxy_addr.clone(),
            ProxyMetaInfo {
                proxy_addr: request.proxy_addr,
                namespace: request.namespace,
                location: request.location,
                state: MetaEntityState::Normal,
                config_version: request.config_version,
                last_heartbeat_ms: now_ms(),
                frozen_since_ms: 0,
                freeze_cooldown_until_ms: 0,
                binary_version: request.binary_version,
            },
        );
        AckResponse {
            status: Status::ok(),
        }
    }

    pub fn proxy_heartbeat(&self, request: ProxyHeartbeatRequest) -> ProxyHeartbeatResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        state.counters.proxy_heartbeat_total += 1;
        let Some(proxy) = state.proxies.get_mut(&request.proxy_addr) else {
            return ProxyHeartbeatResponse {
                status: Status::error("not_found", "proxy not found"),
                config_changed: false,
                namespace: String::new(),
                config_version: 0,
                serving_mode: String::new(),
                drop_percent: 0,
            };
        };
        if proxy.state == MetaEntityState::Frozen {
            return ProxyHeartbeatResponse {
                status: Status::error("resource_frozen", "proxy frozen"),
                config_changed: true,
                namespace: proxy.namespace.clone(),
                config_version: proxy.config_version,
                serving_mode: proxy_serving_mode_for_state(proxy.state).to_string(),
                drop_percent: 0,
            };
        }
        proxy.last_heartbeat_ms = now_ms();
        if !request.binary_version.is_empty() {
            proxy.binary_version = request.binary_version;
        }
        let serving_mode = proxy_serving_mode_for_state(proxy.state).to_string();
        let config_changed = proxy.namespace != request.namespace
            || proxy.config_version > request.config_version
            || proxy.state != MetaEntityState::Normal;
        ProxyHeartbeatResponse {
            status: Status::ok(),
            config_changed,
            namespace: proxy.namespace.clone(),
            config_version: proxy.config_version,
            serving_mode,
            drop_percent: 0,
        }
    }

    pub fn add_namespace(&self, request: AddNamespaceRequest) -> AckResponse {
        self.record_mutation(MetaMutation::AddNamespace(request.clone()));
        self.apply_add_namespace(request)
    }

    pub(super) fn apply_add_namespace(&self, request: AddNamespaceRequest) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        state.counters.namespace_create_total += 1;
        state
            .namespaces
            .entry(request.namespace)
            .or_insert(MetaEntityState::Normal);
        AckResponse {
            status: Status::ok(),
        }
    }

}
