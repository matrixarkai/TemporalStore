// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! SingleNodeMeta server/proxy/namespace registration + heartbeat, extracted from meta.rs.

use super::*;

impl SingleNodeMeta {
    pub fn register_server(&self, request: RegisterServerRequest) -> AckResponse {
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
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
            // A conviction the convicted node can undo by asking is not a
            // conviction. The freeze cooldown alone does not cover this: it
            // defaults to zero, so re-registering immediately returns the server
            // to Normal and the failure detector's decision is erased by the
            // very node it was about. An operator unfreeze is the way back.
            if existing.state == MetaEntityState::Frozen
                && existing.freeze_reason.is_conviction()
                && self.forbid_self_clearing_conviction
            {
                return AckResponse {
                    status: Status::error(
                        "conviction_requires_unfreeze",
                        format!(
                            "server was frozen by the metaserver ({}); an operator must unfreeze it",
                            existing.freeze_reason.as_str()
                        ),
                    ),
                };
            }
        }
        let now = now_ms();
        let server_addr = request.server_addr.clone();
        state.servers.insert(
            server_addr.clone(),
            ServerMetaInfo {
                freeze_reason: FreezeReason::Unspecified,
                server_addr: request.server_addr,
                node_id: request.node_id,
                location: request.location,
                state: MetaEntityState::Normal,
                last_heartbeat_ms: now,
                frozen_since_ms: 0,
                freeze_cooldown_until_ms: 0,
                boot_time_ms: 0,
                // Registration is the server declaring a fresh identity, so any
                // previous reboot verdict is cleared and the anchor is re-taken
                // from its next heartbeat.
                reported_boot_time_ms: 0,
                reboot_detected: false,
                reports_shard_states: false,
                binary_version: request.binary_version,
                shard_loads: Vec::new(),
                shard_stat_loads: Vec::new(),
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

    /// Relabel a registered server's location in place.
    ///
    /// Until now `location` could only be set at registration, so correcting a
    /// mislabelled node meant making it re-register -- which resets its
    /// heartbeat timestamp, its reported shard states, its runtime load and its
    /// freeze bookkeeping, and is refused outright while the node is in freeze
    /// cooldown. That is a disruptive way to fix a label, and it needs the
    /// datanode's cooperation, so an operator could not do it at all.
    ///
    /// The label is not cosmetic: since locations became hierarchical they drive
    /// replica spread and table pinning, so a wrong one actively degrades
    /// placement. This changes the label and nothing else, and bumps the
    /// topology version so clients pick up the placement that follows from it.
    pub fn update_server(&self, request: UpdateServerRequest) -> AckResponse {
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
        self.record_mutation(MetaMutation::UpdateServer(request.clone()));
        self.apply_update_server(request)
    }

    pub(super) fn apply_update_server(&self, request: UpdateServerRequest) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        let Some(server) = state.servers.get(&request.server_addr) else {
            return AckResponse {
                status: Status::error("server_not_found", "server not found"),
            };
        };
        // Matches the reference: a server that is not serving is not relabelled.
        // Its placement is not being consulted anyway, and an operator who wants
        // to relabel a frozen node can unfreeze it first.
        if server.state != MetaEntityState::Normal {
            return AckResponse {
                status: Status::error(
                    "resource_frozen",
                    "only a serving server can be relabelled",
                ),
            };
        }
        if server.location == request.location {
            return AckResponse {
                status: Status::error("not_modified", "server location is unchanged"),
            };
        }

        let previous = server.location.clone();
        state
            .servers
            .get_mut(&request.server_addr)
            .expect("server exists after validation")
            .location = request.location.clone();
        // Placement is derived from location on every topology read, so bumping
        // the version is what makes the new label take effect for clients.
        record_topology_event(
            &mut state,
            "update_server",
            format!("server:{}", request.server_addr),
            format!("location={},previous_location={previous}", request.location),
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
        // Reboot detection. The metaserver anchors on the first non-zero boot
        // time a registered server reports; a later heartbeat claiming a
        // different one means the process restarted in place. That matters even
        // though the heartbeats never stopped: a restarted datanode has dropped
        // every shard the metaserver still believes it is serving, so routing to
        // it returns misses until it reloads. Without this the restart is
        // invisible - the old code simply overwrote boot_time_ms.
        //
        // The verdict is sticky. It clears when the server registers again,
        // which is how a datanode announces it is ready to be trusted.
        let rebooted = if server.reported_boot_time_ms == 0 {
            server.reported_boot_time_ms = request.boot_time_ms;
            false
        } else {
            request.boot_time_ms != 0
                && request.boot_time_ms != server.reported_boot_time_ms
                && !server.reboot_detected
        };
        if rebooted {
            server.reboot_detected = true;
        }
        server.boot_time_ms = request.boot_time_ms;
        if !request.binary_version.is_empty() {
            server.binary_version = request.binary_version;
        }
        server.shard_loads = request.shard_loads;
        server.shard_stat_loads = request.shard_stat_loads;
        server.runtime_load = request.runtime_load;
        // Sticky: once a server has been seen reporting shard states, a later
        // empty report is real information (it dropped everything) rather than
        // an old build that never sends them.
        server.reports_shard_states =
            server.reports_shard_states || !request.shard_states.is_empty();
        server.shard_states = request.shard_states;
        let server_state = server.state.as_str().to_string();
        let anchored = server.reported_boot_time_ms;
        if rebooted {
            record_topology_event(
                &mut state,
                "server_reboot_detected",
                format!("server:{}", request.server_addr),
                format!(
                    "anchored_boot_time_ms={anchored},reported_boot_time_ms={}",
                    request.boot_time_ms
                ),
            );
        }
        ServerHeartbeatResponse {
            status: Status::ok(),
            forbid_auto_register: false,
            topology_version,
            server_state,
        }
    }

    pub fn register_proxy(&self, request: RegisterProxyRequest) -> AckResponse {
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
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
            if existing.state == MetaEntityState::Frozen
                && existing.freeze_reason.is_conviction()
                && self.forbid_self_clearing_conviction
            {
                return AckResponse {
                    status: Status::error(
                        "conviction_requires_unfreeze",
                        format!(
                            "proxy was frozen by the metaserver ({}); an operator must unfreeze it",
                            existing.freeze_reason.as_str()
                        ),
                    ),
                };
            }
        }
        state.proxies.insert(
            request.proxy_addr.clone(),
            ProxyMetaInfo {
                freeze_reason: FreezeReason::Unspecified,
                proxy_addr: request.proxy_addr,
                namespace: request.namespace,
                location: request.location,
                state: MetaEntityState::Normal,
                config_version: request.config_version,
                last_heartbeat_ms: now_ms(),
                frozen_since_ms: 0,
                freeze_cooldown_until_ms: 0,
                binary_version: request.binary_version,
                boot_time_ms: 0,
                restart_count: 0,
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
        // A changed boot time on an address we already know means the proxy restarted
        // in place. Heartbeats never stopped, so this is the only signal that its route
        // cache and config were reset underneath us.
        if request.boot_time_ms != 0 {
            if proxy.boot_time_ms != 0 && proxy.boot_time_ms != request.boot_time_ms {
                proxy.restart_count = proxy.restart_count.saturating_add(1);
            }
            proxy.boot_time_ms = request.boot_time_ms;
        }
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
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
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
