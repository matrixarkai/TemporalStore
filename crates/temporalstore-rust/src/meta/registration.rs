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
        self.counters.server_register_total.fetch_add(1, Ordering::Relaxed);
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
                reported_record_count: 0,
                reported_storage_bytes: 0,
                load_key_count: 0,
                load_memory_bytes: 0,
                worst_shard_state_penalty: 0,
                freeze_reason: FreezeReason::Unspecified,
                server_addr: request.server_addr,
                node_id: request.node_id,
                location: request.location,
                // Re-declared on every registration rather than merged: a
                // machine that came back with different hardware is describing
                // itself now, not amending what it said before.
                numa_nodes: request.numa_nodes,
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
        // Coming back clears the drop clock. Without this the tombstone
        // outlives the drop it recorded: `stamp_dropped_since` keeps the first
        // time it is given, deliberately, so that re-dropping an already
        // dropped resource cannot restart the clock -- and a resource dropped,
        // revived, and dropped again months later would inherit the original
        // time and be collected on the next round with no grace at all.
        stamp_dropped_since(
            &mut state,
            &dropped_key("server", &server_addr),
            MetaEntityState::Normal,
            now,
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
        self.counters.server_heartbeat_total.fetch_add(1, Ordering::Relaxed);
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
        // Summarised here, where the lists change, so that the read path does
        // not have to walk them.
        server.load_key_count = server.shard_loads.iter().map(|load| load.key_count).sum();
        server.load_memory_bytes = server
            .shard_loads
            .iter()
            .map(|load| load.memory_bytes)
            .sum();
        server.reported_record_count = server
            .shard_states
            .iter()
            .map(|reported| reported.total_records as u64)
            .sum();
        server.reported_storage_bytes = server
            .shard_states
            .iter()
            .map(|reported| reported.storage_bytes as u64)
            .sum();
        server.worst_shard_state_penalty = server
            .shard_states
            .iter()
            .map(|state| placement_shard_state_penalty(&state.serving_state))
            .max()
            .unwrap_or_default();
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
        self.counters.proxy_register_total.fetch_add(1, Ordering::Relaxed);
        let proxy_addr = request.proxy_addr.clone();
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
                group: String::new(),
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
        // Coming back clears the drop clock. Without this the tombstone
        // outlives the drop it recorded: `stamp_dropped_since` keeps the first
        // time it is given, deliberately, so that re-dropping an already
        // dropped resource cannot restart the clock -- and a resource dropped,
        // revived, and dropped again months later would inherit the original
        // time and be collected on the next round with no grace at all.
        stamp_dropped_since(
            &mut state,
            &dropped_key("proxy", &proxy_addr),
            MetaEntityState::Normal,
            now_ms(),
        );
        record_topology_event(
            &mut state,
            "register_proxy",
            format!("proxy:{proxy_addr}"),
            "state=normal",
        );
        AckResponse {
            status: Status::ok(),
        }
    }

    pub fn proxy_heartbeat(&self, request: ProxyHeartbeatRequest) -> ProxyHeartbeatResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        self.counters.proxy_heartbeat_total.fetch_add(1, Ordering::Relaxed);
        let Some(proxy) = state.proxies.get_mut(&request.proxy_addr) else {
            return ProxyHeartbeatResponse {
                status: Status::error("not_found", "proxy not found"),
                config_changed: false,
                namespace: String::new(),
                config_version: 0,
                serving_mode: String::new(),
                drop_percent: None,
            };
        };
        if proxy.state == MetaEntityState::Frozen {
            return ProxyHeartbeatResponse {
                status: Status::error("resource_frozen", "proxy frozen"),
                config_changed: true,
                namespace: proxy.namespace.clone(),
                config_version: proxy.config_version,
                serving_mode: proxy_serving_mode_for_state(proxy.state).to_string(),
                drop_percent: None,
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
        let proxy_addr = proxy.proxy_addr.clone();
        let fallback_namespace = proxy.namespace.clone();
        let fallback_version = proxy.config_version;
        let stale_state = proxy.state != MetaEntityState::Normal;
        // The attached group is the authority on what this proxy serves, so the
        // heartbeat is where a reassignment -- or a release back to idle --
        // reaches the proxy.
        let served = Self::proxy_group_config(
            &state,
            &proxy_addr,
            &request.namespace,
            request.config_version,
        );
        let (group_changed, namespace, config_version) =
            (served.changed, served.namespace, served.config_version);
        let attached = state
            .proxies
            .get(&proxy_addr)
            .map(|proxy| !proxy.group.is_empty())
            .unwrap_or(false);
        let (namespace, config_version) = if attached {
            (namespace, config_version)
        } else {
            // No group: fall back to whatever the proxy was configured with, so
            // a deployment that does not use groups behaves exactly as before.
            (fallback_namespace.clone(), fallback_version)
        };
        let config_changed = if attached {
            group_changed
        } else {
            fallback_namespace != request.namespace
                || fallback_version > request.config_version
                || stale_state
        };
        ProxyHeartbeatResponse {
            status: Status::ok(),
            config_changed,
            namespace,
            config_version,
            serving_mode,
            // What the group asks its proxies to shed, and nothing at all when this proxy
            // belongs to no group.
            //
            // The unattached branch used to send 0, which is not the same statement: the
            // proxy applied it, so a proxy configured to shed through its own /config had
            // that wiped by every heartbeat. A deployment that uses no groups is entirely
            // unattached proxies, and namespace and config_version two branches above
            // already fall back to local configuration for exactly that case -- this now
            // says the same thing the same way.
            drop_percent: if attached {
                Some(served.drop_percent)
            } else {
                None
            },
        }
    }

    pub fn add_namespace(&self, request: AddNamespaceRequest) -> AckResponse {
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
        if let Some(status) =
            self.admission_refusal(&MetaMutation::AddNamespace(request.clone()))
        {
            return AckResponse { status };
        }
        self.record_mutation(MetaMutation::AddNamespace(request.clone()));
        self.apply_add_namespace(request)
    }

    pub(super) fn apply_add_namespace(&self, request: AddNamespaceRequest) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        self.counters.namespace_create_total.fetch_add(1, Ordering::Relaxed);
        let namespace = request.namespace;
        let created = !state.namespaces.contains_key(&namespace);
        state
            .namespaces
            .entry(namespace.clone())
            .or_insert(MetaEntityState::Normal);
        if created {
            record_topology_event(
                &mut state,
                "add_namespace",
                format!("namespace:{namespace}"),
                "state=normal",
            );
        }
        AckResponse {
            status: Status::ok(),
        }
    }

    /// Stop serving everything in a namespace.
    ///
    /// The blast radius an operator reaches for is usually a tenant, not one
    /// table: freezing a namespace table by table takes as many calls as there
    /// are tables, and races with any table created meanwhile.
    pub fn freeze_namespace(&self, request: AddNamespaceRequest) -> AckResponse {
        self.set_namespace_state(request, MetaEntityState::Frozen)
    }

    /// Return a namespace to service. Also revives a dropped one, which is what
    /// makes dropping recoverable up until retention forgets it.
    pub fn unfreeze_namespace(&self, request: AddNamespaceRequest) -> AckResponse {
        self.set_namespace_state(request, MetaEntityState::Normal)
    }

    /// Tombstone a namespace. Refused while it still holds a table that is not
    /// itself dropped, so dropping a namespace cannot strand one.
    pub fn drop_namespace(&self, request: AddNamespaceRequest) -> AckResponse {
        self.set_namespace_state(request, MetaEntityState::Dropped)
    }

    fn set_namespace_state(
        &self,
        request: AddNamespaceRequest,
        next: MetaEntityState,
    ) -> AckResponse {
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
        // One write lock across the check, the record and the apply.
        //
        // These checks used to run under a read lock that was released before
        // the change was applied. A table created in that window was stranded:
        // the namespace reached Dropped with a live table still inside it,
        // which is exactly what the emptiness check exists to prevent. The
        // metaserver serves each connection on its own thread, so `drop` and
        // `add_table` arriving together is ordinary traffic, not a corner.
        //
        // Holding the lock across `record_mutation` means one fsync with
        // readers blocked. That is affordable here and nowhere else: the only
        // callers are freeze, unfreeze and drop of a namespace -- operator
        // actions, never a background loop and never per-request.
        let mut state = self.inner.write().expect("meta lock poisoned");
        let Some(current) = state.namespaces.get(&request.namespace).copied() else {
            return AckResponse {
                status: Status::error("namespace_not_found", "namespace not found"),
            };
        };
        if current == next {
            return AckResponse {
                status: Status::error("not_modified", "namespace state is unchanged"),
            };
        }
        // The judgement the propose path also applies, read off the state this
        // thread already holds. The `&self` form takes its own read lock and
        // would deadlock under the write lock held here.
        if let Some(status) = Self::admission_refusal_in(
            &state,
            &MetaMutation::SetNamespaceState(request.clone(), next),
        ) {
            return AckResponse { status };
        }
        // Recorded before the state moves, so a crash between the two replays
        // the change rather than losing it. `record_mutation` does not touch
        // `self.inner`, so holding the lock here cannot deadlock. It answers
        // with the time it recorded, and the drop clock is stamped from that
        // rather than from now, so a replay stamps what the log says.
        let at_ms = self.record_mutation(MetaMutation::SetNamespaceState(request.clone(), next));
        Self::apply_namespace_state_locked(&mut state, &request, next, at_ms)
    }

    pub(crate) fn apply_set_namespace_state(
        &self,
        request: AddNamespaceRequest,
        next: MetaEntityState,
        at_ms: u64,
    ) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        Self::apply_namespace_state_locked(&mut state, &request, next, at_ms)
    }

    /// The state change itself, for a caller that already holds the write lock.
    ///
    /// Split out so the guarded path can check and apply without letting go in
    /// between, while replay keeps entering through
    /// [`Self::apply_set_namespace_state`] and reapplying unconditionally.
    fn apply_namespace_state_locked(
        state: &mut MetaState,
        request: &AddNamespaceRequest,
        next: MetaEntityState,
        at_ms: u64,
    ) -> AckResponse {
        let Some(current) = state.namespaces.get_mut(&request.namespace) else {
            return AckResponse {
                status: Status::error("namespace_not_found", "namespace not found"),
            };
        };
        *current = next;
        stamp_dropped_since(
            state,
            &dropped_key("namespace", &request.namespace),
            next,
            at_ms,
        );
        // Topology is derived on read, so the version bump is what makes clients
        // notice that a namespace stopped, or resumed, serving.
        record_topology_event(
            state,
            "namespace_state",
            format!("namespace:{}", request.namespace),
            format!("state={}", next.as_str()),
        );
        AckResponse {
            status: Status::ok(),
        }
    }

}
