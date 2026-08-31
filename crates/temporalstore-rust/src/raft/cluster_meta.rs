// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! MetaRaftCluster methods, split from raft.rs.
use super::*;

impl MetaRaftCluster {
    /// Local metaserver Raft fixture for unit tests and validation harnesses.
    ///
    /// Production metaserver Raft must use the networked production runtime.
    pub fn new(node_ids: impl IntoIterator<Item = RaftNodeId>) -> Self {
        Self::new_with_config(node_ids, RaftConfig::default())
            .expect("default raft config must be valid")
    }

    /// Local metaserver Raft fixture with explicit config for tests/harnesses.
    pub fn new_with_config(
        node_ids: impl IntoIterator<Item = RaftNodeId>,
        config: RaftConfig,
    ) -> Result<Self, RaftError> {
        config
            .validate()
            .map_err(|err| RaftError::InvalidConfig(err.to_string()))?;
        let mut nodes = BTreeMap::new();
        let mut iter = node_ids.into_iter();
        let leader_id = iter.next().unwrap_or(1);
        nodes.insert(leader_id, new_meta_node(leader_id, RaftRole::Leader));
        for node_id in iter {
            nodes.insert(node_id, new_meta_node(node_id, RaftRole::Follower));
        }
        Ok(Self {
            inner: Arc::new(RwLock::new(MetaRaftClusterInner {
                leader_id,
                nodes,
                config,
            })),
        })
    }

    /// Apply the conviction lock to every node's metadata.
    ///
    /// `TS_META_FORBID_SELF_CLEARING_CONVICTION` was read after `from_env` had
    /// already returned the raft backend, so it reached the single-node
    /// metaserver and nothing else. The checks that consult it run on these
    /// nodes, against a flag that was always false.
    pub fn set_conviction_lock(&self, forbid: bool) {
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        for node in inner.nodes.values_mut() {
            node.meta.set_conviction_lock(forbid);
        }
    }

    pub fn propose(&self, command: MetaCommand) -> Result<(), RaftError> {
        self.propose_inner(command).map(|_| ())
    }

    pub fn propose_mutation(&self, mutation: MetaMutation) -> Result<Status, RaftError> {
        Ok(self
            .propose_inner(MetaCommand::ApplyMutation(mutation))?
            .unwrap_or_else(|| {
                // `apply_meta_committed` answers `None` only when it applied
                // nothing, which for a change just appended and committed means
                // the entry was skipped as already applied. Reporting that as
                // success is what let a numbering defect discard every metadata
                // change after a snapshot install while every caller was told
                // the change had been made.
                //
                // Not reachable today, and that is the point: this is the
                // difference between the next defect of that shape being loud
                // and being silent.
                Status::error(
                    "mutation_not_applied",
                    "the metaserver committed this change and applied nothing",
                )
            }))
    }

    pub fn register(&self, request: RegisterShardRequest) -> RegisterShardResponse {
        RegisterShardResponse {
            status: self.mutation_status(MetaMutation::RegisterShard(request)),
        }
    }

    pub fn get(&self, shard_id: ShardId) -> GetShardResponse {
        self.read_meta().map_or_else(
            |status| GetShardResponse {
                status,
                location: None,
            },
            |meta| meta.get(shard_id),
        )
    }

    pub fn register_server(&self, request: RegisterServerRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::RegisterServer(request)),
        }
    }

    pub fn server_heartbeat(&self, request: ServerHeartbeatRequest) -> ServerHeartbeatResponse {
        self.read_meta().map_or_else(
            |status| ServerHeartbeatResponse {
                status,
                forbid_auto_register: true,
                topology_version: 0,
                server_state: String::new(),
            },
            |meta| meta.server_heartbeat(request),
        )
    }

    pub fn register_proxy(&self, request: RegisterProxyRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::RegisterProxy(request)),
        }
    }

    pub fn proxy_heartbeat(&self, request: ProxyHeartbeatRequest) -> ProxyHeartbeatResponse {
        self.read_meta().map_or_else(
            |status| ProxyHeartbeatResponse {
                status,
                config_changed: false,
                namespace: String::new(),
                config_version: 0,
                serving_mode: "not_serving".to_string(),
                drop_percent: None,
            },
            |meta| meta.proxy_heartbeat(request),
        )
    }

    pub fn add_namespace(&self, request: AddNamespaceRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::AddNamespace(request)),
        }
    }

    pub fn add_table(&self, request: AddTableRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::AddTable(request)),
        }
    }

    pub fn delete_table(&self, request: DeleteTableRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::DeleteTable(request)),
        }
    }

    pub fn update_table(&self, request: UpdateTableRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::UpdateTable(request)),
        }
    }

    pub fn freeze_table(&self, request: DeleteTableRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::FreezeTable(request)),
        }
    }

    pub fn unfreeze_table(&self, request: DeleteTableRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::UnfreezeTable(request)),
        }
    }

    pub fn finish_load(&self, request: LoadFinishRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::FinishLoad(request)),
        }
    }

    pub fn publish_shard_snapshot(&self, request: PublishShardSnapshotRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::PublishShardSnapshot(request)),
        }
    }

    pub fn update_server(&self, request: crate::meta::UpdateServerRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::UpdateServer(request)),
        }
    }

    pub fn notify_server_stop(&self, request: crate::meta::NotifyStopRequest) -> AckResponse {
        let serving = self
            .list_servers()
            .servers
            .into_iter()
            .find(|server| server.server_addr == request.endpoint);
        match serving {
            None => AckResponse {
                status: Status::error("server_not_found", "server not found"),
            },
            Some(server) if server.state != MetaEntityState::Normal => AckResponse {
                status: Status::error("not_modified", "server is already out of service"),
            },
            Some(_) => self.freeze_server(StateChangeRequest {
                endpoint: request.endpoint,
                freeze_cooldown_ms: 0,
                reason: crate::meta::FreezeReason::Stopping,
            }),
        }
    }

    pub fn notify_proxy_stop(&self, request: crate::meta::NotifyStopRequest) -> AckResponse {
        let serving = self
            .list_proxies()
            .proxies
            .into_iter()
            .find(|proxy| proxy.proxy_addr == request.endpoint);
        match serving {
            None => AckResponse {
                status: Status::error("proxy_not_found", "proxy not found"),
            },
            Some(proxy) if proxy.state != MetaEntityState::Normal => AckResponse {
                status: Status::error("not_modified", "proxy is already out of service"),
            },
            Some(_) => self.drop_proxy(StateChangeRequest {
                endpoint: request.endpoint,
                freeze_cooldown_ms: 0,
                reason: crate::meta::FreezeReason::Stopping,
            }),
        }
    }

    /// Mute or resume metadata change across the raft group.
    pub fn set_meta_change_muted(&self, muted: bool) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::SetMetaChangeMuted(muted)),
        }
    }

    pub fn freeze_namespace(&self, request: AddNamespaceRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::SetNamespaceState(
                request,
                MetaEntityState::Frozen,
            )),
        }
    }

    pub fn unfreeze_namespace(&self, request: AddNamespaceRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::SetNamespaceState(
                request,
                MetaEntityState::Normal,
            )),
        }
    }

    pub fn drop_namespace(&self, request: AddNamespaceRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::SetNamespaceState(
                request,
                MetaEntityState::Dropped,
            )),
        }
    }

    pub fn freeze_shard(&self, request: crate::meta::ShardStateRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::SetShardState(
                request,
                MetaEntityState::Frozen,
            )),
        }
    }

    pub fn unfreeze_shard(&self, request: crate::meta::ShardStateRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::SetShardState(
                request,
                MetaEntityState::Normal,
            )),
        }
    }

    pub fn drop_shard(&self, request: crate::meta::ShardStateRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::DropShard(request)),
        }
    }

    pub fn reserved_names(&self) -> crate::meta::ReservedNamesResponse {
        self.read_meta().map_or_else(
            |status| crate::meta::ReservedNamesResponse {
                status,
                reserved: crate::meta::ReservedNames::default(),
            },
            |meta| meta.reserved_names(),
        )
    }

    pub fn set_reserved_names(&self, reserved: crate::meta::ReservedNames) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::SetReservedNames(reserved)),
        }
    }

    pub fn topology_events(
        &self,
        request: crate::meta::TopologyEventsRequest,
    ) -> crate::meta::TopologyEventsResponse {
        self.read_meta().map_or_else(
            |status| crate::meta::TopologyEventsResponse {
                status,
                events: Vec::new(),
                oldest_retained_version: 0,
                missed_events: false,
            },
            |meta| meta.topology_events(request),
        )
    }

    pub fn freeze_server(&self, request: StateChangeRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::FreezeServer(request)),
        }
    }

    pub fn unfreeze_server(&self, request: StateChangeRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::UnfreezeServer(request)),
        }
    }

    pub fn drop_server(&self, request: StateChangeRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::DropServer(request)),
        }
    }

    pub fn put_proxy_group(&self, request: crate::meta::PutProxyGroupRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::PutProxyGroup(request)),
        }
    }

    pub fn drop_proxy_group(&self, request: crate::meta::DropProxyGroupRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::DropProxyGroup(request)),
        }
    }

    pub fn list_proxy_groups(&self) -> crate::meta::ListProxyGroupsResponse {
        self.read_meta().map_or_else(
            |status| crate::meta::ListProxyGroupsResponse {
                status,
                groups: Vec::new(),
            },
            |meta| meta.list_proxy_groups(),
        )
    }

    pub fn freeze_proxy(&self, request: StateChangeRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::FreezeProxy(request)),
        }
    }

    pub fn unfreeze_proxy(&self, request: StateChangeRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::UnfreezeProxy(request)),
        }
    }

    pub fn drop_proxy(&self, request: StateChangeRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::DropProxy(request)),
        }
    }

    pub fn freeze_stale_servers(&self, stale_after_ms: u64) -> StaleServerReport {
        let report = self.freeze_stale_resources_with_policy(
            stale_after_ms,
            SafeModePolicy {
                server_freeze_cooldown_ms: 0,
                proxy_freeze_cooldown_ms: 0,
            },
        );
        StaleServerReport {
            status: report.status,
            frozen_servers: report.frozen_servers,
        }
    }

    pub fn freeze_stale_resources_with_policy(
        &self,
        stale_after_ms: u64,
        policy: SafeModePolicy,
    ) -> StaleResourceReport {
        let now = current_time_ms();
        let servers = self.list_servers();
        if !servers.status.ok {
            return StaleResourceReport {
                status: servers.status,
                frozen_servers: Vec::new(),
                frozen_proxies: Vec::new(),
            };
        }
        let proxies = self.list_proxies();
        if !proxies.status.ok {
            return StaleResourceReport {
                status: proxies.status,
                frozen_servers: Vec::new(),
                frozen_proxies: Vec::new(),
            };
        }

        let mut frozen_servers = Vec::new();
        for server in servers.servers {
            if server.state == MetaEntityState::Normal
                && now.saturating_sub(server.last_heartbeat_ms) > stale_after_ms
            {
                let status = self.freeze_server(StateChangeRequest {
                    endpoint: server.server_addr.clone(),
                    freeze_cooldown_ms: policy.server_freeze_cooldown_ms,
                    reason: crate::meta::FreezeReason::Unresponsive,
                });
                if !status.status.ok {
                    return StaleResourceReport {
                        status: status.status,
                        frozen_servers,
                        frozen_proxies: Vec::new(),
                    };
                }
                frozen_servers.push(server.server_addr);
            }
        }

        let mut frozen_proxies = Vec::new();
        for proxy in proxies.proxies {
            if proxy.state == MetaEntityState::Normal
                && now.saturating_sub(proxy.last_heartbeat_ms) > stale_after_ms
            {
                let status = self.freeze_proxy(StateChangeRequest {
                    endpoint: proxy.proxy_addr.clone(),
                    freeze_cooldown_ms: policy.proxy_freeze_cooldown_ms,
                    reason: crate::meta::FreezeReason::Unresponsive,
                });
                if !status.status.ok {
                    return StaleResourceReport {
                        status: status.status,
                        frozen_servers,
                        frozen_proxies,
                    };
                }
                frozen_proxies.push(proxy.proxy_addr);
            }
        }

        StaleResourceReport {
            status: Status::ok(),
            frozen_servers,
            frozen_proxies,
        }
    }

    pub fn safe_mode_report(&self) -> SafeModeReport {
        self.read_meta().map_or_else(
            |status| SafeModeReport {
                status,
                blocked_servers: Vec::new(),
                blocked_proxies: Vec::new(),
                server_count: 0,
                proxy_count: 0,
            },
            |meta| meta.safe_mode_report(),
        )
    }

    pub fn list_shards(&self, request: crate::meta::ListShardsRequest) -> ListShardsResponse {
        self.read_meta().map_or_else(
            |status| ListShardsResponse {
                status,
                shards: Vec::new(),
                next_after_shard_id: None,
            },
            |meta| meta.list_shards(request),
        )
    }

    pub fn list_servers(&self) -> ListServersResponse {
        self.read_meta().map_or_else(
            |status| ListServersResponse {
                status,
                servers: Vec::new(),
            },
            |meta| meta.list_servers(),
        )
    }

    pub fn list_proxies(&self) -> ListProxiesResponse {
        self.read_meta().map_or_else(
            |status| ListProxiesResponse {
                status,
                proxies: Vec::new(),
            },
            |meta| meta.list_proxies(),
        )
    }

    pub fn list_namespaces(&self) -> ListNamespacesResponse {
        self.read_meta().map_or_else(
            |status| ListNamespacesResponse {
                status,
                namespaces: Vec::new(),
            },
            |meta| meta.list_namespaces(),
        )
    }

    pub fn list_tables(&self) -> ListTablesResponse {
        self.read_meta().map_or_else(
            |status| ListTablesResponse {
                status,
                tables: Vec::new(),
            },
            |meta| meta.list_tables(),
        )
    }

    pub fn get_table_topology(&self, request: GetTableTopologyRequest) -> TableTopologyResponse {
        self.read_meta().map_or_else(
            |status| TableTopologyResponse {
                status,
                table: None,
                shards: Vec::new(),
                unchanged: false,
            },
            |meta| meta.get_table_topology(request),
        )
    }

    pub fn info(&self) -> MetaInfo {
        self.read_meta().map_or_else(
            |status| MetaInfo {
                meta_change_muted: false,
                status,
                stats: MetaStats::default(),
                boot_time_ms: 0,
                durable_mutation_log: false,
            },
            |meta| meta.info(),
        )
    }

    pub fn metric_rows(&self) -> (Vec<crate::meta::ServerMetricRow>, Vec<crate::meta::ProxyMetricRow>) {
        self.read_meta()
            .map(|meta| meta.metric_rows())
            .unwrap_or_default()
    }

    pub fn stats(&self) -> MetaStats {
        self.read_meta()
            .map(|meta| meta.stats())
            .unwrap_or_else(|_| MetaStats::default())
    }

    pub fn preflight_report(&self) -> MetaPreflightReport {
        self.read_meta()
            .map(|meta| meta.preflight_report())
            .unwrap_or_else(|status| MetaPreflightReport {
                status,
                stats: MetaStats::default(),
                normal_servers: 0,
                frozen_servers: 0,
                normal_proxies: 0,
                frozen_proxies: 0,
                dropped_tables: 0,
                shard_routes: 0,
                degraded_reasons: vec!["raft_read_unavailable".to_string()],
            })
    }

    pub fn topology_version_report(
        &self,
        request: TopologyVersionRequest,
    ) -> TopologyVersionReport {
        self.read_meta()
            .map(|meta| meta.topology_version_report(request.clone()))
            .unwrap_or_else(|status| TopologyVersionReport {
                status,
                current_topology_version: 0,
                old_topology_version: request.old_topology_version,
                unchanged: false,
                server_count: 0,
                proxy_count: 0,
                table_count: 0,
                shard_route_count: 0,
                normal_servers: 0,
                frozen_servers: 0,
                dropped_servers: 0,
                normal_proxies: 0,
                frozen_proxies: 0,
                dropped_proxies: 0,
                normal_tables: 0,
                frozen_tables: 0,
                dropped_tables: 0,
                changed_tables: Vec::new(),
                events: Vec::new(),
                event_history_truncated: false,
            })
    }

    pub(super) fn mutation_status(&self, mutation: MetaMutation) -> Status {
        // The mute is the incident lever: while it is set, the metaserver is
        // meant to refuse every recorded metadata mutation. That check lived
        // only in SingleNodeMeta's public methods, and this path proposes
        // straight past them -- so on a raft-backed metaserver, which is what a
        // real deployment runs, setting the mute changed nothing at all.
        //
        // Checked before proposing rather than while applying: replay has to
        // reapply what was already accepted, including changes recorded before
        // the mute was set.
        if !mutation.allowed_while_muted() {
            // An unreadable cluster is left to propose and fail on its own
            // terms. Refusing here would turn "cannot tell" into "muted".
            if self.peek_meta_change_muted() == Some(true) {
                return SingleNodeMeta::muted_status();
            }
        }
        // The same guards the public methods apply. `apply_mutation` dispatches
        // straight to the `apply_*` functions, so without this the propose path
        // goes around every one of them.
        if let Some(Some(status)) =
            self.with_readable_meta(|meta| meta.admission_refusal(&mutation))
        {
            return status;
        }
        self.propose_mutation(mutation)
            .unwrap_or_else(|err| Status::error("raft_error", err.to_string()))
    }

    /// Ask the readable replica a question without copying it.
    ///
    /// Deliberately not `read_meta()`: that clones the entire metadata state,
    /// and everything here runs on every mutation. `None` means no replica could
    /// answer, which is not the same as an answer of "no".
    pub(super) fn with_readable_meta<T>(
        &self,
        ask: impl FnOnce(&SingleNodeMeta) -> T,
    ) -> Option<T> {
        let inner = self.inner.read().expect("meta raft lock poisoned");
        let leader_commit_index = inner.nodes.get(&inner.leader_id)?.commit_index;
        inner
            .nodes
            .values()
            .filter(|node| node.alive && node.commit_index >= leader_commit_index)
            .min_by_key(|node| node.id)
            .map(|node| ask(&node.meta))
    }

    pub(super) fn peek_meta_change_muted(&self) -> Option<bool> {
        self.with_readable_meta(SingleNodeMeta::is_meta_change_muted)
    }

    pub(super) fn read_meta(&self) -> Result<SingleNodeMeta, Status> {
        let inner = self.inner.read().expect("meta raft lock poisoned");
        let leader_commit_index = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or_else(|| Status::error("leader_unavailable", "meta raft leader unavailable"))?
            .commit_index;
        inner
            .nodes
            .values()
            .filter(|node| node.alive && node.commit_index >= leader_commit_index)
            .min_by_key(|node| node.id)
            .map(|node| node.meta.clone())
            .ok_or_else(|| Status::error("leader_unavailable", "meta raft has no readable quorum"))
    }

    pub(super) fn propose_inner(&self, command: MetaCommand) -> Result<Option<Status>, RaftError> {
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        inner.ensure_live_leader()?;
        let entry_bytes = serde_json::to_vec(&command)
            .map(|bytes| bytes.len() as u64)
            .unwrap_or_default();
        if entry_bytes > inner.config.max_memory_replicate_log_bytes {
            return Err(RaftError::LogEntryTooLarge {
                bytes: entry_bytes,
                limit: inner.config.max_memory_replicate_log_bytes,
            });
        }
        let required = majority(inner.nodes.len());
        let live = inner.nodes.values().filter(|node| node.alive).count();
        if live < required {
            return Err(RaftError::NoMajority { live, required });
        }
        let leader_id = inner.leader_id;
        let leader = inner
            .nodes
            .get(&leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let entry = MetaLogEntry {
            term: leader.current_term,
            // Numbered from the log *or the installed snapshot*, whichever is
            // further along. Installing a meta snapshot truncates the log and
            // marks everything up to the snapshot applied, so taking the next
            // index from the log alone restarted numbering at 1 -- indices the
            // node had already applied. Every proposal after an install was
            // skipped as a duplicate, and `propose_mutation` turns "not applied"
            // into `Status::ok`, so the change was discarded and reported
            // successful.
            index: meta_node_last_log_or_snapshot_index(leader) + 1,
            command,
        };
        let mut replicated = 0;
        for node in inner.nodes.values_mut().filter(|node| node.alive) {
            append_meta_entry(node, entry.clone());
            replicated += 1;
        }
        if replicated < required {
            return Err(RaftError::NoMajority {
                live: replicated,
                required,
            });
        }
        let mut leader_status = None;
        for node in inner.nodes.values_mut().filter(|node| node.alive) {
            node.commit_index = entry.index;
            let status = apply_meta_committed(node);
            if node.id == leader_id {
                leader_status = status;
            }
        }
        Ok(leader_status)
    }

    pub fn get_shard_location(
        &self,
        node_id: RaftNodeId,
        shard_id: ShardId,
    ) -> Result<Option<ShardLocation>, RaftError> {
        let inner = self.inner.read().expect("meta raft lock poisoned");
        Ok(inner
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?
            .state
            .shards
            .get(&shard_id)
            .cloned())
    }

    pub fn get_shard_location_from_any_live(
        &self,
        shard_id: ShardId,
    ) -> Result<Option<ShardLocation>, RaftError> {
        let inner = self.inner.read().expect("meta raft lock poisoned");
        let leader_commit_index = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?
            .commit_index;
        let node = inner
            .nodes
            .values()
            .filter(|node| node.alive && node.commit_index >= leader_commit_index)
            .min_by_key(|node| node.id)
            .ok_or(RaftError::LeaderUnavailable)?;
        Ok(node.state.shards.get(&shard_id).cloned())
    }

    pub fn leader_id(&self) -> RaftNodeId {
        self.inner
            .read()
            .expect("meta raft lock poisoned")
            .leader_id
    }

    pub fn transfer_leader(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        inner.ensure_live_leader()?;
        let leader_commit_index = inner.leader_commit_index();
        let candidate = inner
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        if !candidate.alive {
            return Err(RaftError::NodeNotFound(node_id));
        }
        if candidate.commit_index < leader_commit_index {
            return Err(RaftError::ReplicaLagging {
                replica_id: node_id,
                replica_commit_index: candidate.commit_index,
                leader_commit_index,
            });
        }
        inner.elect_leader(node_id)
    }

    pub fn read_index(&self, node_id: RaftNodeId) -> Result<ReadIndexResponse, RaftError> {
        let inner = self.inner.read().expect("meta raft lock poisoned");
        let status = inner.status();
        if !status.leader_lease_valid {
            return Err(RaftError::LeaderUnavailable);
        }
        let node = inner
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        if !node.alive {
            return Err(RaftError::NodeNotFound(node_id));
        }
        if node.commit_index < status.commit_index {
            return Err(RaftError::ReplicaLagging {
                replica_id: node_id,
                replica_commit_index: node.commit_index,
                leader_commit_index: status.commit_index,
            });
        }
        Ok(ReadIndexResponse {
            leader_id: inner.leader_id,
            node_id,
            term: status.current_term,
            read_index: status.commit_index,
        })
    }

    pub fn check_read(
        &self,
        node_id: RaftNodeId,
        options: RaftReadOptions,
    ) -> Result<ReadIndexResponse, RaftError> {
        let inner = self.inner.read().expect("meta raft lock poisoned");
        let node = inner
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        if !node.alive {
            return Err(RaftError::NodeNotFound(node_id));
        }
        if !options.enable_read_from_follower && node_id != inner.leader_id {
            return Err(RaftError::NotLeader { node_id });
        }
        drop(inner);
        match options.strategy {
            RaftReadStrategy::RelaxRead => {
                let status = self.status();
                Ok(ReadIndexResponse {
                    leader_id: status.leader_id,
                    node_id,
                    term: status.current_term,
                    read_index: self.commit_index(node_id)?,
                })
            }
            RaftReadStrategy::LeaseRead | RaftReadStrategy::ReadIndex => self.read_index(node_id),
        }
    }

    pub fn status(&self) -> RaftClusterStatus {
        self.inner.read().expect("meta raft lock poisoned").status()
    }

    pub fn config(&self) -> RaftConfig {
        self.inner
            .read()
            .expect("meta raft lock poisoned")
            .config
            .clone()
    }

    pub fn local_status(&self, node_id: RaftNodeId) -> Result<RaftNodeStatus, RaftError> {
        let inner = self.inner.read().expect("meta raft lock poisoned");
        let leader_commit_index = inner.leader_commit_index();
        inner
            .nodes
            .get(&node_id)
            .map(|node| meta_node_status(node, leader_commit_index))
            .ok_or(RaftError::NodeNotFound(node_id))
    }

    pub fn prometheus_metrics(&self) -> String {
        raft_status_prometheus("meta", self.status())
    }

    pub fn commit_index(&self, node_id: RaftNodeId) -> Result<u64, RaftError> {
        let inner = self.inner.read().expect("meta raft lock poisoned");
        Ok(inner
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?
            .commit_index)
    }

    pub fn set_alive(&self, node_id: RaftNodeId, alive: bool) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        let node = inner
            .nodes
            .get_mut(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        node.alive = alive;
        Ok(())
    }

    pub fn add_node(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        if inner.nodes.contains_key(&node_id) {
            return Err(RaftError::NodeAlreadyExists(node_id));
        }
        inner.ensure_live_leader()?;
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let mut node = new_meta_node(node_id, RaftRole::Follower);
        node.current_term = leader.current_term;
        install_meta_leader_snapshot_tail(
            &mut node,
            leader.installed_snapshot_index,
            leader.installed_snapshot_term,
            leader.log.clone(),
            leader.commit_index,
            leader.state.clone(),
        );
        inner.nodes.insert(node_id, node);
        Ok(())
    }

    pub fn add_node_safely(&self, node_id: RaftNodeId) -> Result<RaftScaleChangeReport, RaftError> {
        self.add_node(node_id)?;
        self.catch_up_live_followers()?;
        Ok(self.scale_change_report())
    }

    pub fn plan_membership_change(
        &self,
        new_voters: impl IntoIterator<Item = RaftNodeId>,
    ) -> Result<RaftMembershipChangePlan, RaftError> {
        let inner = self.inner.read().expect("meta raft lock poisoned");
        inner.plan_membership_change(new_voters)
    }

    pub fn apply_membership_change_safely(
        &self,
        new_voters: impl IntoIterator<Item = RaftNodeId>,
    ) -> Result<RaftMembershipChangeReport, RaftError> {
        let plan = self.plan_membership_change(new_voters)?;
        let joint_membership = JointConsensusMembership {
            old_voters: plan.old_voters.clone(),
            new_voters: plan.new_voters.clone(),
        };
        {
            let mut inner = self.inner.write().expect("meta raft lock poisoned");
            inner.ensure_live_leader()?;
            let leader = inner
                .nodes
                .get(&inner.leader_id)
                .ok_or(RaftError::LeaderUnavailable)?;
            let leader_term = leader.current_term;
            let leader_log = leader.log.clone();
            let leader_commit_index = leader.commit_index;
            let leader_state = leader.state.clone();
            let leader_snapshot_index = leader.installed_snapshot_index;
            let leader_snapshot_term = leader.installed_snapshot_term;
            let leader_meta = leader.meta.clone();
            for node_id in &plan.add_voters {
                if inner.nodes.contains_key(node_id) {
                    continue;
                }
                let mut node = new_meta_node(*node_id, RaftRole::Follower);
                node.current_term = leader_term;
                install_meta_leader_snapshot_tail(
                    &mut node,
                    leader_snapshot_index,
                    leader_snapshot_term,
                    leader_log.clone(),
                    leader_commit_index,
                    leader_state.clone(),
                );
                node.meta = leader_meta.clone();
                inner.nodes.insert(*node_id, node);
            }
        }
        let caught_up_voters = self.catch_up_live_followers()?;
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        for node_id in &plan.remove_voters {
            inner.remove_node_safely(*node_id)?;
        }
        let status = inner.status();
        let committed_membership = RaftMembership {
            shard_id: plan.shard_id,
            voters: inner.nodes.keys().copied().collect(),
            leader_id: status.leader_id,
        };
        Ok(RaftMembershipChangeReport {
            plan,
            joint_membership,
            committed_membership,
            caught_up_voters,
            leader_id: status.leader_id,
            commit_index: status.commit_index,
        })
    }

    pub fn create_snapshot(&self) -> Result<MetaRaftSnapshot, RaftError> {
        let inner = self.inner.read().expect("meta raft lock poisoned");
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .filter(|node| node.alive && node.role == RaftRole::Leader)
            .ok_or(RaftError::LeaderUnavailable)?;
        let last_included_term = leader
            .log
            .iter()
            .rev()
            .find(|entry| entry.index <= leader.commit_index)
            .map(|entry| entry.term)
            .unwrap_or(leader.current_term);
        Ok(MetaRaftSnapshot {
            last_included_term,
            last_included_index: leader.commit_index,
            state: leader.state.clone(),
        })
    }

    pub fn export_meta_snapshot(&self) -> Result<MetaSnapshot, RaftError> {
        let inner = self.inner.read().expect("meta raft lock poisoned");
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .filter(|node| node.alive && node.role == RaftRole::Leader)
            .ok_or(RaftError::LeaderUnavailable)?;
        Ok(leader.meta.export_snapshot())
    }

    pub fn install_meta_snapshot_on_live_nodes(
        &self,
        snapshot: MetaSnapshot,
    ) -> Result<(), RaftError> {
        let validated_meta = SingleNodeMeta::default();
        let status = validated_meta.install_snapshot(snapshot.clone()).status;
        if !status.ok {
            return Err(RaftError::InvalidConfig(status.message));
        }
        let route_state = MetaState {
            shards: snapshot
                .shards
                .iter()
                .map(|(id, location)| (*id, location.clone()))
                .collect(),
            scheduler_state: None,
        };
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .filter(|node| node.alive && node.role == RaftRole::Leader)
            .ok_or(RaftError::LeaderUnavailable)?;
        let raft_snapshot = MetaRaftSnapshot {
            last_included_term: leader.current_term,
            last_included_index: leader.commit_index,
            state: route_state,
        };
        for node in inner.nodes.values_mut().filter(|node| node.alive) {
            install_meta_snapshot_state(node, raft_snapshot.clone());
            // Installed into the node's own metadata rather than into a fresh
            // default that replaces it. A snapshot carries metadata, not the
            // configuration of the process holding it, and replacing the whole
            // meta discarded the conviction lock along with the event bus, the
            // metrics recorder and the counters -- the last three documented as
            // shared by clone, so every handle taken before the install was
            // quietly left writing to an orphan.
            let status = node.meta.install_snapshot(snapshot.clone()).status;
            if !status.ok {
                return Err(RaftError::InvalidConfig(status.message));
            }
        }
        Ok(())
    }

    pub fn maybe_trigger_snapshot(&self) -> Result<RaftSnapshotTriggerReport, RaftError> {
        let (should_trigger, report) = {
            let inner = self.inner.read().expect("meta raft lock poisoned");
            let leader = inner
                .nodes
                .get(&inner.leader_id)
                .filter(|node| node.alive && node.role == RaftRole::Leader)
                .ok_or(RaftError::LeaderUnavailable)?;
            let applied_index = leader
                .applied
                .iter()
                .next_back()
                .copied()
                .unwrap_or_default();
            let applied_log_bytes =
                meta_log_bytes_after(&leader.log, leader.installed_snapshot_index);
            let mut report = RaftSnapshotTriggerReport {
                triggered: false,
                reason: "below_threshold".to_string(),
                leader_id: inner.leader_id,
                applied_index,
                last_snapshot_index: leader.installed_snapshot_index,
                applied_log_bytes,
                max_applied_log_bytes: inner.config.max_applied_log_bytes,
            };
            if !inner.config.can_trigger_snapshot {
                report.reason = "disabled".to_string();
                return Ok(report);
            }
            if applied_index <= leader.installed_snapshot_index {
                report.reason = "no_new_applied_logs".to_string();
                return Ok(report);
            }
            if applied_log_bytes < inner.config.max_applied_log_bytes {
                return Ok(report);
            }
            report.triggered = true;
            report.reason = "applied_log_bytes_threshold".to_string();
            (true, report)
        };

        if should_trigger {
            let snapshot = self.create_snapshot()?;
            let mut inner = self.inner.write().expect("meta raft lock poisoned");
            for node in inner.nodes.values_mut().filter(|node| node.alive) {
                if snapshot.last_included_index >= node.commit_index {
                    install_meta_snapshot_state(node, snapshot.clone());
                }
            }
        }
        Ok(report)
    }

    pub fn install_snapshot(
        &self,
        node_id: RaftNodeId,
        snapshot: MetaRaftSnapshot,
    ) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        let node = inner
            .nodes
            .get_mut(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        if snapshot.last_included_index < node.commit_index {
            return Err(RaftError::StaleSnapshot {
                snapshot_index: snapshot.last_included_index,
                local_commit_index: node.commit_index,
            });
        }
        install_meta_snapshot_state(node, snapshot);
        Ok(())
    }

    pub fn remove_node(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        if inner.nodes.len() == 1 {
            return Err(RaftError::CannotRemoveLastNode);
        }
        inner
            .nodes
            .remove(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        if inner.leader_id == node_id {
            inner.promote_best_live_follower()?;
        }
        Ok(())
    }

    pub fn remove_node_safely(
        &self,
        node_id: RaftNodeId,
    ) -> Result<RaftScaleChangeReport, RaftError> {
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        inner.remove_node_safely(node_id)?;
        Ok(inner.scale_change_report())
    }

    pub fn catch_up(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let leader_log = leader.log.clone();
        let leader_commit_index = leader.commit_index;
        let leader_state = leader.state.clone();
        let leader_snapshot_index = leader.installed_snapshot_index;
        let leader_snapshot_term = leader.installed_snapshot_term;
        let node = inner
            .nodes
            .get_mut(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        install_meta_leader_snapshot_tail(
            node,
            leader_snapshot_index,
            leader_snapshot_term,
            leader_log,
            leader_commit_index,
            leader_state,
        );
        Ok(())
    }

    pub fn catch_up_live_followers(&self) -> Result<Vec<RaftNodeId>, RaftError> {
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        inner.catch_up_live_followers()
    }

    pub fn failover_primary(&self) -> Result<RaftFailoverReport, RaftError> {
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        let old_leader_id = inner.leader_id;
        if inner
            .nodes
            .get(&old_leader_id)
            .map(|node| node.alive && node.role == RaftRole::Leader)
            .unwrap_or(false)
        {
            return Ok(inner.failover_report(old_leader_id));
        }
        inner.promote_best_live_follower()?;
        Ok(inner.failover_report(old_leader_id))
    }

    pub fn replication_health(&self, max_allowed_lag: u64) -> RaftReplicationHealth {
        replication_health_from_status(self.status(), max_allowed_lag)
    }

    pub fn apply_health(&self, max_allowed_apply_lag: u64) -> RaftApplyHealth {
        raft_apply_health_from_status(self.status(), max_allowed_apply_lag)
    }

    pub fn scale_change_report(&self) -> RaftScaleChangeReport {
        self.inner
            .read()
            .expect("meta raft lock poisoned")
            .scale_change_report()
    }
}
