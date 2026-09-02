// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! TemporalStoreClient meta topology sync + route refresh/invalidation + reports, split from client.rs.
use super::*;
use crate::meta::TableServingField;

impl TemporalStoreClient {
    pub fn sync_table_topology(
        &self,
        namespace: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Result<TableOptions, ClientError> {
        let namespace = namespace.into();
        let table_name = table_name.into();
        self.ensure_meta_sync_table_state(&namespace, &table_name);
        self.inner
            .stats
            .lock()
            .expect("client stats lock poisoned")
            .meta_sync_total += 1;
        let meta_addr = self
            .inner
            .options
            .meta_addr
            .as_ref()
            .ok_or_else(|| ClientError::Status("meta_addr is required".to_string()))?;
        let topology: TableTopologyResponse = match post_json_with_options_and_headers(
            meta_addr,
            "/tables/topology",
            &GetTableTopologyRequest {
                client_location: String::new(),
                namespace: namespace.clone(),
                table_name: table_name.clone(),
                old_topology_version: self.last_synced_topology_version(&namespace, &table_name),
            },
            &crate::meta::admin_auth_header(),
            self.inner.options.meta_sync_http_options(),
        ) {
            Ok(topology) => topology,
            Err(err) => {
                self.inner
                    .stats
                    .lock()
                    .expect("client stats lock poisoned")
                    .meta_sync_errors += 1;
                self.record_meta_sync_error(&namespace, &table_name, &err.to_string());
                return Err(err.into());
            }
        };
        if !topology.status.ok {
            self.inner
                .stats
                .lock()
                .expect("client stats lock poisoned")
                .meta_sync_errors += 1;
            self.record_meta_sync_error(&namespace, &table_name, &topology.status.message);
            return Err(ClientError::Status(topology.status.message));
        }
        let table = topology.table.ok_or_else(|| {
            self.record_meta_sync_error(&namespace, &table_name, "table topology missing");
            ClientError::Status("table topology missing".to_string())
        })?;
        // Asked for only when it is going to be used. Stamping routes needs the cluster's
        // topology version, and getting it is its own metaserver round-trip -- but an
        // unchanged reply installs no routes, so on that path the version is wanted for the
        // sync record alone, and the table's own version is already the right answer: the
        // metaserver has just confirmed it is current, which is why it answered unchanged.
        //
        // Asking anyway made every sync cost two round-trips instead of one, and since the
        // unchanged reply is the common case once a topology settles, that was the usual cost
        // rather than an occasional one.
        let route_topology_version = if topology.unchanged {
            table.topology_version
        } else {
            self.current_meta_topology_version()
                .unwrap_or(table.topology_version)
                .max(table.topology_version)
        };
        let serving_options = table.serving_options.clone();
        // Whether the table speaks for a field, or leaves it to this client's own
        // option. Asking the table settles it; the alternative -- inferring it from
        // "the value differs from the default" -- silently overrode any table that
        // chose a default value on purpose, which is exactly what `drop_percent: 0`
        // ("never shed this table") and `max_write_retries: 0` ("never retry a write
        // here") are.
        let table_decides = |field: TableServingField| serving_options.table_decides(field);
        let options = TableOptions {
            table_id: table.table_id,
            io_timeout_ms: if table_decides(TableServingField::IoTimeoutMs) {
                serving_options.io_timeout_ms
            } else {
                self.inner.options.io_timeout_ms
            },
            connect_timeout_ms: if table_decides(TableServingField::ConnectTimeoutMs) {
                serving_options.connect_timeout_ms
            } else {
                self.inner.options.connect_timeout_ms
            },
            continuous_failed_time_ms: if table_decides(TableServingField::ContinuousFailedTimeMs) {
                serving_options.continuous_failed_time_ms
            } else {
                TableOptions::default().continuous_failed_time_ms
            },
            first_shard_id: table.first_shard_id,
            shard_count: table.shard_count,
            partition_version: table.partition_version,
            pin_primary: serving_options.pin_primary,
            replica_read_policy: replica_read_policy_from_meta(
                &serving_options.replica_read_policy,
            ),
            preferred_location: if table_decides(TableServingField::PreferredLocation)
                && !serving_options.preferred_location.is_empty()
            {
                serving_options.preferred_location.clone()
            } else {
                self.inner.options.local_location.clone()
            },
            drop_percent: if table_decides(TableServingField::DropPercent) {
                serving_options.drop_percent.min(100)
            } else {
                self.inner.options.drop_percent.min(100)
            },
            max_read_retries: if table_decides(TableServingField::MaxReadRetries) {
                serving_options.max_read_retries as usize
            } else {
                self.inner.options.max_read_retries
            },
            max_write_retries: if table_decides(TableServingField::MaxWriteRetries) {
                serving_options.max_write_retries as usize
            } else {
                self.inner.options.max_write_retries
            },
            retry_backoff_ms: if table_decides(TableServingField::RetryBackoffMs) {
                serving_options.retry_backoff_ms
            } else {
                self.inner.options.retry_backoff_ms
            },
            ..TableOptions::default()
        };
        let table_key = table_combine_name(&namespace, &table_name);
        // Shards the topology names but cannot route, because it gives them no primary. This
        // happens while a primary is being elected. Their previous routes are deliberately
        // NOT discarded below: a snapshot taken mid-election should not destroy a route that
        // still works, and the old one either still serves or fails and gets refreshed.
        let unroutable: Vec<ShardId> = topology
            .shards
            .iter()
            .filter(|partition| partition.primary.is_none())
            .map(|partition| partition.shard_id)
            .collect();
        let routes = topology
            .shards
            .iter()
            .filter_map(|partition| {
                partition.primary.as_ref().map(|primary| {
                    (
                        partition.shard_id,
                        CachedRoute {
                            table_key: table_key.clone(),
                            partition_id: partition.shard_id,
                            start_bucket: partition.start_bucket,
                            end_bucket: partition.end_bucket,
                            partition_version: table.partition_version,
                            primary_addr: primary.clone(),
                            replica_addrs: partition
                                .replicas
                                .iter()
                                .filter(|replica| *replica != primary)
                                .cloned()
                                .collect(),
                            replica_endpoints: partition
                                .replica_endpoints
                                .iter()
                                .filter(|endpoint| endpoint.server_addr != *primary)
                                .cloned()
                                .collect(),
                            next_replica_index: std::sync::atomic::AtomicUsize::new(0),
                            fetched_at: Instant::now(),
                            topology_version: route_topology_version,
                            refresh_reason: "table_topology_sync".to_string(),
                        },
                    )
                })
            })
            .collect::<Vec<_>>();
        self.inner
            .tables
            .write()
            .expect("client table cache lock poisoned")
            .insert(table_key.clone(), options.clone());

        // "unchanged" means the metaserver did not rebuild the shard list because we already
        // have this version -- so the response carries NO shards, and running the route
        // surgery below against an empty list would delete every route this table has. The
        // serving options still came back and are applied above; the routes are already right.
        //
        // This has to be handled before the version is sent, not after: while the request was
        // hardcoded to version 0 the metaserver could never answer unchanged, which is the
        // only reason the wipe never happened.
        if topology.unchanged {
            self.record_meta_sync_success(
                &namespace,
                &table_name,
                route_topology_version,
                0,
            );
            return Ok(options);
        }

        let mut route_cache = self
            .inner
            .routes
            .write()
            .expect("client route cache lock poisoned");
        let last_shard_id = table
            .first_shard_id
            .saturating_add(table.shard_count.saturating_sub(1));
        route_cache.retain(|shard_id, route| {
            // Keep what the new topology cannot replace.
            if unroutable.contains(shard_id) {
                return true;
            }
            if route.table_key == table_key {
                return false;
            }
            *shard_id < table.first_shard_id || *shard_id > last_shard_id
        });
        for (shard_id, route) in routes {
            route_cache.insert(shard_id, route);
        }
        self.record_meta_sync_success(
            &namespace,
            &table_name,
            route_topology_version,
            unroutable.len() as u64,
        );
        Ok(options)
    }

    pub(super) fn current_meta_topology_version(&self) -> Option<u64> {
        let meta_addr = self.inner.options.meta_addr.as_ref()?;
        let topology = post_json_with_options_and_headers::<_, TopologyVersionReport>(
            meta_addr,
            "/meta/topology_version",
            &TopologyVersionRequest {
                old_topology_version: 0,
            },
            &crate::meta::admin_auth_header(),
            self.inner.options.meta_sync_http_options(),
        )
        .ok()?;
        topology
            .status
            .ok
            .then_some(topology.current_topology_version)
    }

    pub fn refresh_stale_routes_from_meta(
        &self,
    ) -> Result<ClientTopologyRefreshReport, ClientError> {
        let old_topology_version = self.topology_cache_report().max_topology_version;
        let meta_addr = self
            .inner
            .options
            .meta_addr
            .as_ref()
            .ok_or_else(|| ClientError::Status("meta_addr is required".to_string()))?;
        let topology: TopologyVersionReport = post_json_with_options_and_headers(
            meta_addr,
            "/meta/topology_version",
            &TopologyVersionRequest {
                old_topology_version,
            },
            &crate::meta::admin_auth_header(),
            self.inner.options.meta_sync_http_options(),
        )?;
        if !topology.status.ok {
            self.inner
                .stats
                .lock()
                .expect("client stats lock poisoned")
                .meta_sync_errors += 1;
            return Err(ClientError::Status(topology.status.message));
        }

        let open_tables = self.open_table_keys();
        let mut selected = BTreeMap::<String, (String, String)>::new();
        let mut refresh_all = old_topology_version < topology.current_topology_version
            && topology.events.is_empty()
            && !topology.unchanged;
        for event in &topology.events {
            if let Some(table) = event.resource.strip_prefix("table:") {
                if let Some((namespace, table_name)) = table.split_once('/') {
                    let key = table_combine_name(namespace, table_name);
                    if open_tables.iter().any(|open| open == &key) {
                        selected.insert(key, (namespace.to_string(), table_name.to_string()));
                    }
                }
            } else if matches!(
                event.kind.as_str(),
                "register_shard"
                    | "finish_load"
                    | "publish_shard_snapshot"
                    | "register_server"
                    | "server_state"
            ) {
                refresh_all = true;
            }
        }
        if refresh_all {
            for key in &open_tables {
                if let Some((namespace, table_name)) = key.split_once('/') {
                    selected.insert(key.clone(), (namespace.to_string(), table_name.to_string()));
                }
            }
        }

        let mut refreshed_tables = Vec::new();
        let mut skipped_tables = Vec::new();
        for (key, (namespace, table_name)) in selected {
            match self.sync_table_topology(namespace, table_name) {
                Ok(_) => refreshed_tables.push(key),
                Err(_) => skipped_tables.push(key),
            }
        }
        refreshed_tables.sort();
        skipped_tables.sort();
        let status = if skipped_tables.is_empty() {
            Status::ok()
        } else {
            Status::error("partial_refresh", skipped_tables.join(","))
        };
        Ok(ClientTopologyRefreshReport {
            status,
            old_topology_version,
            current_topology_version: topology.current_topology_version,
            unchanged: topology.unchanged,
            refreshed_tables,
            skipped_tables,
            refresh_all,
            event_count: topology.events.len(),
            stale_before_refresh: old_topology_version < topology.current_topology_version,
        })
    }

    pub fn invalidate_routes_from_meta_topology(
        &self,
    ) -> Result<ClientTopologyInvalidationReport, ClientError> {
        let before = self.topology_cache_report();
        if before.route_count == 0 {
            return Ok(ClientTopologyInvalidationReport {
                status: Status::ok(),
                old_topology_version: 0,
                current_topology_version: 0,
                route_count_before: 0,
                invalidated_routes: 0,
                refreshed_tables: Vec::new(),
                skipped_tables: Vec::new(),
                refresh_all: false,
                event_count: 0,
                stale_before_invalidation: false,
            });
        }
        let old_topology_version = before.max_topology_version;
        let meta_addr = self
            .inner
            .options
            .meta_addr
            .as_ref()
            .ok_or_else(|| ClientError::Status("meta_addr is required".to_string()))?;
        let topology: TopologyVersionReport = post_json_with_options_and_headers(
            meta_addr,
            "/meta/topology_version",
            &TopologyVersionRequest {
                old_topology_version,
            },
            &crate::meta::admin_auth_header(),
            self.inner.options.meta_sync_http_options(),
        )?;
        if !topology.status.ok {
            return Err(ClientError::Status(topology.status.message));
        }
        // Remember what the metaserver just said, so routes resolved from here on are
        // stamped with it and a later "unchanged" reply can actually keep the cache.
        self.inner
            .known_topology_version
            .store(topology.current_topology_version, std::sync::atomic::Ordering::Relaxed);

        let stale_before_invalidation = old_topology_version < topology.current_topology_version
            || (before.unknown_topology_version_routes > 0 && !topology.unchanged);
        let route_affecting_change = topology.event_history_truncated
            || topology.events.iter().any(topology_event_affects_routes)
            || (topology.events.is_empty()
                && old_topology_version < topology.current_topology_version);
        let invalidated_routes = if stale_before_invalidation && route_affecting_change {
            let mut routes = self
                .inner
                .routes
                .write()
                .expect("client route cache lock poisoned");
            let before_len = routes.len();
            routes.retain(|_, route| {
                route.topology_version > 0
                    && route.topology_version >= topology.current_topology_version
            });
            before_len.saturating_sub(routes.len())
        } else {
            0
        };

        let refresh = if route_affecting_change && !self.open_table_keys().is_empty() {
            Some(self.refresh_stale_routes_from_meta()?)
        } else {
            None
        };
        Ok(ClientTopologyInvalidationReport {
            status: refresh
                .as_ref()
                .map(|report| report.status.clone())
                .unwrap_or_else(Status::ok),
            old_topology_version,
            current_topology_version: topology.current_topology_version,
            route_count_before: before.route_count,
            invalidated_routes,
            refreshed_tables: refresh
                .as_ref()
                .map(|report| report.refreshed_tables.clone())
                .unwrap_or_default(),
            skipped_tables: refresh
                .as_ref()
                .map(|report| report.skipped_tables.clone())
                .unwrap_or_default(),
            refresh_all: route_affecting_change,
            event_count: topology.events.len(),
            stale_before_invalidation,
        })
    }

    pub fn open_table_keys(&self) -> Vec<String> {
        let mut tables = self
            .inner
            .tables
            .read()
            .expect("client table cache lock poisoned")
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        tables.sort();
        tables
    }

    pub fn start_meta_sync_loop(&self, interval_ms: u64) -> thread::JoinHandle<()> {
        let client = self.clone();
        let options = ClientMetaSyncLoopOptions {
            tick_ms: interval_ms.max(1),
            ..ClientMetaSyncLoopOptions::default()
        };
        thread::spawn(move || loop {
            client.run_due_meta_sync_once(options);
            thread::sleep(Duration::from_millis(options.tick_ms));
        })
    }

    pub fn start_meta_sync_loop_handle(
        &self,
        options: ClientMetaSyncLoopOptions,
    ) -> ClientMetaSyncLoopHandle {
        let client = self.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_for_thread = stop.clone();
        let join = thread::spawn(move || {
            let options = ClientMetaSyncLoopOptions {
                tick_ms: options.tick_ms.max(1),
                max_tables_per_tick: options.max_tables_per_tick.max(1),
            };
            while !stop_for_thread.load(Ordering::Relaxed) {
                client.run_due_meta_sync_once(options);
                sleep_meta_sync_tick(options.tick_ms, &stop_for_thread);
            }
        });
        ClientMetaSyncLoopHandle {
            stop,
            join: Some(join),
        }
    }

    pub fn run_due_meta_sync_once(&self, options: ClientMetaSyncLoopOptions) -> usize {
        let now = now_unix_ms();
        let tables = self.due_meta_sync_tables(now, options.max_tables_per_tick.max(1));
        let count = tables.len();
        for (namespace, table_name) in tables {
            let _ = self.sync_table_topology(namespace, table_name);
        }
        count
    }

    pub fn meta_sync_report(&self) -> ClientMetaSyncReport {
        let mut tables = self
            .inner
            .meta_sync_tables
            .lock()
            .expect("client meta sync table lock poisoned")
            .iter()
            .map(|(table, state)| ClientMetaSyncTableReport {
                table: table.clone(),
                namespace: state.namespace.clone(),
                table_name: state.table_name.clone(),
                sync_generation: state.sync_generation,
                last_success_unix_ms: state.last_success_unix_ms,
                last_error_unix_ms: state.last_error_unix_ms,
                next_sync_after_unix_ms: state.next_sync_after_unix_ms,
                last_topology_version: state.last_topology_version,
                consecutive_errors: state.consecutive_errors,
                shards_without_primary: state.shards_without_primary,
                last_error: state.last_error.clone(),
            })
            .collect::<Vec<_>>();
        tables.sort_by(|left, right| left.table.cmp(&right.table));
        let table_count = tables.len();
        let synced_table_count = tables
            .iter()
            .filter(|table| table.last_success_unix_ms > 0 && table.consecutive_errors == 0)
            .count();
        let error_table_count = tables
            .iter()
            .filter(|table| table.consecutive_errors > 0)
            .count();
        let total_sync_generation = tables.iter().map(|table| table.sync_generation).sum();
        ClientMetaSyncReport {
            table_count,
            synced_table_count,
            error_table_count,
            total_sync_generation,
            tables,
        }
    }

    pub fn close_table(&self, table: &TemporalStoreTable) -> Result<(), ClientError> {
        let removed = self
            .inner
            .tables
            .write()
            .expect("client table cache lock poisoned")
            .remove(&table_combine_name(table.namespace(), table.table_name()))
            .is_some();
        self.inner
            .stats
            .lock()
            .expect("client stats lock poisoned")
            .close_table_calls += 1;
        if removed {
            self.inner
                .routes
                .write()
                .expect("client route cache lock poisoned")
                .clear();
            self.inner
                .meta_sync_tables
                .lock()
                .expect("client meta sync table lock poisoned")
                .remove(&table_combine_name(table.namespace(), table.table_name()));
            Ok(())
        } else {
            Err(ClientError::Status("table not found".to_string()))
        }
    }

    pub fn stats(&self) -> ClientStats {
        let mut stats = *self.inner.stats.lock().expect("client stats lock poisoned");
        // Counted without the lock on the request path; folded in here so callers -- including the
        // proxy, which takes a delta against the previous read -- see one monotonic number.
        stats.route_cache_hits += self
            .inner
            .route_cache_hits
            .load(std::sync::atomic::Ordering::Relaxed);
        stats
    }

    pub fn preflight_report(&self) -> ClientPreflightReport {
        let options = self.inner.options.clone();
        let stats = self.stats();
        let route_cache_size = self.route_cache_size();
        let topology_cache = self.topology_cache_report();
        let meta_sync = self.meta_sync_report();
        let table_cache_size = self
            .inner
            .tables
            .read()
            .expect("client table cache lock poisoned")
            .len();
        let backend_failure_count = self
            .inner
            .backend_failures
            .lock()
            .expect("client backend failure lock poisoned")
            .len();
        let mut degraded_reasons = Vec::new();
        if stats.meta_sync_errors > 0 {
            degraded_reasons.push("meta_sync_errors".to_string());
        }
        if meta_sync.error_table_count > 0 {
            degraded_reasons.push("meta_sync_table_errors".to_string());
        }
        if stats.backend_errors > 0 {
            degraded_reasons.push("backend_errors".to_string());
        }
        if stats.continuous_backend_failures > 0 || backend_failure_count > 0 {
            degraded_reasons.push("backend_failure_backlog".to_string());
        }
        let status = if degraded_reasons.is_empty() {
            Status::ok()
        } else {
            Status::error("degraded", degraded_reasons.join(","))
        };
        ClientPreflightReport {
            status,
            proxy_addr: options.proxy_addr.clone(),
            meta_addr: options.meta_addr.clone(),
            default_shard_id: options.default_shard_id,
            route_cache_size,
            table_cache_size,
            backend_failure_count,
            stats,
            options,
            topology_cache,
            native_partition_sets: self.native_partition_set_report(),
            meta_sync,
            degraded_reasons,
        }
    }

    pub fn native_partition_set_report(&self) -> Vec<ClientPartitionSetReport> {
        let tables = self
            .inner
            .tables
            .read()
            .expect("client table cache lock poisoned")
            .clone();
        let routes = self
            .inner
            .routes
            .read()
            .expect("client route cache lock poisoned")
            .clone();
        let mut reports = tables
            .into_iter()
            .filter_map(|(combine_name, options)| {
                let (namespace, table_name) = combine_name.split_once('/')?;
                let mut members = (0..options.shard_count)
                    .map(|offset| {
                        let partition_id = client_partition_id_for_offset(&options, offset);
                        let shard_id = partition_id;
                        let route = routes.get(&shard_id);
                        ClientPartitionMemberReport {
                            partition_id,
                            shard_id,
                            start_bucket: route.map(|route| route.start_bucket).unwrap_or_else(|| {
                                partition_start_bucket(offset, options.shard_count)
                            }),
                            end_bucket: route
                                .map(|route| route.end_bucket)
                                .unwrap_or_else(|| partition_end_bucket(offset, options.shard_count)),
                            primary_addr: route.map(|route| route.primary_addr.clone()),
                            replica_addrs: route
                                .map(|route| route.replica_addrs.clone())
                                .unwrap_or_default(),
                            replica_count: route
                                .map(|route| {
                                    route.replica_addrs.len().max(route.replica_endpoints.len())
                                })
                                .unwrap_or_default(),
                            topology_version: route
                                .map(|route| route.topology_version)
                                .unwrap_or_default(),
                            route_ready: route.is_some(),
                            refresh_reason: route
                                .map(|route| route.refresh_reason.clone())
                                .unwrap_or_default(),
                        }
                    })
                    .collect::<Vec<_>>();
                members.sort_by_key(|member| member.partition_id);
                let topology_version = members
                    .iter()
                    .map(|member| member.topology_version)
                    .max()
                    .unwrap_or_default();
                let missing_route_count =
                    members.iter().filter(|member| !member.route_ready).count();
                Some(ClientPartitionSetReport {
                    table_id: options.table_id,
                    namespace: namespace.to_string(),
                    table_name: table_name.to_string(),
                    combine_name,
                    first_shard_id: options.first_shard_id,
                    shard_count: options.shard_count,
                    partition_version: options.partition_version,
                    topology_version,
                    partition_count: members.len(),
                    missing_route_count,
                    members,
                })
            })
            .collect::<Vec<_>>();
        reports.sort_by(|left, right| left.combine_name.cmp(&right.combine_name));
        reports
    }

    pub fn topology_cache_report(&self) -> ClientTopologyCacheReport {
        self.topology_cache_report_against(0)
    }

    pub fn topology_cache_report_against(
        &self,
        authoritative_topology_version: u64,
    ) -> ClientTopologyCacheReport {
        let ttl = Duration::from_millis(self.inner.options.route_cache_ttl_ms);
        let routes = self
            .inner
            .routes
            .read()
            .expect("client route cache lock poisoned")
            .iter()
            .map(|(shard_id, route)| {
                let fetched_age_ms = duration_ms_u64(route.fetched_at.elapsed());
                ClientRouteCacheEntryReport {
                    shard_id: *shard_id,
                    table: route.table_key.clone(),
                    partition_id: route.partition_id,
                    start_bucket: route.start_bucket,
                    end_bucket: route.end_bucket,
                    partition_version: route.partition_version,
                    primary_addr: route.primary_addr.clone(),
                    replica_count: route.replica_addrs.len().max(route.replica_endpoints.len()),
                    topology_version: route.topology_version,
                    fetched_age_ms,
                    ttl_expired: route.fetched_at.elapsed() > ttl,
                    refresh_reason: route.refresh_reason.clone(),
                }
            })
            .collect::<Vec<_>>();
        let mut routes = routes;
        routes.sort_by_key(|route| route.shard_id);
        let route_count = routes.len();
        let min_topology_version = routes
            .iter()
            .filter(|route| route.topology_version > 0)
            .map(|route| route.topology_version)
            .min()
            .unwrap_or_default();
        let max_topology_version = routes
            .iter()
            .map(|route| route.topology_version)
            .max()
            .unwrap_or_default();
        let unknown_topology_version_routes = routes
            .iter()
            .filter(|route| route.topology_version == 0)
            .count();
        let stale_route_count = if authoritative_topology_version == 0 {
            0
        } else {
            routes
                .iter()
                .filter(|route| route.topology_version < authoritative_topology_version)
                .count()
        };
        let ttl_expired_routes = routes.iter().filter(|route| route.ttl_expired).count();
        let last_refresh_reason = routes
            .iter()
            .min_by_key(|route| route.fetched_age_ms)
            .map(|route| route.refresh_reason.clone())
            .unwrap_or_default();
        ClientTopologyCacheReport {
            route_count,
            min_topology_version,
            max_topology_version,
            authoritative_topology_version,
            stale_route_count,
            cache_stale: stale_route_count > 0,
            unknown_topology_version_routes,
            ttl_expired_routes,
            last_refresh_reason,
            routes,
        }
    }

    pub fn route_cache_size(&self) -> usize {
        self.inner
            .routes
            .read()
            .expect("client route cache lock poisoned")
            .len()
    }

    pub(super) fn due_meta_sync_tables(&self, now_ms: u64, max_tables: usize) -> Vec<(String, String)> {
        let table_keys = self.open_table_keys();
        let states = self
            .inner
            .meta_sync_tables
            .lock()
            .expect("client meta sync table lock poisoned");
        table_keys
            .into_iter()
            .filter_map(|table| {
                let due = states
                    .get(&table)
                    .map(|state| state.next_sync_after_unix_ms <= now_ms)
                    .unwrap_or(true);
                due.then(|| {
                    table.split_once('/').map(|(namespace, table_name)| {
                        (namespace.to_string(), table_name.to_string())
                    })
                })
                .flatten()
            })
            .take(max_tables)
            .collect()
    }

    #[cfg(test)]
    pub fn insert_cached_route_for_test(&self, shard_id: ShardId, primary_addr: impl Into<String>) {
        self.inner
            .routes
            .write()
            .expect("client route cache lock poisoned")
            .insert(
                shard_id,
                CachedRoute::for_shard(shard_id, primary_addr, "test_insert"),
            );
    }

    #[cfg(test)]
    pub fn insert_backend_failure_for_test(
        &self,
        server_addr: impl Into<String>,
        first_failed_ago_ms: u64,
        last_failed_ago_ms: u64,
        consecutive_failures: u64,
    ) {
        let now = Instant::now();
        self.with_backend_failures(|failures| {
            failures.insert(
                server_addr.into(),
                BackendFailureState {
                    first_failed_at: now - Duration::from_millis(first_failed_ago_ms),
                    last_failed_at: now - Duration::from_millis(last_failed_ago_ms),
                    consecutive_failures,
                },
            );
        });
    }

    pub(super) fn ensure_meta_sync_table_state(&self, namespace: &str, table_name: &str) {
        let key = table_combine_name(namespace, table_name);
        self.inner
            .meta_sync_tables
            .lock()
            .expect("client meta sync table lock poisoned")
            .entry(key)
            .or_insert_with(|| ClientMetaSyncTableState {
                namespace: namespace.to_string(),
                table_name: table_name.to_string(),
                sync_generation: 0,
                last_success_unix_ms: 0,
                last_error_unix_ms: 0,
                next_sync_after_unix_ms: now_unix_ms()
                    .saturating_add(self.inner.options.meta_sync_interval_ms),
                last_topology_version: 0,
                consecutive_errors: 0,
                last_error: String::new(),
                shards_without_primary: 0,
                last_forced_sync_unix_ms: 0,
            });
    }

    /// The topology version this client last synced for a table, or 0 if it has none.
    ///
    /// Sent with the next fetch so the metaserver can answer "unchanged" instead of rebuilding
    /// and shipping the whole shard list. Both halves of that negotiation already existed --
    /// the request field and the metaserver's reply -- with nothing connecting them.
    fn last_synced_topology_version(&self, namespace: &str, table_name: &str) -> u64 {
        let key = table_combine_name(namespace, table_name);
        self.inner
            .meta_sync_tables
            .lock()
            .expect("client meta sync table lock poisoned")
            .get(&key)
            .map(|state| state.last_topology_version)
            .unwrap_or(0)
    }

    /// Whether a failure-driven topology sync for this table is due, claiming the slot if so.
    ///
    /// Claimed as it answers, so concurrent failures cannot all decide they are due and fire
    /// the same sync. Losing that race means skipping a sync another thread is already making,
    /// which is the outcome wanted.
    pub(super) fn forced_sync_is_due(&self, namespace: &str, table_name: &str) -> bool {
        let interval = self.inner.options.topo_error_retry_interval_ms;
        if interval == 0 {
            return true;
        }
        let now = now_unix_ms();
        let key = table_combine_name(namespace, table_name);
        let mut states = self
            .inner
            .meta_sync_tables
            .lock()
            .expect("client meta sync table lock poisoned");
        let state = states
            .entry(key)
            .or_insert_with(|| ClientMetaSyncTableState {
                namespace: namespace.to_string(),
                table_name: table_name.to_string(),
                sync_generation: 0,
                last_success_unix_ms: 0,
                last_error_unix_ms: 0,
                next_sync_after_unix_ms: 0,
                last_topology_version: 0,
                consecutive_errors: 0,
                last_error: String::new(),
                shards_without_primary: 0,
                last_forced_sync_unix_ms: 0,
            });
        let last = state.last_forced_sync_unix_ms;
        // A clock that went backwards should not lock the refresh out until it catches up.
        if last != 0 && now >= last && now.saturating_sub(last) < interval {
            return false;
        }
        state.last_forced_sync_unix_ms = now;
        true
    }

    pub(super) fn record_meta_sync_success(
        &self,
        namespace: &str,
        table_name: &str,
        topology_version: u64,
        shards_without_primary: u64,
    ) {
        let key = table_combine_name(namespace, table_name);
        let now = now_unix_ms();
        let mut states = self
            .inner
            .meta_sync_tables
            .lock()
            .expect("client meta sync table lock poisoned");
        let state = states
            .entry(key)
            .or_insert_with(|| ClientMetaSyncTableState {
                namespace: namespace.to_string(),
                table_name: table_name.to_string(),
                sync_generation: 0,
                last_success_unix_ms: 0,
                last_error_unix_ms: 0,
                next_sync_after_unix_ms: 0,
                last_topology_version: 0,
                consecutive_errors: 0,
                last_error: String::new(),
                shards_without_primary: 0,
                last_forced_sync_unix_ms: 0,
            });
        state.sync_generation = state.sync_generation.saturating_add(1);
        state.last_success_unix_ms = now;
        state.next_sync_after_unix_ms = now.saturating_add(meta_sync_jittered_delay_ms(
            self.inner.options.meta_sync_interval_ms,
            self.inner.options.meta_sync_jitter_percent,
            &table_combine_name(namespace, table_name),
            state.sync_generation,
        ));
        state.last_topology_version = topology_version;
        state.consecutive_errors = 0;
        state.shards_without_primary = shards_without_primary;
        state.last_error.clear();
    }

    pub(super) fn record_meta_sync_error(&self, namespace: &str, table_name: &str, error: &str) {
        let key = table_combine_name(namespace, table_name);
        let now = now_unix_ms();
        let mut states = self
            .inner
            .meta_sync_tables
            .lock()
            .expect("client meta sync table lock poisoned");
        let state = states
            .entry(key)
            .or_insert_with(|| ClientMetaSyncTableState {
                namespace: namespace.to_string(),
                table_name: table_name.to_string(),
                sync_generation: 0,
                last_success_unix_ms: 0,
                last_error_unix_ms: 0,
                next_sync_after_unix_ms: 0,
                last_topology_version: 0,
                consecutive_errors: 0,
                last_error: String::new(),
                shards_without_primary: 0,
                last_forced_sync_unix_ms: 0,
            });
        state.sync_generation = state.sync_generation.saturating_add(1);
        state.last_error_unix_ms = now;
        state.consecutive_errors = state.consecutive_errors.saturating_add(1);
        let backoff_ms = self
            .inner
            .options
            .topo_error_retry_interval_ms
            .saturating_mul(1_u64 << state.consecutive_errors.saturating_sub(1).min(10))
            .min(self.inner.options.meta_sync_interval_ms.max(1));
        state.next_sync_after_unix_ms = now.saturating_add(meta_sync_jittered_delay_ms(
            backoff_ms,
            self.inner.options.meta_sync_jitter_percent,
            &table_combine_name(namespace, table_name),
            state.consecutive_errors,
        ));
        state.last_error = error.to_string();
    }
}
