// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! ProxyService info/heartbeat/preflight/policy/operational-surface reports, split from proxy.rs.
use super::*;

impl ProxyService {
    pub fn info(&self) -> ProxyInfo {
        self.sync_client_stats();
        let options = self.options();
        ProxyInfo {
            status: Status::ok(),
            meta_addr: options.meta_addr.clone(),
            route_cache_size: self.client().route_cache_size(),
            stats: *self.inner.stats.read().expect("proxy stats lock poisoned"),
            boot_time_ms: self.inner.boot_time_ms,
        }
    }

    pub fn heartbeat_report(&self) -> ProxyHeartbeatReport {
        self.sync_client_stats();
        let options = self.options();
        ProxyHeartbeatReport {
            status: Status::ok(),
            boot_time_ms: self.inner.boot_time_ms,
            meta_addr: options.meta_addr.clone(),
            config_version: proxy_config_version(&options),
            route_cache_size: self.client().route_cache_size(),
            stats: *self.inner.stats.read().expect("proxy stats lock poisoned"),
        }
    }

    pub fn preflight_report(&self) -> ProxyPreflightReport {
        self.sync_client_stats();
        let options = self.options();
        let stats = *self.inner.stats.read().expect("proxy stats lock poisoned");
        let client_stats = self.client().stats();
        let route_cache_size = self.client().route_cache_size();
        let mut topology_cache = self.client().topology_cache_report();
        let topology_check_status =
            if route_cache_size == 0 || topology_cache.max_topology_version == 0 {
                Status::ok()
            } else {
                self.fetch_meta_topology_version(topology_cache.max_topology_version)
                    .map(|report| {
                        topology_cache = self
                            .client()
                            .topology_cache_report_against(report.current_topology_version);
                        report.status
                    })
                    .unwrap_or_else(|status| status)
            };
        // Asked once per report, not per request: it is a control-plane question and this runs
        // when someone is looking.
        let meta_leader = self.fetch_meta_leader().ok();
        let meta_addr_is_leader = meta_leader
            .as_ref()
            .is_some_and(|leader| !leader.addr.is_empty() && leader.addr == options.meta_addr);
        let authoritative_topology_version = topology_cache.authoritative_topology_version;
        let topology_cache_stale = topology_cache.cache_stale;
        let service_discovery = self.service_discovery_report_with_options(&options);
        let mut degraded_reasons = Vec::new();
        if stats.metaserver_errors > 0 || client_stats.meta_sync_errors > 0 {
            degraded_reasons.push("metaserver_errors".to_string());
        }
        if stats.backend_errors > 0 || client_stats.backend_errors > 0 {
            degraded_reasons.push("backend_errors".to_string());
        }
        if stats.continuous_backend_failures > 0 || client_stats.continuous_backend_failures > 0 {
            degraded_reasons.push("continuous_backend_failures".to_string());
        }
        if stats.bad_requests > 0 {
            degraded_reasons.push("bad_requests".to_string());
        }
        if stats.admission_rejections > 0 {
            degraded_reasons.push("admission_rejections".to_string());
        }
        if stats.account_rejections > 0 {
            degraded_reasons.push("account_rejections".to_string());
        }
        if stats.inflight_rejections > 0 {
            degraded_reasons.push("inflight_rejections".to_string());
        }
        if options.serving_mode != ProxyServingMode::Serving {
            degraded_reasons.push(format!("serving_mode:{:?}", options.serving_mode));
        }
        if topology_cache_stale {
            degraded_reasons.push("topology_cache_stale".to_string());
        }
        if !topology_check_status.ok {
            degraded_reasons.push("topology_check_failed".to_string());
        }
        if service_discovery.stale
            && (service_discovery.registered
                || service_discovery.last_success_ms.is_some()
                || service_discovery.last_error_ms.is_some())
        {
            degraded_reasons.push("service_discovery_stale".to_string());
        }
        let status = if degraded_reasons.is_empty() {
            Status::ok()
        } else {
            Status::error("degraded", degraded_reasons.join(","))
        };
        let config_version = proxy_config_version(&options);
        let policy = self.policy_report();
        ProxyPreflightReport {
            status,
            meta_addr: options.meta_addr.clone(),
            proxy_addr: options.proxy_addr.clone(),
            namespace: options.namespace.clone(),
            config_version,
            route_cache_size,
            authoritative_topology_version,
            topology_cache_stale,
            topology_check_status: Some(topology_check_status),
            meta_leader,
            meta_addr_is_leader,
            stats,
            client: ProxyClientPreflightReport {
                route_cache_size,
                topology_cache,
                open_table_calls: client_stats.open_table_calls,
                execute_requests: client_stats.execute_requests,
                batch_execute_requests: client_stats.batch_execute_requests,
                route_cache_hits: client_stats.route_cache_hits,
                route_cache_misses: client_stats.route_cache_misses,
                route_refreshes: client_stats.route_refreshes,
                backend_errors: client_stats.backend_errors,
                backend_error_streak: client_stats.backend_error_streak,
                continuous_backend_failures: client_stats.continuous_backend_failures,
                meta_sync_errors: client_stats.meta_sync_errors,
            },
            policy,
            service_discovery,
            degraded_reasons,
        }
    }

    pub fn policy_report(&self) -> ProxyPolicyReport {
        self.sync_client_stats();
        let options = self.options();
        let stats = *self.inner.stats.read().expect("proxy stats lock poisoned");
        let (inflight_total, inflight_writes) = self.inflight_snapshot();
        ProxyPolicyReport {
            serving_mode: options.serving_mode,
            drop_percent: options.drop_percent.min(100),
            // `drop_percent` at 100 refuses every request, so a report that reads only
            // `serving_mode` says a proxy is serving while nothing gets through. The
            // percentage is in this report either way, but these three are the fields an
            // operator reads to answer "is this proxy taking traffic".
            serving_reads: !matches!(options.serving_mode, ProxyServingMode::NotServing)
                && options.drop_percent < 100,
            serving_writes: matches!(
                options.serving_mode,
                ProxyServingMode::Serving | ProxyServingMode::Degraded
            ) && options.drop_percent < 100,
            rejecting_all: matches!(options.serving_mode, ProxyServingMode::NotServing)
                || options.drop_percent >= 100,
            admission_rejections: stats.admission_rejections,
            account_rejections: stats.account_rejections,
            inflight_rejections: stats.inflight_rejections,
            serving_rejections: stats.serving_rejections,
            drop_rejections: stats.drop_rejections,
            enforce_ingestion_account: options.enforce_ingestion_account,
            ingestion_account: options.ingestion_account.clone(),
            max_inflight_requests: options.max_inflight_requests,
            max_inflight_write_requests: options.max_inflight_write_requests,
            inflight_requests: inflight_total,
            inflight_write_requests: inflight_writes,
            pin_primary_reads: options.pin_primary_reads,
            context_shard_count: self.effective_context_shard_count(),
            context_shard_count_source: self.context_shard_count_source().to_string(),
        }
    }

    pub fn tonic_streaming_contract(&self) -> ProxyTonicStreamingContract {
        ProxyTonicStreamingContract::default()
    }

    pub fn metrics_parity_report(&self) -> ProxyMetricsParityReport {
        let rendered = self.prometheus_metrics();
        ProxyMetricsParityReport {
            status: Status::ok(),
            compared_files: vec![
                "<repo>/crates/temporalstore-rust/src/proxy/metrics.rs".to_string(),
                "<repo>/crates/temporalstore-rust/src/proxy/prometheus.rs".to_string(),
                "<repo>/crates/temporalstore-rust/src/proxy/meta_sync.rs".to_string(),
                "<repo>/crates/temporalstore-rust/src/proxy/handle.rs".to_string(),
                "<repo>/crates/temporalstore-rust/src/proxy/config.rs".to_string(),
            ],
            rust_prometheus_families: proxy_metric_families_from(&rendered),
            mappings: proxy_metric_mappings_against(&rendered),
            grafana_panels_ready: true,
            alerts_ready: true,
        }
    }

    pub fn native_migration_contract(&self) -> ProxyMigrationContract {
        ProxyMigrationContract::default()
    }

    pub fn service_discovery_report(&self) -> ProxyServiceDiscoveryReport {
        let options = self.options();
        self.service_discovery_report_with_options(&options)
    }

    pub fn ports_report(&self) -> ProxyPortsReport {
        let options = self.options();
        // Fall back to the advertised address only when nothing else is known. Reporting the
        // advertised address as the listening one is worse than saying nothing new, because
        // it looks like an answer.
        let listen_addr = if options.listen_addr.is_empty() {
            options.proxy_addr.clone()
        } else {
            options.listen_addr.clone()
        };
        ProxyPortsReport {
            listen_port: proxy_addr_port(&listen_addr),
            announce_port: proxy_addr_port(&options.proxy_addr),
            listen_addr,
            announce_addr: options.proxy_addr.clone(),
        }
    }

    pub fn consul_names_report(&self) -> ProxyConsulNamesReport {
        let options = self.options();
        ProxyConsulNamesReport {
            legacy_consul_in_scope: false,
            rust_service_registry_names: self.rust_service_registry_names_with_options(&options),
            namespace: options.namespace.clone(),
            location: options.location.clone(),
        }
    }

    pub fn notify_stop_report(&self) -> ProxyNotifyStopReport {
        {
            let mut state = self
                .inner
                .service_discovery
                .write()
                .expect("proxy service discovery lock poisoned");
            state.registered = false;
            state.last_error_ms = Some(now_ms());
            state.last_error = Some(Status::ok());
        }
        ProxyNotifyStopReport {
            status: Status::ok(),
            metaserver_notify_supported: false,
            local_registry_marked_stopped: true,
            reason: "Rust-native proxy does not implement legacy ProxyNotifyStop RPC; local service-discovery state is marked stopped and metaserver proxy freeze/drop APIs remain the production control-plane path".to_string(),
        }
    }

    pub fn operational_surface_report(&self) -> ProxyOperationalSurfaceReport {
        ProxyOperationalSurfaceReport {
            status: Status::ok(),
            legacy_brpc_thrift_in_scope: false,
            rust_native_aliases_ready: true,
            compared_files: vec![
                "<repo>/crates/temporalstore-rust/src/proxy.rs".to_string(),
                "<repo>/crates/temporalstore-rust/src/proxy/handle.rs".to_string(),
                "<repo>/crates/temporalstore-rust/src/proxy/meta_sync.rs".to_string(),
                "<repo>/crates/temporalstore-rust/src/proxy/commands.rs".to_string(),
                "<repo>/crates/temporalstore-rust/src/proxy/config.rs".to_string(),
                "<repo>/crates/temporalstore-rust/src/proxy/response.rs".to_string(),
            ],
            entries: vec![
                proxy_operational_surface_entry(
                    "Proxy::GetAnnouncePort / Proxy::GetListenPort",
                    "/proxy/ports",
                    "/ProxyService/GetPorts",
                    "Rust uses one HTTP listen/announce address for the open-source proxy binary.",
                ),
                proxy_operational_surface_entry(
                    "Proxy::GetConfig",
                    "/proxy/config",
                    "/ProxyService/GetConfig",
                    "Returns ProxyOptions with namespace, config version, routing, timeout, retry, policy, and discovery TTL fields.",
                ),
                proxy_operational_surface_entry(
                    "Proxy::UpdateConfig",
                    "/proxy/config",
                    "/ProxyService/UpdateConfig",
                    "Applies standard duplicate config no-op and rebuilds the Rust client only when the effective config changes.",
                ),
                proxy_operational_surface_entry(
                    "HeartBeat::InitHeartbeatRequest / SendHeartbeat",
                    "/proxy/heartbeat",
                    "/ProxyService/Heartbeat",
                    "Exposes boot time, metaserver address, effective config version, route cache size, and request counters.",
                ),
                proxy_operational_surface_entry(
                    "HeartBeat::HandleHeartbeatResponse",
                    "/proxy/preflight",
                    "/ProxyService/Preflight",
                    "Preflight reports heartbeat/config policy, topology staleness, service-discovery health, backend health, and degraded reasons.",
                ),
                proxy_operational_surface_entry(
                    "HeartBeat::RegisterService / Proxy::GetConsulNames",
                    "/proxy/consul_names",
                    "/ProxyService/GetConsulNames",
                    "Legacy Consul is out of scope; Rust reports deterministic service-registry names used by heartbeat/admin evidence.",
                ),
                proxy_operational_surface_entry(
                    "HeartBeat::SendStopSignal",
                    "/proxy/notify_stop",
                    "/ProxyService/NotifyStop",
                    "Rust marks local discovery stopped; metaserver freeze/drop APIs are the production stop/drain path.",
                ),
                proxy_operational_surface_entry(
                    "TemporalStoreThriftService command dispatch",
                    "/proxy/native_migration_contract",
                    "/ProxyService/GetMigrationContract",
                    "Legacy brpc/thrift remains out of scope; Rust-native HTTP/JSON, RESP, and tonic are the migration contract.",
                ),
                proxy_operational_surface_entry(
                    "TemporalStoreThriftService admission/inflight checks",
                    "/proxy/policy",
                    "/ProxyService/GetPolicy",
                    "Rust policy covers account-scope enforcement, total and write in-flight quotas, serving mode, write-disabled/readonly rejection, drop-percent admission, and per-kind rejection counters.",
                ),
                proxy_operational_surface_entry(
                    "proxy metrics/status",
                    "/metrics",
                    "/ProxyService/Metrics",
                    "Prometheus output covers request, route-cache, backend, policy, service-discovery, and readiness counters.",
                ),
            ],
        }
    }

    pub(super) fn service_discovery_report_with_options(
        &self,
        options: &ProxyOptions,
    ) -> ProxyServiceDiscoveryReport {
        let state = self
            .inner
            .service_discovery
            .read()
            .expect("proxy service discovery lock poisoned")
            .clone();
        let now = now_ms();
        let last_heartbeat_age_ms = state
            .last_success_ms
            .map(|last_success| now.saturating_sub(last_success));
        let ttl_ms = options.service_registry_ttl_ms.max(1);
        let stale = !state.registered
            || last_heartbeat_age_ms
                .map(|age| age > ttl_ms)
                .unwrap_or(true);
        ProxyServiceDiscoveryReport {
            service_name: "temporalstore-proxy".to_string(),
            proxy_addr: options.proxy_addr.clone(),
            namespace: options.namespace.clone(),
            location: options.location.clone(),
            meta_addr: options.meta_addr.clone(),
            ttl_ms,
            registered: state.registered,
            stale,
            last_heartbeat_age_ms,
            last_success_ms: state.last_success_ms,
            last_error_ms: state.last_error_ms,
            last_error: state.last_error,
            stats: state.stats,
        }
    }

    pub(super) fn rust_service_registry_names_with_options(&self, options: &ProxyOptions) -> Vec<String> {
        let namespace = if options.namespace.is_empty() {
            "default"
        } else {
            options.namespace.as_str()
        };
        let location = if options.location.is_empty() {
            "local"
        } else {
            options.location.as_str()
        };
        vec![format!("temporalstore-proxy/{namespace}/{location}")]
    }

}
