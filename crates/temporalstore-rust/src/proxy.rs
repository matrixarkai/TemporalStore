use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::client::{
    key_is_dropped_by_percent, ClientOptions, ClientStats, ClientTopologyCacheReport,
    ClientTopologyRefreshReport, ReplicaReadPolicy, RequestOptions, TableOptions,
    TemporalStoreClient,
};
use crate::http::{get_json_with_options, post_json_with_options, HttpRequest, HttpRequestOptions};
use crate::meta::GetShardResponse;
use crate::meta::{
    AckResponse, ProxyHeartbeatRequest, ProxyHeartbeatResponse, RegisterProxyRequest,
    TopologyVersionReport, TopologyVersionRequest,
};
use crate::types::{
    BatchExecuteRequest, BatchExecuteResponse, Command, CommandResponse, ExecuteRequest,
    ExecuteResponse, ShardId, Status,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyOptions {
    pub meta_addr: String,
    #[serde(default = "default_proxy_addr")]
    pub proxy_addr: String,
    #[serde(default)]
    pub config_version: u64,
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub location: String,
    #[serde(default)]
    pub binary_version: String,
    pub route_cache_ttl_ms: u64,
    pub connect_timeout_ms: u64,
    pub io_timeout_ms: u64,
    pub max_retries: usize,
    pub refresh_route_on_backend_error: bool,
    pub backend_continuous_failed_time_ms: u64,
    #[serde(default)]
    pub serving_mode: ProxyServingMode,
    #[serde(default)]
    pub drop_percent: u8,
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProxyServingMode {
    #[default]
    Serving,
    Readonly,
    WriteDisabled,
    Degraded,
    NotServing,
}

impl ProxyOptions {
    pub fn new(meta_addr: impl Into<String>) -> Self {
        Self {
            meta_addr: meta_addr.into(),
            ..Self::default()
        }
    }

    fn http_options(&self) -> HttpRequestOptions {
        HttpRequestOptions {
            connect_timeout_ms: self.connect_timeout_ms,
            io_timeout_ms: self.io_timeout_ms,
            max_retries: self.max_retries,
        }
    }
}

impl Default for ProxyOptions {
    fn default() -> Self {
        Self {
            meta_addr: "127.0.0.1:17001".to_string(),
            proxy_addr: "127.0.0.1:17000".to_string(),
            config_version: 0,
            namespace: String::new(),
            location: String::new(),
            binary_version: String::new(),
            route_cache_ttl_ms: 1_000,
            connect_timeout_ms: 200,
            io_timeout_ms: 200,
            max_retries: 0,
            refresh_route_on_backend_error: true,
            backend_continuous_failed_time_ms: 10_000,
            serving_mode: ProxyServingMode::Serving,
            drop_percent: 0,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyStats {
    pub execute_requests: u64,
    pub batch_execute_requests: u64,
    pub route_cache_hits: u64,
    pub route_cache_misses: u64,
    pub route_refreshes: u64,
    pub backend_errors: u64,
    pub continuous_backend_failures: u64,
    pub metaserver_errors: u64,
    pub bad_requests: u64,
    pub admission_rejections: u64,
    pub heartbeat_total: u64,
    pub auto_register_total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyInfo {
    pub status: Status,
    pub meta_addr: String,
    pub route_cache_size: usize,
    pub stats: ProxyStats,
    pub boot_time_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyHeartbeatReport {
    pub status: Status,
    pub boot_time_ms: u64,
    pub meta_addr: String,
    pub config_version: u64,
    pub route_cache_size: usize,
    pub stats: ProxyStats,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyClientPreflightReport {
    pub route_cache_size: usize,
    pub topology_cache: ClientTopologyCacheReport,
    pub open_table_calls: u64,
    pub execute_requests: u64,
    pub batch_execute_requests: u64,
    pub route_cache_hits: u64,
    pub route_cache_misses: u64,
    pub route_refreshes: u64,
    pub backend_errors: u64,
    pub backend_error_streak: u64,
    pub continuous_backend_failures: u64,
    pub meta_sync_errors: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyPreflightReport {
    pub status: Status,
    pub meta_addr: String,
    pub proxy_addr: String,
    pub namespace: String,
    pub config_version: u64,
    pub route_cache_size: usize,
    #[serde(default)]
    pub authoritative_topology_version: u64,
    #[serde(default)]
    pub topology_cache_stale: bool,
    #[serde(default)]
    pub topology_check_status: Option<Status>,
    pub stats: ProxyStats,
    pub client: ProxyClientPreflightReport,
    #[serde(default)]
    pub policy: ProxyPolicyReport,
    pub degraded_reasons: Vec<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyPolicyReport {
    pub serving_mode: ProxyServingMode,
    pub drop_percent: u8,
    pub serving_reads: bool,
    pub serving_writes: bool,
    pub rejecting_all: bool,
    pub admission_rejections: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyTopologyRefreshResponse {
    pub status: Status,
    pub report: Option<ClientTopologyRefreshReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyConfigUpdateReport {
    pub status: Status,
    pub applied: bool,
    pub reason: String,
    pub previous_namespace: String,
    pub previous_config_version: u64,
    pub new_namespace: String,
    pub new_config_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyOpenTableRequest {
    pub namespace: String,
    pub table_name: String,
    #[serde(default)]
    pub pin_primary: Option<bool>,
    #[serde(default)]
    pub replica_read_policy: Option<ProxyReplicaReadPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyOpenTableResponse {
    pub status: Status,
    pub options: Option<ProxyTableOptionsView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyTableExecuteRequest {
    pub namespace: String,
    pub table_name: String,
    pub command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyTableBatchExecuteRequest {
    pub namespace: String,
    pub table_name: String,
    pub commands: Vec<Command>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProxyReplicaReadPolicy {
    PinPrimary,
    FirstReplica,
    RoundRobinReplica,
}

impl From<ProxyReplicaReadPolicy> for ReplicaReadPolicy {
    fn from(value: ProxyReplicaReadPolicy) -> Self {
        match value {
            ProxyReplicaReadPolicy::PinPrimary => ReplicaReadPolicy::PinPrimary,
            ProxyReplicaReadPolicy::FirstReplica => ReplicaReadPolicy::FirstReplica,
            ProxyReplicaReadPolicy::RoundRobinReplica => ReplicaReadPolicy::RoundRobinReplica,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyTableOptionsView {
    pub first_shard_id: ShardId,
    pub shard_count: u64,
    pub pin_primary: bool,
    pub replica_read_policy: ProxyReplicaReadPolicy,
    pub preferred_location: String,
    pub drop_percent: u8,
}

impl From<TableOptions> for ProxyTableOptionsView {
    fn from(options: TableOptions) -> Self {
        Self {
            first_shard_id: options.first_shard_id,
            shard_count: options.shard_count,
            pin_primary: options.pin_primary,
            preferred_location: options.preferred_location,
            drop_percent: options.drop_percent,
            replica_read_policy: match options.replica_read_policy {
                ReplicaReadPolicy::PinPrimary => ProxyReplicaReadPolicy::PinPrimary,
                ReplicaReadPolicy::FirstReplica => ProxyReplicaReadPolicy::FirstReplica,
                ReplicaReadPolicy::RoundRobinReplica => ProxyReplicaReadPolicy::RoundRobinReplica,
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProxyService {
    inner: Arc<ProxyInner>,
}

#[derive(Debug)]
struct ProxyInner {
    options: RwLock<ProxyOptions>,
    client: RwLock<TemporalStoreClient>,
    last_client_stats: RwLock<ClientStats>,
    stats: RwLock<ProxyStats>,
    boot_time_ms: u64,
}

impl ProxyService {
    pub fn new(options: ProxyOptions) -> Self {
        Self {
            inner: Arc::new(ProxyInner {
                client: RwLock::new(proxy_client_from_options(&options)),
                options: RwLock::new(options),
                last_client_stats: RwLock::default(),
                stats: RwLock::default(),
                boot_time_ms: now_ms(),
            }),
        }
    }

    pub fn handle(&self, request: HttpRequest) -> (u16, Vec<u8>) {
        use crate::http::{json_response, parse_json};
        match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/health") => json_response(200, &Status::ok()),
            ("GET", "/readiness") | ("GET", "/cpp_parity") => {
                json_response(200, &crate::production_readiness_report())
            }
            ("GET", "/proxy/info") | ("GET", "/ProxyService/GetInfo") => {
                json_response(200, &self.info())
            }
            ("GET", "/proxy/heartbeat") | ("GET", "/ProxyService/Heartbeat") => {
                json_response(200, &self.heartbeat_report())
            }
            ("GET", "/proxy/preflight") | ("GET", "/ProxyService/Preflight") => {
                json_response(200, &self.preflight_report())
            }
            ("GET", "/proxy/policy") | ("GET", "/ProxyService/GetPolicy") => {
                json_response(200, &self.policy_report())
            }
            ("GET", "/proxy/client_preflight") | ("GET", "/ProxyService/ClientPreflight") => {
                json_response(200, &self.client().preflight_report())
            }
            ("POST", "/proxy/topology/refresh") | ("POST", "/ProxyService/RefreshTopology") => {
                json_response(200, &self.refresh_topology_from_meta())
            }
            ("GET", "/proxy/config") | ("GET", "/ProxyService/GetConfig") => {
                let options = self
                    .inner
                    .options
                    .read()
                    .expect("proxy options lock poisoned")
                    .clone();
                json_response(200, &options)
            }
            ("POST", "/proxy/config") | ("POST", "/ProxyService/UpdateConfig") => {
                match parse_json::<ProxyOptions>(&request.body) {
                    Ok(options) => json_response(200, &self.update_options_report(options)),
                    Err(err) => {
                        self.inc_bad_request();
                        json_response(400, &Status::error("bad_request", err.to_string()))
                    }
                }
            }
            ("GET", path) if path.starts_with("/shards/") => {
                let shard_id = path
                    .trim_start_matches("/shards/")
                    .parse()
                    .unwrap_or_default();
                match self.get_shard(shard_id, true) {
                    Ok(response) => json_response(200, &response),
                    Err(status) => json_response(502, &status),
                }
            }
            ("POST", "/execute") | ("POST", "/ProxyService/ExecuteCmd") => {
                match parse_json::<ExecuteRequest>(&request.body) {
                    Ok(req) => json_response(200, &self.execute(req)),
                    Err(err) => {
                        self.inc_bad_request();
                        json_response(400, &execute_error("bad_request", err.to_string()))
                    }
                }
            }
            ("POST", "/batch_execute") | ("POST", "/ProxyService/BatchExecuteCmd") => {
                match parse_json::<BatchExecuteRequest>(&request.body) {
                    Ok(req) => json_response(200, &self.batch_execute(req)),
                    Err(err) => {
                        self.inc_bad_request();
                        json_response(400, &Status::error("bad_request", err.to_string()))
                    }
                }
            }
            ("POST", "/proxy/open_table")
            | ("POST", "/tables/open")
            | ("POST", "/ProxyService/OpenTable") => {
                match parse_json::<ProxyOpenTableRequest>(&request.body) {
                    Ok(req) => json_response(200, &self.open_table(req)),
                    Err(err) => {
                        self.inc_bad_request();
                        json_response(
                            400,
                            &ProxyOpenTableResponse {
                                status: Status::error("bad_request", err.to_string()),
                                options: None,
                            },
                        )
                    }
                }
            }
            ("POST", "/proxy/table_execute")
            | ("POST", "/table_execute")
            | ("POST", "/ProxyService/TableExecuteCmd")
            | ("POST", "/ProxyService/ExecuteTableCmd") => {
                match parse_json::<ProxyTableExecuteRequest>(&request.body) {
                    Ok(req) => json_response(200, &self.table_execute(req)),
                    Err(err) => {
                        self.inc_bad_request();
                        json_response(400, &execute_error("bad_request", err.to_string()))
                    }
                }
            }
            ("POST", "/proxy/table_batch_execute")
            | ("POST", "/table_batch_execute")
            | ("POST", "/ProxyService/TableBatchExecuteCmd")
            | ("POST", "/ProxyService/BatchExecuteTableCmd") => {
                match parse_json::<ProxyTableBatchExecuteRequest>(&request.body) {
                    Ok(req) => json_response(200, &self.table_batch_execute(req)),
                    Err(err) => {
                        self.inc_bad_request();
                        json_response(400, &Status::error("bad_request", err.to_string()))
                    }
                }
            }
            _ => json_response(404, &Status::error("not_found", "unknown proxy route")),
        }
    }

    pub fn execute(&self, request: ExecuteRequest) -> ExecuteResponse {
        self.inner
            .stats
            .write()
            .expect("proxy stats lock poisoned")
            .execute_requests += 1;
        if let Some(status) =
            self.check_admission_for_commands(std::slice::from_ref(&request.command))
        {
            return execute_error(status.code, status.message);
        }
        let response = self
            .client()
            .execute_with_options(request, RequestOptions::default())
            .unwrap_or_else(|err| execute_error("server_error", err.to_string()));
        self.sync_client_stats();
        response
    }

    pub fn batch_execute(&self, request: BatchExecuteRequest) -> BatchExecuteResponse {
        self.inner
            .stats
            .write()
            .expect("proxy stats lock poisoned")
            .batch_execute_requests += 1;
        if let Some(status) = self.check_admission_for_commands(&request.commands) {
            return BatchExecuteResponse {
                status,
                responses: Vec::new(),
            };
        }
        let response = self
            .client()
            .batch_execute_with_options(request, RequestOptions::default())
            .unwrap_or_else(|err| BatchExecuteResponse {
                status: Status::error("server_error", err.to_string()),
                responses: Vec::new(),
            });
        self.sync_client_stats();
        response
    }

    pub fn open_table(&self, request: ProxyOpenTableRequest) -> ProxyOpenTableResponse {
        match self
            .client()
            .open_table_from_meta(request.namespace, request.table_name)
        {
            Ok(table) => {
                if request.pin_primary.is_some() || request.replica_read_policy.is_some() {
                    let mut options = table.options();
                    if let Some(pin_primary) = request.pin_primary {
                        options.pin_primary = pin_primary;
                    }
                    if let Some(policy) = request.replica_read_policy {
                        options.replica_read_policy = policy.into();
                    }
                    let table = self.client().open_table(
                        table.namespace().to_string(),
                        table.table_name().to_string(),
                        options.clone(),
                    );
                    self.sync_client_stats();
                    return ProxyOpenTableResponse {
                        status: Status::ok(),
                        options: Some(table.options().into()),
                    };
                }
                let options = table.options();
                self.sync_client_stats();
                ProxyOpenTableResponse {
                    status: Status::ok(),
                    options: Some(options.into()),
                }
            }
            Err(err) => {
                self.sync_client_stats();
                ProxyOpenTableResponse {
                    status: Status::error("metaserver_error", err.to_string()),
                    options: None,
                }
            }
        }
    }

    pub fn table_execute(&self, request: ProxyTableExecuteRequest) -> ExecuteResponse {
        self.inner
            .stats
            .write()
            .expect("proxy stats lock poisoned")
            .execute_requests += 1;
        if let Some(status) =
            self.check_admission_for_commands(std::slice::from_ref(&request.command))
        {
            return execute_error(status.code, status.message);
        }
        let response = self
            .table_for_request(request.namespace, request.table_name)
            .and_then(|table| table.execute(request.command))
            .unwrap_or_else(|err| execute_error("server_error", err.to_string()));
        self.sync_client_stats();
        response
    }

    pub fn table_batch_execute(
        &self,
        request: ProxyTableBatchExecuteRequest,
    ) -> BatchExecuteResponse {
        self.inner
            .stats
            .write()
            .expect("proxy stats lock poisoned")
            .batch_execute_requests += 1;
        if let Some(status) = self.check_admission_for_commands(&request.commands) {
            return BatchExecuteResponse {
                status,
                responses: Vec::new(),
            };
        }
        let response = self
            .table_for_request(request.namespace, request.table_name)
            .and_then(|table| table.batch_execute(request.commands))
            .unwrap_or_else(|err| BatchExecuteResponse {
                status: Status::error("server_error", err.to_string()),
                responses: Vec::new(),
            });
        self.sync_client_stats();
        response
    }

    fn table_for_request(
        &self,
        namespace: String,
        table_name: String,
    ) -> Result<crate::client::TemporalStoreTable, crate::client::ClientError> {
        let client = self.client();
        client
            .cached_table(namespace.clone(), table_name.clone())
            .map(Ok)
            .unwrap_or_else(|| client.open_table_from_meta(namespace, table_name))
    }

    pub fn update_options(&self, options: ProxyOptions) {
        let _ = self.update_options_report(options);
    }

    pub fn update_options_report(&self, options: ProxyOptions) -> ProxyConfigUpdateReport {
        let previous = self.options();
        let previous_config_version = proxy_config_version(&previous);
        let new_config_version = proxy_config_version(&options);
        let mut report = ProxyConfigUpdateReport {
            status: Status::ok(),
            applied: false,
            reason: "unchanged".to_string(),
            previous_namespace: previous.namespace.clone(),
            previous_config_version,
            new_namespace: options.namespace.clone(),
            new_config_version,
        };
        if previous.namespace == options.namespace
            && previous_config_version == new_config_version
            && previous.serving_mode == options.serving_mode
            && previous.drop_percent == options.drop_percent
        {
            return report;
        }

        *self
            .inner
            .client
            .write()
            .expect("proxy client lock poisoned") = proxy_client_from_options(&options);
        *self
            .inner
            .last_client_stats
            .write()
            .expect("proxy client stats lock poisoned") = ClientStats::default();
        *self
            .inner
            .options
            .write()
            .expect("proxy options lock poisoned") = options;
        report.applied = true;
        report.reason = "config_changed".to_string();
        report
    }

    pub fn info(&self) -> ProxyInfo {
        let options = self.options();
        ProxyInfo {
            status: Status::ok(),
            meta_addr: options.meta_addr,
            route_cache_size: self.client().route_cache_size(),
            stats: *self.inner.stats.read().expect("proxy stats lock poisoned"),
            boot_time_ms: self.inner.boot_time_ms,
        }
    }

    pub fn heartbeat_report(&self) -> ProxyHeartbeatReport {
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
        let authoritative_topology_version = topology_cache.authoritative_topology_version;
        let topology_cache_stale = topology_cache.cache_stale;
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
        if options.serving_mode != ProxyServingMode::Serving {
            degraded_reasons.push(format!("serving_mode:{:?}", options.serving_mode));
        }
        if topology_cache_stale {
            degraded_reasons.push("topology_cache_stale".to_string());
        }
        if !topology_check_status.ok {
            degraded_reasons.push("topology_check_failed".to_string());
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
            meta_addr: options.meta_addr,
            proxy_addr: options.proxy_addr,
            namespace: options.namespace,
            config_version,
            route_cache_size,
            authoritative_topology_version,
            topology_cache_stale,
            topology_check_status: Some(topology_check_status),
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
            degraded_reasons,
        }
    }

    pub fn policy_report(&self) -> ProxyPolicyReport {
        let options = self.options();
        let stats = *self.inner.stats.read().expect("proxy stats lock poisoned");
        ProxyPolicyReport {
            serving_mode: options.serving_mode,
            drop_percent: options.drop_percent.min(100),
            serving_reads: !matches!(options.serving_mode, ProxyServingMode::NotServing),
            serving_writes: matches!(
                options.serving_mode,
                ProxyServingMode::Serving | ProxyServingMode::Degraded
            ),
            rejecting_all: matches!(options.serving_mode, ProxyServingMode::NotServing),
            admission_rejections: stats.admission_rejections,
        }
    }

    pub fn refresh_topology_from_meta(&self) -> ProxyTopologyRefreshResponse {
        match self.client().refresh_stale_routes_from_meta() {
            Ok(report) => ProxyTopologyRefreshResponse {
                status: report.status.clone(),
                report: Some(report),
            },
            Err(err) => {
                self.inner
                    .stats
                    .write()
                    .expect("proxy stats lock poisoned")
                    .metaserver_errors += 1;
                ProxyTopologyRefreshResponse {
                    status: Status::error("refresh_failed", err.to_string()),
                    report: None,
                }
            }
        }
    }

    fn fetch_meta_topology_version(
        &self,
        old_topology_version: u64,
    ) -> Result<TopologyVersionReport, Status> {
        let options = self.options();
        post_json_with_options::<_, TopologyVersionReport>(
            &options.meta_addr,
            "/meta/topology_version",
            &TopologyVersionRequest {
                old_topology_version,
            },
            options.http_options(),
        )
        .map_err(|err| Status::error("topology_check_failed", err.to_string()))
    }

    pub fn heartbeat_to_meta(&self) -> ProxyHeartbeatResponse {
        let options = self.options();
        self.inner
            .stats
            .write()
            .expect("proxy stats lock poisoned")
            .heartbeat_total += 1;
        let request = ProxyHeartbeatRequest {
            proxy_addr: options.proxy_addr.clone(),
            namespace: options.namespace.clone(),
            config_version: proxy_config_version(&options),
            binary_version: options.binary_version.clone(),
        };
        match post_json_with_options::<_, ProxyHeartbeatResponse>(
            &options.meta_addr,
            "/proxies/heartbeat",
            &request,
            options.http_options(),
        ) {
            Ok(response) if response.status.ok || response.status.code == "resource_frozen" => {
                self.apply_heartbeat_config(&response);
                response
            }
            Ok(response) if response.status.code == "not_found" => {
                if self.auto_register_proxy(&options).status.ok {
                    let response = post_json_with_options::<_, ProxyHeartbeatResponse>(
                        &options.meta_addr,
                        "/proxies/heartbeat",
                        &request,
                        options.http_options(),
                    )
                    .unwrap_or_else(|err| ProxyHeartbeatResponse {
                        status: Status::error("metaserver_error", err.to_string()),
                        config_changed: false,
                        namespace: String::new(),
                        config_version: 0,
                        serving_mode: "not_serving".to_string(),
                        drop_percent: 0,
                    });
                    if response.status.ok {
                        self.apply_heartbeat_config(&response);
                    }
                    response
                } else {
                    response
                }
            }
            Ok(response) => response,
            Err(err) => ProxyHeartbeatResponse {
                status: Status::error("metaserver_error", err.to_string()),
                config_changed: false,
                namespace: String::new(),
                config_version: 0,
                serving_mode: "not_serving".to_string(),
                drop_percent: 0,
            },
        }
    }

    pub fn start_heartbeat_loop(&self, interval_ms: u64) -> thread::JoinHandle<()> {
        let service = self.clone();
        let interval = Duration::from_millis(interval_ms.max(1));
        thread::spawn(move || loop {
            let _ = service.heartbeat_to_meta();
            thread::sleep(interval);
        })
    }

    fn auto_register_proxy(&self, options: &ProxyOptions) -> AckResponse {
        self.inner
            .stats
            .write()
            .expect("proxy stats lock poisoned")
            .auto_register_total += 1;
        post_json_with_options::<_, AckResponse>(
            &options.meta_addr,
            "/proxies/register",
            &RegisterProxyRequest {
                proxy_addr: options.proxy_addr.clone(),
                namespace: options.namespace.clone(),
                location: options.location.clone(),
                config_version: proxy_config_version(options),
                binary_version: options.binary_version.clone(),
            },
            options.http_options(),
        )
        .unwrap_or_else(|err| AckResponse {
            status: Status::error("metaserver_error", err.to_string()),
        })
    }

    fn apply_heartbeat_config(&self, response: &ProxyHeartbeatResponse) {
        let serving_mode = proxy_serving_mode_from_meta(&response.serving_mode);
        let policy_changed = {
            let options = self.options();
            serving_mode.is_some_and(|mode| mode != options.serving_mode)
                || response.drop_percent <= 100 && response.drop_percent != options.drop_percent
        };
        if !response.config_changed && !policy_changed {
            return;
        }
        let mut options = self.options();
        if !response.namespace.is_empty() {
            options.namespace = response.namespace.clone();
        }
        if response.config_version != 0 {
            options.config_version = response.config_version;
        }
        if let Some(serving_mode) = serving_mode {
            options.serving_mode = serving_mode;
        }
        if response.drop_percent <= 100 {
            options.drop_percent = response.drop_percent;
        }
        let _ = self.update_options_report(options);
    }

    fn get_shard(&self, shard_id: ShardId, count_error: bool) -> Result<GetShardResponse, Status> {
        let options = self.options();
        get_json_with_options::<GetShardResponse>(
            &options.meta_addr,
            &format!("/shards/{shard_id}"),
            options.http_options(),
        )
        .map_err(|err| {
            if count_error {
                self.inner
                    .stats
                    .write()
                    .expect("proxy stats lock poisoned")
                    .metaserver_errors += 1;
            }
            Status::error("metaserver_error", err.to_string())
        })
        .and_then(|response| {
            if response.status.ok {
                Ok(response)
            } else {
                Err(response.status)
            }
        })
    }

    fn client(&self) -> TemporalStoreClient {
        self.inner
            .client
            .read()
            .expect("proxy client lock poisoned")
            .clone()
    }

    fn sync_client_stats(&self) {
        let current = self.client().stats();
        let mut last = self
            .inner
            .last_client_stats
            .write()
            .expect("proxy client stats lock poisoned");
        let mut stats = self.inner.stats.write().expect("proxy stats lock poisoned");
        stats.route_cache_hits += current
            .route_cache_hits
            .saturating_sub(last.route_cache_hits);
        stats.route_cache_misses += current
            .route_cache_misses
            .saturating_sub(last.route_cache_misses);
        stats.route_refreshes += current.route_refreshes.saturating_sub(last.route_refreshes);
        stats.backend_errors += current.backend_errors.saturating_sub(last.backend_errors);
        stats.continuous_backend_failures += current
            .continuous_backend_failures
            .saturating_sub(last.continuous_backend_failures);
        *last = current;
    }

    fn options(&self) -> ProxyOptions {
        self.inner
            .options
            .read()
            .expect("proxy options lock poisoned")
            .clone()
    }

    fn check_admission_for_commands(&self, commands: &[Command]) -> Option<Status> {
        let options = self.options();
        let status = proxy_policy_rejection(&options, commands);
        if status.is_some() {
            self.inner
                .stats
                .write()
                .expect("proxy stats lock poisoned")
                .admission_rejections += 1;
        }
        status
    }

    fn inc_bad_request(&self) {
        self.inner
            .stats
            .write()
            .expect("proxy stats lock poisoned")
            .bad_requests += 1;
    }
}

fn execute_error(code: impl Into<String>, message: impl Into<String>) -> ExecuteResponse {
    ExecuteResponse {
        status: Status::error(code, message),
        response: CommandResponse::Empty,
    }
}

fn proxy_policy_rejection(options: &ProxyOptions, commands: &[Command]) -> Option<Status> {
    if matches!(options.serving_mode, ProxyServingMode::NotServing) {
        return Some(Status::error("proxy_not_serving", "proxy is not serving"));
    }
    let has_write = commands.iter().any(proxy_command_is_write);
    if has_write
        && matches!(
            options.serving_mode,
            ProxyServingMode::Readonly | ProxyServingMode::WriteDisabled
        )
    {
        return Some(Status::error(
            "proxy_write_disabled",
            "proxy is not accepting writes",
        ));
    }
    let drop_percent = options.drop_percent.min(100);
    if drop_percent > 0
        && commands
            .iter()
            .filter_map(proxy_command_key)
            .any(|key| key_is_dropped_by_percent(key, drop_percent))
    {
        return Some(Status::error(
            "proxy_traffic_dropped",
            "request dropped by proxy drop_percent",
        ));
    }
    None
}

fn proxy_serving_mode_from_meta(value: &str) -> Option<ProxyServingMode> {
    match value.to_ascii_lowercase().replace('-', "_").as_str() {
        "" => None,
        "serving" => Some(ProxyServingMode::Serving),
        "readonly" | "read_only" => Some(ProxyServingMode::Readonly),
        "write_disabled" => Some(ProxyServingMode::WriteDisabled),
        "degraded" => Some(ProxyServingMode::Degraded),
        "not_serving" | "disabled" | "frozen" | "dropped" => Some(ProxyServingMode::NotServing),
        _ => None,
    }
}

fn proxy_command_is_write(command: &Command) -> bool {
    matches!(
        command,
        Command::CommonDelete { .. }
            | Command::CommonExpire { .. }
            | Command::StringSet { .. }
            | Command::StringSetEx { .. }
            | Command::StringSetConditional { .. }
            | Command::StringDelete { .. }
            | Command::HashSet { .. }
            | Command::HashMultiSet { .. }
            | Command::HashIncrBy { .. }
            | Command::HashDelete { .. }
            | Command::SetAdd { .. }
            | Command::SetRemove { .. }
            | Command::FeatureAppend { .. }
            | Command::FeatureAppendWithPolicy { .. }
            | Command::FeatureReplace { .. }
            | Command::FeatureDelete { .. }
            | Command::SequenceAdd { .. }
            | Command::IpsAdd { .. }
            | Command::IpsAddWithOptions { .. }
            | Command::IpsLoad { .. }
            | Command::IpsRemove { .. }
            | Command::IpsDelete { .. }
            | Command::RiskIncrement { .. }
            | Command::RiskIncrementWithOptions { .. }
            | Command::RiskChangeAdd { .. }
            | Command::RiskSet { .. }
            | Command::RiskSetAndGet { .. }
            | Command::RiskFolSet { .. }
    )
}

fn proxy_command_key(command: &Command) -> Option<&str> {
    match command {
        Command::CommonDelete { key }
        | Command::CommonExpire { key, .. }
        | Command::CommonTtl { key }
        | Command::CommonExists { key }
        | Command::StringSet { key, .. }
        | Command::StringSetEx { key, .. }
        | Command::StringSetConditional { key, .. }
        | Command::StringGet { key }
        | Command::StringDelete { key }
        | Command::HashSet { key, .. }
        | Command::HashGet { key, .. }
        | Command::HashMultiGet { key, .. }
        | Command::HashMultiSet { key, .. }
        | Command::HashIncrBy { key, .. }
        | Command::HashGetAll { key }
        | Command::HashLen { key }
        | Command::HashDelete { key, .. }
        | Command::SetAdd { key, .. }
        | Command::SetMembers { key }
        | Command::SetRemove { key, .. }
        | Command::FeatureAppend { key, .. }
        | Command::FeatureAppendWithPolicy { key, .. }
        | Command::FeatureQuery { key, .. }
        | Command::FeatureQueryFiltered { key, .. }
        | Command::FeatureReplace { key, .. }
        | Command::FeatureDelete { key }
        | Command::FeatureAggQuery { key, .. }
        | Command::SequenceAdd { key, .. }
        | Command::SequenceQuery { key, .. }
        | Command::IpsAdd { key, .. }
        | Command::IpsAddWithOptions { key, .. }
        | Command::IpsLoad { key, .. }
        | Command::IpsQueryLast { key, .. }
        | Command::IpsQueryRange { key, .. }
        | Command::IpsQueryRangeWithOptions { key, .. }
        | Command::IpsSnapshot { key, .. }
        | Command::IpsStat { key, .. }
        | Command::IpsFilter { key, .. }
        | Command::IpsRemove { key, .. }
        | Command::IpsDelete { key }
        | Command::IpsCount { key, .. }
        | Command::RiskIncrement { key, .. }
        | Command::RiskIncrementWithOptions { key, .. }
        | Command::RiskChangeAdd { key, .. }
        | Command::RiskCount { key, .. }
        | Command::RiskQuery { key, .. }
        | Command::RiskDetail { key, .. }
        | Command::RiskSet { key, .. }
        | Command::RiskSetAndGet { key, .. }
        | Command::RiskFamilyQuery { key, .. }
        | Command::RiskFolSet { key, .. }
        | Command::RiskFolQuery { key }
        | Command::RiskManager { key } => Some(key),
        Command::IpsBatchQueryLast { .. } | Command::SequenceBatchQuery { .. } => None,
    }
}

fn default_proxy_addr() -> String {
    "127.0.0.1:17000".to_string()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn proxy_client_from_options(options: &ProxyOptions) -> TemporalStoreClient {
    TemporalStoreClient::with_options(ClientOptions {
        proxy_addr: options.proxy_addr.clone(),
        meta_addr: Some(options.meta_addr.clone()),
        connect_timeout_ms: options.connect_timeout_ms,
        io_timeout_ms: options.io_timeout_ms,
        max_retries: options.max_retries,
        route_cache_ttl_ms: options.route_cache_ttl_ms,
        topo_error_retry_interval_ms: options.backend_continuous_failed_time_ms,
        drop_percent: options.drop_percent.min(100),
        ..ClientOptions::default()
    })
}

fn proxy_config_version(options: &ProxyOptions) -> u64 {
    if options.config_version != 0 {
        return options.config_version;
    }
    let mut version = 1469598103934665603u64;
    let view = ProxyConfigHashView {
        meta_addr: &options.meta_addr,
        proxy_addr: &options.proxy_addr,
        namespace: &options.namespace,
        location: &options.location,
        binary_version: &options.binary_version,
        route_cache_ttl_ms: options.route_cache_ttl_ms,
        connect_timeout_ms: options.connect_timeout_ms,
        io_timeout_ms: options.io_timeout_ms,
        max_retries: options.max_retries,
        refresh_route_on_backend_error: options.refresh_route_on_backend_error,
        backend_continuous_failed_time_ms: options.backend_continuous_failed_time_ms,
    };
    for byte in serde_json::to_vec(&view).unwrap_or_default() {
        version ^= byte as u64;
        version = version.wrapping_mul(1099511628211);
    }
    version
}

#[derive(Serialize)]
struct ProxyConfigHashView<'a> {
    meta_addr: &'a str,
    proxy_addr: &'a str,
    namespace: &'a str,
    location: &'a str,
    binary_version: &'a str,
    route_cache_ttl_ms: u64,
    connect_timeout_ms: u64,
    io_timeout_ms: u64,
    max_retries: usize,
    refresh_route_on_backend_error: bool,
    backend_continuous_failed_time_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::TemporalEngine;
    use crate::http::{json_response, parse_json, serve};
    use crate::meta::{
        AddTableRequest, GetTableTopologyRequest, RegisterShardRequest, ShardLocation,
        UpdateTableRequest,
    };
    use crate::types::Command;
    use crate::ProductionReadinessReport;
    use std::net::TcpListener;
    use std::time::Instant;

    #[test]
    fn proxy_exposes_cpp_parity_readiness_report() {
        let proxy = ProxyService::new(ProxyOptions {
            meta_addr: "127.0.0.1:1".to_string(),
            ..ProxyOptions::default()
        });

        for path in ["/readiness", "/cpp_parity"] {
            let (code, body) = proxy.handle(HttpRequest {
                method: "GET".to_string(),
                path: path.to_string(),
                body: Vec::new(),
            });
            assert_eq!(code, 200);
            let report = parse_json::<ProductionReadinessReport>(&body).unwrap();
            assert!(!report.production_ready);
            assert!(!report.cpp_parity_ready);
            assert!(report.missing_count() > 0);
        }
    }

    #[test]
    fn proxy_caches_route_and_forwards_execute() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        start_server(test_addr(18_310), engine.clone());
        start_meta(test_addr(18_311), test_addr(18_310));
        wait_for_http(&test_addr(18_311));
        wait_for_http(&test_addr(18_310));

        let proxy = ProxyService::new(ProxyOptions {
            meta_addr: test_addr(18_311),
            route_cache_ttl_ms: 60_000,
            ..ProxyOptions::default()
        });
        assert!(
            proxy
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "k".to_string(),
                        value: b"v".to_vec(),
                    },
                })
                .status
                .ok
        );
        assert_eq!(
            proxy
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "k".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"v".to_vec())
            }
        );
        let info = proxy.info();
        assert_eq!(info.route_cache_size, 1);
        assert_eq!(info.stats.route_refreshes, 1);
        assert!(info.stats.route_cache_hits >= 1);
        let heartbeat = proxy.heartbeat_report();
        assert_eq!(heartbeat.route_cache_size, 1);
        assert_eq!(heartbeat.stats.execute_requests, 2);
        assert_ne!(heartbeat.config_version, 0);
        let preflight = proxy.preflight_report();
        assert!(preflight.status.ok);
        assert_eq!(preflight.route_cache_size, 1);
        assert_eq!(preflight.client.route_cache_size, 1);
        assert_eq!(preflight.client.topology_cache.route_count, 1);
        assert_eq!(
            preflight
                .client
                .topology_cache
                .unknown_topology_version_routes,
            1
        );
        assert!(preflight.client.route_refreshes >= 1);
        assert!(preflight.degraded_reasons.is_empty());
    }

    #[test]
    fn proxy_preflight_reports_degraded_bad_request_state() {
        let proxy = ProxyService::new(ProxyOptions {
            meta_addr: "127.0.0.1:1".to_string(),
            namespace: "ns".to_string(),
            ..ProxyOptions::default()
        });

        let (code, _) = proxy.handle(HttpRequest {
            method: "POST".to_string(),
            path: "/execute".to_string(),
            body: b"not-json".to_vec(),
        });
        assert_eq!(code, 400);

        let preflight = proxy.preflight_report();
        assert!(!preflight.status.ok);
        assert_eq!(preflight.status.code, "degraded");
        assert_eq!(preflight.namespace, "ns");
        assert_eq!(preflight.stats.bad_requests, 1);
        assert_eq!(preflight.degraded_reasons, vec!["bad_requests"]);

        let (code, body) = proxy.handle(HttpRequest {
            method: "GET".to_string(),
            path: "/ProxyService/Preflight".to_string(),
            body: Vec::new(),
        });
        assert_eq!(code, 200);
        let routed = parse_json::<ProxyPreflightReport>(&body).unwrap();
        assert_eq!(routed.stats.bad_requests, 1);
    }

    #[test]
    fn proxy_policy_blocks_writes_not_serving_and_drop_percent() {
        let readonly = ProxyService::new(ProxyOptions {
            meta_addr: "127.0.0.1:1".to_string(),
            serving_mode: ProxyServingMode::Readonly,
            ..ProxyOptions::default()
        });
        let write = readonly.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        assert_eq!(write.status.code, "proxy_write_disabled");
        assert_eq!(readonly.policy_report().admission_rejections, 1);
        let preflight = readonly.preflight_report();
        assert_eq!(preflight.policy.serving_mode, ProxyServingMode::Readonly);
        assert!(!preflight.policy.serving_writes);
        assert!(preflight
            .degraded_reasons
            .iter()
            .any(|reason| reason == "admission_rejections"));
        assert!(preflight
            .degraded_reasons
            .iter()
            .any(|reason| reason == "serving_mode:Readonly"));

        let not_serving = ProxyService::new(ProxyOptions {
            meta_addr: "127.0.0.1:1".to_string(),
            serving_mode: ProxyServingMode::NotServing,
            ..ProxyOptions::default()
        });
        let read = not_serving.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        assert_eq!(read.status.code, "proxy_not_serving");
        assert!(not_serving.policy_report().rejecting_all);

        let dropper = ProxyService::new(ProxyOptions {
            meta_addr: "127.0.0.1:1".to_string(),
            drop_percent: 100,
            ..ProxyOptions::default()
        });
        let dropped = dropper.table_batch_execute(ProxyTableBatchExecuteRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            commands: vec![Command::StringGet {
                key: "drop-me".to_string(),
            }],
        });
        assert_eq!(dropped.status.code, "proxy_traffic_dropped");

        let (code, body) = dropper.handle(HttpRequest {
            method: "GET".to_string(),
            path: "/ProxyService/GetPolicy".to_string(),
            body: Vec::new(),
        });
        assert_eq!(code, 200);
        let policy = parse_json::<ProxyPolicyReport>(&body).unwrap();
        assert_eq!(policy.drop_percent, 100);
        assert_eq!(policy.admission_rejections, 1);
    }

    #[test]
    fn proxy_cpp_admin_aliases_expose_info_config_and_client_preflight() {
        let proxy = ProxyService::new(ProxyOptions {
            meta_addr: "127.0.0.1:1".to_string(),
            proxy_addr: "127.0.0.1:17000".to_string(),
            namespace: "ns".to_string(),
            ..ProxyOptions::default()
        });

        let (code, body) = proxy.handle(HttpRequest {
            method: "GET".to_string(),
            path: "/ProxyService/GetInfo".to_string(),
            body: Vec::new(),
        });
        assert_eq!(code, 200);
        let info = parse_json::<ProxyInfo>(&body).unwrap();
        assert_eq!(info.meta_addr, "127.0.0.1:1");

        let (code, body) = proxy.handle(HttpRequest {
            method: "GET".to_string(),
            path: "/ProxyService/GetConfig".to_string(),
            body: Vec::new(),
        });
        assert_eq!(code, 200);
        let config = parse_json::<ProxyOptions>(&body).unwrap();
        assert_eq!(config.namespace, "ns");

        let (code, body) = proxy.handle(HttpRequest {
            method: "GET".to_string(),
            path: "/ProxyService/ClientPreflight".to_string(),
            body: Vec::new(),
        });
        assert_eq!(code, 200);
        let client = parse_json::<crate::ClientPreflightReport>(&body).unwrap();
        assert!(client.status.ok);
        assert_eq!(client.proxy_addr, "127.0.0.1:17000");
    }

    #[test]
    fn proxy_cpp_service_aliases_delegate_to_client_execution_path() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        start_server(test_addr(18_321), engine.clone());
        start_meta(test_addr(18_322), test_addr(18_321));
        wait_for_http(&test_addr(18_321));
        wait_for_http(&test_addr(18_322));

        let proxy = ProxyService::new(ProxyOptions {
            meta_addr: test_addr(18_322),
            route_cache_ttl_ms: 60_000,
            ..ProxyOptions::default()
        });
        let (code, body) = proxy.handle(HttpRequest {
            method: "POST".to_string(),
            path: "/ProxyService/ExecuteCmd".to_string(),
            body: serde_json::to_vec(&ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "cpp-proxy-alias".to_string(),
                    value: b"via-proxy-service".to_vec(),
                },
            })
            .unwrap(),
        });
        assert_eq!(code, 200);
        let response = parse_json::<ExecuteResponse>(&body).unwrap();
        assert!(response.status.ok);

        let (code, body) = proxy.handle(HttpRequest {
            method: "POST".to_string(),
            path: "/ProxyService/BatchExecuteCmd".to_string(),
            body: serde_json::to_vec(&BatchExecuteRequest {
                shard_id: 1,
                commands: vec![
                    Command::StringGet {
                        key: "cpp-proxy-alias".to_string(),
                    },
                    Command::HashSet {
                        key: "cpp-proxy-hash".to_string(),
                        field: "field".to_string(),
                        value: b"value".to_vec(),
                    },
                    Command::HashGet {
                        key: "cpp-proxy-hash".to_string(),
                        field: "field".to_string(),
                    },
                ],
            })
            .unwrap(),
        });
        assert_eq!(code, 200);
        let response = parse_json::<BatchExecuteResponse>(&body).unwrap();
        assert!(response.status.ok);
        assert!(response.responses.iter().all(|item| item.status.ok));
        assert_eq!(
            response
                .responses
                .into_iter()
                .map(|item| item.response)
                .collect::<Vec<_>>(),
            vec![
                CommandResponse::Bytes {
                    value: Some(b"via-proxy-service".to_vec())
                },
                CommandResponse::Empty,
                CommandResponse::Bytes {
                    value: Some(b"value".to_vec())
                },
            ]
        );
        let info = proxy.info();
        assert_eq!(info.stats.execute_requests, 1);
        assert_eq!(info.stats.batch_execute_requests, 1);
        assert!(info.stats.route_cache_hits >= 1);
    }

    #[test]
    fn proxy_config_update_noops_on_same_namespace_and_version_like_cpp() {
        let options = ProxyOptions {
            namespace: "ns-a".to_string(),
            meta_addr: "127.0.0.1:1".to_string(),
            route_cache_ttl_ms: 60_000,
            ..ProxyOptions::default()
        };
        let proxy = ProxyService::new(options.clone());

        let unchanged = proxy.update_options_report(options.clone());
        assert!(unchanged.status.ok);
        assert!(!unchanged.applied);
        assert_eq!(unchanged.reason, "unchanged");
        assert_eq!(unchanged.previous_namespace, "ns-a");
        assert_eq!(unchanged.new_namespace, "ns-a");
        assert_eq!(
            unchanged.previous_config_version,
            unchanged.new_config_version
        );
        assert_eq!(proxy.options().route_cache_ttl_ms, 60_000);

        let changed = proxy.update_options_report(ProxyOptions {
            namespace: "ns-b".to_string(),
            ..options
        });
        assert!(changed.status.ok);
        assert!(changed.applied);
        assert_eq!(changed.reason, "config_changed");
        assert_eq!(changed.previous_namespace, "ns-a");
        assert_eq!(changed.new_namespace, "ns-b");
        assert_eq!(proxy.options().namespace, "ns-b");
    }

    #[test]
    fn proxy_refreshes_route_after_backend_failure() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        start_server(test_addr(18_312), engine.clone());
        start_meta(test_addr(18_313), test_addr(18_312));
        wait_for_http(&test_addr(18_313));
        wait_for_http(&test_addr(18_312));

        let proxy = ProxyService::new(ProxyOptions {
            meta_addr: test_addr(18_313),
            route_cache_ttl_ms: 60_000,
            connect_timeout_ms: 50,
            io_timeout_ms: 200,
            ..ProxyOptions::default()
        });
        proxy
            .client()
            .insert_cached_route_for_test(1, "127.0.0.1:1");

        let response = proxy.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        assert!(response.status.ok);
        let info = proxy.info();
        assert_eq!(info.stats.backend_errors, 1);
        assert_eq!(info.stats.route_refreshes, 1);
    }

    #[test]
    fn proxy_skips_cached_backend_after_continuous_failure_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        start_server(test_addr(18_316), engine.clone());
        start_meta(test_addr(18_317), test_addr(18_316));
        wait_for_http(&test_addr(18_317));
        wait_for_http(&test_addr(18_316));

        let bad_server = "127.0.0.1:1".to_string();
        let proxy = ProxyService::new(ProxyOptions {
            meta_addr: test_addr(18_317),
            route_cache_ttl_ms: 60_000,
            backend_continuous_failed_time_ms: 5,
            connect_timeout_ms: 50,
            io_timeout_ms: 200,
            ..ProxyOptions::default()
        });
        proxy
            .client()
            .insert_cached_route_for_test(1, bad_server.clone());
        proxy
            .client()
            .insert_backend_failure_for_test(bad_server, 20, 10, 3);

        let response = proxy.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        assert!(response.status.ok);
        assert_eq!(
            proxy
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "k".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"v".to_vec())
            }
        );
        let info = proxy.info();
        assert_eq!(info.stats.backend_errors, 0);
        assert_eq!(info.stats.continuous_backend_failures, 1);
        assert!(info.stats.route_refreshes >= 1);
        assert!(info.stats.route_cache_hits >= 1);
    }

    #[test]
    fn proxy_table_execute_opens_topology_and_routes_by_key() {
        let dir_a = tempfile::tempdir().unwrap();
        let engine_a = TemporalEngine::with_local_dirs(
            1024,
            dir_a.path().join("cache"),
            dir_a.path().join("pages"),
            dir_a.path().join("indexes"),
        );
        engine_a.load_shard(1);
        start_server(test_addr(18_318), engine_a.clone());

        let dir_b = tempfile::tempdir().unwrap();
        let engine_b = TemporalEngine::with_local_dirs(
            1024,
            dir_b.path().join("cache"),
            dir_b.path().join("pages"),
            dir_b.path().join("indexes"),
        );
        engine_b.load_shard(2);
        start_server(test_addr(18_319), engine_b.clone());

        let meta = crate::meta::SingleNodeMeta::default();
        assert!(
            meta.add_table(AddTableRequest {
                namespace: "ns".to_string(),
                table_name: "tbl".to_string(),
                first_shard_id: 1,
                shard_count: 2,
                replica_count: 1,
                use_cpp_partition_ids: false,
                partition_version: 0,
                serving_options: crate::meta::TableServingOptions::default(),
            })
            .status
            .ok
        );
        assert!(
            meta.register(RegisterShardRequest {
                shard_id: 1,
                server_addr: test_addr(18_318),
            })
            .status
            .ok
        );
        assert!(
            meta.register(RegisterShardRequest {
                shard_id: 2,
                server_addr: test_addr(18_319),
            })
            .status
            .ok
        );
        start_meta_service(test_addr(18_320), meta);
        wait_for_http(&test_addr(18_318));
        wait_for_http(&test_addr(18_319));
        wait_for_http(&test_addr(18_320));

        let proxy = ProxyService::new(ProxyOptions {
            meta_addr: test_addr(18_320),
            route_cache_ttl_ms: 60_000,
            ..ProxyOptions::default()
        });
        let opened = proxy.open_table(ProxyOpenTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            pin_primary: None,
            replica_read_policy: None,
        });
        assert!(opened.status.ok);
        assert_eq!(
            opened.options,
            Some(ProxyTableOptionsView {
                first_shard_id: 1,
                shard_count: 2,
                pin_primary: true,
                replica_read_policy: ProxyReplicaReadPolicy::PinPrimary,
                preferred_location: String::new(),
                drop_percent: 0,
            })
        );

        let key = key_for_shard(2);
        assert!(
            proxy
                .table_execute(ProxyTableExecuteRequest {
                    namespace: "ns".to_string(),
                    table_name: "tbl".to_string(),
                    command: Command::StringSet {
                        key: key.clone(),
                        value: b"v2".to_vec(),
                    },
                })
                .status
                .ok
        );
        assert_eq!(
            engine_b
                .execute(ExecuteRequest {
                    shard_id: 2,
                    command: Command::StringGet { key: key.clone() },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"v2".to_vec())
            }
        );
        assert_eq!(
            engine_a
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet { key },
                })
                .response,
            CommandResponse::Bytes { value: None }
        );
        assert!(proxy.info().route_cache_size >= 2);
    }

    #[test]
    fn proxy_detects_and_refreshes_stale_topology_cache() {
        let meta = crate::meta::SingleNodeMeta::default();
        assert!(
            meta.add_table(AddTableRequest {
                namespace: "ns".to_string(),
                table_name: "tbl".to_string(),
                first_shard_id: 1,
                shard_count: 1,
                replica_count: 1,
                use_cpp_partition_ids: false,
                partition_version: 0,
                serving_options: crate::meta::TableServingOptions::default(),
            })
            .status
            .ok
        );
        assert!(
            meta.register(RegisterShardRequest {
                shard_id: 1,
                server_addr: test_addr(18_321),
            })
            .status
            .ok
        );
        let meta_addr = free_local_addr();
        start_meta_service(meta_addr.clone(), meta.clone());
        wait_for_http(&meta_addr);

        let proxy = ProxyService::new(ProxyOptions {
            meta_addr,
            route_cache_ttl_ms: 60_000,
            ..ProxyOptions::default()
        });
        let opened = proxy.open_table(ProxyOpenTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            pin_primary: None,
            replica_read_policy: None,
        });
        assert!(opened.status.ok, "{opened:?}");
        assert!(!proxy.preflight_report().topology_cache_stale);

        assert!(
            meta.update_table(UpdateTableRequest {
                namespace: "ns".to_string(),
                table_name: "tbl".to_string(),
                shard_count: Some(1),
                replica_count: Some(1),
                first_shard_id: None,
                use_cpp_partition_ids: None,
                partition_version: None,
                serving_options: Some(crate::meta::TableServingOptionsPatch {
                    drop_percent: Some(1),
                    ..crate::meta::TableServingOptionsPatch::default()
                }),
            })
            .status
            .ok
        );

        let stale = proxy.preflight_report();
        assert!(stale.topology_cache_stale);
        assert!(stale
            .degraded_reasons
            .contains(&"topology_cache_stale".to_string()));

        let refresh = proxy.refresh_topology_from_meta();
        assert!(refresh.status.ok, "{refresh:?}");
        let report = refresh.report.unwrap();
        assert_eq!(report.refreshed_tables, vec!["ns/tbl"]);
        assert!(!proxy.preflight_report().topology_cache_stale);

        let (code, body) = proxy.handle(HttpRequest {
            method: "POST".to_string(),
            path: "/proxy/topology/refresh".to_string(),
            body: Vec::new(),
        });
        assert_eq!(code, 200);
        let routed = parse_json::<ProxyTopologyRefreshResponse>(&body).unwrap();
        assert!(routed.status.ok);
    }

    #[test]
    fn proxy_heartbeat_auto_registers_when_metaserver_returns_not_found() {
        let meta = crate::meta::SingleNodeMeta::default();
        let meta_addr = test_addr(18_314);
        std::thread::spawn({
            let meta = meta.clone();
            move || {
                serve(&meta_addr, move |request| {
                    match (request.method.as_str(), request.path.as_str()) {
                        ("POST", "/proxies/heartbeat") => {
                            let req = parse_json::<ProxyHeartbeatRequest>(&request.body).unwrap();
                            json_response(200, &meta.proxy_heartbeat(req))
                        }
                        ("POST", "/proxies/register") => {
                            let req = parse_json::<RegisterProxyRequest>(&request.body).unwrap();
                            json_response(200, &meta.register_proxy(req))
                        }
                        _ => json_response(404, &Status::error("not_found", "not found")),
                    }
                })
                .unwrap();
            }
        });
        wait_for_http(&test_addr(18_314));

        let proxy = ProxyService::new(ProxyOptions {
            meta_addr: test_addr(18_314),
            proxy_addr: "proxy-a".to_string(),
            namespace: "ns".to_string(),
            location: "zone-a".to_string(),
            binary_version: "v1".to_string(),
            ..ProxyOptions::default()
        });
        let response = proxy.heartbeat_to_meta();
        assert!(response.status.ok);
        let info = proxy.info();
        assert_eq!(info.stats.heartbeat_total, 1);
        assert_eq!(info.stats.auto_register_total, 1);
    }

    #[test]
    fn proxy_heartbeat_applies_metaserver_config_version_like_cpp() {
        let meta = crate::meta::SingleNodeMeta::default();
        meta.register_proxy(RegisterProxyRequest {
            proxy_addr: "proxy-config".to_string(),
            namespace: "serving-ns".to_string(),
            location: "zone-a".to_string(),
            config_version: 9,
            binary_version: "v-meta".to_string(),
        });
        let meta_addr = test_addr(18_326);
        std::thread::spawn({
            let meta = meta.clone();
            move || {
                serve(&meta_addr, move |request| {
                    match (request.method.as_str(), request.path.as_str()) {
                        ("POST", "/proxies/heartbeat") => {
                            let req = parse_json::<ProxyHeartbeatRequest>(&request.body).unwrap();
                            json_response(200, &meta.proxy_heartbeat(req))
                        }
                        _ => json_response(404, &Status::error("not_found", "not found")),
                    }
                })
                .unwrap();
            }
        });
        wait_for_http(&test_addr(18_326));

        let proxy = ProxyService::new(ProxyOptions {
            meta_addr: test_addr(18_326),
            proxy_addr: "proxy-config".to_string(),
            namespace: "stale-ns".to_string(),
            config_version: 1,
            route_cache_ttl_ms: 60_000,
            ..ProxyOptions::default()
        });
        let response = proxy.heartbeat_to_meta();
        assert!(response.status.ok);
        assert!(response.config_changed);

        let options = proxy.options();
        assert_eq!(options.namespace, "serving-ns");
        assert_eq!(options.config_version, 9);
        assert_eq!(options.serving_mode, ProxyServingMode::Serving);
        assert_eq!(options.drop_percent, 0);
        assert_eq!(proxy_config_version(&options), 9);
        assert_eq!(proxy.info().stats.heartbeat_total, 1);
    }

    #[test]
    fn proxy_heartbeat_applies_metaserver_serving_policy_transition() {
        let meta = crate::meta::SingleNodeMeta::default();
        meta.register_proxy(RegisterProxyRequest {
            proxy_addr: "proxy-frozen-policy".to_string(),
            namespace: "policy-ns".to_string(),
            location: "zone-a".to_string(),
            config_version: 5,
            binary_version: "v-meta".to_string(),
        });
        meta.freeze_proxy(crate::meta::StateChangeRequest {
            endpoint: "proxy-frozen-policy".to_string(),
            freeze_cooldown_ms: 0,
        });
        let meta_addr = test_addr(18_327);
        std::thread::spawn({
            let meta = meta.clone();
            move || {
                serve(&meta_addr, move |request| {
                    match (request.method.as_str(), request.path.as_str()) {
                        ("POST", "/proxies/heartbeat") => {
                            let req = parse_json::<ProxyHeartbeatRequest>(&request.body).unwrap();
                            json_response(200, &meta.proxy_heartbeat(req))
                        }
                        _ => json_response(404, &Status::error("not_found", "not found")),
                    }
                })
                .unwrap();
            }
        });
        wait_for_http(&test_addr(18_327));

        let proxy = ProxyService::new(ProxyOptions {
            meta_addr: test_addr(18_327),
            proxy_addr: "proxy-frozen-policy".to_string(),
            namespace: "policy-ns".to_string(),
            config_version: 5,
            serving_mode: ProxyServingMode::Serving,
            route_cache_ttl_ms: 60_000,
            ..ProxyOptions::default()
        });
        let response = proxy.heartbeat_to_meta();
        assert_eq!(response.status.code, "resource_frozen");
        assert_eq!(response.serving_mode, "not_serving");
        let options = proxy.options();
        assert_eq!(options.serving_mode, ProxyServingMode::NotServing);

        let rejected = proxy.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "blocked".to_string(),
            },
        });
        assert_eq!(rejected.status.code, "proxy_not_serving");
    }

    #[test]
    fn proxy_background_heartbeat_loop_auto_registers() {
        let meta = crate::meta::SingleNodeMeta::default();
        let meta_addr = test_addr(18_315);
        std::thread::spawn({
            let meta = meta.clone();
            move || {
                serve(&meta_addr, move |request| {
                    match (request.method.as_str(), request.path.as_str()) {
                        ("POST", "/proxies/heartbeat") => {
                            let req = parse_json::<ProxyHeartbeatRequest>(&request.body).unwrap();
                            json_response(200, &meta.proxy_heartbeat(req))
                        }
                        ("POST", "/proxies/register") => {
                            let req = parse_json::<RegisterProxyRequest>(&request.body).unwrap();
                            json_response(200, &meta.register_proxy(req))
                        }
                        _ => json_response(404, &Status::error("not_found", "not found")),
                    }
                })
                .unwrap();
            }
        });
        wait_for_http(&test_addr(18_315));

        let proxy = ProxyService::new(ProxyOptions {
            meta_addr: test_addr(18_315),
            proxy_addr: "proxy-loop".to_string(),
            namespace: "ns".to_string(),
            ..ProxyOptions::default()
        });
        let _loop = proxy.start_heartbeat_loop(10);
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            let stats = proxy.info().stats;
            if stats.heartbeat_total > 0 && stats.auto_register_total > 0 {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("proxy heartbeat loop did not register and heartbeat");
    }

    fn start_server(addr: String, engine: TemporalEngine) {
        std::thread::spawn(move || {
            serve(&addr, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/execute") => {
                        let req = parse_json::<ExecuteRequest>(&request.body).unwrap();
                        json_response(200, &engine.execute(req))
                    }
                    ("POST", "/batch_execute") => {
                        let req = parse_json::<BatchExecuteRequest>(&request.body).unwrap();
                        json_response(200, &engine.batch_execute(req))
                    }
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });
    }

    fn start_meta(addr: String, server_addr: String) {
        std::thread::spawn(move || {
            serve(&addr, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("GET", "/shards/1") => json_response(
                        200,
                        &GetShardResponse {
                            status: Status::ok(),
                            location: Some(ShardLocation {
                                shard_id: 1,
                                server_addr: server_addr.clone(),
                                latest_snapshot: None,
                            }),
                        },
                    ),
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });
    }

    fn start_meta_service(addr: String, meta: crate::meta::SingleNodeMeta) {
        std::thread::spawn(move || {
            serve(&addr, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("GET", path) if path.starts_with("/shards/") => {
                        let shard_id = path
                            .trim_start_matches("/shards/")
                            .parse()
                            .unwrap_or_default();
                        json_response(200, &meta.get(shard_id))
                    }
                    ("POST", "/tables/topology") => {
                        let req = parse_json::<GetTableTopologyRequest>(&request.body).unwrap();
                        json_response(200, &meta.get_table_topology(req))
                    }
                    ("POST", "/meta/topology_version") => {
                        let req = parse_json::<TopologyVersionRequest>(&request.body).unwrap();
                        json_response(200, &meta.topology_version_report(req))
                    }
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });
    }

    fn key_for_shard(shard_id: ShardId) -> String {
        for index in 0..10_000 {
            let key = format!("table-key-{shard_id}-{index}");
            if crate::client::shard_id_for_key(&key, 1, 2, 1) == shard_id {
                return key;
            }
        }
        panic!("no key found for shard {shard_id}");
    }

    fn test_addr(port: u16) -> String {
        format!("127.0.0.1:{port}")
    }

    fn free_local_addr() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().to_string()
    }

    fn wait_for_http(addr: &str) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if std::net::TcpStream::connect(addr).is_ok() {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("server {addr} did not start");
    }
}
