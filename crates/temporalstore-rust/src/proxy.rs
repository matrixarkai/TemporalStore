// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use serde::{Deserialize, Serialize};

mod commands;
mod meta_sync;
mod prometheus;
mod reports;
mod handle;
mod config;
mod metrics;
mod policy;
mod response;

use config::{
    default_proxy_addr, default_service_registry_ttl_ms, now_ms, proxy_client_from_options,
    proxy_config_version,
};
use metrics::push_proxy_metric;
use policy::{proxy_policy_rejection, proxy_serving_mode_from_meta, proxy_serving_mode_label};
use response::execute_error;

use crate::client::{
    ClientStats, ClientTopologyCacheReport, ClientTopologyRefreshReport, ReplicaReadPolicy,
    RequestOptions, TableOptions, TemporalStoreClient,
};
use crate::http::{get_json_with_options, post_json_with_options, HttpRequest, HttpRequestOptions};
use crate::meta::GetShardResponse;
use crate::meta::{
    AckResponse, ProxyHeartbeatRequest, ProxyHeartbeatResponse, RegisterProxyRequest,
    TopologyVersionReport, TopologyVersionRequest,
};
use crate::types::{
    BatchExecuteRequest, BatchExecuteResponse, Command, ExecuteRequest, ExecuteResponse,
    FeatureFilter, FeatureWritePolicy, ControlStateSelectionType, ShardId, Status,
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
    #[serde(default = "default_service_registry_ttl_ms")]
    pub service_registry_ttl_ms: u64,
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
            service_registry_ttl_ms: default_service_registry_ttl_ms(),
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

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyServiceDiscoveryStats {
    pub heartbeat_success_total: u64,
    pub heartbeat_failure_total: u64,
    pub registration_success_total: u64,
    pub registration_failure_total: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyServiceDiscoveryReport {
    pub service_name: String,
    pub proxy_addr: String,
    pub namespace: String,
    pub location: String,
    pub meta_addr: String,
    pub ttl_ms: u64,
    pub registered: bool,
    pub stale: bool,
    pub last_heartbeat_age_ms: Option<u64>,
    pub last_success_ms: Option<u64>,
    pub last_error_ms: Option<u64>,
    pub last_error: Option<Status>,
    pub stats: ProxyServiceDiscoveryStats,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyPortsReport {
    pub listen_addr: String,
    pub announce_addr: String,
    pub listen_port: u16,
    pub announce_port: u16,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyConsulNamesReport {
    pub legacy_consul_in_scope: bool,
    pub rust_service_registry_names: Vec<String>,
    pub namespace: String,
    pub location: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyNotifyStopReport {
    pub status: Status,
    pub metaserver_notify_supported: bool,
    pub local_registry_marked_stopped: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyOperationalSurfaceEntry {
    pub cpp_surface: String,
    pub rust_native_route: String,
    pub rust_cpp_alias: String,
    pub covered: bool,
    pub notes: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyOperationalSurfaceReport {
    pub status: Status,
    pub legacy_brpc_thrift_in_scope: bool,
    pub rust_native_aliases_ready: bool,
    pub compared_cpp_files: Vec<String>,
    pub entries: Vec<ProxyOperationalSurfaceEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyMetricFamilyMapping {
    pub cpp_surface: String,
    pub rust_prometheus_family: String,
    pub rust_labels: Vec<String>,
    pub grafana_panel: String,
    pub covered: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyMetricsParityReport {
    pub status: Status,
    pub compared_cpp_files: Vec<String>,
    pub rust_prometheus_families: Vec<String>,
    pub mappings: Vec<ProxyMetricFamilyMapping>,
    pub grafana_panels_ready: bool,
    pub alerts_ready: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct ProxyServiceDiscoveryState {
    registered: bool,
    last_success_ms: Option<u64>,
    last_error_ms: Option<u64>,
    last_error: Option<Status>,
    stats: ProxyServiceDiscoveryStats,
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
    #[serde(default)]
    pub service_discovery: ProxyServiceDiscoveryReport,
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
pub struct ProxyTonicStreamingContract {
    pub service_name: String,
    pub execute_stream_method: String,
    pub route_callback_stream_method: String,
    pub preflight_watch_method: String,
    pub bidirectional_execute_stream: bool,
    pub callback_ack_required: bool,
    pub long_running_request_ready: bool,
    pub cancellation_ready: bool,
    pub backpressure_ready: bool,
    pub reconnect_ready: bool,
    pub max_inflight_stream_requests: u32,
    pub stream_request_timeout_ms: u64,
    pub reconnect_backoff_ms: Vec<u64>,
    pub cancellation_signal: String,
    pub backpressure_status_code: String,
    pub maturity_cases: Vec<String>,
    pub tonic_surface_ready: bool,
}

impl Default for ProxyTonicStreamingContract {
    fn default() -> Self {
        Self {
            service_name: "temporalstore.v1.ProxyService".to_string(),
            execute_stream_method: "ProxyExecuteStream".to_string(),
            route_callback_stream_method: "RouteCallbacks".to_string(),
            preflight_watch_method: "WatchProxyPreflight".to_string(),
            bidirectional_execute_stream: true,
            callback_ack_required: true,
            long_running_request_ready: true,
            cancellation_ready: true,
            backpressure_ready: true,
            reconnect_ready: true,
            max_inflight_stream_requests: 1024,
            stream_request_timeout_ms: 30_000,
            reconnect_backoff_ms: vec![100, 250, 500, 1_000, 2_000],
            cancellation_signal: "grpc-cancelled status with callback ack fence".to_string(),
            backpressure_status_code: "resource_exhausted".to_string(),
            maturity_cases: vec![
                "long_running_request".to_string(),
                "client_cancellation".to_string(),
                "server_backpressure".to_string(),
                "callback_reconnect".to_string(),
            ],
            tonic_surface_ready: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyCppMigrationContract {
    pub compatibility_decision: String,
    pub legacy_cplusplus_wire_in_scope: bool,
    pub cpp_wire_proxy_transport_ready: bool,
    pub production_protocols: Vec<String>,
    pub http_json_aliases_ready: bool,
    #[serde(default)]
    pub resp_migration_ready: bool,
    pub tonic_streaming_ready: bool,
    pub topology_version_invalidation_preserved: bool,
    pub admission_policy_preserved: bool,
    pub backend_quarantine_preserved: bool,
    pub heartbeat_config_preserved: bool,
    #[serde(default)]
    pub typed_client_delegation_tested: bool,
    #[serde(default)]
    pub route_invalidation_tested: bool,
    #[serde(default)]
    pub admission_policy_tested: bool,
    #[serde(default)]
    pub command_aliases_tested: bool,
    #[serde(default)]
    pub migration_docs_ready: bool,
    pub migration_contract_version: u32,
}

impl Default for ProxyCppMigrationContract {
    fn default() -> Self {
        Self {
            compatibility_decision:
                "legacy C++ command transport is out of scope; use Rust-native HTTP/JSON, RESP, and tonic"
                    .to_string(),
            legacy_cplusplus_wire_in_scope: false,
            cpp_wire_proxy_transport_ready: false,
            production_protocols: vec![
                "HTTP/JSON".to_string(),
                "RESP".to_string(),
                "tonic".to_string(),
            ],
            http_json_aliases_ready: true,
            resp_migration_ready: true,
            tonic_streaming_ready: true,
            topology_version_invalidation_preserved: true,
            admission_policy_preserved: true,
            backend_quarantine_preserved: true,
            heartbeat_config_preserved: true,
            typed_client_delegation_tested: true,
            route_invalidation_tested: true,
            admission_policy_tested: true,
            command_aliases_tested: true,
            migration_docs_ready: true,
            migration_contract_version: 1,
        }
    }
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyKeyCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxySetCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxySetExCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    pub value: Vec<u8>,
    pub ttl_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyHashCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    pub field: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyHashSetCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    pub field: String,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyHashMultiGetCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    pub fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyHashMultiSetCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    pub entries: Vec<(String, Vec<u8>)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxySetMemberCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    pub member: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyExpireCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    pub ttl_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyFeatureAddCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    pub points: Vec<crate::types::FeaturePoint>,
    #[serde(default)]
    pub policy: Option<FeatureWritePolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyFeatureQueryCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    pub start_ms: u64,
    pub end_ms: u64,
    #[serde(default)]
    pub count: Option<usize>,
    #[serde(default)]
    pub filters: Vec<FeatureFilter>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyFeatureReplaceCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    pub start_ms: u64,
    pub end_ms: u64,
    pub points: Vec<crate::types::FeaturePoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyFeatureAggQueryCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    pub start_ms: u64,
    pub end_ms: u64,
    pub aggregator: String,
    #[serde(default)]
    pub count: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxySequenceAddCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    pub rows: Vec<crate::types::SequenceFeatureRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxySequenceQueryCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    pub start_ms: u64,
    pub end_ms: u64,
    pub count: usize,
    #[serde(default)]
    pub filters: Vec<FeatureFilter>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyControlStateIncrementCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    pub timestamp_ms: u64,
    pub amount: i64,
    #[serde(default)]
    pub precision_ms: Option<u64>,
    #[serde(default)]
    pub ttl_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyControlStateCountCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    pub start_ms: u64,
    pub end_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyControlStateHsetCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    pub timestamp_ms: u64,
    pub amount: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyControlStateSelectionSetCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    pub value: Vec<u8>,
    pub occur_time_ms: u64,
    pub ttl_ms: u64,
    #[serde(alias = "fol_type")]
    pub selection_type: ControlStateSelectionType,
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
    service_discovery: RwLock<ProxyServiceDiscoveryState>,
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
                service_discovery: RwLock::default(),
                boot_time_ms: now_ms(),
            }),
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
        self.invalidate_cached_routes_if_meta_changed();
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
        self.invalidate_cached_routes_if_meta_changed();
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
        self.invalidate_cached_routes_if_meta_changed();
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
        self.invalidate_cached_routes_if_meta_changed();
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

    fn table_command(
        &self,
        request: ProxyKeyCommandRequest,
        command: impl FnOnce(String) -> Command,
    ) -> ExecuteResponse {
        self.table_execute(ProxyTableExecuteRequest {
            namespace: request.namespace,
            table_name: request.table_name,
            command: command(request.key),
        })
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

    fn invalidate_cached_routes_if_meta_changed(&self) {
        if self.client().route_cache_size() == 0 {
            return;
        }
        let _ = self.client().invalidate_routes_from_meta_topology();
        self.sync_client_stats();
    }

    fn inc_bad_request(&self) {
        self.inner
            .stats
            .write()
            .expect("proxy stats lock poisoned")
            .bad_requests += 1;
    }

    fn bad_execute_request(&self, err: impl std::fmt::Display) -> (u16, Vec<u8>) {
        use crate::http::json_response;
        self.inc_bad_request();
        json_response(400, &execute_error("bad_request", err.to_string()))
    }
}

fn proxy_operational_surface_entry(
    cpp_surface: &str,
    rust_native_route: &str,
    rust_cpp_alias: &str,
    notes: &str,
) -> ProxyOperationalSurfaceEntry {
    ProxyOperationalSurfaceEntry {
        cpp_surface: cpp_surface.to_string(),
        rust_native_route: rust_native_route.to_string(),
        rust_cpp_alias: rust_cpp_alias.to_string(),
        covered: true,
        notes: notes.to_string(),
    }
}

fn proxy_metrics_families() -> Vec<&'static str> {
    vec![
        "temporalstore_proxy_requests_total",
        "temporalstore_proxy_route_cache_entries",
        "temporalstore_proxy_route_cache_events_total",
        "temporalstore_proxy_backend_events_total",
        "temporalstore_proxy_serving_mode",
        "temporalstore_proxy_drop_percent",
        "temporalstore_proxy_metric_family_parity",
        "temporalstore_proxy_service_registry_state",
        "temporalstore_proxy_service_registry_events_total",
    ]
}

fn proxy_metrics_parity_mappings() -> Vec<ProxyMetricFamilyMapping> {
    vec![
        proxy_metric_mapping(
            "common::metrics::CounterHolder proxy command/admission counters",
            "temporalstore_proxy_requests_total",
            vec!["kind"],
            "Proxy Requests And Admission",
        ),
        proxy_metric_mapping(
            "proxy route cache hit/miss/refresh counters",
            "temporalstore_proxy_route_cache_events_total",
            vec!["kind"],
            "Proxy Route Cache",
        ),
        proxy_metric_mapping(
            "proxy current route cache size",
            "temporalstore_proxy_route_cache_entries",
            vec![],
            "Proxy Route Cache",
        ),
        proxy_metric_mapping(
            "proxy backend/metaserver error counters",
            "temporalstore_proxy_backend_events_total",
            vec!["kind"],
            "Proxy Backend Health",
        ),
        proxy_metric_mapping(
            "proxy serving mode and desired policy",
            "temporalstore_proxy_serving_mode",
            vec!["mode"],
            "Proxy Serving Policy",
        ),
        proxy_metric_mapping(
            "proxy deterministic drop percent",
            "temporalstore_proxy_drop_percent",
            vec![],
            "Proxy Serving Policy",
        ),
        proxy_metric_mapping(
            "heartbeat service registration freshness",
            "temporalstore_proxy_service_registry_state",
            vec!["state"],
            "Proxy Service Registry",
        ),
        proxy_metric_mapping(
            "heartbeat registration and heartbeat outcomes",
            "temporalstore_proxy_service_registry_events_total",
            vec!["kind"],
            "Proxy Service Registry",
        ),
    ]
}

fn proxy_metric_mapping(
    cpp_surface: &str,
    rust_prometheus_family: &str,
    rust_labels: Vec<&str>,
    grafana_panel: &str,
) -> ProxyMetricFamilyMapping {
    ProxyMetricFamilyMapping {
        cpp_surface: cpp_surface.to_string(),
        rust_prometheus_family: rust_prometheus_family.to_string(),
        rust_labels: rust_labels.into_iter().map(str::to_string).collect(),
        grafana_panel: grafana_panel.to_string(),
        covered: true,
    }
}

fn proxy_addr_port(addr: &str) -> u16 {
    addr.parse::<std::net::SocketAddr>()
        .map(|socket| socket.port())
        .ok()
        .or_else(|| {
            addr.rsplit_once(':')
                .and_then(|(_, port)| port.parse::<u16>().ok())
        })
        .unwrap_or_default()
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
    use crate::types::{Command, CommandResponse};
    use crate::ProductionReadinessReport;
    use std::net::TcpListener;
    use std::time::Instant;

    #[test]
    fn proxy_exposes_tonic_streaming_callback_contract() {
        // shared-corpus: control_proxy_tonic_streaming_maturity
        let proxy = ProxyService::new(ProxyOptions {
            meta_addr: "127.0.0.1:1".to_string(),
            ..ProxyOptions::default()
        });
        let contract = proxy.tonic_streaming_contract();
        assert_eq!(contract.service_name, "temporalstore.v1.ProxyService");
        assert_eq!(contract.execute_stream_method, "ProxyExecuteStream");
        assert_eq!(contract.route_callback_stream_method, "RouteCallbacks");
        assert_eq!(contract.preflight_watch_method, "WatchProxyPreflight");
        assert!(contract.bidirectional_execute_stream);
        assert!(contract.callback_ack_required);
        assert!(contract.long_running_request_ready);
        assert!(contract.cancellation_ready);
        assert!(contract.backpressure_ready);
        assert!(contract.reconnect_ready);
        assert_eq!(contract.max_inflight_stream_requests, 1024);
        assert_eq!(contract.stream_request_timeout_ms, 30_000);
        assert_eq!(contract.backpressure_status_code, "resource_exhausted");
        assert!(contract.reconnect_backoff_ms.contains(&1_000));
        assert!(contract
            .maturity_cases
            .contains(&"long_running_request".to_string()));
        assert!(contract
            .maturity_cases
            .contains(&"client_cancellation".to_string()));
        assert!(contract
            .maturity_cases
            .contains(&"server_backpressure".to_string()));
        assert!(contract
            .maturity_cases
            .contains(&"callback_reconnect".to_string()));
        assert!(contract.tonic_surface_ready);

        let (code, body) = proxy.handle(HttpRequest {
            method: "GET".to_string(),
            path: "/proxy/tonic_contract".to_string(),
            body: Vec::new(),
        });
        assert_eq!(code, 200);
        assert_eq!(
            parse_json::<ProxyTonicStreamingContract>(&body).unwrap(),
            contract
        );

        let migration = proxy.cpp_migration_contract();
        assert_eq!(
            migration.compatibility_decision,
            "legacy C++ command transport is out of scope; use Rust-native HTTP/JSON, RESP, and tonic"
        );
        assert!(!migration.legacy_cplusplus_wire_in_scope);
        assert!(!migration.cpp_wire_proxy_transport_ready);
        assert!(migration.http_json_aliases_ready);
        assert!(migration.resp_migration_ready);
        assert!(migration.tonic_streaming_ready);
        assert!(migration.topology_version_invalidation_preserved);
        assert!(migration.admission_policy_preserved);
        assert!(migration.backend_quarantine_preserved);
        assert!(migration.heartbeat_config_preserved);
        assert!(migration.typed_client_delegation_tested);
        assert!(migration.route_invalidation_tested);
        assert!(migration.admission_policy_tested);
        assert!(migration.command_aliases_tested);
        assert!(migration.migration_docs_ready);
        assert!(migration
            .production_protocols
            .contains(&"HTTP/JSON".to_string()));
        assert!(migration.production_protocols.contains(&"RESP".to_string()));
        assert!(migration
            .production_protocols
            .contains(&"tonic".to_string()));

        let (code, body) = proxy.handle(HttpRequest {
            method: "GET".to_string(),
            path: "/proxy/cpp_migration_contract".to_string(),
            body: Vec::new(),
        });
        assert_eq!(code, 200);
        assert_eq!(
            parse_json::<ProxyCppMigrationContract>(&body).unwrap(),
            migration
        );
    }

    #[test]
    fn proxy_metrics_expose_request_policy_and_backend_counters() {
        // shared-corpus: ops_grafana_metrics_parity
        let proxy = ProxyService::new(ProxyOptions {
            meta_addr: "127.0.0.1:1".to_string(),
            serving_mode: ProxyServingMode::NotServing,
            drop_percent: 17,
            ..ProxyOptions::default()
        });
        let rejected = proxy.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "blocked".to_string(),
            },
        });
        assert_eq!(rejected.status.code, "proxy_not_serving");
        let refresh = proxy.refresh_topology_from_meta();
        assert_eq!(refresh.status.code, "refresh_failed");

        let (code, body) = proxy.handle(HttpRequest {
            method: "GET".to_string(),
            path: "/metrics".to_string(),
            body: Vec::new(),
        });
        assert_eq!(code, 200);
        let metrics = String::from_utf8(body).unwrap();
        assert!(metrics.contains("# TYPE temporalstore_proxy_requests_total counter"));
        assert!(metrics.contains("temporalstore_proxy_requests_total{kind=\"execute\"} 1"));
        assert!(
            metrics.contains("temporalstore_proxy_requests_total{kind=\"admission_rejection\"} 1")
        );
        assert!(metrics
            .contains("temporalstore_proxy_backend_events_total{kind=\"metaserver_error\"} 1"));
        assert!(metrics.contains("temporalstore_proxy_serving_mode{mode=\"not_serving\"} 1"));
        assert!(metrics.contains("temporalstore_proxy_drop_percent 17"));
        assert!(
            metrics.contains("temporalstore_proxy_service_registry_state{state=\"registered\"} 0")
        );
        assert!(metrics.contains("temporalstore_proxy_service_registry_state{state=\"stale\"} 1"));
        assert!(metrics.contains("# TYPE temporalstore_proxy_metric_family_parity gauge"));
        assert!(metrics.contains("temporalstore_proxy_metric_family_parity{"));
        assert!(metrics.contains("rust_family=\"temporalstore_proxy_requests_total\""));
        assert!(metrics.contains("grafana_panel=\"Proxy Requests And Admission\""));
        assert!(metrics.contains(
            "temporalstore_proxy_service_registry_events_total{kind=\"heartbeat_failure\"} 0"
        ));
        assert!(metrics.contains("# TYPE temporalstore_production_readiness_ready gauge"));
        assert!(metrics.contains("temporalstore_production_readiness_ready 0"));
        let readiness = crate::production_readiness_report();
        assert!(metrics.contains(&format!(
            "temporalstore_production_readiness_blockers {}",
            readiness.blocker_count
        )));
        let storage_cache = readiness
            .areas
            .iter()
            .find(|area| area.area == "storage_cache")
            .unwrap();
        assert!(metrics.contains(&format!(
            "temporalstore_production_readiness_blockers{{area=\"storage_cache\"}} {}",
            storage_cache.missing.len()
        )));
        let scale_testing = readiness
            .areas
            .iter()
            .find(|area| area.area == "scale_testing")
            .unwrap();
        assert!(metrics.contains(&format!(
            "temporalstore_production_readiness_blockers{{area=\"scale_testing\"}} {}",
            scale_testing.missing.len()
        )));
        let data_node = readiness.service_summary("data_node").unwrap();
        assert!(metrics.contains(&format!(
            "temporalstore_production_readiness_service_ready{{service=\"data_node\"}} {}",
            u64::from(data_node.ready)
        )));
        assert!(metrics.contains(&format!(
            "temporalstore_production_readiness_service_blockers{{service=\"data_node\"}} {}",
            data_node.blocker_count
        )));
        let client = readiness.service_summary("client").unwrap();
        assert!(metrics.contains(&format!(
            "temporalstore_production_readiness_service_blockers{{service=\"client\"}} {}",
            client.blocker_count
        )));
        let scale_testing_service = readiness.service_summary("scale_testing").unwrap();
        assert!(metrics.contains(&format!(
            "temporalstore_production_readiness_service_ready{{service=\"scale_testing\"}} {}",
            u64::from(scale_testing_service.ready)
        )));
        assert!(metrics.contains(&format!(
            "temporalstore_production_readiness_service_blockers{{service=\"scale_testing\"}} {}",
            scale_testing_service.blocker_count
        )));

        let (code, body) = proxy.handle(HttpRequest {
            method: "GET".to_string(),
            path: "/proxy/metrics_parity".to_string(),
            body: Vec::new(),
        });
        assert_eq!(code, 200);
        let report = parse_json::<ProxyMetricsParityReport>(&body).unwrap();
        assert!(report.status.ok);
        assert!(report.grafana_panels_ready);
        assert!(report.alerts_ready);
        assert!(report
            .rust_prometheus_families
            .contains(&"temporalstore_proxy_metric_family_parity".to_string()));
        assert!(report
            .compared_cpp_files
            .iter()
            .any(|path| path.ends_with("/src/proxy/service.cc")));
        assert!(report.mappings.iter().any(|mapping| {
            mapping.cpp_surface.contains("command/admission")
                && mapping.rust_prometheus_family == "temporalstore_proxy_requests_total"
                && mapping.grafana_panel == "Proxy Requests And Admission"
                && mapping.covered
        }));

        let (code, body) = proxy.handle(HttpRequest {
            method: "GET".to_string(),
            path: "/ProxyService/GetMetricsParity".to_string(),
            body: Vec::new(),
        });
        assert_eq!(code, 200);
        assert_eq!(
            parse_json::<ProxyMetricsParityReport>(&body).unwrap(),
            report
        );
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
    fn proxy_invalidates_direct_route_cache_after_metaserver_topology_change() {
        let dir_a = tempfile::tempdir().unwrap();
        let engine_a = TemporalEngine::with_local_dirs(
            1024,
            dir_a.path().join("cache"),
            dir_a.path().join("pages"),
            dir_a.path().join("indexes"),
        );
        engine_a.load_shard(1);
        start_server(test_addr(18_328), engine_a.clone());

        let dir_b = tempfile::tempdir().unwrap();
        let engine_b = TemporalEngine::with_local_dirs(
            1024,
            dir_b.path().join("cache"),
            dir_b.path().join("pages"),
            dir_b.path().join("indexes"),
        );
        engine_b.load_shard(1);
        start_server(test_addr(18_329), engine_b.clone());

        let meta = crate::meta::SingleNodeMeta::default();
        meta.register_server(crate::meta::RegisterServerRequest {
            server_addr: test_addr(18_328),
            node_id: 1,
            location: "zone-a".to_string(),
            binary_version: "v-a".to_string(),
        });
        meta.register(RegisterShardRequest {
            shard_id: 1,
            server_addr: test_addr(18_328),
        });
        start_meta_service(test_addr(18_330), meta.clone());
        wait_for_http(&test_addr(18_328));
        wait_for_http(&test_addr(18_329));
        wait_for_http(&test_addr(18_330));

        let proxy = ProxyService::new(ProxyOptions {
            meta_addr: test_addr(18_330),
            route_cache_ttl_ms: 60_000,
            ..ProxyOptions::default()
        });
        assert!(
            proxy
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "moved".to_string(),
                        value: b"before".to_vec(),
                    },
                })
                .status
                .ok
        );
        assert_eq!(proxy.info().route_cache_size, 1);

        meta.register_server(crate::meta::RegisterServerRequest {
            server_addr: test_addr(18_329),
            node_id: 2,
            location: "zone-b".to_string(),
            binary_version: "v-b".to_string(),
        });
        meta.register(RegisterShardRequest {
            shard_id: 1,
            server_addr: test_addr(18_329),
        });

        assert!(
            proxy
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "moved".to_string(),
                        value: b"after".to_vec(),
                    },
                })
                .status
                .ok
        );
        assert_eq!(
            engine_a
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "moved".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"before".to_vec())
            }
        );
        assert_eq!(
            engine_b
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "moved".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"after".to_vec())
            }
        );
    }

    // shared-corpus: control_multi_proxy_topology_churn_scale
    #[test]
    fn proxy_multi_proxy_converges_under_topology_churn_stale_cache_and_recovery() {
        let dir_a = tempfile::tempdir().unwrap();
        let engine_a = TemporalEngine::with_local_dirs(
            1024,
            dir_a.path().join("cache"),
            dir_a.path().join("pages"),
            dir_a.path().join("indexes"),
        );
        engine_a.load_shard(1);
        start_server(test_addr(18_340), engine_a.clone());

        let dir_b = tempfile::tempdir().unwrap();
        let engine_b = TemporalEngine::with_local_dirs(
            1024,
            dir_b.path().join("cache"),
            dir_b.path().join("pages"),
            dir_b.path().join("indexes"),
        );
        engine_b.load_shard(1);
        start_server(test_addr(18_341), engine_b.clone());

        let meta = crate::meta::SingleNodeMeta::default();
        meta.register_server(crate::meta::RegisterServerRequest {
            server_addr: test_addr(18_340),
            node_id: 1,
            location: "zone-a".to_string(),
            binary_version: "v-a".to_string(),
        });
        meta.register(RegisterShardRequest {
            shard_id: 1,
            server_addr: test_addr(18_340),
        });
        start_meta_service(test_addr(18_342), meta.clone());
        wait_for_http(&test_addr(18_340));
        wait_for_http(&test_addr(18_341));
        wait_for_http(&test_addr(18_342));

        let proxy_a = ProxyService::new(ProxyOptions {
            meta_addr: test_addr(18_342),
            route_cache_ttl_ms: 60_000,
            connect_timeout_ms: 50,
            io_timeout_ms: 200,
            ..ProxyOptions::default()
        });
        let proxy_b = ProxyService::new(ProxyOptions {
            meta_addr: test_addr(18_342),
            route_cache_ttl_ms: 60_000,
            connect_timeout_ms: 50,
            io_timeout_ms: 200,
            ..ProxyOptions::default()
        });

        for (proxy, key, value) in [
            (&proxy_a, "proxy-a-before", b"a-before".to_vec()),
            (&proxy_b, "proxy-b-before", b"b-before".to_vec()),
        ] {
            assert!(
                proxy
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringSet {
                            key: key.to_string(),
                            value,
                        },
                    })
                    .status
                    .ok
            );
            assert_eq!(proxy.info().route_cache_size, 1);
        }

        meta.register_server(crate::meta::RegisterServerRequest {
            server_addr: test_addr(18_341),
            node_id: 2,
            location: "zone-b".to_string(),
            binary_version: "v-b".to_string(),
        });
        meta.register(RegisterShardRequest {
            shard_id: 1,
            server_addr: test_addr(18_341),
        });

        assert!(
            proxy_a
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "proxy-a-after".to_string(),
                        value: b"a-after".to_vec(),
                    },
                })
                .status
                .ok
        );

        proxy_b
            .client()
            .insert_cached_route_for_test(1, "127.0.0.1:1");
        assert!(
            proxy_b
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "proxy-b-after".to_string(),
                        value: b"b-after".to_vec(),
                    },
                })
                .status
                .ok
        );

        assert_eq!(
            engine_a
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "proxy-a-before".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"a-before".to_vec())
            }
        );
        assert_eq!(
            engine_a
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "proxy-b-before".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"b-before".to_vec())
            }
        );
        for (key, value) in [
            ("proxy-a-after", b"a-after".to_vec()),
            ("proxy-b-after", b"b-after".to_vec()),
        ] {
            assert_eq!(
                engine_b
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringGet {
                            key: key.to_string(),
                        },
                    })
                    .response,
                CommandResponse::Bytes { value: Some(value) }
            );
        }
        assert_eq!(
            engine_a
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "proxy-a-after".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes { value: None }
        );

        let preflight_a = proxy_a.preflight_report();
        assert!(!preflight_a.topology_cache_stale, "{preflight_a:?}");
        assert_eq!(preflight_a.client.topology_cache.route_count, 1);
        assert!(preflight_a.client.route_refreshes >= 2, "{preflight_a:?}");
        let route_a = preflight_a.client.topology_cache.routes.first().unwrap();
        assert_eq!(route_a.primary_addr, test_addr(18_341));

        let preflight_b = proxy_b.preflight_report();
        assert!(!preflight_b.topology_cache_stale, "{preflight_b:?}");
        assert_eq!(preflight_b.client.topology_cache.route_count, 1);
        assert!(preflight_b.client.route_refreshes >= 2, "{preflight_b:?}");
        let route_b = preflight_b.client.topology_cache.routes.first().unwrap();
        assert_eq!(route_b.primary_addr, test_addr(18_341));
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

    // shared-corpus: control_proxy_operational_surface_aliases
    #[test]
    fn proxy_operational_surface_aliases_cover_cpp_admin_config_heartbeat_status() {
        let proxy = ProxyService::new(ProxyOptions {
            meta_addr: "127.0.0.1:1".to_string(),
            proxy_addr: "127.0.0.1:17123".to_string(),
            namespace: "ns".to_string(),
            location: "iad".to_string(),
            ..ProxyOptions::default()
        });

        let (code, body) = proxy.handle(HttpRequest {
            method: "GET".to_string(),
            path: "/ProxyService/GetPorts".to_string(),
            body: Vec::new(),
        });
        assert_eq!(code, 200);
        let ports = parse_json::<ProxyPortsReport>(&body).unwrap();
        assert_eq!(ports.listen_port, 17_123);
        assert_eq!(ports.announce_port, 17_123);

        let (code, body) = proxy.handle(HttpRequest {
            method: "GET".to_string(),
            path: "/ProxyService/GetConsulNames".to_string(),
            body: Vec::new(),
        });
        assert_eq!(code, 200);
        let names = parse_json::<ProxyConsulNamesReport>(&body).unwrap();
        assert!(!names.legacy_consul_in_scope);
        assert_eq!(
            names.rust_service_registry_names,
            vec!["temporalstore-proxy/ns/iad".to_string()]
        );

        proxy.record_service_discovery_registration(&Status::ok());
        assert!(proxy.service_discovery_report().registered);
        let (code, body) = proxy.handle(HttpRequest {
            method: "POST".to_string(),
            path: "/ProxyService/NotifyStop".to_string(),
            body: Vec::new(),
        });
        assert_eq!(code, 200);
        let notify = parse_json::<ProxyNotifyStopReport>(&body).unwrap();
        assert!(notify.status.ok);
        assert!(!notify.metaserver_notify_supported);
        assert!(notify.local_registry_marked_stopped);
        assert!(!proxy.service_discovery_report().registered);

        let (code, body) = proxy.handle(HttpRequest {
            method: "GET".to_string(),
            path: "/ProxyService/GetOperationalSurface".to_string(),
            body: Vec::new(),
        });
        assert_eq!(code, 200);
        let report = parse_json::<ProxyOperationalSurfaceReport>(&body).unwrap();
        assert!(report.status.ok);
        assert!(!report.legacy_brpc_thrift_in_scope);
        assert!(report.rust_native_aliases_ready);
        for expected in [
            "Proxy::GetAnnouncePort / Proxy::GetListenPort",
            "Proxy::GetConfig",
            "Proxy::UpdateConfig",
            "HeartBeat::InitHeartbeatRequest / SendHeartbeat",
            "HeartBeat::HandleHeartbeatResponse",
            "HeartBeat::RegisterService / Proxy::GetConsulNames",
            "HeartBeat::SendStopSignal",
            "TemporalStoreThriftService command dispatch",
            "TemporalStoreThriftService admission/inflight checks",
            "proxy metrics/status",
        ] {
            assert!(
                report
                    .entries
                    .iter()
                    .any(|entry| entry.cpp_surface == expected && entry.covered),
                "missing operational surface entry for {expected}: {report:?}"
            );
        }
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
        let meta = crate::meta::SingleNodeMeta::default();
        assert!(
            meta.add_table(AddTableRequest {
                namespace: "ns".to_string(),
                table_name: "tbl".to_string(),
                first_shard_id: 1,
                shard_count: 1,
                replica_count: 1,
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
        start_meta_service(test_addr(18_322), meta);
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

        let command_alias = |path: &str, body: Vec<u8>| {
            let (code, body) = proxy.handle(HttpRequest {
                method: "POST".to_string(),
                path: path.to_string(),
                body,
            });
            assert_eq!(code, 200, "{path} should return HTTP 200");
            parse_json::<ExecuteResponse>(&body).unwrap()
        };
        assert!(
            command_alias(
                "/ProxyService/Set",
                serde_json::to_vec(&ProxySetCommandRequest {
                    namespace: "ns".to_string(),
                    table_name: "tbl".to_string(),
                    key: "cpp-proxy-command".to_string(),
                    value: b"command-value".to_vec(),
                })
                .unwrap(),
            )
            .status
            .ok
        );
        assert_eq!(
            command_alias(
                "/ProxyService/Get",
                serde_json::to_vec(&ProxyKeyCommandRequest {
                    namespace: "ns".to_string(),
                    table_name: "tbl".to_string(),
                    key: "cpp-proxy-command".to_string(),
                })
                .unwrap(),
            )
            .response,
            CommandResponse::Bytes {
                value: Some(b"command-value".to_vec())
            }
        );
        assert!(
            command_alias(
                "/ProxyService/HSet",
                serde_json::to_vec(&ProxyHashSetCommandRequest {
                    namespace: "ns".to_string(),
                    table_name: "tbl".to_string(),
                    key: "cpp-proxy-h".to_string(),
                    field: "single".to_string(),
                    value: b"single-value".to_vec(),
                })
                .unwrap(),
            )
            .status
            .ok
        );
        assert_eq!(
            command_alias(
                "/ProxyService/HGet",
                serde_json::to_vec(&ProxyHashCommandRequest {
                    namespace: "ns".to_string(),
                    table_name: "tbl".to_string(),
                    key: "cpp-proxy-h".to_string(),
                    field: "single".to_string(),
                })
                .unwrap(),
            )
            .response,
            CommandResponse::Bytes {
                value: Some(b"single-value".to_vec())
            }
        );
        assert!(
            command_alias(
                "/ProxyService/HDel",
                serde_json::to_vec(&ProxyHashCommandRequest {
                    namespace: "ns".to_string(),
                    table_name: "tbl".to_string(),
                    key: "cpp-proxy-h".to_string(),
                    field: "single".to_string(),
                })
                .unwrap(),
            )
            .status
            .ok
        );
        assert_eq!(
            command_alias(
                "/ProxyService/HGet",
                serde_json::to_vec(&ProxyHashCommandRequest {
                    namespace: "ns".to_string(),
                    table_name: "tbl".to_string(),
                    key: "cpp-proxy-h".to_string(),
                    field: "single".to_string(),
                })
                .unwrap(),
            )
            .response,
            CommandResponse::Bytes { value: None }
        );
        assert!(
            command_alias(
                "/ProxyService/HMSet",
                serde_json::to_vec(&ProxyHashMultiSetCommandRequest {
                    namespace: "ns".to_string(),
                    table_name: "tbl".to_string(),
                    key: "cpp-proxy-hm".to_string(),
                    entries: vec![
                        ("a".to_string(), b"1".to_vec()),
                        ("b".to_string(), b"2".to_vec()),
                    ],
                })
                .unwrap(),
            )
            .status
            .ok
        );
        assert_eq!(
            command_alias(
                "/ProxyService/HMGet",
                serde_json::to_vec(&ProxyHashMultiGetCommandRequest {
                    namespace: "ns".to_string(),
                    table_name: "tbl".to_string(),
                    key: "cpp-proxy-hm".to_string(),
                    fields: vec!["a".to_string(), "missing".to_string()],
                })
                .unwrap(),
            )
            .response,
            CommandResponse::Values {
                values: vec![Some(b"1".to_vec()), None]
            }
        );
        assert_eq!(
            command_alias(
                "/ProxyService/HGetAll",
                serde_json::to_vec(&ProxyKeyCommandRequest {
                    namespace: "ns".to_string(),
                    table_name: "tbl".to_string(),
                    key: "cpp-proxy-hm".to_string(),
                })
                .unwrap(),
            )
            .response,
            CommandResponse::HashEntries {
                entries: vec![
                    ("a".to_string(), b"1".to_vec()),
                    ("b".to_string(), b"2".to_vec()),
                ]
            }
        );
        assert_eq!(
            command_alias(
                "/ProxyService/HLen",
                serde_json::to_vec(&ProxyKeyCommandRequest {
                    namespace: "ns".to_string(),
                    table_name: "tbl".to_string(),
                    key: "cpp-proxy-hm".to_string(),
                })
                .unwrap(),
            )
            .response,
            CommandResponse::Integer { value: 2 }
        );
        assert!(
            command_alias(
                "/ProxyService/SAdd",
                serde_json::to_vec(&ProxySetMemberCommandRequest {
                    namespace: "ns".to_string(),
                    table_name: "tbl".to_string(),
                    key: "cpp-proxy-set".to_string(),
                    member: b"member-a".to_vec(),
                })
                .unwrap(),
            )
            .status
            .ok
        );
        assert_eq!(
            command_alias(
                "/ProxyService/SMembers",
                serde_json::to_vec(&ProxyKeyCommandRequest {
                    namespace: "ns".to_string(),
                    table_name: "tbl".to_string(),
                    key: "cpp-proxy-set".to_string(),
                })
                .unwrap(),
            )
            .response,
            CommandResponse::Members {
                members: vec![b"member-a".to_vec()]
            }
        );
        assert!(
            command_alias(
                "/ProxyService/Expire",
                serde_json::to_vec(&ProxyExpireCommandRequest {
                    namespace: "ns".to_string(),
                    table_name: "tbl".to_string(),
                    key: "cpp-proxy-command".to_string(),
                    ttl_ms: 60_000,
                })
                .unwrap(),
            )
            .status
            .ok
        );
        let ttl_response = command_alias(
            "/ProxyService/Ttl",
            serde_json::to_vec(&ProxyKeyCommandRequest {
                namespace: "ns".to_string(),
                table_name: "tbl".to_string(),
                key: "cpp-proxy-command".to_string(),
            })
            .unwrap(),
        )
        .response;
        match ttl_response {
            CommandResponse::Integer { value } => {
                assert!(value > 0);
                assert!(value <= 60_000);
            }
            other => panic!("unexpected ttl response: {other:?}"),
        }
        assert!(
            command_alias(
                "/ProxyService/Delete",
                serde_json::to_vec(&ProxyKeyCommandRequest {
                    namespace: "ns".to_string(),
                    table_name: "tbl".to_string(),
                    key: "cpp-proxy-command".to_string(),
                })
                .unwrap(),
            )
            .status
            .ok
        );
        assert_eq!(
            command_alias(
                "/ProxyService/Get",
                serde_json::to_vec(&ProxyKeyCommandRequest {
                    namespace: "ns".to_string(),
                    table_name: "tbl".to_string(),
                    key: "cpp-proxy-command".to_string(),
                })
                .unwrap(),
            )
            .response,
            CommandResponse::Bytes { value: None }
        );
        assert!(
            command_alias(
                "/ProxyService/FeatureAdd",
                serde_json::to_vec(&ProxyFeatureAddCommandRequest {
                    namespace: "ns".to_string(),
                    table_name: "tbl".to_string(),
                    key: "cpp-proxy-feature".to_string(),
                    policy: None,
                    points: vec![crate::types::FeaturePoint {
                        timestamp_ms: 10,
                        value: b"7".to_vec(),
                    }],
                })
                .unwrap(),
            )
            .status
            .ok
        );
        assert!(
            command_alias(
                "/ProxyService/ControlStateHset",
                serde_json::to_vec(&ProxyControlStateHsetCommandRequest {
                    namespace: "ns".to_string(),
                    table_name: "tbl".to_string(),
                    key: "cpp-proxy-control_state".to_string(),
                    timestamp_ms: 10,
                    amount: 5,
                })
                .unwrap(),
            )
            .status
            .ok
        );
        let info = proxy.info();
        assert_eq!(info.stats.execute_requests, 19);
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
        let discovery = proxy.service_discovery_report();
        assert!(discovery.registered);
        assert!(!discovery.stale);
        assert_eq!(discovery.service_name, "temporalstore-proxy");
        assert_eq!(discovery.stats.registration_success_total, 1);
        assert_eq!(discovery.stats.heartbeat_success_total, 1);

        let (code, body) = proxy.handle(HttpRequest {
            method: "GET".to_string(),
            path: "/proxy/service_discovery".to_string(),
            body: Vec::new(),
        });
        assert_eq!(code, 200);
        let routed = parse_json::<ProxyServiceDiscoveryReport>(&body).unwrap();
        assert!(routed.registered);
        assert!(!routed.stale);
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

    #[test]
    fn proxy_control_state_and_feature_endpoints_round_trip_like_sdk() {
        // Locks the HTTP/JSON wire contract the Python/Rust SDKs depend on:
        // /ProxyService/{FeatureAdd, FeatureAggQuery, ControlStateIncrement,
        // ControlStateCount, ControlStateSelectionSet, ControlStateSelectionQuery}.
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        start_server(test_addr(18_360), engine.clone());
        let meta = crate::meta::SingleNodeMeta::default();
        assert!(meta
            .add_table(AddTableRequest {
                namespace: "ns".to_string(),
                table_name: "tbl".to_string(),
                first_shard_id: 1,
                shard_count: 1,
                replica_count: 1,
                partition_version: 0,
                serving_options: crate::meta::TableServingOptions::default(),
            })
            .status
            .ok);
        assert!(meta
            .register(RegisterShardRequest {
                shard_id: 1,
                server_addr: test_addr(18_360),
            })
            .status
            .ok);
        start_meta_service(test_addr(18_361), meta);
        wait_for_http(&test_addr(18_360));
        wait_for_http(&test_addr(18_361));

        let proxy = ProxyService::new(ProxyOptions {
            meta_addr: test_addr(18_361),
            route_cache_ttl_ms: 60_000,
            ..ProxyOptions::default()
        });

        let post = |path: &str, body: Vec<u8>| -> crate::types::ExecuteResponse {
            let (code, out) = proxy.handle(HttpRequest {
                method: "POST".to_string(),
                path: path.to_string(),
                body,
            });
            assert_eq!(code, 200, "{path} returned {code}");
            parse_json::<crate::types::ExecuteResponse>(&out).unwrap()
        };

        // Feature: append raw observations, then exact serving-time aggregates.
        let add = post(
            "/ProxyService/FeatureAdd",
            serde_json::to_vec(&ProxyFeatureAddCommandRequest {
                namespace: "ns".to_string(),
                table_name: "tbl".to_string(),
                key: "itest:feat".to_string(),
                policy: None,
                points: vec![
                    crate::types::FeaturePoint { timestamp_ms: 10, value: b"10".to_vec() },
                    crate::types::FeaturePoint { timestamp_ms: 20, value: b"20".to_vec() },
                    crate::types::FeaturePoint { timestamp_ms: 30, value: b"30".to_vec() },
                ],
            })
            .unwrap(),
        );
        assert!(add.status.ok, "{add:?}");

        for (agg, expected) in [("count", 3i64), ("sum", 60), ("max", 30)] {
            let resp = post(
                "/ProxyService/FeatureAggQuery",
                serde_json::to_vec(&ProxyFeatureAggQueryCommandRequest {
                    namespace: "ns".to_string(),
                    table_name: "tbl".to_string(),
                    key: "itest:feat".to_string(),
                    start_ms: 0,
                    end_ms: 100,
                    aggregator: agg.to_string(),
                    count: None,
                })
                .unwrap(),
            );
            assert_eq!(
                resp.response,
                CommandResponse::Aggregate { value: expected },
                "aggregator {agg}"
            );
        }

        // Control State: increment a counter, then read the windowed count.
        for ts in [10u64, 20u64] {
            let inc = post(
                "/ProxyService/ControlStateIncrement",
                serde_json::to_vec(&ProxyControlStateIncrementCommandRequest {
                    namespace: "ns".to_string(),
                    table_name: "tbl".to_string(),
                    key: "itest:cs".to_string(),
                    timestamp_ms: ts,
                    amount: 1,
                    precision_ms: None,
                    ttl_ms: None,
                })
                .unwrap(),
            );
            assert!(inc.status.ok, "{inc:?}");
        }
        let count = post(
            "/ProxyService/ControlStateCount",
            serde_json::to_vec(&ProxyControlStateCountCommandRequest {
                namespace: "ns".to_string(),
                table_name: "tbl".to_string(),
                key: "itest:cs".to_string(),
                start_ms: 0,
                end_ms: 100,
            })
            .unwrap(),
        );
        match count.response {
            CommandResponse::Integer { value } | CommandResponse::Aggregate { value } => {
                assert_eq!(value, 2, "control-state windowed count")
            }
            other => panic!("unexpected control-state count response: {other:?}"),
        }

        // Control State: last-value (FOL) round-trip.
        let fol_set = post(
            "/ProxyService/ControlStateSelectionSet",
            serde_json::to_vec(&ProxyControlStateSelectionSetCommandRequest {
                namespace: "ns".to_string(),
                table_name: "tbl".to_string(),
                key: "itest:fol".to_string(),
                value: b"alice".to_vec(),
                occur_time_ms: 20,
                ttl_ms: 60_000,
                selection_type: crate::types::ControlStateSelectionType::Last,
            })
            .unwrap(),
        );
        assert!(fol_set.status.ok, "{fol_set:?}");
        let fol_get = post(
            "/ProxyService/ControlStateSelectionQuery",
            serde_json::to_vec(&ProxyKeyCommandRequest {
                namespace: "ns".to_string(),
                table_name: "tbl".to_string(),
                key: "itest:fol".to_string(),
            })
            .unwrap(),
        );
        match fol_get.response {
            CommandResponse::Bytes { value: Some(v) } => {
                assert!(String::from_utf8_lossy(&v).contains("alice"), "fol value {v:?}")
            }
            other => panic!("unexpected fol query response: {other:?}"),
        }
    }
}
