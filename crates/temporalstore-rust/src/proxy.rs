// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use serde::{Deserialize, Serialize};

mod commands;
mod context;
mod meta_sync;
mod prometheus;
mod reports;
mod handle;
mod config;
mod metrics;
mod policy;
mod response;

use config::{
    default_context_first_shard_id, default_context_io_timeout_ms, default_context_shard_count,
    default_heartbeat_timeout_ms,
    default_auto_register_min_interval_ms,
    default_topology_check_interval_ms,
    default_pin_primary_reads, default_proxy_addr, default_service_registry_ttl_ms, now_ms,
    proxy_client_from_options, proxy_config_version,
};
use commands::proxy_command_is_write;
use metrics::push_proxy_metric;
use policy::{
    proxy_account_rejection, proxy_drop_rejection, proxy_policy_rejection,
    proxy_serving_mode_from_meta, proxy_serving_mode_label, proxy_serving_rejection, ProxyInflight,
    ProxyInflightGuard,
};
use response::execute_error;

use crate::client::{
    ClientStats, ClientTopologyCacheReport, ClientTopologyRefreshReport, ReplicaReadPolicy,
    RequestOptions, TableOptions, TemporalStoreClient,
};
use crate::http::{
    get_json_with_options_and_headers, post_json_with_options_and_headers, HttpRequest,
    HttpRequestOptions,
};
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
    /// Percentage of the KEYSPACE this proxy refuses, not a percentage of requests.
    ///
    /// The decision is taken from a hash of the routing key, so it is the same every time for
    /// the same key: at 50 it is not "half the load is shed", it is "half the keys are refused,
    /// always". Retrying a refused key never succeeds while the setting stands, and the
    /// refusal applies to writes as well as reads.
    ///
    /// That is a usable property -- it is stable and it is reproducible -- but it is not what
    /// a percentage normally means, so the refusal says so rather than leaving a caller to
    /// discover it by retrying.
    #[serde(default)]
    pub drop_percent: u8,
    /// Account (namespace) this proxy is scoped to when
    /// `enforce_ingestion_account` is set. Empty while enforcement is on is a
    /// misconfiguration and fails closed.
    #[serde(default)]
    pub ingestion_account: String,
    /// Reject requests whose namespace does not match `ingestion_account`.
    #[serde(default)]
    pub enforce_ingestion_account: bool,
    /// Maximum concurrent in-flight requests this proxy admits. `0` is
    /// unlimited.
    #[serde(default)]
    pub max_inflight_requests: u64,
    /// Maximum concurrent in-flight *write* requests, counted on top of
    /// `max_inflight_requests` so a write burst cannot starve reads. `0` is
    /// unlimited.
    #[serde(default)]
    pub max_inflight_write_requests: u64,
    /// Default replica-read routing for tables opened through this proxy. `true`
    /// pins reads to the primary for read-after-write safety; set `false` to
    /// allow follower/locality reads. An explicit per-request `pin_primary`
    /// still wins.
    #[serde(default = "default_pin_primary_reads")]
    pub pin_primary_reads: bool,
    /// First shard id used when routing high-level `/context/*` requests by
    /// tenant. Mirrors a table's `first_shard_id`; combined with
    /// `context_shard_count` and the engine's `shard_id_for_key` it selects the
    /// owning shard for a tenant. Defaults to 1.
    #[serde(default = "default_context_first_shard_id")]
    pub context_first_shard_id: ShardId,
    /// Number of shards the context corpus is spread across. `1` keeps every
    /// tenant on `context_first_shard_id` (single-shard deploys).
    /// How many shards the context routes spread tenants over. **0 means follow the cluster**,
    /// which is the default.
    ///
    /// This used to default to 1, so a proxy in front of an eight-shard cluster put every
    /// tenant's context on shard one and said nothing about it -- one shard doing all the work
    /// while seven sat idle, and no error to notice.
    ///
    /// A tenant's shard comes from hashing its id across this range, so CHANGING the count
    /// moves tenants. Context already written under a different count stays where it was
    /// written and is not found under the new one. That is why the effective value and where
    /// it came from are both reported: a deployment that has been running on the old default
    /// with more than one shard is doing a data move, not a config change.
    #[serde(default = "default_context_shard_count")]
    pub context_shard_count: u64,
    /// I/O timeout (ms) for control-plane calls to the metaserver: heartbeat,
    /// auto-register and notify-stop. Deliberately NOT the command `io_timeout_ms`,
    /// which is sized for a data-path hop and defaults to 200ms. A metaserver
    /// pausing briefly -- GC, an election, a snapshot install -- would blow that
    /// budget and make a perfectly healthy proxy miss heartbeats until it is
    /// declared dead. Liveness must not be decided by a data-path deadline.
    #[serde(default = "default_heartbeat_timeout_ms")]
    pub heartbeat_timeout_ms: u64,
    /// Shortest interval (ms) between metaserver topology checks on the request path.
    ///
    /// Every command entry point asks the metaserver "has topology changed" before it
    /// routes. That is a synchronous round-trip, and it ran on EVERY request, so the
    /// metaserver sat in the request path of all proxy traffic and its latency was added
    /// to every operation. Checking once per interval instead bounds how long a topology
    /// change can go unnoticed, in exchange for not paying a round-trip per request.
    ///
    /// Set to 0 to check on every request, which is the older behaviour exactly.
    ///
    /// A stale route is not the only safety net: a request to a backend that has stopped
    /// serving the shard fails, and the failure path force-refreshes the route.
    #[serde(default = "default_topology_check_interval_ms")]
    pub topology_check_interval_ms: u64,
    /// Shortest interval (ms) between attempts to register this proxy with the metaserver
    /// after a heartbeat comes back `not_found`.
    ///
    /// That reply means the metaserver does not know this proxy, and the answer is to
    /// register again -- but it was being retried on EVERY heartbeat. A metaserver that keeps
    /// saying `not_found` then takes a registration attempt plus a second heartbeat from every
    /// proxy in the fleet, every interval, which is the most load precisely when it is least
    /// able to serve it.
    ///
    /// Set to 0 to attempt on every heartbeat, which is the older behaviour.
    #[serde(default = "default_auto_register_min_interval_ms")]
    pub auto_register_min_interval_ms: u64,
    /// The address this proxy is actually bound to, when that differs from the one it
    /// advertises.
    ///
    /// `proxy_addr` is what the proxy tells the metaserver to reach it on. Behind NAT or a
    /// container port mapping that is deliberately NOT the socket it listens on, and the
    /// binary already supports the split -- it binds `TS_PROXY_BIND_ADDR` and advertises
    /// `TS_PROXY_ADVERTISED_ADDR`. The service was never told the first of those, so the
    /// ports report derived BOTH numbers from the advertised one and answered the advertised
    /// address to the question "what am I listening on", which is the question someone asks
    /// precisely when those two have come apart.
    ///
    /// Empty means the two are the same, which is the ordinary case.
    #[serde(default)]
    pub listen_addr: String,
    /// I/O timeout (ms) for forwarding a `/context/*` request to the owning
    /// datanode. Larger than the command io_timeout because extraction /
    /// embedding generation runs inline on the datanode.
    #[serde(default = "default_context_io_timeout_ms")]
    pub context_io_timeout_ms: u64,
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
/// What a proxy will accept.
///
/// Five names, three behaviours. Two pairs mean exactly the same thing to admission, and
/// nothing said so until now -- an operator picking between them was picking between synonyms
/// while believing the choice mattered.
pub enum ProxyServingMode {
    /// Reads and writes both accepted.
    #[default]
    Serving,
    /// Writes refused with `proxy_write_disabled`; reads accepted.
    ///
    /// Identical to `WriteDisabled` in effect. The two exist because both spellings arrive
    /// over the wire from the metaserver, not because they differ.
    Readonly,
    /// Writes refused with `proxy_write_disabled`; reads accepted. Identical to `Readonly`.
    WriteDisabled,
    /// **Reads and writes both accepted -- identical to `Serving` for admission.**
    ///
    /// This is a label, not a control. It changes what the status surfaces and the serving-mode
    /// gauge report, so a fleet can be marked unhealthy without changing what it serves; it
    /// does not shed, restrict, or refuse anything. Setting it expecting protection gets none,
    /// which is the reason it is spelled out here.
    Degraded,
    /// Everything refused with `proxy_not_serving`. This is the drain state.
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

    /// Timeouts for metaserver control-plane calls. Same connect budget, but the
    /// read budget is the heartbeat one -- see `heartbeat_timeout_ms`.
    pub(super) fn control_http_options(&self) -> HttpRequestOptions {
        HttpRequestOptions {
            connect_timeout_ms: self.connect_timeout_ms,
            io_timeout_ms: self.heartbeat_timeout_ms.max(self.io_timeout_ms),
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
            ingestion_account: String::new(),
            enforce_ingestion_account: false,
            max_inflight_requests: 0,
            max_inflight_write_requests: 0,
            pin_primary_reads: default_pin_primary_reads(),
            context_first_shard_id: default_context_first_shard_id(),
            context_shard_count: default_context_shard_count(),
            context_io_timeout_ms: default_context_io_timeout_ms(),
            heartbeat_timeout_ms: default_heartbeat_timeout_ms(),
            topology_check_interval_ms: default_topology_check_interval_ms(),
            auto_register_min_interval_ms: default_auto_register_min_interval_ms(),
            listen_addr: String::new(),
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
    pub account_rejections: u64,
    pub inflight_rejections: u64,
    /// `/context/*` traffic, counted per route. These are the only routes the context
    /// gateway calls, so without them a proxy saturated by gateway traffic reports
    /// `execute` and `batch_execute` at zero and looks idle. They are counted separately
    /// because their costs differ by orders of magnitude: ingest is a fast-ack buffered
    /// write, extract runs extraction on the data node, retrieve is a read.
    pub context_ingest_requests: u64,
    pub context_extract_requests: u64,
    pub context_retrieve_requests: u64,
    pub heartbeat_total: u64,
    /// Beats whose round-trip consumed the whole interval, so the loop had no time left to
    /// sleep. A rising count means the heartbeat period is being set by metaserver latency
    /// rather than by the configured interval.
    pub heartbeat_slow_total: u64,
    /// Topology checks not made because one was made within `topology_check_interval_ms`.
    /// This is the count of metaserver round-trips kept off the request path.
    pub topology_checks_skipped: u64,
    /// Registration attempts not made because one was made within
    /// `auto_register_min_interval_ms`.
    pub auto_register_throttled: u64,
    /// Writes abandoned because the failure did not prove they never arrived, so replaying
    /// them could have applied them twice. These are the writes whose fate is unknown.
    pub writes_of_unknown_outcome: u64,
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
    pub native_surface: String,
    pub rust_native_route: String,
    pub rust_alias: String,
    pub covered: bool,
    pub notes: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyOperationalSurfaceReport {
    pub status: Status,
    pub legacy_brpc_thrift_in_scope: bool,
    pub rust_native_aliases_ready: bool,
    pub compared_files: Vec<String>,
    pub entries: Vec<ProxyOperationalSurfaceEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyMetricFamilyMapping {
    pub native_surface: String,
    pub rust_prometheus_family: String,
    pub rust_labels: Vec<String>,
    pub grafana_panel: String,
    pub covered: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyMetricsParityReport {
    pub status: Status,
    pub compared_files: Vec<String>,
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

/// What `GET /readiness` answers for a proxy.
///
/// The build-wide capability report is still here, flattened, so anything already reading
/// those fields keeps working. What is new is the state of THIS proxy, because the endpoint
/// was answering a question nobody asked: the capability report is a description of what the
/// code supports and is identical on every process, so a drained proxy reported ready and an
/// orchestrator kept sending it traffic it refuses.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyReadinessResponse {
    /// Whether this proxy will accept anything at all. False only while drained.
    pub serving: bool,
    pub serving_mode: ProxyServingMode,
    /// Reads and writes separately, since refusing writes is not the same as refusing traffic.
    pub serving_reads: bool,
    pub serving_writes: bool,
    #[serde(flatten)]
    pub production: crate::ProductionReadinessReport,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyPolicyReport {
    pub serving_mode: ProxyServingMode,
    pub drop_percent: u8,
    pub serving_reads: bool,
    pub serving_writes: bool,
    pub rejecting_all: bool,
    pub admission_rejections: u64,
    #[serde(default)]
    pub account_rejections: u64,
    #[serde(default)]
    pub inflight_rejections: u64,
    #[serde(default)]
    pub enforce_ingestion_account: bool,
    #[serde(default)]
    pub ingestion_account: String,
    #[serde(default)]
    pub max_inflight_requests: u64,
    #[serde(default)]
    pub max_inflight_write_requests: u64,
    #[serde(default)]
    pub inflight_requests: u64,
    #[serde(default)]
    pub inflight_write_requests: u64,
    #[serde(default)]
    pub pin_primary_reads: bool,
    /// Shards the context routes actually spread tenants over, and where that number came
    /// from -- "configured", "cluster", or "fallback_until_cluster_known". Reported because
    /// this used to be a silent 1: nothing said every tenant was landing on one shard.
    #[serde(default)]
    pub context_shard_count: u64,
    #[serde(default)]
    pub context_shard_count_source: String,
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
pub struct ProxyMigrationContract {
    pub compatibility_decision: String,
    pub legacy_wire_in_scope: bool,
    pub native_wire_proxy_transport_ready: bool,
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

impl Default for ProxyMigrationContract {
    fn default() -> Self {
        Self {
            compatibility_decision:
                "legacy command transport is out of scope; use Rust-native HTTP/JSON, RESP, and tonic"
                    .to_string(),
            legacy_wire_in_scope: false,
            native_wire_proxy_transport_ready: false,
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

/// Which counter an admission rejection increments alongside the shared
/// `admission_rejections` total.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProxyRejectionKind {
    Policy,
    Account,
    Inflight,
}

#[derive(Debug, Clone)]
pub struct ProxyService {
    inner: Arc<ProxyInner>,
}

#[derive(Debug)]
struct ProxyInner {
    /// Behind an `Arc` so reading the live options is a refcount bump rather than a deep
    /// copy. `ProxyOptions` carries six heap strings, and `options()` is called on the
    /// request path -- admission reads it for every command and every `/context/*` call --
    /// so cloning it per request meant six allocations to look at a handful of scalars.
    /// Writers swap in a whole new `Arc`, so a reader never observes a half-updated config.
    options: RwLock<Arc<ProxyOptions>>,
    client: RwLock<TemporalStoreClient>,
    last_client_stats: RwLock<ClientStats>,
    stats: RwLock<ProxyStats>,
    service_discovery: RwLock<ProxyServiceDiscoveryState>,
    inflight: ProxyInflight,
    boot_time_ms: u64,
    /// Wall-clock ms of the last topology check, 0 for "never". Read and written on the
    /// request path, so an atomic rather than a lock.
    last_topology_check_ms: std::sync::atomic::AtomicU64,
    /// Wall-clock ms of the last auto-registration attempt, 0 for "never".
    last_auto_register_ms: std::sync::atomic::AtomicU64,
    /// Shard count last read from the metaserver, 0 for "not asked yet". Only consulted when
    /// `context_shard_count` is 0.
    /// The counters every request touches.
    ///
    /// These were fields of `ProxyStats` behind an RwLock, so each one cost an EXCLUSIVE
    /// lock on the request path -- taken to record that a request happened, and in the
    /// topology check's case to record that nothing happened. Eight threads doing that
    /// deliver less aggregate throughput than one, because they serialize on the writer.
    /// They are folded back into `ProxyStats` in `sync_client_stats`, which runs where the
    /// stats are read, so readers see the same numbers.
    ///
    /// Counters NOT here are deliberate: the error and heartbeat paths are not per request,
    /// so a lock there costs nothing worth removing.
    execute_requests: std::sync::atomic::AtomicU64,
    batch_execute_requests: std::sync::atomic::AtomicU64,
    /// `topology_check_interval_ms`, mirrored so the request path can read it without
    /// taking the options lock.
    ///
    /// After the counters became atomics this was the last lock left on the common path:
    /// every request took a read lock and cloned an Arc to fetch one u64 that changes only
    /// when an operator pushes a config. Kept in step by `update_options_report`, which is
    /// the single place the running options are replaced.
    topology_check_interval_ms: std::sync::atomic::AtomicU64,
    topology_checks_skipped: std::sync::atomic::AtomicU64,
    bad_requests: std::sync::atomic::AtomicU64,
    context_ingest_requests: std::sync::atomic::AtomicU64,
    context_extract_requests: std::sync::atomic::AtomicU64,
    context_retrieve_requests: std::sync::atomic::AtomicU64,
    cluster_shard_count: std::sync::atomic::AtomicU64,
    /// Whether the metaserver has been asked for the shard list yet. Distinguishes "we have
    /// not looked" from "we looked and the ids cannot be addressed as a range" -- reporting
    /// the second when the first is true would be a claim we have not earned.
    cluster_shards_checked: std::sync::atomic::AtomicBool,
    /// Whether the shard ids the metaserver listed form a contiguous run from
    /// `context_first_shard_id`. Only meaningful once `cluster_shards_checked` is true.
    cluster_shards_contiguous: std::sync::atomic::AtomicBool,
}

impl ProxyService {
    pub fn new(options: ProxyOptions) -> Self {
        // Read before the options move into the Arc below.
        let topology_check_interval_ms = options.topology_check_interval_ms;
        Self {
            inner: Arc::new(ProxyInner {
                client: RwLock::new(proxy_client_from_options(&options)),
                options: RwLock::new(Arc::new(options)),
                last_client_stats: RwLock::default(),
                stats: RwLock::default(),
                service_discovery: RwLock::default(),
                inflight: ProxyInflight::default(),
                boot_time_ms: now_ms(),
                last_topology_check_ms: std::sync::atomic::AtomicU64::new(0),
                last_auto_register_ms: std::sync::atomic::AtomicU64::new(0),
                execute_requests: std::sync::atomic::AtomicU64::new(0),
                batch_execute_requests: std::sync::atomic::AtomicU64::new(0),
                topology_check_interval_ms: std::sync::atomic::AtomicU64::new(
                    topology_check_interval_ms,
                ),
                topology_checks_skipped: std::sync::atomic::AtomicU64::new(0),
                bad_requests: std::sync::atomic::AtomicU64::new(0),
                context_ingest_requests: std::sync::atomic::AtomicU64::new(0),
                context_extract_requests: std::sync::atomic::AtomicU64::new(0),
                context_retrieve_requests: std::sync::atomic::AtomicU64::new(0),
                cluster_shard_count: std::sync::atomic::AtomicU64::new(0),
                cluster_shards_checked: std::sync::atomic::AtomicBool::new(false),
                cluster_shards_contiguous: std::sync::atomic::AtomicBool::new(false),
            }),
        }
    }

    pub fn execute(&self, request: ExecuteRequest) -> ExecuteResponse {
        self.inner
            .execute_requests
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _admitted = match self.admit(None, std::slice::from_ref(&request.command)) {
            Ok(guard) => guard,
            Err(status) => return execute_error(status.code, status.message),
        };
        self.invalidate_cached_routes_if_meta_changed();
        let response = self
            .client()
            .execute_with_options(request, RequestOptions::default())
            .unwrap_or_else(|err| execute_error(proxy_client_error_code(&err), err.to_string()));
        response
    }

    pub fn batch_execute(&self, request: BatchExecuteRequest) -> BatchExecuteResponse {
        self.inner
            .batch_execute_requests
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _admitted = match self.admit(None, &request.commands) {
            Ok(guard) => guard,
            Err(status) => {
                return BatchExecuteResponse {
                    status,
                    responses: Vec::new(),
                }
            }
        };
        self.invalidate_cached_routes_if_meta_changed();
        let response = self
            .client()
            .batch_execute_with_options(request, RequestOptions::default())
            .unwrap_or_else(|err| BatchExecuteResponse {
                status: Status::error(proxy_client_error_code(&err), err.to_string()),
                responses: Vec::new(),
            });
        response
    }

    pub fn open_table(&self, request: ProxyOpenTableRequest) -> ProxyOpenTableResponse {
        // Opening a table is a metaserver round-trip, so it is real work and belongs inside
        // the concurrency envelope. Without a slot, a caller could issue unbounded concurrent
        // open_table calls and never touch `max_inflight_requests` -- the quota exists to
        // bound the work in flight, and this was work it could not see. It counts as a read:
        // it mutates nothing on the data path.
        let _admitted = match self.admit_open_table(&request.namespace) {
            Ok(guard) => guard,
            Err(status) => {
                return ProxyOpenTableResponse {
                    status,
                    options: None,
                }
            }
        };
        let pin_primary_default = self.options().pin_primary_reads;
        match self
            .client()
            .open_table_from_meta(request.namespace, request.table_name)
        {
            Ok(table) => {
                let pin_primary = request.pin_primary.unwrap_or(pin_primary_default);
                if pin_primary != table.options().pin_primary
                    || request.replica_read_policy.is_some()
                {
                    let mut options = table.options();
                    options.pin_primary = pin_primary;
                    if let Some(policy) = request.replica_read_policy {
                        options.replica_read_policy = policy.into();
                    }
                    let table = self.client().open_table(
                        table.namespace().to_string(),
                        table.table_name().to_string(),
                        options.clone(),
                    );
                                return ProxyOpenTableResponse {
                        status: Status::ok(),
                        options: Some(table.options().into()),
                    };
                }
                let options = table.options();
                        ProxyOpenTableResponse {
                    status: Status::ok(),
                    options: Some(options.into()),
                }
            }
            Err(err) => {
                        ProxyOpenTableResponse {
                    status: Status::error("metaserver_error", err.to_string()),
                    options: None,
                }
            }
        }
    }

    pub fn table_execute(&self, request: ProxyTableExecuteRequest) -> ExecuteResponse {
        self.inner
            .execute_requests
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _admitted = match self.admit(
            Some(&request.namespace),
            std::slice::from_ref(&request.command),
        ) {
            Ok(guard) => guard,
            Err(status) => return execute_error(status.code, status.message),
        };
        self.invalidate_cached_routes_if_meta_changed();
        let response = self
            .table_for_request(request.namespace, request.table_name)
            .and_then(|table| table.execute(request.command))
            .unwrap_or_else(|err| execute_error(proxy_client_error_code(&err), err.to_string()));
        response
    }

    pub fn table_batch_execute(
        &self,
        request: ProxyTableBatchExecuteRequest,
    ) -> BatchExecuteResponse {
        self.inner
            .batch_execute_requests
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _admitted = match self.admit(Some(&request.namespace), &request.commands) {
            Ok(guard) => guard,
            Err(status) => {
                return BatchExecuteResponse {
                    status,
                    responses: Vec::new(),
                }
            }
        };
        self.invalidate_cached_routes_if_meta_changed();
        let response = self
            .table_for_request(request.namespace, request.table_name)
            .and_then(|table| table.batch_execute(request.commands))
            .unwrap_or_else(|err| BatchExecuteResponse {
                status: Status::error(proxy_client_error_code(&err), err.to_string()),
                responses: Vec::new(),
            });
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

    /// `POST /proxy/config` replaces the whole options document, so a caller
    /// that omits a field gets the serde default for it. For the admission
    /// options that default is the permissive one, and silently dropping
    /// account enforcement because a config push predates the field is not an
    /// acceptable failure mode. Keys absent from the body therefore carry the
    /// running value forward; only an explicit key changes them.
    /// Apply a `POST /proxy/config` body on top of the running options, keeping any field
    /// the body does not mention.
    ///
    /// The route replaces the WHOLE options document, so every key an operator left out took
    /// its serde default rather than its running value. Only five admission fields were
    /// carried forward, which meant a config push that omitted `serving_mode` silently put a
    /// DRAINED proxy back into service, and one that omitted `location` erased the locality
    /// that decides which replica reads go to. Neither said anything in the report.
    ///
    /// Done by merging JSON objects rather than by listing fields, so a field added later is
    /// carried forward without anyone remembering to come back here. A hand-maintained list
    /// is what limited this to five fields in the first place.
    ///
    /// `config_version` is deliberately NOT carried forward: zero means "derive the version
    /// from the config hash", and carrying a previous explicit version onto changed content
    /// would report the new config under the old number.
    pub(super) fn merge_config_push(&self, body: &[u8]) -> Result<ProxyOptions, String> {
        let supplied = serde_json::from_slice::<serde_json::Value>(body).map_err(|err| err.to_string())?;
        let serde_json::Value::Object(mut supplied) = supplied else {
            // Not an object at all -- let the normal parse produce the normal error.
            return serde_json::from_slice::<ProxyOptions>(body).map_err(|err| err.to_string());
        };
        let current = serde_json::to_value(&*self.options()).map_err(|err| err.to_string())?;
        let serde_json::Value::Object(mut merged) = current else {
            return serde_json::from_slice::<ProxyOptions>(body).map_err(|err| err.to_string());
        };
        let supplied_version = supplied.remove("config_version");
        merged.remove("config_version");
        for (key, value) in supplied {
            merged.insert(key, value);
        }
        if let Some(version) = supplied_version {
            merged.insert("config_version".to_string(), version);
        }
        serde_json::from_value::<ProxyOptions>(serde_json::Value::Object(merged))
            .map_err(|err| err.to_string())
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
        // Before the options move into the Arc: the request path reads this without a
        // lock, so it has to be updated wherever the running options are replaced. This is
        // that one place.
        self.inner.topology_check_interval_ms.store(
            options.topology_check_interval_ms,
            std::sync::atomic::Ordering::Relaxed,
        );
        *self
            .inner
            .options
            .write()
            .expect("proxy options lock poisoned") = Arc::new(options);
        report.applied = true;
        report.reason = "config_changed".to_string();
        report
    }

    fn get_shard(&self, shard_id: ShardId, count_error: bool) -> Result<GetShardResponse, Status> {
        let options = self.options();
        get_json_with_options_and_headers::<GetShardResponse>(
            &options.meta_addr,
            &format!("/shards/{shard_id}"),
            &crate::meta::admin_auth_header(),
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

    /// Fold the client's counters into this proxy's, from the READ side.
    ///
    /// This used to run on every request. It only ever computes deltas against monotonic
    /// client counters, so running it when the numbers are read gives the same answer for
    /// three locks and a `ClientStats` clone less per request -- and it is strictly fresher,
    /// because three of the five readers (`info`, `heartbeat_report`, `policy_report`) did
    /// not sync at all and so reported whatever the last data request happened to leave
    /// behind. On a proxy whose counters moved through background meta-sync alone, they were
    /// simply stale.
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
        stats.writes_of_unknown_outcome += current
            .writes_of_unknown_outcome
            .saturating_sub(last.writes_of_unknown_outcome);
        // The request-path counters live outside the lock; picked up here, where the stats
        // are being written anyway, so every reader still sees a current number.
        stats.execute_requests = self
            .inner
            .execute_requests
            .load(std::sync::atomic::Ordering::Relaxed);
        stats.batch_execute_requests = self
            .inner
            .batch_execute_requests
            .load(std::sync::atomic::Ordering::Relaxed);
        stats.topology_checks_skipped = self
            .inner
            .topology_checks_skipped
            .load(std::sync::atomic::Ordering::Relaxed);
        stats.bad_requests = self
            .inner
            .bad_requests
            .load(std::sync::atomic::Ordering::Relaxed);
        stats.context_ingest_requests = self
            .inner
            .context_ingest_requests
            .load(std::sync::atomic::Ordering::Relaxed);
        stats.context_extract_requests = self
            .inner
            .context_extract_requests
            .load(std::sync::atomic::Ordering::Relaxed);
        stats.context_retrieve_requests = self
            .inner
            .context_retrieve_requests
            .load(std::sync::atomic::Ordering::Relaxed);
        *last = current;
    }

    /// The live options. Cheap: clones an `Arc`, not the six strings inside it. Callers that
    /// need to MUTATE take an owned copy explicitly via `options_owned`.
    fn options(&self) -> Arc<ProxyOptions> {
        Arc::clone(
            &self
                .inner
                .options
                .read()
                .expect("proxy options lock poisoned"),
        )
    }

    /// An owned, mutable copy of the live options, for the config-update paths.
    fn options_owned(&self) -> ProxyOptions {
        (*self.options()).clone()
    }

    fn reject(&self, status: Status, kind: ProxyRejectionKind) -> Status {
        let mut stats = self.inner.stats.write().expect("proxy stats lock poisoned");
        stats.admission_rejections += 1;
        match kind {
            ProxyRejectionKind::Policy => {}
            ProxyRejectionKind::Account => stats.account_rejections += 1,
            ProxyRejectionKind::Inflight => stats.inflight_rejections += 1,
        }
        status
    }

    /// Namespace scope check for the request paths that carry one. Requests
    /// admitted here still have to pass `admit`.
    fn check_account_scope(&self, namespace: &str) -> Option<Status> {
        let options = self.options();
        proxy_account_rejection(&options, namespace)
            .map(|status| self.reject(status, ProxyRejectionKind::Account))
    }

    /// Full admission for a request: account scope (where a namespace is
    /// carried), serving-mode/drop policy, then an in-flight slot. The returned
    /// guard releases the slot when it is dropped, so every return path from the
    /// caller decrements exactly once.
    fn admit(
        &self,
        namespace: Option<&str>,
        commands: &[Command],
    ) -> Result<ProxyInflightGuard<'_>, Status> {
        let options = self.options();
        if let Some(namespace) = namespace {
            if let Some(status) = proxy_account_rejection(&options, namespace) {
                return Err(self.reject(status, ProxyRejectionKind::Account));
            }
        }
        if let Some(status) = proxy_policy_rejection(&options, commands) {
            return Err(self.reject(status, ProxyRejectionKind::Policy));
        }
        let is_write = commands.iter().any(proxy_command_is_write);
        self.inner
            .inflight
            .try_acquire(
                is_write,
                options.max_inflight_requests,
                options.max_inflight_write_requests,
            )
            .map_err(|rejection| self.reject(rejection.status(), ProxyRejectionKind::Inflight))
    }

    /// Admission for the high-level `/context/*` routes. These forward straight
    /// to the owning datanode instead of going through command execution, so
    /// without this they would be the one class of traffic -- and the only class
    /// the context gateway sends -- that ignores drain, write-disable, account
    /// scope, and the in-flight quotas.
    pub(super) fn admit_context(
        &self,
        scope: &context::ProxyContextScope,
        is_write: bool,
    ) -> Result<ProxyInflightGuard<'_>, (u16, Vec<u8>)> {
        let options = self.options();
        let rejection = proxy_account_rejection(&options, &scope.account_id)
            .map(|status| (status, ProxyRejectionKind::Account))
            .or_else(|| {
                proxy_serving_rejection(&options, is_write)
                    .map(|status| (status, ProxyRejectionKind::Policy))
            })
            .or_else(|| {
                proxy_drop_rejection(&options, &context_drop_key(scope))
                    .map(|status| (status, ProxyRejectionKind::Policy))
            });
        if let Some((status, kind)) = rejection {
            return Err(self.context_rejection_response(status, kind));
        }
        self.inner
            .inflight
            .try_acquire(
                is_write,
                options.max_inflight_requests,
                options.max_inflight_write_requests,
            )
            .map_err(|rejection| {
                self.context_rejection_response(rejection.status(), ProxyRejectionKind::Inflight)
            })
    }

    fn context_rejection_response(
        &self,
        status: Status,
        kind: ProxyRejectionKind,
    ) -> (u16, Vec<u8>) {
        let status = self.reject(status, kind);
        crate::http::json_response(proxy_rejection_http_status(&status.code), &status)
    }

    /// Read-only view of the live options, for tests and callers that need to see what the
    /// metaserver has granted this proxy.
    pub fn config_snapshot(&self) -> ProxyOptions {
        self.options_owned()
    }

    /// Admission for `open_table`: account scope, serving mode, then an in-flight slot. There
    /// is no `Command` here, so the drop percentage -- which keys on a command's routing key --
    /// does not apply; a dropped tenant is still refused when it tries to execute.
    fn admit_open_table(&self, namespace: &str) -> Result<ProxyInflightGuard<'_>, Status> {
        let options = self.options();
        if let Some(status) = proxy_account_rejection(&options, namespace) {
            return Err(self.reject(status, ProxyRejectionKind::Account));
        }
        if let Some(status) = proxy_serving_rejection(&options, false) {
            return Err(self.reject(status, ProxyRejectionKind::Policy));
        }
        self.inner
            .inflight
            .try_acquire(
                false,
                options.max_inflight_requests,
                options.max_inflight_write_requests,
            )
            .map_err(|rejection| self.reject(rejection.status(), ProxyRejectionKind::Inflight))
    }

    /// Admission for `GET /shards/{id}`.
    ///
    /// This route answers a client asking where a shard lives, and answering means a
    /// metaserver round-trip. It took no in-flight slot, so it was the one piece of client
    /// traffic that could not be bounded: `max_inflight_requests` capped execute and the
    /// context routes while an unbounded number of lookups went straight through to the
    /// metaserver. A client stampede was amplified onto the metaserver by the very component
    /// meant to shield it. A drained proxy served them too.
    ///
    /// Admitted as a read. There is no namespace on the wire for this route, so account
    /// scope cannot be checked here -- the shard id alone carries no tenant.
    fn admit_shard_lookup(&self) -> Result<ProxyInflightGuard<'_>, Status> {
        let options = self.options();
        if let Some(status) = proxy_serving_rejection(&options, false) {
            return Err(self.reject(status, ProxyRejectionKind::Policy));
        }
        self.inner
            .inflight
            .try_acquire(
                false,
                options.max_inflight_requests,
                options.max_inflight_write_requests,
            )
            .map_err(|rejection| self.reject(rejection.status(), ProxyRejectionKind::Inflight))
    }

    /// Admission for `POST /proxy/topology/refresh`.
    ///
    /// Bounded, but deliberately NOT refused while draining. This is how an operator makes a
    /// proxy pick up a topology change, and a proxy that has been drained is exactly when
    /// someone needs to do that -- gating it on serving mode would take the tool away at the
    /// moment it is wanted. What it does need is a ceiling: each call is a metaserver
    /// round-trip, and nothing stopped a script from issuing them without limit.
    fn admit_topology_refresh(&self) -> Result<ProxyInflightGuard<'_>, Status> {
        let options = self.options();
        self.inner
            .inflight
            .try_acquire(
                false,
                options.max_inflight_requests,
                options.max_inflight_write_requests,
            )
            .map_err(|rejection| self.reject(rejection.status(), ProxyRejectionKind::Inflight))
    }

    /// The options the proxy actually handed its client, so callers (and tests) can see what
    /// was carried through rather than what was merely configured.
    pub fn client_options_snapshot(&self) -> crate::client::ClientOptions {
        self.client().client_options()
    }

    pub(super) fn inflight_snapshot(&self) -> (u64, u64) {
        self.inner.inflight.snapshot()
    }

    fn invalidate_cached_routes_if_meta_changed(&self) {
        // The cheap question first, but only as a question. Nearly every request arrives
        // inside the interval and leaves here, and asking for the client is a read lock and
        // an Arc clone while counting the route cache takes the cache's own lock -- both
        // were paid by every request on its way to finding out there was nothing to do.
        //
        // The slot is claimed below, not here, and that ordering is load-bearing. Claiming
        // it before the empty-cache check means a proxy with no routes yet burns the slot,
        // no check is then due for a whole interval after its routes appear, and under
        // topology churn it keeps serving routes it should have dropped -- reads come back
        // empty. That is measured, not theoretical: it is what
        // `proxy_multi_proxy_converges_under_topology_churn_stale_cache_and_recovery`
        // reports when the claim happens too early.
        if !self.topology_check_due_now() {
            self.inner
                .topology_checks_skipped
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return;
        }
        let client = self.client();
        if client.route_cache_size() == 0 {
            // Nothing to invalidate, and deliberately no slot consumed: the first request
            // that does have routes must still find a check due.
            return;
        }
        if !self.claim_topology_check() {
            self.inner
                .topology_checks_skipped
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return;
        }
        let _ = client.invalidate_routes_from_meta_topology();
    }

    /// How many shards the context routes actually spread tenants over.
    ///
    /// A configured non-zero value wins. Otherwise the cluster's shard count is used, which is
    /// read from the metaserver on the heartbeat rather than per request -- a lookup on the
    /// request path is what the route cache and the topology interval exist to avoid.
    ///
    /// Falls back to 1 only while the count is still unknown (before the first heartbeat, or
    /// if the metaserver cannot be reached). That is the old behaviour, so a proxy that cannot
    /// ask degrades to what it did before rather than to nothing.
    pub(super) fn effective_context_shard_count(&self) -> u64 {
        let configured = self.options().context_shard_count;
        if configured != 0 {
            return configured;
        }
        match self
            .inner
            .cluster_shard_count
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            0 => 1,
            known => known,
        }
    }

    /// Where `effective_context_shard_count` got its answer, for the status surfaces.
    pub(super) fn context_shard_count_source(&self) -> &'static str {
        if self.options().context_shard_count != 0 {
            "configured"
        } else if self
            .inner
            .cluster_shard_count
            .load(std::sync::atomic::Ordering::Relaxed)
            != 0
        {
            "cluster"
        } else if !self
            .inner
            .cluster_shards_checked
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            "fallback_until_cluster_known"
        } else {
            "fallback_cluster_shards_not_contiguous"
        }
    }

    /// Learn how many shards the context routes may span, from the metaserver.
    ///
    /// Called from the heartbeat, which already talks to the metaserver on a fixed cadence, so
    /// this adds no new schedule and nothing to the request path.
    ///
    /// It asks for the shard LIST rather than a count, because a count is not enough to be
    /// safe. A tenant's shard is `context_first_shard_id + hash % count`, which addresses a
    /// CONTIGUOUS run of ids. A cluster whose shards are 1-4 and 100-103 has a count of eight
    /// and no shards 5-8, so adopting the count alone would send those tenants to ids that do
    /// not exist -- worse than the single-shard default it replaces, because the default at
    /// least addressed a shard that was there.
    ///
    /// So the count is adopted only when the ids actually form a contiguous run starting at
    /// `context_first_shard_id`. Otherwise the range stays unknown, the fallback of 1 applies,
    /// and the reason is reported rather than left to be inferred from failures.
    pub(super) fn refresh_cluster_shard_count(&self) {
        let options = self.options();
        if options.context_shard_count != 0 {
            return;
        }
        let Ok(listed) = get_json_with_options_and_headers::<crate::meta::ListShardsResponse>(
            &options.meta_addr,
            "/shards",
            &crate::meta::admin_auth_header(),
            options.control_http_options(),
        ) else {
            return;
        };
        if !listed.status.ok || listed.shards.is_empty() {
            return;
        }
        let mut ids: Vec<crate::types::ShardId> =
            listed.shards.iter().map(|shard| shard.shard_id).collect();
        ids.sort_unstable();
        ids.dedup();
        let contiguous_from_first = ids
            .iter()
            .enumerate()
            .all(|(offset, id)| *id == options.context_first_shard_id + offset as u64);
        // A short page means there are more shards than were listed; a partial view could look
        // contiguous when the whole set is not, so do not conclude anything from it.
        let complete = listed.next_after_shard_id.is_none();
        let usable = complete && contiguous_from_first;
        self.inner.cluster_shard_count.store(
            if usable { ids.len() as u64 } else { 0 },
            std::sync::atomic::Ordering::Relaxed,
        );
        self.inner.cluster_shards_contiguous.store(
            usable,
            std::sync::atomic::Ordering::Relaxed,
        );
        self.inner
            .cluster_shards_checked
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// This proxy's answer to a readiness probe, and the HTTP status that goes with it.
    ///
    /// A drained proxy answers 503. That is the whole point of a readiness probe: drain exists
    /// to stop traffic arriving, and answering 200 while refusing every request with
    /// `proxy_not_serving` defeats the control it is supposed to serve.
    ///
    /// Read-only and write-disabled still answer 200 -- they serve reads, and a probe that
    /// failed for them would take a proxy out of rotation that is doing useful work. The two
    /// flags say which is which for anyone who needs the distinction.
    pub fn readiness_response(&self) -> (u16, ProxyReadinessResponse) {
        let options = self.options();
        let serving_reads = !matches!(options.serving_mode, ProxyServingMode::NotServing);
        let serving_writes = matches!(
            options.serving_mode,
            ProxyServingMode::Serving | ProxyServingMode::Degraded
        );
        let response = ProxyReadinessResponse {
            serving: serving_reads,
            serving_mode: options.serving_mode,
            serving_reads,
            serving_writes,
            production: crate::production_readiness_report(),
        };
        let status = if serving_reads { 200 } else { 503 };
        (status, response)
    }

    /// Whether enough time has passed to try registering with the metaserver again.
    ///
    /// Claims the slot as it answers, so two heartbeats cannot both decide they are due.
    pub(super) fn auto_register_is_due(&self) -> bool {
        let interval = self.options().auto_register_min_interval_ms;
        if interval == 0 {
            return true;
        }
        let now = now_ms();
        let last = self
            .inner
            .last_auto_register_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        if last != 0 && now >= last && now.saturating_sub(last) < interval {
            return false;
        }
        self.inner
            .last_auto_register_ms
            .compare_exchange(
                last,
                now,
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_ok()
    }

    /// Whether enough time has passed to ask the metaserver about topology again.
    ///
    /// Claims the slot as a side effect, so concurrent requests do not all decide they are
    /// due and issue the same round-trip. Losing that race means skipping a check that
    /// another thread is making right now, which is the correct outcome.
    /// Whether a topology check is due, without claiming it.
    ///
    /// Split from the claim so the request path can ask the cheap question -- a load --
    /// before paying for a client handle or the route-cache lock, and still leave the slot
    /// for whoever actually does the work. Answering yes here does not entitle the caller
    /// to the check; `claim_topology_check` does.
    fn topology_check_due_now(&self) -> bool {
        let interval = self
            .inner
            .topology_check_interval_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        if interval == 0 {
            return true;
        }
        let now = now_ms();
        let last = self
            .inner
            .last_topology_check_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        // `now < last` only if the wall clock went backwards; treat that as due rather than
        // locking the check out until the clock catches up.
        !(last != 0 && now >= last && now.saturating_sub(last) < interval)
    }

    /// Take the check slot if it is still there. One caller wins per interval.
    fn claim_topology_check(&self) -> bool {
        let interval = self
            .inner
            .topology_check_interval_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        if interval == 0 {
            return true;
        }
        let now = now_ms();
        let last = self
            .inner
            .last_topology_check_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        if last != 0 && now >= last && now.saturating_sub(last) < interval {
            return false;
        }
        self.inner
            .last_topology_check_ms
            .compare_exchange(
                last,
                now,
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_ok()
    }

    fn inc_bad_request(&self) {
        self.inner
            .bad_requests
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn bad_execute_request(&self, err: impl std::fmt::Display) -> (u16, Vec<u8>) {
        use crate::http::json_response;
        self.inc_bad_request();
        json_response(400, &execute_error("bad_request", err.to_string()))
    }
}

/// Deterministic drop key for a context scope. Keyed on account+tenant so a
/// drop percentage sheds whole tenants instead of a random slice of every
/// tenant's session.
fn context_drop_key(scope: &context::ProxyContextScope) -> String {
    format!("{}|{}", scope.account_id, scope.tenant_id)
}

/// HTTP status for an admission rejection on the `/context/*` routes, which
/// return the transport code rather than embedding it in a body the gateway
/// would have to parse to notice it was shed.
/// The status code a client error should reach the caller as.
///
/// Everything used to arrive as `server_error`, which told an application only that something
/// went wrong -- not whether its write had landed. Those are different situations with
/// different correct responses: a failure that provably did not apply can simply be retried,
/// while one that may have applied has to be reconciled, and retrying it is how a write gets
/// counted twice. The proxy knows which it was; the caller could not find out.
fn proxy_client_error_code(err: &crate::client::ClientError) -> &'static str {
    match err {
        crate::client::ClientError::WriteOutcomeUnknown(_) => "write_outcome_unknown",
        _ => "server_error",
    }
}

fn proxy_rejection_http_status(code: &str) -> u16 {
    match code {
        "proxy_account_denied" => 403,
        "proxy_inflight_quota_exceeded"
        | "proxy_write_inflight_quota_exceeded"
        | "proxy_traffic_dropped" => 429,
        _ => 503,
    }
}

fn proxy_operational_surface_entry(
    native_surface: &str,
    rust_native_route: &str,
    rust_alias: &str,
    notes: &str,
) -> ProxyOperationalSurfaceEntry {
    ProxyOperationalSurfaceEntry {
        native_surface: native_surface.to_string(),
        rust_native_route: rust_native_route.to_string(),
        rust_alias: rust_alias.to_string(),
        covered: true,
        notes: notes.to_string(),
    }
}

/// The metric families this proxy actually publishes, read back off the rendered endpoint.
///
/// This used to be a hand-written list, and it had fallen eight families behind what the
/// proxy emits -- every admission metric among them, so anyone building a dashboard from the
/// report got no in-flight quota, no account enforcement and no read pinning. Reading the
/// rendered output back means the list cannot be behind by construction.
pub(super) fn proxy_metric_families_from(rendered: &str) -> Vec<String> {
    let mut families: Vec<String> = rendered
        .lines()
        .filter_map(|line| line.strip_prefix("# TYPE "))
        .filter_map(|rest| rest.split_whitespace().next())
        .map(str::to_string)
        .collect();
    families.sort();
    families.dedup();
    families
}

fn proxy_metrics_parity_mappings() -> Vec<ProxyMetricFamilyMapping> {
    vec![
        proxy_metric_mapping(
            "proxy command/admission counters",
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
    native_surface: &str,
    rust_prometheus_family: &str,
    rust_labels: Vec<&str>,
    grafana_panel: &str,
) -> ProxyMetricFamilyMapping {
    ProxyMetricFamilyMapping {
        native_surface: native_surface.to_string(),
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

    #[test]
    fn a_pushed_topology_check_interval_reaches_the_request_path() {
        // The interval is mirrored outside the options lock so the request path can read
        // it without taking one. A mirror that is not updated when the options are
        // replaced is worse than the lock it saved: the proxy would go on checking at the
        // old cadence, an operator's config push would appear to be accepted, and nothing
        // anywhere would say the two disagreed.
        let proxy = scoped_proxy(ProxyOptions {
            topology_check_interval_ms: 60_000,
            ..ProxyOptions::default()
        });

        // Claim once so the interval is actually in play; after that a minute must pass.
        assert!(proxy.claim_topology_check(), "the first check is due");
        assert!(
            !proxy.topology_check_due_now(),
            "a minute has not passed, so no check is due"
        );

        // Turn the interval off. Every check is due at zero, which is the escape hatch
        // operators use to restore per-request checking.
        let mut pushed = (*proxy.options()).clone();
        pushed.topology_check_interval_ms = 0;
        let report = proxy.update_options_report(pushed);
        assert!(report.applied, "the push must be applied: {report:?}");

        assert!(
            proxy.topology_check_due_now(),
            "interval 0 means every check is due -- if this fails the mirror kept the old \
             value and the proxy is still checking on a cadence the operator replaced"
        );

        // And back the other way, so this cannot pass by the mirror being stuck at zero.
        let mut restored = (*proxy.options()).clone();
        restored.topology_check_interval_ms = 60_000;
        let report = proxy.update_options_report(restored);
        assert!(report.applied, "the second push must be applied: {report:?}");
        assert!(
            !proxy.topology_check_due_now(),
            "with the interval restored, the check claimed at the top of this test is still              inside it, so nothing is due -- a mirror stuck at the pushed zero would say due              here, which is what makes this the reverse-direction check"
        );
    }

    /// Per-request bookkeeping under CONCURRENCY, measured per function.
    ///
    /// Single-threaded this work is ~110ns against a request whose real cost is a network
    /// round trip, so it rounds to nothing. The cost of an exclusive lock is not its
    /// latency, it is the serialization: threads taking `stats.write()` on every request
    /// queue behind one writer, and eight of them then deliver LESS aggregate throughput
    /// than one. Only a contended benchmark shows that.
    ///
    /// The proxy MUST have a cached route. `invalidate_cached_routes_if_meta_changed`
    /// returns at its first guard when the route cache is empty, so without one this
    /// measures that guard and never reaches the due-check or its counter -- which is
    /// exactly the work being changed.
    ///
    /// Run with `--ignored --nocapture`.
    #[test]
    #[ignore]
    fn bench_proxy_per_request_bookkeeping() {
        let threads: usize = std::env::var("BENCH_THREADS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8);
        let per_thread: usize = std::env::var("BENCH_ITERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(50_000);

        let run = |label: &str, sync: bool, topo: bool| {
            let proxy = std::sync::Arc::new(scoped_proxy(ProxyOptions {
                namespace: "bench".to_string(),
                // long enough that every iteration takes the "not due" path, which is the
                // one every request actually walks
                topology_check_interval_ms: 3_600_000,
                ..ProxyOptions::default()
            }));
            proxy
                .client()
                .insert_cached_route_for_test(1, "127.0.0.1:1".to_string());
            for _ in 0..1_000 {
                proxy.sync_client_stats();
                proxy.invalidate_cached_routes_if_meta_changed();
            }
            assert!(
                proxy.client().route_cache_size() > 0,
                "the route cache must be populated or the topology check returns early"
            );

            let gate = std::sync::Arc::new(std::sync::Barrier::new(threads + 1));
            let mut handles = Vec::new();
            for _ in 0..threads {
                let proxy = std::sync::Arc::clone(&proxy);
                let gate = std::sync::Arc::clone(&gate);
                handles.push(std::thread::spawn(move || {
                    gate.wait();
                    for _ in 0..per_thread {
                        if sync {
                            proxy.sync_client_stats();
                        }
                        if topo {
                            proxy.invalidate_cached_routes_if_meta_changed();
                        }
                    }
                }));
            }
            gate.wait();
            let start = std::time::Instant::now();
            for handle in handles {
                handle.join().expect("bench thread panicked");
            }
            let elapsed = start.elapsed();
            let ops = (threads * per_thread) as u128;
            println!(
                "BENCH {label} threads={threads} ops={ops} ns_per_op={} ops_per_sec={}",
                elapsed.as_nanos() / ops,
                ops * 1_000_000_000 / elapsed.as_nanos().max(1)
            );
        };

        run("sync_plus_topology", true, true);
        run("topology_only", false, true);
        run("sync_only", true, false);
    }
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

        let migration = proxy.native_migration_contract();
        assert_eq!(
            migration.compatibility_decision,
            "legacy command transport is out of scope; use Rust-native HTTP/JSON, RESP, and tonic"
        );
        assert!(!migration.legacy_wire_in_scope);
        assert!(!migration.native_wire_proxy_transport_ready);
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
            path: "/proxy/native_migration_contract".to_string(),
            body: Vec::new(),
        });
        assert_eq!(code, 200);
        assert_eq!(
            parse_json::<ProxyMigrationContract>(&body).unwrap(),
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
            .compared_files
            .iter()
            .any(|path| path.ends_with("/src/proxy/handle.rs")));
        assert!(report.mappings.iter().any(|mapping| {
            mapping.native_surface.contains("command/admission")
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
            numa_nodes: Vec::new(),
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
            // This test moves a shard and immediately re-requests, so it wants a topology
            // check on every request. Zero is exactly the pre-interval behaviour.
            topology_check_interval_ms: 0,
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
            numa_nodes: Vec::new(),
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
            numa_nodes: Vec::new(),
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
            numa_nodes: Vec::new(),
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

    fn read_command() -> Command {
        Command::StringGet {
            key: "k".to_string(),
        }
    }

    fn write_command() -> Command {
        Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        }
    }

    fn scoped_proxy(options: ProxyOptions) -> ProxyService {
        ProxyService::new(ProxyOptions {
            meta_addr: "127.0.0.1:1".to_string(),
            ..options
        })
    }

    #[test]
    fn proxy_account_scope_rejects_foreign_namespaces() {
        let proxy = scoped_proxy(ProxyOptions {
            ingestion_account: "tenant-a".to_string(),
            enforce_ingestion_account: true,
            ..ProxyOptions::default()
        });

        let denied = proxy.table_execute(ProxyTableExecuteRequest {
            namespace: "tenant-b".to_string(),
            table_name: "t".to_string(),
            command: read_command(),
        });
        assert_eq!(denied.status.code, "proxy_account_denied");

        let denied_open = proxy.open_table(ProxyOpenTableRequest {
            namespace: "tenant-b".to_string(),
            table_name: "t".to_string(),
            pin_primary: None,
            replica_read_policy: None,
        });
        assert_eq!(denied_open.status.code, "proxy_account_denied");
        assert!(denied_open.options.is_none());

        let policy = proxy.policy_report();
        assert_eq!(policy.account_rejections, 2);
        assert_eq!(policy.admission_rejections, 2);
        assert!(policy.enforce_ingestion_account);
        assert_eq!(policy.ingestion_account, "tenant-a");

        assert!(proxy
            .preflight_report()
            .degraded_reasons
            .iter()
            .any(|reason| reason == "account_rejections"));

        // The proxy's own account still reaches routing, where the unreachable
        // metaserver -- not admission -- is what fails it.
        let allowed = proxy.table_execute(ProxyTableExecuteRequest {
            namespace: "tenant-a".to_string(),
            table_name: "t".to_string(),
            command: read_command(),
        });
        assert_ne!(allowed.status.code, "proxy_account_denied");
        assert_eq!(proxy.policy_report().account_rejections, 2);
    }

    #[test]
    fn proxy_account_enforcement_without_an_account_fails_closed() {
        let proxy = scoped_proxy(ProxyOptions {
            enforce_ingestion_account: true,
            ..ProxyOptions::default()
        });

        let response = proxy.table_execute(ProxyTableExecuteRequest {
            namespace: "anything".to_string(),
            table_name: "t".to_string(),
            command: read_command(),
        });
        assert_eq!(response.status.code, "proxy_account_not_configured");
        assert_eq!(proxy.policy_report().account_rejections, 1);
    }

    #[test]
    fn proxy_account_scope_is_off_by_default() {
        let proxy = scoped_proxy(ProxyOptions {
            ingestion_account: "tenant-a".to_string(),
            ..ProxyOptions::default()
        });
        let response = proxy.table_execute(ProxyTableExecuteRequest {
            namespace: "tenant-b".to_string(),
            table_name: "t".to_string(),
            command: read_command(),
        });
        assert_ne!(response.status.code, "proxy_account_denied");
        assert_eq!(proxy.policy_report().account_rejections, 0);
    }

    #[test]
    fn proxy_inflight_quota_rejects_only_while_slots_are_held() {
        let proxy = scoped_proxy(ProxyOptions {
            max_inflight_requests: 1,
            ..ProxyOptions::default()
        });

        let held = proxy
            .admit(None, std::slice::from_ref(&read_command()))
            .expect("first request is admitted");
        assert_eq!(proxy.inflight_snapshot(), (1, 0));

        let status = proxy
            .admit(None, std::slice::from_ref(&read_command()))
            .expect_err("the single slot is taken");
        assert_eq!(status.code, "proxy_inflight_quota_exceeded");
        assert_eq!(proxy.inflight_snapshot(), (1, 0));

        drop(held);
        assert_eq!(proxy.inflight_snapshot(), (0, 0));
        proxy
            .admit(None, std::slice::from_ref(&read_command()))
            .expect("the slot is released with the guard");

        let policy = proxy.policy_report();
        assert_eq!(policy.inflight_rejections, 1);
        assert_eq!(policy.admission_rejections, 1);
        assert_eq!(policy.max_inflight_requests, 1);
    }

    #[test]
    fn proxy_write_quota_leaves_read_capacity_free() {
        let proxy = scoped_proxy(ProxyOptions {
            max_inflight_write_requests: 1,
            ..ProxyOptions::default()
        });

        let held = proxy
            .admit(None, std::slice::from_ref(&write_command()))
            .expect("first write is admitted");
        assert_eq!(proxy.inflight_snapshot(), (1, 1));

        let status = proxy
            .admit(None, std::slice::from_ref(&write_command()))
            .expect_err("the single write slot is taken");
        assert_eq!(status.code, "proxy_write_inflight_quota_exceeded");
        // The rejected write must not leak a slot in the shared total.
        assert_eq!(proxy.inflight_snapshot(), (1, 1));

        let reader = proxy
            .admit(None, std::slice::from_ref(&read_command()))
            .expect("reads are unaffected by the write quota");
        assert_eq!(proxy.inflight_snapshot(), (2, 1));

        drop(reader);
        drop(held);
        assert_eq!(proxy.inflight_snapshot(), (0, 0));
        assert_eq!(proxy.policy_report().inflight_rejections, 1);
    }

    #[test]
    fn proxy_unlimited_quotas_never_reject() {
        let proxy = scoped_proxy(ProxyOptions::default());
        let mut guards = Vec::new();
        for _ in 0..64 {
            guards.push(
                proxy
                    .admit(None, std::slice::from_ref(&write_command()))
                    .expect("the default configuration is unlimited"),
            );
        }
        assert_eq!(proxy.inflight_snapshot(), (64, 64));
        drop(guards);
        assert_eq!(proxy.inflight_snapshot(), (0, 0));
        assert_eq!(proxy.policy_report().inflight_rejections, 0);
    }

    #[test]
    fn proxy_pin_primary_reads_defaults_on_and_is_reported() {
        let proxy = scoped_proxy(ProxyOptions::default());
        let policy = proxy.policy_report();
        assert!(policy.pin_primary_reads);

        let follower_reads = scoped_proxy(ProxyOptions {
            pin_primary_reads: false,
            ..ProxyOptions::default()
        });
        assert!(!follower_reads.policy_report().pin_primary_reads);
        assert_ne!(
            proxy_config_version(&proxy.options()),
            proxy_config_version(&follower_reads.options()),
            "the read-routing policy has to move config_version so a rollout can confirm it"
        );
    }

    #[test]
    fn proxy_metrics_expose_admission_quotas() {
        let proxy = scoped_proxy(ProxyOptions {
            max_inflight_requests: 8,
            max_inflight_write_requests: 2,
            enforce_ingestion_account: true,
            ingestion_account: "tenant-a".to_string(),
            ..ProxyOptions::default()
        });
        let _ = proxy.table_execute(ProxyTableExecuteRequest {
            namespace: "tenant-b".to_string(),
            table_name: "t".to_string(),
            command: read_command(),
        });

        let metrics = proxy.prometheus_metrics();
        assert!(metrics
            .contains("temporalstore_proxy_requests_total{kind=\"account_rejection\"} 1"));
        assert!(metrics.contains("temporalstore_proxy_inflight_limit{kind=\"total\"} 8"));
        assert!(metrics.contains("temporalstore_proxy_inflight_limit{kind=\"write\"} 2"));
        assert!(metrics.contains("temporalstore_proxy_inflight_requests{kind=\"total\"} 0"));
        assert!(metrics.contains("temporalstore_proxy_account_enforcement 1"));
        assert!(metrics.contains("temporalstore_proxy_pin_primary_reads 1"));
    }

    use serde_json::json;

    fn context_scope(account: &str, tenant: &str) -> context::ProxyContextScope {
        context::ProxyContextScope {
            tenant_id: tenant.to_string(),
            account_id: account.to_string(),
            user_id: String::new(),
            session_id: "s1".to_string(),
        }
    }

    fn context_ingest_body(account: &str, tenant: &str) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "scope": {"account_id": account, "tenant_id": tenant, "session_id": "s1"},
            "messages": [{"role": "user", "content": "hello"}],
        }))
        .unwrap()
    }

    fn post(proxy: &ProxyService, path: &str, body: Vec<u8>) -> (u16, Vec<u8>) {
        proxy.handle(HttpRequest {
            method: "POST".to_string(),
            path: path.to_string(),
            body,
        })
    }

    #[test]
    fn context_routes_honour_account_scope() {
        let proxy = scoped_proxy(ProxyOptions {
            ingestion_account: "tenant-a".to_string(),
            enforce_ingestion_account: true,
            ..ProxyOptions::default()
        });

        let (code, body) = post(
            &proxy,
            "/context/ingest",
            context_ingest_body("tenant-b", "t"),
        );
        assert_eq!(code, 403);
        assert!(String::from_utf8_lossy(&body).contains("proxy_account_denied"));

        let (code, _) = post(
            &proxy,
            "/context/retrieve",
            serde_json::to_vec(&json!({
                "scope": {"account_id": "tenant-b", "tenant_id": "t", "session_id": "s1"},
                "query": "q",
            }))
            .unwrap(),
        );
        assert_eq!(code, 403);
        assert_eq!(proxy.policy_report().account_rejections, 2);
    }

    #[test]
    fn context_routes_honour_drain_and_write_disable() {
        let drained = scoped_proxy(ProxyOptions {
            serving_mode: ProxyServingMode::NotServing,
            ..ProxyOptions::default()
        });
        let (code, body) = post(
            &drained,
            "/context/ingest",
            context_ingest_body("acct", "t"),
        );
        assert_eq!(code, 503);
        assert!(String::from_utf8_lossy(&body).contains("proxy_not_serving"));

        let readonly = scoped_proxy(ProxyOptions {
            serving_mode: ProxyServingMode::Readonly,
            ..ProxyOptions::default()
        });
        let (code, body) = post(
            &readonly,
            "/context/extract",
            context_ingest_body("acct", "t"),
        );
        assert_eq!(code, 503);
        assert!(String::from_utf8_lossy(&body).contains("proxy_write_disabled"));

        // A read-only proxy still serves retrieval; it fails on the unreachable
        // shard rather than on admission.
        let (_, body) = post(
            &readonly,
            "/context/retrieve",
            serde_json::to_vec(&json!({
                "scope": {"account_id": "acct", "tenant_id": "t", "session_id": "s1"},
                "query": "q",
            }))
            .unwrap(),
        );
        let body = String::from_utf8_lossy(&body).to_string();
        assert!(!body.contains("proxy_write_disabled"), "{body}");
        assert!(!body.contains("proxy_not_serving"), "{body}");
    }

    #[test]
    fn the_metrics_report_lists_exactly_what_the_endpoint_emits() {
        // The list in this report was written by hand and had fallen eight families behind
        // the endpoint -- including every admission metric, so a dashboard built from the
        // report showed no in-flight quota, no account enforcement and no read pinning. It is
        // now read back off the rendered output, and this pins that it stays that way.
        let proxy = scoped_proxy(ProxyOptions::default());
        let emitted = proxy_metric_families_from(&proxy.prometheus_metrics());
        let report = proxy.metrics_parity_report();

        assert_eq!(
            report.rust_prometheus_families, emitted,
            "the report must list what the endpoint actually publishes"
        );
        assert!(
            emitted.len() >= 17,
            "expected the full family set, saw {}: {emitted:?}",
            emitted.len()
        );
        for family in [
            "temporalstore_proxy_inflight_requests",
            "temporalstore_proxy_inflight_limit",
            "temporalstore_proxy_account_enforcement",
            "temporalstore_proxy_pin_primary_reads",
        ] {
            assert!(
                report.rust_prometheus_families.iter().any(|f| f == family),
                "{family} is published but was missing from the report"
            );
        }

        // The other half: a mapping that names a family nothing emits is a dashboard wired to
        // a metric that will never arrive. Derived lists cannot catch that -- mappings carry
        // external panel names and stay hand-written -- so check them against reality here.
        for mapping in &report.mappings {
            assert!(
                emitted.contains(&mapping.rust_prometheus_family),
                "mapping for panel {:?} names {:?}, which the endpoint does not emit",
                mapping.grafana_panel,
                mapping.rust_prometheus_family
            );
        }
    }

    #[test]
    fn the_ports_report_distinguishes_what_is_bound_from_what_is_advertised() {
        // Behind NAT or a container port mapping these are deliberately different, and that
        // is exactly when someone asks the proxy what it is listening on. The report used to
        // derive both numbers from the advertised address and answer the advertised one.
        let mapped = scoped_proxy(ProxyOptions {
            proxy_addr: "10.1.2.3:9000".to_string(),
            listen_addr: "0.0.0.0:17000".to_string(),
            ..ProxyOptions::default()
        });
        let ports = mapped.ports_report();
        assert_eq!(ports.listen_addr, "0.0.0.0:17000", "what the socket is bound to");
        assert_eq!(ports.listen_port, 17_000);
        assert_eq!(ports.announce_addr, "10.1.2.3:9000", "what other nodes are told to use");
        assert_eq!(ports.announce_port, 9_000);

        // The ordinary case: nothing is mapped, so both answers are the same and the report
        // reads exactly as it did before.
        let plain = scoped_proxy(ProxyOptions {
            proxy_addr: "127.0.0.1:17123".to_string(),
            ..ProxyOptions::default()
        });
        let ports = plain.ports_report();
        assert_eq!(ports.listen_addr, "127.0.0.1:17123");
        assert_eq!(ports.announce_addr, "127.0.0.1:17123");
        assert_eq!(ports.listen_port, 17_123);
        assert_eq!(ports.announce_port, 17_123);
    }

    #[test]
    fn a_metaserver_that_keeps_saying_not_found_is_not_hammered() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        // not_found means "I do not know this proxy", and registering again is the right
        // answer -- but it was being retried on every heartbeat. A metaserver stuck on
        // not_found then takes a registration AND a second heartbeat from every proxy, every
        // interval, which is the heaviest load at the moment it is least able to serve it.
        let registers = std::sync::Arc::new(AtomicUsize::new(0));
        let heartbeats = std::sync::Arc::new(AtomicUsize::new(0));
        let addr = test_addr(18_394);
        let r = registers.clone();
        let h = heartbeats.clone();
        let addr_for_thread = addr.clone();
        std::thread::spawn(move || {
            serve(&addr_for_thread, move |request| {
                match request.path.as_str() {
                    "/proxies/heartbeat" => {
                        h.fetch_add(1, Ordering::SeqCst);
                        crate::http::json_response(
                            200,
                            &ProxyHeartbeatResponse {
                                status: Status::error("not_found", "unknown proxy"),
                                config_changed: false,
                                namespace: String::new(),
                                config_version: 0,
                                serving_mode: "serving".to_string(),
                                drop_percent: 0,
                            },
                        )
                    }
                    "/proxies/register" => {
                        r.fetch_add(1, Ordering::SeqCst);
                        crate::http::json_response(
                            200,
                            &AckResponse {
                                status: Status::error("still_unknown", "no"),
                            },
                        )
                    }
                    _ => crate::http::json_response(404, &Status::error("not_found", "no route")),
                }
            })
            .unwrap();
        });
        wait_for_http(&addr);

        let proxy = ProxyService::new(ProxyOptions {
            meta_addr: addr.clone(),
            ..ProxyOptions::default()
        });
        for _ in 0..6 {
            let _ = proxy.heartbeat_to_meta();
        }
        assert_eq!(
            registers.load(Ordering::SeqCst),
            1,
            "six heartbeats against a not_found metaserver should attempt registration once,              not once each"
        );
        assert_eq!(heartbeats.load(Ordering::SeqCst), 6, "heartbeats themselves continue");
        assert_eq!(
            proxy.info().stats.auto_register_throttled, 5,
            "the attempts not made should be counted, not silently dropped"
        );

        // Zero restores the older behaviour exactly, so the escape hatch cannot rot.
        registers.store(0, Ordering::SeqCst);
        let eager = ProxyService::new(ProxyOptions {
            meta_addr: addr,
            auto_register_min_interval_ms: 0,
            ..ProxyOptions::default()
        });
        for _ in 0..4 {
            let _ = eager.heartbeat_to_meta();
        }
        assert_eq!(
            registers.load(Ordering::SeqCst),
            4,
            "with the interval at zero every heartbeat should try to register"
        );
    }

    #[test]
    fn every_proxy_counter_is_exposed_on_the_metrics_endpoint() {
        // A counter nobody can read is a counter nobody acts on. topology_checks_skipped was
        // added and never exposed, so the round-trips it was counting stayed invisible.
        //
        // The destructuring below has NO `..` rest pattern on purpose: adding a field to
        // ProxyStats stops this test compiling until whoever added it says where it surfaces.
        // That is the only guard that survives someone forgetting -- a hand-kept list of
        // metric names would drift exactly the way this one already did.
        let ProxyStats {
            execute_requests,
            batch_execute_requests,
            bad_requests,
            context_ingest_requests,
            context_extract_requests,
            context_retrieve_requests,
            admission_rejections,
            account_rejections,
            inflight_rejections,
            heartbeat_total,
            heartbeat_slow_total,
            auto_register_total,
            auto_register_throttled,
            writes_of_unknown_outcome,
            route_cache_hits,
            route_cache_misses,
            route_refreshes,
            topology_checks_skipped,
            backend_errors,
            continuous_backend_failures,
            metaserver_errors,
        } = ProxyStats::default();

        // Field -> the label it is published under. Values are only here so the destructured
        // bindings are used; what is asserted is that each label reaches the endpoint.
        let published: [(&str, u64); 21] = [
            ("kind=\"execute\"", execute_requests),
            ("kind=\"batch_execute\"", batch_execute_requests),
            ("kind=\"bad_request\"", bad_requests),
            ("kind=\"context_ingest\"", context_ingest_requests),
            ("kind=\"context_extract\"", context_extract_requests),
            ("kind=\"context_retrieve\"", context_retrieve_requests),
            ("kind=\"admission_rejection\"", admission_rejections),
            ("kind=\"account_rejection\"", account_rejections),
            ("kind=\"inflight_rejection\"", inflight_rejections),
            ("kind=\"heartbeat\"", heartbeat_total),
            ("kind=\"heartbeat_slow\"", heartbeat_slow_total),
            ("kind=\"auto_register\"", auto_register_total),
            ("kind=\"auto_register_throttled\"", auto_register_throttled),
            ("kind=\"write_of_unknown_outcome\"", writes_of_unknown_outcome),
            ("kind=\"hit\"", route_cache_hits),
            ("kind=\"miss\"", route_cache_misses),
            ("kind=\"refresh\"", route_refreshes),
            ("kind=\"topology_check_skipped\"", topology_checks_skipped),
            ("kind=\"backend_error\"", backend_errors),
            ("kind=\"continuous_backend_failure\"", continuous_backend_failures),
            ("kind=\"metaserver_error\"", metaserver_errors),
        ];

        let proxy = scoped_proxy(ProxyOptions::default());
        let metrics = proxy.prometheus_metrics();
        let missing: Vec<&str> = published
            .iter()
            .map(|(label, _)| *label)
            .filter(|label| !metrics.contains(label))
            .collect();
        assert!(
            missing.is_empty(),
            "these counters never reach /metrics: {missing:?}"
        );
    }

    #[test]
    fn a_partial_config_push_keeps_every_field_it_does_not_mention() {
        let proxy = scoped_proxy(ProxyOptions {
            serving_mode: ProxyServingMode::NotServing,
            location: "zone-a".to_string(),
            heartbeat_timeout_ms: 9_000,
            max_inflight_requests: 7,
            ingestion_account: "tenant-a".to_string(),
            enforce_ingestion_account: true,
            ..ProxyOptions::default()
        });

        // A push that mentions one field. Everything else must survive it. The dangerous one
        // is serving_mode: this proxy has been DRAINED, and taking the default would put it
        // back into service without saying so.
        // meta_addr is supplied here on purpose: without it the body would simply fail to
        // parse, and this test is about the fields that parse FINE and quietly reset.
        let (code, _) = proxy.handle(HttpRequest {
            method: "POST".to_string(),
            path: "/proxy/config".to_string(),
            body: br#"{"meta_addr": "127.0.0.1:1", "drop_percent": 5}"#.to_vec(),
        });
        assert_eq!(code, 200);

        let after = proxy.config_snapshot();
        assert_eq!(after.drop_percent, 5, "the field that WAS supplied must apply");
        assert_eq!(
            after.serving_mode,
            ProxyServingMode::NotServing,
            "a drained proxy must not silently start serving again"
        );
        assert_eq!(after.location, "zone-a", "locality decides which replica reads go to");
        assert_eq!(after.heartbeat_timeout_ms, 9_000);
        assert_eq!(after.max_inflight_requests, 7);
        assert_eq!(after.ingestion_account, "tenant-a");
        assert!(after.enforce_ingestion_account);
        assert_eq!(after.meta_addr, "127.0.0.1:1");


        // Supplying a field still overrides it -- carrying forward must not become ignoring.
        let (code, _) = proxy.handle(HttpRequest {
            method: "POST".to_string(),
            path: "/proxy/config".to_string(),
            body: br#"{"serving_mode": "serving", "location": "zone-b"}"#.to_vec(),
        });
        assert_eq!(code, 200);
        let after = proxy.config_snapshot();
        assert_eq!(after.serving_mode, ProxyServingMode::Serving);
        assert_eq!(after.location, "zone-b");
        assert_eq!(after.drop_percent, 5, "the earlier push is still in effect");

        // Malformed bodies still fail rather than merging into nonsense.
        let (code, _) = proxy.handle(HttpRequest {
            method: "POST".to_string(),
            path: "/proxy/config".to_string(),
            body: b"not json".to_vec(),
        });
        assert_eq!(code, 400);

        // And a field with no default of its own now survives being left out, so a partial
        // push no longer has to restate the whole document just to be accepted.
        let (code, _) = proxy.handle(HttpRequest {
            method: "POST".to_string(),
            path: "/proxy/config".to_string(),
            body: br#"{"drop_percent": 6}"#.to_vec(),
        });
        assert_eq!(code, 200, "omitting a required field must carry it forward, not 400");
        let after = proxy.config_snapshot();
        assert_eq!(after.meta_addr, "127.0.0.1:1");
        assert_eq!(after.drop_percent, 6);
        assert_eq!(after.location, "zone-b", "and the rest still carries");
    }

    #[test]
    fn shard_lookup_and_topology_refresh_are_bounded_by_the_inflight_quota() {
        // Both reach the metaserver, and neither took a slot. max_inflight_requests capped
        // execute and the context routes while an unbounded number of these went straight
        // through -- so a client stampede was amplified onto the metaserver by the component
        // whose job is to shield it.
        let proxy = scoped_proxy(ProxyOptions {
            max_inflight_requests: 1,
            ..ProxyOptions::default()
        });
        let held = proxy
            .admit_context(&context_scope("acct", "t"), false)
            .expect("first request is admitted");
        assert_eq!(proxy.inflight_snapshot(), (1, 0));

        let (code, body) = proxy.handle(HttpRequest {
            method: "GET".to_string(),
            path: "/shards/1".to_string(),
            body: Vec::new(),
        });
        assert_eq!(code, 429, "a shard lookup must take the quota");
        assert!(String::from_utf8_lossy(&body).contains("proxy_inflight_quota_exceeded"));

        let (code, body) = proxy.handle(HttpRequest {
            method: "POST".to_string(),
            path: "/proxy/topology/refresh".to_string(),
            body: Vec::new(),
        });
        assert_eq!(code, 429, "a topology refresh must take the quota");
        assert!(String::from_utf8_lossy(&body).contains("proxy_inflight_quota_exceeded"));

        drop(held);
        assert_eq!(proxy.inflight_snapshot(), (0, 0));

        // Releasing matters more than acquiring: a slot leaked on the way out would wedge the
        // proxy shut, which is worse than the unbounded route this replaces. The metaserver
        // here is unreachable, so these take the error path -- exactly where a leak would hide.
        for _ in 0..3 {
            let _ = proxy.handle(HttpRequest {
                method: "GET".to_string(),
                path: "/shards/1".to_string(),
                body: Vec::new(),
            });
            let _ = proxy.handle(HttpRequest {
                method: "POST".to_string(),
                path: "/proxy/topology/refresh".to_string(),
                body: Vec::new(),
            });
        }
        assert_eq!(
            proxy.inflight_snapshot(),
            (0, 0),
            "every slot must come back, including on the failure path"
        );
    }

    #[test]
    fn draining_stops_shard_lookups_but_still_allows_a_topology_refresh() {
        // Drain means stop answering clients, and a shard lookup is a client asking where to
        // go. A topology refresh is not client traffic -- it is how an operator makes the
        // proxy notice a change, and a drained proxy is exactly when that is wanted, so it
        // stays available on purpose.
        let drained = scoped_proxy(ProxyOptions {
            serving_mode: ProxyServingMode::NotServing,
            ..ProxyOptions::default()
        });

        let (code, body) = drained.handle(HttpRequest {
            method: "GET".to_string(),
            path: "/shards/1".to_string(),
            body: Vec::new(),
        });
        assert_eq!(code, 503);
        assert!(String::from_utf8_lossy(&body).contains("proxy_not_serving"));

        let (code, body) = drained.handle(HttpRequest {
            method: "POST".to_string(),
            path: "/proxy/topology/refresh".to_string(),
            body: Vec::new(),
        });
        assert_ne!(code, 503, "an operator must still be able to refresh a drained proxy");
        assert!(
            !String::from_utf8_lossy(&body).contains("proxy_not_serving"),
            "topology refresh must not be gated on serving mode"
        );
    }

    #[test]
    fn context_routes_share_the_inflight_quota() {
        let proxy = scoped_proxy(ProxyOptions {
            max_inflight_requests: 1,
            ..ProxyOptions::default()
        });
        let held = proxy
            .admit_context(&context_scope("acct", "t"), true)
            .expect("first context request is admitted");
        assert_eq!(proxy.inflight_snapshot(), (1, 1));

        let (code, body) = post(&proxy, "/context/ingest", context_ingest_body("acct", "t"));
        assert_eq!(code, 429);
        assert!(String::from_utf8_lossy(&body).contains("proxy_inflight_quota_exceeded"));

        drop(held);
        assert_eq!(proxy.inflight_snapshot(), (0, 0));
        assert_eq!(proxy.policy_report().inflight_rejections, 1);
    }

    #[test]
    fn config_push_without_admission_fields_keeps_account_enforcement() {
        let proxy = scoped_proxy(ProxyOptions {
            ingestion_account: "tenant-a".to_string(),
            enforce_ingestion_account: true,
            max_inflight_requests: 4,
            pin_primary_reads: false,
            ..ProxyOptions::default()
        });

        // A config push shaped before these options existed.
        let (code, _) = post(
            &proxy,
            "/proxy/config",
            serde_json::to_vec(&json!({
                "meta_addr": "127.0.0.1:1",
                "proxy_addr": "127.0.0.1:17000",
                "route_cache_ttl_ms": 1000,
                "connect_timeout_ms": 200,
                "io_timeout_ms": 200,
                "max_retries": 0,
                "refresh_route_on_backend_error": true,
                "backend_continuous_failed_time_ms": 10000,
                "drop_percent": 5,
            }))
            .unwrap(),
        );
        assert_eq!(code, 200);

        let policy = proxy.policy_report();
        assert!(policy.enforce_ingestion_account, "enforcement must survive");
        assert_eq!(policy.ingestion_account, "tenant-a");
        assert_eq!(policy.max_inflight_requests, 4);
        assert!(!policy.pin_primary_reads);
        assert_eq!(policy.drop_percent, 5, "supplied fields still apply");

        // An explicit key does change it.
        let (code, _) = post(
            &proxy,
            "/proxy/config",
            serde_json::to_vec(&json!({
                "meta_addr": "127.0.0.1:1",
                "route_cache_ttl_ms": 1000,
                "connect_timeout_ms": 200,
                "io_timeout_ms": 200,
                "max_retries": 0,
                "refresh_route_on_backend_error": true,
                "backend_continuous_failed_time_ms": 10000,
                "enforce_ingestion_account": false,
            }))
            .unwrap(),
        );
        assert_eq!(code, 200);
        assert!(!proxy.policy_report().enforce_ingestion_account);
    }

    #[test]
    fn proxy_control_plane_timeout_is_not_the_command_timeout() {
        // A metaserver that pauses briefly must not cost a healthy proxy its liveness.
        // The command path is sized for a data hop (200ms); heartbeats get their own budget.
        let options = ProxyOptions {
            meta_addr: "127.0.0.1:1".to_string(),
            ..ProxyOptions::default()
        };
        assert_eq!(options.io_timeout_ms, 200);
        assert_eq!(options.heartbeat_timeout_ms, 5_000);
        assert_eq!(options.http_options().io_timeout_ms, 200);
        assert_eq!(options.control_http_options().io_timeout_ms, 5_000);

        // A deployment that widens the command timeout past the heartbeat budget keeps the
        // wider one -- the control plane is never given LESS room than the data path.
        let slow_backend = ProxyOptions {
            io_timeout_ms: 9_000,
            ..options.clone()
        };
        assert_eq!(slow_backend.control_http_options().io_timeout_ms, 9_000);

        // It is part of the config hash, so a rollout can confirm the change landed.
        assert_ne!(
            proxy_config_version(&options),
            proxy_config_version(&ProxyOptions {
                heartbeat_timeout_ms: 30_000,
                ..options
            })
        );
    }

    #[test]
    fn proxy_heartbeat_reports_boot_time_so_a_restart_is_visible() {
        let proxy = ProxyService::new(ProxyOptions {
            meta_addr: "127.0.0.1:1".to_string(),
            proxy_addr: "127.0.0.1:17000".to_string(),
            ..ProxyOptions::default()
        });
        let info = proxy.info();
        assert!(info.boot_time_ms > 0, "proxy knows when it started");

        // A restarted process reports a different boot time on the same address, which is the
        // only signal the metaserver gets: the address never changes and the heartbeats never
        // stop, so without this an in-place reboot is invisible.
        let restarted = ProxyService::new(ProxyOptions {
            meta_addr: "127.0.0.1:1".to_string(),
            proxy_addr: "127.0.0.1:17000".to_string(),
            ..ProxyOptions::default()
        });
        assert_eq!(restarted.info().meta_addr, info.meta_addr);
        assert!(restarted.info().boot_time_ms >= info.boot_time_ms);
    }

    #[test]
    fn proxy_forgets_config_when_the_metaserver_rejects_it_but_not_when_it_is_unreachable() {
        // Explicit rejection: the metaserver answered and said no. The proxy must stop acting
        // on a grant that has been withdrawn.
        let rejected = ProxyService::new(ProxyOptions {
            meta_addr: "127.0.0.1:1".to_string(),
            namespace: "granted-ns".to_string(),
            config_version: 7,
            ..ProxyOptions::default()
        });
        rejected.clear_config_authority();
        let after = rejected.config_snapshot();
        assert!(after.namespace.is_empty());
        assert_eq!(after.config_version, 0);

        // Clearing twice is a no-op, so a proxy that is being rejected every few milliseconds
        // does not churn its config version on every beat.
        let before_version = proxy_config_version(&rejected.config_snapshot());
        rejected.clear_config_authority();
        assert_eq!(
            proxy_config_version(&rejected.config_snapshot()),
            before_version
        );

        // Transport failure: an unreachable metaserver must NOT cost the proxy its config.
        // heartbeat_to_meta against a dead address takes the Err branch.
        let unreachable = ProxyService::new(ProxyOptions {
            meta_addr: "127.0.0.1:1".to_string(),
            namespace: "granted-ns".to_string(),
            config_version: 7,
            ..ProxyOptions::default()
        });
        let response = unreachable.heartbeat_to_meta();
        assert!(!response.status.ok);
        let kept = unreachable.config_snapshot();
        assert_eq!(kept.namespace, "granted-ns");
        assert_eq!(kept.config_version, 7);
    }
    #[test]
    fn open_table_is_bounded_by_the_inflight_quota() {
        let proxy = scoped_proxy(ProxyOptions {
            max_inflight_requests: 1,
            ..ProxyOptions::default()
        });

        // Opening a table is a metaserver round-trip, so it must consume a slot. Holding the
        // only one means the next open is refused rather than making an unbounded number of
        // concurrent metaserver calls.
        let held = proxy
            .admit(None, std::slice::from_ref(&read_command()))
            .expect("first request is admitted");
        assert_eq!(proxy.inflight_snapshot(), (1, 0));

        let refused = proxy.open_table(ProxyOpenTableRequest {
            namespace: "ns".to_string(),
            table_name: "t".to_string(),
            pin_primary: None,
            replica_read_policy: None,
        });
        assert_eq!(refused.status.code, "proxy_inflight_quota_exceeded");
        assert!(refused.options.is_none());
        assert_eq!(proxy.policy_report().inflight_rejections, 1);

        // The slot is released with the guard, and open_table releases its own slot too --
        // otherwise a single open would permanently consume capacity.
        drop(held);
        assert_eq!(proxy.inflight_snapshot(), (0, 0));
        let attempted = proxy.open_table(ProxyOpenTableRequest {
            namespace: "ns".to_string(),
            table_name: "t".to_string(),
            pin_primary: None,
            replica_read_policy: None,
        });
        assert_ne!(attempted.status.code, "proxy_inflight_quota_exceeded");
        assert_eq!(
            proxy.inflight_snapshot(),
            (0, 0),
            "open_table must not leak its slot"
        );
    }

    #[test]
    fn open_table_is_refused_while_the_proxy_is_drained() {
        let drained = scoped_proxy(ProxyOptions {
            serving_mode: ProxyServingMode::NotServing,
            ..ProxyOptions::default()
        });
        let response = drained.open_table(ProxyOpenTableRequest {
            namespace: "ns".to_string(),
            table_name: "t".to_string(),
            pin_primary: None,
            replica_read_policy: None,
        });
        assert_eq!(response.status.code, "proxy_not_serving");

        // A read-only proxy still opens tables: reads are still served, and open_table does
        // not write anything.
        let readonly = scoped_proxy(ProxyOptions {
            serving_mode: ProxyServingMode::Readonly,
            ..ProxyOptions::default()
        });
        let response = readonly.open_table(ProxyOpenTableRequest {
            namespace: "ns".to_string(),
            table_name: "t".to_string(),
            pin_primary: None,
            replica_read_policy: None,
        });
        assert_ne!(response.status.code, "proxy_not_serving");
        assert_ne!(response.status.code, "proxy_write_disabled");
    }
    #[test]
    fn context_routes_are_counted_so_gateway_traffic_is_visible() {
        let proxy = scoped_proxy(ProxyOptions::default());

        // These three routes are the only ones the context gateway calls. Without their own
        // counters a proxy saturated by gateway traffic reports execute/batch_execute at zero
        // and reads as idle.
        let _ = post(&proxy, "/context/ingest", context_ingest_body("acct", "t"));
        let _ = post(&proxy, "/context/ingest", context_ingest_body("acct", "t"));
        let _ = post(&proxy, "/context/extract", context_ingest_body("acct", "t"));
        let _ = post(
            &proxy,
            "/context/retrieve",
            serde_json::to_vec(&json!({
                "scope": {"account_id": "acct", "tenant_id": "t", "session_id": "s1"},
                "query": "q",
            }))
            .unwrap(),
        );

        let stats = proxy.preflight_report().stats;
        assert_eq!(stats.context_ingest_requests, 2);
        assert_eq!(stats.context_extract_requests, 1);
        assert_eq!(stats.context_retrieve_requests, 1);
        // Counted separately from command traffic, not folded into it.
        assert_eq!(stats.execute_requests, 0);

        let metrics = proxy.prometheus_metrics();
        assert!(metrics.contains("temporalstore_proxy_requests_total{kind=\"context_ingest\"} 2"));
        assert!(metrics.contains("temporalstore_proxy_requests_total{kind=\"context_extract\"} 1"));
        assert!(metrics.contains("temporalstore_proxy_requests_total{kind=\"context_retrieve\"} 1"));
    }

    #[test]
    fn context_requests_are_counted_even_when_refused() {
        // Offered load is what an operator needs to see. A proxy rejecting everything is
        // still being asked for work, and a counter that only advanced on success would make
        // a fully-drained proxy look idle rather than overloaded.
        let drained = scoped_proxy(ProxyOptions {
            serving_mode: ProxyServingMode::NotServing,
            ..ProxyOptions::default()
        });
        let (code, _) = post(&drained, "/context/ingest", context_ingest_body("acct", "t"));
        assert_eq!(code, 503);
        assert_eq!(drained.preflight_report().stats.context_ingest_requests, 1);
    }
    #[test]
    fn refresh_route_on_backend_error_flag_reaches_the_client() {
        // The proxy accepted this option, defaulted it, read it from the environment and
        // folded it into the config hash -- while never passing it to the client, which
        // refreshed unconditionally. Setting it to false changed nothing and reported
        // nothing. Pin that it is actually carried now.
        let on = ProxyOptions {
            meta_addr: "127.0.0.1:1".to_string(),
            ..ProxyOptions::default()
        };
        assert!(on.refresh_route_on_backend_error, "default stays on");

        let off = ProxyOptions {
            refresh_route_on_backend_error: false,
            ..on.clone()
        };
        // It already moved the config hash, which is how a rollout notices the change; the
        // point of this test is the half that was missing.
        assert_ne!(proxy_config_version(&on), proxy_config_version(&off));

        let proxy_on = ProxyService::new(on);
        let proxy_off = ProxyService::new(off);
        assert!(proxy_on.client_options_snapshot().refresh_route_on_backend_error);
        assert!(!proxy_off.client_options_snapshot().refresh_route_on_backend_error);
    }

    #[test]
    fn every_option_a_config_push_can_change_is_noticed() {
        // Whether a pushed config is applied is decided by comparing its version to the
        // running one. That version was hashed from a hand-listed subset of the options,
        // and the list had fallen behind: `context_shard_count`, `context_first_shard_id`,
        // `context_io_timeout_ms`, `service_registry_ttl_ms` and `listen_addr` were all
        // missing, so a push that changed only one of them produced the same version and
        // was answered "unchanged" -- telling the operator, in the report, that their
        // change was a no-op.
        //
        // Rather than list the fields again here -- which is the mistake being fixed --
        // this walks the serialized options and changes each one in turn, so a field
        // added later is covered without anyone editing this test.
        //
        // The proxy is built from exactly this baseline. An earlier version of this test
        // built it with a helper that overrode meta_addr, so every push differed in a
        // field that IS hashed, every assertion passed, and the test proved nothing. The
        // guard below is what catches that: if pushing the baseline back is treated as a
        // change, the comparison is not being exercised at all.
        let baseline = ProxyOptions {
            config_version: 0,
            meta_addr: "127.0.0.1:1".to_string(),
            ..ProxyOptions::default()
        };
        let unchanged =
            ProxyService::new(baseline.clone()).update_options_report(baseline.clone());
        assert!(
            !unchanged.applied,
            "pushing an identical config must be a no-op, or the assertions below pass \
             without testing anything (reason: {})",
            unchanged.reason
        );

        let serde_json::Value::Object(fields) = serde_json::to_value(&baseline).unwrap() else {
            panic!("options serialize as an object");
        };

        let mut checked = 0;
        for (name, value) in fields {
            // config_version IS the version, so changing it is not a content change.
            if name == "config_version" {
                continue;
            }
            let candidates: Vec<serde_json::Value> = match &value {
                serde_json::Value::Bool(flag) => vec![serde_json::Value::Bool(!flag)],
                serde_json::Value::Number(number) => {
                    vec![serde_json::Value::from(number.as_u64().unwrap_or(0) + 1)]
                }
                serde_json::Value::String(text) => vec![
                    serde_json::Value::from(format!("{text}-changed")),
                    // enum-valued fields reject arbitrary text; offer real alternatives
                    serde_json::Value::from("not_serving"),
                    serde_json::Value::from("draining"),
                ],
                other => panic!("teach this test how to change {name} (it is {other})"),
            };

            let changed = candidates
                .into_iter()
                .filter_map(|candidate| {
                    let serde_json::Value::Object(mut document) =
                        serde_json::to_value(&baseline).unwrap()
                    else {
                        return None;
                    };
                    document.insert(name.clone(), candidate);
                    serde_json::from_value::<ProxyOptions>(serde_json::Value::Object(document)).ok()
                })
                .find(|candidate| candidate != &baseline)
                .unwrap_or_else(|| {
                    panic!("could not produce a changed value for {name}; teach this test about it")
                });

            let proxy = ProxyService::new(baseline.clone());
            let report = proxy.update_options_report(changed);
            assert!(
                report.applied,
                "a config push changing {name} must be applied, not answered \"{}\"",
                report.reason
            );
            checked += 1;
        }
        assert!(
            checked >= 20,
            "expected to exercise every option; only walked {checked}"
        );
    }
    #[test]
    fn proxy_location_reaches_replica_selection() {
        // The proxy has always accepted a location, reported it to the metaserver and shown
        // it in its own status -- but never handed it to the client, whose `local_location`
        // is the fallback used when a table does not name a preferred_location, and which is
        // what actually picks a replica. So a proxy told it lives in zone-a read cross-zone.
        let located = ProxyService::new(ProxyOptions {
            meta_addr: "127.0.0.1:1".to_string(),
            location: "zone-a".to_string(),
            ..ProxyOptions::default()
        });
        assert_eq!(located.client_options_snapshot().local_location, "zone-a");

        // An unset location stays unset rather than inventing a preference.
        let unlocated = ProxyService::new(ProxyOptions {
            meta_addr: "127.0.0.1:1".to_string(),
            ..ProxyOptions::default()
        });
        assert!(unlocated.client_options_snapshot().local_location.is_empty());

        // A config push that changes the location has to reach the client too, otherwise the
        // proxy would report one location and route by another.
        let _ = located.update_options_report(ProxyOptions {
            meta_addr: "127.0.0.1:1".to_string(),
            location: "zone-b".to_string(),
            config_version: 99,
            ..ProxyOptions::default()
        });
        assert_eq!(located.client_options_snapshot().local_location, "zone-b");
    }
    #[test]
    fn context_shards_follow_the_cluster_unless_told_otherwise() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        // The count used to default to 1, so a proxy in front of a multi-shard cluster put
        // every tenant's context on one shard -- silently, with six or seven shards idle and
        // no error to notice. It now follows the cluster unless a value is configured.
        let stats_calls = std::sync::Arc::new(AtomicUsize::new(0));
        let addr = test_addr(18_404);
        let calls = stats_calls.clone();
        let addr_for_thread = addr.clone();
        std::thread::spawn(move || {
            serve(&addr_for_thread, move |request| match request.path.as_str() {
                "/shards" => {
                    calls.fetch_add(1, Ordering::SeqCst);
                    crate::http::json_response(
                        200,
                        &crate::meta::ListShardsResponse {
                            status: Status::ok(),
                            shards: (1..=8)
                                .map(|shard_id| crate::meta::ShardListEntry {
                                    shard_id,
                                    server_addr: "127.0.0.1:1".to_string(),
                                    namespace: "ns".to_string(),
                                    table_name: "tbl".to_string(),
                                    latest_snapshot: None,
                                })
                                .collect(),
                            next_after_shard_id: None,
                        },
                    )
                }
                "/proxies/heartbeat" => crate::http::json_response(
                    200,
                    &ProxyHeartbeatResponse {
                        status: Status::ok(),
                        config_changed: false,
                        namespace: String::new(),
                        config_version: 0,
                        serving_mode: "serving".to_string(),
                        drop_percent: 0,
                    },
                ),
                _ => crate::http::json_response(404, &Status::error("not_found", "no route")),
            })
            .unwrap();
        });
        wait_for_http(&addr);

        // Before the first heartbeat the cluster count is unknown, so it behaves as it always
        // did rather than guessing. Degrading to the old answer beats degrading to none.
        let following = ProxyService::new(ProxyOptions {
            meta_addr: addr.clone(),
            ..ProxyOptions::default()
        });
        assert_eq!(following.effective_context_shard_count(), 1);
        assert_eq!(
            following.context_shard_count_source(),
            "fallback_until_cluster_known"
        );

        // One heartbeat is enough to learn it.
        let _ = following.heartbeat_to_meta();
        assert_eq!(
            following.effective_context_shard_count(),
            8,
            "a proxy in front of an eight-shard cluster should spread context over eight"
        );
        assert_eq!(following.context_shard_count_source(), "cluster");
        assert_eq!(
            following.policy_report().context_shard_count,
            8,
            "the effective value has to be visible, since it used to be a silent 1"
        );

        // Tenants actually land on more than one shard now.
        let mut landed = std::collections::BTreeSet::new();
        for i in 0..64 {
            let scope = context::ProxyContextScope {
                tenant_id: format!("tenant-{i}"),
                account_id: "acct".to_string(),
                ..Default::default()
            };
            landed.insert(following.context_shard_id(context::context_tenant_hash(&scope)));
        }
        assert!(
            landed.len() > 1,
            "64 tenants over 8 shards should not all land on one, saw {landed:?}"
        );

        // A cluster whose shard ids are NOT a contiguous run from the first id cannot be
        // addressed by "first + hash % count" -- the arithmetic would name ids that do not
        // exist. Adopting the count there would be worse than the single-shard default it
        // replaces, so it is refused and the reason is reported.
        let gapped = test_addr(18_406);
        let gapped_for_thread = gapped.clone();
        std::thread::spawn(move || {
            serve(&gapped_for_thread, move |request| match request.path.as_str() {
                "/shards" => crate::http::json_response(
                    200,
                    &crate::meta::ListShardsResponse {
                        status: Status::ok(),
                        // 1-4 and 100-103: eight shards, not eight consecutive ids.
                        shards: (1..=4)
                            .chain(100..=103)
                            .map(|shard_id| crate::meta::ShardListEntry {
                                shard_id,
                                server_addr: "127.0.0.1:1".to_string(),
                                namespace: "ns".to_string(),
                                table_name: "tbl".to_string(),
                                latest_snapshot: None,
                            })
                            .collect(),
                        next_after_shard_id: None,
                    },
                ),
                "/proxies/heartbeat" => crate::http::json_response(
                    200,
                    &ProxyHeartbeatResponse {
                        status: Status::ok(),
                        config_changed: false,
                        namespace: String::new(),
                        config_version: 0,
                        serving_mode: "serving".to_string(),
                        drop_percent: 0,
                    },
                ),
                _ => crate::http::json_response(404, &Status::error("not_found", "no route")),
            })
            .unwrap();
        });
        wait_for_http(&gapped);

        let scattered = ProxyService::new(ProxyOptions {
            meta_addr: gapped,
            ..ProxyOptions::default()
        });
        let _ = scattered.heartbeat_to_meta();
        assert_eq!(
            scattered.effective_context_shard_count(),
            1,
            "a count that would address shards 5-8, which do not exist, must not be adopted"
        );
        assert_eq!(
            scattered.context_shard_count_source(),
            "fallback_cluster_shards_not_contiguous",
            "and the reason has to be visible, not inferred from failures"
        );

        // A configured value still wins, and needs no cluster lookup at all.
        let before = stats_calls.load(Ordering::SeqCst);
        let configured = ProxyService::new(ProxyOptions {
            meta_addr: addr.clone(),
            context_shard_count: 4,
            ..ProxyOptions::default()
        });
        let _ = configured.heartbeat_to_meta();
        assert_eq!(configured.effective_context_shard_count(), 4);
        assert_eq!(configured.context_shard_count_source(), "configured");
        assert_eq!(
            stats_calls.load(Ordering::SeqCst),
            before,
            "an explicitly configured count must not ask the cluster anything"
        );
    }

    #[test]
    fn a_drained_proxy_fails_its_readiness_probe() {
        // The runbook points operators at GET /readiness for exactly this, and it used to
        // answer with a build-wide capability report: the same bytes on every process, with a
        // hardcoded 200. So a proxy that had been drained -- refusing every request with
        // proxy_not_serving -- reported itself ready, and anything using the probe to decide
        // where to send traffic kept sending it. The drain control was defeated by the probe
        // meant to honour it.
        let drained = scoped_proxy(ProxyOptions {
            serving_mode: ProxyServingMode::NotServing,
            ..ProxyOptions::default()
        });
        let (status, body) = drained.handle(HttpRequest {
            method: "GET".to_string(),
            path: "/readiness".to_string(),
            body: Vec::new(),
        });
        assert_eq!(
            status, 503,
            "a drained proxy must fail its probe, or draining it does not take it \
             out of rotation"
        );
        let parsed = parse_json::<ProxyReadinessResponse>(&body).expect("readiness body parses");
        assert!(!parsed.serving);
        assert_eq!(parsed.serving_mode, ProxyServingMode::NotServing);

        // Serving answers 200, and so does read-only: it still serves reads, and failing its
        // probe would pull a proxy doing useful work out of rotation. The flags carry the
        // distinction for anyone who needs it.
        for (mode, expect_writes) in [
            (ProxyServingMode::Serving, true),
            (ProxyServingMode::Degraded, true),
            (ProxyServingMode::Readonly, false),
            (ProxyServingMode::WriteDisabled, false),
        ] {
            let proxy = scoped_proxy(ProxyOptions {
                serving_mode: mode,
                ..ProxyOptions::default()
            });
            let (status, body) = proxy.handle(HttpRequest {
                method: "GET".to_string(),
                path: "/readiness".to_string(),
                body: Vec::new(),
            });
            assert_eq!(status, 200, "{mode:?} serves reads and must pass its probe");
            let parsed =
                parse_json::<ProxyReadinessResponse>(&body).expect("readiness body parses");
            assert!(parsed.serving, "{mode:?} should report serving");
            assert_eq!(
                parsed.serving_writes, expect_writes,
                "{mode:?} should report serving_writes as {expect_writes}"
            );
            // The capability report is still carried, so anything already reading those fields
            // keeps working rather than being broken by this.
            assert!(
                !parsed.production.areas.is_empty(),
                "the capability report must still be included"
            );
        }
    }

    #[test]
    fn every_serving_mode_states_what_it_actually_refuses() {
        // Five modes, three behaviours. The pairs that mean the same thing are not obvious from
        // their names -- Degraded in particular reads like a restriction and is not one, it
        // accepts exactly what Serving accepts. This pins the real matrix so the documentation
        // beside the enum cannot drift away from it, and so that anyone narrowing one mode has
        // to decide what happens to the mode it is currently a synonym for.
        let cases = [
            (ProxyServingMode::Serving, true, true),
            (ProxyServingMode::Degraded, true, true),
            (ProxyServingMode::Readonly, true, false),
            (ProxyServingMode::WriteDisabled, true, false),
            (ProxyServingMode::NotServing, false, false),
        ];

        for (mode, reads_allowed, writes_allowed) in cases {
            let proxy = scoped_proxy(ProxyOptions {
                serving_mode: mode,
                ..ProxyOptions::default()
            });

            let read = proxy.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "k".to_string(),
                },
            });
            let read_refused = read.status.code == "proxy_not_serving"
                || read.status.code == "proxy_write_disabled";
            assert_eq!(
                !read_refused, reads_allowed,
                "{mode:?}: reads_allowed should be {reads_allowed}, got status {:?}",
                read.status.code
            );

            let write = proxy.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "k".to_string(),
                    value: b"v".to_vec(),
                },
            });
            let write_refused = write.status.code == "proxy_not_serving"
                || write.status.code == "proxy_write_disabled";
            assert_eq!(
                !write_refused, writes_allowed,
                "{mode:?}: writes_allowed should be {writes_allowed}, got status {:?}",
                write.status.code
            );

            // The report has to agree with what admission actually did, or an operator reading
            // the status surface is told one thing while traffic experiences another.
            let policy = proxy.policy_report();
            assert_eq!(
                policy.serving_writes, writes_allowed,
                "{mode:?}: the policy report disagrees with admission about writes"
            );
            assert_eq!(
                policy.serving_reads, reads_allowed,
                "{mode:?}: the policy report disagrees with admission about reads"
            );
        }
    }

    #[test]
    fn drop_percent_refuses_the_same_keys_every_time_including_writes() {
        // drop_percent reads like a load-shedding knob and is not one. The decision comes from
        // a hash of the routing key, so at 50 it is not "half the requests" -- it is "half the
        // keys, always", writes included. A caller that treats the refusal as transient and
        // retries is refused identically forever, which is why the message now says so.
        let proxy = scoped_proxy(ProxyOptions {
            drop_percent: 50,
            ..ProxyOptions::default()
        });

        // Find one refused key and one accepted key, then show each is stable.
        let mut refused = None;
        let mut accepted = None;
        for i in 0..200 {
            let key = format!("key-{i}");
            let dropped = proxy
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet { key: key.clone() },
                })
                .status
                .code
                == "proxy_traffic_dropped";
            if dropped && refused.is_none() {
                refused = Some(key);
            } else if !dropped && accepted.is_none() {
                accepted = Some(key);
            }
            if refused.is_some() && accepted.is_some() {
                break;
            }
        }
        let refused = refused.expect("at 50 percent some key is refused");
        let accepted = accepted.expect("at 50 percent some key is not refused");

        for attempt in 0..5 {
            let response = proxy.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: refused.clone(),
                },
            });
            assert_eq!(
                response.status.code, "proxy_traffic_dropped",
                "attempt {attempt} on a refused key must be refused again -- the decision \
                 is per key, not per request"
            );
            assert!(
                response.status.message.contains("will not succeed"),
                "the refusal must say retrying this key is pointless, got {:?}",
                response.status.message
            );
        }

        // Writes are refused on the same basis as reads. The refusal is explicit rather than a
        // silent discard, but it is a refusal, and it is permanent for that key.
        let write = proxy.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: refused.clone(),
                value: b"v".to_vec(),
            },
        });
        assert_eq!(
            write.status.code, "proxy_traffic_dropped",
            "drop_percent applies to writes, not only reads"
        );

        // And a key on the other side of the hash is never refused.
        for _ in 0..5 {
            assert_ne!(
                proxy
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringGet {
                            key: accepted.clone()
                        },
                    })
                    .status
                    .code,
                "proxy_traffic_dropped",
                "an accepted key must stay accepted"
            );
        }
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
    fn proxy_admin_aliases_expose_info_config_and_client_preflight() {
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
    fn proxy_operational_surface_aliases_cover_admin_config_heartbeat_status() {
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
                    .any(|entry| entry.native_surface == expected && entry.covered),
                "missing operational surface entry for {expected}: {report:?}"
            );
        }
    }

    #[test]
    fn proxy_service_aliases_delegate_to_client_execution_path() {
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
                    key: "native-proxy-alias".to_string(),
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
                        key: "native-proxy-alias".to_string(),
                    },
                    Command::HashSet {
                        key: "native-proxy-hash".to_string(),
                        field: "field".to_string(),
                        value: b"value".to_vec(),
                    },
                    Command::HashGet {
                        key: "native-proxy-hash".to_string(),
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
                    key: "native-proxy-command".to_string(),
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
                    key: "native-proxy-command".to_string(),
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
                    key: "native-proxy-h".to_string(),
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
                    key: "native-proxy-h".to_string(),
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
                    key: "native-proxy-h".to_string(),
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
                    key: "native-proxy-h".to_string(),
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
                    key: "native-proxy-hm".to_string(),
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
                    key: "native-proxy-hm".to_string(),
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
                    key: "native-proxy-hm".to_string(),
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
                    key: "native-proxy-hm".to_string(),
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
                    key: "native-proxy-set".to_string(),
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
                    key: "native-proxy-set".to_string(),
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
                    key: "native-proxy-command".to_string(),
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
                key: "native-proxy-command".to_string(),
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
                    key: "native-proxy-command".to_string(),
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
                    key: "native-proxy-command".to_string(),
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
                    key: "native-proxy-feature".to_string(),
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
                    key: "native-proxy-control_state".to_string(),
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
    fn proxy_config_update_noops_on_same_namespace_and_version_like_native() {
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
    fn proxy_heartbeat_applies_metaserver_config_version_like_native() {
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
            reason: crate::meta::FreezeReason::Unspecified,
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
                                state: crate::meta::MetaEntityState::Normal,
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

    #[test]
    fn a_timed_out_request_on_a_pooled_socket_is_not_resent() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        // The keep-alive pool reconnects and sends again when a pooled socket fails, which is
        // right when the server had reaped that socket -- nothing was served on it. It was
        // also doing it when the peer simply stopped answering, which sends the request a
        // second time even though the first may have been processed. One layer below the
        // routing guard, and it would undo that guard from underneath.
        let hits = std::sync::Arc::new(AtomicUsize::new(0));
        let addr = test_addr(18_392);
        let h = hits.clone();
        let addr_for_thread = addr.clone();
        std::thread::spawn(move || {
            serve(&addr_for_thread, move |_request| {
                // First exchange answers immediately, so the socket lands in the pool.
                // Everything after that accepts the request and goes quiet.
                if h.fetch_add(1, Ordering::SeqCst) > 0 {
                    std::thread::sleep(Duration::from_millis(700));
                }
                crate::http::json_response(200, &Status::ok())
            })
            .unwrap();
        });
        wait_for_http(&addr);

        let options = HttpRequestOptions {
            connect_timeout_ms: 200,
            io_timeout_ms: 120,
            max_retries: 0,
        };
        // Warm the pool: this must succeed and leave a keep-alive socket for this thread.
        crate::http::request_bytes_with_options(
            &addr,
            "POST",
            "/warm",
            b"{}",
            "application/json",
            options,
        )
        .expect("first exchange should succeed and pool its socket");

        let before = hits.load(Ordering::SeqCst);
        let _ = crate::http::request_bytes_with_options(
            &addr,
            "POST",
            "/slow",
            b"{}",
            "application/json",
            options,
        );
        // Leave room for a second attempt to arrive, so this fails loudly rather than racing.
        std::thread::sleep(Duration::from_millis(500));
        assert_eq!(
            hits.load(Ordering::SeqCst) - before,
            1,
            "a request that timed out on a pooled socket must not be sent again on a fresh one"
        );
    }

    #[test]
    fn a_caller_can_tell_an_unknown_write_from_a_failed_one() {
        // The counter added alongside this tells an OPERATOR how many writes are unaccounted
        // for. It does not help the application holding the error, and that application has
        // the same decision the client had: retry, or not. Retrying a write that already
        // applied is how it gets applied twice -- the exact thing the client stopped doing
        // internally. Handing back the same "server_error" for both cases pushes that bug one
        // layer out.
        let slow = test_addr(18_402);
        let slow_for_thread = slow.clone();
        std::thread::spawn(move || {
            serve(&slow_for_thread, move |_request| {
                std::thread::sleep(Duration::from_millis(500));
                crate::http::json_response(200, &Status::ok())
            })
            .unwrap();
        });
        start_meta(test_addr(18_403), slow.clone());
        wait_for_http(&test_addr(18_403));

        let proxy = ProxyService::new(ProxyOptions {
            meta_addr: test_addr(18_403),
            route_cache_ttl_ms: 60_000,
            connect_timeout_ms: 200,
            io_timeout_ms: 120,
            ..ProxyOptions::default()
        });
        let response = proxy.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        assert!(!response.status.ok);
        assert_eq!(
            response.status.code, "write_outcome_unknown",
            "a caller must be able to see that retrying this write is not free"
        );

        // A read that fails the same way is NOT unknown -- repeating a read cannot change
        // anything, so it keeps the ordinary code and callers keep retrying it freely.
        let response = proxy.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        assert!(!response.status.ok);
        assert_ne!(
            response.status.code, "write_outcome_unknown",
            "a read is always safe to repeat and must not be labelled unknown"
        );
    }

    #[test]
    fn writes_of_unknown_outcome_are_counted_separately_from_ordinary_failures() {
        // After a write stopped being replayed on timeout, "backend_errors" stopped being
        // enough to reason about. A refused connection is harmless -- the write never landed
        // and it was retried. A timeout is not: the write may have been applied and nothing
        // retried it. Those two were indistinguishable in the counters, so nobody could
        // answer the one question that matters after an incident: did any writes end up in a
        // state we cannot determine?
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        let slow = test_addr(18_396);
        let slow_for_thread = slow.clone();
        std::thread::spawn(move || {
            serve(&slow_for_thread, move |_request| {
                std::thread::sleep(Duration::from_millis(500));
                crate::http::json_response(200, &Status::ok())
            })
            .unwrap();
        });
        start_meta(test_addr(18_397), slow.clone());
        wait_for_http(&test_addr(18_397));

        let timing_out = ProxyService::new(ProxyOptions {
            meta_addr: test_addr(18_397),
            route_cache_ttl_ms: 60_000,
            connect_timeout_ms: 200,
            io_timeout_ms: 120,
            ..ProxyOptions::default()
        });
        let _ = timing_out.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        assert_eq!(
            timing_out.info().stats.writes_of_unknown_outcome, 1,
            "a write that timed out is a write nobody can account for"
        );

        // A refused connection is the opposite case and must NOT be counted: it proves the
        // write never arrived, and it was replayed onto a healthy datanode.
        start_server(test_addr(18_398), engine.clone());
        start_meta(test_addr(18_399), test_addr(18_398));
        wait_for_http(&test_addr(18_399));
        wait_for_http(&test_addr(18_398));
        let refused = ProxyService::new(ProxyOptions {
            meta_addr: test_addr(18_399),
            route_cache_ttl_ms: 60_000,
            connect_timeout_ms: 50,
            io_timeout_ms: 200,
            ..ProxyOptions::default()
        });
        refused.client().insert_cached_route_for_test(1, "127.0.0.1:1");
        let response = refused.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        assert!(response.status.ok, "a refused write is replayed and should succeed");
        assert_eq!(
            refused.info().stats.writes_of_unknown_outcome, 0,
            "a refused write provably never arrived, so it is not unknown"
        );
    }

    #[test]
    fn a_write_whose_outcome_is_unknown_is_not_sent_twice() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        // A datanode that accepts the request and then stops answering. That is the case
        // that matters: the connection succeeded, the request was delivered, and the client
        // is left not knowing whether it was applied.
        let hits = std::sync::Arc::new(AtomicUsize::new(0));
        let slow = test_addr(18_390);
        let h = hits.clone();
        let slow_for_thread = slow.clone();
        std::thread::spawn(move || {
            serve(&slow_for_thread, move |_request| {
                h.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(500));
                json_response(200, &Status::ok())
            })
            .unwrap();
        });
        start_meta(test_addr(18_391), slow.clone());
        wait_for_http(&test_addr(18_391));

        let proxy = ProxyService::new(ProxyOptions {
            meta_addr: test_addr(18_391),
            route_cache_ttl_ms: 60_000,
            connect_timeout_ms: 200,
            io_timeout_ms: 120,
            ..ProxyOptions::default()
        });

        hits.store(0, Ordering::SeqCst);
        let _ = proxy.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "counted".to_string(),
                value: b"v".to_vec(),
            },
        });
        // Leave time for a second attempt to land, so this fails loudly rather than racing.
        std::thread::sleep(Duration::from_millis(400));
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "a write that timed out must not be sent again -- the timeout says the datanode \
             stopped answering, not that it never received the write"
        );

        // A read is a different matter: repeating it cannot change anything, so the
        // refresh-and-retry still applies. Pinned so the guard cannot quietly widen.
        hits.store(0, Ordering::SeqCst);
        let _ = proxy.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "counted".to_string(),
            },
        });
        std::thread::sleep(Duration::from_millis(400));
        assert!(
            hits.load(Ordering::SeqCst) >= 2,
            "a read should still be retried after a backend failure, saw {}",
            hits.load(Ordering::SeqCst)
        );
    }

    #[test]
    fn topology_check_stays_off_the_request_path() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let topo_posts = std::sync::Arc::new(AtomicUsize::new(0));

        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        start_server(test_addr(18_384), engine.clone());

        let meta = crate::meta::SingleNodeMeta::default();
        meta.register_server(crate::meta::RegisterServerRequest {
            numa_nodes: Vec::new(),
            server_addr: test_addr(18_384),
            node_id: 1,
            location: "zone-a".to_string(),
            binary_version: "v-a".to_string(),
        });
        meta.register(RegisterShardRequest {
            shard_id: 1,
            server_addr: test_addr(18_384),
        });

        let addr = test_addr(18_385);
        let tp = topo_posts.clone();
        let m = meta.clone();
        std::thread::spawn(move || {
            serve(&addr, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("GET", path) if path.starts_with("/shards/") => {
                        let shard_id =
                            path.trim_start_matches("/shards/").parse().unwrap_or_default();
                        json_response(200, &m.get(shard_id))
                    }
                    ("POST", "/tables/topology") => {
                        let req = parse_json::<GetTableTopologyRequest>(&request.body).unwrap();
                        json_response(200, &m.get_table_topology(req))
                    }
                    ("POST", "/meta/topology_version") => {
                        tp.fetch_add(1, Ordering::SeqCst);
                        let req = parse_json::<TopologyVersionRequest>(&request.body).unwrap();
                        json_response(200, &m.topology_version_report(req))
                    }
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });
        wait_for_http(&test_addr(18_384));
        wait_for_http(&test_addr(18_385));

        let run = |options: ProxyOptions| {
            let proxy = ProxyService::new(options);
            for i in 0..10 {
                assert!(
                    proxy
                        .execute(ExecuteRequest {
                            shard_id: 1,
                            command: Command::StringSet {
                                key: format!("k{i}"),
                                value: b"v".to_vec(),
                            },
                        })
                        .status
                        .ok
                );
            }
        };

        // Default interval: the burst shares one check instead of paying a metaserver
        // round-trip each. This is the whole point -- the metaserver was synchronously in
        // the path of every request, and its latency was added to every operation.
        topo_posts.store(0, Ordering::SeqCst);
        run(ProxyOptions {
            meta_addr: test_addr(18_385),
            route_cache_ttl_ms: 60_000,
            ..ProxyOptions::default()
        });
        let throttled = topo_posts.load(Ordering::SeqCst);
        assert!(
            throttled <= 2,
            "10 requests inside the interval should share a check, made {throttled}"
        );

        // Zero restores the older behaviour exactly, for anyone who wants a check per
        // request. Asserted so the escape hatch cannot rot.
        topo_posts.store(0, Ordering::SeqCst);
        run(ProxyOptions {
            meta_addr: test_addr(18_385),
            route_cache_ttl_ms: 60_000,
            topology_check_interval_ms: 0,
            ..ProxyOptions::default()
        });
        let every = topo_posts.load(Ordering::SeqCst);
        assert!(
            every >= 8,
            "with the interval at zero every request should check, made only {every}"
        );
    }

    #[test]
    fn route_cache_serves_repeat_requests_instead_of_re_resolving() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let shard_gets = std::sync::Arc::new(AtomicUsize::new(0));
        let topo_posts = std::sync::Arc::new(AtomicUsize::new(0));

        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        start_server(test_addr(18_380), engine.clone());

        let meta = crate::meta::SingleNodeMeta::default();
        meta.register_server(crate::meta::RegisterServerRequest {
            numa_nodes: Vec::new(),
            server_addr: test_addr(18_380),
            node_id: 1,
            location: "zone-a".to_string(),
            binary_version: "v-a".to_string(),
        });
        meta.register(RegisterShardRequest {
            shard_id: 1,
            server_addr: test_addr(18_380),
        });

        let addr = test_addr(18_381);
        let sg = shard_gets.clone();
        let tp = topo_posts.clone();
        let m = meta.clone();
        std::thread::spawn(move || {
            serve(&addr, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("GET", path) if path.starts_with("/shards/") => {
                        sg.fetch_add(1, Ordering::SeqCst);
                        let shard_id = path.trim_start_matches("/shards/").parse().unwrap_or_default();
                        json_response(200, &m.get(shard_id))
                    }
                    ("POST", "/tables/topology") => {
                        let req = parse_json::<GetTableTopologyRequest>(&request.body).unwrap();
                        json_response(200, &m.get_table_topology(req))
                    }
                    ("POST", "/meta/topology_version") => {
                        tp.fetch_add(1, Ordering::SeqCst);
                        let req = parse_json::<TopologyVersionRequest>(&request.body).unwrap();
                        json_response(200, &m.topology_version_report(req))
                    }
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });
        wait_for_http(&test_addr(18_380));
        wait_for_http(&test_addr(18_381));

        let proxy = ProxyService::new(ProxyOptions {
            meta_addr: test_addr(18_381),
            route_cache_ttl_ms: 60_000,
            ..ProxyOptions::default()
        });

        for i in 0..10 {
            assert!(proxy.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet { key: format!("k{i}"), value: b"v".to_vec() },
            }).status.ok);
        }

        let gets = shard_gets.load(Ordering::SeqCst);
        let posts = topo_posts.load(Ordering::SeqCst);
        let report = proxy.preflight_report();

        // Ten identical requests behind a 60s TTL. This used to be 10 shard lookups and
        // zero cache hits: routes resolved by shard lookup were stamped topology version 0,
        // "unknown" counted as stale, so every request invalidated the entry it had just
        // written and resolved again. The cache existed and never once returned a hit.
        assert!(
            gets <= 2,
            "10 requests should resolve at most twice, took {gets} shard lookups              (hits={} misses={} refreshes={})",
            report.client.route_cache_hits,
            report.client.route_cache_misses,
            report.client.route_refreshes
        );
        assert!(
            report.client.route_cache_hits >= 7,
            "the cache should serve the repeats, saw {} hits across 10 requests",
            report.client.route_cache_hits
        );

        // NOT asserted low on purpose: the topology check itself is still one POST per
        // request on every command entry point. That is a separate cost and a separate
        // change; this test pins the route cache, not the topology check.
        assert!(posts >= 1, "topology is still checked, saw {posts}");
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

    /// Minimal datanode surface that serves the low-level endpoints the proxy
    /// forwards to: the raw fast store (`/ingest/batch`, `/execute`) and the
    /// batched context endpoints (`/context/ingest_extract`, `/context/retrieve`).
    fn start_context_server(addr: String, engine: TemporalEngine) {
        use crate::context_workflow::{
            ingest_extract_context, retrieve_context, ContextIngestExtractRequest,
            ContextRetrieveRequest,
        };
        use crate::ingestion::IngestionBatchRequest;
        std::thread::spawn(move || {
            serve(&addr, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/ingest/batch") => {
                        let req = parse_json::<IngestionBatchRequest>(&request.body).unwrap();
                        json_response(200, &engine.ingest_batch(req))
                    }
                    ("POST", "/execute") => {
                        let req = parse_json::<ExecuteRequest>(&request.body).unwrap();
                        json_response(200, &engine.execute(req))
                    }
                    ("POST", "/context/ingest_extract") => {
                        let req =
                            parse_json::<ContextIngestExtractRequest>(&request.body).unwrap();
                        json_response(200, &ingest_extract_context(&engine, req))
                    }
                    ("POST", "/context/retrieve") => {
                        let req = parse_json::<ContextRetrieveRequest>(&request.body).unwrap();
                        json_response(200, &retrieve_context(&engine, req))
                    }
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });
    }

    #[test]
    fn context_route_lookup_uses_the_shared_route_cache() {
        // The context routes are the whole of the gateway's traffic, and every one of them
        // used to resolve its shard with a direct `/shards/{id}` GET -- no cache, no TTL, no
        // backend-failure accounting. That put a metaserver round-trip in front of every
        // ingest, extract and retrieve, and put the metaserver in the request path of all of
        // them, while the command path had been caching the identical lookup all along.
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        start_context_server(test_addr(18_372), engine.clone());
        start_meta(test_addr(18_373), test_addr(18_372));
        wait_for_http(&test_addr(18_373));
        wait_for_http(&test_addr(18_372));

        let proxy = ProxyService::new(ProxyOptions {
            meta_addr: test_addr(18_373),
            route_cache_ttl_ms: 60_000,
            context_first_shard_id: 1,
            context_shard_count: 1,
            ..ProxyOptions::default()
        });

        let body = serde_json::to_vec(&serde_json::json!({
            "scope": {"tenant_id": "team-alpha", "account_id": "acct_42"},
            "messages": [{"role": "user", "content": "route cache check"}]
        }))
        .unwrap();
        for attempt in 0..3 {
            let (code, _) = proxy.handle(HttpRequest {
                method: "POST".to_string(),
                path: "/context/ingest".to_string(),
                body: body.clone(),
            });
            assert_eq!(code, 200, "context ingest {attempt} should succeed");
        }

        let report = proxy.preflight_report();
        // One resolve for the first request; the other two come from the cache. Before this
        // the count would have been three lookups and an empty cache, because the context
        // path never went through the client that owns the cache.
        assert_eq!(
            report.client.route_refreshes, 1,
            "three context requests should resolve the shard once, saw {} refreshes",
            report.client.route_refreshes
        );
        assert!(
            report.client.route_cache_hits >= 2,
            "the last two requests should be cache hits, saw {}",
            report.client.route_cache_hits
        );
        assert_eq!(
            report.client.route_cache_size, 1,
            "the context lookup should populate the shared route cache"
        );
    }

    #[test]
    fn context_route_cache_is_invalidated_when_the_shard_moves() {
        // Caching the context route is only safe if something notices the shard moving. A
        // proxy fronting the context gateway serves these three routes and nothing else --
        // it never calls execute -- so if the context path did not check topology itself,
        // nothing on that proxy ever would, and it would keep sending the gateway to the old
        // datanode for the whole of route_cache_ttl_ms.
        let dir_a = tempfile::tempdir().unwrap();
        let engine_a = TemporalEngine::with_local_dirs(
            1024,
            dir_a.path().join("cache"),
            dir_a.path().join("pages"),
            dir_a.path().join("indexes"),
        );
        engine_a.load_shard(1);
        start_context_server(test_addr(18_374), engine_a.clone());

        let dir_b = tempfile::tempdir().unwrap();
        let engine_b = TemporalEngine::with_local_dirs(
            1024,
            dir_b.path().join("cache"),
            dir_b.path().join("pages"),
            dir_b.path().join("indexes"),
        );
        engine_b.load_shard(1);
        start_context_server(test_addr(18_375), engine_b.clone());

        let meta = crate::meta::SingleNodeMeta::default();
        meta.register_server(crate::meta::RegisterServerRequest {
            numa_nodes: Vec::new(),
            server_addr: test_addr(18_374),
            node_id: 1,
            location: "zone-a".to_string(),
            binary_version: "v-a".to_string(),
        });
        meta.register(RegisterShardRequest {
            shard_id: 1,
            server_addr: test_addr(18_374),
        });
        start_meta_service(test_addr(18_376), meta.clone());
        wait_for_http(&test_addr(18_374));
        wait_for_http(&test_addr(18_375));
        wait_for_http(&test_addr(18_376));

        let proxy = ProxyService::new(ProxyOptions {
            meta_addr: test_addr(18_376),
            route_cache_ttl_ms: 60_000,
            context_first_shard_id: 1,
            context_shard_count: 1,
            ..ProxyOptions::default()
        });

        let body = serde_json::to_vec(&serde_json::json!({
            "scope": {"tenant_id": "team-alpha", "account_id": "acct_42"},
            "messages": [{"role": "user", "content": "before the move"}]
        }))
        .unwrap();
        let ingest = |body: Vec<u8>| {
            proxy.handle(HttpRequest {
                method: "POST".to_string(),
                path: "/context/ingest".to_string(),
                body,
            })
        };

        let (code, _) = ingest(body.clone());
        assert_eq!(code, 200);
        let before = proxy.preflight_report();
        assert_eq!(before.client.route_refreshes, 1, "first request resolves once");

        // The shard moves to the second datanode.
        meta.register_server(crate::meta::RegisterServerRequest {
            numa_nodes: Vec::new(),
            server_addr: test_addr(18_375),
            node_id: 2,
            location: "zone-b".to_string(),
            binary_version: "v-b".to_string(),
        });
        meta.register(RegisterShardRequest {
            shard_id: 1,
            server_addr: test_addr(18_375),
        });

        // Deliberately NOT disabling the topology check interval here: wait it out instead,
        // so this covers what a deployment actually runs rather than an escape hatch.
        std::thread::sleep(std::time::Duration::from_millis(
            ProxyOptions::default().topology_check_interval_ms + 25,
        ));

        let moved_body = serde_json::to_vec(&serde_json::json!({
            "scope": {"tenant_id": "team-alpha", "account_id": "acct_42"},
            "messages": [{"role": "user", "content": "after the move"}]
        }))
        .unwrap();
        let (code, _) = ingest(moved_body);
        assert_eq!(code, 200);

        let after = proxy.preflight_report();
        assert!(
            after.client.route_refreshes > before.client.route_refreshes,
            "the context path must re-resolve after the shard moves, refreshes stayed at {}",
            after.client.route_refreshes
        );
    }

    #[test]
    fn proxy_routes_high_level_context_ingest_and_retrieve_by_tenant() {
        use crate::context_workflow::{ContextIngestExtractReport, ContextRetrieveReport};

        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        start_context_server(test_addr(18_366), engine.clone());
        start_meta(test_addr(18_367), test_addr(18_366));
        wait_for_http(&test_addr(18_367));
        wait_for_http(&test_addr(18_366));

        let proxy = ProxyService::new(ProxyOptions {
            meta_addr: test_addr(18_367),
            route_cache_ttl_ms: 60_000,
            context_first_shard_id: 1,
            context_shard_count: 1,
            ..ProxyOptions::default()
        });

        // The proxy owns hashing: the tenant hash it derives here must match the
        // shard it routes to, and retrieve must land on the same shard.
        let scope = context::ProxyContextScope {
            tenant_id: "team-alpha".to_string(),
            account_id: "acct_42".to_string(),
            ..Default::default()
        };
        assert_eq!(proxy.context_shard_id(context::context_tenant_hash(&scope)), 1);

        // FAST path: /context/ingest stores raw events in one routed HashMultiSet
        // write (no extraction) and returns an ExecuteResponse, not an extraction
        // report. The buffer is verified by the commit-replay below.
        let ingest_body = serde_json::to_vec(&serde_json::json!({
            "scope": {"tenant_id": "team-alpha", "account_id": "acct_42"},
            "messages": [
                {"role": "user", "content": "the launch checklist owner is Dana"},
                {"role": "assistant", "content": "noted: Dana owns the launch checklist"}
            ]
        }))
        .unwrap();
        let (code, body) = proxy.handle(HttpRequest {
            method: "POST".to_string(),
            path: "/context/ingest".to_string(),
            body: ingest_body,
        });
        assert_eq!(code, 200);
        let fast = parse_json::<crate::types::ExecuteResponse>(&body).unwrap();
        assert!(fast.status.ok, "fast ingest status: {:?}", fast.status);

        // BATCHED commit with NO messages: the proxy replays the buffered raw
        // events (HGETALL) and extracts them via /context/ingest_extract.
        let commit_body = serde_json::to_vec(&serde_json::json!({
            "scope": {"tenant_id": "team-alpha", "account_id": "acct_42"}
        }))
        .unwrap();
        let (code, body) = proxy.handle(HttpRequest {
            method: "POST".to_string(),
            path: "/context/extract".to_string(),
            body: commit_body,
        });
        assert_eq!(code, 200);
        let report = parse_json::<ContextIngestExtractReport>(&body).unwrap();
        assert!(report.status.ok, "extract status: {:?}", report.status);
        assert_eq!(report.accepted, 2, "commit replayed the buffer: {report:?}");
        assert!(!report.node_hashes.is_empty());
        let node_hashes = report.node_hashes.clone();

        // Retrieve by the node hashes the ingest returned (the datanode's local
        // workflow retrieves by node hash). The proxy re-derives the same
        // shard/tenant from the scope, so the request lands on the same data.
        let retrieve_body = serde_json::to_vec(&serde_json::json!({
            "scope": {"tenant_id": "team-alpha", "account_id": "acct_42"},
            "query": "who owns the launch checklist",
            "node_hashes": node_hashes,
        }))
        .unwrap();
        let (code, body) = proxy.handle(HttpRequest {
            method: "POST".to_string(),
            path: "/context/retrieve".to_string(),
            body: retrieve_body,
        });
        assert_eq!(code, 200);
        let retrieved = parse_json::<ContextRetrieveReport>(&body).unwrap();
        assert!(retrieved.status.ok, "retrieve status: {:?}", retrieved.status);
        assert!(
            retrieved.node_count >= 1,
            "expected retrieval to see the ingested tenant nodes, got {retrieved:?}"
        );

        // A different tenant asking for the SAME node hashes must not see
        // team-alpha's nodes (keys are tenant-scoped), proving the proxy's
        // per-tenant hashing is wired end to end.
        let other_body = serde_json::to_vec(&serde_json::json!({
            "scope": {"tenant_id": "team-beta", "account_id": "acct_42"},
            "query": "who owns the launch checklist",
            "node_hashes": report.node_hashes,
        }))
        .unwrap();
        let (code, body) = proxy.handle(HttpRequest {
            method: "POST".to_string(),
            path: "/context/retrieve".to_string(),
            body: other_body,
        });
        assert_eq!(code, 200);
        let other = parse_json::<ContextRetrieveReport>(&body).unwrap();
        assert!(other.status.ok);
        assert_eq!(other.node_count, 0, "cross-tenant leak: {other:?}");
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
