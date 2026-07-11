use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize};

mod commands;
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
    ClientStats, ClientTopologyCacheReport, ClientTopologyRefreshReport,
    MatrixArkBatchAppendRequest, MatrixArkRetrieveContextPackRequest, ReplicaReadPolicy,
    RequestOptions, TableOptions, TemporalStoreClient,
};
use crate::http::{get_json_with_options, post_json_with_options, HttpRequest, HttpRequestOptions};
use crate::meta::GetShardResponse;
use crate::meta::{
    AckResponse, ProxyHeartbeatRequest, ProxyHeartbeatResponse, RegisterProxyRequest,
    TopologyVersionReport, TopologyVersionRequest,
};
use crate::types::{
    BatchExecuteRequest, BatchExecuteResponse, Command, CommandResponse, ExecuteRequest,
    ExecuteResponse, FeatureFilter, FeaturePoint, FeatureWritePolicy, RiskFamily, RiskFolType,
    SequenceFeatureRow, SequenceQuerySpec, ShardId, Status, StringSetCondition,
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
    #[serde(alias = "namespace_name")]
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
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyTableBatchExecuteRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub commands: Vec<Command>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyKeyCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxySetCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    #[serde(deserialize_with = "proxy_bytes_from_json")]
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxySetExCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    #[serde(deserialize_with = "proxy_bytes_from_json")]
    pub value: Vec<u8>,
    #[serde(alias = "ttl")]
    pub ttl_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxySetConditionalCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    #[serde(deserialize_with = "proxy_bytes_from_json")]
    pub value: Vec<u8>,
    #[serde(default)]
    pub ttl_ms: Option<u64>,
    pub condition: StringSetCondition,
    #[serde(default)]
    pub return_old: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyHashCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    pub field: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyHashSetCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    pub field: String,
    #[serde(deserialize_with = "proxy_bytes_from_json")]
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyHashIncrByCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    pub field: String,
    pub increment: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyHashMultiGetCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    #[serde(default, deserialize_with = "proxy_string_vec_from_json")]
    pub fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyHashMultiSetCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    #[serde(default, deserialize_with = "proxy_hash_entries_from_json")]
    pub entries: Vec<(String, Vec<u8>)>,
    #[serde(default, deserialize_with = "proxy_string_vec_from_json")]
    pub fields: Vec<String>,
    #[serde(default, deserialize_with = "proxy_vec_bytes_from_json")]
    pub values: Vec<Vec<u8>>,
}

impl ProxyHashMultiSetCommandRequest {
    fn entries(&self) -> Result<Vec<(String, Vec<u8>)>, Status> {
        if !self.entries.is_empty() {
            return Ok(self.entries.clone());
        }
        if self.fields.len() != self.values.len() {
            return Err(Status::error(
                "bad_request",
                format!(
                    "fields/values length mismatch: fields={}, values={}",
                    self.fields.len(),
                    self.values.len()
                ),
            ));
        }
        Ok(self
            .fields
            .iter()
            .cloned()
            .zip(self.values.iter().cloned())
            .collect())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxySetMemberCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    #[serde(deserialize_with = "proxy_bytes_from_json")]
    pub member: Vec<u8>,
}

fn proxy_bytes_from_json<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    proxy_bytes_value(value).map_err(serde::de::Error::custom)
}

fn proxy_vec_bytes_from_json<'de, D>(deserializer: D) -> Result<Vec<Vec<u8>>, D::Error>
where
    D: Deserializer<'de>,
{
    let values = Vec::<serde_json::Value>::deserialize(deserializer)?;
    values
        .into_iter()
        .map(proxy_bytes_value)
        .collect::<Result<Vec<_>, _>>()
        .map_err(serde::de::Error::custom)
}

fn proxy_string_vec_from_json<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let values = Vec::<serde_json::Value>::deserialize(deserializer)?;
    values
        .into_iter()
        .map(proxy_string_value)
        .collect::<Result<Vec<_>, _>>()
        .map_err(serde::de::Error::custom)
}

fn proxy_string_value(value: serde_json::Value) -> Result<String, String> {
    match value {
        serde_json::Value::String(value) => Ok(value),
        value => proxy_bytes_value(value).map(|bytes| String::from_utf8_lossy(&bytes).into_owned()),
    }
}

fn proxy_hash_entries_from_json<'de, D>(deserializer: D) -> Result<Vec<(String, Vec<u8>)>, D::Error>
where
    D: Deserializer<'de>,
{
    let values = Vec::<serde_json::Value>::deserialize(deserializer)?;
    values
        .into_iter()
        .map(|value| match value {
            serde_json::Value::Array(mut items) if items.len() == 2 => {
                let raw_value = items.pop().unwrap();
                let raw_field = items.pop().unwrap();
                let field = raw_field
                    .as_str()
                    .ok_or_else(|| "hash entry field must be a string".to_string())?
                    .to_string();
                Ok((field, proxy_bytes_value(raw_value)?))
            }
            serde_json::Value::Object(mut object) => {
                let field = object
                    .remove("field")
                    .and_then(|value| value.as_str().map(ToOwned::to_owned))
                    .ok_or_else(|| "hash entry field must be present".to_string())?;
                let value = object
                    .remove("value")
                    .ok_or_else(|| "hash entry value must be present".to_string())?;
                Ok((field, proxy_bytes_value(value)?))
            }
            _ => Err("hash entry must be [field, value] or {field, value}".to_string()),
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(serde::de::Error::custom)
}

fn proxy_bytes_value(value: serde_json::Value) -> Result<Vec<u8>, String> {
    match value {
        serde_json::Value::Null => Ok(Vec::new()),
        serde_json::Value::String(value) => Ok(value.into_bytes()),
        serde_json::Value::Array(_) => serde_json::from_value(value).map_err(|err| err.to_string()),
        serde_json::Value::Object(mut object) => {
            if let Some(bytes) = object.remove("bytes") {
                serde_json::from_value(bytes).map_err(|err| err.to_string())
            } else if let Some(value) = object.remove("value") {
                proxy_bytes_value(value)
            } else {
                serde_json::to_vec(&serde_json::Value::Object(object))
                    .map_err(|err| err.to_string())
            }
        }
        value => serde_json::to_vec(&value).map_err(|err| err.to_string()),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyExpireCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    #[serde(alias = "ttl")]
    pub ttl_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyFeatureAddCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    #[serde(alias = "point_list")]
    pub points: Vec<crate::types::FeaturePoint>,
    #[serde(default)]
    pub policy: Option<FeatureWritePolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyFeatureQueryCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    #[serde(alias = "start_ts")]
    pub start_ms: u64,
    #[serde(alias = "end_ts")]
    pub end_ms: u64,
    #[serde(default)]
    pub count: Option<usize>,
    #[serde(default)]
    pub filters: Vec<FeatureFilter>,
    #[serde(default)]
    pub fields: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyFeatureReplaceCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    #[serde(alias = "start_ts")]
    pub start_ms: u64,
    #[serde(alias = "end_ts")]
    pub end_ms: u64,
    #[serde(alias = "point_list")]
    pub points: Vec<crate::types::FeaturePoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyFeatureAggQueryCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    #[serde(alias = "start_ts")]
    pub start_ms: u64,
    #[serde(alias = "end_ts")]
    pub end_ms: u64,
    pub aggregator: String,
    #[serde(default)]
    pub count: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxySequenceAddCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    #[serde(alias = "row_list")]
    pub rows: Vec<crate::types::SequenceFeatureRow>,
    #[serde(default)]
    pub policy: Option<FeatureWritePolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxySequenceQueryCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    #[serde(alias = "start_ts")]
    pub start_ms: u64,
    #[serde(alias = "end_ts")]
    pub end_ms: u64,
    #[serde(alias = "limit")]
    pub count: usize,
    #[serde(default)]
    pub filters: Vec<FeatureFilter>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxySequenceBatchQueryCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub queries: Vec<SequenceQuerySpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyIpsAddCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    #[serde(alias = "ts", alias = "occur_time")]
    pub timestamp_ms: u64,
    #[serde(deserialize_with = "proxy_bytes_from_json")]
    pub instance: Vec<u8>,
    #[serde(default)]
    pub action_type: Option<u32>,
    #[serde(default)]
    pub table_id: Option<u64>,
    #[serde(default)]
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyIpsQueryLastCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyIpsRangeCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    #[serde(alias = "start_ts")]
    pub start_ms: u64,
    #[serde(alias = "end_ts")]
    pub end_ms: u64,
    #[serde(default)]
    pub count: Option<usize>,
    #[serde(default)]
    pub action_type: Option<u32>,
    #[serde(default)]
    pub table_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyIpsBatchQueryLastCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub keys: Vec<String>,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyIpsLoadCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    pub points: Vec<FeaturePoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyIpsRemoveCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    #[serde(alias = "ts", alias = "occur_time")]
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyRiskIncrementCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    #[serde(alias = "ts", alias = "occur_time")]
    pub timestamp_ms: u64,
    #[serde(alias = "value")]
    pub amount: i64,
    #[serde(
        default,
        alias = "precision",
        deserialize_with = "proxy_optional_precision_ms_from_json"
    )]
    pub precision_ms: Option<u64>,
    #[serde(default, alias = "ttl")]
    pub ttl_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyRiskCountCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    #[serde(alias = "start_ts")]
    pub start_ms: u64,
    #[serde(alias = "end_ts")]
    pub end_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyRiskQueryCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    #[serde(alias = "start_ts")]
    pub start_ms: u64,
    #[serde(alias = "end_ts")]
    pub end_ms: u64,
    pub aggregator: String,
    #[serde(default)]
    pub count: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyRiskChangeAddCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    #[serde(alias = "ts", alias = "occur_time")]
    pub timestamp_ms: u64,
    #[serde(deserialize_with = "proxy_bytes_from_json")]
    pub value: Vec<u8>,
    #[serde(
        default,
        alias = "precision",
        deserialize_with = "proxy_optional_precision_ms_from_json"
    )]
    pub precision_ms: Option<u64>,
    #[serde(default, alias = "ttl")]
    pub ttl_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyRiskFamilySetCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub family: RiskFamily,
    pub key: String,
    #[serde(alias = "ts", alias = "occur_time")]
    pub timestamp_ms: u64,
    #[serde(alias = "value")]
    pub amount: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyRiskFamilyQueryCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub family: RiskFamily,
    pub key: String,
    #[serde(alias = "start_ts")]
    pub start_ms: u64,
    #[serde(alias = "end_ts")]
    pub end_ms: u64,
    pub aggregator: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyRiskFamilySetAndGetCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub family: RiskFamily,
    pub key: String,
    #[serde(alias = "ts", alias = "occur_time")]
    pub timestamp_ms: u64,
    #[serde(alias = "value")]
    pub amount: i64,
    #[serde(alias = "start_ts")]
    pub start_ms: u64,
    #[serde(alias = "end_ts")]
    pub end_ms: u64,
    pub aggregator: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextUpsertNodeCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub node: crate::types::ContextNode,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextGetNodeCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub node_hash: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextGetNodesCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub node_hashes: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextWriteEventCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub node_hash: u64,
    pub event: crate::types::ContextEvent,
    #[serde(default)]
    pub first_write_only: bool,
    #[serde(default)]
    pub cold_storage: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextWriteExtractedEventCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub node_hash: u64,
    pub event: crate::types::ContextEvent,
    #[serde(default)]
    pub indexes: crate::types::ContextExtractedEventIndexes,
    #[serde(default)]
    pub first_write_only: bool,
    #[serde(default)]
    pub cold_storage: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextQueryEventsCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub node_hash: u64,
    pub start_time_ms: u64,
    pub end_time_ms: u64,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub current_valid_only: bool,
    #[serde(default)]
    pub as_of_ms: u64,
    #[serde(default)]
    pub kinds: Vec<u32>,
    #[serde(default)]
    pub statuses: Vec<u32>,
    #[serde(default)]
    pub min_confidence: f32,
    #[serde(default)]
    pub min_importance: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextWriteIndexRefCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub index_name: String,
    pub index_value_hash: u64,
    #[serde(default)]
    pub scope_hash: u64,
    pub event_time_ms: u64,
    pub index_ref: crate::types::ContextIndexRef,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextQueryIndexCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub index_name: String,
    pub index_value_hash: u64,
    #[serde(default)]
    pub scope_hash: u64,
    pub start_time_ms: u64,
    pub end_time_ms: u64,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextQueryIndexIntersectionCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub predicates: Vec<crate::types::ContextIndexLookup>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextWritePackAuditCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub audit: crate::types::ContextPackAudit,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextQueryPackAuditCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub session_hash: u64,
    pub start_time_ms: u64,
    pub end_time_ms: u64,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextMarkSummaryDirtyCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub marker: crate::types::ContextSummaryDirtyMarker,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextQuerySummaryDirtyCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub node_hash: u64,
    pub start_time_ms: u64,
    pub end_time_ms: u64,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextUpsertEntityCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub entity: crate::types::ContextEntity,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextGetEntityCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub node_hash: u64,
    pub entity_hash: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextQueryEntitiesCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub node_hash: u64,
    pub entity_hashes: Vec<u64>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextUpsertChildRefCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub child_ref: crate::types::ContextChildRef,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextQueryChildrenCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub parent_hash: u64,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextUpsertEmbeddingCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub embedding: crate::types::ContextEmbedding,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextQueryEmbeddingsCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub ref_hashes: Vec<u64>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextTraverseTreeCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub start_node_hash: u64,
    pub query_vector: Vec<f32>,
    #[serde(default)]
    pub max_depth: Option<u32>,
    #[serde(default)]
    pub top_k_per_depth: Option<usize>,
    #[serde(default)]
    pub max_children_scored_per_parent: Option<usize>,
    #[serde(default)]
    pub max_candidate_nodes: Option<usize>,
    #[serde(default)]
    pub leaf_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextUpsertSummaryCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub summary: crate::types::ContextSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextQuerySummariesCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub node_hash: u64,
    pub level: u32,
    pub as_of_ms: u64,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextWriteCompressionEventCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub event: crate::types::ContextCompressionEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextQueryCompressionEventsCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub node_hashes: Vec<u64>,
    pub start_time_ms: u64,
    pub end_time_ms: u64,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextCompressEventsCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub node_hash: u64,
    pub source_start_ms: u64,
    pub source_end_ms: u64,
    pub compressed_time_ms: u64,
    #[serde(default)]
    pub max_source_events: Option<usize>,
    #[serde(default)]
    pub min_confidence: f32,
    #[serde(default)]
    pub min_importance: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyContextQueryNodeContextCommandRequest {
    pub namespace: String,
    pub table_name: String,
    pub tenant_hash: u64,
    pub node_hash: u64,
    #[serde(default)]
    pub summary_level: Option<u32>,
    pub as_of_ms: u64,
    #[serde(default)]
    pub cold_start_time_ms: u64,
    #[serde(default)]
    pub cold_end_time_ms: u64,
    #[serde(default)]
    pub compression_limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyRiskHsetCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    #[serde(default)]
    pub timestamp_ms: u64,
    #[serde(default)]
    pub amount: i64,
    #[serde(default)]
    pub value: Option<serde_json::Value>,
    #[serde(default, alias = "occur_time")]
    pub occur_time_seconds: u64,
    #[serde(default, alias = "ttl")]
    pub ttl_ms: u64,
    #[serde(default)]
    pub precision_ms: Option<u64>,
    #[serde(default)]
    pub htype: ProxyRiskHType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyRiskHqueryCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    #[serde(default)]
    pub start_ms: Option<u64>,
    #[serde(default)]
    pub end_ms: Option<u64>,
    #[serde(default)]
    pub windows: Vec<ProxyRiskWindow>,
    #[serde(default)]
    pub htype: ProxyRiskHType,
    #[serde(default)]
    pub aggregator: Option<String>,
    #[serde(default)]
    pub precision_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ProxyRiskHType {
    #[default]
    Count,
    Min,
    Max,
    Change,
}

impl<'de> Deserialize<'de> for ProxyRiskHType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            serde_json::Value::Number(number) => match number.as_i64() {
                Some(0) => Ok(Self::Count),
                Some(1) => Ok(Self::Min),
                Some(2) => Ok(Self::Max),
                Some(3) => Ok(Self::Change),
                _ => Err(serde::de::Error::custom("unknown RiskHType value")),
            },
            serde_json::Value::String(value) => match value.as_str() {
                "COUNT" | "count" | "Count" | "0" => Ok(Self::Count),
                "MIN" | "min" | "Min" | "1" => Ok(Self::Min),
                "MAX" | "max" | "Max" | "2" => Ok(Self::Max),
                "CHANGE" | "change" | "Change" | "3" => Ok(Self::Change),
                _ => Err(serde::de::Error::custom("unknown RiskHType value")),
            },
            _ => Err(serde::de::Error::custom(
                "RiskHType must be a string or integer",
            )),
        }
    }
}

impl ProxyRiskHType {
    fn aggregator(self) -> &'static str {
        match self {
            Self::Count => "sum",
            Self::Min => "min",
            Self::Max => "max",
            Self::Change => "change",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyRiskFolSetCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    #[serde(default)]
    pub value: serde_json::Value,
    #[serde(default)]
    pub occur_time_ms: u64,
    #[serde(default, alias = "occur_time")]
    pub occur_time_seconds: u64,
    #[serde(default)]
    pub ttl_ms: u64,
    #[serde(default, alias = "ttl")]
    pub ttl_seconds: u64,
    #[serde(default = "default_risk_fol_type")]
    pub fol_type: RiskFolType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyRiskCpcSetCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    #[serde(default)]
    pub values: Vec<String>,
    #[serde(default, alias = "occur_time")]
    pub timestamp_ms: u64,
    #[serde(default, alias = "ttl")]
    pub ttl_ms: u64,
    #[serde(
        default,
        alias = "precision",
        deserialize_with = "proxy_optional_precision_ms_from_json"
    )]
    pub precision_ms: Option<u64>,
    #[serde(default)]
    pub dont_upgrade_cpc: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyRiskCpcQueryCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    #[serde(default)]
    pub start_ms: Option<u64>,
    #[serde(default)]
    pub end_ms: Option<u64>,
    #[serde(default = "default_risk_cpc_aggregator")]
    pub aggregator: String,
    #[serde(default)]
    pub windows: Vec<ProxyRiskWindow>,
    #[serde(default)]
    pub with_detail: bool,
    #[serde(
        default,
        alias = "precision",
        deserialize_with = "proxy_optional_precision_ms_from_json"
    )]
    pub precision_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyRiskManagerCommandRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    pub table_name: String,
    pub key: String,
    #[serde(default)]
    pub op_type: Option<serde_json::Value>,
    #[serde(default)]
    pub field_list: Vec<ProxyRiskManagerKvPair>,
    #[serde(default)]
    pub start_offset: String,
    #[serde(default)]
    pub end_offset: String,
    #[serde(default)]
    pub is_cpc: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyRiskManagerKvPair {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyRiskWindow {
    #[serde(default)]
    pub start_offset: i64,
    #[serde(default)]
    pub end_offset: i64,
    #[serde(default)]
    pub unit: ProxyRiskWindowUnit,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ProxyRiskWindowUnit {
    Second,
    Minute,
    #[default]
    Hour,
    Day,
}

impl<'de> Deserialize<'de> for ProxyRiskWindowUnit {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            serde_json::Value::Number(number) => match number.as_i64() {
                Some(0) => Ok(Self::Second),
                Some(1) => Ok(Self::Minute),
                Some(2) => Ok(Self::Hour),
                Some(3) => Ok(Self::Day),
                _ => Err(serde::de::Error::custom("unknown RiskWindowUnit value")),
            },
            serde_json::Value::String(value) => match value.as_str() {
                "Second" | "SECOND" | "second" | "0" => Ok(Self::Second),
                "Minute" | "MINUTE" | "minute" | "1" => Ok(Self::Minute),
                "Hour" | "HOUR" | "hour" | "2" => Ok(Self::Hour),
                "Day" | "DAY" | "day" | "3" => Ok(Self::Day),
                _ => Err(serde::de::Error::custom("unknown RiskWindowUnit value")),
            },
            _ => Err(serde::de::Error::custom(
                "RiskWindowUnit must be a string or integer",
            )),
        }
    }
}

impl ProxyRiskWindowUnit {
    fn duration_ms(self) -> i64 {
        match self {
            Self::Second => 1_000,
            Self::Minute => 60_000,
            Self::Hour => 60 * 60_000,
            Self::Day => 24 * 60 * 60_000,
        }
    }
}

fn default_risk_cpc_aggregator() -> String {
    "sum".to_string()
}

fn default_risk_fol_type() -> RiskFolType {
    RiskFolType::First
}

fn proxy_optional_precision_ms_from_json<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    value
        .map(proxy_precision_ms_value)
        .transpose()
        .map_err(serde::de::Error::custom)
}

fn proxy_precision_ms_value(value: serde_json::Value) -> Result<u64, String> {
    match value {
        serde_json::Value::Null => Ok(0),
        serde_json::Value::Number(number) => number
            .as_u64()
            .map(cpp_risk_precision_to_ms)
            .ok_or_else(|| "precision must be a non-negative integer".to_string()),
        serde_json::Value::String(value) => {
            if let Ok(number) = value.parse::<u64>() {
                return Ok(cpp_risk_precision_to_ms(number));
            }
            match value.as_str() {
                "DISABLE" | "disable" => Ok(0),
                "OneSecond" | "one_second" => Ok(1_000),
                "FiveSeconds" | "five_seconds" => Ok(5_000),
                "TenSeconds" | "ten_seconds" => Ok(10_000),
                "OneMinute" | "one_minute" => Ok(60_000),
                "FiveMinutes" | "five_minutes" => Ok(5 * 60_000),
                "TenMinutes" | "ten_minutes" => Ok(10 * 60_000),
                "OneHour" | "one_hour" => Ok(60 * 60_000),
                "OneDay" | "one_day" => Ok(24 * 60 * 60_000),
                "OneMonth" | "one_month" => Ok(30 * 24 * 60 * 60_000),
                _ => Err("unknown RiskPrecision value".to_string()),
            }
        }
        _ => Err("precision must be a string or integer".to_string()),
    }
}

fn cpp_risk_precision_to_ms(value: u64) -> u64 {
    match value {
        10 => 1_000,
        14 => 5_000,
        19 => 10_000,
        30 => 60_000,
        34 => 5 * 60_000,
        39 => 10 * 60_000,
        50 => 60 * 60_000,
        60 => 24 * 60 * 60_000,
        80 => 30 * 24 * 60 * 60_000,
        value => value,
    }
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

    pub fn handle(&self, request: HttpRequest) -> (u16, Vec<u8>) {
        use crate::http::{json_response, parse_json};
        macro_rules! proxy_command_route {
            ($request_ty:ty, |$req:ident| $builder:block) => {
                match parse_json::<$request_ty>(&request.body) {
                    Ok(req) => {
                        let $req: $request_ty = req;
                        let (namespace, table_name, command) = $builder;
                        json_response(
                            200,
                            &self.table_execute_command(namespace, table_name, command),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            };
        }
        match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/health") => json_response(200, &Status::ok()),
            ("GET", "/metrics") | ("GET", "/ProxyService/Metrics") => {
                (200, self.prometheus_metrics().into_bytes())
            }
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
            ("GET", "/proxy/tonic_contract") | ("GET", "/ProxyService/GetTonicContract") => {
                json_response(200, &self.tonic_streaming_contract())
            }
            ("GET", "/proxy/metrics_parity") | ("GET", "/ProxyService/GetMetricsParity") => {
                json_response(200, &self.metrics_parity_report())
            }
            ("GET", "/proxy/cpp_migration_contract")
            | ("GET", "/ProxyService/GetCppMigrationContract") => {
                json_response(200, &self.cpp_migration_contract())
            }
            ("GET", "/proxy/service_discovery") | ("GET", "/ProxyService/GetServiceDiscovery") => {
                json_response(200, &self.service_discovery_report())
            }
            ("GET", "/proxy/ports") | ("GET", "/ProxyService/GetPorts") => {
                json_response(200, &self.ports_report())
            }
            ("GET", "/proxy/consul_names") | ("GET", "/ProxyService/GetConsulNames") => {
                json_response(200, &self.consul_names_report())
            }
            ("POST", "/proxy/notify_stop") | ("POST", "/ProxyService/NotifyStop") => {
                json_response(200, &self.notify_stop_report())
            }
            ("GET", "/proxy/operational_surface")
            | ("GET", "/ProxyService/GetOperationalSurface") => {
                json_response(200, &self.operational_surface_report())
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
            ("POST", "/ProxyService/MatrixArkBatchAppendRecords")
            | ("POST", "/matrixark/append_records")
            | ("POST", "/matrixark/batch_append_records") => {
                match parse_json::<MatrixArkBatchAppendRequest>(&request.body) {
                    Ok(req) => match self.client().matrixark_batch_append_records_request(req) {
                        Ok(appended_records) => json_response(
                            200,
                            &serde_json::json!({
                                "status": Status::ok(),
                                "appended_records": appended_records,
                            }),
                        ),
                        Err(err) => json_response(
                            400,
                            &Status::error("matrixark_batch_append_error", err.to_string()),
                        ),
                    },
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/Get") | ("POST", "/Get") => {
                match parse_json::<ProxyKeyCommandRequest>(&request.body) {
                    Ok(req) => json_response(
                        200,
                        &self.table_command(req, |key| Command::StringGet { key }),
                    ),
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/Set") | ("POST", "/Set") => {
                match parse_json::<ProxySetCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::StringSet {
                            key: req.key,
                            value: req.value,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/SetEx") => {
                match parse_json::<ProxySetExCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::StringSetEx {
                            key: req.key,
                            value: req.value,
                            ttl_ms: req.ttl_ms,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/StringSetConditional") => {
                match parse_json::<ProxySetConditionalCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::StringSetConditional {
                            key: req.key,
                            value: req.value,
                            ttl_ms: req.ttl_ms,
                            condition: req.condition,
                            return_old: req.return_old,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/Delete") | ("POST", "/ProxyService/CommonDelete") => {
                match parse_json::<ProxyKeyCommandRequest>(&request.body) {
                    Ok(req) => json_response(
                        200,
                        &self.table_command(req, |key| Command::CommonDelete { key }),
                    ),
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/Expire") => {
                match parse_json::<ProxyExpireCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::CommonExpire {
                            key: req.key,
                            ttl_ms: req.ttl_ms,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/Ttl") => {
                match parse_json::<ProxyKeyCommandRequest>(&request.body) {
                    Ok(req) => json_response(
                        200,
                        &self.table_command(req, |key| Command::CommonTtl { key }),
                    ),
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/Exists") => {
                match parse_json::<ProxyKeyCommandRequest>(&request.body) {
                    Ok(req) => json_response(
                        200,
                        &self.table_command(req, |key| Command::CommonExists { key }),
                    ),
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/FeatureAdd")
            | ("POST", "/FeatureAdd")
            | ("POST", "/ProxyService/FeatureAppendWithPolicy") => {
                match parse_json::<ProxyFeatureAddCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = if let Some(policy) = req.policy {
                            Command::FeatureAppendWithPolicy {
                                key: req.key,
                                points: req.points,
                                policy,
                            }
                        } else {
                            Command::FeatureAppend {
                                key: req.key,
                                points: req.points,
                            }
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/FeatureQuery")
            | ("POST", "/FeatureQuery")
            | ("POST", "/ProxyService/FeatureQueryFiltered") => {
                match parse_json::<ProxyFeatureQueryCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = if req.filters.is_empty() {
                            Command::FeatureQuery {
                                key: req.key,
                                start_ms: req.start_ms,
                                end_ms: req.end_ms,
                                count: req.count,
                            }
                        } else {
                            Command::FeatureQueryFiltered {
                                key: req.key,
                                start_ms: req.start_ms,
                                end_ms: req.end_ms,
                                count: req.count,
                                filters: req.filters,
                            }
                        };
                        let fields = feature_query_fields(&req.fields);
                        let response = self.table_execute(ProxyTableExecuteRequest {
                            namespace: req.namespace,
                            table_name: req.table_name,
                            command,
                        });
                        json_response(
                            200,
                            &feature_points_response_json_with_fields(response, &fields),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/FeatureReplace") => {
                match parse_json::<ProxyFeatureReplaceCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::FeatureReplace {
                            key: req.key,
                            start_ms: req.start_ms,
                            end_ms: req.end_ms,
                            points: req.points,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/FeatureDelete") => {
                match parse_json::<ProxyKeyCommandRequest>(&request.body) {
                    Ok(req) => json_response(
                        200,
                        &self.table_command(req, |key| Command::FeatureDelete { key }),
                    ),
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/FeatureAggQuery") => {
                match parse_json::<ProxyFeatureAggQueryCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::FeatureAggQuery {
                            key: req.key,
                            start_ms: req.start_ms,
                            end_ms: req.end_ms,
                            aggregator: req.aggregator,
                            count: req.count,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/SequenceAdd")
            | ("POST", "/ProxyService/SequenceAddWithPolicy") => {
                match parse_json::<ProxySequenceAddCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = if let Some(policy) = req.policy {
                            Command::SequenceAddWithPolicy {
                                key: req.key,
                                rows: req.rows,
                                policy,
                            }
                        } else {
                            Command::SequenceAdd {
                                key: req.key,
                                rows: req.rows,
                            }
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/SequenceQuery") => {
                match parse_json::<ProxySequenceQueryCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::SequenceQuery {
                            key: req.key,
                            start_ms: req.start_ms,
                            end_ms: req.end_ms,
                            count: req.count,
                            filters: req.filters,
                        };
                        let response = self.table_execute(ProxyTableExecuteRequest {
                            namespace: req.namespace,
                            table_name: req.table_name,
                            command,
                        });
                        json_response(200, &sequence_rows_response_json(response))
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/SequenceBatchQuery") => {
                match parse_json::<ProxySequenceBatchQueryCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::SequenceBatchQuery {
                            queries: req.queries,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/IpsAdd") | ("POST", "/ProxyService/IpsAddWithOptions") => {
                match parse_json::<ProxyIpsAddCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::IpsAddWithOptions {
                            key: req.key,
                            timestamp_ms: req.timestamp_ms,
                            instance: req.instance,
                            action_type: req.action_type,
                            table_id: req.table_id,
                            request_id: req.request_id,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/IpsQueryLast") => {
                match parse_json::<ProxyIpsQueryLastCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::IpsQueryLast {
                            key: req.key,
                            count: req.count,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/IpsQuery") | ("POST", "/ProxyService/IpsQueryWithOptions") => {
                match parse_json::<ProxyIpsRangeCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::IpsQueryRangeWithOptions {
                            key: req.key,
                            start_ms: req.start_ms,
                            end_ms: req.end_ms,
                            count: req.count,
                            action_type: req.action_type,
                            table_id: req.table_id,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/IpsBatchQueryLast") => {
                match parse_json::<ProxyIpsBatchQueryLastCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::IpsBatchQueryLast {
                            keys: req.keys,
                            count: req.count,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/IpsStat") => {
                match parse_json::<ProxyIpsRangeCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::IpsStat {
                            key: req.key,
                            start_ms: req.start_ms,
                            end_ms: req.end_ms,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/IpsFilter") => {
                match parse_json::<ProxyIpsRangeCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::IpsFilter {
                            key: req.key,
                            start_ms: req.start_ms,
                            end_ms: req.end_ms,
                            count: req.count,
                            action_type: req.action_type,
                            table_id: req.table_id,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/IpsSnapshot") => {
                match parse_json::<ProxyIpsRangeCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::IpsSnapshot {
                            key: req.key,
                            start_ms: req.start_ms,
                            end_ms: req.end_ms,
                            count: req.count,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/IpsSnapshotReport") => {
                match parse_json::<ProxyIpsRangeCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::IpsSnapshotReport {
                            key: req.key,
                            start_ms: req.start_ms,
                            end_ms: req.end_ms,
                            count: req.count,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/IpsLoad") => {
                match parse_json::<ProxyIpsLoadCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::IpsLoad {
                            key: req.key,
                            points: req.points,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/IpsRemove") => {
                match parse_json::<ProxyIpsRemoveCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::IpsRemove {
                            key: req.key,
                            timestamp_ms: req.timestamp_ms,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/IpsDelete") => {
                match parse_json::<ProxyKeyCommandRequest>(&request.body) {
                    Ok(req) => json_response(
                        200,
                        &self.table_command(req, |key| Command::IpsDelete { key }),
                    ),
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/IpsCount") => {
                match parse_json::<ProxyIpsRangeCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::IpsCount {
                            key: req.key,
                            start_ms: req.start_ms,
                            end_ms: req.end_ms,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/RiskIncrement")
            | ("POST", "/ProxyService/RiskIncrementWithOptions") => {
                match parse_json::<ProxyRiskIncrementCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = if req.precision_ms.is_some() || req.ttl_ms.is_some() {
                            Command::RiskIncrementWithOptions {
                                key: req.key,
                                timestamp_ms: req.timestamp_ms,
                                amount: req.amount,
                                precision_ms: req.precision_ms,
                                ttl_ms: req.ttl_ms,
                            }
                        } else {
                            Command::RiskIncrement {
                                key: req.key,
                                timestamp_ms: req.timestamp_ms,
                                amount: req.amount,
                            }
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/RiskCount") => {
                match parse_json::<ProxyRiskCountCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::RiskCount {
                            key: req.key,
                            start_ms: req.start_ms,
                            end_ms: req.end_ms,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/RiskQuery") => {
                match parse_json::<ProxyRiskQueryCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::RiskQuery {
                            key: req.key,
                            start_ms: req.start_ms,
                            end_ms: req.end_ms,
                            aggregator: req.aggregator,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/RiskDetail") => {
                match parse_json::<ProxyRiskQueryCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::RiskDetail {
                            key: req.key,
                            start_ms: req.start_ms,
                            end_ms: req.end_ms,
                            count: req.count,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/RiskChangeAdd") => {
                match parse_json::<ProxyRiskChangeAddCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::RiskChangeAdd {
                            key: req.key,
                            timestamp_ms: req.timestamp_ms,
                            value: req.value,
                            precision_ms: req.precision_ms,
                            ttl_ms: req.ttl_ms,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/RiskFamilySet") => {
                match parse_json::<ProxyRiskFamilySetCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::RiskSet {
                            family: req.family,
                            key: req.key,
                            timestamp_ms: req.timestamp_ms,
                            amount: req.amount,
                            precision_ms: None,
                            ttl_ms: None,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/RiskFamilyQuery") => {
                match parse_json::<ProxyRiskFamilyQueryCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::RiskFamilyQuery {
                            family: req.family,
                            key: req.key,
                            start_ms: req.start_ms,
                            end_ms: req.end_ms,
                            aggregator: req.aggregator,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/RiskFamilySetAndGet") => {
                match parse_json::<ProxyRiskFamilySetAndGetCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::RiskSetAndGet {
                            family: req.family,
                            key: req.key,
                            timestamp_ms: req.timestamp_ms,
                            amount: req.amount,
                            start_ms: req.start_ms,
                            end_ms: req.end_ms,
                            aggregator: req.aggregator,
                            precision_ms: None,
                            ttl_ms: None,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/RiskHset") | ("POST", "/RiskHset") => {
                match parse_json::<ProxyRiskHsetCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::RiskSet {
                            family: crate::types::RiskFamily::H,
                            key: req.key.clone(),
                            timestamp_ms: risk_hset_timestamp_ms(&req),
                            amount: risk_hset_amount(&req),
                            precision_ms: req.precision_ms,
                            ttl_ms: risk_hset_ttl_ms(req.ttl_ms),
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/RiskHquery") | ("POST", "/RiskHquery") => {
                match parse_json::<ProxyRiskHqueryCommandRequest>(&request.body) {
                    Ok(req) => json_response(200, &self.risk_hquery(req)),
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/RiskCPCSet") | ("POST", "/RiskCPCSet") => {
                match parse_json::<ProxyRiskCpcSetCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::RiskSet {
                            family: RiskFamily::Cpc,
                            key: req.key,
                            timestamp_ms: risk_cpc_timestamp_ms(req.timestamp_ms),
                            amount: req.values.len() as i64,
                            precision_ms: req.precision_ms,
                            ttl_ms: risk_hset_ttl_ms(req.ttl_ms),
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/RiskCPCQuery") | ("POST", "/RiskCPCQuery") => {
                match parse_json::<ProxyRiskCpcQueryCommandRequest>(&request.body) {
                    Ok(req) => json_response(200, &self.risk_cpc_query(req)),
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/RiskFolSet") | ("POST", "/RiskFolSet") => {
                match parse_json::<ProxyRiskFolSetCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::RiskFolSet {
                            key: req.key,
                            value: risk_fol_value_bytes(&req.value),
                            occur_time_ms: risk_fol_time_ms(
                                req.occur_time_ms,
                                req.occur_time_seconds,
                            ),
                            ttl_ms: risk_fol_ttl_ms(req.ttl_ms, req.ttl_seconds),
                            fol_type: req.fol_type,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/RiskFolQuery") | ("POST", "/RiskFolQuery") => {
                match parse_json::<ProxyKeyCommandRequest>(&request.body) {
                    Ok(req) => {
                        let response = self.table_command(req, |key| Command::RiskFolQuery { key });
                        json_response(200, &risk_fol_query_response_json(response))
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/RiskManager") | ("POST", "/RiskManager") => {
                match parse_json::<ProxyRiskManagerCommandRequest>(&request.body) {
                    Ok(req) => {
                        let response = self.table_execute(ProxyTableExecuteRequest {
                            namespace: req.namespace,
                            table_name: req.table_name,
                            command: Command::RiskManager {
                                key: req.key,
                                op_type: risk_manager_op_type(req.op_type.as_ref()),
                                field_list: req
                                    .field_list
                                    .into_iter()
                                    .map(|item| (item.key, item.value))
                                    .collect(),
                                start_offset: req.start_offset,
                                end_offset: req.end_offset,
                                is_cpc: req.is_cpc,
                            },
                        });
                        json_response(200, &hash_entries_response_json(response, true))
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/RiskDebug") => {
                match parse_json::<ProxyRiskCountCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::RiskDebug {
                            key: req.key,
                            start_ms: req.start_ms,
                            end_ms: req.end_ms,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/ContextNodeUpsert")
            | ("POST", "/ProxyService/ContextUpsertNode") => {
                proxy_command_route!(ProxyContextUpsertNodeCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextUpsertNode {
                            tenant_hash: req.tenant_hash,
                            node: req.node,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/ContextNodeGet") | ("POST", "/ProxyService/ContextGetNode") => {
                proxy_command_route!(ProxyContextGetNodeCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextGetNode {
                            tenant_hash: req.tenant_hash,
                            node_hash: req.node_hash,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/ContextNodesGet")
            | ("POST", "/ProxyService/ContextGetNodes") => {
                proxy_command_route!(ProxyContextGetNodesCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextGetNodes {
                            tenant_hash: req.tenant_hash,
                            node_hashes: req.node_hashes,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/ContextWriteEvent") => {
                proxy_command_route!(ProxyContextWriteEventCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextWriteEvent {
                            tenant_hash: req.tenant_hash,
                            node_hash: req.node_hash,
                            event: req.event,
                            first_write_only: req.first_write_only,
                            cold_storage: req.cold_storage,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/ContextWriteExtractedEvent") => {
                proxy_command_route!(ProxyContextWriteExtractedEventCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextWriteExtractedEvent {
                            tenant_hash: req.tenant_hash,
                            node_hash: req.node_hash,
                            event: req.event,
                            indexes: req.indexes,
                            first_write_only: req.first_write_only,
                            cold_storage: req.cold_storage,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/ContextQueryEvents") => {
                proxy_command_route!(ProxyContextQueryEventsCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextQueryEvents {
                            tenant_hash: req.tenant_hash,
                            node_hash: req.node_hash,
                            start_time_ms: req.start_time_ms,
                            end_time_ms: req.end_time_ms,
                            limit: req.limit,
                            current_valid_only: req.current_valid_only,
                            as_of_ms: req.as_of_ms,
                            kinds: req.kinds,
                            statuses: req.statuses,
                            min_confidence: req.min_confidence,
                            min_importance: req.min_importance,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/ContextWriteIndexRef") => {
                proxy_command_route!(ProxyContextWriteIndexRefCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextWriteIndexRef {
                            tenant_hash: req.tenant_hash,
                            index_name: req.index_name,
                            index_value_hash: req.index_value_hash,
                            scope_hash: req.scope_hash,
                            event_time_ms: req.event_time_ms,
                            index_ref: req.index_ref,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/ContextQueryIndex") => {
                proxy_command_route!(ProxyContextQueryIndexCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextQueryIndex {
                            tenant_hash: req.tenant_hash,
                            index_name: req.index_name,
                            index_value_hash: req.index_value_hash,
                            scope_hash: req.scope_hash,
                            start_time_ms: req.start_time_ms,
                            end_time_ms: req.end_time_ms,
                            limit: req.limit,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/ContextQueryIndexIntersection") => {
                proxy_command_route!(ProxyContextQueryIndexIntersectionCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextQueryIndexIntersection {
                            tenant_hash: req.tenant_hash,
                            predicates: req.predicates,
                            limit: req.limit,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/ContextWritePackAudit") => {
                proxy_command_route!(ProxyContextWritePackAuditCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextWritePackAudit {
                            tenant_hash: req.tenant_hash,
                            audit: req.audit,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/ContextQueryPackAudit") => {
                proxy_command_route!(ProxyContextQueryPackAuditCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextQueryPackAudit {
                            tenant_hash: req.tenant_hash,
                            session_hash: req.session_hash,
                            start_time_ms: req.start_time_ms,
                            end_time_ms: req.end_time_ms,
                            limit: req.limit,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/ContextMarkSummaryDirty") => {
                proxy_command_route!(ProxyContextMarkSummaryDirtyCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextMarkSummaryDirty {
                            tenant_hash: req.tenant_hash,
                            marker: req.marker,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/ContextQuerySummaryDirty") => {
                proxy_command_route!(ProxyContextQuerySummaryDirtyCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextQuerySummaryDirty {
                            tenant_hash: req.tenant_hash,
                            node_hash: req.node_hash,
                            start_time_ms: req.start_time_ms,
                            end_time_ms: req.end_time_ms,
                            limit: req.limit,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/ContextUpsertEntity") => {
                proxy_command_route!(ProxyContextUpsertEntityCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextUpsertEntity {
                            tenant_hash: req.tenant_hash,
                            entity: req.entity,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/ContextGetEntity") => {
                proxy_command_route!(ProxyContextGetEntityCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextGetEntity {
                            tenant_hash: req.tenant_hash,
                            node_hash: req.node_hash,
                            entity_hash: req.entity_hash,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/ContextQueryEntities") => {
                proxy_command_route!(ProxyContextQueryEntitiesCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextQueryEntities {
                            tenant_hash: req.tenant_hash,
                            node_hash: req.node_hash,
                            entity_hashes: req.entity_hashes,
                            limit: req.limit,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/ContextUpsertChildRef") => {
                proxy_command_route!(ProxyContextUpsertChildRefCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextUpsertChildRef {
                            tenant_hash: req.tenant_hash,
                            child_ref: req.child_ref,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/ContextQueryChildren") => {
                proxy_command_route!(ProxyContextQueryChildrenCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextQueryChildren {
                            tenant_hash: req.tenant_hash,
                            parent_hash: req.parent_hash,
                            limit: req.limit,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/ContextUpsertEmbedding") => {
                proxy_command_route!(ProxyContextUpsertEmbeddingCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextUpsertEmbedding {
                            tenant_hash: req.tenant_hash,
                            embedding: req.embedding,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/ContextQueryEmbeddings") => {
                proxy_command_route!(ProxyContextQueryEmbeddingsCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextQueryEmbeddings {
                            tenant_hash: req.tenant_hash,
                            ref_hashes: req.ref_hashes,
                            limit: req.limit,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/ContextTraverseTree") => {
                proxy_command_route!(ProxyContextTraverseTreeCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextTraverseTree {
                            tenant_hash: req.tenant_hash,
                            start_node_hash: req.start_node_hash,
                            query_vector: req.query_vector,
                            max_depth: req.max_depth,
                            top_k_per_depth: req.top_k_per_depth,
                            max_children_scored_per_parent: req.max_children_scored_per_parent,
                            max_candidate_nodes: req.max_candidate_nodes,
                            leaf_only: req.leaf_only,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/ContextUpsertSummary") => {
                proxy_command_route!(ProxyContextUpsertSummaryCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextUpsertSummary {
                            tenant_hash: req.tenant_hash,
                            summary: req.summary,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/ContextQuerySummaries") => {
                proxy_command_route!(ProxyContextQuerySummariesCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextQuerySummaries {
                            tenant_hash: req.tenant_hash,
                            node_hash: req.node_hash,
                            level: req.level,
                            as_of_ms: req.as_of_ms,
                            limit: req.limit,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/ContextWriteCompressionEvent") => {
                proxy_command_route!(ProxyContextWriteCompressionEventCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextWriteCompressionEvent {
                            tenant_hash: req.tenant_hash,
                            event: req.event,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/ContextQueryCompressionEvents") => {
                proxy_command_route!(ProxyContextQueryCompressionEventsCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextQueryCompressionEvents {
                            tenant_hash: req.tenant_hash,
                            node_hashes: req.node_hashes,
                            start_time_ms: req.start_time_ms,
                            end_time_ms: req.end_time_ms,
                            limit: req.limit,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/ContextCompressEvents") => {
                proxy_command_route!(ProxyContextCompressEventsCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextCompressEvents {
                            tenant_hash: req.tenant_hash,
                            node_hash: req.node_hash,
                            source_start_ms: req.source_start_ms,
                            source_end_ms: req.source_end_ms,
                            compressed_time_ms: req.compressed_time_ms,
                            max_source_events: req.max_source_events,
                            min_confidence: req.min_confidence,
                            min_importance: req.min_importance,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/ContextQueryNodeContext") => {
                proxy_command_route!(ProxyContextQueryNodeContextCommandRequest, |req| {
                    (
                        req.namespace,
                        req.table_name,
                        Command::ContextQueryNodeContext {
                            tenant_hash: req.tenant_hash,
                            node_hash: req.node_hash,
                            summary_level: req.summary_level,
                            as_of_ms: req.as_of_ms,
                            cold_start_time_ms: req.cold_start_time_ms,
                            cold_end_time_ms: req.cold_end_time_ms,
                            compression_limit: req.compression_limit,
                        },
                    )
                })
            }
            ("POST", "/ProxyService/MatrixArkRetrieveContextPack")
            | ("POST", "/matrixark/retrieve_context_pack") => {
                match parse_json::<MatrixArkRetrieveContextPackRequest>(&request.body) {
                    Ok(req) => match self
                        .client()
                        .matrixark_retrieve_context_pack_native_json(req)
                    {
                        Ok(pack) => json_response(200, &pack),
                        Err(err) => json_response(
                            400,
                            &Status::error("context_pack_error", err.to_string()),
                        ),
                    },
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/MatrixArkScanCandidates")
            | ("POST", "/matrixark/scan_candidates") => {
                match parse_json::<MatrixArkRetrieveContextPackRequest>(&request.body) {
                    Ok(req) => match self.client().matrixark_scan_candidates_native_json(req) {
                        Ok(candidates) => json_response(200, &candidates),
                        Err(err) => json_response(
                            400,
                            &Status::error("scan_candidates_error", err.to_string()),
                        ),
                    },
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/HGet") => {
                match parse_json::<ProxyHashCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::HashGet {
                            key: req.key,
                            field: req.field,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/HSet") => {
                match parse_json::<ProxyHashSetCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::HashSet {
                            key: req.key,
                            field: req.field,
                            value: req.value,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/HIncrBy") | ("POST", "/ProxyService/HashIncrBy") => {
                match parse_json::<ProxyHashIncrByCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::HashIncrBy {
                            key: req.key,
                            field: req.field,
                            increment: req.increment,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/HDel") => {
                match parse_json::<ProxyHashCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::HashDelete {
                            key: req.key,
                            field: req.field,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/HMGet") | ("POST", "/HMGet") => {
                match parse_json::<ProxyHashMultiGetCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::HashMultiGet {
                            key: req.key,
                            fields: req.fields,
                        };
                        let response = self.table_execute(ProxyTableExecuteRequest {
                            namespace: req.namespace,
                            table_name: req.table_name,
                            command,
                        });
                        json_response(200, &hmget_response_json(response))
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/HMSet") | ("POST", "/HMSet") => {
                match parse_json::<ProxyHashMultiSetCommandRequest>(&request.body) {
                    Ok(req) => {
                        let entries = match req.entries() {
                            Ok(entries) => entries,
                            Err(status) => {
                                return json_response(
                                    400,
                                    &execute_error(status.code, status.message),
                                )
                            }
                        };
                        let command = Command::HashMultiSet {
                            key: req.key,
                            entries,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/HGetAll") | ("POST", "/HGetAll") => {
                match parse_json::<ProxyKeyCommandRequest>(&request.body) {
                    Ok(req) => {
                        let response = self.table_command(req, |key| Command::HashGetAll { key });
                        json_response(200, &hash_entries_response_json(response, false))
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/HLen") | ("POST", "/HLen") => {
                match parse_json::<ProxyKeyCommandRequest>(&request.body) {
                    Ok(req) => {
                        let response = self.table_command(req, |key| Command::HashLen { key });
                        json_response(200, &integer_response_json(response, "len"))
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/SAdd") => {
                match parse_json::<ProxySetMemberCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::SetAdd {
                            key: req.key,
                            member: req.member,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/SMembers") => {
                match parse_json::<ProxyKeyCommandRequest>(&request.body) {
                    Ok(req) => json_response(
                        200,
                        &self.table_command(req, |key| Command::SetMembers { key }),
                    ),
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/SRem") => {
                match parse_json::<ProxySetMemberCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::SetRemove {
                            key: req.key,
                            member: req.member,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
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

    fn risk_hquery(&self, request: ProxyRiskHqueryCommandRequest) -> serde_json::Value {
        let bounds = risk_hquery_bounds(&request);
        let aggregator = request
            .aggregator
            .clone()
            .unwrap_or_else(|| request.htype.aggregator().to_string());
        let mut result_list = Vec::with_capacity(bounds.len());
        let mut responses = Vec::with_capacity(bounds.len());
        let mut status = Status::ok();
        for (start_ms, end_ms) in bounds {
            let response = self.table_execute(ProxyTableExecuteRequest {
                namespace: request.namespace.clone(),
                table_name: request.table_name.clone(),
                command: Command::RiskFamilyQuery {
                    family: RiskFamily::H,
                    key: request.key.clone(),
                    start_ms,
                    end_ms,
                    aggregator: aggregator.clone(),
                },
            });
            if !response.status.ok && status.ok {
                status = response.status.clone();
            }
            let result = match &response.response {
                CommandResponse::Integer { value } => *value,
                _ => 0,
            };
            result_list.push(serde_json::json!({
                "has_result": response.status.ok,
                "result": result,
            }));
            responses.push(response);
        }
        serde_json::json!({
            "status": status,
            "result_list": result_list,
            "responses": responses,
        })
    }

    fn risk_cpc_query(&self, request: ProxyRiskCpcQueryCommandRequest) -> serde_json::Value {
        let bounds = risk_cpc_query_bounds(&request);
        let mut count_list = Vec::with_capacity(bounds.len());
        let mut responses = Vec::with_capacity(bounds.len());
        let mut status = Status::ok();
        for (start_ms, end_ms) in bounds {
            let response = self.table_execute(ProxyTableExecuteRequest {
                namespace: request.namespace.clone(),
                table_name: request.table_name.clone(),
                command: Command::RiskFamilyQuery {
                    family: RiskFamily::Cpc,
                    key: request.key.clone(),
                    start_ms,
                    end_ms,
                    aggregator: request.aggregator.clone(),
                },
            });
            if !response.status.ok && status.ok {
                status = response.status.clone();
            }
            let count = match &response.response {
                CommandResponse::Integer { value } => *value,
                _ => 0,
            };
            count_list.push(count);
            responses.push(response);
        }
        let detail_lists: Vec<serde_json::Value> = if request.with_detail {
            count_list
                .iter()
                .map(|count| serde_json::json!({ "detail": [count.to_string()] }))
                .collect()
        } else {
            Vec::new()
        };
        serde_json::json!({
            "status": status,
            "count_list": count_list,
            "detail_lists": detail_lists,
            "responses": responses,
        })
    }

    fn table_execute_command(
        &self,
        namespace: String,
        table_name: String,
        command: Command,
    ) -> ExecuteResponse {
        self.table_execute(ProxyTableExecuteRequest {
            namespace,
            table_name,
            command,
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
            service_discovery,
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

    pub fn tonic_streaming_contract(&self) -> ProxyTonicStreamingContract {
        ProxyTonicStreamingContract::default()
    }

    pub fn metrics_parity_report(&self) -> ProxyMetricsParityReport {
        ProxyMetricsParityReport {
            status: Status::ok(),
            compared_cpp_files: vec![
                "<repo>/src/common/metrics.h".to_string(),
                "<repo>/src/common/metrics.cc".to_string(),
                "<repo>/src/proxy/heartbeat.cc".to_string(),
                "<repo>/src/proxy/service.cc".to_string(),
                "<repo>/src/proxy/flags.cc".to_string(),
            ],
            rust_prometheus_families: proxy_metrics_families()
                .into_iter()
                .map(str::to_string)
                .collect(),
            mappings: proxy_metrics_parity_mappings(),
            grafana_panels_ready: true,
            alerts_ready: true,
        }
    }

    pub fn cpp_migration_contract(&self) -> ProxyCppMigrationContract {
        ProxyCppMigrationContract::default()
    }

    pub fn service_discovery_report(&self) -> ProxyServiceDiscoveryReport {
        let options = self.options();
        self.service_discovery_report_with_options(&options)
    }

    pub fn ports_report(&self) -> ProxyPortsReport {
        let options = self.options();
        let listen_port = proxy_addr_port(&options.proxy_addr);
        ProxyPortsReport {
            listen_addr: options.proxy_addr.clone(),
            announce_addr: options.proxy_addr,
            listen_port,
            announce_port: listen_port,
        }
    }

    pub fn consul_names_report(&self) -> ProxyConsulNamesReport {
        let options = self.options();
        ProxyConsulNamesReport {
            legacy_consul_in_scope: false,
            rust_service_registry_names: self.rust_service_registry_names_with_options(&options),
            namespace: options.namespace,
            location: options.location,
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
            compared_cpp_files: vec![
                "<repo>/src/proxy/proxy.h".to_string(),
                "<repo>/src/proxy/proxy.cc".to_string(),
                "<repo>/src/proxy/heartbeat.h".to_string(),
                "<repo>/src/proxy/heartbeat.cc".to_string(),
                "<repo>/src/proxy/service.h".to_string(),
                "<repo>/src/proxy/service.cc".to_string(),
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
                    "Applies C++-style duplicate config no-op and rebuilds the Rust client only when the effective config changes.",
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
                    "Bcache2ThriftService command dispatch",
                    "/proxy/cpp_migration_contract",
                    "/ProxyService/GetCppMigrationContract",
                    "Legacy brpc/thrift remains out of scope; Rust-native HTTP/JSON, RESP, and tonic are the migration contract.",
                ),
                proxy_operational_surface_entry(
                    "Bcache2ThriftService admission/inflight checks",
                    "/proxy/policy",
                    "/ProxyService/GetPolicy",
                    "Rust policy covers serving mode, write-disabled/readonly rejection, drop-percent admission, and rejection counters.",
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

    fn service_discovery_report_with_options(
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

    fn rust_service_registry_names_with_options(&self, options: &ProxyOptions) -> Vec<String> {
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

    pub fn prometheus_metrics(&self) -> String {
        self.sync_client_stats();
        let options = self.options();
        let stats = *self.inner.stats.read().expect("proxy stats lock poisoned");
        let client = self.client().stats();
        let mut out = String::new();
        out.push_str("# HELP temporalstore_proxy_requests_total Proxy request counters by kind.\n");
        out.push_str("# TYPE temporalstore_proxy_requests_total counter\n");
        for (kind, value) in [
            ("execute", stats.execute_requests),
            ("batch_execute", stats.batch_execute_requests),
            ("bad_request", stats.bad_requests),
            ("admission_rejection", stats.admission_rejections),
            ("heartbeat", stats.heartbeat_total),
            ("auto_register", stats.auto_register_total),
        ] {
            push_proxy_metric(
                &mut out,
                "temporalstore_proxy_requests_total",
                &[("kind", kind)],
                value,
            );
        }

        out.push_str(
            "# HELP temporalstore_proxy_route_cache_entries Current proxy route cache entries.\n",
        );
        out.push_str("# TYPE temporalstore_proxy_route_cache_entries gauge\n");
        push_proxy_metric(
            &mut out,
            "temporalstore_proxy_route_cache_entries",
            &[],
            self.client().route_cache_size() as u64,
        );

        out.push_str("# HELP temporalstore_proxy_route_cache_events_total Proxy route cache and refresh events.\n");
        out.push_str("# TYPE temporalstore_proxy_route_cache_events_total counter\n");
        for (kind, value) in [
            ("hit", stats.route_cache_hits),
            ("miss", stats.route_cache_misses),
            ("refresh", stats.route_refreshes),
        ] {
            push_proxy_metric(
                &mut out,
                "temporalstore_proxy_route_cache_events_total",
                &[("kind", kind)],
                value,
            );
        }

        out.push_str("# HELP temporalstore_proxy_backend_events_total Proxy backend and metaserver failure counters.\n");
        out.push_str("# TYPE temporalstore_proxy_backend_events_total counter\n");
        for (kind, value) in [
            ("backend_error", stats.backend_errors),
            (
                "continuous_backend_failure",
                stats.continuous_backend_failures,
            ),
            ("metaserver_error", stats.metaserver_errors),
            ("client_meta_sync_error", client.meta_sync_errors),
        ] {
            push_proxy_metric(
                &mut out,
                "temporalstore_proxy_backend_events_total",
                &[("kind", kind)],
                value,
            );
        }

        out.push_str("# HELP temporalstore_proxy_serving_mode Current proxy serving mode as a one-hot gauge.\n");
        out.push_str("# TYPE temporalstore_proxy_serving_mode gauge\n");
        for mode in [
            ProxyServingMode::Serving,
            ProxyServingMode::Readonly,
            ProxyServingMode::WriteDisabled,
            ProxyServingMode::Degraded,
            ProxyServingMode::NotServing,
        ] {
            push_proxy_metric(
                &mut out,
                "temporalstore_proxy_serving_mode",
                &[("mode", proxy_serving_mode_label(mode))],
                u64::from(options.serving_mode == mode),
            );
        }

        out.push_str("# HELP temporalstore_proxy_drop_percent Current proxy deterministic drop percentage.\n");
        out.push_str("# TYPE temporalstore_proxy_drop_percent gauge\n");
        push_proxy_metric(
            &mut out,
            "temporalstore_proxy_drop_percent",
            &[],
            options.drop_percent as u64,
        );

        out.push_str("# HELP temporalstore_proxy_metric_family_parity C++ proxy operational metric surface mapped to Rust Prometheus families.\n");
        out.push_str("# TYPE temporalstore_proxy_metric_family_parity gauge\n");
        for mapping in proxy_metrics_parity_mappings() {
            push_proxy_metric(
                &mut out,
                "temporalstore_proxy_metric_family_parity",
                &[
                    ("cpp_surface", mapping.cpp_surface.as_str()),
                    ("rust_family", mapping.rust_prometheus_family.as_str()),
                    ("grafana_panel", mapping.grafana_panel.as_str()),
                ],
                u64::from(mapping.covered),
            );
        }

        let service_discovery = self.service_discovery_report_with_options(&options);
        out.push_str("# HELP temporalstore_proxy_service_registry_state Proxy service-discovery registration state.\n");
        out.push_str("# TYPE temporalstore_proxy_service_registry_state gauge\n");
        push_proxy_metric(
            &mut out,
            "temporalstore_proxy_service_registry_state",
            &[("state", "registered")],
            u64::from(service_discovery.registered),
        );
        push_proxy_metric(
            &mut out,
            "temporalstore_proxy_service_registry_state",
            &[("state", "stale")],
            u64::from(service_discovery.stale),
        );

        out.push_str("# HELP temporalstore_proxy_service_registry_events_total Proxy service-discovery heartbeat and registration events.\n");
        out.push_str("# TYPE temporalstore_proxy_service_registry_events_total counter\n");
        for (kind, value) in [
            (
                "heartbeat_success",
                service_discovery.stats.heartbeat_success_total,
            ),
            (
                "heartbeat_failure",
                service_discovery.stats.heartbeat_failure_total,
            ),
            (
                "registration_success",
                service_discovery.stats.registration_success_total,
            ),
            (
                "registration_failure",
                service_discovery.stats.registration_failure_total,
            ),
        ] {
            push_proxy_metric(
                &mut out,
                "temporalstore_proxy_service_registry_events_total",
                &[("kind", kind)],
                value,
            );
        }

        let readiness = crate::production_readiness_report();
        out.push_str(
            "# HELP temporalstore_production_readiness_ready Production readiness gate state.\n",
        );
        out.push_str("# TYPE temporalstore_production_readiness_ready gauge\n");
        push_proxy_metric(
            &mut out,
            "temporalstore_production_readiness_ready",
            &[],
            u64::from(readiness.production_ready),
        );

        out.push_str(
            "# HELP temporalstore_production_readiness_blockers Production readiness blockers.\n",
        );
        out.push_str("# TYPE temporalstore_production_readiness_blockers gauge\n");
        push_proxy_metric(
            &mut out,
            "temporalstore_production_readiness_blockers",
            &[],
            readiness.blocker_count as u64,
        );
        for area in &readiness.areas {
            push_proxy_metric(
                &mut out,
                "temporalstore_production_readiness_blockers",
                &[("area", area.area.as_str())],
                area.missing.len() as u64,
            );
        }
        out.push_str(
            "# HELP temporalstore_production_readiness_service_ready Production readiness by service.\n",
        );
        out.push_str("# TYPE temporalstore_production_readiness_service_ready gauge\n");
        out.push_str(
            "# HELP temporalstore_production_readiness_service_blockers Production readiness blockers by service.\n",
        );
        out.push_str("# TYPE temporalstore_production_readiness_service_blockers gauge\n");
        for service in &readiness.service_summaries {
            push_proxy_metric(
                &mut out,
                "temporalstore_production_readiness_service_ready",
                &[("service", service.service.as_str())],
                u64::from(service.ready),
            );
            push_proxy_metric(
                &mut out,
                "temporalstore_production_readiness_service_blockers",
                &[("service", service.service.as_str())],
                service.blocker_count as u64,
            );
        }
        out
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
                self.record_service_discovery_heartbeat(&response.status);
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
                        self.record_service_discovery_heartbeat(&response.status);
                        self.apply_heartbeat_config(&response);
                    } else {
                        self.record_service_discovery_error(&response.status);
                    }
                    response
                } else {
                    self.record_service_discovery_error(&response.status);
                    response
                }
            }
            Ok(response) => {
                self.record_service_discovery_error(&response.status);
                response
            }
            Err(err) => {
                let status = Status::error("metaserver_error", err.to_string());
                self.record_service_discovery_error(&status);
                ProxyHeartbeatResponse {
                    status,
                    config_changed: false,
                    namespace: String::new(),
                    config_version: 0,
                    serving_mode: "not_serving".to_string(),
                    drop_percent: 0,
                }
            }
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
        let response = post_json_with_options::<_, AckResponse>(
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
        });
        self.record_service_discovery_registration(&response.status);
        response
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

    fn record_service_discovery_heartbeat(&self, status: &Status) {
        let mut state = self
            .inner
            .service_discovery
            .write()
            .expect("proxy service discovery lock poisoned");
        if status.ok || status.code == "resource_frozen" {
            state.registered = true;
            state.last_success_ms = Some(now_ms());
            state.last_error = None;
            state.stats.heartbeat_success_total += 1;
        } else {
            state.last_error_ms = Some(now_ms());
            state.last_error = Some(status.clone());
            state.stats.heartbeat_failure_total += 1;
        }
    }

    fn record_service_discovery_registration(&self, status: &Status) {
        let mut state = self
            .inner
            .service_discovery
            .write()
            .expect("proxy service discovery lock poisoned");
        if status.ok {
            state.registered = true;
            state.last_success_ms = Some(now_ms());
            state.last_error = None;
            state.stats.registration_success_total += 1;
        } else {
            state.registered = false;
            state.last_error_ms = Some(now_ms());
            state.last_error = Some(status.clone());
            state.stats.registration_failure_total += 1;
        }
    }

    fn record_service_discovery_error(&self, status: &Status) {
        let mut state = self
            .inner
            .service_discovery
            .write()
            .expect("proxy service discovery lock poisoned");
        state.last_error_ms = Some(now_ms());
        state.last_error = Some(status.clone());
        state.stats.heartbeat_failure_total += 1;
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

fn risk_cpc_timestamp_ms(timestamp_ms: u64) -> u64 {
    if timestamp_ms == 0 {
        now_ms()
    } else if timestamp_ms < 10_000_000_000 {
        timestamp_ms.saturating_mul(1_000)
    } else {
        timestamp_ms
    }
}

fn execute_response_json(response: ExecuteResponse) -> serde_json::Value {
    serde_json::to_value(response).unwrap_or_else(|err| {
        serde_json::json!({
            "status": Status::error("serialization_error", err.to_string()),
            "response": CommandResponse::Empty,
        })
    })
}

fn response_object_mut(
    value: &mut serde_json::Value,
) -> Option<&mut serde_json::Map<String, serde_json::Value>> {
    value
        .get_mut("response")
        .and_then(|response| response.as_object_mut())
}

fn hmget_response_json(response: ExecuteResponse) -> serde_json::Value {
    let aliases = match &response.response {
        CommandResponse::Values { values } => {
            let response_values = values
                .iter()
                .map(|value| value.clone().unwrap_or_default())
                .collect::<Vec<_>>();
            let exists = values
                .iter()
                .map(|value| value.is_some())
                .collect::<Vec<bool>>();
            Some((response_values, exists))
        }
        _ => None,
    };
    let mut value = execute_response_json(response);
    if let (Some((values, exists)), Some(response)) = (aliases, response_object_mut(&mut value)) {
        response.insert("values".to_string(), serde_json::json!(values));
        response.insert("exists".to_string(), serde_json::json!(exists));
    }
    value
}

fn feature_points_response_json_with_fields(
    response: ExecuteResponse,
    fields: &[String],
) -> serde_json::Value {
    let point_list = match &response.response {
        CommandResponse::FeaturePoints { points } => Some(project_feature_points(points, fields)),
        _ => None,
    };
    let mut value = execute_response_json(response);
    if let (Some(point_list), Some(response)) = (point_list, response_object_mut(&mut value)) {
        response.insert("point_list".to_string(), serde_json::json!(point_list));
    }
    value
}

fn feature_query_fields(fields: &str) -> Vec<String> {
    fields
        .split(|ch: char| ch == ',' || ch == ';' || ch.is_ascii_whitespace())
        .map(str::trim)
        .filter(|field| matches!(*field, "gid" | "action_type" | "duration" | "author_id"))
        .map(ToOwned::to_owned)
        .collect()
}

fn project_feature_points(points: &[FeaturePoint], fields: &[String]) -> Vec<FeaturePoint> {
    if fields.is_empty() {
        return points.to_vec();
    }
    points
        .iter()
        .map(|point| {
            SequenceFeatureRow::decode_cpp_feature_value(point.timestamp_ms, &point.value)
                .map(|row| FeaturePoint {
                    timestamp_ms: point.timestamp_ms,
                    value: encode_projected_cpp_feature_value(&row, fields),
                })
                .unwrap_or_else(|| point.clone())
        })
        .collect()
}

fn encode_projected_cpp_feature_value(row: &SequenceFeatureRow, fields: &[String]) -> Vec<u8> {
    let mut out = Vec::new();
    for field in fields {
        match field.as_str() {
            "gid" => encode_proxy_varint_field(&mut out, 1, row.gid),
            "action_type" => encode_proxy_varint_field(&mut out, 2, row.action_type as u64),
            "duration" => encode_proxy_varint_field(&mut out, 3, row.duration as u64),
            "author_id" => encode_proxy_varint_field(&mut out, 4, row.author_id),
            _ => {}
        }
    }
    out
}

fn encode_proxy_varint_field(out: &mut Vec<u8>, field_number: u64, value: u64) {
    encode_proxy_varint(out, field_number << 3);
    encode_proxy_varint(out, value);
}

fn encode_proxy_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn sequence_rows_response_json(response: ExecuteResponse) -> serde_json::Value {
    let row_list = match &response.response {
        CommandResponse::SequenceRows { rows } => Some(rows.clone()),
        _ => None,
    };
    let mut value = execute_response_json(response);
    if let (Some(row_list), Some(response)) = (row_list, response_object_mut(&mut value)) {
        response.insert("row_list".to_string(), serde_json::json!(row_list));
    }
    value
}

fn hash_entries_response_json(
    response: ExecuteResponse,
    include_result: bool,
) -> serde_json::Value {
    let aliases = match &response.response {
        CommandResponse::HashEntries { entries } => {
            let fields = entries
                .iter()
                .map(|(field, _)| field.clone())
                .collect::<Vec<_>>();
            let values = entries
                .iter()
                .map(|(_, value)| value.clone())
                .collect::<Vec<_>>();
            let result = entries
                .iter()
                .map(|(key, value)| {
                    serde_json::json!({
                        "key": key,
                        "value": String::from_utf8_lossy(value).into_owned(),
                    })
                })
                .collect::<Vec<_>>();
            Some((fields, values, result))
        }
        _ => None,
    };
    let mut value = execute_response_json(response);
    if let (Some((fields, values, result)), Some(response)) =
        (aliases, response_object_mut(&mut value))
    {
        response.insert("fields".to_string(), serde_json::json!(fields));
        response.insert("values".to_string(), serde_json::json!(values));
        if include_result {
            response.insert("result".to_string(), serde_json::json!(result));
        }
    }
    value
}

fn integer_response_json(response: ExecuteResponse, alias: &str) -> serde_json::Value {
    let integer = match &response.response {
        CommandResponse::Integer { value } => Some(*value),
        _ => None,
    };
    let mut value = execute_response_json(response);
    if let (Some(integer), Some(response)) = (integer, response_object_mut(&mut value)) {
        response.insert(alias.to_string(), serde_json::json!(integer));
    }
    value
}

fn risk_fol_query_response_json(response: ExecuteResponse) -> serde_json::Value {
    let result = match &response.response {
        CommandResponse::Bytes { value } => value
            .as_ref()
            .map(|bytes| String::from_utf8_lossy(bytes).into_owned()),
        _ => None,
    };
    let mut value = execute_response_json(response);
    if let (Some(result), Some(response)) = (result, response_object_mut(&mut value)) {
        response.insert("result".to_string(), serde_json::json!(result));
    }
    value
}

fn risk_hset_timestamp_ms(request: &ProxyRiskHsetCommandRequest) -> u64 {
    if request.timestamp_ms != 0 {
        return risk_cpc_timestamp_ms(request.timestamp_ms);
    }
    risk_cpc_timestamp_ms(request.occur_time_seconds)
}

fn risk_hset_ttl_ms(ttl_seconds: u64) -> Option<u64> {
    (ttl_seconds > 0).then(|| ttl_seconds.saturating_mul(1_000))
}

fn risk_hset_amount(request: &ProxyRiskHsetCommandRequest) -> i64 {
    if request.amount != 0 {
        return request.amount;
    }
    match request.value.as_ref() {
        Some(value) if value.is_i64() => value.as_i64().unwrap_or_default(),
        Some(value) if value.is_u64() => value.as_u64().unwrap_or_default() as i64,
        Some(value) if value.is_f64() => value.as_f64().unwrap_or_default() as i64,
        Some(value) => value
            .as_str()
            .and_then(|text| text.trim().parse::<i64>().ok())
            .unwrap_or_else(|| {
                if matches!(request.htype, ProxyRiskHType::Count) {
                    1
                } else {
                    0
                }
            }),
        None => {
            if matches!(request.htype, ProxyRiskHType::Count) {
                1
            } else {
                0
            }
        }
    }
}

fn risk_fol_value_bytes(value: &serde_json::Value) -> Vec<u8> {
    if let Some(text) = value.as_str() {
        return text.as_bytes().to_vec();
    }
    if let Some(items) = value.as_array() {
        return items
            .iter()
            .map(|item| item.as_u64().unwrap_or_default() as u8)
            .collect();
    }
    if value.is_null() {
        Vec::new()
    } else {
        value.to_string().into_bytes()
    }
}

fn risk_fol_time_ms(occur_time_ms: u64, occur_time_seconds: u64) -> u64 {
    if occur_time_ms != 0 {
        return risk_cpc_timestamp_ms(occur_time_ms);
    }
    risk_cpc_timestamp_ms(occur_time_seconds)
}

fn risk_fol_ttl_ms(ttl_ms: u64, ttl_seconds: u64) -> u64 {
    if ttl_ms != 0 {
        ttl_ms
    } else {
        ttl_seconds.saturating_mul(1_000)
    }
}

fn risk_manager_op_type(value: Option<&serde_json::Value>) -> Option<String> {
    match value {
        Some(serde_json::Value::Number(number)) => number.as_i64().map(|value| value.to_string()),
        Some(serde_json::Value::String(value)) if !value.trim().is_empty() => {
            Some(value.trim().to_string())
        }
        _ => None,
    }
}

fn risk_cpc_query_bounds(request: &ProxyRiskCpcQueryCommandRequest) -> Vec<(u64, u64)> {
    if let (Some(start_ms), Some(end_ms)) = (request.start_ms, request.end_ms) {
        return vec![(start_ms, end_ms)];
    }
    risk_window_bounds(&request.windows)
}

fn risk_hquery_bounds(request: &ProxyRiskHqueryCommandRequest) -> Vec<(u64, u64)> {
    if let (Some(start_ms), Some(end_ms)) = (request.start_ms, request.end_ms) {
        return vec![(start_ms, end_ms)];
    }
    risk_window_bounds(&request.windows)
}

fn risk_window_bounds(windows: &[ProxyRiskWindow]) -> Vec<(u64, u64)> {
    if windows.is_empty() {
        return vec![(0, u64::MAX)];
    }
    let now = now_ms() as i64;
    windows
        .iter()
        .map(|window| {
            let unit_ms = window.unit.duration_ms();
            let start = now.saturating_add(window.start_offset.saturating_mul(unit_ms));
            let end = now.saturating_add(window.end_offset.saturating_mul(unit_ms));
            (start.max(0) as u64, end.max(0) as u64)
        })
        .collect()
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
            assert!(report
                .missing_by_area("storage_cache")
                .expect("storage cache area must exist")
                .is_empty());
            assert!(report
                .missing_by_area("raft_replication")
                .expect("raft replication area must exist")
                .iter()
                .any(|item| item.contains("multi-process rollout evidence")));
            assert!(report
                .missing_by_area("scale_testing")
                .expect("scale testing area must exist")
                .is_empty());
        }
    }

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
        // shared-corpus: ops_grafana_metrics_cpp_parity
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
            "Bcache2ThriftService command dispatch",
            "Bcache2ThriftService admission/inflight checks",
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
                    fields: Vec::new(),
                    values: Vec::new(),
                })
                .unwrap(),
            )
            .status
            .ok
        );
        let hmget_body = serde_json::to_vec(&ProxyHashMultiGetCommandRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            key: "cpp-proxy-hm".to_string(),
            fields: vec!["a".to_string(), "missing".to_string()],
        })
        .unwrap();
        assert_eq!(
            command_alias("/ProxyService/HMGet", hmget_body.clone()).response,
            CommandResponse::Values {
                values: vec![Some(b"1".to_vec()), None]
            }
        );
        let (code, hmget_alias_body) = proxy.handle(HttpRequest {
            method: "POST".to_string(),
            path: "/ProxyService/HMGet".to_string(),
            body: hmget_body,
        });
        assert_eq!(code, 200, "HMGet alias response should return HTTP 200");
        let hmget_aliases = parse_json::<serde_json::Value>(&hmget_alias_body).unwrap();
        assert_eq!(
            hmget_aliases["response"]["values"],
            serde_json::json!([[49], []])
        );
        assert_eq!(
            hmget_aliases["response"]["exists"],
            serde_json::json!([true, false])
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
                "/ProxyService/RiskHset",
                serde_json::to_vec(&ProxyRiskHsetCommandRequest {
                    namespace: "ns".to_string(),
                    table_name: "tbl".to_string(),
                    key: "cpp-proxy-risk".to_string(),
                    timestamp_ms: 10,
                    amount: 5,
                    value: None,
                    occur_time_seconds: 0,
                    ttl_ms: 0,
                    precision_ms: None,
                    htype: ProxyRiskHType::Count,
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
}
