use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::http::{
    get_json, get_json_with_options, post_json, post_json_with_options, HttpError,
    HttpRequestOptions,
};
use crate::meta::GetShardResponse;
use crate::meta::{
    GetTableTopologyRequest, ServerEndpoint, TableTopologyResponse, TopologyVersionReport,
    TopologyVersionRequest,
};
use crate::types::{
    parse_cpp_feature_filters, BatchExecuteRequest, BatchExecuteResponse, Command, CommandResponse,
    ContextEvent, ContextIndexRef, ContextNode, ContextPackAudit, ContextSummaryDirtyMarker,
    ExecuteRequest, ExecuteResponse, FeatureFilter, FeaturePoint, FeatureWritePolicy,
    IpsSnapshotReport, IpsStats, RiskFamily, RiskFolType, SequenceFeatureRow, SequenceQuerySpec,
    ShardId, Status,
};

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("http error: {0}")]
    Http(#[from] HttpError),
    #[error("server returned error: {0}")]
    Status(String),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("unexpected response for {operation}: {response:?}")]
    UnexpectedResponse {
        operation: &'static str,
        response: CommandResponse,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientOptions {
    pub proxy_addr: String,
    pub meta_addr: Option<String>,
    pub default_shard_id: ShardId,
    pub connect_timeout_ms: u64,
    pub io_timeout_ms: u64,
    pub max_retries: usize,
    pub max_read_retries: usize,
    pub max_write_retries: usize,
    pub retry_backoff_ms: u64,
    pub route_cache_ttl_ms: u64,
    pub meta_sync_interval_ms: u64,
    pub topo_error_retry_interval_ms: u64,
    pub local_location: String,
    pub drop_percent: u8,
}

impl ClientOptions {
    pub fn proxy(proxy_addr: impl Into<String>) -> Self {
        Self {
            proxy_addr: proxy_addr.into(),
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

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            proxy_addr: "127.0.0.1:17000".to_string(),
            meta_addr: None,
            default_shard_id: 1,
            connect_timeout_ms: 200,
            io_timeout_ms: 200,
            max_retries: 0,
            max_read_retries: 1,
            max_write_retries: 0,
            retry_backoff_ms: 2,
            route_cache_ttl_ms: 1_000,
            meta_sync_interval_ms: 10 * 60 * 1_000,
            topo_error_retry_interval_ms: 5_000,
            local_location: String::new(),
            drop_percent: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestOptions {
    pub trace_id: u64,
}

impl Default for RequestOptions {
    fn default() -> Self {
        Self { trace_id: 0 }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableOptions {
    pub io_timeout_ms: u64,
    pub connect_timeout_ms: u64,
    pub continuous_failed_time_ms: u64,
    pub first_shard_id: ShardId,
    pub shard_count: u64,
    pub pin_primary: bool,
    pub replica_read_policy: ReplicaReadPolicy,
    pub preferred_location: String,
    pub drop_percent: u8,
    pub max_read_retries: usize,
    pub max_write_retries: usize,
    pub retry_backoff_ms: u64,
}

impl Default for TableOptions {
    fn default() -> Self {
        Self {
            io_timeout_ms: 200,
            connect_timeout_ms: 200,
            continuous_failed_time_ms: 10_000,
            first_shard_id: 1,
            shard_count: 1,
            pin_primary: true,
            replica_read_policy: ReplicaReadPolicy::PinPrimary,
            preferred_location: String::new(),
            drop_percent: 0,
            max_read_retries: 1,
            max_write_retries: 0,
            retry_backoff_ms: 2,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaReadPolicy {
    PinPrimary,
    FirstReplica,
    RoundRobinReplica,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ClientCompatibilityMode {
    RustNative,
    CppWireMigrationOutOfScope,
}

impl Default for ClientCompatibilityMode {
    fn default() -> Self {
        Self::RustNative
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClientDeploymentPlacementPolicy {
    pub deployment_name: String,
    pub neptune_routing_enabled: bool,
    pub preferred_location: String,
    pub replica_read_policy: ReplicaReadPolicy,
    pub require_location_affinity: bool,
    pub placement_hook_ready: bool,
}

impl ClientDeploymentPlacementPolicy {
    pub fn neptune(
        deployment_name: impl Into<String>,
        preferred_location: impl Into<String>,
    ) -> Self {
        Self {
            deployment_name: deployment_name.into(),
            neptune_routing_enabled: true,
            preferred_location: preferred_location.into(),
            replica_read_policy: ReplicaReadPolicy::RoundRobinReplica,
            require_location_affinity: true,
            placement_hook_ready: true,
        }
    }

    pub fn apply_to_table_options(&self, options: &mut TableOptions) {
        if !self.preferred_location.is_empty() {
            options.preferred_location = self.preferred_location.clone();
        }
        options.replica_read_policy = self.replica_read_policy;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClientMigrationCompatibilityReport {
    pub compatibility_mode: ClientCompatibilityMode,
    pub rust_native_http_ready: bool,
    pub rust_native_tonic_ready: bool,
    pub brpc_thrift_in_scope: bool,
    pub cpp_wire_compatible_ready: bool,
    pub migration_layer_ready: bool,
    #[serde(default)]
    pub typed_table_client_ready: bool,
    #[serde(default)]
    pub topology_sync_ready: bool,
    #[serde(default)]
    pub retry_budgets_ready: bool,
    #[serde(default)]
    pub neptune_routing_hooks_ready: bool,
    #[serde(default)]
    pub placement_hooks_ready: bool,
    #[serde(default)]
    pub production_replacement_contract: ClientProductionReplacementContract,
    pub blockers: Vec<String>,
}

impl Default for ClientMigrationCompatibilityReport {
    fn default() -> Self {
        Self {
            compatibility_mode: ClientCompatibilityMode::CppWireMigrationOutOfScope,
            rust_native_http_ready: true,
            rust_native_tonic_ready: true,
            brpc_thrift_in_scope: false,
            cpp_wire_compatible_ready: false,
            migration_layer_ready: false,
            typed_table_client_ready: true,
            topology_sync_ready: true,
            retry_budgets_ready: true,
            neptune_routing_hooks_ready: true,
            placement_hooks_ready: true,
            production_replacement_contract: ClientProductionReplacementContract::default(),
            blockers: vec![
                "brpc/thrift wire compatibility is explicitly out of scope for the Rust-native target"
                    .to_string(),
                "existing C++ callers must migrate through the documented Rust HTTP/JSON, RESP, and tonic API"
                    .to_string(),
            ],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClientProductionReplacementContract {
    pub compatibility_decision: String,
    pub cplusplus_wire_protocols: Vec<String>,
    pub production_protocols: Vec<String>,
    pub typed_table_client_preserved: bool,
    pub topology_sync_preserved: bool,
    pub retry_budget_preserved: bool,
    pub neptune_routing_hooks_preserved: bool,
    pub placement_hooks_preserved: bool,
    pub migration_contract_version: u32,
}

impl Default for ClientProductionReplacementContract {
    fn default() -> Self {
        Self {
            compatibility_decision:
                "brpc/thrift migration shims are out of scope; use Rust-native migration contract"
                    .to_string(),
            cplusplus_wire_protocols: vec!["brpc".to_string(), "thrift".to_string()],
            production_protocols: vec![
                "HTTP/JSON".to_string(),
                "RESP".to_string(),
                "tonic".to_string(),
            ],
            typed_table_client_preserved: true,
            topology_sync_preserved: true,
            retry_budget_preserved: true,
            neptune_routing_hooks_preserved: true,
            placement_hooks_preserved: true,
            migration_contract_version: 1,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientStats {
    pub open_table_calls: u64,
    pub close_table_calls: u64,
    pub execute_requests: u64,
    pub batch_execute_requests: u64,
    pub route_cache_hits: u64,
    pub route_cache_misses: u64,
    pub route_refreshes: u64,
    pub backend_errors: u64,
    pub backend_error_streak: u64,
    pub continuous_backend_failures: u64,
    pub backend_successes_after_error: u64,
    pub meta_sync_total: u64,
    pub meta_sync_errors: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientPreflightReport {
    pub status: Status,
    pub proxy_addr: String,
    pub meta_addr: Option<String>,
    pub default_shard_id: ShardId,
    pub route_cache_size: usize,
    pub table_cache_size: usize,
    pub backend_failure_count: usize,
    pub stats: ClientStats,
    pub options: ClientOptions,
    pub topology_cache: ClientTopologyCacheReport,
    #[serde(default)]
    pub cpp_partition_sets: Vec<ClientCppPartitionSetReport>,
    #[serde(default)]
    pub meta_sync: ClientMetaSyncReport,
    pub degraded_reasons: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientCppPartitionSetReport {
    pub namespace: String,
    pub table_name: String,
    pub combine_name: String,
    pub first_shard_id: ShardId,
    pub shard_count: u64,
    pub topology_version: u64,
    pub partition_count: usize,
    pub missing_route_count: usize,
    pub members: Vec<ClientCppPartitionMemberReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientCppPartitionMemberReport {
    pub partition_id: ShardId,
    pub shard_id: ShardId,
    pub primary_addr: Option<String>,
    pub replica_addrs: Vec<String>,
    pub replica_count: usize,
    pub topology_version: u64,
    pub route_ready: bool,
    pub refresh_reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientMetaSyncReport {
    pub table_count: usize,
    pub synced_table_count: usize,
    pub error_table_count: usize,
    pub total_sync_generation: u64,
    pub tables: Vec<ClientMetaSyncTableReport>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientMetaSyncLoopOptions {
    pub tick_ms: u64,
    pub max_tables_per_tick: usize,
}

impl Default for ClientMetaSyncLoopOptions {
    fn default() -> Self {
        Self {
            tick_ms: 1_000,
            max_tables_per_tick: 128,
        }
    }
}

#[derive(Debug)]
pub struct ClientMetaSyncLoopHandle {
    stop: Arc<AtomicBool>,
    join: Option<thread::JoinHandle<()>>,
}

impl ClientMetaSyncLoopHandle {
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    pub fn is_stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }

    pub fn stop_and_join(mut self) -> thread::Result<()> {
        self.stop();
        if let Some(join) = self.join.take() {
            join.join()
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientMetaSyncTableReport {
    pub table: String,
    pub namespace: String,
    pub table_name: String,
    pub sync_generation: u64,
    pub last_success_unix_ms: u64,
    pub last_error_unix_ms: u64,
    pub next_sync_after_unix_ms: u64,
    pub last_topology_version: u64,
    pub consecutive_errors: u64,
    pub last_error: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientTopologyCacheReport {
    pub route_count: usize,
    pub min_topology_version: u64,
    pub max_topology_version: u64,
    #[serde(default)]
    pub authoritative_topology_version: u64,
    #[serde(default)]
    pub stale_route_count: usize,
    #[serde(default)]
    pub cache_stale: bool,
    pub unknown_topology_version_routes: usize,
    pub ttl_expired_routes: usize,
    pub last_refresh_reason: String,
    pub routes: Vec<ClientRouteCacheEntryReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientRouteCacheEntryReport {
    pub shard_id: ShardId,
    pub primary_addr: String,
    pub replica_count: usize,
    pub topology_version: u64,
    pub fetched_age_ms: u64,
    pub ttl_expired: bool,
    pub refresh_reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientTopologyRefreshReport {
    pub status: Status,
    pub old_topology_version: u64,
    pub current_topology_version: u64,
    pub unchanged: bool,
    pub refreshed_tables: Vec<String>,
    pub skipped_tables: Vec<String>,
    pub refresh_all: bool,
    pub event_count: usize,
    pub stale_before_refresh: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientTopologyInvalidationReport {
    pub status: Status,
    pub old_topology_version: u64,
    pub current_topology_version: u64,
    pub route_count_before: usize,
    pub invalidated_routes: usize,
    pub refreshed_tables: Vec<String>,
    pub skipped_tables: Vec<String>,
    pub refresh_all: bool,
    pub event_count: usize,
    pub stale_before_invalidation: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientRetryDecision {
    pub retryable: bool,
    pub topology_retry: bool,
    pub safe_budget_free_write_retry: bool,
    pub would_retry: bool,
}

impl ClientStats {
    fn record_backend_error(&mut self, became_continuous: bool) {
        self.backend_errors += 1;
        self.backend_error_streak += 1;
        if became_continuous {
            self.continuous_backend_failures += 1;
        }
    }

    fn record_backend_success(&mut self) {
        if self.backend_error_streak > 0 {
            self.backend_successes_after_error += 1;
        }
        self.backend_error_streak = 0;
    }
}

#[derive(Debug, Clone)]
pub struct TemporalStoreClient {
    inner: Arc<ClientInner>,
}

#[derive(Debug)]
struct ClientInner {
    options: ClientOptions,
    routes: Mutex<HashMap<ShardId, CachedRoute>>,
    backend_failures: Mutex<HashMap<String, BackendFailureState>>,
    tables: Mutex<HashMap<String, TableOptions>>,
    meta_sync_tables: Mutex<HashMap<String, ClientMetaSyncTableState>>,
    stats: Mutex<ClientStats>,
}

#[derive(Debug, Clone)]
struct ClientMetaSyncTableState {
    namespace: String,
    table_name: String,
    sync_generation: u64,
    last_success_unix_ms: u64,
    last_error_unix_ms: u64,
    next_sync_after_unix_ms: u64,
    last_topology_version: u64,
    consecutive_errors: u64,
    last_error: String,
}

#[derive(Debug, Clone)]
struct CachedRoute {
    primary_addr: String,
    replica_addrs: Vec<String>,
    replica_endpoints: Vec<ServerEndpoint>,
    next_replica_index: usize,
    fetched_at: Instant,
    topology_version: u64,
    refresh_reason: String,
}

#[derive(Debug, Clone)]
struct BackendFailureState {
    first_failed_at: Instant,
    last_failed_at: Instant,
    consecutive_failures: u64,
}

impl TemporalStoreClient {
    pub fn new(proxy_addr: impl Into<String>) -> Self {
        Self::with_options(ClientOptions::proxy(proxy_addr))
    }

    pub fn with_options(options: ClientOptions) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                options,
                routes: Mutex::default(),
                backend_failures: Mutex::default(),
                tables: Mutex::default(),
                meta_sync_tables: Mutex::default(),
                stats: Mutex::default(),
            }),
        }
    }

    pub fn open_table(
        &self,
        namespace: impl Into<String>,
        table_name: impl Into<String>,
        options: TableOptions,
    ) -> TemporalStoreTable {
        let namespace = namespace.into();
        let table_name = table_name.into();
        let combine_name = table_combine_name(&namespace, &table_name);
        self.inner
            .tables
            .lock()
            .expect("client table cache lock poisoned")
            .insert(combine_name, options.clone());
        self.ensure_meta_sync_table_state(&namespace, &table_name);
        self.inner
            .stats
            .lock()
            .expect("client stats lock poisoned")
            .open_table_calls += 1;
        TemporalStoreTable {
            client: self.clone(),
            namespace,
            table_name,
            shard_id: self.inner.options.default_shard_id,
            options,
        }
    }

    pub fn open_table_from_meta(
        &self,
        namespace: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Result<TemporalStoreTable, ClientError> {
        let namespace = namespace.into();
        let table_name = table_name.into();
        let options = self.sync_table_topology(namespace.clone(), table_name.clone())?;
        Ok(self.open_table(namespace, table_name, options))
    }

    pub fn cached_table(
        &self,
        namespace: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Option<TemporalStoreTable> {
        let namespace = namespace.into();
        let table_name = table_name.into();
        let options = self
            .inner
            .tables
            .lock()
            .expect("client table cache lock poisoned")
            .get(&table_combine_name(&namespace, &table_name))
            .cloned()?;
        Some(TemporalStoreTable {
            client: self.clone(),
            namespace,
            table_name,
            shard_id: self.inner.options.default_shard_id,
            options,
        })
    }

    pub fn deployment_placement_policy(
        &self,
        deployment_name: impl Into<String>,
    ) -> ClientDeploymentPlacementPolicy {
        ClientDeploymentPlacementPolicy::neptune(
            deployment_name,
            self.inner.options.local_location.clone(),
        )
    }

    pub fn migration_compatibility_report(&self) -> ClientMigrationCompatibilityReport {
        ClientMigrationCompatibilityReport::default()
    }

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
        let topology: TableTopologyResponse = match post_json_with_options(
            meta_addr,
            "/tables/topology",
            &GetTableTopologyRequest {
                namespace: namespace.clone(),
                table_name: table_name.clone(),
                old_topology_version: 0,
            },
            self.inner.options.http_options(),
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
        let route_topology_version = self
            .current_meta_topology_version()
            .unwrap_or(table.topology_version)
            .max(table.topology_version);
        let serving_options = table.serving_options.clone();
        let default_serving_options = crate::meta::TableServingOptions::default();
        let options = TableOptions {
            io_timeout_ms: if serving_options.io_timeout_ms == default_serving_options.io_timeout_ms
            {
                self.inner.options.io_timeout_ms
            } else {
                serving_options.io_timeout_ms
            },
            connect_timeout_ms: if serving_options.connect_timeout_ms
                == default_serving_options.connect_timeout_ms
            {
                self.inner.options.connect_timeout_ms
            } else {
                serving_options.connect_timeout_ms
            },
            continuous_failed_time_ms: if serving_options.continuous_failed_time_ms
                == default_serving_options.continuous_failed_time_ms
            {
                TableOptions::default().continuous_failed_time_ms
            } else {
                serving_options.continuous_failed_time_ms
            },
            first_shard_id: table.first_shard_id,
            shard_count: table.shard_count,
            pin_primary: serving_options.pin_primary,
            replica_read_policy: replica_read_policy_from_meta(
                &serving_options.replica_read_policy,
            ),
            preferred_location: if serving_options.preferred_location.is_empty() {
                self.inner.options.local_location.clone()
            } else {
                serving_options.preferred_location.clone()
            },
            drop_percent: if serving_options.drop_percent == default_serving_options.drop_percent {
                self.inner.options.drop_percent.min(100)
            } else {
                serving_options.drop_percent.min(100)
            },
            max_read_retries: if serving_options.max_read_retries
                == default_serving_options.max_read_retries
            {
                self.inner.options.max_read_retries
            } else {
                serving_options.max_read_retries as usize
            },
            max_write_retries: if serving_options.max_write_retries
                == default_serving_options.max_write_retries
            {
                self.inner.options.max_write_retries
            } else {
                serving_options.max_write_retries as usize
            },
            retry_backoff_ms: if serving_options.retry_backoff_ms
                == default_serving_options.retry_backoff_ms
            {
                self.inner.options.retry_backoff_ms
            } else {
                serving_options.retry_backoff_ms
            },
            ..TableOptions::default()
        };
        let routes = topology
            .partitions
            .iter()
            .filter_map(|partition| {
                partition.primary.as_ref().map(|primary| {
                    (
                        partition.shard_id,
                        CachedRoute {
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
                            next_replica_index: 0,
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
            .lock()
            .expect("client table cache lock poisoned")
            .insert(table_combine_name(&namespace, &table_name), options.clone());
        let mut route_cache = self
            .inner
            .routes
            .lock()
            .expect("client route cache lock poisoned");
        let last_shard_id = table
            .first_shard_id
            .saturating_add(table.shard_count.saturating_sub(1));
        route_cache
            .retain(|shard_id, _| *shard_id < table.first_shard_id || *shard_id > last_shard_id);
        for (shard_id, route) in routes {
            route_cache.insert(shard_id, route);
        }
        self.record_meta_sync_success(&namespace, &table_name, route_topology_version);
        Ok(options)
    }

    fn current_meta_topology_version(&self) -> Option<u64> {
        let meta_addr = self.inner.options.meta_addr.as_ref()?;
        let topology = post_json_with_options::<_, TopologyVersionReport>(
            meta_addr,
            "/meta/topology_version",
            &TopologyVersionRequest {
                old_topology_version: 0,
            },
            self.inner.options.http_options(),
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
        let topology: TopologyVersionReport = post_json_with_options(
            meta_addr,
            "/meta/topology_version",
            &TopologyVersionRequest {
                old_topology_version,
            },
            self.inner.options.http_options(),
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
        let topology: TopologyVersionReport = post_json_with_options(
            meta_addr,
            "/meta/topology_version",
            &TopologyVersionRequest {
                old_topology_version,
            },
            self.inner.options.http_options(),
        )?;
        if !topology.status.ok {
            return Err(ClientError::Status(topology.status.message));
        }

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
                .lock()
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
            .lock()
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
            .lock()
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
                .lock()
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
        *self.inner.stats.lock().expect("client stats lock poisoned")
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
            .lock()
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
            cpp_partition_sets: self.cpp_partition_set_report(),
            meta_sync,
            degraded_reasons,
        }
    }

    pub fn cpp_partition_set_report(&self) -> Vec<ClientCppPartitionSetReport> {
        let tables = self
            .inner
            .tables
            .lock()
            .expect("client table cache lock poisoned")
            .clone();
        let routes = self
            .inner
            .routes
            .lock()
            .expect("client route cache lock poisoned")
            .clone();
        let mut reports = tables
            .into_iter()
            .filter_map(|(combine_name, options)| {
                let (namespace, table_name) = combine_name.split_once('/')?;
                let mut members = (0..options.shard_count)
                    .map(|offset| {
                        let shard_id = options.first_shard_id.saturating_add(offset);
                        let route = routes.get(&shard_id);
                        ClientCppPartitionMemberReport {
                            partition_id: shard_id,
                            shard_id,
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
                Some(ClientCppPartitionSetReport {
                    namespace: namespace.to_string(),
                    table_name: table_name.to_string(),
                    combine_name,
                    first_shard_id: options.first_shard_id,
                    shard_count: options.shard_count,
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
            .lock()
            .expect("client route cache lock poisoned")
            .iter()
            .map(|(shard_id, route)| {
                let fetched_age_ms = duration_ms_u64(route.fetched_at.elapsed());
                ClientRouteCacheEntryReport {
                    shard_id: *shard_id,
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
            .lock()
            .expect("client route cache lock poisoned")
            .len()
    }

    fn due_meta_sync_tables(&self, now_ms: u64, max_tables: usize) -> Vec<(String, String)> {
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
            .lock()
            .expect("client route cache lock poisoned")
            .insert(
                shard_id,
                CachedRoute {
                    primary_addr: primary_addr.into(),
                    replica_addrs: Vec::new(),
                    replica_endpoints: Vec::new(),
                    next_replica_index: 0,
                    fetched_at: Instant::now(),
                    topology_version: 0,
                    refresh_reason: "test_insert".to_string(),
                },
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
        self.inner
            .backend_failures
            .lock()
            .expect("client backend failure lock poisoned")
            .insert(
                server_addr.into(),
                BackendFailureState {
                    first_failed_at: now - Duration::from_millis(first_failed_ago_ms),
                    last_failed_at: now - Duration::from_millis(last_failed_ago_ms),
                    consecutive_failures,
                },
            );
    }

    fn ensure_meta_sync_table_state(&self, namespace: &str, table_name: &str) {
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
            });
    }

    fn record_meta_sync_success(&self, namespace: &str, table_name: &str, topology_version: u64) {
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
            });
        state.sync_generation = state.sync_generation.saturating_add(1);
        state.last_success_unix_ms = now;
        state.next_sync_after_unix_ms =
            now.saturating_add(self.inner.options.meta_sync_interval_ms);
        state.last_topology_version = topology_version;
        state.consecutive_errors = 0;
        state.last_error.clear();
    }

    fn record_meta_sync_error(&self, namespace: &str, table_name: &str, error: &str) {
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
            });
        state.sync_generation = state.sync_generation.saturating_add(1);
        state.last_error_unix_ms = now;
        state.consecutive_errors = state.consecutive_errors.saturating_add(1);
        let backoff_ms = self
            .inner
            .options
            .topo_error_retry_interval_ms
            .saturating_mul(state.consecutive_errors.max(1))
            .min(self.inner.options.meta_sync_interval_ms.max(1));
        state.next_sync_after_unix_ms = now.saturating_add(backoff_ms);
        state.last_error = error.to_string();
    }

    pub fn execute(&self, request: ExecuteRequest) -> Result<ExecuteResponse, HttpError> {
        post_json(&self.inner.options.proxy_addr, "/execute", &request)
    }

    pub fn execute_with_options(
        &self,
        request: ExecuteRequest,
        options: RequestOptions,
    ) -> Result<ExecuteResponse, ClientError> {
        let _trace_id = options.trace_id;
        self.execute_routed(request, false).map_err(Into::into)
    }

    pub fn batch_execute(
        &self,
        request: BatchExecuteRequest,
    ) -> Result<BatchExecuteResponse, HttpError> {
        post_json(&self.inner.options.proxy_addr, "/batch_execute", &request)
    }

    pub fn batch_execute_with_options(
        &self,
        request: BatchExecuteRequest,
        options: RequestOptions,
    ) -> Result<BatchExecuteResponse, ClientError> {
        let _trace_id = options.trace_id;
        let http_options = self.inner.options.http_options();
        if self.inner.options.meta_addr.is_some() {
            let server_addr = self.resolve_route(request.shard_id, false, None)?;
            return post_json_with_options(&server_addr, "/batch_execute", &request, http_options)
                .or_else(|_| {
                    let became_continuous = self.record_backend_failure(
                        &server_addr,
                        self.inner.options.topo_error_retry_interval_ms,
                    );
                    self.inner
                        .stats
                        .lock()
                        .expect("client stats lock poisoned")
                        .record_backend_error(became_continuous);
                    let refreshed = self.resolve_route(request.shard_id, true, None)?;
                    Ok(post_json_with_options(
                        &refreshed,
                        "/batch_execute",
                        &request,
                        http_options,
                    )?)
                });
        }
        Ok(post_json_with_options(
            &self.inner.options.proxy_addr,
            "/batch_execute",
            &request,
            http_options,
        )?)
    }

    pub fn get_shard(&self, shard_id: u64) -> Result<GetShardResponse, HttpError> {
        get_json(
            &self.inner.options.proxy_addr,
            &format!("/shards/{shard_id}"),
        )
    }

    pub fn refresh_route(&self, shard_id: ShardId) -> Result<String, ClientError> {
        self.resolve_route(shard_id, true, None)
    }

    fn execute_routed(
        &self,
        request: ExecuteRequest,
        force_primary: bool,
    ) -> Result<ExecuteResponse, ClientError> {
        self.execute_routed_with_http(
            request,
            force_primary,
            self.inner.options.http_options(),
            None,
        )
    }

    fn execute_routed_with_http(
        &self,
        request: ExecuteRequest,
        force_primary: bool,
        http_options: HttpRequestOptions,
        continuous_failed_time_ms: Option<u64>,
    ) -> Result<ExecuteResponse, ClientError> {
        self.execute_routed_with_http_and_policy(
            request,
            force_primary,
            http_options,
            continuous_failed_time_ms,
            ReplicaReadPolicy::PinPrimary,
            None,
        )
    }

    fn execute_routed_with_http_and_policy(
        &self,
        request: ExecuteRequest,
        force_primary: bool,
        http_options: HttpRequestOptions,
        continuous_failed_time_ms: Option<u64>,
        replica_read_policy: ReplicaReadPolicy,
        preferred_location: Option<&str>,
    ) -> Result<ExecuteResponse, ClientError> {
        if self.inner.options.meta_addr.is_some() {
            let policy = if force_primary {
                ReplicaReadPolicy::PinPrimary
            } else {
                replica_read_policy
            };
            let server_addr = self.resolve_route_with_policy(
                request.shard_id,
                false,
                continuous_failed_time_ms,
                policy,
                preferred_location,
            )?;
            return post_json_with_options(&server_addr, "/execute", &request, http_options)
                .or_else(|_| {
                    let became_continuous = self.record_backend_failure(
                        &server_addr,
                        continuous_failed_time_ms
                            .unwrap_or(self.inner.options.topo_error_retry_interval_ms),
                    );
                    self.inner
                        .stats
                        .lock()
                        .expect("client stats lock poisoned")
                        .record_backend_error(became_continuous);
                    let refreshed = self.resolve_route_with_policy(
                        request.shard_id,
                        true,
                        continuous_failed_time_ms,
                        policy,
                        preferred_location,
                    )?;
                    let response =
                        post_json_with_options(&refreshed, "/execute", &request, http_options)?;
                    self.record_backend_success(&refreshed);
                    self.inner
                        .stats
                        .lock()
                        .expect("client stats lock poisoned")
                        .record_backend_success();
                    Ok(response)
                });
        }

        let _ = force_primary;
        Ok(post_json_with_options(
            &self.inner.options.proxy_addr,
            "/execute",
            &request,
            http_options,
        )?)
    }

    fn batch_execute_with_http(
        &self,
        request: BatchExecuteRequest,
        http_options: HttpRequestOptions,
        continuous_failed_time_ms: Option<u64>,
    ) -> Result<BatchExecuteResponse, ClientError> {
        if self.inner.options.meta_addr.is_some() {
            let server_addr =
                self.resolve_route(request.shard_id, false, continuous_failed_time_ms)?;
            return post_json_with_options(&server_addr, "/batch_execute", &request, http_options)
                .or_else(|_| {
                    let became_continuous = self.record_backend_failure(
                        &server_addr,
                        continuous_failed_time_ms
                            .unwrap_or(self.inner.options.topo_error_retry_interval_ms),
                    );
                    self.inner
                        .stats
                        .lock()
                        .expect("client stats lock poisoned")
                        .record_backend_error(became_continuous);
                    let refreshed =
                        self.resolve_route(request.shard_id, true, continuous_failed_time_ms)?;
                    let response = post_json_with_options(
                        &refreshed,
                        "/batch_execute",
                        &request,
                        http_options,
                    )?;
                    self.record_backend_success(&refreshed);
                    self.inner
                        .stats
                        .lock()
                        .expect("client stats lock poisoned")
                        .record_backend_success();
                    Ok(response)
                });
        }
        Ok(post_json_with_options(
            &self.inner.options.proxy_addr,
            "/batch_execute",
            &request,
            http_options,
        )?)
    }

    fn resolve_route(
        &self,
        shard_id: ShardId,
        force_refresh: bool,
        continuous_failed_time_ms: Option<u64>,
    ) -> Result<String, ClientError> {
        self.resolve_route_with_policy(
            shard_id,
            force_refresh,
            continuous_failed_time_ms,
            ReplicaReadPolicy::PinPrimary,
            None,
        )
    }

    fn resolve_route_with_policy(
        &self,
        shard_id: ShardId,
        force_refresh: bool,
        continuous_failed_time_ms: Option<u64>,
        replica_read_policy: ReplicaReadPolicy,
        preferred_location: Option<&str>,
    ) -> Result<String, ClientError> {
        let ttl = Duration::from_millis(self.inner.options.route_cache_ttl_ms);
        if !force_refresh {
            let mut route_cache = self
                .inner
                .routes
                .lock()
                .expect("client route cache lock poisoned");
            if let Some(route) = route_cache.get_mut(&shard_id) {
                if route.fetched_at.elapsed() <= ttl {
                    let server_addr =
                        choose_cached_route(route, replica_read_policy, preferred_location);
                    if self.backend_failure_is_continuous(
                        &server_addr,
                        continuous_failed_time_ms
                            .unwrap_or(self.inner.options.topo_error_retry_interval_ms),
                    ) {
                        self.inner
                            .stats
                            .lock()
                            .expect("client stats lock poisoned")
                            .continuous_backend_failures += 1;
                    } else {
                        self.inner
                            .stats
                            .lock()
                            .expect("client stats lock poisoned")
                            .route_cache_hits += 1;
                        return Ok(server_addr);
                    }
                }
            }
        }
        self.inner
            .stats
            .lock()
            .expect("client stats lock poisoned")
            .route_cache_misses += 1;

        let meta_addr = self
            .inner
            .options
            .meta_addr
            .as_ref()
            .unwrap_or(&self.inner.options.proxy_addr);
        let response: GetShardResponse = get_json_with_options(
            meta_addr,
            &format!("/shards/{shard_id}"),
            self.inner.options.http_options(),
        )?;
        if !response.status.ok {
            return Err(ClientError::Status(response.status.message));
        }
        let server_addr = response
            .location
            .ok_or_else(|| ClientError::Status("route missing".to_string()))?
            .server_addr;
        self.inner
            .routes
            .lock()
            .expect("client route cache lock poisoned")
            .insert(
                shard_id,
                CachedRoute {
                    primary_addr: server_addr.clone(),
                    replica_addrs: Vec::new(),
                    replica_endpoints: Vec::new(),
                    next_replica_index: 0,
                    fetched_at: Instant::now(),
                    topology_version: 0,
                    refresh_reason: "shard_lookup".to_string(),
                },
            );
        self.inner
            .stats
            .lock()
            .expect("client stats lock poisoned")
            .route_refreshes += 1;
        Ok(server_addr)
    }

    fn record_backend_failure(&self, server_addr: &str, continuous_failed_time_ms: u64) -> bool {
        let mut failures = self
            .inner
            .backend_failures
            .lock()
            .expect("client backend failure lock poisoned");
        let now = Instant::now();
        let state =
            failures
                .entry(server_addr.to_string())
                .or_insert_with(|| BackendFailureState {
                    first_failed_at: now,
                    last_failed_at: now,
                    consecutive_failures: 0,
                });
        state.last_failed_at = now;
        state.consecutive_failures += 1;
        state.first_failed_at.elapsed() >= Duration::from_millis(continuous_failed_time_ms)
    }

    fn record_backend_success(&self, server_addr: &str) {
        self.inner
            .backend_failures
            .lock()
            .expect("client backend failure lock poisoned")
            .remove(server_addr);
    }

    fn backend_failure_is_continuous(
        &self,
        server_addr: &str,
        continuous_failed_time_ms: u64,
    ) -> bool {
        self.inner
            .backend_failures
            .lock()
            .expect("client backend failure lock poisoned")
            .get(server_addr)
            .map(|state| {
                state.first_failed_at.elapsed() >= Duration::from_millis(continuous_failed_time_ms)
            })
            .unwrap_or(false)
    }
}

#[derive(Debug, Clone)]
pub struct TemporalStoreTable {
    client: TemporalStoreClient,
    namespace: String,
    table_name: String,
    shard_id: ShardId,
    options: TableOptions,
}

impl TemporalStoreTable {
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub fn shard_id_for_key(&self, key: &str) -> ShardId {
        let options = self.table_options();
        shard_id_for_key(
            key,
            options.first_shard_id,
            options.shard_count,
            self.shard_id,
        )
    }

    pub fn options(&self) -> TableOptions {
        self.table_options()
    }

    fn table_options(&self) -> TableOptions {
        self.client
            .inner
            .tables
            .lock()
            .expect("client table cache lock poisoned")
            .get(&table_combine_name(&self.namespace, &self.table_name))
            .cloned()
            .unwrap_or_else(|| self.options.clone())
    }

    pub fn pipeline(&self) -> TemporalStorePipeline {
        TemporalStorePipeline {
            table: self.clone(),
            commands: Vec::new(),
        }
    }

    pub fn client_stats(&self) -> ClientStats {
        self.client.stats()
    }

    pub fn client_route_cache_size(&self) -> usize {
        self.client.route_cache_size()
    }

    pub fn set(
        &self,
        key: impl Into<String>,
        value: impl Into<Vec<u8>>,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::StringSet {
            key: key.into(),
            value: value.into(),
        })
    }

    pub fn exists(&self, key: impl Into<String>) -> Result<bool, ClientError> {
        match self
            .execute(Command::CommonExists { key: key.into() })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value != 0),
            response => Err(ClientError::UnexpectedResponse {
                operation: "exists",
                response,
            }),
        }
    }

    pub fn setex(
        &self,
        key: impl Into<String>,
        value: impl Into<Vec<u8>>,
        ttl_ms: u64,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::StringSetEx {
            key: key.into(),
            value: value.into(),
            ttl_ms,
        })
    }

    pub fn get(&self, key: impl Into<String>) -> Result<Option<Vec<u8>>, ClientError> {
        match self
            .execute(Command::StringGet { key: key.into() })?
            .response
        {
            CommandResponse::Bytes { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "get",
                response,
            }),
        }
    }

    pub fn hset(
        &self,
        key: impl Into<String>,
        field: impl Into<String>,
        value: impl Into<Vec<u8>>,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::HashSet {
            key: key.into(),
            field: field.into(),
            value: value.into(),
        })
    }

    pub fn hget(
        &self,
        key: impl Into<String>,
        field: impl Into<String>,
    ) -> Result<Option<Vec<u8>>, ClientError> {
        match self
            .execute(Command::HashGet {
                key: key.into(),
                field: field.into(),
            })?
            .response
        {
            CommandResponse::Bytes { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "hget",
                response,
            }),
        }
    }

    pub fn hdel(
        &self,
        key: impl Into<String>,
        field: impl Into<String>,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::HashDelete {
            key: key.into(),
            field: field.into(),
        })
    }

    pub fn hmget(
        &self,
        key: impl Into<String>,
        fields: Vec<String>,
    ) -> Result<Vec<Option<Vec<u8>>>, ClientError> {
        match self
            .execute(Command::HashMultiGet {
                key: key.into(),
                fields,
            })?
            .response
        {
            CommandResponse::Values { values } => Ok(values),
            response => Err(ClientError::UnexpectedResponse {
                operation: "hmget",
                response,
            }),
        }
    }

    pub fn hmset(
        &self,
        key: impl Into<String>,
        entries: Vec<(String, Vec<u8>)>,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::HashMultiSet {
            key: key.into(),
            entries,
        })
    }

    pub fn hincrby(
        &self,
        key: impl Into<String>,
        field: impl Into<String>,
        increment: i64,
    ) -> Result<i64, ClientError> {
        match self
            .execute(Command::HashIncrBy {
                key: key.into(),
                field: field.into(),
                increment,
            })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "hincrby",
                response,
            }),
        }
    }

    pub fn hgetall(&self, key: impl Into<String>) -> Result<Vec<(String, Vec<u8>)>, ClientError> {
        match self
            .execute(Command::HashGetAll { key: key.into() })?
            .response
        {
            CommandResponse::HashEntries { entries } => Ok(entries),
            response => Err(ClientError::UnexpectedResponse {
                operation: "hgetall",
                response,
            }),
        }
    }

    pub fn hlen(&self, key: impl Into<String>) -> Result<i64, ClientError> {
        match self.execute(Command::HashLen { key: key.into() })?.response {
            CommandResponse::Integer { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "hlen",
                response,
            }),
        }
    }

    pub fn del(&self, key: impl Into<String>) -> Result<(), ClientError> {
        self.expect_empty(Command::CommonDelete { key: key.into() })
    }

    pub fn expire(&self, key: impl Into<String>, ttl_ms: u64) -> Result<(), ClientError> {
        self.expect_empty(Command::CommonExpire {
            key: key.into(),
            ttl_ms,
        })
    }

    pub fn ttl(&self, key: impl Into<String>) -> Result<i64, ClientError> {
        match self
            .execute(Command::CommonTtl { key: key.into() })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ttl",
                response,
            }),
        }
    }

    pub fn sadd(
        &self,
        key: impl Into<String>,
        member: impl Into<Vec<u8>>,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::SetAdd {
            key: key.into(),
            member: member.into(),
        })
    }

    pub fn smembers(&self, key: impl Into<String>) -> Result<Vec<Vec<u8>>, ClientError> {
        match self
            .execute(Command::SetMembers { key: key.into() })?
            .response
        {
            CommandResponse::Members { members } => Ok(members),
            response => Err(ClientError::UnexpectedResponse {
                operation: "smembers",
                response,
            }),
        }
    }

    pub fn srem(
        &self,
        key: impl Into<String>,
        member: impl Into<Vec<u8>>,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::SetRemove {
            key: key.into(),
            member: member.into(),
        })
    }

    pub fn feature_append(
        &self,
        key: impl Into<String>,
        points: Vec<FeaturePoint>,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::FeatureAppend {
            key: key.into(),
            points,
        })
    }

    pub fn feature_append_with_policy(
        &self,
        key: impl Into<String>,
        points: Vec<FeaturePoint>,
        policy: FeatureWritePolicy,
    ) -> Result<bool, ClientError> {
        match self
            .execute(Command::FeatureAppendWithPolicy {
                key: key.into(),
                points,
                policy,
            })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value != 0),
            response => Err(ClientError::UnexpectedResponse {
                operation: "feature_append_with_policy",
                response,
            }),
        }
    }

    pub fn feature_query(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
    ) -> Result<Vec<FeaturePoint>, ClientError> {
        match self
            .execute(Command::FeatureQuery {
                key: key.into(),
                start_ms,
                end_ms,
                count,
            })?
            .response
        {
            CommandResponse::FeaturePoints { points } => Ok(points),
            response => Err(ClientError::UnexpectedResponse {
                operation: "feature_query",
                response,
            }),
        }
    }

    pub fn feature_query_filtered(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
        filters: Vec<FeatureFilter>,
    ) -> Result<Vec<FeaturePoint>, ClientError> {
        match self
            .execute(Command::FeatureQueryFiltered {
                key: key.into(),
                start_ms,
                end_ms,
                count,
                filters,
            })?
            .response
        {
            CommandResponse::FeaturePoints { points } => Ok(points),
            response => Err(ClientError::UnexpectedResponse {
                operation: "feature_query_filtered",
                response,
            }),
        }
    }

    pub fn feature_query_cpp_filters(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
        filters: &[String],
    ) -> Result<Vec<FeaturePoint>, ClientError> {
        let filters = parse_cpp_feature_filters(filters.iter().map(String::as_str))
            .map_err(ClientError::InvalidRequest)?;
        self.feature_query_filtered(key, start_ms, end_ms, count, filters)
    }

    pub fn feature_replace(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        points: Vec<FeaturePoint>,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::FeatureReplace {
            key: key.into(),
            start_ms,
            end_ms,
            points,
        })
    }

    pub fn feature_delete(&self, key: impl Into<String>) -> Result<(), ClientError> {
        self.expect_empty(Command::FeatureDelete { key: key.into() })
    }

    pub fn feature_agg_query(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        aggregator: impl Into<String>,
        count: Option<usize>,
    ) -> Result<i64, ClientError> {
        match self
            .execute(Command::FeatureAggQuery {
                key: key.into(),
                start_ms,
                end_ms,
                aggregator: aggregator.into(),
                count,
            })?
            .response
        {
            CommandResponse::Aggregate { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "feature_agg_query",
                response,
            }),
        }
    }

    pub fn sequence_add(
        &self,
        key: impl Into<String>,
        rows: Vec<SequenceFeatureRow>,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::SequenceAdd {
            key: key.into(),
            rows,
        })
    }

    pub fn sequence_query(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        count: usize,
        filters: Vec<FeatureFilter>,
    ) -> Result<Vec<SequenceFeatureRow>, ClientError> {
        match self
            .execute(Command::SequenceQuery {
                key: key.into(),
                start_ms,
                end_ms,
                count,
                filters,
            })?
            .response
        {
            CommandResponse::SequenceRows { rows } => Ok(rows),
            response => Err(ClientError::UnexpectedResponse {
                operation: "sequence_query",
                response,
            }),
        }
    }

    pub fn sequence_batch_query(
        &self,
        queries: Vec<SequenceQuerySpec>,
    ) -> Result<Vec<(String, Vec<SequenceFeatureRow>)>, ClientError> {
        match self
            .execute(Command::SequenceBatchQuery { queries })?
            .response
        {
            CommandResponse::SequenceRowGroups { groups } => Ok(groups),
            response => Err(ClientError::UnexpectedResponse {
                operation: "sequence_batch_query",
                response,
            }),
        }
    }

    pub fn ips_add(
        &self,
        key: impl Into<String>,
        timestamp_ms: u64,
        instance: impl Into<Vec<u8>>,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::IpsAdd {
            key: key.into(),
            timestamp_ms,
            instance: instance.into(),
        })
    }

    pub fn ips_add_with_options(
        &self,
        key: impl Into<String>,
        timestamp_ms: u64,
        instance: impl Into<Vec<u8>>,
        action_type: Option<u32>,
        table_id: Option<u64>,
        request_id: Option<String>,
    ) -> Result<bool, ClientError> {
        match self
            .execute(Command::IpsAddWithOptions {
                key: key.into(),
                timestamp_ms,
                instance: instance.into(),
                action_type,
                table_id,
                request_id,
            })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value != 0),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ips_add_with_options",
                response,
            }),
        }
    }

    pub fn ips_query_last(
        &self,
        key: impl Into<String>,
        count: usize,
    ) -> Result<Vec<FeaturePoint>, ClientError> {
        match self
            .execute(Command::IpsQueryLast {
                key: key.into(),
                count,
            })?
            .response
        {
            CommandResponse::FeaturePoints { points } => Ok(points),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ips_query_last",
                response,
            }),
        }
    }

    pub fn ips_query_range(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
    ) -> Result<Vec<FeaturePoint>, ClientError> {
        match self
            .execute(Command::IpsQueryRange {
                key: key.into(),
                start_ms,
                end_ms,
                count,
            })?
            .response
        {
            CommandResponse::FeaturePoints { points } => Ok(points),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ips_query_range",
                response,
            }),
        }
    }

    pub fn ips_load(
        &self,
        key: impl Into<String>,
        points: Vec<FeaturePoint>,
    ) -> Result<i64, ClientError> {
        match self
            .execute(Command::IpsLoad {
                key: key.into(),
                points,
            })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ips_load",
                response,
            }),
        }
    }

    pub fn ips_snapshot(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
    ) -> Result<Vec<FeaturePoint>, ClientError> {
        match self
            .execute(Command::IpsSnapshot {
                key: key.into(),
                start_ms,
                end_ms,
                count,
            })?
            .response
        {
            CommandResponse::FeaturePoints { points } => Ok(points),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ips_snapshot",
                response,
            }),
        }
    }

    pub fn ips_snapshot_report(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
    ) -> Result<IpsSnapshotReport, ClientError> {
        match self
            .execute(Command::IpsSnapshotReport {
                key: key.into(),
                start_ms,
                end_ms,
                count,
            })?
            .response
        {
            CommandResponse::IpsSnapshotReport { report } => Ok(report),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ips_snapshot_report",
                response,
            }),
        }
    }

    pub fn ips_stat(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<IpsStats, ClientError> {
        match self
            .execute(Command::IpsStat {
                key: key.into(),
                start_ms,
                end_ms,
            })?
            .response
        {
            CommandResponse::IpsStats { stats } => Ok(stats),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ips_stat",
                response,
            }),
        }
    }

    pub fn ips_filter(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
        action_type: Option<u32>,
        table_id: Option<u64>,
    ) -> Result<Vec<FeaturePoint>, ClientError> {
        match self
            .execute(Command::IpsFilter {
                key: key.into(),
                start_ms,
                end_ms,
                count,
                action_type,
                table_id,
            })?
            .response
        {
            CommandResponse::FeaturePoints { points } => Ok(points),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ips_filter",
                response,
            }),
        }
    }

    pub fn ips_batch_query_last(
        &self,
        keys: Vec<String>,
        count: usize,
    ) -> Result<Vec<(String, Vec<FeaturePoint>)>, ClientError> {
        match self
            .execute(Command::IpsBatchQueryLast { keys, count })?
            .response
        {
            CommandResponse::FeaturePointGroups { groups } => Ok(groups),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ips_batch_query_last",
                response,
            }),
        }
    }

    pub fn ips_remove(
        &self,
        key: impl Into<String>,
        timestamp_ms: u64,
    ) -> Result<bool, ClientError> {
        match self
            .execute(Command::IpsRemove {
                key: key.into(),
                timestamp_ms,
            })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value != 0),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ips_remove",
                response,
            }),
        }
    }

    pub fn ips_delete(&self, key: impl Into<String>) -> Result<bool, ClientError> {
        match self
            .execute(Command::IpsDelete { key: key.into() })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value != 0),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ips_delete",
                response,
            }),
        }
    }

    pub fn ips_count(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<i64, ClientError> {
        match self
            .execute(Command::IpsCount {
                key: key.into(),
                start_ms,
                end_ms,
            })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ips_count",
                response,
            }),
        }
    }

    pub fn ips_query_range_with_options(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
        action_type: Option<u32>,
        table_id: Option<u64>,
    ) -> Result<Vec<FeaturePoint>, ClientError> {
        match self
            .execute(Command::IpsQueryRangeWithOptions {
                key: key.into(),
                start_ms,
                end_ms,
                count,
                action_type,
                table_id,
            })?
            .response
        {
            CommandResponse::FeaturePoints { points } => Ok(points),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ips_query_range_with_options",
                response,
            }),
        }
    }

    pub fn context_upsert_node(
        &self,
        tenant_hash: u64,
        node: ContextNode,
    ) -> Result<String, ClientError> {
        match self
            .execute(Command::ContextUpsertNode { tenant_hash, node })?
            .response
        {
            CommandResponse::ContextObjectKey { object_key } => Ok(object_key),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_upsert_node",
                response,
            }),
        }
    }

    pub fn context_get_node(
        &self,
        tenant_hash: u64,
        node_hash: u64,
    ) -> Result<Option<ContextNode>, ClientError> {
        match self
            .execute(Command::ContextGetNode {
                tenant_hash,
                node_hash,
            })?
            .response
        {
            CommandResponse::ContextNode { node, .. } => Ok(node),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_get_node",
                response,
            }),
        }
    }

    pub fn context_write_event(
        &self,
        tenant_hash: u64,
        node_hash: u64,
        event: ContextEvent,
        first_write_only: bool,
    ) -> Result<String, ClientError> {
        match self
            .execute(Command::ContextWriteEvent {
                tenant_hash,
                node_hash,
                event,
                first_write_only,
            })?
            .response
        {
            CommandResponse::ContextObjectKey { object_key } => Ok(object_key),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_write_event",
                response,
            }),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn context_query_events(
        &self,
        tenant_hash: u64,
        node_hash: u64,
        start_time_ms: u64,
        end_time_ms: u64,
        limit: Option<usize>,
        current_valid_only: bool,
        as_of_ms: u64,
        kinds: Vec<u32>,
        statuses: Vec<u32>,
        min_confidence: f32,
        min_importance: f32,
    ) -> Result<Vec<ContextEvent>, ClientError> {
        match self
            .execute(Command::ContextQueryEvents {
                tenant_hash,
                node_hash,
                start_time_ms,
                end_time_ms,
                limit,
                current_valid_only,
                as_of_ms,
                kinds,
                statuses,
                min_confidence,
                min_importance,
            })?
            .response
        {
            CommandResponse::ContextEvents { events, .. } => Ok(events),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_query_events",
                response,
            }),
        }
    }

    pub fn context_write_index_ref(
        &self,
        tenant_hash: u64,
        index_name: impl Into<String>,
        index_value_hash: u64,
        scope_hash: u64,
        event_time_ms: u64,
        index_ref: ContextIndexRef,
    ) -> Result<String, ClientError> {
        match self
            .execute(Command::ContextWriteIndexRef {
                tenant_hash,
                index_name: index_name.into(),
                index_value_hash,
                scope_hash,
                event_time_ms,
                index_ref,
            })?
            .response
        {
            CommandResponse::ContextObjectKey { object_key } => Ok(object_key),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_write_index_ref",
                response,
            }),
        }
    }

    pub fn context_query_index(
        &self,
        tenant_hash: u64,
        index_name: impl Into<String>,
        index_value_hash: u64,
        scope_hash: u64,
        start_time_ms: u64,
        end_time_ms: u64,
        limit: Option<usize>,
    ) -> Result<Vec<ContextIndexRef>, ClientError> {
        match self
            .execute(Command::ContextQueryIndex {
                tenant_hash,
                index_name: index_name.into(),
                index_value_hash,
                scope_hash,
                start_time_ms,
                end_time_ms,
                limit,
            })?
            .response
        {
            CommandResponse::ContextIndexRefs { refs, .. } => Ok(refs),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_query_index",
                response,
            }),
        }
    }

    pub fn context_write_pack_audit(
        &self,
        tenant_hash: u64,
        audit: ContextPackAudit,
    ) -> Result<String, ClientError> {
        match self
            .execute(Command::ContextWritePackAudit { tenant_hash, audit })?
            .response
        {
            CommandResponse::ContextObjectKey { object_key } => Ok(object_key),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_write_pack_audit",
                response,
            }),
        }
    }

    pub fn context_query_pack_audit(
        &self,
        tenant_hash: u64,
        session_hash: u64,
        start_time_ms: u64,
        end_time_ms: u64,
        limit: Option<usize>,
    ) -> Result<Vec<ContextPackAudit>, ClientError> {
        match self
            .execute(Command::ContextQueryPackAudit {
                tenant_hash,
                session_hash,
                start_time_ms,
                end_time_ms,
                limit,
            })?
            .response
        {
            CommandResponse::ContextPackAudits { audits, .. } => Ok(audits),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_query_pack_audit",
                response,
            }),
        }
    }

    pub fn context_mark_summary_dirty(
        &self,
        tenant_hash: u64,
        marker: ContextSummaryDirtyMarker,
    ) -> Result<String, ClientError> {
        match self
            .execute(Command::ContextMarkSummaryDirty {
                tenant_hash,
                marker,
            })?
            .response
        {
            CommandResponse::ContextObjectKey { object_key } => Ok(object_key),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_mark_summary_dirty",
                response,
            }),
        }
    }

    pub fn context_query_summary_dirty(
        &self,
        tenant_hash: u64,
        node_hash: u64,
        start_time_ms: u64,
        end_time_ms: u64,
        limit: Option<usize>,
    ) -> Result<Vec<ContextSummaryDirtyMarker>, ClientError> {
        match self
            .execute(Command::ContextQuerySummaryDirty {
                tenant_hash,
                node_hash,
                start_time_ms,
                end_time_ms,
                limit,
            })?
            .response
        {
            CommandResponse::ContextSummaryDirtyMarkers { markers, .. } => Ok(markers),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_query_summary_dirty",
                response,
            }),
        }
    }

    pub fn risk_increment(
        &self,
        key: impl Into<String>,
        timestamp_ms: u64,
        amount: i64,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::RiskIncrement {
            key: key.into(),
            timestamp_ms,
            amount,
        })
    }

    pub fn risk_increment_with_options(
        &self,
        key: impl Into<String>,
        timestamp_ms: u64,
        amount: i64,
        precision_ms: Option<u64>,
        ttl_ms: Option<u64>,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::RiskIncrementWithOptions {
            key: key.into(),
            timestamp_ms,
            amount,
            precision_ms,
            ttl_ms,
        })
    }

    pub fn risk_change_add(
        &self,
        key: impl Into<String>,
        timestamp_ms: u64,
        value: impl Into<Vec<u8>>,
        precision_ms: Option<u64>,
        ttl_ms: Option<u64>,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::RiskChangeAdd {
            key: key.into(),
            timestamp_ms,
            value: value.into(),
            precision_ms,
            ttl_ms,
        })
    }

    pub fn risk_count(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<i64, ClientError> {
        match self
            .execute(Command::RiskCount {
                key: key.into(),
                start_ms,
                end_ms,
            })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "risk_count",
                response,
            }),
        }
    }

    pub fn risk_query(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        aggregator: impl Into<String>,
    ) -> Result<i64, ClientError> {
        match self
            .execute(Command::RiskQuery {
                key: key.into(),
                start_ms,
                end_ms,
                aggregator: aggregator.into(),
            })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "risk_query",
                response,
            }),
        }
    }

    pub fn risk_detail(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
    ) -> Result<Vec<FeaturePoint>, ClientError> {
        match self
            .execute(Command::RiskDetail {
                key: key.into(),
                start_ms,
                end_ms,
                count,
            })?
            .response
        {
            CommandResponse::FeaturePoints { points } => Ok(points),
            response => Err(ClientError::UnexpectedResponse {
                operation: "risk_detail",
                response,
            }),
        }
    }

    pub fn risk_family_set(
        &self,
        family: RiskFamily,
        key: impl Into<String>,
        timestamp_ms: u64,
        amount: i64,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::RiskSet {
            family,
            key: key.into(),
            timestamp_ms,
            amount,
        })
    }

    pub fn risk_family_query(
        &self,
        family: RiskFamily,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        aggregator: impl Into<String>,
    ) -> Result<i64, ClientError> {
        match self
            .execute(Command::RiskFamilyQuery {
                family,
                key: key.into(),
                start_ms,
                end_ms,
                aggregator: aggregator.into(),
            })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "risk_family_query",
                response,
            }),
        }
    }

    pub fn risk_family_set_and_get(
        &self,
        family: RiskFamily,
        key: impl Into<String>,
        timestamp_ms: u64,
        amount: i64,
        start_ms: u64,
        end_ms: u64,
        aggregator: impl Into<String>,
    ) -> Result<i64, ClientError> {
        match self
            .execute(Command::RiskSetAndGet {
                family,
                key: key.into(),
                timestamp_ms,
                amount,
                start_ms,
                end_ms,
                aggregator: aggregator.into(),
            })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "risk_family_set_and_get",
                response,
            }),
        }
    }

    pub fn risk_fol_set(
        &self,
        key: impl Into<String>,
        value: impl Into<Vec<u8>>,
        occur_time_ms: u64,
        ttl_ms: u64,
        fol_type: RiskFolType,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::RiskFolSet {
            key: key.into(),
            value: value.into(),
            occur_time_ms,
            ttl_ms,
            fol_type,
        })
    }

    pub fn risk_fol_query(&self, key: impl Into<String>) -> Result<Option<Vec<u8>>, ClientError> {
        match self
            .execute(Command::RiskFolQuery { key: key.into() })?
            .response
        {
            CommandResponse::Bytes { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "risk_fol_query",
                response,
            }),
        }
    }

    pub fn risk_manager(
        &self,
        key: impl Into<String>,
    ) -> Result<Vec<(String, Vec<u8>)>, ClientError> {
        match self
            .execute(Command::RiskManager { key: key.into() })?
            .response
        {
            CommandResponse::HashEntries { entries } => Ok(entries),
            response => Err(ClientError::UnexpectedResponse {
                operation: "risk_manager",
                response,
            }),
        }
    }

    pub fn risk_debug(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<Vec<(String, Vec<u8>)>, ClientError> {
        match self
            .execute(Command::RiskDebug {
                key: key.into(),
                start_ms,
                end_ms,
            })?
            .response
        {
            CommandResponse::HashEntries { entries } => Ok(entries),
            response => Err(ClientError::UnexpectedResponse {
                operation: "risk_debug",
                response,
            }),
        }
    }

    pub fn execute(&self, command: Command) -> Result<ExecuteResponse, ClientError> {
        self.client
            .inner
            .stats
            .lock()
            .expect("client stats lock poisoned")
            .execute_requests += 1;
        let write = is_write(&command);
        if write {
            self.refresh_table_topology_before_write_if_due()?;
        }
        let shard_id = self.shard_id_for_command(&command);
        let table_options = self.table_options();
        if command_is_dropped(&command, table_options.drop_percent) {
            return Ok(ExecuteResponse {
                status: Status::error("traffic_dropped", "request dropped by table drop_percent"),
                response: CommandResponse::Empty,
            });
        }
        let force_primary = write || table_options.pin_primary;
        let retry_budget_attempts = retry_attempts_for(&table_options, write);
        let mut attempt = 0;
        let mut topology_refresh_used = false;
        let response = loop {
            let current = self.client.execute_routed_with_http_and_policy(
                ExecuteRequest {
                    shard_id,
                    command: command.clone(),
                },
                force_primary,
                self.http_options(),
                Some(table_options.continuous_failed_time_ms),
                table_options.replica_read_policy,
                if table_options.preferred_location.is_empty() {
                    None
                } else {
                    Some(table_options.preferred_location.as_str())
                },
            )?;
            let decision = classify_cpp_retry_decision(
                &current.status,
                write,
                attempt,
                retry_budget_attempts,
                topology_refresh_used,
            );
            if current.status.ok || !decision.would_retry {
                break current;
            }
            if decision.topology_retry && !topology_refresh_used {
                topology_refresh_used = true;
                self.refresh_table_topology_after_status();
            }
            sleep_before_retry(&table_options, attempt);
            attempt += 1;
        };
        if !response.status.ok {
            return Err(ClientError::Status(response.status.message));
        }
        Ok(response)
    }

    pub fn batch_execute(
        &self,
        commands: Vec<Command>,
    ) -> Result<BatchExecuteResponse, ClientError> {
        self.client
            .inner
            .stats
            .lock()
            .expect("client stats lock poisoned")
            .batch_execute_requests += 1;
        let write = commands.iter().any(is_write);
        if write {
            self.refresh_table_topology_before_write_if_due()?;
        }
        let table_options = self.table_options();
        if let Some(command) = commands
            .iter()
            .find(|command| command_is_dropped(command, table_options.drop_percent))
        {
            return Ok(BatchExecuteResponse {
                status: Status::error(
                    "traffic_dropped",
                    format!(
                        "batch command for key {:?} dropped by table drop_percent",
                        command_key(command)
                    ),
                ),
                responses: Vec::new(),
            });
        }
        if self.options.shard_count > 1 {
            return self.batch_execute_grouped_by_shard(commands);
        }
        let request = BatchExecuteRequest {
            shard_id: self.shard_id,
            commands,
        };
        self.batch_execute_single_shard_with_retry(request, table_options)
    }

    fn batch_execute_single_shard_with_retry(
        &self,
        request: BatchExecuteRequest,
        table_options: TableOptions,
    ) -> Result<BatchExecuteResponse, ClientError> {
        let write = request.commands.iter().any(is_write);
        let retry_budget_attempts = retry_attempts_for(&table_options, write);
        let mut attempt = 0;
        let mut topology_refresh_used = false;
        let response = loop {
            let current = self.client.batch_execute_with_http(
                request.clone(),
                self.http_options(),
                Some(table_options.continuous_failed_time_ms),
            )?;
            let decision = classify_cpp_retry_decision(
                &current.status,
                write,
                attempt,
                retry_budget_attempts,
                topology_refresh_used,
            );
            if current.status.ok || !decision.would_retry {
                break current;
            }
            if decision.topology_retry && !topology_refresh_used {
                topology_refresh_used = true;
                self.refresh_table_topology_after_status();
            }
            sleep_before_retry(&table_options, attempt);
            attempt += 1;
        };
        Ok(response)
    }

    fn refresh_table_topology_before_write_if_due(&self) -> Result<(), ClientError> {
        if self.client.inner.options.meta_addr.is_none() {
            return Ok(());
        }
        let key = table_combine_name(&self.namespace, &self.table_name);
        let due = self
            .client
            .inner
            .meta_sync_tables
            .lock()
            .expect("client meta sync table lock poisoned")
            .get(&key)
            .map(|state| state.next_sync_after_unix_ms <= now_unix_ms())
            .unwrap_or(false);
        if due {
            self.client
                .sync_table_topology(self.namespace.clone(), self.table_name.clone())?;
        }
        Ok(())
    }

    fn batch_execute_grouped_by_shard(
        &self,
        commands: Vec<Command>,
    ) -> Result<BatchExecuteResponse, ClientError> {
        let mut groups: BTreeMap<ShardId, Vec<(usize, Command)>> = BTreeMap::new();
        let total = commands.len();
        for (index, command) in commands.into_iter().enumerate() {
            groups
                .entry(self.shard_id_for_command(&command))
                .or_default()
                .push((index, command));
        }

        let mut responses: Vec<Option<ExecuteResponse>> = vec![None; total];
        for (shard_id, group) in groups {
            let indexes: Vec<usize> = group.iter().map(|(index, _)| *index).collect();
            let commands: Vec<Command> = group.into_iter().map(|(_, command)| command).collect();
            let response = self.batch_execute_single_shard_with_retry(
                BatchExecuteRequest { shard_id, commands },
                self.table_options(),
            )?;
            if !response.status.ok {
                return Ok(BatchExecuteResponse {
                    status: response.status,
                    responses: Vec::new(),
                });
            }
            if response.responses.len() != indexes.len() {
                return Ok(BatchExecuteResponse {
                    status: Status::error("bad_response", "batch response length mismatch"),
                    responses: Vec::new(),
                });
            }
            for (index, response) in indexes.into_iter().zip(response.responses.into_iter()) {
                responses[index] = Some(response);
            }
        }

        Ok(BatchExecuteResponse {
            status: Status::ok(),
            responses: responses
                .into_iter()
                .map(|response| {
                    response.unwrap_or_else(|| ExecuteResponse {
                        status: Status::error("missing_response", "batch response missing"),
                        response: CommandResponse::Empty,
                    })
                })
                .collect(),
        })
    }

    fn shard_id_for_command(&self, command: &Command) -> ShardId {
        command_routing_key(command)
            .as_deref()
            .map(|key| self.shard_id_for_key(key))
            .unwrap_or(self.shard_id)
    }

    fn expect_empty(&self, command: Command) -> Result<(), ClientError> {
        match self.execute(command)?.response {
            CommandResponse::Empty => Ok(()),
            response => Err(ClientError::UnexpectedResponse {
                operation: "empty",
                response,
            }),
        }
    }

    fn http_options(&self) -> HttpRequestOptions {
        HttpRequestOptions {
            connect_timeout_ms: self.options.connect_timeout_ms,
            io_timeout_ms: self.options.io_timeout_ms,
            max_retries: self.client.inner.options.max_retries,
        }
    }

    fn refresh_table_topology_after_status(&self) {
        if self.client.inner.options.meta_addr.is_none() {
            return;
        }
        let _ = self
            .client
            .sync_table_topology(self.namespace.clone(), self.table_name.clone());
    }
}

fn choose_cached_route(
    route: &mut CachedRoute,
    replica_read_policy: ReplicaReadPolicy,
    preferred_location: Option<&str>,
) -> String {
    match replica_read_policy {
        ReplicaReadPolicy::PinPrimary => route.primary_addr.clone(),
        ReplicaReadPolicy::FirstReplica => {
            choose_location_affine_replica(route, preferred_location)
                .or_else(|| route.replica_addrs.first().cloned())
                .unwrap_or_else(|| route.primary_addr.clone())
        }
        ReplicaReadPolicy::RoundRobinReplica => {
            if let Some(replica) = choose_location_affine_replica(route, preferred_location) {
                return replica;
            }
            if route.replica_addrs.is_empty() {
                return route.primary_addr.clone();
            }
            let replica =
                route.replica_addrs[route.next_replica_index % route.replica_addrs.len()].clone();
            route.next_replica_index = (route.next_replica_index + 1) % route.replica_addrs.len();
            replica
        }
    }
}

fn choose_location_affine_replica(
    route: &CachedRoute,
    preferred_location: Option<&str>,
) -> Option<String> {
    let preferred_location = preferred_location?.trim();
    if preferred_location.is_empty() {
        return None;
    }
    route
        .replica_endpoints
        .iter()
        .find(|endpoint| endpoint.location == preferred_location)
        .map(|endpoint| endpoint.server_addr.clone())
}

#[derive(Debug, Clone)]
pub struct TemporalStorePipeline {
    table: TemporalStoreTable,
    commands: Vec<Command>,
}

impl TemporalStorePipeline {
    pub fn set(&mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) {
        self.commands.push(Command::StringSet {
            key: key.into(),
            value: value.into(),
        });
    }

    pub fn get(&mut self, key: impl Into<String>) {
        self.commands.push(Command::StringGet { key: key.into() });
    }

    pub fn del(&mut self, key: impl Into<String>) {
        self.commands
            .push(Command::CommonDelete { key: key.into() });
    }

    pub fn hset(
        &mut self,
        key: impl Into<String>,
        field: impl Into<String>,
        value: impl Into<Vec<u8>>,
    ) {
        self.commands.push(Command::HashSet {
            key: key.into(),
            field: field.into(),
            value: value.into(),
        });
    }

    pub fn hget(&mut self, key: impl Into<String>, field: impl Into<String>) {
        self.commands.push(Command::HashGet {
            key: key.into(),
            field: field.into(),
        });
    }

    pub fn hdel(&mut self, key: impl Into<String>, field: impl Into<String>) {
        self.commands.push(Command::HashDelete {
            key: key.into(),
            field: field.into(),
        });
    }

    pub fn hmset(&mut self, key: impl Into<String>, entries: Vec<(String, Vec<u8>)>) {
        self.commands.push(Command::HashMultiSet {
            key: key.into(),
            entries,
        });
    }

    pub fn hmget(&mut self, key: impl Into<String>, fields: Vec<String>) {
        self.commands.push(Command::HashMultiGet {
            key: key.into(),
            fields,
        });
    }

    pub fn sync(&mut self) -> Result<BatchExecuteResponse, ClientError> {
        if self.commands.is_empty() {
            return Ok(BatchExecuteResponse {
                status: Status::ok(),
                responses: Vec::new(),
            });
        }
        let commands = std::mem::take(&mut self.commands);
        self.table.batch_execute(commands)
    }
}

fn is_write(command: &Command) -> bool {
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
            | Command::ContextUpsertNode { .. }
            | Command::ContextWriteEvent { .. }
            | Command::ContextWriteIndexRef { .. }
            | Command::ContextWritePackAudit { .. }
            | Command::ContextMarkSummaryDirty { .. }
    )
}

fn table_combine_name(namespace: &str, table_name: &str) -> String {
    format!("{namespace}/{table_name}")
}

fn topology_event_affects_routes(event: &crate::meta::TopologyChangeEvent) -> bool {
    event.resource.starts_with("table:")
        || matches!(
            event.kind.as_str(),
            "register_shard"
                | "finish_load"
                | "publish_shard_snapshot"
                | "register_server"
                | "server_state"
                | "add_table"
                | "delete_table"
                | "update_table"
                | "table_state"
        )
}

fn sleep_meta_sync_tick(tick_ms: u64, stop: &AtomicBool) {
    let deadline = Instant::now() + Duration::from_millis(tick_ms.max(1));
    while Instant::now() < deadline {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        thread::sleep(
            Duration::from_millis(10).min(deadline.saturating_duration_since(Instant::now())),
        );
    }
}

fn duration_ms_u64(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

pub fn shard_id_for_key(
    key: &str,
    first_shard_id: ShardId,
    shard_count: u64,
    default_shard_id: ShardId,
) -> ShardId {
    if shard_count == 0 {
        return default_shard_id;
    }
    first_shard_id + slot_id_for_key(key) % shard_count
}

pub fn slot_id_for_key(key: &str) -> u64 {
    crc64_jones(key.as_bytes()) >> 34
}

pub fn crc64_jones(bytes: &[u8]) -> u64 {
    let mut crc = 0_u64;
    for byte in bytes {
        let mut entry = (crc ^ u64::from(*byte)) & 0xff;
        for _ in 0..8 {
            if entry & 1 == 1 {
                entry = (entry >> 1) ^ 0x95ac9329ac4bc9b5;
            } else {
                entry >>= 1;
            }
        }
        crc = entry ^ (crc >> 8);
    }
    crc
}

pub fn stable_key_hash(key: &str) -> u64 {
    crc64_jones(key.as_bytes())
}

pub fn key_is_dropped_by_percent(key: &str, drop_percent: u8) -> bool {
    let drop_percent = drop_percent.min(100);
    drop_percent > 0 && (stable_key_hash(key) % 100) < u64::from(drop_percent)
}

fn command_is_dropped(command: &Command, drop_percent: u8) -> bool {
    command_routing_key(command)
        .as_deref()
        .map(|key| key_is_dropped_by_percent(key, drop_percent))
        .unwrap_or(false)
}

fn retry_attempts_for(options: &TableOptions, write: bool) -> usize {
    let retries = if write {
        options.max_write_retries
    } else {
        options.max_read_retries
    };
    retries.saturating_add(1)
}

fn replica_read_policy_from_meta(policy: &str) -> ReplicaReadPolicy {
    match policy {
        "first_replica" => ReplicaReadPolicy::FirstReplica,
        "round_robin_replica" => ReplicaReadPolicy::RoundRobinReplica,
        _ => ReplicaReadPolicy::PinPrimary,
    }
}

fn sleep_before_retry(options: &TableOptions, attempt: usize) {
    if options.retry_backoff_ms == 0 {
        return;
    }
    let multiplier = u64::try_from(attempt.saturating_add(1)).unwrap_or(u64::MAX);
    let sleep_ms = options.retry_backoff_ms.saturating_mul(multiplier);
    thread::sleep(Duration::from_millis(sleep_ms));
}

fn classify_cpp_retry_decision(
    status: &Status,
    write: bool,
    attempt: usize,
    retry_budget_attempts: usize,
    topology_refresh_used: bool,
) -> ClientRetryDecision {
    let retryable = status_is_cpp_retryable(status);
    let topology_retry = status_is_cpp_topology_retryable(status);
    let has_budget = attempt + 1 < retry_budget_attempts;
    let safe_budget_free_write_retry = write && topology_retry && !topology_refresh_used;
    let would_retry = retryable
        && (has_budget
            || (!write && topology_retry && !topology_refresh_used)
            || safe_budget_free_write_retry);
    ClientRetryDecision {
        retryable,
        topology_retry,
        safe_budget_free_write_retry,
        would_retry,
    }
}

fn status_is_cpp_retryable(status: &Status) -> bool {
    if status.ok {
        return false;
    }
    let code = normalize_status_code(&status.code);
    matches!(
        code.as_str(),
        "deadlineexceeded"
            | "deadline_exceeded"
            | "unavailable"
            | "internal"
            | "retrylater"
            | "retry_later"
            | "partitionloading"
            | "partition_loading"
            | "metachanged"
            | "meta_changed"
            | "topomerror"
            | "topom_error"
            | "notserving"
            | "not_serving"
            | "staleloadversion"
            | "stale_load_version"
    )
}

fn status_is_cpp_topology_retryable(status: &Status) -> bool {
    if status.ok {
        return false;
    }
    let code = normalize_status_code(&status.code);
    matches!(
        code.as_str(),
        "partitionloading"
            | "partition_loading"
            | "metachanged"
            | "meta_changed"
            | "topomerror"
            | "topom_error"
            | "notserving"
            | "not_serving"
            | "staleloadversion"
            | "stale_load_version"
    )
}

fn normalize_status_code(code: &str) -> String {
    code.chars()
        .filter(|ch| *ch != '-' && *ch != ' ')
        .flat_map(char::to_lowercase)
        .collect()
}

fn command_key(command: &Command) -> Option<&str> {
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
        | Command::IpsSnapshotReport { key, .. }
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
        | Command::RiskManager { key }
        | Command::RiskDebug { key, .. } => Some(key),
        Command::IpsBatchQueryLast { .. }
        | Command::SequenceBatchQuery { .. }
        | Command::ContextUpsertNode { .. }
        | Command::ContextGetNode { .. }
        | Command::ContextWriteEvent { .. }
        | Command::ContextQueryEvents { .. }
        | Command::ContextWriteIndexRef { .. }
        | Command::ContextQueryIndex { .. }
        | Command::ContextWritePackAudit { .. }
        | Command::ContextQueryPackAudit { .. }
        | Command::ContextMarkSummaryDirty { .. }
        | Command::ContextQuerySummaryDirty { .. } => None,
    }
}

fn command_routing_key(command: &Command) -> Option<String> {
    command_key(command)
        .map(str::to_string)
        .or_else(|| context_command_key(command))
}

fn context_command_key(command: &Command) -> Option<String> {
    match command {
        Command::ContextUpsertNode { tenant_hash, node } => {
            Some(context_node_key(*tenant_hash, node.node_hash))
        }
        Command::ContextGetNode {
            tenant_hash,
            node_hash,
        } => Some(context_node_key(*tenant_hash, *node_hash)),
        Command::ContextWriteEvent {
            tenant_hash,
            node_hash,
            ..
        }
        | Command::ContextQueryEvents {
            tenant_hash,
            node_hash,
            ..
        } => Some(context_event_key(*tenant_hash, *node_hash)),
        Command::ContextWriteIndexRef {
            tenant_hash,
            index_name,
            index_value_hash,
            scope_hash,
            ..
        }
        | Command::ContextQueryIndex {
            tenant_hash,
            index_name,
            index_value_hash,
            scope_hash,
            ..
        } => Some(context_index_key(
            *tenant_hash,
            index_name,
            *index_value_hash,
            *scope_hash,
        )),
        Command::ContextWritePackAudit { tenant_hash, audit } => {
            Some(context_audit_key(*tenant_hash, audit.session_hash))
        }
        Command::ContextQueryPackAudit {
            tenant_hash,
            session_hash,
            ..
        } => Some(context_audit_key(*tenant_hash, *session_hash)),
        Command::ContextMarkSummaryDirty {
            tenant_hash,
            marker,
        } => Some(context_dirty_key(*tenant_hash, marker.node_hash)),
        Command::ContextQuerySummaryDirty {
            tenant_hash,
            node_hash,
            ..
        } => Some(context_dirty_key(*tenant_hash, *node_hash)),
        _ => None,
    }
}

fn context_node_key(tenant_hash: u64, node_hash: u64) -> String {
    format!("ctx:node:{tenant_hash}:{node_hash}")
}

fn context_event_key(tenant_hash: u64, node_hash: u64) -> String {
    format!("ctx:event:{tenant_hash}:{node_hash}")
}

fn context_index_key(
    tenant_hash: u64,
    index_name: &str,
    index_value_hash: u64,
    scope_hash: u64,
) -> String {
    format!("ctxidx:{tenant_hash}:{index_name}:{index_value_hash}:{scope_hash}")
}

fn context_audit_key(tenant_hash: u64, session_hash: u64) -> String {
    format!("ctx:audit:{tenant_hash}:{session_hash}")
}

fn context_dirty_key(tenant_hash: u64, node_hash: u64) -> String {
    format!("ctx:dirty:{tenant_hash}:{node_hash}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::TemporalEngine;
    use crate::http::{json_response, parse_json, serve};
    use crate::meta::{GetShardResponse, ShardLocation, TableMetaInfo, TablePartition};

    #[test]
    fn client_preflight_reports_cache_stats_and_backend_failures() {
        let client = TemporalStoreClient::with_options(ClientOptions {
            proxy_addr: "127.0.0.1:17000".to_string(),
            meta_addr: Some("127.0.0.1:17001".to_string()),
            default_shard_id: 7,
            ..ClientOptions::default()
        });
        let table = client.open_table(
            "ns",
            "tbl",
            TableOptions {
                first_shard_id: 7,
                ..TableOptions::default()
            },
        );
        client.insert_cached_route_for_test(7, "127.0.0.1:17002");
        client.insert_backend_failure_for_test("127.0.0.1:17002", 20, 10, 3);

        let report = client.preflight_report();
        assert_eq!(report.proxy_addr, "127.0.0.1:17000");
        assert_eq!(report.meta_addr.as_deref(), Some("127.0.0.1:17001"));
        assert_eq!(report.default_shard_id, 7);
        assert_eq!(report.route_cache_size, 1);
        assert_eq!(report.topology_cache.route_count, 1);
        assert_eq!(report.topology_cache.unknown_topology_version_routes, 1);
        assert_eq!(report.topology_cache.last_refresh_reason, "test_insert");
        assert_eq!(report.topology_cache.routes[0].shard_id, 7);
        assert_eq!(report.cpp_partition_sets.len(), 1);
        assert_eq!(report.cpp_partition_sets[0].namespace, "ns");
        assert_eq!(report.cpp_partition_sets[0].table_name, "tbl");
        assert_eq!(report.cpp_partition_sets[0].first_shard_id, 7);
        assert_eq!(report.cpp_partition_sets[0].partition_count, 1);
        assert_eq!(report.cpp_partition_sets[0].missing_route_count, 0);
        assert_eq!(report.cpp_partition_sets[0].members[0].partition_id, 7);
        assert_eq!(
            report.cpp_partition_sets[0].members[0]
                .primary_addr
                .as_deref(),
            Some("127.0.0.1:17002")
        );
        let stale = client.topology_cache_report_against(2);
        assert!(stale.cache_stale);
        assert_eq!(stale.authoritative_topology_version, 2);
        assert_eq!(stale.stale_route_count, 1);
        assert_eq!(report.table_cache_size, 1);
        assert_eq!(report.backend_failure_count, 1);
        assert_eq!(report.stats.open_table_calls, 1);
        assert_eq!(report.status.code, "degraded");
        assert_eq!(report.degraded_reasons, vec!["backend_failure_backlog"]);
        assert_eq!(table.shard_id(), 7);
    }

    #[test]
    fn client_exposes_neptune_placement_hooks_and_migration_scope() {
        let client = TemporalStoreClient::with_options(ClientOptions {
            proxy_addr: "127.0.0.1:17000".to_string(),
            local_location: "zone-a".to_string(),
            ..ClientOptions::default()
        });
        let policy = client.deployment_placement_policy("neptune-prod");
        assert_eq!(policy.deployment_name, "neptune-prod");
        assert!(policy.neptune_routing_enabled);
        assert_eq!(policy.preferred_location, "zone-a");
        assert_eq!(
            policy.replica_read_policy,
            ReplicaReadPolicy::RoundRobinReplica
        );
        assert!(policy.require_location_affinity);
        assert!(policy.placement_hook_ready);

        let mut table_options = TableOptions::default();
        policy.apply_to_table_options(&mut table_options);
        assert_eq!(table_options.preferred_location, "zone-a");
        assert_eq!(
            table_options.replica_read_policy,
            ReplicaReadPolicy::RoundRobinReplica
        );

        let migration = client.migration_compatibility_report();
        assert_eq!(
            migration.compatibility_mode,
            ClientCompatibilityMode::CppWireMigrationOutOfScope
        );
        assert!(migration.rust_native_http_ready);
        assert!(migration.rust_native_tonic_ready);
        assert!(!migration.brpc_thrift_in_scope);
        assert!(!migration.cpp_wire_compatible_ready);
        assert!(!migration.migration_layer_ready);
        assert!(migration.typed_table_client_ready);
        assert!(migration.topology_sync_ready);
        assert!(migration.retry_budgets_ready);
        assert!(migration.neptune_routing_hooks_ready);
        assert!(migration.placement_hooks_ready);
        assert_eq!(
            migration
                .production_replacement_contract
                .compatibility_decision,
            "brpc/thrift migration shims are out of scope; use Rust-native migration contract"
        );
        assert!(migration
            .production_replacement_contract
            .production_protocols
            .contains(&"HTTP/JSON".to_string()));
        assert!(migration
            .production_replacement_contract
            .production_protocols
            .contains(&"tonic".to_string()));
        assert!(
            migration
                .production_replacement_contract
                .typed_table_client_preserved
        );
        assert!(
            migration
                .production_replacement_contract
                .topology_sync_preserved
        );
        assert!(
            migration
                .production_replacement_contract
                .retry_budget_preserved
        );
        assert!(
            migration
                .production_replacement_contract
                .neptune_routing_hooks_preserved
        );
        assert!(
            migration
                .production_replacement_contract
                .placement_hooks_preserved
        );
        assert!(migration
            .blockers
            .iter()
            .any(|blocker| blocker.contains("brpc/thrift")));
    }

    #[test]
    fn cpp_partition_set_report_marks_missing_routes() {
        let client = TemporalStoreClient::new("127.0.0.1:17000");
        let _table = client.open_table(
            "ns",
            "wide",
            TableOptions {
                first_shard_id: 10,
                shard_count: 3,
                ..TableOptions::default()
            },
        );
        client.insert_cached_route_for_test(11, "127.0.0.1:17111");

        let reports = client.cpp_partition_set_report();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].combine_name, "ns/wide");
        assert_eq!(reports[0].partition_count, 3);
        assert_eq!(reports[0].missing_route_count, 2);
        assert_eq!(
            reports[0]
                .members
                .iter()
                .map(|member| (member.partition_id, member.route_ready))
                .collect::<Vec<_>>(),
            vec![(10, false), (11, true), (12, false)]
        );
        assert_eq!(
            reports[0].members[1].primary_addr.as_deref(),
            Some("127.0.0.1:17111")
        );
    }

    #[test]
    fn table_typed_methods_and_pipeline_match_cpp_client_shape() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        let server_addr = free_local_addr();
        let proxy_addr = free_local_addr();
        let engine_for_server = engine.clone();
        let server_addr_for_listener = server_addr.clone();
        std::thread::spawn(move || {
            serve(&server_addr_for_listener, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/execute") => {
                        let req = parse_json::<ExecuteRequest>(&request.body).unwrap();
                        json_response(200, &engine_for_server.execute(req))
                    }
                    ("POST", "/batch_execute") => {
                        let req = parse_json::<BatchExecuteRequest>(&request.body).unwrap();
                        json_response(200, &engine_for_server.batch_execute(req))
                    }
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });
        let server_addr_for_proxy = server_addr.clone();
        let proxy_addr_for_listener = proxy_addr.clone();
        std::thread::spawn(move || {
            serve(&proxy_addr_for_listener, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("GET", "/shards/1") => json_response(
                        200,
                        &GetShardResponse {
                            status: Status::ok(),
                            location: Some(ShardLocation {
                                shard_id: 1,
                                server_addr: server_addr_for_proxy.clone(),
                                latest_snapshot: None,
                            }),
                        },
                    ),
                    ("POST", "/execute") => {
                        let req = parse_json::<ExecuteRequest>(&request.body).unwrap();
                        let resp: ExecuteResponse =
                            post_json(&server_addr_for_proxy, "/execute", &req).unwrap();
                        json_response(200, &resp)
                    }
                    ("POST", "/batch_execute") => {
                        let req = parse_json::<BatchExecuteRequest>(&request.body).unwrap();
                        let resp: BatchExecuteResponse =
                            post_json(&server_addr_for_proxy, "/batch_execute", &req).unwrap();
                        json_response(200, &resp)
                    }
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });
        wait_for_http(&proxy_addr);

        let client = TemporalStoreClient::with_options(ClientOptions {
            proxy_addr: proxy_addr.clone(),
            max_retries: 1,
            ..ClientOptions::default()
        });
        let table = client.open_table("ns", "tbl", TableOptions::default());
        assert_eq!(table.namespace(), "ns");
        assert_eq!(table.table_name(), "tbl");

        table.hset("hk", "f", b"hv".to_vec()).unwrap();
        assert_eq!(table.hget("hk", "f").unwrap(), Some(b"hv".to_vec()));
        table.set("sk", b"sv".to_vec()).unwrap();
        assert_eq!(table.get("sk").unwrap(), Some(b"sv".to_vec()));
        table.setex("ttl", b"v".to_vec(), 10_000).unwrap();
        assert!(table.ttl("ttl").unwrap() > 0);
        table
            .feature_append(
                "feature",
                vec![
                    FeaturePoint {
                        timestamp_ms: 10,
                        value: b"2".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 20,
                        value: b"3".to_vec(),
                    },
                ],
            )
            .unwrap();
        assert_eq!(
            table.feature_query("feature", 0, 30, None).unwrap(),
            vec![
                FeaturePoint {
                    timestamp_ms: 10,
                    value: b"2".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 20,
                    value: b"3".to_vec(),
                },
            ]
        );
        assert_eq!(
            table
                .feature_agg_query("feature", 0, 30, "sum", None)
                .unwrap(),
            5
        );
        table
            .feature_replace(
                "feature",
                10,
                20,
                vec![FeaturePoint {
                    timestamp_ms: 15,
                    value: b"9".to_vec(),
                }],
            )
            .unwrap();
        assert_eq!(
            table
                .feature_agg_query("feature", 0, 30, "max", None)
                .unwrap(),
            9
        );
        table.feature_delete("feature").unwrap();
        assert!(table
            .feature_query("feature", 0, 30, None)
            .unwrap()
            .is_empty());
        table.ips_add("ips-a", 10, b"a10".to_vec()).unwrap();
        table.ips_add("ips-a", 20, b"a20".to_vec()).unwrap();
        table.ips_add("ips-b", 15, b"b15".to_vec()).unwrap();
        assert_eq!(
            table.ips_query_range("ips-a", 0, 30, Some(1)).unwrap(),
            vec![FeaturePoint {
                timestamp_ms: 10,
                value: b"a10".to_vec(),
            }]
        );
        assert_eq!(table.ips_count("ips-a", 0, 30).unwrap(), 2);
        assert_eq!(
            table
                .ips_batch_query_last(vec!["ips-a".to_string(), "ips-b".to_string()], 1)
                .unwrap(),
            vec![
                (
                    "ips-a".to_string(),
                    vec![FeaturePoint {
                        timestamp_ms: 20,
                        value: b"a20".to_vec(),
                    }],
                ),
                (
                    "ips-b".to_string(),
                    vec![FeaturePoint {
                        timestamp_ms: 15,
                        value: b"b15".to_vec(),
                    }],
                ),
            ]
        );
        assert!(table.ips_remove("ips-a", 10).unwrap());
        assert_eq!(table.ips_count("ips-a", 0, 30).unwrap(), 1);
        assert!(table.ips_delete("ips-a").unwrap());
        assert_eq!(
            table
                .ips_load(
                    "ips-load",
                    vec![
                        FeaturePoint {
                            timestamp_ms: 10,
                            value: b"l10".to_vec(),
                        },
                        FeaturePoint {
                            timestamp_ms: 20,
                            value: b"l20".to_vec(),
                        },
                    ],
                )
                .unwrap(),
            2
        );
        assert!(table
            .ips_add_with_options(
                "ips-load",
                30,
                b"opt30".to_vec(),
                Some(7),
                Some(42),
                Some("typed-req".to_string()),
            )
            .unwrap());
        assert_eq!(
            table.ips_snapshot("ips-load", 0, 25, None).unwrap(),
            vec![
                FeaturePoint {
                    timestamp_ms: 10,
                    value: b"l10".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 20,
                    value: b"l20".to_vec(),
                },
            ]
        );
        assert_eq!(
            table
                .ips_filter("ips-load", 0, 40, Some(10), Some(7), Some(42))
                .unwrap(),
            vec![FeaturePoint {
                timestamp_ms: 30,
                value: b"opt30".to_vec(),
            }]
        );
        assert_eq!(
            table.ips_stat("ips-load", 0, 40).unwrap(),
            IpsStats {
                total: 3,
                first_timestamp_ms: Some(10),
                last_timestamp_ms: Some(30),
                action_type_counts: vec![(7, 1)],
                table_id_counts: vec![(42, 1)],
            }
        );
        let snapshot_report = table
            .ips_snapshot_report("ips-load", 0, 40, Some(2))
            .unwrap();
        assert_eq!(snapshot_report.key, "ips-load");
        assert_eq!(snapshot_report.requested_count, Some(2));
        assert_eq!(snapshot_report.returned_count, 2);
        assert_eq!(snapshot_report.total_in_range, 3);
        assert_eq!(snapshot_report.action_type_counts, vec![(7, 1)]);
        assert_eq!(snapshot_report.table_id_counts, vec![(42, 1)]);
        assert_eq!(snapshot_report.packed_timestamped_page_count, 2);

        table.risk_increment("risk", 10, 5).unwrap();
        table.risk_increment("risk", 20, -2).unwrap();
        table.risk_increment("risk", 30, 7).unwrap();
        assert_eq!(table.risk_query("risk", 0, 40, "sum").unwrap(), 10);
        assert_eq!(table.risk_query("risk", 0, 40, "last").unwrap(), 7);
        assert_eq!(
            table.risk_detail("risk", 15, 40, Some(2)).unwrap(),
            vec![
                FeaturePoint {
                    timestamp_ms: 20,
                    value: b"-2".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 30,
                    value: b"7".to_vec(),
                },
            ]
        );
        table
            .risk_family_set(RiskFamily::H, "risk-cpp", 10, 5)
            .unwrap();
        assert_eq!(
            table
                .risk_family_set_and_get(RiskFamily::H, "risk-cpp", 20, 7, 0, 30, "sum")
                .unwrap(),
            12
        );
        table
            .risk_family_set(RiskFamily::Cpc, "risk-cpp", 10, 3)
            .unwrap();
        assert_eq!(
            table
                .risk_family_set_and_get(RiskFamily::Cpc, "risk-cpp", 20, 4, 0, 30, "sum")
                .unwrap(),
            7
        );
        table
            .risk_family_set(RiskFamily::Fol, "risk-cpp", 10, 11)
            .unwrap();
        assert_eq!(
            table
                .risk_family_query(RiskFamily::Fol, "risk-cpp", 0, 30, "sum")
                .unwrap(),
            11
        );
        table
            .risk_fol_set(
                "risk-fol-first",
                b"middle".to_vec(),
                20,
                60_000,
                RiskFolType::First,
            )
            .unwrap();
        table
            .risk_fol_set(
                "risk-fol-first",
                b"first".to_vec(),
                10,
                60_000,
                RiskFolType::First,
            )
            .unwrap();
        table
            .risk_fol_set(
                "risk-fol-first",
                b"last".to_vec(),
                30,
                60_000,
                RiskFolType::First,
            )
            .unwrap();
        assert_eq!(
            table.risk_fol_query("risk-fol-first").unwrap(),
            Some(b"first".to_vec())
        );
        table
            .risk_fol_set(
                "risk-fol-last",
                b"middle".to_vec(),
                20,
                60_000,
                RiskFolType::Last,
            )
            .unwrap();
        table
            .risk_fol_set(
                "risk-fol-last",
                b"first".to_vec(),
                10,
                60_000,
                RiskFolType::Last,
            )
            .unwrap();
        table
            .risk_fol_set(
                "risk-fol-last",
                b"last".to_vec(),
                30,
                60_000,
                RiskFolType::Last,
            )
            .unwrap();
        assert_eq!(
            table.risk_fol_query("risk-fol-last").unwrap(),
            Some(b"last".to_vec())
        );
        assert_eq!(
            table.risk_manager("risk-cpp").unwrap(),
            vec![
                ("h_events".to_string(), b"2".to_vec()),
                ("h_sum".to_string(), b"12".to_vec()),
                ("cpc_events".to_string(), b"2".to_vec()),
                ("cpc_sum".to_string(), b"7".to_vec()),
                ("fol_events".to_string(), b"1".to_vec()),
                ("fol_sum".to_string(), b"11".to_vec()),
            ]
        );
        let debug = table.risk_debug("risk-cpp", 0, 15).unwrap();
        assert!(debug.contains(&("key".to_string(), b"risk-cpp".to_vec())));
        assert!(debug.contains(&("h_window_events".to_string(), b"1".to_vec())));
        assert!(debug.contains(&("cpc_window_sum".to_string(), b"3".to_vec())));
        assert!(debug.contains(&("fol_window_last_timestamp_ms".to_string(), b"10".to_vec())));

        let mut pipeline = table.pipeline();
        assert!(pipeline.sync().unwrap().responses.is_empty());
        pipeline.hset("pk", "pf", b"pv".to_vec());
        pipeline.hget("pk", "pf");
        let response = pipeline.sync().unwrap();
        assert_eq!(response.responses.len(), 2);
        assert_eq!(
            response.responses[1].response,
            CommandResponse::Bytes {
                value: Some(b"pv".to_vec())
            }
        );
    }

    #[test]
    fn direct_client_refreshes_cached_route_after_failure() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        let server_addr = free_local_addr();
        let meta_addr = free_local_addr();
        let engine_for_server = engine.clone();
        let server_addr_for_listener = server_addr.clone();
        std::thread::spawn(move || {
            serve(&server_addr_for_listener, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/execute") => {
                        let req = parse_json::<ExecuteRequest>(&request.body).unwrap();
                        json_response(200, &engine_for_server.execute(req))
                    }
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });
        let live_server = server_addr.clone();
        let meta_addr_for_listener = meta_addr.clone();
        std::thread::spawn(move || {
            serve(&meta_addr_for_listener, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("GET", "/shards/1") => json_response(
                        200,
                        &GetShardResponse {
                            status: Status::ok(),
                            location: Some(ShardLocation {
                                shard_id: 1,
                                server_addr: live_server.clone(),
                                latest_snapshot: None,
                            }),
                        },
                    ),
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });
        wait_for_http(&meta_addr);

        let client = TemporalStoreClient::with_options(ClientOptions {
            proxy_addr: "127.0.0.1:1".to_string(),
            meta_addr: Some(meta_addr.clone()),
            route_cache_ttl_ms: 60_000,
            connect_timeout_ms: 50,
            io_timeout_ms: 200,
            ..ClientOptions::default()
        });
        client.inner.routes.lock().unwrap().insert(
            1,
            CachedRoute {
                primary_addr: "127.0.0.1:1".to_string(),
                replica_addrs: Vec::new(),
                replica_endpoints: Vec::new(),
                next_replica_index: 0,
                fetched_at: Instant::now(),
                topology_version: 0,
                refresh_reason: "test_insert".to_string(),
            },
        );
        let table = client.open_table("ns", "tbl", TableOptions::default());
        table.set("k", b"v".to_vec()).unwrap();
        assert_eq!(table.get("k").unwrap(), Some(b"v".to_vec()));
        let stats = client.stats();
        assert_eq!(stats.backend_errors, 1);
        assert_eq!(stats.backend_error_streak, 0);
        assert_eq!(stats.backend_successes_after_error, 1);
    }

    #[test]
    fn table_write_refreshes_topology_after_meta_changed_without_write_retry_budget() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        let stale_addr = free_local_addr();
        let live_addr = free_local_addr();
        let meta_addr = free_local_addr();
        let stale_attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let stale_attempts_for_server = stale_attempts.clone();
        let stale_addr_for_listener = stale_addr.clone();
        std::thread::spawn(move || {
            serve(&stale_addr_for_listener, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/execute") => {
                        stale_attempts_for_server.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        json_response(
                            200,
                            &ExecuteResponse {
                                status: Status::error("meta_changed", "route moved"),
                                response: CommandResponse::Empty,
                            },
                        )
                    }
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });

        let engine_for_live = engine.clone();
        let live_addr_for_listener = live_addr.clone();
        std::thread::spawn(move || {
            serve(&live_addr_for_listener, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/execute") => {
                        let req = parse_json::<ExecuteRequest>(&request.body).unwrap();
                        json_response(200, &engine_for_live.execute(req))
                    }
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });

        let live_addr_for_meta = live_addr.clone();
        let meta_addr_for_listener = meta_addr.clone();
        std::thread::spawn(move || {
            serve(&meta_addr_for_listener, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/tables/topology") => json_response(
                        200,
                        &TableTopologyResponse {
                            status: Status::ok(),
                            table: Some(TableMetaInfo {
                                table_id: 7,
                                namespace: "ns".to_string(),
                                table_name: "tbl".to_string(),
                                state: crate::meta::MetaEntityState::Normal,
                                topology_version: 2,
                                first_shard_id: 1,
                                shard_count: 1,
                                replica_count: 1,
                                use_cpp_partition_ids: false,
                                partition_version: 0,
                                serving_options: crate::meta::TableServingOptions::default(),
                            }),
                            partitions: vec![TablePartition {
                                shard_id: 1,
                                start_slot: 0,
                                end_slot: u64::MAX,
                                primary: Some(live_addr_for_meta.clone()),
                                replicas: vec![live_addr_for_meta.clone()],
                                primary_endpoint: None,
                                replica_endpoints: Vec::new(),
                            }],
                            unchanged: false,
                        },
                    ),
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });
        wait_for_http(&stale_addr);
        wait_for_http(&live_addr);
        wait_for_http(&meta_addr);

        let client = TemporalStoreClient::with_options(ClientOptions {
            proxy_addr: "127.0.0.1:1".to_string(),
            meta_addr: Some(meta_addr.clone()),
            route_cache_ttl_ms: 60_000,
            max_write_retries: 0,
            retry_backoff_ms: 0,
            ..ClientOptions::default()
        });
        let table = client.open_table(
            "ns",
            "tbl",
            TableOptions {
                first_shard_id: 1,
                shard_count: 1,
                max_write_retries: 0,
                retry_backoff_ms: 0,
                ..TableOptions::default()
            },
        );
        client.insert_cached_route_for_test(1, stale_addr);

        table.set("k", b"v".to_vec()).unwrap();
        assert_eq!(table.get("k").unwrap(), Some(b"v".to_vec()));
        assert_eq!(stale_attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            client.topology_cache_report().routes[0].primary_addr,
            live_addr
        );
    }

    #[test]
    fn client_backend_pool_skips_cached_route_after_continuous_failure_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        let server_addr = free_local_addr();
        let meta_addr = free_local_addr();
        let engine_for_server = engine.clone();
        let server_addr_for_listener = server_addr.clone();
        std::thread::spawn(move || {
            serve(&server_addr_for_listener, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/execute") => {
                        let req = parse_json::<ExecuteRequest>(&request.body).unwrap();
                        json_response(200, &engine_for_server.execute(req))
                    }
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });
        let live_server = server_addr.clone();
        let meta_addr_for_listener = meta_addr.clone();
        std::thread::spawn(move || {
            serve(&meta_addr_for_listener, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("GET", "/shards/1") => json_response(
                        200,
                        &GetShardResponse {
                            status: Status::ok(),
                            location: Some(ShardLocation {
                                shard_id: 1,
                                server_addr: live_server.clone(),
                                latest_snapshot: None,
                            }),
                        },
                    ),
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });
        wait_for_http(&meta_addr);

        let bad_server = "127.0.0.1:1".to_string();
        let client = TemporalStoreClient::with_options(ClientOptions {
            proxy_addr: bad_server.clone(),
            meta_addr: Some(meta_addr.clone()),
            route_cache_ttl_ms: 60_000,
            connect_timeout_ms: 50,
            io_timeout_ms: 200,
            ..ClientOptions::default()
        });
        client.inner.routes.lock().unwrap().insert(
            1,
            CachedRoute {
                primary_addr: bad_server.clone(),
                replica_addrs: Vec::new(),
                replica_endpoints: Vec::new(),
                next_replica_index: 0,
                fetched_at: Instant::now(),
                topology_version: 0,
                refresh_reason: "test_insert".to_string(),
            },
        );
        client.inner.backend_failures.lock().unwrap().insert(
            bad_server,
            BackendFailureState {
                first_failed_at: Instant::now() - Duration::from_millis(20),
                last_failed_at: Instant::now() - Duration::from_millis(10),
                consecutive_failures: 3,
            },
        );

        let table = client.open_table(
            "ns",
            "tbl",
            TableOptions {
                continuous_failed_time_ms: 0,
                ..TableOptions::default()
            },
        );
        table.set("k", b"v".to_vec()).unwrap();
        assert_eq!(table.get("k").unwrap(), Some(b"v".to_vec()));

        let stats = client.stats();
        assert_eq!(stats.backend_errors, 0);
        assert_eq!(stats.route_cache_hits, 1);
        assert!(stats.route_refreshes >= 1);
        assert_eq!(stats.continuous_backend_failures, 1);
    }

    #[test]
    fn client_opens_table_from_metaserver_topology() {
        let meta_addr = free_local_addr();
        let meta_addr_for_listener = meta_addr.clone();
        std::thread::spawn(move || {
            serve(&meta_addr_for_listener, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/tables/topology") => json_response(
                        200,
                        &TableTopologyResponse {
                            status: Status::ok(),
                            table: Some(crate::meta::TableMetaInfo {
                                table_id: 1,
                                namespace: "ns".to_string(),
                                table_name: "tbl".to_string(),
                                state: crate::meta::MetaEntityState::Normal,
                                topology_version: 7,
                                first_shard_id: 10,
                                shard_count: 4,
                                replica_count: 1,
                                use_cpp_partition_ids: false,
                                partition_version: 0,
                                serving_options: crate::meta::TableServingOptions::default(),
                            }),
                            partitions: Vec::new(),
                            unchanged: false,
                        },
                    ),
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });
        wait_for_http(&meta_addr);

        let client = TemporalStoreClient::with_options(ClientOptions {
            meta_addr: Some(meta_addr.clone()),
            drop_percent: 17,
            ..ClientOptions::default()
        });
        let table = client.open_table_from_meta("ns", "tbl").unwrap();
        assert_eq!(table.namespace(), "ns");
        assert_eq!(table.table_name(), "tbl");
        assert_eq!(table.options().drop_percent, 17);
        let routed = table.shard_id_for_key("routing-key");
        assert!((10..14).contains(&routed));
    }

    #[test]
    fn client_meta_sync_report_tracks_success_and_table_errors() {
        let meta_addr = free_local_addr();
        let meta_addr_for_listener = meta_addr.clone();
        std::thread::spawn(move || {
            serve(&meta_addr_for_listener, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/tables/topology") => {
                        let req = parse_json::<GetTableTopologyRequest>(&request.body).unwrap();
                        if req.table_name == "bad" {
                            return json_response(
                                200,
                                &TableTopologyResponse {
                                    status: Status::error("not_found", "missing table"),
                                    table: None,
                                    partitions: Vec::new(),
                                    unchanged: false,
                                },
                            );
                        }
                        json_response(
                            200,
                            &TableTopologyResponse {
                                status: Status::ok(),
                                table: Some(TableMetaInfo {
                                    table_id: 11,
                                    namespace: req.namespace,
                                    table_name: req.table_name,
                                    state: crate::meta::MetaEntityState::Normal,
                                    topology_version: 9,
                                    first_shard_id: 3,
                                    shard_count: 2,
                                    replica_count: 1,
                                    use_cpp_partition_ids: false,
                                    partition_version: 0,
                                    serving_options: crate::meta::TableServingOptions::default(),
                                }),
                                partitions: Vec::new(),
                                unchanged: false,
                            },
                        )
                    }
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });
        wait_for_http(&meta_addr);

        let client = TemporalStoreClient::with_options(ClientOptions {
            meta_addr: Some(meta_addr),
            meta_sync_interval_ms: 25,
            ..ClientOptions::default()
        });
        let table = client.open_table_from_meta("ns", "tbl").unwrap();
        assert_eq!(table.options().first_shard_id, 3);
        let err = client.sync_table_topology("ns", "bad").unwrap_err();
        assert!(err.to_string().contains("missing table"));

        let report = client.meta_sync_report();
        assert_eq!(report.table_count, 2);
        assert_eq!(report.synced_table_count, 1);
        assert_eq!(report.error_table_count, 1);
        assert_eq!(report.total_sync_generation, 2);
        let good = report
            .tables
            .iter()
            .find(|table| table.table == "ns/tbl")
            .unwrap();
        assert_eq!(good.sync_generation, 1);
        assert_eq!(good.last_topology_version, 9);
        assert_eq!(good.consecutive_errors, 0);
        assert!(good.last_success_unix_ms > 0);
        assert!(good.next_sync_after_unix_ms >= good.last_success_unix_ms);
        let bad = report
            .tables
            .iter()
            .find(|table| table.table == "ns/bad")
            .unwrap();
        assert_eq!(bad.sync_generation, 1);
        assert_eq!(bad.consecutive_errors, 1);
        assert_eq!(bad.last_error, "missing table");
        assert!(bad.last_error_unix_ms > 0);
        assert!(bad.next_sync_after_unix_ms > bad.last_error_unix_ms);

        let preflight = client.preflight_report();
        assert_eq!(preflight.meta_sync.error_table_count, 1);
        assert!(preflight
            .degraded_reasons
            .contains(&"meta_sync_table_errors".to_string()));
    }

    #[test]
    fn client_applies_metaserver_table_serving_options() {
        let meta_addr = free_local_addr();
        let meta_addr_for_listener = meta_addr.clone();
        std::thread::spawn(move || {
            serve(&meta_addr_for_listener, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/tables/topology") => json_response(
                        200,
                        &TableTopologyResponse {
                            status: Status::ok(),
                            table: Some(crate::meta::TableMetaInfo {
                                table_id: 1,
                                namespace: "ns".to_string(),
                                table_name: "tbl".to_string(),
                                state: crate::meta::MetaEntityState::Normal,
                                topology_version: 7,
                                first_shard_id: 10,
                                shard_count: 4,
                                replica_count: 2,
                                use_cpp_partition_ids: false,
                                partition_version: 0,
                                serving_options: crate::meta::TableServingOptions {
                                    pin_primary: false,
                                    replica_read_policy: "round_robin_replica".to_string(),
                                    preferred_location: "zone-b".to_string(),
                                    drop_percent: 23,
                                    max_read_retries: 4,
                                    max_write_retries: 2,
                                    retry_backoff_ms: 17,
                                    continuous_failed_time_ms: 19,
                                    io_timeout_ms: 321,
                                    connect_timeout_ms: 123,
                                },
                            }),
                            partitions: Vec::new(),
                            unchanged: false,
                        },
                    ),
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });
        wait_for_http(&meta_addr);

        let client = TemporalStoreClient::with_options(ClientOptions {
            meta_addr: Some(meta_addr.clone()),
            drop_percent: 0,
            ..ClientOptions::default()
        });
        let table = client.open_table_from_meta("ns", "tbl").unwrap();
        let options = table.options();
        assert!(!options.pin_primary);
        assert_eq!(
            options.replica_read_policy,
            ReplicaReadPolicy::RoundRobinReplica
        );
        assert_eq!(options.preferred_location, "zone-b");
        assert_eq!(options.drop_percent, 23);
        assert_eq!(options.max_read_retries, 4);
        assert_eq!(options.max_write_retries, 2);
        assert_eq!(options.retry_backoff_ms, 17);
        assert_eq!(options.continuous_failed_time_ms, 19);
        assert_eq!(options.io_timeout_ms, 321);
        assert_eq!(options.connect_timeout_ms, 123);
    }

    #[test]
    fn table_read_policy_can_select_secondary_from_metaserver_topology() {
        let primary_addr = free_local_addr();
        let replica_addr = free_local_addr();
        let meta_addr = free_local_addr();

        let primary_server = primary_addr.clone();
        std::thread::spawn(move || {
            serve(&primary_server, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/execute") => {
                        let req = parse_json::<ExecuteRequest>(&request.body).unwrap();
                        match req.command {
                            Command::StringSet { .. } => json_response(
                                200,
                                &ExecuteResponse {
                                    status: Status::ok(),
                                    response: CommandResponse::Empty,
                                },
                            ),
                            Command::StringGet { .. } => json_response(
                                200,
                                &ExecuteResponse {
                                    status: Status::ok(),
                                    response: CommandResponse::Bytes {
                                        value: Some(b"primary".to_vec()),
                                    },
                                },
                            ),
                            _ => json_response(400, &Status::error("bad_request", "unexpected")),
                        }
                    }
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });

        let replica_server = replica_addr.clone();
        std::thread::spawn(move || {
            serve(&replica_server, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/execute") => {
                        let req = parse_json::<ExecuteRequest>(&request.body).unwrap();
                        match req.command {
                            Command::StringGet { .. } => json_response(
                                200,
                                &ExecuteResponse {
                                    status: Status::ok(),
                                    response: CommandResponse::Bytes {
                                        value: Some(b"replica".to_vec()),
                                    },
                                },
                            ),
                            _ => json_response(
                                200,
                                &ExecuteResponse {
                                    status: Status::error("wrong_endpoint", "replica got write"),
                                    response: CommandResponse::Empty,
                                },
                            ),
                        }
                    }
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });

        let primary_for_meta = primary_addr.clone();
        let replica_for_meta = replica_addr.clone();
        let meta_addr_for_listener = meta_addr.clone();
        std::thread::spawn(move || {
            serve(&meta_addr_for_listener, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/tables/topology") => json_response(
                        200,
                        &TableTopologyResponse {
                            status: Status::ok(),
                            table: Some(TableMetaInfo {
                                table_id: 7,
                                namespace: "ns".to_string(),
                                table_name: "tbl".to_string(),
                                state: crate::meta::MetaEntityState::Normal,
                                topology_version: 1,
                                first_shard_id: 1,
                                shard_count: 1,
                                replica_count: 2,
                                use_cpp_partition_ids: false,
                                partition_version: 0,
                                serving_options: crate::meta::TableServingOptions::default(),
                            }),
                            partitions: vec![TablePartition {
                                shard_id: 1,
                                start_slot: 0,
                                end_slot: u64::MAX,
                                primary: Some(primary_for_meta.clone()),
                                replicas: vec![primary_for_meta.clone(), replica_for_meta.clone()],
                                primary_endpoint: None,
                                replica_endpoints: Vec::new(),
                            }],
                            unchanged: false,
                        },
                    ),
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });
        wait_for_http(&primary_addr);
        wait_for_http(&replica_addr);
        wait_for_http(&meta_addr);

        let client = TemporalStoreClient::with_options(ClientOptions {
            proxy_addr: "127.0.0.1:1".to_string(),
            meta_addr: Some(meta_addr.clone()),
            route_cache_ttl_ms: 60_000,
            ..ClientOptions::default()
        });
        let synced = client.sync_table_topology("ns", "tbl").unwrap();
        let table = client.open_table(
            "ns",
            "tbl",
            TableOptions {
                pin_primary: false,
                replica_read_policy: ReplicaReadPolicy::FirstReplica,
                ..synced
            },
        );

        table.set("k", b"v".to_vec()).unwrap();
        assert_eq!(table.get("k").unwrap(), Some(b"replica".to_vec()));
        assert!(client.stats().route_cache_hits >= 2);
    }

    #[test]
    fn client_router_matches_cpp_crc64_slot_formula() {
        assert_eq!(crc64_jones(b"123456789"), 0xe9c6d914c4b8d9ca);
        assert_eq!(slot_id_for_key("123456789"), 0x3a71_b645);
        assert_eq!(
            shard_id_for_key("123456789", 10, 4, 1),
            10 + (0x3a71_b645 % 4)
        );
        assert_eq!(stable_key_hash("123456789"), crc64_jones(b"123456789"));
    }

    #[test]
    fn client_router_round_robins_secondary_reads_like_cpp_router() {
        let mut route = CachedRoute {
            primary_addr: "primary".to_string(),
            replica_addrs: vec!["replica-a".to_string(), "replica-b".to_string()],
            replica_endpoints: Vec::new(),
            next_replica_index: 0,
            fetched_at: Instant::now(),
            topology_version: 7,
            refresh_reason: "test_insert".to_string(),
        };

        assert_eq!(
            choose_cached_route(&mut route, ReplicaReadPolicy::RoundRobinReplica, None),
            "replica-a"
        );
        assert_eq!(
            choose_cached_route(&mut route, ReplicaReadPolicy::RoundRobinReplica, None),
            "replica-b"
        );
        assert_eq!(
            choose_cached_route(&mut route, ReplicaReadPolicy::RoundRobinReplica, None),
            "replica-a"
        );
        assert_eq!(
            choose_cached_route(&mut route, ReplicaReadPolicy::PinPrimary, None),
            "primary"
        );
    }

    #[test]
    fn client_router_prefers_same_location_replica_when_available() {
        let mut route = CachedRoute {
            primary_addr: "primary".to_string(),
            replica_addrs: vec!["replica-remote".to_string(), "replica-local".to_string()],
            replica_endpoints: vec![
                ServerEndpoint {
                    server_addr: "replica-remote".to_string(),
                    location: "zone-b".to_string(),
                },
                ServerEndpoint {
                    server_addr: "replica-local".to_string(),
                    location: "zone-a".to_string(),
                },
            ],
            next_replica_index: 0,
            fetched_at: Instant::now(),
            topology_version: 7,
            refresh_reason: "test_insert".to_string(),
        };

        assert_eq!(
            choose_cached_route(&mut route, ReplicaReadPolicy::FirstReplica, Some("zone-a")),
            "replica-local"
        );
        assert_eq!(
            choose_cached_route(
                &mut route,
                ReplicaReadPolicy::RoundRobinReplica,
                Some("zone-a")
            ),
            "replica-local"
        );
        assert_eq!(
            choose_cached_route(
                &mut route,
                ReplicaReadPolicy::FirstReplica,
                Some("missing-zone")
            ),
            "replica-remote"
        );
    }

    #[test]
    fn client_table_drop_percent_rejects_sampled_requests_before_network() {
        let client = TemporalStoreClient::with_options(ClientOptions {
            proxy_addr: "127.0.0.1:1".to_string(),
            ..ClientOptions::default()
        });
        let table = client.open_table(
            "ns",
            "tbl",
            TableOptions {
                drop_percent: 100,
                ..TableOptions::default()
            },
        );

        let response = table
            .execute(Command::StringGet {
                key: "always-dropped".to_string(),
            })
            .unwrap();
        assert_eq!(response.status.code, "traffic_dropped");

        let batch = table
            .batch_execute(vec![Command::StringSet {
                key: "also-dropped".to_string(),
                value: b"v".to_vec(),
            }])
            .unwrap();
        assert_eq!(batch.status.code, "traffic_dropped");
        assert!(batch.responses.is_empty());
        assert_eq!(client.stats().route_refreshes, 0);
    }

    #[test]
    fn table_write_refreshes_due_topology_before_network() {
        let data_addr = free_local_addr();
        let observed_shard = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        std::thread::spawn({
            let data_addr = data_addr.clone();
            let observed_shard = std::sync::Arc::clone(&observed_shard);
            move || {
                serve(&data_addr, move |request| {
                    match (request.method.as_str(), request.path.as_str()) {
                        ("POST", "/execute") => {
                            let req = parse_json::<ExecuteRequest>(&request.body).unwrap();
                            observed_shard
                                .store(req.shard_id, std::sync::atomic::Ordering::Relaxed);
                            json_response(
                                200,
                                &ExecuteResponse {
                                    status: Status::ok(),
                                    response: CommandResponse::Empty,
                                },
                            )
                        }
                        _ => json_response(404, &Status::error("not_found", "not found")),
                    }
                })
                .unwrap();
            }
        });
        wait_for_http(&data_addr);

        let meta_addr = free_local_addr();
        let first_shard = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(10));
        std::thread::spawn({
            let meta_addr = meta_addr.clone();
            let data_addr = data_addr.clone();
            let first_shard = std::sync::Arc::clone(&first_shard);
            move || {
                serve(&meta_addr, move |request| {
                    match (request.method.as_str(), request.path.as_str()) {
                        ("POST", "/tables/topology") => {
                            let first_shard_id =
                                first_shard.load(std::sync::atomic::Ordering::Relaxed);
                            json_response(
                                200,
                                &TableTopologyResponse {
                                    status: Status::ok(),
                                    table: Some(crate::meta::TableMetaInfo {
                                        table_id: 1,
                                        namespace: "ns".to_string(),
                                        table_name: "tbl".to_string(),
                                        state: crate::meta::MetaEntityState::Normal,
                                        topology_version: first_shard_id,
                                        first_shard_id,
                                        shard_count: 1,
                                        replica_count: 1,
                                        use_cpp_partition_ids: false,
                                        partition_version: 0,
                                        serving_options: crate::meta::TableServingOptions::default(
                                        ),
                                    }),
                                    partitions: vec![crate::meta::TablePartition {
                                        shard_id: first_shard_id,
                                        start_slot: 0,
                                        end_slot: u64::MAX,
                                        primary: Some(data_addr.clone()),
                                        replicas: vec![data_addr.clone()],
                                        primary_endpoint: None,
                                        replica_endpoints: Vec::new(),
                                    }],
                                    unchanged: false,
                                },
                            )
                        }
                        _ => json_response(404, &Status::error("not_found", "not found")),
                    }
                })
                .unwrap();
            }
        });
        wait_for_http(&meta_addr);

        let client = TemporalStoreClient::with_options(ClientOptions {
            meta_addr: Some(meta_addr),
            meta_sync_interval_ms: 1,
            ..ClientOptions::default()
        });
        let table = client.open_table_from_meta("ns", "tbl").unwrap();
        assert_eq!(table.options().first_shard_id, 10);
        first_shard.store(20, std::sync::atomic::Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(5));

        table.set("stale-write", b"value".to_vec()).unwrap();
        assert_eq!(
            observed_shard.load(std::sync::atomic::Ordering::Relaxed),
            20
        );
        assert_eq!(table.options().first_shard_id, 20);
    }

    #[test]
    fn client_drop_percent_sampler_is_deterministic_and_bounded() {
        assert!(!key_is_dropped_by_percent("k", 0));
        assert!(key_is_dropped_by_percent("k", 100));
        assert_eq!(
            key_is_dropped_by_percent("stable-key", 17),
            key_is_dropped_by_percent("stable-key", 17)
        );
        assert_eq!(
            key_is_dropped_by_percent("k", 255),
            key_is_dropped_by_percent("k", 100)
        );
    }

    #[test]
    fn client_retries_cpp_retryable_read_status_before_returning() {
        let proxy_addr = free_local_addr();
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let attempts_for_server = attempts.clone();
        let proxy_addr_for_listener = proxy_addr.clone();
        std::thread::spawn(move || {
            serve(&proxy_addr_for_listener, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/execute") => {
                        let attempt =
                            attempts_for_server.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        if attempt == 0 {
                            json_response(
                                200,
                                &ExecuteResponse {
                                    status: Status::error("retry_later", "loading"),
                                    response: CommandResponse::Empty,
                                },
                            )
                        } else {
                            json_response(
                                200,
                                &ExecuteResponse {
                                    status: Status::ok(),
                                    response: CommandResponse::Bytes {
                                        value: Some(b"ok".to_vec()),
                                    },
                                },
                            )
                        }
                    }
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });
        wait_for_http(&proxy_addr);

        let client = TemporalStoreClient::with_options(ClientOptions {
            proxy_addr: proxy_addr.clone(),
            ..ClientOptions::default()
        });
        let table = client.open_table(
            "ns",
            "tbl",
            TableOptions {
                retry_backoff_ms: 0,
                ..TableOptions::default()
            },
        );

        assert_eq!(table.get("retry-key").unwrap(), Some(b"ok".to_vec()));
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[test]
    fn client_does_not_retry_write_status_without_write_retry_budget() {
        let proxy_addr = free_local_addr();
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let attempts_for_server = attempts.clone();
        let proxy_addr_for_listener = proxy_addr.clone();
        std::thread::spawn(move || {
            serve(&proxy_addr_for_listener, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/execute") => {
                        attempts_for_server.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        json_response(
                            200,
                            &ExecuteResponse {
                                status: Status::error("retry_later", "write loading"),
                                response: CommandResponse::Empty,
                            },
                        )
                    }
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });
        wait_for_http(&proxy_addr);

        let client = TemporalStoreClient::with_options(ClientOptions {
            proxy_addr: proxy_addr.clone(),
            ..ClientOptions::default()
        });
        let table = client.open_table("ns", "tbl", TableOptions::default());

        let err = table.set("retry-write", b"v".to_vec()).unwrap_err();
        assert!(err.to_string().contains("write loading"));
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn client_retry_classifier_separates_safe_topology_retry_from_unsafe_write_retry() {
        let unsafe_write_retry = classify_cpp_retry_decision(
            &Status::error("retry_later", "possibly applied"),
            true,
            0,
            1,
            false,
        );
        assert!(unsafe_write_retry.retryable);
        assert!(!unsafe_write_retry.topology_retry);
        assert!(!unsafe_write_retry.safe_budget_free_write_retry);
        assert!(
            !unsafe_write_retry.would_retry,
            "write retry without budget must not duplicate a possibly applied write"
        );

        let safe_topology_retry = classify_cpp_retry_decision(
            &Status::error("meta_changed", "not applied on stale route"),
            true,
            0,
            1,
            false,
        );
        assert!(safe_topology_retry.retryable);
        assert!(safe_topology_retry.topology_retry);
        assert!(safe_topology_retry.safe_budget_free_write_retry);
        assert!(
            safe_topology_retry.would_retry,
            "C++ stale topology rejection may refresh and retry once even with no write retry budget"
        );

        let duplicate_topology_retry = classify_cpp_retry_decision(
            &Status::error("meta_changed", "still stale"),
            true,
            1,
            1,
            true,
        );
        assert!(!duplicate_topology_retry.safe_budget_free_write_retry);
        assert!(
            !duplicate_topology_retry.would_retry,
            "budget-free topology retry is intentionally single-shot"
        );
    }

    #[test]
    fn client_background_meta_sync_updates_existing_table_handle() {
        let meta_addr = free_local_addr();
        let first_shard = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(10));
        std::thread::spawn({
            let first_shard = std::sync::Arc::clone(&first_shard);
            let meta_addr = meta_addr.clone();
            move || {
                serve(&meta_addr, move |request| {
                    match (request.method.as_str(), request.path.as_str()) {
                        ("POST", "/tables/topology") => {
                            let first_shard_id =
                                first_shard.load(std::sync::atomic::Ordering::Relaxed);
                            json_response(
                                200,
                                &TableTopologyResponse {
                                    status: Status::ok(),
                                    table: Some(crate::meta::TableMetaInfo {
                                        table_id: 1,
                                        namespace: "ns".to_string(),
                                        table_name: "tbl".to_string(),
                                        state: crate::meta::MetaEntityState::Normal,
                                        topology_version: first_shard_id,
                                        first_shard_id,
                                        shard_count: 2,
                                        replica_count: 1,
                                        use_cpp_partition_ids: false,
                                        partition_version: 0,
                                        serving_options: crate::meta::TableServingOptions::default(
                                        ),
                                    }),
                                    partitions: Vec::new(),
                                    unchanged: false,
                                },
                            )
                        }
                        _ => json_response(404, &Status::error("not_found", "not found")),
                    }
                })
                .unwrap();
            }
        });
        wait_for_http(&meta_addr);

        let client = TemporalStoreClient::with_options(ClientOptions {
            meta_addr: Some(meta_addr),
            meta_sync_interval_ms: 10,
            topo_error_retry_interval_ms: 5,
            ..ClientOptions::default()
        });
        let table = client.open_table_from_meta("ns", "tbl").unwrap();
        assert!((10..12).contains(&table.shard_id_for_key("k")));
        first_shard.store(20, std::sync::atomic::Ordering::Relaxed);
        let syncer = client.start_meta_sync_loop_handle(ClientMetaSyncLoopOptions {
            tick_ms: 5,
            max_tables_per_tick: 1,
        });

        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            let shard = table.shard_id_for_key("k");
            if (20..22).contains(&shard) {
                assert!(client.stats().meta_sync_total >= 2);
                syncer.stop_and_join().unwrap();
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        syncer.stop_and_join().unwrap();
        panic!("client meta sync loop did not refresh table options");
    }

    #[test]
    fn table_routes_keys_to_shards_and_pipeline_splits_batches() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.load_shard(2);
        let server_addr = free_local_addr();
        let meta_addr = free_local_addr();
        let engine_for_server = engine.clone();
        let server_addr_for_thread = server_addr.clone();
        std::thread::spawn(move || {
            serve(&server_addr_for_thread, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/execute") => {
                        let req = parse_json::<ExecuteRequest>(&request.body).unwrap();
                        json_response(200, &engine_for_server.execute(req))
                    }
                    ("POST", "/batch_execute") => {
                        let req = parse_json::<BatchExecuteRequest>(&request.body).unwrap();
                        json_response(200, &engine_for_server.batch_execute(req))
                    }
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });
        let server_addr_for_meta = server_addr.clone();
        let meta_addr_for_thread = meta_addr.clone();
        std::thread::spawn(move || {
            serve(&meta_addr_for_thread, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("GET", "/shards/1") | ("GET", "/shards/2") => {
                        let shard_id = request.path.trim_start_matches("/shards/").parse().unwrap();
                        json_response(
                            200,
                            &GetShardResponse {
                                status: Status::ok(),
                                location: Some(ShardLocation {
                                    shard_id,
                                    server_addr: server_addr_for_meta.clone(),
                                    latest_snapshot: None,
                                }),
                            },
                        )
                    }
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });
        wait_for_http(&server_addr);
        wait_for_http(&meta_addr);

        let client = TemporalStoreClient::with_options(ClientOptions {
            proxy_addr: "127.0.0.1:1".to_string(),
            meta_addr: Some(meta_addr),
            route_cache_ttl_ms: 60_000,
            ..ClientOptions::default()
        });
        let table = client.open_table(
            "ns",
            "tbl",
            TableOptions {
                first_shard_id: 1,
                shard_count: 2,
                ..TableOptions::default()
            },
        );
        let key_one = key_for_shard(&table, 1);
        let key_two = key_for_shard(&table, 2);

        table.set(&key_one, b"one".to_vec()).unwrap();
        table.set(&key_two, b"two".to_vec()).unwrap();
        assert_eq!(table.get(&key_one).unwrap(), Some(b"one".to_vec()));
        assert_eq!(table.get(&key_two).unwrap(), Some(b"two".to_vec()));

        table
            .hmset(
                &key_one,
                vec![
                    ("a".to_string(), b"1".to_vec()),
                    ("b".to_string(), b"2".to_vec()),
                ],
            )
            .unwrap();
        assert_eq!(table.hlen(&key_one).unwrap(), 2);
        assert_eq!(
            table
                .hmget(&key_one, vec!["a".to_string(), "z".to_string()])
                .unwrap(),
            vec![Some(b"1".to_vec()), None]
        );

        let mut pipeline = table.pipeline();
        pipeline.set(&key_one, b"one-batch".to_vec());
        pipeline.set(&key_two, b"two-batch".to_vec());
        pipeline.get(&key_one);
        pipeline.get(&key_two);
        let response = pipeline.sync().unwrap();
        assert_eq!(response.responses.len(), 4);
        assert_eq!(
            response.responses[2].response,
            CommandResponse::Bytes {
                value: Some(b"one-batch".to_vec())
            }
        );
        assert_eq!(
            response.responses[3].response,
            CommandResponse::Bytes {
                value: Some(b"two-batch".to_vec())
            }
        );

        let stats = client.stats();
        assert!(stats.route_cache_hits > 0);
        assert!(stats.route_refreshes >= 2);
        assert_eq!(client.route_cache_size(), 2);
        client.close_table(&table).unwrap();
        assert_eq!(client.route_cache_size(), 0);
        assert!(client
            .cached_table("ns".to_string(), "tbl".to_string())
            .is_none());
    }

    fn key_for_shard(table: &TemporalStoreTable, shard_id: ShardId) -> String {
        (0..10_000)
            .map(|index| format!("key-{shard_id}-{index}"))
            .find(|key| table.shard_id_for_key(key) == shard_id)
            .unwrap()
    }

    fn free_local_addr() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
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
