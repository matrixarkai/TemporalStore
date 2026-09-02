// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::http::{
    get_json, get_json_with_options, post_json, post_json_with_options,
    post_json_with_options_and_headers, HttpError,
    HttpRequestOptions,
};
use crate::meta::GetShardResponse;
use crate::meta::{
    GetTableTopologyRequest, ServerEndpoint, TableTopologyResponse, TopologyVersionReport,
    TopologyVersionRequest,
};
mod client_routing;
mod client_meta_sync;
mod commands;
// The proxy's drop decision hashes the same routing key this builds; it is re-exported
// rather than copied because the copy that used to live there had drifted.
pub(crate) use commands::command_routing_key;
mod table_feature;
mod table_context;
mod table_control_state;
mod retry;
mod routing;

use commands::{command_is_dropped, command_key, is_write};
use retry::{
    classify_retry_decision, replica_read_policy_from_meta, retry_attempts_for,
    sleep_before_retry,
};
pub use routing::{
    crc64_jones, key_is_dropped_by_percent, shard_id_for_key, bucket_id_for_key, stable_key_hash,
};

use crate::types::{
    parse_feature_filters, BatchExecuteRequest, BatchExecuteResponse, Command, CommandResponse,
    ContextEvent, ContextIndexRef, ContextNode, ContextPackAudit, ContextDirtyNode,
    ExecuteRequest, ExecuteResponse, FeatureFilter, FeaturePoint, FeatureWritePolicy,
    ControlStateFamily, ControlStateSelectionType, SequenceFeatureRow, SequenceQuerySpec,
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
    /// A write failed in a way that does not say whether it was applied.
    ///
    /// The datanode stopped answering; it may have processed the write, or not. The write was
    /// deliberately not sent again, because repeating it could apply it twice. That leaves the
    /// caller with a decision only the caller can make -- reconcile, or accept the risk -- and
    /// it can only make it if it is told this is what happened, rather than being handed the
    /// same generic failure as a write that provably never arrived.
    #[error("write outcome unknown: {0}")]
    WriteOutcomeUnknown(String),
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
    pub meta_sync_deadline_ms: u64,
    pub meta_sync_jitter_percent: u8,
    pub local_location: String,
    pub drop_percent: u8,
    /// Re-resolve a shard's route and retry once when its backend fails, rather than
    /// surfacing the error against the address we already had. This is what recovers a
    /// request whose shard has moved, so it defaults ON; turning it off is for deployments
    /// that would rather see the failure immediately than pay a metaserver round-trip and a
    /// second attempt on every backend blip.
    pub refresh_route_on_backend_error: bool,
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

    fn meta_sync_http_options(&self) -> HttpRequestOptions {
        let mut options = self.http_options();
        if self.meta_sync_deadline_ms > 0 {
            options.io_timeout_ms = options.io_timeout_ms.min(self.meta_sync_deadline_ms);
            options.connect_timeout_ms = options.connect_timeout_ms.min(self.meta_sync_deadline_ms);
        }
        options
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
            meta_sync_deadline_ms: 200,
            meta_sync_jitter_percent: 20,
            local_location: String::new(),
            drop_percent: 0,
            refresh_route_on_backend_error: true,
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

fn next_client_instance_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableOptions {
    pub table_id: u64,
    pub io_timeout_ms: u64,
    pub connect_timeout_ms: u64,
    pub continuous_failed_time_ms: u64,
    pub first_shard_id: ShardId,
    pub shard_count: u64,
    pub partition_version: u32,
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
            table_id: 0,
            io_timeout_ms: 200,
            connect_timeout_ms: 200,
            continuous_failed_time_ms: 10_000,
            first_shard_id: 1,
            shard_count: 1,
            partition_version: 0,
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
    WireMigrationOutOfScope,
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
    pub legacy_wire_in_scope: bool,
    pub native_wire_compatible_ready: bool,
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
            compatibility_mode: ClientCompatibilityMode::WireMigrationOutOfScope,
            rust_native_http_ready: true,
            rust_native_tonic_ready: true,
            legacy_wire_in_scope: false,
            native_wire_compatible_ready: false,
            migration_layer_ready: false,
            typed_table_client_ready: true,
            topology_sync_ready: true,
            retry_budgets_ready: true,
            neptune_routing_hooks_ready: true,
            placement_hooks_ready: true,
            production_replacement_contract: ClientProductionReplacementContract::default(),
            blockers: vec![
                "legacy wire compatibility is explicitly out of scope for the Rust-native target"
                    .to_string(),
                "existing legacy callers must migrate through the documented Rust HTTP/JSON, RESP, and tonic API"
                    .to_string(),
            ],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClientProductionReplacementContract {
    pub compatibility_decision: String,
    pub legacy_wire_protocols_in_scope: Vec<String>,
    pub production_protocols: Vec<String>,
    pub supported_command_families: Vec<String>,
    pub typed_table_client_preserved: bool,
    pub topology_sync_preserved: bool,
    pub retry_budget_preserved: bool,
    pub neptune_routing_hooks_preserved: bool,
    pub placement_hooks_preserved: bool,
    #[serde(default)]
    pub http_json_contract_tested: bool,
    #[serde(default)]
    pub resp_contract_tested: bool,
    #[serde(default)]
    pub tonic_contract_tested: bool,
    #[serde(default)]
    pub typed_table_client_tested: bool,
    #[serde(default)]
    pub topology_sync_tested: bool,
    #[serde(default)]
    pub retry_budget_tested: bool,
    #[serde(default)]
    pub migration_docs_ready: bool,
    pub migration_contract_version: u32,
}

impl Default for ClientProductionReplacementContract {
    fn default() -> Self {
        Self {
            compatibility_decision:
                "legacy wire migration shims are out of scope; use Rust-native migration contract"
                    .to_string(),
            legacy_wire_protocols_in_scope: Vec::new(),
            production_protocols: vec![
                "HTTP/JSON".to_string(),
                "RESP".to_string(),
                "tonic".to_string(),
            ],
            supported_command_families: vec![
                "common".to_string(),
                "string".to_string(),
                "hash".to_string(),
                "set".to_string(),
                "feature".to_string(),
                "sequence".to_string(),
                "control_state".to_string(),
                "redis".to_string(),
                "admin".to_string(),
                "context".to_string(),
            ],
            typed_table_client_preserved: true,
            topology_sync_preserved: true,
            retry_budget_preserved: true,
            neptune_routing_hooks_preserved: true,
            placement_hooks_preserved: true,
            http_json_contract_tested: true,
            resp_contract_tested: true,
            tonic_contract_tested: true,
            typed_table_client_tested: true,
            topology_sync_tested: true,
            retry_budget_tested: true,
            migration_docs_ready: true,
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
    /// Writes that failed with an outcome nobody can determine, and so were deliberately not
    /// sent again.
    ///
    /// A refused connection proves the write never arrived and it is simply retried. A
    /// timeout does not: the datanode stopped answering, and the write may or may not have
    /// been applied. Repeating it there would apply it twice, so it is not repeated -- which
    /// means these are the writes whose fate is genuinely unknown. That is a number an
    /// operator has to be able to see; it was previously indistinguishable from an ordinary
    /// backend error.
    pub writes_of_unknown_outcome: u64,
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
    pub native_partition_sets: Vec<ClientPartitionSetReport>,
    #[serde(default)]
    pub meta_sync: ClientMetaSyncReport,
    pub degraded_reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientDirectSdkParityReport {
    pub rust_native_migration_contract_ready: bool,
    pub typed_table_client_ready: bool,
    pub native_partition_set_route_cache_ready: bool,
    pub partition_member_version_ready: bool,
    pub topology_sync_ready: bool,
    pub meta_syncer_ready: bool,
    pub retry_budget_ready: bool,
    pub route_invalidation_ready: bool,
    pub placement_hooks_ready: bool,
    pub location_affine_secondary_reads_ready: bool,
    pub primary_only_writes_ready: bool,
    pub direct_sdk_command_families: Vec<String>,
    pub native_partition_set_count: usize,
    pub native_partition_member_count: usize,
    pub missing_route_count: usize,
    pub max_topology_version: u64,
    pub meta_sync_generation: u64,
    pub ready: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientPartitionSetReport {
    pub table_id: u64,
    pub namespace: String,
    pub table_name: String,
    pub combine_name: String,
    pub first_shard_id: ShardId,
    pub shard_count: u64,
    pub partition_version: u32,
    pub topology_version: u64,
    pub partition_count: usize,
    pub missing_route_count: usize,
    pub members: Vec<ClientPartitionMemberReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientPartitionMemberReport {
    pub partition_id: ShardId,
    pub shard_id: ShardId,
    #[serde(rename = "start_slot")]
    pub start_bucket: u64,
    #[serde(rename = "end_slot")]
    pub end_bucket: u64,
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
    /// Shards the last sync could not route because the topology named no primary for them.
    ///
    /// Their previous routes are kept rather than discarded -- a snapshot taken while a
    /// primary is being elected should not destroy a working route -- but the sync is not a
    /// clean one, and reporting it as clean is how this stayed invisible.
    #[serde(default)]
    pub shards_without_primary: u64,
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
    pub table: String,
    pub partition_id: ShardId,
    #[serde(rename = "start_slot")]
    pub start_bucket: u64,
    #[serde(rename = "end_slot")]
    pub end_bucket: u64,
    pub partition_version: u32,
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
    fn record_write_of_unknown_outcome(&mut self) {
        self.writes_of_unknown_outcome += 1;
    }

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
    /// Cached routes by shard.
    ///
    /// A reader-writer lock: resolving a route is a read now that the round-robin cursor
    /// is atomic. It writes only when a route is fetched, replaced by a topology sync, or
    /// cleared.
    routes: RwLock<HashMap<ShardId, CachedRoute>>,
    /// Cached-route hits, counted outside `stats`.
    ///
    /// Every request that finds its route cached lands here, and taking the stats mutex to add one
    /// serialised those requests against each other. It is folded into `ClientStats` when the
    /// stats are read, so callers see the same number; only the write side stops taking a lock.
    route_cache_hits: AtomicU64,
    backend_failures: Mutex<HashMap<String, BackendFailureState>>,
    /// How many entries `backend_failures` holds, maintained under that lock.
    ///
    /// Read without taking the lock so the common case -- nothing has failed -- answers without
    /// serialising. Every cached route lookup asks whether the address it chose is failing, and
    /// on a healthy deployment the map it was locking to consult is empty.
    backend_failure_entries: AtomicUsize,
    /// Table options by "namespace/table".
    ///
    /// A reader-writer lock, not a mutex: every table-scoped request reads this at least
    /// twice -- once to resolve the table and once for its options -- and writes happen
    /// only when a table is opened, refreshed by the metaserver sync, or dropped. Under a
    /// mutex those reads serialized against each other for no reason, which made resolving
    /// a cached table the most expensive thing the proxy did per request.
    /// The open tables, behind an `Arc` so a reader can hold the map without holding the lock.
    ///
    /// Every namespaced request looks a table up here. Taking the read lock for it is an atomic
    /// read-modify-write, so the requests serialise against each other on a map that changes only
    /// when a table is opened or closed. Readers take a per-thread snapshot instead, refreshed
    /// when `tables_version` moves.
    tables: RwLock<Arc<HashMap<String, TableOptions>>>,
    tables_version: AtomicU64,
    /// Distinguishes this client from any other on the same thread.
    ///
    /// The snapshot is thread-local, so every client a thread touches shares it. Two freshly
    /// built clients are both on version 0; keyed by version alone they would read each other's
    /// tables.
    instance_id: u64,
    meta_sync_tables: Mutex<HashMap<String, ClientMetaSyncTableState>>,
    stats: Mutex<ClientStats>,
    /// Topology version this client last heard from the metaserver.
    ///
    /// Routes resolved by direct shard lookup carry no version of their own, and a route
    /// stamped 0 reads as "unknown", which the staleness check treats as stale. Every such
    /// route was stamped 0, so every check found the cache stale and dropped it, and the
    /// next request resolved again -- a cache that could never converge. Recording what the
    /// topology was when the route was resolved is what lets "unchanged" mean anything.
    known_topology_version: AtomicU64,
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
    shards_without_primary: u64,
    /// When a request failure last forced a topology sync for this table, out of band from
    /// the scheduled one. Used to space those out; see `refresh_table_topology_after_status`.
    last_forced_sync_unix_ms: u64,
}

#[derive(Debug)]
struct CachedRoute {
    table_key: String,
    partition_id: ShardId,
    start_bucket: u64,
    end_bucket: u64,
    partition_version: u32,
    primary_addr: String,
    replica_addrs: Vec<String>,
    replica_endpoints: Vec<ServerEndpoint>,
    /// Round-robin cursor for replica reads.
    ///
    /// An atomic, because it is the ONLY thing a route lookup writes -- and only under the
    /// round-robin policy. While it was a plain field the lookup needed `get_mut`, so the
    /// whole route cache was taken exclusively on every request to advance one number, and
    /// under the default pin-primary policy it advanced nothing at all.
    next_replica_index: std::sync::atomic::AtomicUsize,
    fetched_at: Instant,
    topology_version: u64,
    refresh_reason: String,
}

impl Clone for CachedRoute {
    fn clone(&self) -> Self {
        Self {
            table_key: self.table_key.clone(),
            partition_id: self.partition_id,
            start_bucket: self.start_bucket,
            end_bucket: self.end_bucket,
            partition_version: self.partition_version,
            primary_addr: self.primary_addr.clone(),
            replica_addrs: self.replica_addrs.clone(),
            replica_endpoints: self.replica_endpoints.clone(),
            next_replica_index: std::sync::atomic::AtomicUsize::new(
                self.next_replica_index
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            fetched_at: self.fetched_at,
            topology_version: self.topology_version,
            refresh_reason: self.refresh_reason.clone(),
        }
    }
}

impl CachedRoute {
    fn for_shard(shard_id: ShardId, primary_addr: impl Into<String>, refresh_reason: &str) -> Self {
        Self {
            table_key: String::new(),
            partition_id: shard_id,
            start_bucket: 0,
            end_bucket: 0,
            partition_version: 0,
            primary_addr: primary_addr.into(),
            replica_addrs: Vec::new(),
            replica_endpoints: Vec::new(),
            next_replica_index: std::sync::atomic::AtomicUsize::new(0),
            fetched_at: Instant::now(),
            topology_version: 0,
            refresh_reason: refresh_reason.to_string(),
        }
    }
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

    /// The options this client is actually running with. Useful for confirming that a
    /// configured knob was carried through rather than merely accepted.
    pub fn client_options(&self) -> ClientOptions {
        self.inner.options.clone()
    }

    /// Resolve the datanode that currently owns `shard_id`, through this client's shared
    /// route cache.
    ///
    /// Callers that forward raw HTTP to a shard (rather than going through `execute`) still
    /// need an address, and without this they tend to grow their own metaserver lookup --
    /// which then misses the cache, the TTL and the backend-failure accounting that every
    /// other caller gets for free.
    pub fn shard_primary_addr(
        &self,
        shard_id: ShardId,
        force_refresh: bool,
    ) -> Result<String, ClientError> {
        self.resolve_route(shard_id, force_refresh, None)
    }

    /// Record that a request to `server_addr` failed, so a backend that keeps failing stops
    /// being handed out from the route cache.
    ///
    /// This is the same accounting `execute` does; a caller that forwards on its own has to
    /// report failures itself or the continuous-failure check never fires for its traffic.
    pub fn note_backend_failure(&self, server_addr: &str) {
        self.record_backend_failure(
            server_addr,
            self.inner.options.topo_error_retry_interval_ms,
        );
    }

    pub fn with_options(options: ClientOptions) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                options,
                routes: RwLock::default(),
                route_cache_hits: AtomicU64::new(0),
                backend_failures: Mutex::default(),
                backend_failure_entries: AtomicUsize::new(0),
                tables: RwLock::new(Arc::new(HashMap::new())),
                tables_version: AtomicU64::new(0),
                instance_id: next_client_instance_id(),
                meta_sync_tables: Mutex::default(),
                stats: Mutex::default(),
                known_topology_version: AtomicU64::new(0),
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
        let combined_name = table_combine_name(&namespace, &table_name);
        self.with_tables_mut(|tables| {
            tables.insert(combined_name.clone(), options.clone())
        });
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
            combined_name,
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

    /// Every change to the open-table map goes through here.
    ///
    /// The map is published by moving `tables_version`, which is what makes a reader's snapshot
    /// stale. Bumping it at each call site is an invariant that holds until someone adds another
    /// site, so there is one site. Released after the write so an Acquire load that sees the new
    /// version also sees the new map.
    pub(crate) fn with_tables_mut<R>(
        &self,
        f: impl FnOnce(&mut HashMap<String, TableOptions>) -> R,
    ) -> R {
        let mut guard = self
            .inner
            .tables
            .write()
            .expect("client table cache lock poisoned");
        let out = f(Arc::make_mut(&mut guard));
        self.inner.tables_version.fetch_add(1, Ordering::Release);
        out
    }

    /// Run `f` against the open-table map without taking its lock.
    ///
    /// The snapshot is refreshed only when `tables_version` moves, which happens when a table is
    /// opened, closed or re-synced -- never on a request. Keyed by instance as well as version so
    /// one client's tables are never served to another on the same thread.
    fn with_tables<R>(&self, f: impl FnOnce(&HashMap<String, TableOptions>) -> R) -> R {
        thread_local! {
            static SNAPSHOT: std::cell::RefCell<Option<(u64, u64, Arc<HashMap<String, TableOptions>>)>> =
                const { std::cell::RefCell::new(None) };
        }
        let instance = self.inner.instance_id;
        let version = self.inner.tables_version.load(Ordering::Acquire);
        SNAPSHOT.with(|cell| {
            let Ok(mut slot) = cell.try_borrow_mut() else {
                let tables = Arc::clone(&self.inner.tables.read().expect("client table cache lock poisoned"));
                return f(&tables);
            };
            if !matches!(&*slot, Some((id, seen, _)) if *id == instance && *seen == version) {
                let fresh = Arc::clone(&self.inner.tables.read().expect("client table cache lock poisoned"));
                *slot = Some((instance, version, fresh));
            }
            let (_, _, tables) = slot.as_ref().expect("just filled");
            f(tables)
        })
    }

    pub fn cached_table(
        &self,
        namespace: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Option<TemporalStoreTable> {
        let namespace = namespace.into();
        let table_name = table_name.into();
        let combined_name = table_combine_name(&namespace, &table_name);
        // A read, so it shares the lock with every request doing the same.
        let options = self.with_tables(|tables| tables.get(&combined_name).cloned())?;
        Some(TemporalStoreTable {
            client: self.clone(),
            namespace,
            table_name,
            shard_id: self.inner.options.default_shard_id,
            options,
            combined_name,
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

    pub fn direct_sdk_parity_report(&self) -> ClientDirectSdkParityReport {
        let migration = self.migration_compatibility_report();
        let replacement = &migration.production_replacement_contract;
        let partition_sets = self.native_partition_set_report();
        let topology = self.topology_cache_report();
        let meta_sync = self.meta_sync_report();
        let native_partition_set_count = partition_sets.len();
        let native_partition_member_count = partition_sets
            .iter()
            .map(|partition_set| partition_set.members.len())
            .sum::<usize>();
        let missing_route_count = partition_sets
            .iter()
            .map(|partition_set| partition_set.missing_route_count)
            .sum::<usize>();
        let native_partition_set_route_cache_ready = native_partition_set_count > 0
            && native_partition_member_count > 0
            && missing_route_count == 0;
        let partition_member_version_ready = partition_sets.iter().all(|partition_set| {
            partition_set.partition_count == partition_set.shard_count as usize
                && partition_set.topology_version > 0
                && partition_set.members.iter().all(|member| {
                    member.route_ready
                        && member.topology_version == partition_set.topology_version
                        && member.start_bucket <= member.end_bucket
                })
        });
        let topology_sync_ready = replacement.topology_sync_tested
            && topology.route_count >= native_partition_member_count
            && topology.max_topology_version > 0
            && topology.unknown_topology_version_routes == 0;
        let meta_syncer_ready = meta_sync.table_count > 0
            && meta_sync.synced_table_count == meta_sync.table_count
            && meta_sync.error_table_count == 0
            && meta_sync.total_sync_generation > 0;
        let retry_budget_ready = replacement.retry_budget_tested
            && self.inner.options.max_read_retries >= 1
            && self.inner.options.max_write_retries == 0;
        let route_invalidation_ready = topology_sync_ready
            && topology.ttl_expired_routes == 0
            && topology.stale_route_count == 0;
        let placement = self.deployment_placement_policy("direct-sdk-parity");
        let placement_hooks_ready =
            replacement.placement_hooks_preserved && placement.placement_hook_ready;
        let location_affine_secondary_reads_ready = placement_hooks_ready
            && placement.require_location_affinity
            && placement.replica_read_policy == ReplicaReadPolicy::RoundRobinReplica;
        let primary_only_writes_ready = partition_sets.iter().all(|partition_set| {
            partition_set
                .members
                .iter()
                .all(|member| member.primary_addr.is_some())
        });
        let rust_native_migration_contract_ready = migration.rust_native_http_ready
            && migration.rust_native_tonic_ready
            && replacement.http_json_contract_tested
            && replacement.resp_contract_tested
            && replacement.tonic_contract_tested
            && !migration.legacy_wire_in_scope;
        let typed_table_client_ready =
            migration.typed_table_client_ready && replacement.typed_table_client_tested;

        let mut blockers = Vec::new();
        for (ready, label) in [
            (
                rust_native_migration_contract_ready,
                "rust_native_migration_contract_missing",
            ),
            (typed_table_client_ready, "typed_table_client_missing"),
            (
                native_partition_set_route_cache_ready,
                "native_partition_set_route_cache_missing",
            ),
            (
                partition_member_version_ready,
                "partition_member_version_missing",
            ),
            (topology_sync_ready, "topology_sync_missing"),
            (meta_syncer_ready, "meta_syncer_missing"),
            (retry_budget_ready, "retry_budget_missing"),
            (route_invalidation_ready, "route_invalidation_missing"),
            (placement_hooks_ready, "placement_hooks_missing"),
            (
                location_affine_secondary_reads_ready,
                "location_affine_secondary_reads_missing",
            ),
            (primary_only_writes_ready, "primary_only_writes_missing"),
        ] {
            if !ready {
                blockers.push(label.to_string());
            }
        }

        ClientDirectSdkParityReport {
            rust_native_migration_contract_ready,
            typed_table_client_ready,
            native_partition_set_route_cache_ready,
            partition_member_version_ready,
            topology_sync_ready,
            meta_syncer_ready,
            retry_budget_ready,
            route_invalidation_ready,
            placement_hooks_ready,
            location_affine_secondary_reads_ready,
            primary_only_writes_ready,
            direct_sdk_command_families: replacement.supported_command_families.clone(),
            native_partition_set_count,
            native_partition_member_count,
            missing_route_count,
            max_topology_version: topology.max_topology_version,
            meta_sync_generation: meta_sync.total_sync_generation,
            ready: blockers.is_empty(),
            blockers,
        }
    }

}

#[derive(Debug, Clone)]
pub struct TemporalStoreTable {
    client: TemporalStoreClient,
    namespace: String,
    table_name: String,
    shard_id: ShardId,
    /// What this table looked like when the handle was opened.
    ///
    /// Only a fallback for `table_options()`, for the case where the client holds no
    /// entry for this table. Everything else must go through `table_options()`, which
    /// reads the copy the metaserver sync keeps current -- this one never changes, so
    /// anything reading it directly is frozen at open time.
    options: TableOptions,
    /// This handle's key into the client's table map, built once.
    ///
    /// It is `namespace` and `table_name` joined, and neither changes for the
    /// life of the handle -- but it was rebuilt, and allocated, on every
    /// request that asked for the live options.
    combined_name: String,
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

    /// The table's options as they stand now, not as they stood when the table was taken.
    ///
    /// Read live on purpose: a table re-synced after a split changes its shard count, and a
    /// handle taken before that must not keep hashing keys over the old one. `shard_id_for_key`
    /// calls this, so it runs for every keyed command -- through the snapshot rather than the
    /// map's lock, which every such command was otherwise taking.
    fn table_options(&self) -> TableOptions {
        self.client
            .with_tables(|tables| tables.get(&self.combined_name).cloned())
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
                self.http_options(&table_options),
                Some(table_options.continuous_failed_time_ms),
                table_options.replica_read_policy,
                if table_options.preferred_location.is_empty() {
                    None
                } else {
                    Some(table_options.preferred_location.as_str())
                },
            )?;
            let decision = classify_retry_decision(
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
        // Shed by key, not by batch. `command_is_dropped` hashes the routing
        // key, so the rate is per key -- but refusing the whole batch as soon
        // as one key fell in the dropped range turned it into 1-(1-p)^n per
        // batch. A 1% rate refused 63% of hundred-key batches, and 5% refused
        // 99.5% of them, which is not a shed rate an operator can reason about.
        //
        // A shed key gets its own slot with its own status, beside the results
        // of the keys that were not shed. That is the shape a batch already
        // has: the grouped path fills a missing slot with a per-response error
        // inside an otherwise-ok batch.
        let shed: Vec<bool> = commands
            .iter()
            .map(|command| command_is_dropped(command, table_options.drop_percent))
            .collect();
        if shed.iter().any(|dropped| *dropped) {
            let kept: Vec<Command> = commands
                .iter()
                .zip(shed.iter())
                .filter(|(_, dropped)| !**dropped)
                .map(|(command, _)| command.clone())
                .collect();
            // Nothing survived: the batch as a whole was shed, and it answers
            // exactly as it always has. Only a batch with survivors changes,
            // and only so the survivors are served.
            if kept.is_empty() {
                let first = commands
                    .first()
                    .and_then(command_key)
                    .map(|key| key.to_string());
                return Ok(BatchExecuteResponse {
                    status: Status::error(
                        "traffic_dropped",
                        format!(
                            "batch command for key {:?} dropped by table drop_percent",
                            first
                        ),
                    ),
                    responses: Vec::new(),
                });
            }
            let served = self.batch_execute(kept)?;
            // A failure that stopped the batch is still the batch's failure.
            if !served.status.ok {
                return Ok(served);
            }
            let mut served = served.responses.into_iter();
            let responses = shed
                .iter()
                .map(|dropped| {
                    if *dropped {
                        ExecuteResponse {
                            status: Status::error(
                                "traffic_dropped",
                                "request dropped by table drop_percent",
                            ),
                            response: CommandResponse::Empty,
                        }
                    } else {
                        served.next().unwrap_or_else(|| ExecuteResponse {
                            status: Status::error(
                                "missing_response",
                                "batch response missing",
                            ),
                            response: CommandResponse::Empty,
                        })
                    }
                })
                .collect();
            return Ok(BatchExecuteResponse {
                status: Status::ok(),
                responses,
            });
        }
        // The LIVE shard count, not the one this handle was opened with. Read from the
        // snapshot, a table that gained shards after the handle was created kept sending
        // every command to the single shard the handle started on -- while
        // `shard_id_for_command`, on this same handle, already knew which shard each key
        // belonged to. The handle worked out the right answer and then declined to use it.
        if table_options.shard_count > 1 {
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
                self.http_options(&table_options),
                Some(table_options.continuous_failed_time_ms),
            )?;
            let decision = classify_retry_decision(
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
        let due = self
            .client
            .inner
            .meta_sync_tables
            .lock()
            .expect("client meta sync table lock poisoned")
            .get(&self.combined_name)
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
            // A command with no routing key pins to the handle's shard, and that is load
            // bearing rather than a lazy default: a multi-part blob upload is Begin, then
            // Appends, then Commit, and the staged parts live on the node that served the
            // Begin. Hashing something per command would scatter the parts of one upload
            // across shards and none of them would commit.
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

    /// Takes the table's options rather than reading them, so it cannot be called with
    /// stale ones. It used to read `self.options` -- the copy this handle was opened
    /// with -- and so kept sending the timeouts the table had when the handle was
    /// created, for the whole life of the handle. `options()` meanwhile reported the
    /// current ones, so the handle told you one timeout and used another.
    fn http_options(&self, table_options: &TableOptions) -> HttpRequestOptions {
        HttpRequestOptions {
            connect_timeout_ms: table_options.connect_timeout_ms,
            io_timeout_ms: table_options.io_timeout_ms,
            max_retries: self.client.inner.options.max_retries,
        }
    }

    #[cfg(test)]
    pub(crate) fn http_options_for_test(&self) -> HttpRequestOptions {
        self.http_options(&self.table_options())
    }

    /// Re-sync this table's topology because a request failed in a way that suggests the
    /// cached one is wrong.
    ///
    /// Spaced by `topo_error_retry_interval_ms`. The per-request guard above stops one command
    /// doing this twice, but says nothing about the other requests in flight: a shard moving
    /// makes MANY requests fail at once, and each one arriving here unthrottled is a separate
    /// metaserver round-trip -- a sync storm aimed at the metaserver at the moment it is
    /// working through a topology change. One request's failure is enough to learn what all of
    /// them need.
    fn refresh_table_topology_after_status(&self) {
        if self.client.inner.options.meta_addr.is_none() {
            return;
        }
        if !self.client.forced_sync_is_due(&self.namespace, &self.table_name) {
            return;
        }
        let _ = self
            .client
            .sync_table_topology(self.namespace.clone(), self.table_name.clone());
    }
}

fn choose_cached_route(
    route: &CachedRoute,
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
            // Advance and read in one step. The counter runs unbounded and is taken
            // modulo the replica count, which keeps the rotation correct across a wrap.
            let next = route
                .next_replica_index
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            route.replica_addrs[next % route.replica_addrs.len()].clone()
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

fn table_combine_name(namespace: &str, table_name: &str) -> String {
    format!("{namespace}/{table_name}")
}

fn client_partition_id_for_offset(options: &TableOptions, offset: u64) -> ShardId {
    options.first_shard_id.saturating_add(offset)
}

fn partition_start_bucket(offset: u64, shard_count: u64) -> u64 {
    let bucket_count = 1_u64 << 30;
    if shard_count == 0 {
        return 0;
    }
    bucket_count.saturating_mul(offset) / shard_count
}

fn partition_end_bucket(offset: u64, shard_count: u64) -> u64 {
    let bucket_count = 1_u64 << 30;
    if shard_count == 0 {
        return 0;
    }
    (bucket_count.saturating_mul(offset.saturating_add(1)) / shard_count).saturating_sub(1)
}

fn meta_sync_jittered_delay_ms(
    base_ms: u64,
    jitter_percent: u8,
    table_key: &str,
    generation: u64,
) -> u64 {
    let base_ms = base_ms.max(1);
    let jitter_bound = base_ms.saturating_mul(jitter_percent.min(100) as u64) / 100;
    if jitter_bound == 0 {
        return base_ms;
    }
    let seed = format!("{table_key}:{generation}");
    base_ms.saturating_add(stable_key_hash(&seed) % jitter_bound.saturating_add(1))
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

#[cfg(test)]
mod tests;
