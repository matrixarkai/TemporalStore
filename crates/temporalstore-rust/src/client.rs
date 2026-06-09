use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use thiserror::Error;

use crate::http::{
    get_json, get_json_with_options, post_json, post_json_with_options, HttpError,
    HttpRequestOptions,
};
use crate::meta::GetShardResponse;
use crate::meta::{GetTableTopologyRequest, TableTopologyResponse};
use crate::types::{
    parse_cpp_feature_filters, BatchExecuteRequest, BatchExecuteResponse, Command, CommandResponse,
    ExecuteRequest, ExecuteResponse, FeatureFilter, FeaturePoint, FeatureWritePolicy, IpsStats,
    RiskFamily, SequenceFeatureRow, SequenceQuerySpec, ShardId, Status,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientOptions {
    pub proxy_addr: String,
    pub meta_addr: Option<String>,
    pub default_shard_id: ShardId,
    pub connect_timeout_ms: u64,
    pub io_timeout_ms: u64,
    pub max_retries: usize,
    pub route_cache_ttl_ms: u64,
    pub meta_sync_interval_ms: u64,
    pub topo_error_retry_interval_ms: u64,
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
            route_cache_ttl_ms: 1_000,
            meta_sync_interval_ms: 10 * 60 * 1_000,
            topo_error_retry_interval_ms: 5_000,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableOptions {
    pub io_timeout_ms: u64,
    pub connect_timeout_ms: u64,
    pub continuous_failed_time_ms: u64,
    pub first_shard_id: ShardId,
    pub shard_count: u64,
    pub pin_primary: bool,
    pub replica_read_policy: ReplicaReadPolicy,
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
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaReadPolicy {
    PinPrimary,
    FirstReplica,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
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
    stats: Mutex<ClientStats>,
}

#[derive(Debug, Clone)]
struct CachedRoute {
    primary_addr: String,
    replica_addrs: Vec<String>,
    fetched_at: Instant,
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

    pub fn sync_table_topology(
        &self,
        namespace: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Result<TableOptions, ClientError> {
        let namespace = namespace.into();
        let table_name = table_name.into();
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
                return Err(err.into());
            }
        };
        if !topology.status.ok {
            self.inner
                .stats
                .lock()
                .expect("client stats lock poisoned")
                .meta_sync_errors += 1;
            return Err(ClientError::Status(topology.status.message));
        }
        let table = topology
            .table
            .ok_or_else(|| ClientError::Status("table topology missing".to_string()))?;
        let options = TableOptions {
            first_shard_id: table.first_shard_id,
            shard_count: table.shard_count,
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
                            fetched_at: Instant::now(),
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
        route_cache.clear();
        for (shard_id, route) in routes {
            route_cache.insert(shard_id, route);
        }
        Ok(options)
    }

    pub fn start_meta_sync_loop(&self, interval_ms: u64) -> thread::JoinHandle<()> {
        let client = self.clone();
        let interval = Duration::from_millis(interval_ms.max(1));
        thread::spawn(move || loop {
            let tables = client
                .inner
                .tables
                .lock()
                .expect("client table cache lock poisoned")
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            for table in tables {
                if let Some((namespace, table_name)) = table.split_once('/') {
                    let _ =
                        client.sync_table_topology(namespace.to_string(), table_name.to_string());
                }
            }
            thread::sleep(interval);
        })
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
            Ok(())
        } else {
            Err(ClientError::Status("table not found".to_string()))
        }
    }

    pub fn stats(&self) -> ClientStats {
        *self.inner.stats.lock().expect("client stats lock poisoned")
    }

    pub fn route_cache_size(&self) -> usize {
        self.inner
            .routes
            .lock()
            .expect("client route cache lock poisoned")
            .len()
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
                    fetched_at: Instant::now(),
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
        )
    }

    fn execute_routed_with_http_and_policy(
        &self,
        request: ExecuteRequest,
        force_primary: bool,
        http_options: HttpRequestOptions,
        continuous_failed_time_ms: Option<u64>,
        replica_read_policy: ReplicaReadPolicy,
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
        )
    }

    fn resolve_route_with_policy(
        &self,
        shard_id: ShardId,
        force_refresh: bool,
        continuous_failed_time_ms: Option<u64>,
        replica_read_policy: ReplicaReadPolicy,
    ) -> Result<String, ClientError> {
        let ttl = Duration::from_millis(self.inner.options.route_cache_ttl_ms);
        if !force_refresh {
            if let Some(route) = self
                .inner
                .routes
                .lock()
                .expect("client route cache lock poisoned")
                .get(&shard_id)
                .cloned()
            {
                if route.fetched_at.elapsed() <= ttl {
                    let server_addr = choose_cached_route(&route, replica_read_policy);
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
                    fetched_at: Instant::now(),
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

    pub fn execute(&self, command: Command) -> Result<ExecuteResponse, ClientError> {
        self.client
            .inner
            .stats
            .lock()
            .expect("client stats lock poisoned")
            .execute_requests += 1;
        let shard_id = self.shard_id_for_command(&command);
        let table_options = self.table_options();
        let force_primary = is_write(&command) || table_options.pin_primary;
        let response = self.client.execute_routed_with_http_and_policy(
            ExecuteRequest {
                shard_id,
                command: command.clone(),
            },
            force_primary,
            self.http_options(),
            Some(table_options.continuous_failed_time_ms),
            table_options.replica_read_policy,
        )?;
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
        if self.options.shard_count > 1 {
            return self.batch_execute_grouped_by_shard(commands);
        }
        let request = BatchExecuteRequest {
            shard_id: self.shard_id,
            commands,
        };
        self.client.batch_execute_with_http(
            request,
            self.http_options(),
            Some(self.table_options().continuous_failed_time_ms),
        )
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
            let response = self.client.batch_execute_with_http(
                BatchExecuteRequest { shard_id, commands },
                self.http_options(),
                Some(self.table_options().continuous_failed_time_ms),
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
        command_key(command)
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
}

fn choose_cached_route(route: &CachedRoute, replica_read_policy: ReplicaReadPolicy) -> String {
    match replica_read_policy {
        ReplicaReadPolicy::PinPrimary => route.primary_addr.clone(),
        ReplicaReadPolicy::FirstReplica => route
            .replica_addrs
            .first()
            .cloned()
            .unwrap_or_else(|| route.primary_addr.clone()),
    }
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
            | Command::RiskSet { .. }
            | Command::RiskSetAndGet { .. }
    )
}

fn table_combine_name(namespace: &str, table_name: &str) -> String {
    format!("{namespace}/{table_name}")
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
        | Command::IpsStat { key, .. }
        | Command::IpsFilter { key, .. }
        | Command::IpsRemove { key, .. }
        | Command::IpsDelete { key }
        | Command::IpsCount { key, .. }
        | Command::RiskIncrement { key, .. }
        | Command::RiskIncrementWithOptions { key, .. }
        | Command::RiskCount { key, .. }
        | Command::RiskQuery { key, .. }
        | Command::RiskDetail { key, .. }
        | Command::RiskSet { key, .. }
        | Command::RiskSetAndGet { key, .. }
        | Command::RiskFamilyQuery { key, .. }
        | Command::RiskManager { key } => Some(key),
        Command::IpsBatchQueryLast { .. } | Command::SequenceBatchQuery { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::TemporalEngine;
    use crate::http::{json_response, parse_json, serve};
    use crate::meta::{GetShardResponse, ShardLocation, TableMetaInfo, TablePartition};

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
        let server_addr = test_addr(18_210);
        let proxy_addr = test_addr(18_211);
        let engine_for_server = engine.clone();
        std::thread::spawn(move || {
            serve(&server_addr, move |request| {
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
        let server_addr_for_proxy = test_addr(18_210);
        std::thread::spawn(move || {
            serve(&proxy_addr, move |request| {
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
        wait_for_http(&test_addr(18_211));

        let client = TemporalStoreClient::with_options(ClientOptions {
            proxy_addr: test_addr(18_211),
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
        let server_addr = test_addr(18_212);
        let meta_addr = test_addr(18_213);
        let engine_for_server = engine.clone();
        std::thread::spawn(move || {
            serve(&server_addr, move |request| {
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
        let live_server = test_addr(18_212);
        std::thread::spawn(move || {
            serve(&meta_addr, move |request| {
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
        wait_for_http(&test_addr(18_213));

        let client = TemporalStoreClient::with_options(ClientOptions {
            proxy_addr: "127.0.0.1:1".to_string(),
            meta_addr: Some(test_addr(18_213)),
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
                fetched_at: Instant::now(),
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
    fn client_backend_pool_skips_cached_route_after_continuous_failure_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        let server_addr = test_addr(18_216);
        let meta_addr = test_addr(18_217);
        let engine_for_server = engine.clone();
        std::thread::spawn(move || {
            serve(&server_addr, move |request| {
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
        let live_server = test_addr(18_216);
        std::thread::spawn(move || {
            serve(&meta_addr, move |request| {
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
        wait_for_http(&test_addr(18_217));

        let bad_server = "127.0.0.1:1".to_string();
        let client = TemporalStoreClient::with_options(ClientOptions {
            proxy_addr: bad_server.clone(),
            meta_addr: Some(test_addr(18_217)),
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
                fetched_at: Instant::now(),
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
                continuous_failed_time_ms: 5,
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
        let meta_addr = test_addr(18_214);
        std::thread::spawn(move || {
            serve(&meta_addr, move |request| {
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
        wait_for_http(&test_addr(18_214));

        let client = TemporalStoreClient::with_options(ClientOptions {
            meta_addr: Some(test_addr(18_214)),
            ..ClientOptions::default()
        });
        let table = client.open_table_from_meta("ns", "tbl").unwrap();
        assert_eq!(table.namespace(), "ns");
        assert_eq!(table.table_name(), "tbl");
        let routed = table.shard_id_for_key("routing-key");
        assert!((10..14).contains(&routed));
    }

    #[test]
    fn table_read_policy_can_select_secondary_from_metaserver_topology() {
        let primary_addr = test_addr(18_222);
        let replica_addr = test_addr(18_223);
        let meta_addr = test_addr(18_224);

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
        std::thread::spawn(move || {
            serve(&meta_addr, move |request| {
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
                            }),
                            partitions: vec![TablePartition {
                                shard_id: 1,
                                start_slot: 0,
                                end_slot: u64::MAX,
                                primary: Some(primary_for_meta.clone()),
                                replicas: vec![primary_for_meta.clone(), replica_for_meta.clone()],
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
        wait_for_http(&test_addr(18_224));

        let client = TemporalStoreClient::with_options(ClientOptions {
            proxy_addr: "127.0.0.1:1".to_string(),
            meta_addr: Some(test_addr(18_224)),
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
    fn client_background_meta_sync_updates_existing_table_handle() {
        let meta_addr = test_addr(18_215);
        let first_shard = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(10));
        std::thread::spawn({
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
                                        shard_count: 2,
                                        replica_count: 1,
                                        use_cpp_partition_ids: false,
                                        partition_version: 0,
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
        wait_for_http(&test_addr(18_215));

        let client = TemporalStoreClient::with_options(ClientOptions {
            meta_addr: Some(test_addr(18_215)),
            ..ClientOptions::default()
        });
        let table = client.open_table_from_meta("ns", "tbl").unwrap();
        assert!((10..12).contains(&table.shard_id_for_key("k")));
        first_shard.store(20, std::sync::atomic::Ordering::Relaxed);
        let _syncer = client.start_meta_sync_loop(10);

        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            let shard = table.shard_id_for_key("k");
            if (20..22).contains(&shard) {
                assert!(client.stats().meta_sync_total >= 2);
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
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
        let server_addr = test_addr(18_220);
        let meta_addr = test_addr(18_221);
        let engine_for_server = engine.clone();
        std::thread::spawn(move || {
            serve(&server_addr, move |request| {
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
        let server_addr_for_meta = test_addr(18_220);
        std::thread::spawn(move || {
            serve(&meta_addr, move |request| {
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
        wait_for_http(&test_addr(18_221));

        let client = TemporalStoreClient::with_options(ClientOptions {
            proxy_addr: "127.0.0.1:1".to_string(),
            meta_addr: Some(test_addr(18_221)),
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
        client.close_table(&table).unwrap();
    }

    fn key_for_shard(table: &TemporalStoreTable, shard_id: ShardId) -> String {
        (0..10_000)
            .map(|index| format!("key-{shard_id}-{index}"))
            .find(|key| table.shard_id_for_key(key) == shard_id)
            .unwrap()
    }

    fn test_addr(port: u16) -> String {
        format!("127.0.0.1:{port}")
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
