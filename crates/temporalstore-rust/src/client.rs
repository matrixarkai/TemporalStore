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
    BatchExecuteRequest, BatchExecuteResponse, Command, CommandResponse, ExecuteRequest,
    ExecuteResponse, FeatureFilter, FeaturePoint, SequenceFeatureRow, ShardId, Status,
};

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("http error: {0}")]
    Http(#[from] HttpError),
    #[error("server returned error: {0}")]
    Status(String),
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
}

impl Default for TableOptions {
    fn default() -> Self {
        Self {
            io_timeout_ms: 200,
            connect_timeout_ms: 200,
            continuous_failed_time_ms: 10_000,
            first_shard_id: 1,
            shard_count: 1,
        }
    }
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
    pub backend_successes_after_error: u64,
    pub meta_sync_total: u64,
    pub meta_sync_errors: u64,
}

impl ClientStats {
    fn record_backend_error(&mut self) {
        self.backend_errors += 1;
        self.backend_error_streak += 1;
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
    tables: Mutex<HashMap<String, TableOptions>>,
    stats: Mutex<ClientStats>,
}

#[derive(Debug, Clone)]
struct CachedRoute {
    server_addr: String,
    fetched_at: Instant,
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
            .entry(combine_name)
            .or_insert_with(|| options.clone());
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
        self.inner
            .tables
            .lock()
            .expect("client table cache lock poisoned")
            .insert(table_combine_name(&namespace, &table_name), options.clone());
        self.inner
            .routes
            .lock()
            .expect("client route cache lock poisoned")
            .clear();
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
            let server_addr = self.resolve_route(request.shard_id, false)?;
            return post_json_with_options(&server_addr, "/batch_execute", &request, http_options)
                .or_else(|_| {
                    let refreshed = self.resolve_route(request.shard_id, true)?;
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
        self.resolve_route(shard_id, true)
    }

    fn execute_routed(
        &self,
        request: ExecuteRequest,
        force_primary: bool,
    ) -> Result<ExecuteResponse, ClientError> {
        self.execute_routed_with_http(request, force_primary, self.inner.options.http_options())
    }

    fn execute_routed_with_http(
        &self,
        request: ExecuteRequest,
        force_primary: bool,
        http_options: HttpRequestOptions,
    ) -> Result<ExecuteResponse, ClientError> {
        if self.inner.options.meta_addr.is_some() {
            let server_addr = self.resolve_route(request.shard_id, false)?;
            return post_json_with_options(&server_addr, "/execute", &request, http_options)
                .or_else(|_| {
                    self.inner
                        .stats
                        .lock()
                        .expect("client stats lock poisoned")
                        .record_backend_error();
                    let refreshed = self.resolve_route(request.shard_id, true)?;
                    let response =
                        post_json_with_options(&refreshed, "/execute", &request, http_options)?;
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
    ) -> Result<BatchExecuteResponse, ClientError> {
        if self.inner.options.meta_addr.is_some() {
            let server_addr = self.resolve_route(request.shard_id, false)?;
            return post_json_with_options(&server_addr, "/batch_execute", &request, http_options)
                .or_else(|_| {
                    self.inner
                        .stats
                        .lock()
                        .expect("client stats lock poisoned")
                        .record_backend_error();
                    let refreshed = self.resolve_route(request.shard_id, true)?;
                    let response = post_json_with_options(
                        &refreshed,
                        "/batch_execute",
                        &request,
                        http_options,
                    )?;
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

    fn resolve_route(&self, shard_id: ShardId, force_refresh: bool) -> Result<String, ClientError> {
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
                    self.inner
                        .stats
                        .lock()
                        .expect("client stats lock poisoned")
                        .route_cache_hits += 1;
                    return Ok(route.server_addr);
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
                    server_addr: server_addr.clone(),
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

    pub fn execute(&self, command: Command) -> Result<ExecuteResponse, ClientError> {
        self.client
            .inner
            .stats
            .lock()
            .expect("client stats lock poisoned")
            .execute_requests += 1;
        let shard_id = self.shard_id_for_command(&command);
        let response = self.client.execute_routed_with_http(
            ExecuteRequest {
                shard_id,
                command: command.clone(),
            },
            is_write(&command),
            self.http_options(),
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
        self.client
            .batch_execute_with_http(request, self.http_options())
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
            | Command::StringDelete { .. }
            | Command::HashSet { .. }
            | Command::HashMultiSet { .. }
            | Command::HashIncrBy { .. }
            | Command::HashDelete { .. }
            | Command::SetAdd { .. }
            | Command::SetRemove { .. }
            | Command::FeatureAppend { .. }
            | Command::FeatureReplace { .. }
            | Command::FeatureDelete { .. }
            | Command::SequenceAdd { .. }
            | Command::IpsAdd { .. }
            | Command::RiskIncrement { .. }
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
        | Command::FeatureQuery { key, .. }
        | Command::FeatureReplace { key, .. }
        | Command::FeatureDelete { key }
        | Command::FeatureAggQuery { key, .. }
        | Command::SequenceAdd { key, .. }
        | Command::SequenceQuery { key, .. }
        | Command::IpsAdd { key, .. }
        | Command::IpsQueryLast { key, .. }
        | Command::IpsQueryRange { key, .. }
        | Command::IpsRemove { key, .. }
        | Command::IpsDelete { key }
        | Command::IpsCount { key, .. }
        | Command::RiskIncrement { key, .. }
        | Command::RiskCount { key, .. }
        | Command::RiskQuery { key, .. }
        | Command::RiskDetail { key, .. } => Some(key),
        Command::IpsBatchQueryLast { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::TemporalEngine;
    use crate::http::{json_response, parse_json, serve};
    use crate::meta::{GetShardResponse, ShardLocation};

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
                server_addr: "127.0.0.1:1".to_string(),
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
        assert_eq!(stats.route_refreshes, 2);
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
