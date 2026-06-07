use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::http::{
    get_json_with_options, post_json_with_options, HttpError, HttpRequest, HttpRequestOptions,
};
use crate::meta::GetShardResponse;
use crate::types::{
    BatchExecuteRequest, BatchExecuteResponse, CommandResponse, ExecuteRequest, ExecuteResponse,
    ShardId, Status,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyOptions {
    pub meta_addr: String,
    pub route_cache_ttl_ms: u64,
    pub connect_timeout_ms: u64,
    pub io_timeout_ms: u64,
    pub max_retries: usize,
    pub refresh_route_on_backend_error: bool,
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
            route_cache_ttl_ms: 1_000,
            connect_timeout_ms: 200,
            io_timeout_ms: 200,
            max_retries: 0,
            refresh_route_on_backend_error: true,
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
    pub metaserver_errors: u64,
    pub bad_requests: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyInfo {
    pub status: Status,
    pub meta_addr: String,
    pub route_cache_size: usize,
    pub stats: ProxyStats,
    pub boot_time_ms: u64,
}

#[derive(Debug, Clone)]
pub struct ProxyService {
    inner: Arc<ProxyInner>,
}

#[derive(Debug)]
struct ProxyInner {
    options: RwLock<ProxyOptions>,
    routes: RwLock<HashMap<ShardId, CachedRoute>>,
    stats: RwLock<ProxyStats>,
    boot_time_ms: u64,
}

#[derive(Debug, Clone)]
struct CachedRoute {
    server_addr: String,
    fetched_at: Instant,
}

impl ProxyService {
    pub fn new(options: ProxyOptions) -> Self {
        Self {
            inner: Arc::new(ProxyInner {
                options: RwLock::new(options),
                routes: RwLock::default(),
                stats: RwLock::default(),
                boot_time_ms: now_ms(),
            }),
        }
    }

    pub fn handle(&self, request: HttpRequest) -> (u16, Vec<u8>) {
        use crate::http::{json_response, parse_json};
        match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/health") => json_response(200, &Status::ok()),
            ("GET", "/proxy/info") => json_response(200, &self.info()),
            ("GET", "/proxy/config") => {
                let options = self
                    .inner
                    .options
                    .read()
                    .expect("proxy options lock poisoned")
                    .clone();
                json_response(200, &options)
            }
            ("POST", "/proxy/config") => match parse_json::<ProxyOptions>(&request.body) {
                Ok(options) => {
                    self.update_options(options);
                    json_response(200, &Status::ok())
                }
                Err(err) => {
                    self.inc_bad_request();
                    json_response(400, &Status::error("bad_request", err.to_string()))
                }
            },
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
            ("POST", "/execute") => match parse_json::<ExecuteRequest>(&request.body) {
                Ok(req) => json_response(200, &self.execute(req)),
                Err(err) => {
                    self.inc_bad_request();
                    json_response(400, &execute_error("bad_request", err.to_string()))
                }
            },
            ("POST", "/batch_execute") => match parse_json::<BatchExecuteRequest>(&request.body) {
                Ok(req) => json_response(200, &self.batch_execute(req)),
                Err(err) => {
                    self.inc_bad_request();
                    json_response(400, &Status::error("bad_request", err.to_string()))
                }
            },
            _ => json_response(404, &Status::error("not_found", "unknown proxy route")),
        }
    }

    pub fn execute(&self, request: ExecuteRequest) -> ExecuteResponse {
        self.inner
            .stats
            .write()
            .expect("proxy stats lock poisoned")
            .execute_requests += 1;
        match self.resolve_route(request.shard_id, false) {
            Ok(server_addr) => match self.post_execute(&server_addr, &request) {
                Ok(response) => response,
                Err(err) => {
                    self.inner
                        .stats
                        .write()
                        .expect("proxy stats lock poisoned")
                        .backend_errors += 1;
                    if self.options().refresh_route_on_backend_error {
                        match self.resolve_route(request.shard_id, true) {
                            Ok(refreshed) => self
                                .post_execute(&refreshed, &request)
                                .unwrap_or_else(|err| {
                                    execute_error("server_error", err.to_string())
                                }),
                            Err(status) => execute_error(status.code, status.message),
                        }
                    } else {
                        execute_error("server_error", err.to_string())
                    }
                }
            },
            Err(status) => execute_error(status.code, status.message),
        }
    }

    pub fn batch_execute(&self, request: BatchExecuteRequest) -> BatchExecuteResponse {
        self.inner
            .stats
            .write()
            .expect("proxy stats lock poisoned")
            .batch_execute_requests += 1;
        match self.resolve_route(request.shard_id, false) {
            Ok(server_addr) => match self.post_batch_execute(&server_addr, &request) {
                Ok(response) => response,
                Err(err) => {
                    self.inner
                        .stats
                        .write()
                        .expect("proxy stats lock poisoned")
                        .backend_errors += 1;
                    if self.options().refresh_route_on_backend_error {
                        match self.resolve_route(request.shard_id, true) {
                            Ok(refreshed) => self
                                .post_batch_execute(&refreshed, &request)
                                .unwrap_or_else(|err| BatchExecuteResponse {
                                    status: Status::error("server_error", err.to_string()),
                                    responses: Vec::new(),
                                }),
                            Err(status) => BatchExecuteResponse {
                                status,
                                responses: Vec::new(),
                            },
                        }
                    } else {
                        BatchExecuteResponse {
                            status: Status::error("server_error", err.to_string()),
                            responses: Vec::new(),
                        }
                    }
                }
            },
            Err(status) => BatchExecuteResponse {
                status,
                responses: Vec::new(),
            },
        }
    }

    pub fn update_options(&self, options: ProxyOptions) {
        *self
            .inner
            .options
            .write()
            .expect("proxy options lock poisoned") = options;
        self.inner
            .routes
            .write()
            .expect("proxy routes lock poisoned")
            .clear();
    }

    pub fn info(&self) -> ProxyInfo {
        let options = self.options();
        ProxyInfo {
            status: Status::ok(),
            meta_addr: options.meta_addr,
            route_cache_size: self
                .inner
                .routes
                .read()
                .expect("proxy routes lock poisoned")
                .len(),
            stats: *self.inner.stats.read().expect("proxy stats lock poisoned"),
            boot_time_ms: self.inner.boot_time_ms,
        }
    }

    fn resolve_route(&self, shard_id: ShardId, force_refresh: bool) -> Result<String, Status> {
        let options = self.options();
        let ttl = Duration::from_millis(options.route_cache_ttl_ms);
        if !force_refresh {
            if let Some(route) = self
                .inner
                .routes
                .read()
                .expect("proxy routes lock poisoned")
                .get(&shard_id)
                .cloned()
            {
                if route.fetched_at.elapsed() <= ttl {
                    self.inner
                        .stats
                        .write()
                        .expect("proxy stats lock poisoned")
                        .route_cache_hits += 1;
                    return Ok(route.server_addr);
                }
            }
        }
        self.inner
            .stats
            .write()
            .expect("proxy stats lock poisoned")
            .route_cache_misses += 1;
        let response = self.get_shard(shard_id, true)?;
        let server_addr = response
            .location
            .ok_or_else(|| Status::error("shard_not_found", "shard is not registered"))?
            .server_addr;
        self.inner
            .routes
            .write()
            .expect("proxy routes lock poisoned")
            .insert(
                shard_id,
                CachedRoute {
                    server_addr: server_addr.clone(),
                    fetched_at: Instant::now(),
                },
            );
        self.inner
            .stats
            .write()
            .expect("proxy stats lock poisoned")
            .route_refreshes += 1;
        Ok(server_addr)
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

    fn post_execute(
        &self,
        server_addr: &str,
        request: &ExecuteRequest,
    ) -> Result<ExecuteResponse, HttpError> {
        let options = self.options();
        post_json_with_options(server_addr, "/execute", request, options.http_options())
    }

    fn post_batch_execute(
        &self,
        server_addr: &str,
        request: &BatchExecuteRequest,
    ) -> Result<BatchExecuteResponse, HttpError> {
        let options = self.options();
        post_json_with_options(
            server_addr,
            "/batch_execute",
            request,
            options.http_options(),
        )
    }

    fn options(&self) -> ProxyOptions {
        self.inner
            .options
            .read()
            .expect("proxy options lock poisoned")
            .clone()
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

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::TemporalEngine;
    use crate::http::{json_response, parse_json, serve};
    use crate::meta::ShardLocation;
    use crate::types::Command;

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
        proxy.inner.routes.write().unwrap().insert(
            1,
            CachedRoute {
                server_addr: "127.0.0.1:1".to_string(),
                fetched_at: Instant::now(),
            },
        );

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
                            }),
                        },
                    ),
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });
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
