// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! TemporalStoreClient execute + route-resolution + backend-failure methods, split from client.rs.
use super::*;

impl TemporalStoreClient {
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
                .or_else(|err| {
                    let became_continuous = self.record_backend_failure(
                        &server_addr,
                        self.inner.options.topo_error_retry_interval_ms,
                    );
                    self.inner
                        .stats
                        .lock()
                        .expect("client stats lock poisoned")
                        .record_backend_error(became_continuous);
                    if !self.inner.options.refresh_route_on_backend_error {
                        return Err(err.into());
                    }
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

    pub(super) fn execute_routed(
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

    pub(super) fn execute_routed_with_http(
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

    pub(super) fn execute_routed_with_http_and_policy(
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
                .or_else(|err| {
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
                    if !self.inner.options.refresh_route_on_backend_error {
                        return Err(err.into());
                    }
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

    pub(super) fn batch_execute_with_http(
        &self,
        request: BatchExecuteRequest,
        http_options: HttpRequestOptions,
        continuous_failed_time_ms: Option<u64>,
    ) -> Result<BatchExecuteResponse, ClientError> {
        if self.inner.options.meta_addr.is_some() {
            let server_addr =
                self.resolve_route(request.shard_id, false, continuous_failed_time_ms)?;
            return post_json_with_options(&server_addr, "/batch_execute", &request, http_options)
                .or_else(|err| {
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
                    if !self.inner.options.refresh_route_on_backend_error {
                        return Err(err.into());
                    }
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

    pub(super) fn resolve_route(
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

    pub(super) fn resolve_route_with_policy(
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
                CachedRoute::for_shard(shard_id, server_addr.clone(), "shard_lookup"),
            );
        self.inner
            .stats
            .lock()
            .expect("client stats lock poisoned")
            .route_refreshes += 1;
        Ok(server_addr)
    }

    pub(super) fn record_backend_failure(&self, server_addr: &str, continuous_failed_time_ms: u64) -> bool {
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

    pub(super) fn record_backend_success(&self, server_addr: &str) {
        self.inner
            .backend_failures
            .lock()
            .expect("client backend failure lock poisoned")
            .remove(server_addr);
    }

    pub(super) fn backend_failure_is_continuous(
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
