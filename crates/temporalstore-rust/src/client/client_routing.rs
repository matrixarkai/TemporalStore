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
                    if !Self::may_send_again(&request.commands, &err) {
                        self.inner
                            .stats
                            .lock()
                            .expect("client stats lock poisoned")
                            .record_write_of_unknown_outcome();
                        let _ = self.resolve_route(request.shard_id, true, None);
                        return Err(ClientError::WriteOutcomeUnknown(err.to_string()));
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

    /// Whether a request that just failed may simply be sent again.
    ///
    /// A read always may -- repeating it cannot change anything. A write may only if the
    /// error proves it never arrived: a refused connection sent no bytes, so nothing was
    /// applied and re-sending is the recovery the caller wants. A read timeout proves
    /// nothing of the sort -- the datanode stopped answering, which is not the same as never
    /// having received the write -- and re-sending there counts `ControlStateIncrement`
    /// twice with nothing downstream able to tell.
    ///
    /// The command layer above already drew this line: it retries a write only when the
    /// backend REFUSED it (`safe_budget_free_write_retry` requires a topology retry), a
    /// definite "not applied". This layer had no such reasoning and re-sent regardless.
    fn may_send_again(commands: &[Command], err: &HttpError) -> bool {
        !commands.iter().any(super::commands::is_write) || err.request_never_reached_the_server()
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
                    if !Self::may_send_again(std::slice::from_ref(&request.command), &err) {
                        self.inner
                            .stats
                            .lock()
                            .expect("client stats lock poisoned")
                            .record_write_of_unknown_outcome();
                        // Drop the stale route so the NEXT request re-resolves, but do not
                        // send this write a second time -- its outcome is unknown, not known
                        // to be "never happened".
                        let _ = self.resolve_route_with_policy(
                            request.shard_id,
                            true,
                            continuous_failed_time_ms,
                            policy,
                            preferred_location,
                        );
                        return Err(ClientError::WriteOutcomeUnknown(err.to_string()));
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
        self.batch_execute_with_http_and_policy(
            request,
            http_options,
            continuous_failed_time_ms,
            ReplicaReadPolicy::PinPrimary,
            None,
        )
    }

    /// A batch sent under a chosen replica read policy.
    ///
    /// The route is resolved with the policy on every attempt, including the refreshes after a
    /// backend failure -- a retry that silently fell back to the primary would make the policy
    /// hold only until the first error.
    pub(super) fn batch_execute_with_http_and_policy(
        &self,
        request: BatchExecuteRequest,
        http_options: HttpRequestOptions,
        continuous_failed_time_ms: Option<u64>,
        replica_read_policy: ReplicaReadPolicy,
        preferred_location: Option<&str>,
    ) -> Result<BatchExecuteResponse, ClientError> {
        if self.inner.options.meta_addr.is_some() {
            let server_addr =
                self.resolve_route_with_policy(
                request.shard_id,
                false,
                continuous_failed_time_ms,
                replica_read_policy,
                preferred_location,
            )?;
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
                    if !Self::may_send_again(&request.commands, &err) {
                        self.inner
                            .stats
                            .lock()
                            .expect("client stats lock poisoned")
                            .record_write_of_unknown_outcome();
                        let _ = self.resolve_route_with_policy(
                            request.shard_id,
                            true,
                            continuous_failed_time_ms,
                            replica_read_policy,
                            preferred_location,
                        );
                        return Err(ClientError::WriteOutcomeUnknown(err.to_string()));
                    }
                    let refreshed = self.resolve_route_with_policy(
                        request.shard_id,
                        true,
                        continuous_failed_time_ms,
                        replica_read_policy,
                        preferred_location,
                    )?;
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
            let route_cache = self
                .inner
                .routes
                .read()
                .expect("client route cache lock poisoned");
            if let Some(route) = route_cache.get(&shard_id) {
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
                            .route_cache_hits
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
        // Stamp the topology this route was resolved against. Without it the route is
        // version 0 = unknown, the staleness check treats unknown as stale, and the entry is
        // dropped on the very next request -- which is why the cache never returned a hit.
        let mut cached = CachedRoute::for_shard(shard_id, server_addr.clone(), "shard_lookup");
        cached.topology_version = self
            .inner
            .known_topology_version
            .load(std::sync::atomic::Ordering::Relaxed);
        self.inner
            .routes
            .write()
            .expect("client route cache lock poisoned")
            .insert(shard_id, cached);
        self.inner
            .stats
            .lock()
            .expect("client stats lock poisoned")
            .route_refreshes += 1;
        Ok(server_addr)
    }

    /// The count the route lookup trusts, for tests that check it against the map.
    #[cfg(test)]
    pub(crate) fn backend_failure_entries_for_test(&self) -> usize {
        self.inner
            .backend_failure_entries
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Every mutation of the backend-failure map goes through here.
    ///
    /// `backend_failure_entries` is what lets the read path skip the lock, and it is only safe to
    /// trust while it equals the map's length. Updating it at each call site is the kind of
    /// invariant that holds until someone adds a fourth site, so there is one site: the count is
    /// restated from the map itself, under the lock that just changed it.
    pub(super) fn with_backend_failures<R>(
        &self,
        f: impl FnOnce(&mut HashMap<String, BackendFailureState>) -> R,
    ) -> R {
        let mut failures = self
            .inner
            .backend_failures
            .lock()
            .expect("client backend failure lock poisoned");
        let out = f(&mut failures);
        self.inner
            .backend_failure_entries
            .store(failures.len(), std::sync::atomic::Ordering::Relaxed);
        out
    }

    pub(super) fn record_backend_failure(&self, server_addr: &str, continuous_failed_time_ms: u64) -> bool {
        let now = Instant::now();
        self.with_backend_failures(|failures| {
            let state = failures
                .entry(server_addr.to_string())
                .or_insert_with(|| BackendFailureState {
                    first_failed_at: now,
                    last_failed_at: now,
                    consecutive_failures: 0,
                });
            state.last_failed_at = now;
            state.consecutive_failures += 1;
            state.first_failed_at.elapsed() >= Duration::from_millis(continuous_failed_time_ms)
        })
    }

    pub(super) fn record_backend_success(&self, server_addr: &str) {
        self.with_backend_failures(|failures| {
            failures.remove(server_addr);
        });
    }

    pub(super) fn backend_failure_is_continuous(
        &self,
        server_addr: &str,
        continuous_failed_time_ms: u64,
    ) -> bool {
        // Nothing has failed, so there is no entry to find and no reason to take the lock.
        // Every cached route lookup reaches here, so on a healthy deployment this is the whole
        // path: the map was being locked once per request to be told it was empty.
        if self
            .inner
            .backend_failure_entries
            .load(std::sync::atomic::Ordering::Relaxed)
            == 0
        {
            return false;
        }
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

/// Which replica policy a batch reads under, and from where.
///
/// A batch may carry writes and those must reach the primary, so the table's policy applies only
/// to a batch that carries none -- the same distinction `force_primary` draws on the single
/// command path, drawn from the same two inputs. `pin_primary` still wins, and a table that has
/// configured nothing reads `PinPrimary`, so a default table's batches do not move.
pub(super) fn batch_read_policy(
    write: bool,
    table_options: &TableOptions,
) -> (ReplicaReadPolicy, Option<&str>) {
    if write || table_options.pin_primary {
        return (ReplicaReadPolicy::PinPrimary, None);
    }
    (
        table_options.replica_read_policy,
        if table_options.preferred_location.is_empty() {
            None
        } else {
            Some(table_options.preferred_location.as_str())
        },
    )
}

#[cfg(test)]
mod batch_routing_tests {
    use super::*;

    /// The client-level batch still takes the primary. The table-level one no longer has to.
    ///
    /// `batch_execute_with_options` resolves through plain `resolve_route`, which hard-codes
    /// `PinPrimary` and no location. It is handed a bare `BatchExecuteRequest` with no table
    /// options, so there is no policy for it to read and it stays as it is -- this holds it there.
    ///
    /// The table-level batch is what changed. It has the table's options, so `batch_read_policy`
    /// gives a batch carrying no writes the table's own policy and location, and a table
    /// configured to read from a nearby replica now does so for its batches too. A batch carrying
    /// a write still takes the primary, because the write must; so does `pin_primary`, which is
    /// on by default -- so a default table's batches do not move. The tests below hold each of
    /// those.
    #[test]
    fn a_batch_goes_to_the_primary_even_when_a_single_read_would_not() {
        let client = TemporalStoreClient::new("127.0.0.1:1");
        {
            let mut routes = client
                .inner
                .routes
                .write()
                .expect("client route cache lock poisoned");
            let mut route = CachedRoute::for_shard(1, "primary:1", "test_insert");
            route.replica_addrs = vec!["replica:1".to_string()];
            routes.insert(1, route);
        }

        let single = client
            .resolve_route_with_policy(1, false, None, ReplicaReadPolicy::FirstReplica, None)
            .expect("route");
        assert_eq!(
            single, "replica:1",
            "a single read honours the table's replica policy"
        );

        let batch = client.resolve_route(1, false, None).expect("route");
        assert_eq!(
            batch, "primary:1",
            "a batch takes the primary regardless of the policy"
        );

        assert_ne!(
            single, batch,
            "the two paths disagree by construction -- if they ever agree, one of them changed              and the comment above needs to change with it"
        );
    }

    /// A batch carrying no writes reads under the table's own policy, and from its own location.
    #[test]
    fn a_read_only_batch_reads_under_the_table_policy() {
        let options = TableOptions {
            pin_primary: false,
            replica_read_policy: ReplicaReadPolicy::FirstReplica,
            preferred_location: "zone-a".to_string(),
            ..TableOptions::default()
        };
        assert_eq!(
            batch_read_policy(false, &options),
            (ReplicaReadPolicy::FirstReplica, Some("zone-a")),
            "a batch of reads should route like the reads it is made of"
        );
    }

    /// One write in the batch sends the whole batch to the primary.
    ///
    /// The batch is one request to one server, so the policy is decided for the batch, not per
    /// command. A single write in it therefore takes all of it to the primary -- there is no
    /// splitting the difference.
    #[test]
    fn one_write_takes_the_whole_batch_to_the_primary() {
        let options = TableOptions {
            pin_primary: false,
            replica_read_policy: ReplicaReadPolicy::FirstReplica,
            preferred_location: "zone-a".to_string(),
            ..TableOptions::default()
        };
        assert_eq!(
            batch_read_policy(true, &options),
            (ReplicaReadPolicy::PinPrimary, None),
            "a batch containing a write must reach the primary"
        );
    }

    /// `pin_primary` outranks the read policy, as it does on the single-command path.
    #[test]
    fn pin_primary_outranks_the_read_policy() {
        let options = TableOptions {
            pin_primary: true,
            replica_read_policy: ReplicaReadPolicy::FirstReplica,
            preferred_location: "zone-a".to_string(),
            ..TableOptions::default()
        };
        assert_eq!(
            batch_read_policy(false, &options),
            (ReplicaReadPolicy::PinPrimary, None),
            "pin_primary is the operator saying primary, and it wins"
        );
    }

    /// A table that configured nothing routes exactly where it did before.
    ///
    /// The blast radius of the change, stated as a test: `TableOptions::default()` sets
    /// `pin_primary` AND `PinPrimary`, so a table nobody configured sends its batches to the
    /// primary whether or not they carry writes. Only a table that asked for replica reads moves.
    #[test]
    fn a_default_table_batches_where_it_always_did() {
        let options = TableOptions::default();
        assert_eq!(
            batch_read_policy(false, &options),
            (ReplicaReadPolicy::PinPrimary, None)
        );
        assert_eq!(
            batch_read_policy(true, &options),
            (ReplicaReadPolicy::PinPrimary, None)
        );
    }

    /// An unset location is no location, not a location named "".
    ///
    /// `preferred_location` is a `String` and its unset value is empty, so passing it through
    /// unchecked would ask route selection to match a zone whose name is the empty string.
    #[test]
    fn an_unset_location_is_no_location() {
        let options = TableOptions {
            pin_primary: false,
            replica_read_policy: ReplicaReadPolicy::FirstReplica,
            preferred_location: String::new(),
            ..TableOptions::default()
        };
        assert_eq!(
            batch_read_policy(false, &options),
            (ReplicaReadPolicy::FirstReplica, None)
        );
    }
}
