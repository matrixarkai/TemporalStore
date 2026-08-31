// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// Server Raft route handler + raft execute/read/membership helpers, split from
// server.rs (textual include!, shared flat scope + use-imports; no mod wrapper).

fn data_raft_read_policy_from_env() -> DataRaftReadPolicy {
    let mode = std::env::var("TS_DATA_RAFT_READ_MODE")
        .or_else(|_| std::env::var("TS_SERVER_RAFT_READ_MODE"))
        .unwrap_or_else(|_| "leader".to_string())
        .parse::<DataRaftReadMode>()
        .unwrap_or_else(|err| panic!("invalid TS_DATA_RAFT_READ_MODE: {err}"));
    DataRaftReadPolicy {
        mode,
        bounded_stale_max_index_lag: env_u64("TS_DATA_RAFT_BOUNDED_STALE_MAX_INDEX_LAG", 0),
        read_index_timeout_ms: env_u64("TS_DATA_RAFT_READ_INDEX_TIMEOUT_MS", 1_000),
    }
}

fn parse_raft_nodes(advertised_addr: &str, local_node_id: RaftNodeId) -> Vec<ProductionRaftNode> {
    std::env::var("TS_RAFT_NODES")
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|part| {
                    let (id, addr) = part.split_once('=')?;
                    Some(ProductionRaftNode {
                        node_id: id.trim().parse().ok()?,
                        addr: addr.trim().to_string(),
                    })
                })
                .collect::<Vec<_>>()
        })
        .filter(|nodes| !nodes.is_empty())
        .unwrap_or_else(|| {
            BTreeMap::from([(local_node_id, advertised_addr.to_string())])
                .into_iter()
                .map(|(node_id, addr)| ProductionRaftNode { node_id, addr })
                .collect()
        })
}

fn handle_server_raft_route(
    state: &ServerRaftState,
    request: &HttpRequest,
) -> Option<(u16, Vec<u8>)> {
    let response = match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/raft/status") => json_response(200, &state.runtime.status()),
        ("GET", "/raft/control/matrixraft_runtime_admin")
        | ("POST", "/raft/control/matrixraft_runtime_admin") => json_response(
            200,
            &state.runtime.cluster().matrixraft_runtime_admin_report(),
        ),
        ("GET", "/raft/control/matrixraft_local_status")
        | ("POST", "/raft/control/matrixraft_local_status") => {
            json_response(200, &state.runtime.cluster().matrixraft_local_status_report())
        }
        ("POST", "/raft/apply_health") => match parse_json::<RaftApplyHealthRequest>(&request.body)
        {
            Ok(req) => json_response(
                200,
                &state.runtime.local_apply_health(req.max_allowed_apply_lag),
            ),
            Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
        },
        ("POST", "/raft/membership/apply") => {
            match parse_json::<RaftMembershipApplyRequest>(&request.body) {
                Ok(req) => {
                    let response = match state.runtime.apply_membership_change_safely(req.voters) {
                        Ok(report) => RaftMembershipApplyResponse {
                            status: Status::ok(),
                            report: Some(report),
                        },
                        Err(err) => RaftMembershipApplyResponse {
                            status: Status::error("raft_error", err.to_string()),
                            report: None,
                        },
                    };
                    json_response(200, &response)
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("GET", "/raft/control/list_membership") | ("POST", "/raft/control/list_membership") => {
            json_response(200, &server_raft_membership_response(state))
        }
        ("POST", "/raft/control/add_node") => {
            match parse_json::<RaftControlNodeRequest>(&request.body) {
                Ok(req) => {
                    let mut voters = state.runtime.cluster().membership().voters;
                    if !voters.contains(&req.node_id) {
                        voters.push(req.node_id);
                        voters.sort_unstable();
                    }
                    json_response(200, &server_raft_apply_membership_response(state, voters))
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/control/remove_node") => {
            match parse_json::<RaftControlNodeRequest>(&request.body) {
                Ok(req) => {
                    let voters = state
                        .runtime
                        .cluster()
                        .membership()
                        .voters
                        .into_iter()
                        .filter(|node_id| *node_id != req.node_id)
                        .collect::<Vec<_>>();
                    json_response(200, &server_raft_apply_membership_response(state, voters))
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/control/trigger_snapshot") => {
            let response = match state.runtime.cluster().maybe_trigger_snapshot() {
                Ok(report) => RaftControlSnapshotResponse {
                    status: Status::ok(),
                    report: Some(report),
                },
                Err(err) => RaftControlSnapshotResponse {
                    status: Status::error("raft_error", err.to_string()),
                    report: None,
                },
            };
            json_response(200, &response)
        }
        ("POST", "/raft/control/read_index") => {
            match parse_json::<RaftControlNodeRequest>(&request.body) {
                Ok(req) => {
                    let response = match state.runtime.cluster().read_index(req.node_id) {
                        Ok(read_index) => RaftControlReadIndexResponse {
                            status: Status::ok(),
                            response: Some(read_index),
                        },
                        Err(err) => RaftControlReadIndexResponse {
                            status: Status::error("raft_error", err.to_string()),
                            response: None,
                        },
                    };
                    json_response(200, &response)
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/control/transfer_leader") => {
            match parse_json::<RaftControlNodeRequest>(&request.body) {
                Ok(req) => {
                    let status = state
                        .runtime
                        .cluster()
                        .transfer_leader(req.node_id)
                        .map(|_| Status::ok())
                        .unwrap_or_else(|err| Status::error("raft_error", err.to_string()));
                    json_response(200, &RaftAdminLivenessResponse { status })
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/control/accept_leadership") => {
            match parse_json::<RaftControlLeadershipRequest>(&request.body) {
                Ok(req) => {
                    let status = if req.node_id != state.local_node_id {
                        Status::error(
                            "bad_request",
                            format!(
                                "node {} cannot accept leadership for node {}",
                                state.local_node_id, req.node_id
                            ),
                        )
                    } else {
                        state
                            .runtime
                            .cluster()
                            .catch_up(req.node_id)
                            .and_then(|_| state.runtime.cluster().transfer_leader(req.node_id))
                            .map(|_| Status::ok())
                            .unwrap_or_else(|err| Status::error("raft_error", err.to_string()))
                    };
                    json_response(200, &status)
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/admin/liveness") => {
            if !state.local_admin_enabled {
                return Some(json_response(
                    403,
                    &Status::error("forbidden", "local admin disabled"),
                ));
            }
            match parse_json::<RaftAdminLivenessRequest>(&request.body) {
                Ok(req) => {
                    let status = state
                        .runtime
                        .cluster()
                        .set_alive(req.node_id, req.alive)
                        .map(|_| Status::ok())
                        .unwrap_or_else(|err| Status::error("raft_error", err.to_string()));
                    json_response(200, &RaftAdminLivenessResponse { status })
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/admin/elect") => {
            if !state.local_admin_enabled {
                return Some(json_response(
                    403,
                    &Status::error("forbidden", "local admin disabled"),
                ));
            }
            match parse_json::<RaftAdminElectRequest>(&request.body) {
                Ok(req) => {
                    let cluster = state.runtime.cluster();
                    let status = cluster
                        .catch_up(req.node_id)
                        .and_then(|_| cluster.elect_leader(req.node_id))
                        .map(|_| Status::ok())
                        .unwrap_or_else(|err| Status::error("raft_error", err.to_string()));
                    json_response(200, &RaftAdminLivenessResponse { status })
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/admin/failover") => {
            if !state.local_admin_enabled {
                return Some(json_response(
                    403,
                    &Status::error("forbidden", "local admin disabled"),
                ));
            }
            let response = match state.runtime.cluster().failover_primary() {
                Ok(report) => RaftAdminFailoverResponse {
                    status: Status::ok(),
                    report: Some(report),
                },
                Err(err) => RaftAdminFailoverResponse {
                    status: Status::error("raft_error", err.to_string()),
                    report: None,
                },
            };
            json_response(200, &response)
        }
        ("POST", "/raft/admin/bootstrap_external_snapshot") => {
            if !state.local_admin_enabled {
                return Some(json_response(
                    403,
                    &Status::error("forbidden", "local admin disabled"),
                ));
            }
            match parse_json::<RaftAdminBootstrapExternalSnapshotRequest>(&request.body) {
                Ok(req) => {
                    let store = Arc::new(FileObjectStore::with_uri_scheme(
                        PathBuf::from(&req.object_root),
                        uri_scheme(&req.snapshot.uri),
                    ));
                    let snapshot_store = S3SnapshotStore::new(
                        req.cluster_id,
                        req.bucket,
                        PathBuf::from(&req.local_root),
                        store,
                    );
                    let response = match tokio::runtime::Runtime::new()
                        .map_err(|err| err.to_string())
                        .and_then(|tokio_runtime| {
                            tokio_runtime
                                .block_on(
                                    state
                                        .runtime
                                        .cluster()
                                        .bootstrap_replica_from_external_snapshot(
                                            req.target_id,
                                            &snapshot_store,
                                            &req.snapshot,
                                            PathBuf::from(&req.local_root)
                                                .join(format!("restore-node-{}", req.target_id)),
                                        ),
                                )
                                .map_err(|err| err.to_string())
                        }) {
                        Ok(plan) => RaftAdminBootstrapExternalSnapshotResponse {
                            status: Status::ok(),
                            plan: Some(plan),
                        },
                        Err(err) => RaftAdminBootstrapExternalSnapshotResponse {
                            status: Status::error("raft_error", err),
                            plan: None,
                        },
                    };
                    json_response(200, &response)
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/admin/publish_external_snapshot") => {
            if !state.local_admin_enabled {
                return Some(json_response(
                    403,
                    &Status::error("forbidden", "local admin disabled"),
                ));
            }
            match parse_json::<RaftAdminPublishExternalSnapshotRequest>(&request.body) {
                Ok(req) => {
                    let store = Arc::new(FileObjectStore::with_uri_scheme(
                        PathBuf::from(&req.object_root),
                        "s3",
                    ));
                    let snapshot_store = S3SnapshotStore::new(
                        req.cluster_id,
                        req.bucket,
                        PathBuf::from(&req.local_root),
                        store,
                    );
                    let response = match tokio::runtime::Runtime::new()
                        .map_err(|err| err.to_string())
                        .and_then(|tokio_runtime| {
                            tokio_runtime
                                .block_on(
                                    state
                                        .runtime
                                        .cluster()
                                        .publish_leader_snapshot_to_store(&snapshot_store),
                                )
                                .map_err(|err| err.to_string())
                        }) {
                        Ok(report) => RaftAdminPublishExternalSnapshotResponse {
                            status: Status::ok(),
                            report: Some(report),
                        },
                        Err(err) => RaftAdminPublishExternalSnapshotResponse {
                            status: Status::error("raft_error", err),
                            report: None,
                        },
                    };
                    json_response(200, &response)
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/admin/local_catch_up") => {
            if !state.local_admin_enabled {
                return Some(json_response(
                    403,
                    &Status::error("forbidden", "local admin disabled"),
                ));
            }
            match parse_json::<RaftAdminCatchUpRequest>(&request.body) {
                Ok(req) => {
                    let status = state
                        .runtime
                        .cluster()
                        .catch_up(req.node_id)
                        .map(|_| Status::ok())
                        .unwrap_or_else(|err| Status::error("raft_error", err.to_string()));
                    json_response(200, &RaftAdminLivenessResponse { status })
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/admin/wait_applied") => {
            if !state.local_admin_enabled {
                return Some(json_response(
                    403,
                    &Status::error("forbidden", "local admin disabled"),
                ));
            }
            match parse_json::<RaftAdminWaitAppliedRequest>(&request.body) {
                Ok(req) => {
                    let status = state
                        .runtime
                        .wait_for_applied_index(req.node_id, req.index, req.timeout_ms)
                        .map(|_| Status::ok())
                        .unwrap_or_else(|err| Status::error("raft_error", err.to_string()));
                    json_response(200, &RaftAdminLivenessResponse { status })
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/admin/block_peer") => {
            if !state.local_admin_enabled {
                return Some(json_response(
                    403,
                    &Status::error("forbidden", "local admin disabled"),
                ));
            }
            match parse_json::<RaftAdminPeerBlockRequest>(&request.body) {
                Ok(req) => {
                    let mut blocked = state
                        .blocked_peers
                        .lock()
                        .expect("blocked peer lock poisoned");
                    if req.blocked {
                        blocked.insert(req.peer_id);
                    } else {
                        blocked.remove(&req.peer_id);
                    }
                    json_response(
                        200,
                        &RaftAdminLivenessResponse {
                            status: Status::ok(),
                        },
                    )
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/propose") => {
            match parse_json::<DistributedRaftProposeRequest>(&request.body) {
                Ok(req) => {
                    json_response(200, &command_response(state.runtime.propose(req.command)))
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/read") => match parse_json::<DistributedRaftReadRequest>(&request.body) {
            Ok(req) => json_response(
                200,
                &command_response(state.runtime.read_local(req.node_id, req.command)),
            ),
            Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
        },
        _ if request.path.starts_with("/raft/") => {
            if let Some(peer_id) = incoming_raft_peer_id(request) {
                if state
                    .blocked_peers
                    .lock()
                    .expect("blocked peer lock poisoned")
                    .contains(&peer_id)
                {
                    return Some(json_response(
                        503,
                        &Status::error("raft_peer_blocked", "local chaos peer block active"),
                    ));
                }
            }
            handle_authenticated_raft_http(
                &state.runtime.cluster(),
                HttpRequest {
                    method: request.method.clone(),
                    path: request.path.clone(),
                    body: request.body.clone(),
                },
                state.runtime.peer_auth_token().unwrap_or_default(),
            )
        }
        _ => return None,
    };
    Some(response)
}

fn server_raft_membership_response(state: &ServerRaftState) -> RaftControlMembershipResponse {
    let membership = state.runtime.cluster().membership();
    RaftControlMembershipResponse {
        status: Status::ok(),
        shard_id: membership.shard_id,
        leader_id: membership.leader_id,
        voters: membership.voters,
    }
}

fn server_raft_apply_membership_response(
    state: &ServerRaftState,
    voters: Vec<RaftNodeId>,
) -> RaftMembershipApplyResponse {
    match state.runtime.apply_membership_change_safely(voters) {
        Ok(report) => RaftMembershipApplyResponse {
            status: Status::ok(),
            report: Some(report),
        },
        Err(err) => RaftMembershipApplyResponse {
            status: Status::error("raft_error", err.to_string()),
            report: None,
        },
    }
}

fn execute_via_server_raft(state: &ServerRaftState, request: ExecuteRequest) -> ExecuteResponse {
    let result = if is_raft_read_command(&request.command) {
        read_via_server_raft(state, request.command)
    } else {
        state.runtime.propose(request.command)
    };
    match result {
        Ok(response) => ExecuteResponse {
            status: Status::ok(),
            response,
        },
        Err(err) => ExecuteResponse {
            status: Status::error("raft_error", err.to_string()),
            response: CommandResponse::Empty,
        },
    }
}

fn read_via_server_raft(
    state: &ServerRaftState,
    command: Command,
) -> Result<CommandResponse, temporalstore_rust::RaftError> {
    let target_node_id = match state.read_policy.mode {
        DataRaftReadMode::Leader | DataRaftReadMode::Linearizable => {
            state.runtime.status().leader_id
        }
        DataRaftReadMode::BoundedStale | DataRaftReadMode::UnsafeAnyReplica => state.local_node_id,
    };
    let cluster = state.runtime.cluster();
    let read_index_response = cluster.check_data_raft_read_policy(target_node_id, state.read_policy)?;
    // R3: the read-index round returns the frontier this read must observe, but the previous
    // code discarded it and served immediately — so a replica that had COMMITTED but not yet
    // APPLIED up to that index could answer with stale/half-applied state. Wait for the target
    // to apply through the returned read_index before serving. Gated so default behavior is
    // unchanged.
    // R3: a replica can hold commit_index == leader_commit while its state machine has applied
    // only a prefix of that. Wait for apply to reach the read index before serving, so the read
    // never returns half-applied state as fresh.
    cluster.wait_for_applied_index(
        target_node_id,
        read_index_response.read_index,
        state.read_policy.read_index_timeout_ms,
    )?;
    cluster.read_from_replica(target_node_id, command)
}

fn command_response(
    result: Result<CommandResponse, temporalstore_rust::RaftError>,
) -> DistributedRaftCommandResponse {
    match result {
        Ok(response) => DistributedRaftCommandResponse {
            status: Status::ok(),
            response,
        },
        Err(err) => DistributedRaftCommandResponse {
            status: Status::error("raft_error", err.to_string()),
            response: CommandResponse::Empty,
        },
    }
}

fn incoming_raft_peer_id(request: &HttpRequest) -> Option<RaftNodeId> {
    match request.path.as_str() {
        "/raft/append_entries" | "/raft/install_snapshot" | "/raft/install_snapshot_chunk" => {
            // A binary body has no field to read out by name, so decode it. Returning None
            // here would quietly stop attributing the request to the peer that sent it.
            if temporalstore_rust::raft::is_binary_rpc(&request.body) {
                return temporalstore_rust::raft::decode_append_entries(&request.body)
                    .ok()
                    .map(|decoded| decoded.leader_id);
            }
            serde_json::from_slice::<serde_json::Value>(&request.body)
                .ok()?
                .get("leader_id")?
                .as_u64()
        }
        "/raft/request_vote" => serde_json::from_slice::<serde_json::Value>(&request.body)
            .ok()?
            .get("candidate_id")?
            .as_u64(),
        _ => None,
    }
}

fn is_raft_read_command(command: &Command) -> bool {
    matches!(
        command,
        Command::CommonTtl { .. }
            | Command::CommonExists { .. }
            | Command::StringGet { .. }
            | Command::HashGet { .. }
            | Command::HashMultiGet { .. }
            | Command::HashGetAll { .. }
            | Command::HashLen { .. }
            | Command::SetMembers { .. }
            | Command::FeatureQuery { .. }
            | Command::FeatureQueryFiltered { .. }
            | Command::FeatureAggQuery { .. }
            | Command::SequenceQuery { .. }
            | Command::SequenceBatchQuery { .. }
            | Command::ControlStateCount { .. }
            | Command::ControlStateQuery { .. }
            | Command::ControlStateDetail { .. }
            // NB: neither ControlStateSetAndGet nor ...WithOptions belongs here. Both MUTATE
            // (they add `amount` to the series and persist a control-state page --
            // execute_on_shard.rs) and the engine's is_write_command classifies both as writes,
            // exactly as the control-state SETANDGET commands are registered as writes.
            // Serving a mutation on the local read path
            // applies it only on the leader and skips the raft log, so followers never see it ->
            // replica divergence + loss on failover. They must fall through to `propose`.
            | Command::ControlStateFamilyQuery { .. }
            | Command::ControlStateManager { .. }
    )
}

fn startup_load_shard_request(shard_id: u64, node_id: u64) -> LoadShardRequest {
    LoadShardRequest {
        shard_id,
        load_version: env_u64("TS_SHARD_LOAD_VERSION", 0),
        local_node_id: if node_id == 0 { None } else { Some(node_id) },
        shard_uri: std::env::var("TS_SHARD_URI")
            .unwrap_or_else(|_| format!("local://shard/{shard_id}")),
        start_routing_bucket: env_u32("TS_SHARD_START_ROUTING_SLOT", 0),
        end_routing_bucket: env_u32("TS_SHARD_END_ROUTING_SLOT", u32::MAX),
        readonly: env_bool("TS_SHARD_READONLY", env_bool("TS_SERVER_READONLY", false)),
        table_name: std::env::var("TS_TABLE_NAME").unwrap_or_default(),
    }
}

fn update_membership_with_finish_callback(
    engine: &TemporalEngine,
    meta_addr: &str,
    server_addr: &str,
    request: MembershipUpdateRequest,
) -> Status {
    let shard_id = request.shard_id;
    let status = engine.update_membership(request);
    if status.ok {
        if let Some(info) = engine.get_info(shard_id).info {
            let _ = post_json::<_, AckResponse>(
                meta_addr,
                "/shards/finish_load",
                &LoadFinishRequest {
                    server_addr: server_addr.to_string(),
                    shard_id,
                    load_version: info.load_version,
                    status: status.clone(),
                    scheduler_task_id: None,
                    scheduler_generation: None,
                },
            );
        }
    }
    status
}

