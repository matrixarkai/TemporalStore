//! Test part 2, split from tests.rs.
#![allow(clippy::all)]
use super::*;
use super::helpers::*;

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
                                shards: Vec::new(),
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
                            shards: Vec::new(),
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

// shared-corpus: control_client_metasync_outage_churn_stress
#[test]
fn client_metasync_backoff_deadline_and_topology_refresh_survive_outage_churn() {
    let meta_addr = free_local_addr();
    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let meta_addr_for_listener = meta_addr.clone();
    let attempts_for_listener = std::sync::Arc::clone(&attempts);
    std::thread::spawn(move || {
        serve(&meta_addr_for_listener, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/meta/topology_version") => json_response(
                    200,
                    &crate::meta::TopologyVersionReport {
                        status: Status::ok(),
                        current_topology_version: 40,
                        old_topology_version: 0,
                        unchanged: false,
                        server_count: 1,
                        proxy_count: 0,
                        table_count: 1,
                        shard_route_count: 1,
                        normal_servers: 1,
                        frozen_servers: 0,
                        dropped_servers: 0,
                        normal_proxies: 0,
                        frozen_proxies: 0,
                        dropped_proxies: 0,
                        normal_tables: 1,
                        frozen_tables: 0,
                        dropped_tables: 0,
                        changed_tables: Vec::new(),
                        events: Vec::new(),
                        event_history_truncated: false,
                    },
                ),
                ("POST", "/tables/topology") => {
                    let attempt =
                        attempts_for_listener.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if attempt < 2 {
                        return json_response(
                            200,
                            &TableTopologyResponse {
                                status: Status::error("metaserver_unavailable", "outage"),
                                table: None,
                                shards: Vec::new(),
                                unchanged: false,
                            },
                        );
                    }
                    json_response(
                        200,
                        &TableTopologyResponse {
                            status: Status::ok(),
                            table: Some(TableMetaInfo {
                                table_id: 91,
                                namespace: "ns".to_string(),
                                table_name: "churn".to_string(),
                                state: crate::meta::MetaEntityState::Normal,
                                topology_version: 40,
                                first_shard_id: 40,
                                shard_count: 1,
                                replica_count: 1,
                                use_cpp_partition_ids: false,
                                partition_version: 0,
                                serving_options: crate::meta::TableServingOptions::default(),
                            }),
                            shards: vec![TableShard {
                                shard_id: 40,
                                start_bucket: 0,
                                end_bucket: 1_073_741_823,
                                primary: Some("127.0.0.1:27440".to_string()),
                                replicas: vec!["127.0.0.1:27440".to_string()],
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
    });
    wait_for_http(&meta_addr);

    let client = TemporalStoreClient::with_options(ClientOptions {
        meta_addr: Some(meta_addr),
        meta_sync_interval_ms: 50,
        topo_error_retry_interval_ms: 5,
        meta_sync_deadline_ms: 7,
        meta_sync_jitter_percent: 50,
        ..ClientOptions::default()
    });
    client.open_table(
        "ns",
        "churn",
        TableOptions {
            first_shard_id: 10,
            shard_count: 1,
            ..TableOptions::default()
        },
    );

    std::thread::sleep(Duration::from_millis(80));
    assert_eq!(
        client.run_due_meta_sync_once(ClientMetaSyncLoopOptions {
            tick_ms: 1,
            max_tables_per_tick: 1,
        }),
        1
    );
    let first_error = client
        .meta_sync_report()
        .tables
        .into_iter()
        .find(|table| table.table == "ns/churn")
        .unwrap();
    assert_eq!(first_error.consecutive_errors, 1);
    assert_eq!(first_error.last_error, "outage");
    let first_delay = first_error
        .next_sync_after_unix_ms
        .saturating_sub(first_error.last_error_unix_ms);
    assert!((5..=8).contains(&first_delay));

    std::thread::sleep(Duration::from_millis(12));
    assert_eq!(
        client.run_due_meta_sync_once(ClientMetaSyncLoopOptions {
            tick_ms: 1,
            max_tables_per_tick: 1,
        }),
        1
    );
    let second_error = client
        .meta_sync_report()
        .tables
        .into_iter()
        .find(|table| table.table == "ns/churn")
        .unwrap();
    assert_eq!(second_error.consecutive_errors, 2);
    let second_delay = second_error
        .next_sync_after_unix_ms
        .saturating_sub(second_error.last_error_unix_ms);
    assert!((10..=15).contains(&second_delay));

    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(
        client.run_due_meta_sync_once(ClientMetaSyncLoopOptions {
            tick_ms: 1,
            max_tables_per_tick: 1,
        }),
        1
    );
    let success = client
        .meta_sync_report()
        .tables
        .into_iter()
        .find(|table| table.table == "ns/churn")
        .unwrap();
    assert_eq!(success.consecutive_errors, 0);
    assert_eq!(success.last_topology_version, 40);
    assert!(success.next_sync_after_unix_ms > success.last_success_unix_ms);
    let table = client.cached_table("ns", "churn").unwrap();
    assert_eq!(table.options().first_shard_id, 40);
    assert_eq!(client.topology_cache_report().max_topology_version, 40);
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
                        shards: Vec::new(),
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
                        shards: vec![TableShard {
                            shard_id: 1,
                            start_bucket: 0,
                            end_bucket: u64::MAX,
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
fn client_router_matches_cpp_crc64_bucket_formula() {
    assert_eq!(crc64_jones(b"123456789"), 0xe9c6d914c4b8d9ca);
    assert_eq!(bucket_id_for_key("123456789"), 0x3a71_b645);
    assert_eq!(
        shard_id_for_key("123456789", 10, 4, 1),
        10 + (0x3a71_b645 % 4)
    );
    assert_eq!(stable_key_hash("123456789"), crc64_jones(b"123456789"));
}

#[test]
fn client_router_round_robins_secondary_reads_like_cpp_router() {
    let mut route = CachedRoute {
        table_key: String::new(),
        partition_id: 1,
        start_bucket: 0,
        end_bucket: 0,
        use_cpp_partition_ids: false,
        partition_version: 0,
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
        table_key: String::new(),
        partition_id: 1,
        start_bucket: 0,
        end_bucket: 0,
        use_cpp_partition_ids: false,
        partition_version: 0,
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

// shared-corpus: control_client_deployment_placement_routing_hooks
#[test]
fn client_deployment_placement_routes_reads_to_local_secondary_and_writes_to_primary() {
    let primary_addr = free_local_addr();
    let replica_addr = free_local_addr();
    let meta_addr = free_local_addr();
    let primary_writes = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let primary_reads = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let replica_writes = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let replica_reads = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    std::thread::spawn({
        let primary_addr = primary_addr.clone();
        let primary_writes = std::sync::Arc::clone(&primary_writes);
        let primary_reads = std::sync::Arc::clone(&primary_reads);
        move || {
            serve(&primary_addr, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/execute") => {
                        let req = parse_json::<ExecuteRequest>(&request.body).unwrap();
                        match req.command {
                            Command::StringSet { .. } => {
                                primary_writes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                json_response(
                                    200,
                                    &ExecuteResponse {
                                        status: Status::ok(),
                                        response: CommandResponse::Empty,
                                    },
                                )
                            }
                            Command::StringGet { .. } => {
                                primary_reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                json_response(
                                    200,
                                    &ExecuteResponse {
                                        status: Status::ok(),
                                        response: CommandResponse::Bytes {
                                            value: Some(b"primary".to_vec()),
                                        },
                                    },
                                )
                            }
                            _ => json_response(400, &Status::error("bad_request", "unexpected")),
                        }
                    }
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        }
    });

    std::thread::spawn({
        let replica_addr = replica_addr.clone();
        let replica_writes = std::sync::Arc::clone(&replica_writes);
        let replica_reads = std::sync::Arc::clone(&replica_reads);
        move || {
            serve(&replica_addr, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/execute") => {
                        let req = parse_json::<ExecuteRequest>(&request.body).unwrap();
                        match req.command {
                            Command::StringGet { .. } => {
                                replica_reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                json_response(
                                    200,
                                    &ExecuteResponse {
                                        status: Status::ok(),
                                        response: CommandResponse::Bytes {
                                            value: Some(b"replica-local".to_vec()),
                                        },
                                    },
                                )
                            }
                            Command::StringSet { .. } => {
                                replica_writes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                json_response(
                                    200,
                                    &ExecuteResponse {
                                        status: Status::error(
                                            "wrong_endpoint",
                                            "replica received primary-only write",
                                        ),
                                        response: CommandResponse::Empty,
                                    },
                                )
                            }
                            _ => json_response(400, &Status::error("bad_request", "unexpected")),
                        }
                    }
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        }
    });

    std::thread::spawn({
        let meta_addr = meta_addr.clone();
        let primary_addr = primary_addr.clone();
        let replica_addr = replica_addr.clone();
        move || {
            serve(&meta_addr, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/tables/topology") => json_response(
                        200,
                        &TableTopologyResponse {
                            status: Status::ok(),
                            table: Some(TableMetaInfo {
                                table_id: 81,
                                namespace: "ns".to_string(),
                                table_name: "placed".to_string(),
                                state: crate::meta::MetaEntityState::Normal,
                                topology_version: 11,
                                first_shard_id: 81,
                                shard_count: 1,
                                replica_count: 2,
                                use_cpp_partition_ids: false,
                                partition_version: 3,
                                serving_options: crate::meta::TableServingOptions {
                                    pin_primary: false,
                                    replica_read_policy: "round_robin_replica".to_string(),
                                    preferred_location: String::new(),
                                    drop_percent: 0,
                                    max_read_retries: 1,
                                    max_write_retries: 1,
                                    retry_backoff_ms: 1,
                                    continuous_failed_time_ms: 100,
                                    io_timeout_ms: 1_000,
                                    connect_timeout_ms: 1_000,
                                },
                            }),
                            shards: vec![TableShard {
                                shard_id: 81,
                                start_bucket: 0,
                                end_bucket: u64::MAX,
                                primary: Some(primary_addr.clone()),
                                replicas: vec![primary_addr.clone(), replica_addr.clone()],
                                primary_endpoint: Some(ServerEndpoint {
                                    server_addr: primary_addr.clone(),
                                    location: "zone-primary".to_string(),
                                }),
                                replica_endpoints: vec![
                                    ServerEndpoint {
                                        server_addr: primary_addr.clone(),
                                        location: "zone-primary".to_string(),
                                    },
                                    ServerEndpoint {
                                        server_addr: replica_addr.clone(),
                                        location: "zone-local".to_string(),
                                    },
                                ],
                            }],
                            unchanged: false,
                        },
                    ),
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        }
    });

    wait_for_http(&primary_addr);
    wait_for_http(&replica_addr);
    wait_for_http(&meta_addr);

    let client = TemporalStoreClient::with_options(ClientOptions {
        proxy_addr: "127.0.0.1:1".to_string(),
        meta_addr: Some(meta_addr),
        local_location: "zone-local".to_string(),
        route_cache_ttl_ms: 60_000,
        ..ClientOptions::default()
    });
    let placement = client.deployment_placement_policy("neptune-prod");
    assert_eq!(placement.preferred_location, "zone-local");
    assert!(placement.require_location_affinity);

    let table = client.open_table_from_meta("ns", "placed").unwrap();
    assert_eq!(
        table.options().replica_read_policy,
        ReplicaReadPolicy::RoundRobinReplica
    );
    assert_eq!(table.options().preferred_location, "zone-local");
    assert!(!table.options().pin_primary);

    table.set("placed-key", b"value".to_vec()).unwrap();
    assert_eq!(
        table.get("placed-key").unwrap(),
        Some(b"replica-local".to_vec())
    );

    assert_eq!(primary_writes.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(primary_reads.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(replica_reads.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(replica_writes.load(std::sync::atomic::Ordering::SeqCst), 0);
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

