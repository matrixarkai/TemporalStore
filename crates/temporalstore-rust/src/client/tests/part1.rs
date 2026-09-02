// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Test part 1, split from tests.rs.
#![allow(clippy::all)]
use super::*;
use super::helpers::*;

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
    assert_eq!(report.native_partition_sets.len(), 1);
    assert_eq!(report.native_partition_sets[0].namespace, "ns");
    assert_eq!(report.native_partition_sets[0].table_name, "tbl");
    assert_eq!(report.native_partition_sets[0].first_shard_id, 7);
    assert_eq!(report.native_partition_sets[0].partition_count, 1);
    assert_eq!(report.native_partition_sets[0].missing_route_count, 0);
    assert_eq!(report.native_partition_sets[0].members[0].partition_id, 7);
    assert_eq!(
        report.native_partition_sets[0].members[0]
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
        ClientCompatibilityMode::WireMigrationOutOfScope
    );
    assert!(migration.rust_native_http_ready);
    assert!(migration.rust_native_tonic_ready);
    assert!(!migration.legacy_wire_in_scope);
    assert!(!migration.native_wire_compatible_ready);
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
        "legacy wire migration shims are out of scope; use Rust-native migration contract"
    );
    assert!(migration
        .production_replacement_contract
        .production_protocols
        .contains(&"HTTP/JSON".to_string()));
    assert!(migration
        .production_replacement_contract
        .production_protocols
        .contains(&"RESP".to_string()));
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
    assert!(
        migration
            .production_replacement_contract
            .http_json_contract_tested
    );
    assert!(
        migration
            .production_replacement_contract
            .resp_contract_tested
    );
    assert!(
        migration
            .production_replacement_contract
            .tonic_contract_tested
    );
    assert!(
        migration
            .production_replacement_contract
            .typed_table_client_tested
    );
    assert!(
        migration
            .production_replacement_contract
            .topology_sync_tested
    );
    assert!(
        migration
            .production_replacement_contract
            .retry_budget_tested
    );
    assert!(
        migration
            .production_replacement_contract
            .migration_docs_ready
    );
    assert!(migration
        .blockers
        .iter()
        .any(|blocker| blocker.contains("legacy wire")));
    for family in [
        "common", "string", "hash", "set", "feature", "sequence", "control_state", "redis", "admin",
        "context",
    ] {
        assert!(migration
            .production_replacement_contract
            .supported_command_families
            .contains(&family.to_string()));
    }
}

#[test]
fn native_partition_set_report_marks_missing_routes() {
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

    let reports = client.native_partition_set_report();
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
fn table_typed_methods_and_pipeline_match_client_shape() {
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
                            registered_at_ms: 0,
                            preferred_location: String::new(),
                            state: crate::meta::MetaEntityState::Normal,
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

    table.control_state_increment("control_state", 10, 5).unwrap();
    table.control_state_increment("control_state", 20, -2).unwrap();
    table.control_state_increment("control_state", 30, 7).unwrap();
    assert_eq!(table.control_state_query("control_state", 0, 40, "sum").unwrap(), 10);
    assert_eq!(table.control_state_query("control_state", 0, 40, "last").unwrap(), 7);
    assert_eq!(
        table.control_state_detail("control_state", 15, 40, Some(2)).unwrap(),
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
        .control_state_family_set(ControlStateFamily::Counter, "control_state-native", 10, 5)
        .unwrap();
    assert_eq!(
        table
            .control_state_family_set_and_get(ControlStateFamily::Counter, "control_state-native", 20, 7, 0, 30, "sum")
            .unwrap(),
        12
    );
    table
        .control_state_family_set(ControlStateFamily::Distinct, "control_state-native", 10, 3)
        .unwrap();
    assert_eq!(
        table
            .control_state_family_set_and_get(ControlStateFamily::Distinct, "control_state-native", 20, 4, 0, 30, "sum")
            .unwrap(),
        7
    );
    table
        .control_state_family_set(ControlStateFamily::Selection, "control_state-native", 10, 11)
        .unwrap();
    assert_eq!(
        table
            .control_state_family_query(ControlStateFamily::Selection, "control_state-native", 0, 30, "sum")
            .unwrap(),
        11
    );
    table
        .control_state_selection_set(
            "control_state-fol-first",
            b"middle".to_vec(),
            20,
            60_000,
            ControlStateSelectionType::First,
        )
        .unwrap();
    table
        .control_state_selection_set(
            "control_state-fol-first",
            b"first".to_vec(),
            10,
            60_000,
            ControlStateSelectionType::First,
        )
        .unwrap();
    table
        .control_state_selection_set(
            "control_state-fol-first",
            b"last".to_vec(),
            30,
            60_000,
            ControlStateSelectionType::First,
        )
        .unwrap();
    assert_eq!(
        table.control_state_selection_query("control_state-fol-first").unwrap(),
        Some(b"first".to_vec())
    );
    table
        .control_state_selection_set(
            "control_state-fol-last",
            b"middle".to_vec(),
            20,
            60_000,
            ControlStateSelectionType::Last,
        )
        .unwrap();
    table
        .control_state_selection_set(
            "control_state-fol-last",
            b"first".to_vec(),
            10,
            60_000,
            ControlStateSelectionType::Last,
        )
        .unwrap();
    table
        .control_state_selection_set(
            "control_state-fol-last",
            b"last".to_vec(),
            30,
            60_000,
            ControlStateSelectionType::Last,
        )
        .unwrap();
    assert_eq!(
        table.control_state_selection_query("control_state-fol-last").unwrap(),
        Some(b"last".to_vec())
    );
    assert_eq!(
        table.control_state_manager("control_state-native").unwrap(),
        vec![
            ("h_events".to_string(), b"2".to_vec()),
            ("h_sum".to_string(), b"12".to_vec()),
            ("cpc_events".to_string(), b"2".to_vec()),
            ("cpc_sum".to_string(), b"7".to_vec()),
            ("fol_events".to_string(), b"1".to_vec()),
            ("fol_sum".to_string(), b"11".to_vec()),
        ]
    );
    let debug = table.control_state_debug("control_state-native", 0, 15).unwrap();
    assert!(debug.contains(&("key".to_string(), b"control_state-native".to_vec())));
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
                            registered_at_ms: 0,
                            preferred_location: String::new(),
                            state: crate::meta::MetaEntityState::Normal,
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
    client
        .inner
        .routes
        .write()
        .unwrap()
        .insert(1, CachedRoute::for_shard(1, "127.0.0.1:1", "test_insert"));
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
                            partition_version: 0,
                            serving_options: crate::meta::TableServingOptions::default(),
                        }),
                        shards: vec![TableShard {
                            shard_id: 1,
                            start_bucket: 0,
                            end_bucket: u64::MAX,
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
fn the_backend_failure_count_tracks_the_map_it_stands_in_for() {
    // The route lookup skips the failure-map lock when this count is zero, so the count is only
    // safe while it equals the map's length. Every mutation goes through `with_backend_failures`
    // for that reason; this fails if a path is ever added that does not.
    let client = TemporalStoreClient::new("127.0.0.1:1");
    assert_eq!(
        client.backend_failure_entries_for_test(),
        0,
        "a fresh client has recorded no failures"
    );

    client.insert_backend_failure_for_test("10.0.0.1:7", 50, 10, 3);
    assert_eq!(
        client.backend_failure_entries_for_test(),
        1,
        "an inserted failure must be visible to the fast path, or the lookup skips a check it          should have made"
    );

    client.insert_backend_failure_for_test("10.0.0.2:7", 50, 10, 1);
    assert_eq!(client.backend_failure_entries_for_test(), 2);

    client.record_backend_success("10.0.0.1:7");
    assert_eq!(
        client.backend_failure_entries_for_test(),
        1,
        "clearing one address must not clear the count for the other"
    );

    client.record_backend_success("10.0.0.2:7");
    assert_eq!(
        client.backend_failure_entries_for_test(),
        0,
        "with the map empty again the lookup may go back to skipping the lock"
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
                            registered_at_ms: 0,
                            preferred_location: String::new(),
                            state: crate::meta::MetaEntityState::Normal,
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
    client.inner.routes.write().unwrap().insert(
        1,
        CachedRoute::for_shard(1, bad_server.clone(), "test_insert"),
    );
    client.insert_backend_failure_for_test(bad_server, 20, 10, 3);

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
                            partition_version: 0,
                            serving_options: crate::meta::TableServingOptions::default(),
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

