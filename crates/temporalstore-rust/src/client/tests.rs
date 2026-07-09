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
    assert!(!migration.legacy_cplusplus_wire_in_scope);
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
        "legacy C++ wire migration shims are out of scope; use Rust-native migration contract"
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
        .any(|blocker| blocker.contains("legacy C++ wire")));
    for family in [
        "common", "string", "hash", "set", "feature", "sequence", "ips", "risk", "redis", "admin",
        "context",
    ] {
        assert!(migration
            .production_replacement_contract
            .supported_command_families
            .contains(&family.to_string()));
    }
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

// shared-corpus: control_client_cpp_partition_set_route_cache
#[test]
fn client_route_cache_preserves_cpp_partition_set_member_version_hierarchy() {
    let meta_addr = free_local_addr();
    let primary_addr = "127.0.0.1:27101".to_string();
    let replica_addr = "127.0.0.1:27102".to_string();
    let meta_addr_for_listener = meta_addr.clone();
    let primary_for_meta = primary_addr.clone();
    let replica_for_meta = replica_addr.clone();
    std::thread::spawn(move || {
        serve(&meta_addr_for_listener, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/tables/topology") => json_response(
                    200,
                    &TableTopologyResponse {
                        status: Status::ok(),
                        table: Some(TableMetaInfo {
                            table_id: 42,
                            namespace: "ns".to_string(),
                            table_name: "cpp_parts".to_string(),
                            state: crate::meta::MetaEntityState::Normal,
                            topology_version: 12,
                            first_shard_id: crate::partition_id::PartitionId::new(42, 0, 0, 17)
                                .unwrap()
                                .id(),
                            shard_count: 2,
                            replica_count: 2,
                            use_cpp_partition_ids: true,
                            partition_version: 17,
                            serving_options: crate::meta::TableServingOptions::default(),
                        }),
                        partitions: vec![
                            TablePartition {
                                shard_id: crate::partition_id::PartitionId::new(42, 0, 0, 17)
                                    .unwrap()
                                    .id(),
                                start_slot: 0,
                                end_slot: 536_870_911,
                                primary: Some(primary_for_meta.clone()),
                                replicas: vec![primary_for_meta.clone(), replica_for_meta.clone()],
                                primary_endpoint: Some(crate::meta::ServerEndpoint {
                                    server_addr: primary_for_meta.clone(),
                                    location: "zone-a".to_string(),
                                }),
                                replica_endpoints: vec![crate::meta::ServerEndpoint {
                                    server_addr: replica_for_meta.clone(),
                                    location: "zone-b".to_string(),
                                }],
                            },
                            TablePartition {
                                shard_id: crate::partition_id::PartitionId::new(42, 1, 0, 17)
                                    .unwrap()
                                    .id(),
                                start_slot: 536_870_912,
                                end_slot: 1_073_741_823,
                                primary: Some(replica_for_meta.clone()),
                                replicas: vec![replica_for_meta.clone()],
                                primary_endpoint: Some(crate::meta::ServerEndpoint {
                                    server_addr: replica_for_meta.clone(),
                                    location: "zone-b".to_string(),
                                }),
                                replica_endpoints: Vec::new(),
                            },
                        ],
                        unchanged: false,
                    },
                ),
                ("POST", "/meta/topology_version") => json_response(
                    200,
                    &crate::meta::TopologyVersionReport {
                        status: Status::ok(),
                        current_topology_version: 12,
                        old_topology_version: 0,
                        unchanged: false,
                        server_count: 0,
                        proxy_count: 0,
                        table_count: 1,
                        shard_route_count: 2,
                        normal_servers: 0,
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
                _ => json_response(404, &Status::error("not_found", "not found")),
            }
        })
        .unwrap();
    });
    wait_for_http(&meta_addr);

    let client = TemporalStoreClient::with_options(ClientOptions {
        proxy_addr: "127.0.0.1:1".to_string(),
        meta_addr: Some(meta_addr),
        route_cache_ttl_ms: 60_000,
        ..ClientOptions::default()
    });
    let options = client.sync_table_topology("ns", "cpp_parts").unwrap();
    assert_eq!(options.table_id, 42);
    assert!(options.use_cpp_partition_ids);
    assert_eq!(options.partition_version, 17);

    let report = client.preflight_report();
    assert_eq!(report.cpp_partition_sets.len(), 1);
    let partition_set = &report.cpp_partition_sets[0];
    assert_eq!(partition_set.table_id, 42);
    assert_eq!(partition_set.combine_name, "ns/cpp_parts");
    assert!(partition_set.use_cpp_partition_ids);
    assert_eq!(partition_set.partition_version, 17);
    assert_eq!(partition_set.topology_version, 12);
    assert_eq!(partition_set.partition_count, 2);
    assert_eq!(partition_set.missing_route_count, 0);
    assert_eq!(partition_set.members[0].start_slot, 0);
    assert_eq!(partition_set.members[0].end_slot, 536_870_911);
    assert_eq!(partition_set.members[1].start_slot, 536_870_912);
    assert_eq!(partition_set.members[1].end_slot, 1_073_741_823);
    assert_eq!(
        partition_set.members[0].primary_addr.as_deref(),
        Some(primary_addr.as_str())
    );
    assert_eq!(
        partition_set.members[0].replica_addrs,
        vec![replica_addr.clone()]
    );

    let topology_report = client.topology_cache_report();
    assert_eq!(topology_report.route_count, 2);
    assert!(topology_report
        .routes
        .iter()
        .all(|route| route.table == "ns/cpp_parts"
            && route.use_cpp_partition_ids
            && route.partition_version == 17));
    assert_eq!(
        topology_report.routes[0].partition_id,
        partition_set.members[0].partition_id
    );

    let parity = client.direct_sdk_parity_report();
    assert!(
        parity.ready,
        "direct SDK parity blockers: {:?}",
        parity.blockers
    );
    assert!(parity.rust_native_migration_contract_ready);
    assert!(parity.typed_table_client_ready);
    assert!(parity.cpp_partition_set_route_cache_ready);
    assert!(parity.partition_member_version_ready);
    assert!(parity.topology_sync_ready);
    assert!(parity.meta_syncer_ready);
    assert!(parity.retry_budget_ready);
    assert!(parity.route_invalidation_ready);
    assert!(parity.placement_hooks_ready);
    assert!(parity.location_affine_secondary_reads_ready);
    assert!(parity.primary_only_writes_ready);
    assert_eq!(parity.cpp_partition_set_count, 1);
    assert_eq!(parity.cpp_partition_member_count, 2);
    assert_eq!(parity.missing_route_count, 0);
    assert_eq!(parity.max_topology_version, 12);
    assert!(parity.meta_sync_generation > 0);
    for family in ["string", "hash", "feature", "redis", "admin", "context"] {
        assert!(parity
            .direct_sdk_command_families
            .contains(&family.to_string()));
    }
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
    client
        .inner
        .routes
        .lock()
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
        CachedRoute::for_shard(1, bad_server.clone(), "test_insert"),
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
                            partitions: vec![TablePartition {
                                shard_id: 40,
                                start_slot: 0,
                                end_slot: 1_073_741_823,
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
        table_key: String::new(),
        partition_id: 1,
        start_slot: 0,
        end_slot: 0,
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
        start_slot: 0,
        end_slot: 0,
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
                            partitions: vec![TablePartition {
                                shard_id: 81,
                                start_slot: 0,
                                end_slot: u64::MAX,
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
                        observed_shard.store(req.shard_id, std::sync::atomic::Ordering::Relaxed);
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
                        let first_shard_id = first_shard.load(std::sync::atomic::Ordering::Relaxed);
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
                                    serving_options: crate::meta::TableServingOptions::default(),
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
                        let first_shard_id = first_shard.load(std::sync::atomic::Ordering::Relaxed);
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
    let other = client.open_table(
        "ns",
        "other",
        TableOptions {
            first_shard_id: 99,
            ..TableOptions::default()
        },
    );
    let mut other_route = CachedRoute::for_shard(99, "127.0.0.1:19999", "test_other_table");
    other_route.table_key = table_combine_name("ns", "other");
    client
        .inner
        .routes
        .lock()
        .expect("client route cache lock poisoned")
        .insert(99, other_route);
    assert_eq!(client.route_cache_size(), 3);
    client.close_table(&table).unwrap();
    assert_eq!(client.route_cache_size(), 1);
    assert!(client
        .cached_table("ns".to_string(), "tbl".to_string())
        .is_none());
    assert!(client
        .cached_table("ns".to_string(), "other".to_string())
        .is_some());
    assert_eq!(other.shard_id_for_key("other-key"), 99);
}

// shared-corpus: control_client_pipeline_batch_partial_timeout_contract
#[test]
fn pipeline_batches_partial_failures_and_timeout_budget_contract() {
    let proxy_addr = free_local_addr();
    let batch_requests = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let batch_requests_for_server = std::sync::Arc::clone(&batch_requests);
    let proxy_addr_for_listener = proxy_addr.clone();
    std::thread::spawn(move || {
        serve(&proxy_addr_for_listener, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/batch_execute") => {
                    batch_requests_for_server.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let req = parse_json::<BatchExecuteRequest>(&request.body).unwrap();
                    assert_eq!(req.commands.len(), 3);
                    json_response(
                        200,
                        &BatchExecuteResponse {
                            status: Status::ok(),
                            responses: vec![
                                ExecuteResponse {
                                    status: Status::ok(),
                                    response: CommandResponse::Empty,
                                },
                                ExecuteResponse {
                                    status: Status::error("partial_failure", "field rejected"),
                                    response: CommandResponse::Empty,
                                },
                                ExecuteResponse {
                                    status: Status::ok(),
                                    response: CommandResponse::Bytes {
                                        value: Some(b"after-partial".to_vec()),
                                    },
                                },
                            ],
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
        proxy_addr,
        max_retries: 0,
        ..ClientOptions::default()
    });
    let table = client.open_table(
        "ns",
        "pipe",
        TableOptions {
            connect_timeout_ms: 200,
            io_timeout_ms: 250,
            max_write_retries: 0,
            retry_backoff_ms: 0,
            ..TableOptions::default()
        },
    );
    let http_options = table.http_options_for_test();
    assert_eq!(http_options.connect_timeout_ms, 200);
    assert_eq!(http_options.io_timeout_ms, 250);
    assert_eq!(http_options.max_retries, 0);

    let mut pipeline = table.pipeline();
    pipeline.set("pipe-key", b"value".to_vec());
    pipeline.hset("pipe-key", "field", b"value".to_vec());
    pipeline.get("pipe-key");
    let response = pipeline.sync().unwrap();
    assert!(response.status.ok);
    assert_eq!(response.responses.len(), 3);
    assert!(response.responses[0].status.ok);
    assert_eq!(response.responses[1].status.code, "partial_failure");
    assert_eq!(
        response.responses[2].response,
        CommandResponse::Bytes {
            value: Some(b"after-partial".to_vec())
        }
    );
    assert_eq!(
        batch_requests.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "unsafe write batches must not be retried without explicit write budget"
    );

    let unsafe_write_retry = classify_cpp_retry_decision(
        &Status::error("retry_later", "possibly applied"),
        true,
        0,
        1,
        false,
    );
    assert!(unsafe_write_retry.retryable);
    assert!(!unsafe_write_retry.would_retry);
    let stale_route_retry = classify_cpp_retry_decision(
        &Status::error("meta_changed", "not applied"),
        true,
        0,
        1,
        false,
    );
    assert!(stale_route_retry.safe_budget_free_write_retry);
    assert!(stale_route_retry.would_retry);
}

#[test]
fn matrixark_batch_append_records_uses_proxy_table_batch_route() {
    let proxy_addr = free_local_addr();
    let seen_path = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let seen_path_for_server = std::sync::Arc::clone(&seen_path);
    let proxy_addr_for_listener = proxy_addr.clone();
    std::thread::spawn(move || {
        serve(&proxy_addr_for_listener, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/ProxyService/BatchExecuteTableCmd") => {
                    *seen_path_for_server.lock().unwrap() = request.path.clone();
                    let req =
                        parse_json::<ProxyTableBatchExecuteClientRequest>(&request.body).unwrap();
                    assert_eq!(req.namespace, "matrixark");
                    assert_eq!(req.table_name, "records");
                    assert_eq!(req.commands.len(), 2);
                    match &req.commands[0] {
                        Command::HashMultiSet { key, entries } => {
                            assert_eq!(key, "session-a");
                            assert_eq!(entries.len(), 2);
                            assert_eq!(entries[0].0, "record-1");
                            assert_eq!(entries[1].0, "record-2");
                        }
                        other => panic!("unexpected first command: {other:?}"),
                    }
                    match &req.commands[1] {
                        Command::HashMultiSet { key, entries } => {
                            assert_eq!(key, "session-b");
                            assert_eq!(entries.len(), 1);
                            assert_eq!(entries[0].0, "record-3");
                        }
                        other => panic!("unexpected second command: {other:?}"),
                    }
                    json_response(
                        200,
                        &BatchExecuteResponse {
                            status: Status::ok(),
                            responses: vec![
                                ExecuteResponse {
                                    status: Status::ok(),
                                    response: CommandResponse::Empty,
                                },
                                ExecuteResponse {
                                    status: Status::ok(),
                                    response: CommandResponse::Empty,
                                },
                            ],
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
        proxy_addr,
        max_retries: 0,
        ..ClientOptions::default()
    });
    let written = client
        .matrixark_batch_append_records(
            "matrixark",
            "records",
            vec![
                MatrixArkRecordAppend {
                    key: "session-a".to_string(),
                    field: "record-1".to_string(),
                    value: b"one".to_vec(),
                },
                MatrixArkRecordAppend {
                    key: "session-a".to_string(),
                    field: "record-2".to_string(),
                    value: b"two".to_vec(),
                },
                MatrixArkRecordAppend {
                    key: "session-b".to_string(),
                    field: "record-3".to_string(),
                    value: b"three".to_vec(),
                },
            ],
        )
        .expect("matrixark append");
    assert_eq!(written, 3);
    assert_eq!(
        seen_path.lock().unwrap().as_str(),
        "/ProxyService/BatchExecuteTableCmd"
    );
}

#[test]
fn matrixark_retrieve_context_pack_helper_uses_record_log_protocol() {
    let client = TemporalStoreClient::with_options(ClientOptions {
        proxy_addr: "127.0.0.1:17000".to_string(),
        ..ClientOptions::default()
    });
    let request_json = client
        .matrixark_retrieve_context_pack_request_json(MatrixArkRetrieveContextPackRequest {
            metaserver: String::new(),
            namespace: "matrixark".to_string(),
            table: "records".to_string(),
            storage_prefix: "tenant-a:session-a".to_string(),
            query: "who owns gpu procurement".to_string(),
            max_selected_refs: 8,
            record: serde_json::json!({
                "query": "who owns gpu procurement",
                "ranking": {"max_selected_refs": 8}
            }),
            scope: Some(serde_json::json!(["tenant-a", "session-a"])),
            secondary_index_groups: vec![vec!["keyword:gpu".to_string()]],
            top_level_response: true,
        })
        .expect("record-log request json");
    let envelope: serde_json::Value = serde_json::from_str(&request_json).unwrap();
    assert_eq!(envelope["op"], "matrixark_retrieve_context_pack");
    assert_eq!(envelope["metaserver"], "127.0.0.1:17000");
    assert_eq!(envelope["storage_prefix"], "tenant-a:session-a");
    assert!(envelope["top_level_response"].as_bool().unwrap());

    let response = serde_json::json!({
        "ok": true,
        "op": "matrixark_retrieve_context_pack",
        "context_pack": {
            "native_context_pack": true,
            "selected_refs": [{"ref_hash": "r1"}]
        }
    });
    let pack = client
        .parse_matrixark_retrieve_context_pack_response(&response.to_string())
        .expect("context pack response");
    assert!(pack["native_context_pack"].as_bool().unwrap());
    assert_eq!(pack["selected_refs"][0]["ref_hash"], "r1");
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
