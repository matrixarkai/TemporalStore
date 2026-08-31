// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

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
fn failure_driven_topology_syncs_are_spaced_out() {
    // A request that fails in a way suggesting the cached topology is wrong triggers a
    // re-sync. The guard on that is per REQUEST, which says nothing about the other requests
    // in flight -- and a shard moving fails many at once. Unthrottled, each one is its own
    // metaserver round-trip: a sync storm aimed at the metaserver exactly while it is working
    // through the topology change that caused the failures. One failure is enough to learn
    // what all of them need.
    let client = TemporalStoreClient::with_options(ClientOptions {
        proxy_addr: "127.0.0.1:1".to_string(),
        meta_addr: Some("127.0.0.1:1".to_string()),
        topo_error_retry_interval_ms: 5_000,
        ..ClientOptions::default()
    });

    assert!(
        client.forced_sync_is_due("ns", "tbl"),
        "the first failure should learn the topology"
    );
    for attempt in 0..8 {
        assert!(
            !client.forced_sync_is_due("ns", "tbl"),
            "concurrent failure {attempt} must not each fire their own sync"
        );
    }

    // Per table, not global: another table's failure is its own question.
    assert!(
        client.forced_sync_is_due("ns", "other"),
        "a different table must not be throttled by this one"
    );

    // Zero disables the spacing, which is the older behaviour, kept reachable.
    let eager = TemporalStoreClient::with_options(ClientOptions {
        proxy_addr: "127.0.0.1:1".to_string(),
        meta_addr: Some("127.0.0.1:1".to_string()),
        topo_error_retry_interval_ms: 0,
        ..ClientOptions::default()
    });
    for attempt in 0..4 {
        assert!(
            eager.forced_sync_is_due("ns", "tbl"),
            "with spacing off, attempt {attempt} should still sync"
        );
    }
}

#[test]
fn a_second_sync_asks_only_for_what_changed_and_keeps_its_routes() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    // The request has always carried old_topology_version and the metaserver has always
    // answered "unchanged" when it is current -- with the shard list omitted, because there is
    // no point rebuilding what the caller already has. The client hardcoded 0, so that answer
    // could never be given: every sync of every table rebuilt and shipped the whole shard list.
    //
    // It could not simply start sending the version either. An unchanged reply carries no
    // shards, and the route surgery would have read that as "this table has no shards" and
    // deleted them all. Both halves are needed, in that order.
    let seen_versions = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let builds = std::sync::Arc::new(AtomicUsize::new(0));
    let seen_version_calls = std::sync::Arc::new(AtomicUsize::new(0));
    let meta_addr = free_local_addr();
    let meta_addr_for_listener = meta_addr.clone();
    let versions = seen_versions.clone();
    let built = builds.clone();
    let version_calls = seen_version_calls.clone();
    std::thread::spawn(move || {
        serve(&meta_addr_for_listener, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/tables/topology") => {
                    let req = parse_json::<crate::meta::GetTableTopologyRequest>(&request.body)
                        .unwrap();
                    versions.lock().unwrap().push(req.old_topology_version);
                    let table = crate::meta::TableMetaInfo {
                        table_id: 1,
                        namespace: "ns".to_string(),
                        table_name: "tbl".to_string(),
                        state: crate::meta::MetaEntityState::Normal,
                        topology_version: 7,
                        first_shard_id: 1,
                        shard_count: 1,
                        replica_count: 1,
                        partition_version: 0,
                        serving_options: crate::meta::TableServingOptions::default(),
                    };
                    // Exactly what the metaserver does: current version in, no shards back.
                    if req.old_topology_version >= table.topology_version {
                        return json_response(
                            200,
                            &TableTopologyResponse {
                                status: Status::ok(),
                                table: Some(table),
                                shards: Vec::new(),
                                unchanged: true,
                            },
                        );
                    }
                    built.fetch_add(1, Ordering::SeqCst);
                    json_response(
                        200,
                        &TableTopologyResponse {
                            status: Status::ok(),
                            table: Some(table),
                            shards: vec![TableShard {
                                shard_id: 1,
                                start_bucket: 0,
                                end_bucket: u64::MAX,
                                primary: Some("127.0.0.1:29101".to_string()),
                                replicas: Vec::new(),
                                primary_endpoint: None,
                                replica_endpoints: Vec::new(),
                            }],
                            unchanged: false,
                        },
                    )
                }
                // Only the CALL is interesting here. What comes back does not matter: an
                // unparsed answer just falls back to the table's own version, and what this
                // test measures is whether the call is made at all.
                ("POST", "/meta/topology_version") => {
                    version_calls.fetch_add(1, Ordering::SeqCst);
                    json_response(503, &Status::error("unavailable", "not part of this test"))
                }
                _ => json_response(404, &Status::error("not_found", "no route")),
            }
        })
        .unwrap();
    });
    wait_for_http(&meta_addr);

    let client = TemporalStoreClient::with_options(ClientOptions {
        proxy_addr: meta_addr.clone(),
        meta_addr: Some(meta_addr.clone()),
        ..ClientOptions::default()
    });

    client
        .sync_table_topology("ns".to_string(), "tbl".to_string())
        .expect("first sync succeeds");
    assert_eq!(client.topology_cache_report().route_count, 1);
    let version_calls_after_first = seen_version_calls.load(Ordering::SeqCst);

    client
        .sync_table_topology("ns".to_string(), "tbl".to_string())
        .expect("second sync succeeds");

    let versions = seen_versions.lock().unwrap().clone();
    assert_eq!(
        versions.first().copied(),
        Some(0),
        "the first sync has nothing to declare"
    );
    assert_eq!(
        versions.get(1).copied(),
        Some(7),
        "the second sync must declare what it already has, or the metaserver cannot skip work"
    );
    assert_eq!(
        builds.load(Ordering::SeqCst),
        1,
        "the shard list should be built once, not once per sync"
    );
    assert_eq!(
        client.topology_cache_report().route_count,
        1,
        "an unchanged reply carries no shards and must not be read as having none"
    );
    assert_eq!(
        seen_version_calls.load(Ordering::SeqCst),
        version_calls_after_first,
        "a sync that changed nothing should cost one round-trip, not two -- the \
         cluster topology version is only needed to stamp routes, and an \
         unchanged reply installs none"
    );
}

#[test]
fn a_topology_missing_a_primary_keeps_the_route_it_cannot_replace() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    // A topology snapshot taken while a primary is being elected names the shard but gives it
    // no primary. That entry used to be dropped, AND the purge that precedes the insert
    // removed the shard's previous route -- so a working route was destroyed on the strength
    // of an incomplete snapshot, and the sync reported success. The route is now kept, and
    // the sync says how many shards it could not route.
    let round = std::sync::Arc::new(AtomicUsize::new(0));
    let meta_addr = free_local_addr();
    let meta_addr_for_listener = meta_addr.clone();
    let r = round.clone();
    std::thread::spawn(move || {
        serve(&meta_addr_for_listener, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/tables/topology") => {
                    // First sync: a healthy topology. Second: the same shard, no primary.
                    let healthy = r.fetch_add(1, Ordering::SeqCst) == 0;
                    json_response(
                        200,
                        &TableTopologyResponse {
                            status: Status::ok(),
                            table: Some(crate::meta::TableMetaInfo {
                                table_id: 1,
                                namespace: "ns".to_string(),
                                table_name: "tbl".to_string(),
                                state: crate::meta::MetaEntityState::Normal,
                                topology_version: if healthy { 7 } else { 8 },
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
                                primary: if healthy {
                                    Some("127.0.0.1:29001".to_string())
                                } else {
                                    None
                                },
                                replicas: Vec::new(),
                                primary_endpoint: None,
                                replica_endpoints: Vec::new(),
                            }],
                            unchanged: false,
                        },
                    )
                }
                _ => json_response(404, &Status::error("not_found", "no route")),
            }
        })
        .unwrap();
    });
    wait_for_http(&meta_addr);

    let client = TemporalStoreClient::with_options(ClientOptions {
        proxy_addr: meta_addr.clone(),
        meta_addr: Some(meta_addr.clone()),
        ..ClientOptions::default()
    });

    client
        .sync_table_topology("ns".to_string(), "tbl".to_string())
        .expect("first sync succeeds");
    assert_eq!(
        client.topology_cache_report().route_count,
        1,
        "the healthy topology should install a route"
    );

    client
        .sync_table_topology("ns".to_string(), "tbl".to_string())
        .expect("second sync still succeeds");
    assert_eq!(
        client.topology_cache_report().route_count,
        1,
        "a topology with no primary must not delete the route it cannot replace"
    );

    let synced = client.meta_sync_report();
    let table = synced
        .tables
        .iter()
        .find(|table| table.table_name == "tbl")
        .expect("the table is reported");
    assert_eq!(
        table.shards_without_primary, 1,
        "an incomplete sync must say so rather than reporting a clean success"
    );
}

#[test]
fn a_tables_timeouts_reach_the_wire_after_they_change() {
    // The table handle keeps a copy of the options it was opened with, and the client
    // keeps the live ones, refreshed on every sync. Routing already reads the live
    // copy; the request timeouts did not. So a table whose timeouts changed reported
    // the new ones through `options()` while still putting the old ones on the wire,
    // for the whole life of the handle.
    use std::sync::atomic::{AtomicU64, Ordering};
    let io_ms = std::sync::Arc::new(AtomicU64::new(1111));
    let connect_ms = std::sync::Arc::new(AtomicU64::new(2222));
    let meta_addr = free_local_addr();
    let meta_addr_for_listener = meta_addr.clone();
    let io_for_server = io_ms.clone();
    let connect_for_server = connect_ms.clone();
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
                            shard_count: 1,
                            replica_count: 1,
                            partition_version: 0,
                            serving_options: crate::meta::TableServingOptions {
                                io_timeout_ms: io_for_server.load(Ordering::Relaxed),
                                connect_timeout_ms: connect_for_server.load(Ordering::Relaxed),
                                ..crate::meta::TableServingOptions::default()
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
        meta_addr: Some(meta_addr),
        ..ClientOptions::default()
    });
    let table = client.open_table_from_meta("ns", "tbl").unwrap();
    assert_eq!(table.http_options_for_test().io_timeout_ms, 1111);
    assert_eq!(table.http_options_for_test().connect_timeout_ms, 2222);

    io_ms.store(3333, Ordering::Relaxed);
    connect_ms.store(4444, Ordering::Relaxed);
    client.sync_table_topology("ns", "tbl").unwrap();

    assert_eq!(
        table.options().io_timeout_ms,
        3333,
        "the handle reports the refreshed timeout"
    );
    assert_eq!(
        table.http_options_for_test().io_timeout_ms,
        3333,
        "and must actually send it: reporting one timeout while using another is the bug"
    );
    assert_eq!(table.http_options_for_test().connect_timeout_ms, 4444);
}

#[test]
fn a_batch_is_split_across_shards_the_table_gained_after_it_was_opened() {
    // Whether a batch is grouped by shard was decided from the shard count the handle
    // was opened with. A table that gains shards afterwards kept sending every command
    // to the one shard the handle started on -- while `shard_id_for_key`, on the same
    // handle, already knew the right shard for each key. The handle worked out the
    // correct answer and then declined to use it.
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;
    let shard_count = std::sync::Arc::new(AtomicU64::new(1));
    let seen: std::sync::Arc<Mutex<Vec<ShardId>>> = std::sync::Arc::new(Mutex::new(Vec::new()));

    let backend_addr = free_local_addr();
    let backend_for_listener = backend_addr.clone();
    let seen_for_server = seen.clone();
    std::thread::spawn(move || {
        serve(&backend_for_listener, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/batch_execute") => {
                    let req = parse_json::<BatchExecuteRequest>(&request.body).unwrap();
                    seen_for_server.lock().unwrap().push(req.shard_id);
                    json_response(
                        200,
                        &BatchExecuteResponse {
                            status: Status::ok(),
                            responses: req
                                .commands
                                .iter()
                                .map(|_| ExecuteResponse {
                                    status: Status::ok(),
                                    response: CommandResponse::Empty,
                                })
                                .collect(),
                        },
                    )
                }
                _ => json_response(404, &Status::error("not_found", "not found")),
            }
        })
        .unwrap();
    });
    wait_for_http(&backend_addr);

    let meta_addr = free_local_addr();
    let meta_addr_for_listener = meta_addr.clone();
    let count_for_server = shard_count.clone();
    let backend_for_meta = backend_addr.clone();
    std::thread::spawn(move || {
        serve(&meta_addr_for_listener, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/tables/topology") => {
                    let count = count_for_server.load(Ordering::Relaxed);
                    let shards = (0..count)
                        .map(|offset| crate::meta::TableShard {
                            shard_id: 10 + offset,
                            start_bucket: offset * 4096,
                            end_bucket: (offset + 1) * 4096 - 1,
                            primary: Some(backend_for_meta.clone()),
                            replicas: Vec::new(),
                            primary_endpoint: None,
                            replica_endpoints: Vec::new(),
                        })
                        .collect();
                    json_response(
                        200,
                        &TableTopologyResponse {
                            status: Status::ok(),
                            table: Some(crate::meta::TableMetaInfo {
                                table_id: 1,
                                namespace: "ns".to_string(),
                                table_name: "tbl".to_string(),
                                state: crate::meta::MetaEntityState::Normal,
                                topology_version: 7 + count,
                                first_shard_id: 10,
                                shard_count: count,
                                replica_count: 1,
                                partition_version: 0,
                                serving_options: crate::meta::TableServingOptions::default(),
                            }),
                            shards,
                            unchanged: false,
                        },
                    )
                }
                ("GET", path) if path.starts_with("/shards/") => {
                    let shard_id: ShardId = path.trim_start_matches("/shards/").parse().unwrap();
                    json_response(
                        200,
                        &GetShardResponse {
                            status: Status::ok(),
                            location: Some(ShardLocation {
                                state: crate::meta::MetaEntityState::Normal,
                                shard_id,
                                server_addr: backend_for_meta.clone(),
                                latest_snapshot: None,
                                // This stub pins nothing, which is what every
                                // shard means until somebody says otherwise.
                                preferred_location: String::new(),
                            }),
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
        ..ClientOptions::default()
    });
    let table = client.open_table_from_meta("ns", "tbl").unwrap();
    assert_eq!(table.options().shard_count, 1);

    shard_count.store(4, Ordering::Relaxed);
    client.sync_table_topology("ns", "tbl").unwrap();
    assert_eq!(table.options().shard_count, 4);

    let keys: Vec<String> = (0..40).map(|n| format!("key-{n}")).collect();
    let want: std::collections::BTreeSet<ShardId> =
        keys.iter().map(|key| table.shard_id_for_key(key)).collect();
    assert!(
        want.len() > 1,
        "the test needs keys that land on different shards; got {want:?}"
    );

    seen.lock().unwrap().clear();
    table
        .batch_execute(
            keys.iter()
                .map(|key| Command::StringGet { key: key.clone() })
                .collect(),
        )
        .unwrap();

    let got: std::collections::BTreeSet<ShardId> = seen.lock().unwrap().iter().copied().collect();
    assert_eq!(
        got, want,
        "each command must go to the shard its key routes to, not to the shard the handle was opened on"
    );
}

#[test]
fn a_table_that_asks_for_a_default_value_still_decides_it() {
    // A table set to shed nothing and to retry no writes, opened by a client that
    // sheds half its traffic and retries writes twice.
    //
    // Both of the table's values equal the field's default. That used to be read as
    // "the table said nothing", so the client's settings won: the table was shed at
    // 50% and its writes were retried, which is the reverse of what it asked for.
    // `drop_percent: 0` is the only way to say "never shed this table" and
    // `max_write_retries: 0` the only way to say "never retry a write here", so the
    // two settings whose whole purpose is to hold something back were the two that
    // could not be expressed.
    //
    // max_read_retries is left unset here as the control: it must still come from
    // the client, or the fix would just be "the table always wins".
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
                            partition_version: 0,
                            serving_options: crate::meta::TableServingOptions {
                                drop_percent: 0,
                                max_write_retries: 0,
                                set_fields: ["drop_percent", "max_write_retries"]
                                    .into_iter()
                                    .map(str::to_string)
                                    .collect(),
                                ..crate::meta::TableServingOptions::default()
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
        drop_percent: 50,
        max_write_retries: 2,
        max_read_retries: 9,
        ..ClientOptions::default()
    });
    let table = client.open_table_from_meta("ns", "tbl").unwrap();
    let options = table.options();
    assert_eq!(
        options.drop_percent, 0,
        "the table asked to shed nothing; the client's 50% must not override it"
    );
    assert_eq!(
        options.max_write_retries, 0,
        "the table asked for no write retries; the client's 2 must not override it"
    );
    assert_eq!(
        options.max_read_retries, 9,
        "a field the table never set must still come from the client"
    );
}

#[test]
fn a_table_record_written_before_set_fields_still_inherits_from_the_client() {
    // The compatibility half. Records persisted before tables recorded which fields
    // they set carry an empty set, and nothing can recover what was meant from the
    // values alone. Those must keep behaving exactly as they did: a value equal to
    // the default is read as unset and the client's own option is used.
    let options = crate::meta::TableServingOptions::default();
    assert!(options.set_fields.is_empty());
    for field in [
        crate::meta::TableServingField::DropPercent,
        crate::meta::TableServingField::MaxWriteRetries,
        crate::meta::TableServingField::MaxReadRetries,
        crate::meta::TableServingField::IoTimeoutMs,
        crate::meta::TableServingField::ConnectTimeoutMs,
        crate::meta::TableServingField::RetryBackoffMs,
        crate::meta::TableServingField::ContinuousFailedTimeMs,
    ] {
        assert!(
            !options.table_decides(field),
            "{} carries no record of being set, so the client must still decide it",
            field.name()
        );
    }
    // And a differing value is still read as set, which is all an older record ever
    // had to go on.
    let changed = crate::meta::TableServingOptions {
        drop_percent: 23,
        ..crate::meta::TableServingOptions::default()
    };
    assert!(changed.table_decides(crate::meta::TableServingField::DropPercent));
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
                                set_fields: Default::default(),
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
fn client_router_matches_crc64_bucket_formula() {
    assert_eq!(crc64_jones(b"123456789"), 0xe9c6d914c4b8d9ca);
    assert_eq!(bucket_id_for_key("123456789"), 0x3a71_b645);
    assert_eq!(
        shard_id_for_key("123456789", 10, 4, 1),
        10 + (0x3a71_b645 % 4)
    );
    assert_eq!(stable_key_hash("123456789"), crc64_jones(b"123456789"));
}

#[test]
fn client_router_round_robins_secondary_reads_like_router() {
    let mut route = CachedRoute {
        table_key: String::new(),
        partition_id: 1,
        start_bucket: 0,
        end_bucket: 0,
        partition_version: 0,
        primary_addr: "primary".to_string(),
        replica_addrs: vec!["replica-a".to_string(), "replica-b".to_string()],
        replica_endpoints: Vec::new(),
        next_replica_index: std::sync::atomic::AtomicUsize::new(0),
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
        next_replica_index: std::sync::atomic::AtomicUsize::new(0),
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
                                    set_fields: Default::default(),
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

