// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::http::{json_response, parse_json, serve_listener};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
enum Fault {
    TopLevel,
    Child,
    TombstoneChild,
    Incomplete,
    WrongType,
    ShortValues,
    Corrupt,
    WrongIdentity,
    Missing,
    Tombstone,
    HistoryError,
    HistoryShape,
    HistoryCorrupt,
}

fn fixture(fault: Fault) -> (ProxyService, String, Arc<AtomicUsize>) {
    let tenant = 42;
    let id = format!("{tenant:016x}-0000000000000001");
    let record = MemoryRecord {
        id: if matches!(fault, Fault::WrongIdentity) {
            "different-id".into()
        } else {
            id.clone()
        },
        context_ref: id.clone(),
        memory: "preserve this memory".into(),
        metadata: Value::Null,
        tenant_id: "tenant".into(),
        account_id: "account".into(),
        user_id: "user".into(),
        agent_id: String::new(),
        run_id: String::new(),
        created_at_ms: 1,
        updated_at_ms: 1,
    };
    let proxy = ProxyService::new(ProxyOptions {
        context_shard_count: 1,
        route_cache_ttl_ms: 600_000,
        topology_check_interval_ms: u64::MAX,
        ..Default::default()
    });
    let shard_id = proxy.context_shard_id(tenant);
    let writes = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&writes);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        serve_listener(listener, move |request| match request.path.as_str() {
            "/batch_execute" => {
                let batch: BatchExecuteRequest = parse_json(&request.body).unwrap();
                if !batch
                    .commands
                    .iter()
                    .all(|command| matches!(command, Command::HashMultiGet { .. }))
                {
                    observed.fetch_add(1, Ordering::Relaxed);
                    return json_response(
                        200,
                        &BatchExecuteResponse {
                            status: Status::error(
                                "unexpected_write",
                                "read failure must prevent writes",
                            ),
                            responses: Vec::new(),
                        },
                    );
                }
                let mut responses = batch
                    .commands
                    .into_iter()
                    .enumerate()
                    .map(|(index, command)| {
                        let Command::HashMultiGet { fields, .. } = command else {
                            unreachable!()
                        };
                        let mut values = fields
                            .iter()
                            .map(|_| {
                                if index == 1 {
                                    matches!(fault, Fault::Tombstone).then(|| b"1".to_vec())
                                } else if matches!(fault, Fault::Missing) {
                                    None
                                } else if matches!(fault, Fault::Corrupt) {
                                    Some(vec![255])
                                } else {
                                    Some(record.encode())
                                }
                            })
                            .collect::<Vec<_>>();
                        if index == 0 && matches!(fault, Fault::ShortValues) {
                            values.clear();
                        }
                        ExecuteResponse {
                            status: if (index == 0 && matches!(fault, Fault::Child))
                                || (index == 1 && matches!(fault, Fault::TombstoneChild))
                            {
                                Status::error(
                                    "catalog_read_unavailable",
                                    "injected child read error",
                                )
                            } else {
                                Status::ok()
                            },
                            response: if index == 0 && matches!(fault, Fault::WrongType) {
                                CommandResponse::Empty
                            } else {
                                CommandResponse::Values { values }
                            },
                        }
                    })
                    .collect::<Vec<_>>();
                if matches!(fault, Fault::Incomplete) {
                    responses.pop();
                }
                json_response(
                    200,
                    &BatchExecuteResponse {
                        status: if matches!(fault, Fault::TopLevel) {
                            Status::error("catalog_read_unavailable", "injected batch read error")
                        } else {
                            Status::ok()
                        },
                        responses,
                    },
                )
            }
            "/execute" => {
                let request: ExecuteRequest = parse_json(&request.body).unwrap();
                if !matches!(request.command, Command::HashGetAll { .. }) {
                    observed.fetch_add(1, Ordering::Relaxed);
                }
                json_response(
                    200,
                    &ExecuteResponse {
                        status: if matches!(fault, Fault::HistoryError) {
                            Status::error("history_read_unavailable", "injected history failure")
                        } else {
                            Status::ok()
                        },
                        response: if matches!(fault, Fault::HistoryShape) {
                            CommandResponse::Empty
                        } else {
                            CommandResponse::HashEntries {
                                entries: if matches!(fault, Fault::HistoryCorrupt) {
                                    vec![("history-entry".into(), vec![255])]
                                } else {
                                    Vec::new()
                                },
                            }
                        },
                    },
                )
            }
            _ => {
                observed.fetch_add(1, Ordering::Relaxed);
                json_response(
                    500,
                    &Status::error("unexpected_write", "no model or mutation expected"),
                )
            }
        })
        .unwrap();
    });
    proxy.client().insert_cached_route_for_test(shard_id, addr);
    proxy.memory_remember_indexes_ready(tenant);
    (proxy, id, writes)
}

fn call(proxy: &ProxyService, method: &str, path: String, body: Value) -> (u16, Value) {
    let request = HttpRequest {
        method: method.into(),
        path,
        body: serde_json::to_vec(&body).unwrap(),
    };
    let (status, body) = proxy.handle_memory_route(&request).expect("memory route");
    (status, serde_json::from_slice(&body).unwrap())
}

#[test]
fn addressable_operations_preserve_read_errors_without_mutating() {
    for fault in [
        Fault::TopLevel,
        Fault::Child,
        Fault::TombstoneChild,
        Fault::Incomplete,
        Fault::WrongType,
        Fault::ShortValues,
        Fault::Corrupt,
        Fault::WrongIdentity,
    ] {
        let (proxy, id, writes) = fixture(fault);
        for (method, path, body) in [
            ("GET", format!("/memories/{id}"), json!({})),
            (
                "PUT",
                format!("/memories/{id}"),
                json!({"metadata":{"tag":"new"}}),
            ),
            ("DELETE", format!("/memories/{id}"), json!({})),
            (
                "POST",
                format!("/memories/{id}/feedback"),
                json!({"feedback":"POSITIVE"}),
            ),
        ] {
            let (status, body) = call(&proxy, method, path, body);
            assert_eq!(status, 502, "{fault:?}: {body}");
            let code = if matches!(
                fault,
                Fault::TopLevel | Fault::Child | Fault::TombstoneChild
            ) {
                "catalog_read_unavailable"
            } else if matches!(fault, Fault::Incomplete) {
                "bad_response"
            } else {
                "invalid_memory_node_lookup"
            };
            assert_eq!(body["code"], code, "{fault:?}: {body}");
        }
        let (status, body) = call(
            &proxy,
            "POST",
            "/memories/batch/update".into(),
            json!({
                "memories":[{"memory_id":id,"metadata":{"tag":"new"}}]
            }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["updated"], 0);
        assert_eq!(body["failed"][0]["status"], 502);
        assert!(body["failed"][0]["code"].as_str().is_some());
        assert_eq!(writes.load(Ordering::Relaxed), 0);
    }
}

#[test]
fn genuine_absence_and_tombstones_remain_not_found() {
    for fault in [Fault::Missing, Fault::Tombstone] {
        let (proxy, id, writes) = fixture(fault);
        for (method, body) in [
            ("GET", json!({})),
            ("PUT", json!({"metadata":{}})),
            ("DELETE", json!({})),
        ] {
            assert_eq!(call(&proxy, method, format!("/memories/{id}"), body).0, 404);
        }
        let (status, body) = call(
            &proxy,
            "POST",
            "/memories/batch/update".into(),
            json!({
                "memories":[{"memory_id":id,"metadata":{}}]
            }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["failed"][0]["status"], 404);
        assert_eq!(writes.load(Ordering::Relaxed), 0);
    }
}

#[test]
fn history_errors_and_invalid_envelopes_do_not_become_empty_history() {
    for (fault, code) in [
        (Fault::HistoryError, "history_read_unavailable"),
        (Fault::HistoryShape, "invalid_memory_history_response"),
        (Fault::HistoryCorrupt, "invalid_memory_history_record"),
    ] {
        let (proxy, id, writes) = fixture(fault);
        let (status, body) = call(&proxy, "GET", format!("/memories/{id}/history"), json!({}));
        assert_eq!(status, 502);
        assert_eq!(body["code"], code);
        assert_eq!(writes.load(Ordering::Relaxed), 0);
    }
}
