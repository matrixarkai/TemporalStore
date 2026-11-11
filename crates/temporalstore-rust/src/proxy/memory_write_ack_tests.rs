// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::http::{json_response, parse_json, serve_listener};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
enum Fault {
    First,
    Last,
    Missing,
    AppliedUnknown,
    CatalogRead,
    CatalogCorrupt,
    CatalogIdentity,
    CatalogShape,
}

#[derive(Default)]
struct Observed {
    batches: AtomicUsize,
    ready_writes: AtomicUsize,
}

fn fixture(
    fault: Fault,
    fail_batch: usize,
    ready: bool,
) -> (tempfile::TempDir, ProxyService, String, u64, Arc<Observed>) {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(crate::engine::TemporalEngine::with_local_dirs(
        16 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("index"),
    ));
    let proxy = ProxyService::new(ProxyOptions {
        context_shard_count: 1,
        route_cache_ttl_ms: 600_000,
        topology_check_interval_ms: u64::MAX,
        ..Default::default()
    });
    let scope = MemoryScope {
        tenant_id: "ack".into(),
        account_id: "test".into(),
        ..Default::default()
    };
    let tenant = context::context_tenant_hash(&scope.context_scope(&MemoryIdentity::default()));
    let id = format!("{tenant:016x}-0000000000000001");
    let record = MemoryRecord {
        id: id.clone(),
        context_ref: id.clone(),
        memory: "retained memory".into(),
        metadata: Value::Null,
        tenant_id: "ack".into(),
        account_id: "test".into(),
        user_id: "alice".into(),
        agent_id: String::new(),
        run_id: String::new(),
        created_at_ms: 1,
        updated_at_ms: 1,
    };
    let shard_id = proxy.context_shard_id(tenant);
    engine.load_shard(shard_id);
    let mut commands = vec![
        memory_catalog_write(tenant, std::iter::once(&record)),
        memory_node_lookup_write(tenant, std::iter::once(&record)),
        Command::SetAdd {
            key: memory_order_key(tenant),
            member: memory_order_member(1, &id),
        },
        Command::ZSetAdd {
            key: memory_identity_key(tenant),
            member: MEMORY_TOTAL_MEMBER.to_vec(),
            score: 1.0,
        },
    ];
    if ready {
        commands.extend([
            Command::StringSet {
                key: memory_order_ready_key(tenant),
                value: b"1".to_vec(),
            },
            Command::StringSet {
                key: memory_identity_ready_key(tenant),
                value: MEMORY_IDENTITY_INDEX_VERSION.to_vec(),
            },
        ]);
        proxy.memory_remember_indexes_ready(tenant);
    }
    assert!(
        engine
            .batch_execute(BatchExecuteRequest { shard_id, commands })
            .status
            .ok
    );
    let observed = Arc::new(Observed::default());
    let seen = Arc::clone(&observed);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        serve_listener(listener,move |request| match request.path.as_str() {
            "/batch_execute" => {
                let batch:BatchExecuteRequest = parse_json(&request.body).unwrap();
                let read = batch.commands.iter().all(|command| matches!(command,
                    Command::HashMultiGet {..}|Command::HashGetAll {..}|Command::ZSetScore {..}));
                if read {
                    let mut result = engine.batch_execute(batch);
                    if matches!(fault,Fault::CatalogRead) {
                        result.responses[0].status=Status::error("catalog_read_failed","injected catalog read failure");
                    }
                    if matches!(fault,Fault::CatalogCorrupt) {
                        result.responses[0].response=CommandResponse::HashEntries {
                            entries:vec![(record.id.clone(),vec![255])],
                        };
                    }
                    if matches!(fault,Fault::CatalogIdentity) {
                        result.responses[0].response=CommandResponse::HashEntries {
                            entries:vec![("different-id".into(),record.encode())],
                        };
                    }
                    if matches!(fault,Fault::CatalogShape) {
                        result.responses[0].response=CommandResponse::Empty;
                    }
                    return json_response(200,&result);
                }
                let ordinal = seen.batches.fetch_add(1,Ordering::Relaxed)+1;
                for command in &batch.commands {
                    if matches!(command,Command::StringSet {key,..} if key==&memory_order_ready_key(tenant)||key==&memory_identity_ready_key(tenant)) {
                        seen.ready_writes.fetch_add(1,Ordering::Relaxed);
                    }
                }
                if ordinal!=fail_batch { return json_response(200,&engine.batch_execute(batch)); }
                let mut result = if matches!(fault,Fault::AppliedUnknown) { engine.batch_execute(batch) }
                    else { BatchExecuteResponse {status:Status::ok(), responses:batch.commands.iter().map(|_| ExecuteResponse {
                        status:Status::ok(),response:CommandResponse::Empty,
                    }).collect()} };
                if matches!(fault,Fault::Missing) { result.responses.pop(); }
                else {
                    let index = if matches!(fault,Fault::Last) { result.responses.len()-1 } else { 0 };
                    result.responses[index].status=Status::error(
                        if matches!(fault,Fault::AppliedUnknown) {"write_outcome_unknown"} else {"mutation_child_failed"},
                        "injected mutation acknowledgment failure",
                    );
                }
                json_response(200,&result)
            }
            "/execute" => {
                let body:ExecuteRequest = parse_json(&request.body).unwrap();
                if matches!(&body.command,Command::StringSet {key,..} if key==&memory_order_ready_key(tenant)||key==&memory_identity_ready_key(tenant)) {
                    seen.ready_writes.fetch_add(1,Ordering::Relaxed);
                }
                json_response(200,&engine.execute(body))
            }
            "/context/ingest_explicit" => {
                let decoded=crate::context_workflow::ingest_wire::decode_request(&request.body).unwrap();
                (200,crate::context_workflow::ingest_wire::encode_response(Status::ok(),decoded.sources.len(),0))
            }
            _ => panic!("unexpected path {}",request.path),
        }).unwrap();
    });
    proxy.client().insert_cached_route_for_test(shard_id, addr);
    (dir, proxy, id, tenant, observed)
}

fn call(proxy: &ProxyService, method: &str, path: String, body: Value) -> (u16, Value) {
    let (status, body) = proxy
        .handle_memory_route(&HttpRequest {
            method: method.into(),
            path,
            body: serde_json::to_vec(&body).unwrap(),
        })
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

#[test]
fn mutation_routes_reject_failed_or_missing_child_acknowledgments() {
    for fault in [Fault::First, Fault::Last, Fault::Missing] {
        for operation in ["add", "update", "batch", "delete", "delete_all", "reset"] {
            let (_dir, proxy, id, _tenant, _seen) = fixture(fault, 1, true);
            let (status, body) = match operation {
                "add" => call(
                    &proxy,
                    "POST",
                    "/memories".into(),
                    json!({"infer":false,"messages":"explicit memory","scope":{"tenant_id":"ack","account_id":"test"}}),
                ),
                "update" => call(
                    &proxy,
                    "PUT",
                    format!("/memories/{id}"),
                    json!({"metadata":{"tag":"new"}}),
                ),
                "batch" => call(
                    &proxy,
                    "POST",
                    "/memories/batch/update".into(),
                    json!({"memories":[{"memory_id":id,"metadata":{}}]}),
                ),
                "delete" => call(&proxy, "DELETE", format!("/memories/{id}"), json!({})),
                _ => call(
                    &proxy,
                    "POST",
                    format!("/memories/{operation}"),
                    json!({"scope":{"tenant_id":"ack","account_id":"test"}}),
                ),
            };
            if operation == "batch" {
                assert_eq!(status, 200, "{operation} {fault:?} {body}");
                assert_eq!(body["updated"], 0);
                assert_eq!(body["failed"][0]["status"], 502);
            } else {
                assert_eq!(status, 502, "{operation} {fault:?} {body}");
            }
        }
    }
}

#[test]
fn unknown_outcome_is_not_reported_as_success_or_replayed() {
    for batch in [false, true] {
        let (_dir, proxy, id, tenant, seen) = fixture(Fault::AppliedUnknown, 1, true);
        let (status, body) = if batch {
            call(
                &proxy,
                "POST",
                "/memories/batch/update".into(),
                json!({"memories":[{"memory_id":id,"metadata":{"applied":true}}]}),
            )
        } else {
            call(
                &proxy,
                "PUT",
                format!("/memories/{id}"),
                json!({"metadata":{"applied":true}}),
            )
        };
        if batch {
            assert_eq!(body["updated"], 0);
            assert_eq!(body["failed"][0]["code"], "write_outcome_unknown");
        } else {
            assert_eq!(status, 502);
            assert_eq!(body["code"], "write_outcome_unknown");
        }
        assert_eq!(seen.batches.load(Ordering::Relaxed), 1);
        assert_eq!(
            proxy
                .memory_read_one_checked(tenant, &id)
                .unwrap()
                .unwrap()
                .metadata["applied"],
            true
        );
    }
}

#[test]
fn failed_backfill_or_catalog_scan_never_publishes_readiness() {
    for fault in [
        Fault::First,
        Fault::Last,
        Fault::CatalogRead,
        Fault::CatalogCorrupt,
        Fault::CatalogIdentity,
        Fault::CatalogShape,
    ] {
        let (_dir, proxy, _id, tenant, seen) = fixture(fault, 1, false);
        let error = proxy.memory_prepare_indexes(tenant).unwrap_err();
        if matches!(
            fault,
            Fault::CatalogRead
                | Fault::CatalogCorrupt
                | Fault::CatalogIdentity
                | Fault::CatalogShape
        ) {
            assert_eq!(seen.batches.load(Ordering::Relaxed), 0);
            assert!(
                error.code.starts_with("catalog_read_")
                    || error.code.starts_with("invalid_memory_catalog_")
            );
        }
        assert_eq!(seen.ready_writes.load(Ordering::Relaxed), 0);
        assert!(!proxy
            .inner
            .memory_indexes_ready
            .read()
            .unwrap()
            .contains(&tenant));
    }
}

#[test]
fn failed_reset_cleanup_does_not_republish_ready_markers() {
    let (_dir, proxy, _id, tenant, seen) = fixture(Fault::Last, 2, true);
    let (status, body) = call(
        &proxy,
        "POST",
        "/memories/reset".into(),
        json!({"scope":{"tenant_id":"ack","account_id":"test"}}),
    );
    assert_eq!(status, 502, "{body}");
    assert_eq!(seen.ready_writes.load(Ordering::Relaxed), 0);
    assert!(!proxy
        .inner
        .memory_indexes_ready
        .read()
        .unwrap()
        .contains(&tenant));
}

#[test]
fn acknowledgment_checker_requires_exact_counts_and_all_successes() {
    let success = ExecuteResponse {
        status: Status::ok(),
        response: CommandResponse::Empty,
    };
    for responses in [vec![], vec![success.clone(), success.clone()]] {
        assert!(memory_write_ack(
            &BatchExecuteResponse {
                status: Status::ok(),
                responses
            },
            1
        )
        .is_err());
    }
    assert!(memory_write_ack(
        &BatchExecuteResponse {
            status: Status::ok(),
            responses: vec![success]
        },
        1
    )
    .is_ok());
}
