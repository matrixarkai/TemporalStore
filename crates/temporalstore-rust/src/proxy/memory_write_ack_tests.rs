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
    DeleteNoop,
    CleanupAppliedUnknown,
    CleanupRejected,
}

#[derive(Default)]
struct Observed {
    batches: AtomicUsize,
    ready_writes: AtomicUsize,
    engine: Option<Arc<crate::engine::TemporalEngine>>,
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
    if matches!(fault, Fault::CleanupAppliedUnknown | Fault::CleanupRejected) {
        let mut other = record.clone();
        other.id = format!("{tenant:016x}-0000000000000002");
        other.context_ref = other.id.clone();
        other.user_id = "bob".into();
        other.memory = "unrelated retained memory".into();
        commands.extend([
            memory_catalog_write(tenant, std::iter::once(&other)),
            memory_node_lookup_write(tenant, std::iter::once(&other)),
            Command::SetAdd { key: memory_order_key(tenant), member: memory_order_member(1, &other.id) },
            Command::ZSetAdd { key: memory_identity_key(tenant), member: MEMORY_TOTAL_MEMBER.to_vec(), score: 2.0 },
        ]);
        for record in [&record, &other] {
            commands.push(Command::HashSet {
                key: memory_identity_postings_key(tenant), field: record.id.clone(),
                value: memory_identity_posting(std::slice::from_ref(record)),
            });
            for member in memory_identity_members(record) {
                commands.push(Command::ZSetAdd {
                    key: memory_identity_key(tenant), member: member.clone(), score: 1.0,
                });
                commands.push(Command::SetAdd {
                    key: memory_identity_records_key(tenant, &member), member: record.id.as_bytes().to_vec(),
                });
            }
        }
    }
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
    let observed = Arc::new(Observed {
        engine: Some(Arc::clone(&engine)),
        ..Observed::default()
    });
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
                if matches!(fault, Fault::DeleteNoop)
                    && batch.commands.iter().all(|command| matches!(command,
                        Command::HashDelete { key, .. } if key == &memory_catalog_key(tenant))) {
                    let mut result = engine.batch_execute(batch);
                    for response in &mut result.responses {
                        response.response = CommandResponse::Integer { value: 0 };
                    }
                    return json_response(200, &result);
                }
                for command in &batch.commands {
                    if matches!(command,Command::StringSet {key,..} if key==&memory_order_ready_key(tenant)||key==&memory_identity_ready_key(tenant)) {
                        seen.ready_writes.fetch_add(1,Ordering::Relaxed);
                    }
                }
                if ordinal!=fail_batch { return json_response(200,&engine.batch_execute(batch)); }
                let mut result = if matches!(fault,Fault::AppliedUnknown | Fault::CleanupAppliedUnknown) { engine.batch_execute(batch) }
                    else { BatchExecuteResponse {status:Status::ok(), responses:batch.commands.iter().map(|_| ExecuteResponse {
                        status:Status::ok(),response:CommandResponse::Empty,
                    }).collect()} };
                if matches!(fault, Fault::CleanupAppliedUnknown | Fault::CleanupRejected) {
                    result.status = Status::error(
                        if matches!(fault, Fault::CleanupAppliedUnknown) { "write_outcome_unknown" } else { "cleanup_rejected" },
                        "injected cleanup acknowledgment failure",
                    );
                    return json_response(200, &result);
                }
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
                if matches!(fault, Fault::DeleteNoop)
                    && matches!(&body.command, Command::HashDelete { key, .. } if key == &memory_catalog_key(tenant)) {
                    // Another delete wins after the proxy reads the record.
                    assert!(engine.execute(body).status.ok);
                    return json_response(200, &ExecuteResponse {
                        status: Status::ok(), response: CommandResponse::Integer { value: 0 },
                    });
                }
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
fn batch_delete_ack_rejects_a_concurrent_noop() {
    let (_dir, proxy, id, _tenant, seen) = fixture(Fault::DeleteNoop, usize::MAX, true);
    let (status, body) = call(&proxy, "POST", "/memories/batch/delete".into(),
        json!({"memories":[id]}));
    assert_eq!(status, 200);
    assert_eq!(body["deleted"], 0, "{body}");
    assert_eq!(body["failed"].as_array().unwrap().len(), 1);
    assert_eq!(body["failed"][0]["status"], 409);
    assert_eq!(body["failed"][0]["code"], "memory_delete_not_applied");
    assert_eq!(seen.batches.load(Ordering::Relaxed), 1);
}

#[test]
fn delete_cleanup_failure_preserves_single_removal() {
    for fault in [Fault::CleanupAppliedUnknown, Fault::CleanupRejected] {
        let (_dir, proxy, id, tenant, seen) = fixture(fault, 1, true);
        let (status, _) = call(&proxy, "DELETE", format!("/memories/{id}"), json!({}));
        assert_eq!(status, 502);
        assert!(proxy.memory_read_one_checked(tenant, &id).unwrap().is_none(),
            "an acknowledged catalog removal must not be reversed: {fault:?}");
        assert_eq!(seen.batches.load(Ordering::Relaxed), 1, "no compensating write");
    }
}

#[test]
fn delete_cleanup_failure_preserves_scoped_tombstones() {
    for fault in [Fault::CleanupAppliedUnknown, Fault::CleanupRejected] {
        let (_dir, proxy, id, tenant, seen) = fixture(fault, 1, true);
        let (status, _) = call(&proxy, "POST", "/memories/delete_all".into(),
            json!({"scope":{"tenant_id":"ack","account_id":"test"},"user_id":"alice"}));
        assert_eq!(status, 502);
        assert!(proxy.memory_read_one_checked(tenant, &id).unwrap().is_none(),
            "an acknowledged tombstone must not be cleared: {fault:?}");
        let other = format!("{tenant:016x}-0000000000000002");
        assert_eq!(proxy.memory_read_one_checked(tenant, &other).unwrap().unwrap().user_id, "bob");
        assert_eq!(seen.batches.load(Ordering::Relaxed), 1, "no compensating write");
    }
}

#[test]
fn delete_cleanup_failure_stays_failed_in_batch_response() {
    let (_dir, proxy, id, tenant, seen) = fixture(Fault::CleanupAppliedUnknown, 2, true);
    let (status, body) = call(&proxy, "POST", "/memories/batch/delete".into(),
        json!({"memories":[id]}));
    assert_eq!(status, 200);
    assert_eq!(body["deleted"], 0);
    assert_eq!(body["failed"][0]["code"], "write_outcome_unknown");
    assert!(proxy.memory_read_one_checked(tenant, &id).unwrap().is_none());
    assert_eq!(seen.batches.load(Ordering::Relaxed), 2);
}

fn seed_delete_page(proxy: &ProxyService, tenant: u64, id: &str, seen: &Observed, count: usize) -> Vec<String> {
    let original = proxy.memory_read_one_checked(tenant, id).unwrap().unwrap();
    let mut ids = vec![id.to_owned()];
    let mut commands = Vec::new();
    for index in 1..count {
        let mut record = original.clone();
        record.id = format!("{tenant:016x}-{:016x}", index + 2);
        record.context_ref = record.id.clone();
        ids.push(record.id.clone());
        commands.extend([
            memory_catalog_write(tenant, std::iter::once(&record)),
            memory_node_lookup_write(tenant, std::iter::once(&record)),
            Command::SetAdd { key: memory_order_key(tenant), member: memory_order_member(1, &record.id) },
            Command::ZSetCountAdjust { key: memory_identity_key(tenant), member: MEMORY_TOTAL_MEMBER.to_vec(), increment: 1 },
        ]);
        for member in memory_identity_members(&record) {
            commands.push(Command::ZSetCountAdjust { key: memory_identity_key(tenant), member, increment: 1 });
        }
    }
    assert!(seen.engine.as_ref().unwrap().batch_execute(BatchExecuteRequest {
        shard_id: proxy.context_shard_id(tenant), commands,
    }).status.ok);
    ids
}

#[test]
fn batch_delete_pages_storage_counts_actual_removals() {
    let (_dir, proxy, id, tenant, seen) = fixture(Fault::CleanupRejected, usize::MAX, true);
    let ids = seed_delete_page(&proxy, tenant, &id, &seen, 2);
    let engine = seen.engine.as_ref().unwrap();
    let command = |field: &str| Command::HashDelete {
        key: memory_catalog_key(tenant), field: field.to_owned(),
    };
    for expected in [1, 0] {
        let response = engine.execute(ExecuteRequest {
            shard_id: proxy.context_shard_id(tenant), command: command(&id),
        });
        assert!(response.status.ok);
        assert_eq!(response.response, CommandResponse::Integer { value: expected });
    }
    let response = engine.batch_execute(BatchExecuteRequest {
        shard_id: proxy.context_shard_id(tenant),
        commands: vec![command(&ids[1]), command(&ids[1]), command("missing")],
    });
    assert!(response.status.ok);
    for (child, expected) in response.responses.iter().zip([1, 0, 0]) {
        assert!(child.status.ok);
        assert_eq!(child.response, CommandResponse::Integer { value: expected });
    }
    assert_eq!(response.responses.len(), 3);
}

#[test]
fn batch_delete_pages_share_mutation_calls_and_keep_unrelated_records() {
    for count in [16, MEMORY_UPDATE_PAGE_SIZE + 1] {
        let (_dir, proxy, id, tenant, seen) = fixture(Fault::CleanupRejected, usize::MAX, true);
        let ids = seed_delete_page(&proxy, tenant, &id, &seen, count);
        let (status, body) = call(&proxy, "POST", "/memories/batch/delete".into(), json!({"memories":ids}));
        assert_eq!(status, 200);
        assert_eq!(body["deleted"], count, "{body}");
        assert!(body["failed"].as_array().unwrap().is_empty(), "{body}");
        assert_eq!(seen.batches.load(Ordering::Relaxed), 2 * count.div_ceil(MEMORY_UPDATE_PAGE_SIZE));
        assert!(proxy.memory_read_many_checked(tenant, &ids).unwrap().iter().all(Option::is_none));
        let bob = format!("{tenant:016x}-0000000000000002");
        assert!(proxy.memory_read_one_checked(tenant, &bob).unwrap().is_some());
        let response = seen.engine.as_ref().unwrap().execute(ExecuteRequest {
            shard_id: proxy.context_shard_id(tenant), command: Command::ZSetScore {
                key: memory_identity_key(tenant), member: MEMORY_TOTAL_MEMBER.to_vec(),
            },
        });
        assert!(response.status.ok);
        assert_eq!(response.response, CommandResponse::Bytes { value: Some(b"1".to_vec()) });
        let (status, history) = call(&proxy, "GET", format!("/memories/{id}/history"), json!({}));
        assert_eq!(status, 200);
        assert_eq!(history["results"][0]["event"], "DELETE");
    }
}

#[test]
fn batch_delete_pages_reject_duplicates_and_missing_ids() {
    let (_dir, proxy, id, tenant, seen) = fixture(Fault::CleanupRejected, usize::MAX, true);
    let missing = format!("{tenant:016x}-ffffffffffffffff");
    let (_, body) = call(&proxy, "POST", "/memories/batch/delete".into(),
        json!({"memories":[id, id, missing]}));
    assert_eq!(body["deleted"], 1);
    let failed = body["failed"].as_array().unwrap();
    assert_eq!(failed.len(), 2);
    assert!(failed.iter().any(|row| row["code"] == "duplicate_memory_id"));
    assert!(failed.iter().any(|row| row["code"] == "memory_not_found"));
    assert_eq!(seen.batches.load(Ordering::Relaxed), 2);
}

#[test]
fn batch_delete_pages_unknown_primary_is_not_acknowledged_or_compensated() {
    let (_dir, proxy, id, tenant, seen) = fixture(Fault::CleanupAppliedUnknown, 1, true);
    let ids = seed_delete_page(&proxy, tenant, &id, &seen, 16);
    let (_, body) = call(&proxy, "POST", "/memories/batch/delete".into(), json!({"memories":ids}));
    assert_eq!(body["deleted"], 0);
    assert_eq!(body["failed"].as_array().unwrap().len(), 16);
    assert!(body["failed"].as_array().unwrap().iter().all(|row| row["code"] == "write_outcome_unknown"));
    assert_eq!(seen.batches.load(Ordering::Relaxed), 1);
    assert!(proxy.memory_read_many_checked(tenant, &ids).unwrap().iter().all(Option::is_none));
}

#[test]
fn batch_delete_pages_only_cleanup_confirmed_children() {
    let (_dir, proxy, id, tenant, seen) = fixture(Fault::AppliedUnknown, 1, true);
    let ids = seed_delete_page(&proxy, tenant, &id, &seen, 2);
    let (_, body) = call(&proxy, "POST", "/memories/batch/delete".into(), json!({"memories":ids}));
    assert_eq!(body["deleted"], 1, "{body}");
    assert_eq!(body["results"][0]["memory_id"], ids[1]);
    assert_eq!(body["failed"][0]["code"], "write_outcome_unknown");
    assert_eq!(seen.batches.load(Ordering::Relaxed), 2);
    let (_, history) = call(&proxy, "GET", format!("/memories/{id}/history"), json!({}));
    assert!(history["results"].as_array().unwrap().is_empty());
    assert!(proxy.memory_read_many_checked(tenant, &ids).unwrap().iter().all(Option::is_none));
}

#[test]
fn batch_delete_pages_cleanup_failure_never_restores_a_page() {
    for fault in [Fault::CleanupAppliedUnknown, Fault::CleanupRejected] {
        let (_dir, proxy, id, tenant, seen) = fixture(fault, 2, true);
        let ids = seed_delete_page(&proxy, tenant, &id, &seen, 16);
        let (_, body) = call(&proxy, "POST", "/memories/batch/delete".into(), json!({"memories":ids}));
        assert_eq!(body["deleted"], 0);
        assert_eq!(body["failed"].as_array().unwrap().len(), 16);
        assert_eq!(seen.batches.load(Ordering::Relaxed), 2);
        assert!(proxy.memory_read_many_checked(tenant, &ids).unwrap().iter().all(Option::is_none));
    }
}

#[test]
fn batch_delete_pages_incomplete_primary_response_is_not_success() {
    let (_dir, proxy, id, _tenant, seen) = fixture(Fault::Missing, 1, true);
    let (_, body) = call(&proxy, "POST", "/memories/batch/delete".into(), json!({"memories":[id]}));
    assert_eq!(body["deleted"], 0);
    assert_eq!(body["failed"][0]["code"], "bad_response");
    assert_eq!(seen.batches.load(Ordering::Relaxed), 1);
}

#[test]
fn delete_cleanup_failure_checkpoint_retains_removal() {
    for scoped in [false, true] {
        for fault in [Fault::CleanupAppliedUnknown, Fault::CleanupRejected] {
            let (dir, proxy, id, tenant, seen) = fixture(fault, 1, true);
            let (status, _) = if scoped {
                call(&proxy, "POST", "/memories/delete_all".into(),
                    json!({"scope":{"tenant_id":"ack","account_id":"test"},"user_id":"alice"}))
            } else {
                call(&proxy, "DELETE", format!("/memories/{id}"), json!({}))
            };
            assert_eq!(status, 502);
            let source = seen.engine.as_ref().unwrap();
            let shard_id = proxy.context_shard_id(tenant);
            source.materialize_synthetic_blocks(shard_id);
            source.flush_shard_index(shard_id);
            let bytes = source.export_index_bytes(shard_id).unwrap();
            let (start_routing_bucket, end_routing_bucket) = source.shard_routing_range(shard_id);
            let restored = crate::engine::TemporalEngine::with_local_dirs(
                16 << 20, dir.path().join("restored-cache"), dir.path().join("blocks"),
                dir.path().join("restored-index"),
            );
            restored.install_index_bytes_in_empty_wal(shard_id, &bytes,
                crate::engine::routing_range_stamp::RoutingRangeStamp {
                    start_routing_bucket, end_routing_bucket,
                }).unwrap();
            restored.load_shard(shard_id);
            let response = restored.execute(ExecuteRequest {
                shard_id, command: Command::HashGet {
                    key: if scoped { memory_tombstone_key(tenant) } else { memory_catalog_key(tenant) },
                    field: id.clone(),
                },
            });
            assert!(response.status.ok);
            assert_eq!(response.response, CommandResponse::Bytes {
                value: scoped.then(|| b"1".to_vec()),
            });
        }
    }
}

#[test]
fn batch_delete_ack_requires_true_and_matching_id() {
    assert!(memory_delete_ack("m1", 200, br#"{"deleted":true,"id":"m1"}"#).is_ok());
    assert_eq!(memory_delete_ack("m1", 200, br#"{"deleted":true,"id":"other"}"#),
        Err((502, "invalid_memory_delete_response".into())));
}

#[test]
fn batch_delete_ack_rejects_false_and_missing_fields() {
    assert_eq!(memory_delete_ack("m1", 200, br#"{"deleted":false,"id":"m1"}"#),
        Err((409, "memory_delete_not_applied".into())));
    for body in [br#"{}"#.as_slice(), br#"{"id":"m1"}"#, br#"{"deleted":true}"#,
        br#"{"deleted":1,"id":"m1"}"#] {
        assert_eq!(memory_delete_ack("m1", 200, body),
            Err((502, "invalid_memory_delete_response".into())));
    }
}

#[test]
fn batch_delete_ack_preserves_remote_failure_codes() {
    assert_eq!(memory_delete_ack("m1", 502,
        br#"{"deleted":true,"id":"m1","code":"write_outcome_unknown"}"#),
        Err((502, "write_outcome_unknown".into())));
    assert_eq!(memory_delete_ack("m1", 404, br#"{"deleted":false,"id":"m1"}"#),
        Err((404, "memory_not_found".into())));
}

#[test]
fn batch_delete_ack_rejects_malformed_bodies() {
    for body in [b"not-json".as_slice(), b"null", b"[]",
        br#"{"deleted":true,"deleted":false,"id":"m1"}"#] {
        assert_eq!(memory_delete_ack("m1", 200, body),
            Err((502, "invalid_memory_delete_response".into())));
    }
    assert_eq!(memory_delete_ack("m1", 503, b"unavailable"),
        Err((503, "memory_delete_failed".into())));
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
