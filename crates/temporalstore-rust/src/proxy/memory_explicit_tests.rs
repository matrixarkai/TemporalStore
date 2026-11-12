// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::context_workflow::{ingest_context_without_inference, ingest_wire};
use crate::http::{json_response, parse_json, serve_listener};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[path = "memory_inferred_route_tests.rs"]
mod inferred_routes;

struct Fixture {
    _dir: tempfile::TempDir,
    proxy: ProxyService,
    tenant: u64,
    provider: Value,
    explicit: Arc<AtomicUsize>,
    inferred: Arc<AtomicUsize>,
    chats: Arc<AtomicUsize>,
    inputs: Arc<Mutex<Vec<Vec<String>>>>,
}

fn fixture(ack_mode: u8) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let engine = crate::TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    let proxy = ProxyService::new(ProxyOptions {
        context_shard_count: 1,
        route_cache_ttl_ms: 600_000,
        topology_check_interval_ms: u64::MAX,
        ..Default::default()
    });
    let tenant = context::context_tenant_hash(
        &MemoryScope {
            tenant_id: "explicit".into(),
            account_id: "test".into(),
            ..Default::default()
        }
        .context_scope(&MemoryIdentity::default()),
    );
    let shard = proxy.context_shard_id(tenant);
    engine.load_shard(shard);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    proxy
        .client()
        .insert_cached_route_for_test(shard, addr.clone());
    proxy.memory_remember_indexes_ready(tenant);
    let explicit = Arc::new(AtomicUsize::new(0));
    let inferred = Arc::new(AtomicUsize::new(0));
    let chats = Arc::new(AtomicUsize::new(0));
    let inputs = Arc::new(Mutex::new(Vec::<Vec<String>>::new()));
    let (write_count, infer_count, chat_count, captured) = (
        Arc::clone(&explicit),
        Arc::clone(&inferred),
        Arc::clone(&chats),
        Arc::clone(&inputs),
    );
    std::thread::spawn(move || {
        serve_listener(listener, move |request| match request.path.as_str() {
            "/execute" => json_response(200, &engine.execute(parse_json(&request.body).unwrap())),
            "/batch_execute" => {
                let mut result = engine.batch_execute(parse_json(&request.body).unwrap());
                if ack_mode == 4
                    && write_count.load(Ordering::Relaxed) > 0
                    && !result.responses.is_empty()
                {
                    result.responses[0].status =
                        Status::error("catalog_write_failed", "catalog write failed");
                }
                json_response(200, &result)
            }
            "/context/ingest_explicit" => {
                write_count.fetch_add(1, Ordering::Relaxed);
                match ack_mode {
                    1 => (200, ingest_wire::encode_response(Status::ok(), 0, 0)),
                    2 => json_response(200, &json!({"status":{"ok":true}})),
                    3 => json_response(404, &Status::error("not_found", "unsupported endpoint")),
                    _ => {
                        let decoded = ingest_wire::decode_request(&request.body).unwrap();
                        let result = ingest_context_without_inference(&engine, decoded);
                        (
                            200,
                            ingest_wire::encode_response(
                                result.status,
                                result.accepted,
                                result.failed,
                            ),
                        )
                    }
                }
            }
            "/context/ingest_facts" => {
                infer_count.fetch_add(1, Ordering::Relaxed);
                if ack_mode == 19 {
                    return json_response(404, &Status::error("not_found", "fact endpoint unavailable"));
                }
                let mut decoded = ingest_wire::decode_request(&request.body).unwrap();
                if ack_mode < 10 { decoded.provider = Default::default(); }
                let body = crate::context_workflow::memory_wire::execute_request(&engine, decoded);
                let mut receipt = crate::sdk::v1::ContextMemoryIngestResponse::decode(body.as_slice()).unwrap();
                match ack_mode {
                    11 => receipt.sources[0].facts.clear(),
                    12 => receipt.acknowledgment.as_mut().unwrap().accepted = 0,
                    13 => receipt.sources[1].source_ref = receipt.sources[0].source_ref.clone(),
                    14 => receipt.sources[0].facts[0].entity_hash = 1,
                    18 => receipt.sources[0].node_hash = 1,
                    _ => {},
                }
                (200, receipt.encode_to_vec())
            }
            "/context/ingest_extract" => {
                infer_count.fetch_add(1, Ordering::Relaxed);
                json_response(200, &json!({"status":{"ok":true}}))
            }
            "/context/retrieve/nodes" => json_response(
                200,
                &crate::context_workflow::retrieve_context_from_nodes(
                    &engine,
                    parse_json(&request.body).unwrap(),
                ),
            ),
            "/context/retrieve" => json_response(200, &crate::context_workflow::retrieve_context(
                &engine, parse_json(&request.body).unwrap())),
            "/v1/embeddings" => {
                let body: Value = parse_json(&request.body).unwrap();
                assert_eq!(body["model"], "explicit-encoder");
                let texts: Vec<String> = serde_json::from_value(body["input"].clone()).unwrap();
                let data = texts
                    .iter()
                    .enumerate()
                    .map(|(index, _)| json!({"index":index,"embedding":[1.0,0.0,0.0,0.0]}))
                    .collect::<Vec<_>>();
                captured.lock().unwrap().push(texts);
                json_response(200, &json!({"data":data}))
            }
            "/v1/chat/completions" => {
                chat_count.fetch_add(1, Ordering::Relaxed);
                if ack_mode >= 10 {
                    let content = json!({"l0":"User preferences", "l1":"Uses Rust services and Vim editing.",
                        "entities":if ack_mode == 15 { json!([]) } else if ack_mode == 17 { json!([
                            {"kind":"preference","name":"language","value":"Rust services"},
                            {"kind":"preference","name":"language","value":"Go services"}
                        ]) } else { json!([
                            {"kind":"preference","name":"language","value":"Rust services"},
                            {"kind":"preference","name":"editor","value":"Vim editing"}
                        ]) }}).to_string();
                    return json_response(200, &json!({"choices":[{"message":{"content":content}}]}));
                }
                json_response(503, &Status::error("unavailable", "chat must not be used"))
            }
            _ => panic!("unexpected path {}", request.path),
        })
        .unwrap();
    });
    Fixture {
        _dir: dir,
        proxy,
        tenant,
        explicit,
        inferred,
        chats,
        inputs,
        provider: json!({"provider_name":"encoder","provider_kind":"open_ai_compatible",
            "base_url":format!("http://{addr}/v1"),"model":"unavailable-chat",
            "embedding_model":"explicit-encoder","mock_mode":false,"max_retries":0}),
    }
}

fn add(fixture: &Fixture, mut body: Value) -> (u16, Value) {
    body["scope"] = json!({"tenant_id":"explicit","account_id":"test"});
    body["user_id"] = json!("alice");
    body["provider"] = fixture.provider.clone();
    let request: MemoryAddRequest = serde_json::from_value(body).unwrap();
    let (status, body) = fixture.proxy.memory_add(request);
    (status, serde_json::from_slice(&body).unwrap())
}

#[test]
fn explicit_memory_is_embedded_addressable_and_searchable_without_chat() {
    let fixture = fixture(0);
    let raw = "  Keep exact Rust memory.\n";
    let (status, response) = add(
        &fixture,
        json!({"infer":false,"messages":[
        {"role":"system","content":"must not be a memory"},
        {"role":"assistant","content":raw,"name":"cedar"}
    ],"metadata":{"topic":"storage"}}),
    );
    assert_eq!(status, 200, "{response}");
    assert_eq!(response["results"][0]["memory"], raw);
    assert_eq!(response["results"][0]["role"], "assistant");
    assert_eq!(response["results"][0]["actor_id"], "cedar");
    let id = response["results"][0]["id"].as_str().unwrap();
    let record = fixture.proxy.memory_read_one(fixture.tenant, id).unwrap();
    assert_eq!(record.memory, raw);
    assert_eq!(record.metadata["topic"], "storage");
    assert_eq!(record.metadata["role"], "assistant");
    assert_eq!(record.metadata["actor_id"], "cedar");
    assert_eq!(*fixture.inputs.lock().unwrap(), vec![vec![raw.to_owned()]]);
    let (status, found) = fixture.proxy.memory_search(
        serde_json::from_value(json!({
            "scope":{"tenant_id":"explicit","account_id":"test"}, "user_id":"alice",
            "query":"Rust memory","limit":1,"provider":fixture.provider
        }))
        .unwrap(),
    );
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&found));
    let found: Value = serde_json::from_slice(&found).unwrap();
    assert_eq!(found["results"][0]["id"], id);
    assert_eq!(fixture.explicit.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.inferred.load(Ordering::Relaxed), 0);
    assert_eq!(fixture.chats.load(Ordering::Relaxed), 0);
    let (status, response) = fixture.proxy.memory_update(
        id,
        serde_json::from_value(json!({
            "data":"Updated Rust memory", "provider":fixture.provider
        }))
        .unwrap(),
    );
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&response));
    let (status, response) = fixture.proxy.memory_batch_update(
        serde_json::from_value(json!({
            "memories":[{"memory_id":id,"text":"Final Rust memory"}],"provider":fixture.provider
        }))
        .unwrap(),
    );
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&response));
    let response: Value = serde_json::from_slice(&response).unwrap();
    assert_eq!(response["updated"], 1);
    assert_eq!(
        fixture
            .proxy
            .memory_read_one(fixture.tenant, id)
            .unwrap()
            .memory,
        "Final Rust memory"
    );
    assert_eq!(fixture.explicit.load(Ordering::Relaxed), 3);
    assert_eq!(fixture.inferred.load(Ordering::Relaxed), 0);
    assert_eq!(fixture.chats.load(Ordering::Relaxed), 0);
}

#[test]
fn default_memory_add_keeps_inference_route_and_explicit_system_only_is_empty() {
    let fixture = fixture(0);
    let (status, _) = add(&fixture, json!({"messages":"remember this"}));
    assert_eq!(status, 200);
    assert_eq!(fixture.inferred.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.explicit.load(Ordering::Relaxed), 0);
    let (status, body) = add(
        &fixture,
        json!({"infer":false,"messages":[{"role":"system","content":"instructions"}]}),
    );
    assert_eq!(status, 200);
    assert_eq!(body["results"], json!([]));
    assert_eq!(fixture.explicit.load(Ordering::Relaxed), 0);
}

#[test]
fn metadata_updates_keep_the_embedded_revision_searchable() {
    let fixture = fixture(0);
    let (status, added) = add(
        &fixture,
        json!({"infer":false,"messages":"Keep the durable retrieval memory."}),
    );
    assert_eq!(status, 200, "{added}");
    let id = added["results"][0]["id"].as_str().unwrap();
    let before = fixture.proxy.memory_read_one(fixture.tenant, id).unwrap();
    let (status, body) = fixture.proxy.memory_update(
        id,
        serde_json::from_value(json!({"metadata":{"category":"first"}})).unwrap(),
    );
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let (status, body) = fixture.proxy.memory_batch_update(
        serde_json::from_value(json!({
            "memories":[{"memory_id":id,"metadata":{"category":"latest"}}]
        })).unwrap(),
    );
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["updated"], 1);
    let after = fixture.proxy.memory_read_one(fixture.tenant, id).unwrap();
    assert_eq!(after.context_ref, before.context_ref);
    assert_eq!(after.memory, before.memory);
    assert!(after.updated_at_ms > before.updated_at_ms);
    assert_eq!(fixture.explicit.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.inputs.lock().unwrap().len(), 1);
    let (status, body) = fixture.proxy.memory_search(
        serde_json::from_value(json!({
            "scope":{"tenant_id":"explicit","account_id":"test"},"user_id":"alice",
            "query":"durable retrieval memory","limit":1,"provider":fixture.provider
        })).unwrap(),
    );
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let found: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(found["results"][0]["id"], id);
    assert_eq!(found["results"][0]["metadata"]["category"], "latest");
    assert_eq!(fixture.inferred.load(Ordering::Relaxed), 0);
    assert_eq!(fixture.chats.load(Ordering::Relaxed), 0);
}

#[test]
fn explicit_memory_rejects_bad_ack_old_server_and_failed_catalog_writes() {
    for mode in 1..=4 {
        let fixture = fixture(mode);
        let (status, body) = add(
            &fixture,
            json!({"infer":false,"messages":"explicit memory"}),
        );
        assert_eq!(status, 502, "mode={mode} {body}");
        assert!(body.get("results").is_none());
        assert_eq!(fixture.inferred.load(Ordering::Relaxed), 0);
        assert_eq!(fixture.chats.load(Ordering::Relaxed), 0);
    }
}

#[test]
fn memory_add_limits_are_checked_before_backend_work() {
    let fixture = fixture(0);
    let (status, _) = add(
        &fixture,
        json!({"infer":false,"messages":vec!["memory";1001]}),
    );
    assert_eq!(status, 400);
    let request = crate::http::HttpRequest {
        method: "POST".into(),
        path: "/memories".into(),
        body: vec![0; ingest_wire::EXPLICIT_MAX_BODY_BYTES + 1],
    };
    let response = fixture.proxy.handle_memory_route(&request).unwrap();
    assert_eq!(response.0, 400);
    assert_eq!(fixture.explicit.load(Ordering::Relaxed), 0);
    assert_eq!(fixture.inferred.load(Ordering::Relaxed), 0);
    assert_eq!(fixture.chats.load(Ordering::Relaxed), 0);
}
