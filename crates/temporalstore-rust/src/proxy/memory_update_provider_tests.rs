// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::http::{json_response, parse_json, serve_listener};
use std::sync::{Arc, Mutex};

const TENANT: u64 = 42;

fn fixture() -> (
    tempfile::TempDir,
    ProxyService,
    Arc<Mutex<Vec<Value>>>,
    Vec<String>,
) {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(crate::engine::TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    ));
    let proxy = ProxyService::new(ProxyOptions {
        context_shard_count: 1,
        route_cache_ttl_ms: 600_000,
        topology_check_interval_ms: u64::MAX,
        ..ProxyOptions::default()
    });
    let shard_id = proxy.context_shard_id(TENANT);
    engine.load_shard(shard_id);
    let records = (1..=5)
        .map(|ordinal| {
            let id = format!("{TENANT:016x}-{ordinal:016x}");
            MemoryRecord {
                context_ref: id.clone(),
                id,
                memory: format!("original memory {ordinal}"),
                metadata: Value::Null,
                tenant_id: "tenant".into(),
                account_id: "account".into(),
                user_id: "alice".into(),
                agent_id: "agent".into(),
                run_id: "session".into(),
                created_at_ms: 1,
                updated_at_ms: 1,
            }
        })
        .collect::<Vec<_>>();
    for command in [
        memory_catalog_write(TENANT, records.iter()),
        memory_node_lookup_write(TENANT, records.iter()),
    ] {
        assert!(
            engine
                .execute(ExecuteRequest { shard_id, command })
                .status
                .ok
        );
    }
    let calls = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&calls);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        serve_listener(listener, move |request| match request.path.as_str() {
            "/execute" => json_response(200, &engine.execute(parse_json(&request.body).unwrap())),
            "/batch_execute" => json_response(
                200,
                &engine.batch_execute(parse_json(&request.body).unwrap()),
            ),
            "/context/ingest_explicit" => {
                let decoded = crate::context_workflow::ingest_wire::decode_request(&request.body).unwrap();
                let count = decoded.sources.len();
                captured
                    .lock()
                    .unwrap()
                    .push(serde_json::to_value(decoded).unwrap());
                (200, crate::context_workflow::ingest_wire::encode_response(Status::ok(), count, 0))
            }
            _ => panic!("unexpected path: {}", request.path),
        })
        .unwrap();
    });
    proxy.client().insert_cached_route_for_test(shard_id, addr);
    proxy.memory_remember_indexes_ready(TENANT);
    (
        dir,
        proxy,
        calls,
        records.into_iter().map(|record| record.id).collect(),
    )
}

fn provider(name: &str, encoder: &str) -> Value {
    serde_json::to_value(crate::ContextModelProviderConfig {
        provider_name: name.into(), provider_kind: crate::ContextProviderKind::Ollama,
        base_url: "http://127.0.0.1:11434".into(), model: "qwen2.5:3b".into(),
        embedding_model: encoder.into(), mock_mode: false, ..Default::default()
    }).unwrap()
}

#[test]
fn single_memory_update_forwards_the_selected_provider() {
    let (_dir, proxy, calls, ids) = fixture();
    let selected = provider("local", "all-minilm");
    let request =
        serde_json::from_value(json!({"data":"updated text","provider":selected})).unwrap();
    let (status, body) = proxy.memory_update(&ids[0], request);
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0]["provider"], selected);
    assert_eq!(calls[0]["sources"][0]["body"], "updated text");
    assert_eq!(
        proxy.memory_read_one(TENANT, &ids[0]).unwrap().memory,
        "updated text"
    );
}

#[test]
fn batch_memory_update_inherits_and_separates_model_providers() {
    let (_dir, proxy, calls, ids) = fixture();
    let common = provider("local", "all-minilm");
    let other = provider("alternate", "second-encoder");
    let request = serde_json::from_value(json!({
        "provider":common,
        "memories":[
            {"memory_id":ids[0],"text":"first"},
            {"memory_id":ids[1],"text":"second"},
            {"id":ids[2],"data":"third","provider":other},
            {"memory_id":ids[3],"text":"fourth","provider":null},
            {"memory_id":ids[4],"text":"fifth","provider":common}
        ]
    }))
    .unwrap();
    let (status, body) = proxy.memory_batch_update(request);
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let response: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(response["updated"], 5);
    assert!(response["failed"].as_array().unwrap().is_empty());
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    let inherited = calls
        .iter()
        .find(|call| call["provider"] == common)
        .unwrap();
    let overridden = calls.iter().find(|call| call["provider"] == other).unwrap();
    assert_eq!(inherited["sources"].as_array().unwrap().len(), 4);
    assert_eq!(overridden["sources"].as_array().unwrap().len(), 1);
    assert_eq!(overridden["sources"][0]["body"], "third");
}

#[test]
fn memory_updates_without_provider_keep_server_default_selection() {
    let (_dir, proxy, calls, ids) = fixture();
    let single = serde_json::from_value(json!({"data":"single"})).unwrap();
    assert_eq!(proxy.memory_update(&ids[0], single).0, 200);
    let batch = serde_json::from_value(json!({
        "memories":[{"memory_id":ids[1],"text":"batch"}]
    }))
    .unwrap();
    assert_eq!(proxy.memory_batch_update(batch).0, 200);
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert!(calls
        .iter()
        .all(|call| call["provider"] == serde_json::to_value(crate::ContextModelProviderConfig::default()).unwrap()));
}
