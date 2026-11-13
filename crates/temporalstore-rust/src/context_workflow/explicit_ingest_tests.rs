// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::http::{json_response, serve_listener};
use prost::Message;
use serde_json::json;
use std::sync::{Arc, Mutex};

fn request(provider: ContextModelProviderConfig) -> ContextIngestExtractRequest {
    ContextIngestExtractRequest {
        shard_id: 1,
        tenant_hash: 42,
        query: String::new(),
        start_time_ms: 0,
        end_time_ms: 2000,
        max_events: 10,
        provider,
        sources: (0..2)
            .map(|index| ContextExtractRequest {
                shard_id: 1,
                tenant_hash: 42,
                source_kind: ContextSourceKind::Chat,
                source_id: format!("explicit-{index}"),
                title: "user".into(),
                body: "  Keep exact Rust memory.\n".into(),
                timestamp_ms: 1000 + index,
                provider: inherited_context_source_provider(),
            })
            .collect(),
    }
}

#[test]
fn explicit_ingest_skips_chat_deduplicates_encoder_and_recovers_for_retrieval() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let inputs = Arc::new(Mutex::new(Vec::<Vec<String>>::new()));
    let captured = Arc::clone(&inputs);
    std::thread::spawn(move || {
        serve_listener(listener, move |request| {
            assert_eq!(
                request.path, "/v1/embeddings",
                "explicit ingestion must never invoke chat"
            );
            let body: Value = serde_json::from_slice(&request.body).unwrap();
            assert_eq!(body["model"], "explicit-encoder");
            let texts: Vec<String> = serde_json::from_value(body["input"].clone()).unwrap();
            let data = texts
                .iter()
                .enumerate()
                .map(|(index, _)| json!({"index":index,"embedding":[1.0,0.0,0.0,0.0]}))
                .collect::<Vec<_>>();
            captured.lock().unwrap().push(texts);
            json_response(200, &json!({"data":data}))
        })
        .unwrap();
    });
    let provider = ContextModelProviderConfig {
        provider_name: "explicit-encoder".into(),
        provider_kind: ContextProviderKind::OpenAiCompatible,
        base_url: format!("http://{addr}/v1"),
        model: "chat-is-unavailable".into(),
        embedding_model: "explicit-encoder".into(),
        mock_mode: false,
        max_retries: 0,
        ..Default::default()
    };
    let dir = tempfile::tempdir().unwrap();
    let open = || {
        let engine = TemporalEngine::with_local_dirs(
            16 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine
    };
    let engine = open();
    let report = ingest_context_without_inference(&engine, request(provider));
    assert!(report.status.ok, "{:?}", report.status);
    assert_eq!((report.accepted, report.failed), (2, 0));
    assert_eq!(
        *inputs.lock().unwrap(),
        vec![vec!["  Keep exact Rust memory.\n".to_string()]]
    );
    for extract in &report.extracts {
        assert_eq!(extract.extracted_entity_count, 0);
        assert_eq!(extract.event.text, "  Keep exact Rust memory.\n");
        assert_eq!(extract.node.vector, extract.event.vector);
        assert_eq!(extract.node.vector.len(), 4);
        assert!(!extract.embedding_generation.mock_mode);
        assert_eq!(
            extract.node.embedding_model_hash,
            context_embedding_model_hash("explicit-encoder")
        );
    }
    engine.write_ahead_log_store().flush(1).unwrap();
    drop(engine);
    let recovered = open();
    let mut query = report.retrieve_request;
    query.query = "Rust memory".into();
    let retrieved = retrieve_context_from_nodes(&recovered, query);
    assert!(retrieved.status.ok, "{:?}", retrieved.status);
    assert!(retrieved
        .blocks
        .iter()
        .any(|block| block.text.contains("Keep exact Rust memory.")));
}

#[test]
fn explicit_protobuf_round_trip_keeps_encoder_and_rejects_unknown_variants() {
    let mut input = request(ContextModelProviderConfig {
        provider_kind: ContextProviderKind::Ollama,
        embedding_model: "selected-encoder".into(),
        fallback_provider: Some(Box::new(ContextModelProviderConfig::default())),
        ..Default::default()
    });
    input.query = "query retained".into();
    let bytes = ingest_wire::encode_request(input.clone()).unwrap();
    assert_eq!(ingest_wire::decode_request(&bytes).unwrap(), input);
    assert!(ingest_wire::decode_request(b"{}").is_err());
    let mut wire = crate::sdk::v1::ContextExplicitIngestRequest::decode(bytes.as_slice()).unwrap();
    wire.protocol_version = 2;
    assert!(ingest_wire::decode_request(&wire.encode_to_vec()).is_err());
    wire.protocol_version = 1;
    wire.sources[0].source_kind = 999;
    assert!(ingest_wire::decode_request(&wire.encode_to_vec()).is_err());
    wire.sources[0].source_kind = 2;
    wire.provider.as_mut().unwrap().provider_kind = 999;
    assert!(ingest_wire::decode_request(&wire.encode_to_vec()).is_err());
}

#[test]
fn explicit_embedding_fallback_records_its_actual_model_identity() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        serve_listener(listener, move |request| {
            assert_eq!(request.path, "/v1/embeddings");
            let body: Value = serde_json::from_slice(&request.body).unwrap();
            if body["model"] == "primary-encoder" {
                return json_response(503, &json!({"error":"offline"}));
            }
            assert_eq!(body["model"], "fallback-encoder");
            let data = body["input"]
                .as_array()
                .unwrap()
                .iter()
                .enumerate()
                .map(|(index, _)| json!({"index":index,"embedding":[1.0,0.0,0.0,0.0]}))
                .collect::<Vec<_>>();
            json_response(200, &json!({"data":data}))
        })
        .unwrap();
    });
    let fallback = ContextModelProviderConfig {
        provider_name: "fallback".into(),
        provider_kind: ContextProviderKind::OpenAiCompatible,
        base_url: format!("http://{addr}/v1"),
        embedding_model: "fallback-encoder".into(),
        mock_mode: false,
        max_retries: 0,
        ..Default::default()
    };
    let provider = ContextModelProviderConfig {
        provider_name: "primary".into(),
        embedding_model: "primary-encoder".into(),
        fallback_provider: Some(Box::new(fallback.clone())),
        ..fallback
    };
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let report = ingest_context_without_inference(&engine, request(provider));
    assert!(report.status.ok, "{:?}", report.status);
    for extract in report.extracts {
        assert!(extract.embedding_generation.fallback_used);
        assert_eq!(
            extract.embedding_generation.embedding_model,
            "fallback-encoder"
        );
        assert_eq!(
            extract.node.embedding_model_hash,
            context_embedding_model_hash("fallback-encoder")
        );
        assert_eq!(
            extract.event.embedding_model_hash,
            extract.node.embedding_model_hash
        );
    }
}

#[test]
fn explicit_protobuf_ack_requires_complete_success() {
    assert!(
        ingest_wire::validate_response(&ingest_wire::encode_response(Status::ok(), 2, 0), 2)
            .is_ok()
    );
    for bytes in [
        Vec::new(),
        b"{}".to_vec(),
        ingest_wire::encode_response(Status::ok(), 1, 0),
        ingest_wire::encode_response(Status::ok(), 2, 1),
        ingest_wire::encode_response(Status::error("failed", "write failed"), 2, 0),
    ] {
        assert!(ingest_wire::validate_response(&bytes, 2).is_err());
    }
    let mut ack = crate::sdk::v1::ContextExplicitIngestResponse::decode(
        ingest_wire::encode_response(Status::ok(), 2, 0).as_slice(),
    )
    .unwrap();
    ack.protocol_version = 2;
    assert!(ingest_wire::validate_response(&ack.encode_to_vec(), 2).is_err());
}

#[test]
fn explicit_wire_limits_source_slots_before_message_allocation() {
    let mut oversized = vec![8, 1];
    for _ in 0..=ingest_wire::EXPLICIT_MAX_SOURCES {
        oversized.extend_from_slice(&[34, 0]);
    }
    let error = ingest_wire::decode_request(&oversized).unwrap_err();
    assert!(error.message.contains("source limit"));
    let mut input = request(ContextModelProviderConfig::default());
    input.sources.resize(
        ingest_wire::EXPLICIT_MAX_SOURCES + 1,
        input.sources[0].clone(),
    );
    assert!(ingest_wire::encode_request(input)
        .unwrap_err()
        .message
        .contains("source limit"));
    assert!(
        ingest_wire::decode_request(&vec![0; ingest_wire::EXPLICIT_MAX_BODY_BYTES + 1])
            .unwrap_err()
            .message
            .contains("body limit")
    );
}

#[test]
fn explicit_rpc_ack_counts_all_windows_and_retains_partial_failures() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let mut input = request(ContextModelProviderConfig::default());
    let template = input.sources[0].clone();
    input.sources = (0..65)
        .map(|index| ContextExtractRequest {
            source_id: format!("window-{index}"),
            ..template.clone()
        })
        .collect();
    let response = ingest_wire::execute_request(&engine, input.clone());
    ingest_wire::validate_response(&response, 65).unwrap();
    input.sources[0].body = "x".repeat(ContextWorkflowPolicy::default().max_extract_body_bytes + 1);
    let response = crate::sdk::v1::ContextExplicitIngestResponse::decode(
        ingest_wire::execute_request(&engine, input).as_slice(),
    )
    .unwrap();
    assert_eq!((response.accepted, response.failed), (64, 1));
    assert!(!response.status.unwrap().ok);
}

#[test]
#[ignore = "requires an installed Ollama encoder; writes only an isolated temporary store"]
fn explicit_live_ollama_encoder_needs_no_installed_chat_model() {
    let model = std::env::var("TS_TEST_OLLAMA_EMBEDDING_MODEL")
        .expect("set TS_TEST_OLLAMA_EMBEDDING_MODEL to an installed encoder");
    let provider = ContextModelProviderConfig {
        provider_name: "live-explicit-encoder".into(),
        provider_kind: ContextProviderKind::Ollama,
        base_url: std::env::var("TS_TEST_OLLAMA_BASE_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:11434".into()),
        model: "temporalstore-unused-chat-model".into(),
        embedding_model: model.clone(),
        mock_mode: false,
        timeout_ms: 30_000,
        max_retries: 0,
        ..Default::default()
    };
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let started = std::time::Instant::now();
    let report = ingest_context_without_inference(&engine, request(provider));
    assert!(report.status.ok, "{:?}", report.status);
    assert_eq!(report.accepted, 2);
    for extract in &report.extracts {
        assert!(!extract.embedding_generation.mock_mode);
        assert_eq!(extract.embedding_generation.embedding_model, model);
        assert_eq!(extract.extracted_entity_count, 0);
        assert!(!extract.node.vector.is_empty());
        assert!(extract.node.vector.iter().all(|value| value.is_finite()));
        assert!(extract.node.vector.iter().any(|value| value.abs() > 0.0));
        assert_eq!(
            extract.node.embedding_model_hash,
            context_embedding_model_hash(&model)
        );
        assert_eq!(extract.node.vector, extract.event.vector);
    }
    println!(
        "explicit_live_ingest_ms={} dimensions={}",
        started.elapsed().as_millis(),
        report.extracts[0].node.vector.len()
    );
}
