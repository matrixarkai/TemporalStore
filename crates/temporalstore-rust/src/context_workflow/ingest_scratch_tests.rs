// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::http::{json_response, serve_listener};
use serde_json::json;
use std::sync::{Arc, Mutex};

#[test]
fn reused_source_scratch_preserves_rows_across_model_windows() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let inputs = Arc::new(Mutex::new(Vec::<Vec<String>>::new()));
    let captured = Arc::clone(&inputs);
    std::thread::spawn(move || {
        serve_listener(listener, move |request| {
            assert_eq!(request.path, "/v1/embeddings");
            let body: Value = serde_json::from_slice(&request.body).unwrap();
            let texts: Vec<String> = serde_json::from_value(body["input"].clone()).unwrap();
            let data = texts
                .iter()
                .enumerate()
                .map(|(index, text)| {
                    let topic: usize = text.rsplit_once(' ').unwrap().1.parse().unwrap();
                    json!({"index":index,"embedding":[topic + 1,1,0,0]})
                })
                .collect::<Vec<_>>();
            captured.lock().unwrap().push(texts);
            json_response(200, &json!({"data":data}))
        })
        .unwrap();
    });
    let provider = ContextModelProviderConfig {
        provider_name: "scratch-encoder".into(),
        provider_kind: ContextProviderKind::OpenAiCompatible,
        base_url: format!("http://{addr}/v1"),
        embedding_model: "scratch-encoder".into(),
        mock_mode: false,
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
    let report = ingest_context_without_inference(
        &engine,
        ContextIngestExtractRequest {
            shard_id: 1,
            tenant_hash: 42,
            sources: (0..65)
                .map(|index| ContextExtractRequest {
                    shard_id: 1,
                    tenant_hash: 42,
                    source_kind: ContextSourceKind::Chat,
                    source_id: format!("scratch-{index}"),
                    title: "user".into(),
                    body: format!("durable topic {}", index % 5),
                    timestamp_ms: 1000 + index,
                    provider: inherited_context_source_provider(),
                })
                .collect(),
            query: String::new(),
            start_time_ms: 0,
            end_time_ms: 2000,
            max_events: 10,
            provider,
        },
    );
    assert!(report.status.ok, "{:?}", report.status);
    assert_eq!((report.accepted, report.failed), (65, 0));
    assert_eq!(report.extracts.len(), 65);
    assert_eq!(
        *inputs.lock().unwrap(),
        vec![
            (0..5)
                .map(|topic| format!("durable topic {topic}"))
                .collect::<Vec<_>>(),
            vec!["durable topic 4".to_string()],
        ]
    );
    for (index, extract) in report.extracts.iter().enumerate() {
        assert_eq!(extract.source_ref, format!("scratch-{index}"));
        assert_eq!(extract.event.text, format!("durable topic {}", index % 5));
        assert_eq!(
            extract.event.vector,
            vec![(index % 5 + 1) as f32, 1.0, 0.0, 0.0]
        );
        assert_eq!(extract.node.vector, extract.event.vector);
        assert_eq!(extract.embedding_generation.requested_vector_count, 2);
        assert_eq!(extract.embedding_generation.generated_vector_count, 2);
    }
}
