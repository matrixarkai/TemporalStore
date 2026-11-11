// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::http::{json_response, serve_listener};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone, Copy)]
enum ResponseMode {
    Unavailable,
    Slow,
    Partial,
    Malformed,
    MalformedEnvelope,
}

fn endpoint(mode: ResponseMode) -> (ContextModelProviderConfig, Arc<AtomicUsize>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    std::thread::spawn(move || {
        serve_listener(listener, move |request| {
            assert_eq!(request.path, "/api/chat");
            let ordinal = observed.fetch_add(1, Ordering::Relaxed);
            if ordinal == 0 && matches!(mode, ResponseMode::MalformedEnvelope) {
                return (200, b"{invalid".to_vec());
            }
            if matches!(mode, ResponseMode::Unavailable | ResponseMode::Slow) {
                if matches!(mode, ResponseMode::Slow) {
                    std::thread::sleep(std::time::Duration::from_millis(200));
                }
                return json_response(503, &json!({"error":"provider unavailable"}));
            }
            let content = if ordinal == 0 {
                match mode {
                    ResponseMode::Partial => json!({"items":[{
                        "source_index":0,"l0":"batch source","l1":"batch synthesis",
                        "entities":[]
                    }]})
                    .to_string(),
                    ResponseMode::Malformed => "{invalid".into(),
                    ResponseMode::Unavailable
                    | ResponseMode::Slow
                    | ResponseMode::MalformedEnvelope => unreachable!(),
                }
            } else {
                json!({"l0":"single source","l1":"single synthesis","entities":[]}).to_string()
            };
            json_response(200, &json!({"message":{"content":content}}))
        })
        .unwrap();
    });
    (
        ContextModelProviderConfig {
            provider_name: "local-model".into(),
            provider_kind: ContextProviderKind::Ollama,
            base_url: format!("http://{addr}"),
            model: "test-generator".into(),
            embedding_model: "test-encoder".into(),
            timeout_ms: 2_000,
            max_retries: 0,
            mock_mode: false,
            ..ContextModelProviderConfig::default()
        },
        calls,
    )
}

fn requests(provider: &ContextModelProviderConfig, count: usize) -> Vec<ContextExtractRequest> {
    (0..count)
        .map(|index| ContextExtractRequest {
            shard_id: 1,
            tenant_hash: 42,
            source_kind: ContextSourceKind::Chat,
            source_id: format!("source-{index}"),
            title: "deployment".into(),
            body: "The cedar deployment runs on Friday.".into(),
            timestamp_ms: 1000 + index as u64,
            provider: provider.clone(),
        })
        .collect()
}

#[test]
fn unavailable_summary_batch_does_not_fan_out_into_individual_calls() {
    let (provider, calls) = endpoint(ResponseMode::Unavailable);
    let requests = requests(&provider, 64);
    let borrowed = requests.iter().collect::<Vec<_>>();
    let results = context_summaries_for_extract_batch(&provider, &borrowed);
    assert_eq!(results.len(), requests.len());
    for result in results {
        let error = result
            .err()
            .expect("provider failure must not become synthetic success");
        assert_eq!(error.code, "model_provider_request_failed");
    }
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[test]
fn summary_batch_distinguishes_streamed_io_from_malformed_json() {
    use crate::http::HttpError;
    use crate::llm_context::OllamaError;
    let timeout = serde_json::Error::io(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "streamed response timed out",
    ));
    assert!(summary_batch_transport_failure(&OllamaError::Http(
        HttpError::Json(timeout)
    )));
    let malformed = serde_json::from_str::<Value>("{invalid").unwrap_err();
    assert!(!summary_batch_transport_failure(&OllamaError::Http(
        HttpError::Json(malformed)
    )));
    let malformed = serde_json::from_str::<Value>("{invalid").unwrap_err();
    assert!(!summary_batch_transport_failure(&OllamaError::ContentJson(
        malformed
    )));
}

#[test]
fn timed_out_summary_batch_does_not_start_individual_model_requests() {
    let (mut provider, calls) = endpoint(ResponseMode::Slow);
    provider.timeout_ms = 25;
    let requests = requests(&provider, 8);
    let borrowed = requests.iter().collect::<Vec<_>>();
    let results = context_summaries_for_extract_batch(&provider, &borrowed);
    assert_eq!(results.len(), 8);
    assert!(results.iter().all(Result::is_err));
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[test]
fn partial_summary_batch_retries_only_the_missing_source() {
    let (provider, calls) = endpoint(ResponseMode::Partial);
    let requests = requests(&provider, 2);
    let borrowed = requests.iter().collect::<Vec<_>>();
    let results = context_summaries_for_extract_batch(&provider, &borrowed);
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].as_ref().unwrap().l0, "batch source");
    assert_eq!(results[1].as_ref().unwrap().l0, "single source");
    assert_eq!(calls.load(Ordering::Relaxed), 2);
}

#[test]
fn malformed_summary_content_keeps_individual_source_recovery() {
    for mode in [ResponseMode::Malformed, ResponseMode::MalformedEnvelope] {
        let (provider, calls) = endpoint(mode);
        let requests = requests(&provider, 2);
        let borrowed = requests.iter().collect::<Vec<_>>();
        let results = context_summaries_for_extract_batch(&provider, &borrowed);
        assert_eq!(results.len(), 2);
        assert!(results
            .iter()
            .all(|result| result.as_ref().unwrap().l0 == "single source"));
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }
}

#[test]
fn failed_summary_batch_still_uses_an_explicit_fallback_provider() {
    let (mut provider, calls) = endpoint(ResponseMode::Unavailable);
    provider.fallback_provider = Some(Box::new(ContextModelProviderConfig {
        provider_name: "configured-fallback".into(),
        ..ContextModelProviderConfig::default()
    }));
    let requests = requests(&provider, 4);
    let borrowed = requests.iter().collect::<Vec<_>>();
    let results = context_extract_summaries_with_fallback_batch(&borrowed);
    assert_eq!(results.len(), 4);
    for result in results {
        let summary = result.unwrap();
        assert!(summary.provider.mock_mode);
        assert!(summary
            .provider
            .provider_name
            .contains("configured-fallback"));
    }
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}
