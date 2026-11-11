// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::http::{json_response, serve_listener};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone, Copy)]
enum ResponseMode {
    Isolated,
    SlowSuccess,
    Unavailable,
    Slow,
    Partial,
    Malformed,
    MalformedEnvelope,
    CrossSource,
}

fn endpoint(mode: ResponseMode) -> (ContextModelProviderConfig, Arc<AtomicUsize>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    std::thread::spawn(move || {
        serve_listener(listener, move |request| {
            assert_eq!(request.path, "/api/chat");
            let body: Value = serde_json::from_slice(&request.body).unwrap();
            let schema = &body["format"];
            assert_eq!(schema["additionalProperties"], false);
            let batch_sources = body["messages"][1]["content"]
                .as_str()
                .and_then(|prompt| prompt.strip_prefix(CONTEXT_SUMMARY_BATCH_PROMPT_PREFIX))
                .map(|source_json| serde_json::from_str::<Vec<Value>>(source_json).unwrap());
            if let Some(sources) = batch_sources.as_ref() {
                let items = &schema["properties"]["items"];
                assert_eq!(items["minItems"], sources.len());
                assert_eq!(items["maxItems"], sources.len());
                assert_eq!(
                    items["items"]["properties"]["source_index"]["maximum"],
                    sources.len() - 1
                );
                assert_eq!(
                    items["items"]["properties"]["l1"]["maxLength"],
                    MAX_SUMMARY_L1_CHARS
                );
            } else {
                assert_eq!(schema["properties"]["l1"]["maxLength"], MAX_SUMMARY_L1_CHARS);
            }
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
            let content = if matches!(mode, ResponseMode::Isolated | ResponseMode::SlowSuccess) {
                if matches!(mode, ResponseMode::SlowSuccess) {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                assert!(batch_sources.is_none());
                let source: Value = serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
                assert!(source.get("source_id").is_none());
                assert!(source.get("source_index").is_none());
                json!({"l0":source["body"],"l1":source["body"],"entities":[]}).to_string()
            } else if ordinal == 0 {
                match mode {
                    ResponseMode::Partial => json!({"items":[{
                        "source_index":0,"l0":"batch source","l1":"batch synthesis",
                        "entities":[]
                    }]})
                    .to_string(),
                    ResponseMode::Malformed => "{invalid".into(),
                    ResponseMode::CrossSource => json!({"items":[
                        {"source_index":0,"l0":"first source","l1":batch_sources.as_ref().unwrap()[1]["body"],"entities":[]},
                        {"source_index":1,"l0":"second source","l1":"second synthesis","entities":[]}
                    ]}).to_string(),
                    ResponseMode::Isolated
                    | ResponseMode::SlowSuccess
                    | ResponseMode::Unavailable
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
fn cross_source_batch_copy_retries_only_the_contaminated_item() {
    let (provider, calls) = endpoint(ResponseMode::CrossSource);
    let mut sources = requests(&provider, 2);
    sources[1].body = "I prefer Rust services and local durable storage.".into();
    let results = context_summaries_for_extract_batch_with_limit(
        &provider,
        &sources.iter().collect::<Vec<_>>(),
        32,
    );
    assert_eq!(results[0].as_ref().unwrap().l0, "single source");
    assert_eq!(results[1].as_ref().unwrap().l0, "second source");
    assert_eq!(calls.load(Ordering::Relaxed), 2);
}

#[test]
fn verbatim_copy_guard_keeps_shared_evidence_and_checks_entity_fields() {
    let provider = ContextModelProviderConfig::default();
    let mut sources = requests(&provider, 2);
    let mut item = ContextSummaryBatchItem {
        source_index: 0,
        l0: sources[0].body.clone(),
        l1: sources[0].body.clone(),
        entities: Vec::new(),
    };
    assert!(!summary_batch_copies_other_source(
        &item,
        &[&sources[0], &sources[1]]
    ));
    sources[1].body = "I prefer Rust services and local durable storage.".into();
    item.entities.push(ContextExtractedEntity {
        kind: "fact".into(),
        name: "copied".into(),
        value: sources[1].body.clone(),
    });
    assert!(summary_batch_copies_other_source(
        &item,
        &[&sources[0], &sources[1]]
    ));
    let shared = sources[1].body.clone();
    sources[0].body.push_str(&shared);
    assert!(!summary_batch_copies_other_source(
        &item,
        &[&sources[0], &sources[1]]
    ));
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
    let results = context_summaries_for_extract_batch_with_limit(&provider, &borrowed, 32);
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
        let results = context_summaries_for_extract_batch_with_limit(&provider, &borrowed, 32);
        assert_eq!(results.len(), 2);
        assert!(results
            .iter()
            .all(|result| result.as_ref().unwrap().l0 == "single source"));
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }
}

#[test]
fn independent_sources_have_separate_model_inputs_and_keep_their_original_links() {
    let (provider, calls) = endpoint(ResponseMode::Isolated);
    let mut sources = requests(&provider, 2);
    sources[1].body = "I prefer Rust services and local durable storage.".into();
    let results = context_summaries_for_extract_batch_with_limit(
        &provider,
        &sources.iter().collect::<Vec<_>>(),
        1,
    );
    assert_eq!(results.len(), 2);
    for (result, source) in results.iter().zip(&sources) {
        let summary = result.as_ref().unwrap();
        assert_eq!(summary.l0, source.body);
        assert_eq!(summary.l1, source.body);
        assert!(summary
            .l2_ref
            .ends_with(&format!("/source/{}", source.source_id)));
    }
    assert_eq!(calls.load(Ordering::Relaxed), 2);
}

#[test]
fn isolated_source_calls_share_a_window_budget_instead_of_a_timeout_per_source() {
    let (mut provider, calls) = endpoint(ResponseMode::SlowSuccess);
    provider.timeout_ms = 25;
    let sources = requests(&provider, 32);
    let started = std::time::Instant::now();
    let results = context_summaries_for_extract_batch_with_limit(
        &provider,
        &sources.iter().collect::<Vec<_>>(),
        1,
    );
    assert_eq!(results.len(), 32);
    assert!(results.iter().any(Result::is_err));
    assert!(calls.load(Ordering::Relaxed) <= 3);
    assert!(started.elapsed() < std::time::Duration::from_secs(1));
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
