// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::http::{json_response, serve_listener};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const BAD_BODY: &str = "The backup window starts every weekday at 03:00.";
const GOOD_BODY: &str = "The invoices are retained for seven years.";

fn source(provider: &ContextModelProviderConfig, body: &str) -> ContextExtractRequest {
    ContextExtractRequest {
        shard_id: 1,
        tenant_hash: 42,
        source_kind: ContextSourceKind::Chat,
        source_id: "source".into(),
        title: "Operations notes".into(),
        body: body.into(),
        timestamp_ms: 1,
        provider: provider.clone(),
    }
}

#[test]
fn repairs_only_title_echoes_without_changing_valid_model_content() {
    let request = source(&ContextModelProviderConfig::default(), GOOD_BODY);
    let pair = repair_label_only_summaries(" operations NOTES ".into(), GOOD_BODY.into(), &request)
        .unwrap();
    assert_eq!(pair, (GOOD_BODY.into(), GOOD_BODY.into()));
    let pair =
        repair_label_only_summaries(GOOD_BODY.into(), request.title.clone(), &request).unwrap();
    assert_eq!(pair, (GOOD_BODY.into(), GOOD_BODY.into()));
    let short = "Invoices have a seven-year retention period.".to_string();
    let pair = repair_label_only_summaries(short.clone(), GOOD_BODY.into(), &request).unwrap();
    assert_eq!(pair, (short, GOOD_BODY.into()));
}

#[test]
fn rejects_two_title_echoes_but_keeps_title_only_source_evidence() {
    let mut request = source(&ContextModelProviderConfig::default(), BAD_BODY);
    let error = repair_label_only_summaries(request.title.clone(), request.title.clone(), &request)
        .unwrap_err();
    assert_eq!(error.code, "model_provider_label_only_summary");
    request.body = request.title.clone();
    assert!(
        repair_label_only_summaries(request.title.clone(), request.title.clone(), &request).is_ok()
    );
    request.body.clear();
    assert!(
        repair_label_only_summaries(request.title.clone(), request.title.clone(), &request).is_ok()
    );
}

#[test]
fn short_level_repair_keeps_the_long_model_summary_intact() {
    let request = source(&ContextModelProviderConfig::default(), GOOD_BODY);
    let long = (0..64)
        .map(|index| format!("fact{index}"))
        .collect::<Vec<_>>()
        .join(" ");
    let (short, retained) =
        repair_label_only_summaries(request.title.clone(), long.clone(), &request).unwrap();
    assert_eq!(short.split_whitespace().count(), 32);
    assert_eq!(retained, long);
    let unicode = "\u{754c}".repeat(512);
    let (short, retained) =
        repair_label_only_summaries(request.title.clone(), unicode.clone(), &request).unwrap();
    assert_eq!(short.chars().count(), MAX_SUMMARY_L0_CHARS);
    assert_eq!(retained, unicode);
}

fn endpoint(
    kind: ContextProviderKind,
    reject_bad: bool,
) -> (ContextModelProviderConfig, Arc<AtomicUsize>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    std::thread::spawn(move || {
        serve_listener(listener, move |request| {
            observed.fetch_add(1, Ordering::Relaxed);
            let request: Value = serde_json::from_slice(&request.body).unwrap();
            let prompt = request["messages"][1]["content"].as_str().unwrap();
            let output = |source: &Value| {
                let bad = reject_bad && source["body"] == BAD_BODY;
                let mut output = serde_json::json!({
                    "l0":source["title"],
                    "l1":if bad { &source["title"] } else { &source["body"] },
                    "entities":[{"kind":"fact","name":"retention","value":"seven years"}],
                });
                if let Some(index) = source.get("source_index") {
                    output["source_index"] = index.clone();
                }
                output
            };
            let content =
                if let Some(sources) = prompt.strip_prefix(CONTEXT_SUMMARY_BATCH_PROMPT_PREFIX) {
                    let sources: Vec<Value> = serde_json::from_str(sources).unwrap();
                    serde_json::json!({"items":sources.iter().map(output).collect::<Vec<_>>()})
                } else {
                    output(&serde_json::from_str::<Value>(prompt).unwrap())
                }
                .to_string();
            json_response(
                200,
                &serde_json::json!({
                    "message":{"content":content},
                    "choices":[{"message":{"content":content}}],
                }),
            )
        })
        .unwrap();
    });
    (
        ContextModelProviderConfig {
            provider_name: "summary-fixture".into(),
            provider_kind: kind,
            base_url: format!("http://{addr}"),
            model: "fixture-model".into(),
            mock_mode: false,
            timeout_ms: 5_000,
            max_retries: 0,
            ..Default::default()
        },
        calls,
    )
}

#[test]
fn both_provider_protocols_repair_or_reject_title_only_summaries() {
    for kind in [
        ContextProviderKind::Ollama,
        ContextProviderKind::OpenAiCompatible,
    ] {
        let (provider, calls) = endpoint(kind, true);
        let bad = source(&provider, BAD_BODY);
        let error = context_summaries_for_extract(&provider, &bad).unwrap_err();
        assert_eq!(error.code, "model_provider_label_only_summary");
        let good = source(&provider, GOOD_BODY);
        let result = context_summaries_for_extract(&provider, &good).unwrap();
        assert_eq!(result.l0, GOOD_BODY);
        assert_eq!(result.l1, GOOD_BODY);
        assert_eq!(result.entities.len(), 1);
        assert_eq!(result.entities[0].value, "seven years");
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }
}

#[test]
fn invalid_source_does_not_discard_its_valid_batch_neighbor() {
    for limit in [1, 2] {
        let (provider, calls) = endpoint(ContextProviderKind::Ollama, true);
        let bad = source(&provider, BAD_BODY);
        let good = source(&provider, GOOD_BODY);
        let results =
            context_summaries_for_extract_batch_with_limit(&provider, &[&bad, &good], limit);
        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0].as_ref().unwrap_err().code,
            "model_provider_label_only_summary"
        );
        assert_eq!(results[1].as_ref().unwrap().l0, GOOD_BODY);
        assert_eq!(results[1].as_ref().unwrap().l1, GOOD_BODY);
        assert_eq!(results[1].as_ref().unwrap().entities.len(), 1);
        assert_eq!(
            calls.load(Ordering::Relaxed),
            2,
            "retry only the unusable source"
        );
    }
}
