// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn source(provider: &ContextModelProviderConfig, id: &str, body: &str) -> ContextExtractRequest {
    ContextExtractRequest {
        shard_id: 1,
        tenant_hash: 42,
        source_kind: ContextSourceKind::Chat,
        source_id: id.into(),
        title: "Cedar deployment".into(),
        body: body.into(),
        timestamp_ms: 1000,
        provider: provider.clone(),
    }
}

fn entity_text(summary: &ContextExtractSummaries) -> String {
    summary
        .entities
        .iter()
        .map(|entity| format!("{} {}", entity.name, entity.value))
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

#[test]
#[ignore = "requires an installed local Ollama chat model; no stored memories are written"]
fn live_ollama_summary_keeps_durable_facts_in_entities() {
    let model = std::env::var("TS_TEST_OLLAMA_CHAT_MODEL")
        .expect("set TS_TEST_OLLAMA_CHAT_MODEL to an installed model");
    let provider = ContextModelProviderConfig {
        provider_name: "live-local-model".into(),
        provider_kind: ContextProviderKind::Ollama,
        base_url: std::env::var("TS_TEST_OLLAMA_BASE_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:11434".into()),
        model,
        timeout_ms: 30_000,
        max_retries: 0,
        mock_mode: false,
        ..ContextModelProviderConfig::default()
    };
    let preference = source(&provider, "preference", "I prefer Rust for Cedar services.");
    let started = std::time::Instant::now();
    let single = context_summaries_for_extract(&provider, &preference).unwrap();
    assert!(single.l0.to_lowercase().contains("rust"), "{single:?}");
    assert!(single.l1.to_lowercase().contains("rust"), "{single:?}");
    assert!(entity_text(&single).contains("rust"), "{single:?}");
    assert!(single
        .entities
        .iter()
        .any(|entity| entity.kind == "preference"));
    println!("single_summary_ms={}", started.elapsed().as_millis());

    let deployment = source(
        &provider,
        "deployment",
        "The Cedar deployment is on Friday and uses TemporalStore.",
    );
    let started = std::time::Instant::now();
    let results = context_summaries_for_extract_batch(&provider, &[&deployment, &preference]);
    assert_eq!(results.len(), 2);
    let deployment_summary = results[0].as_ref().unwrap();
    let preference_summary = results[1].as_ref().unwrap();
    let facts = entity_text(deployment_summary);
    assert!(facts.contains("friday"), "{deployment_summary:?}");
    assert!(facts.contains("temporalstore"), "{deployment_summary:?}");
    assert!(
        deployment_summary.l1.to_lowercase().contains("friday"),
        "{deployment_summary:?}"
    );
    assert!(
        deployment_summary
            .l1
            .to_lowercase()
            .contains("temporalstore"),
        "{deployment_summary:?}"
    );
    assert!(!facts.contains("rust"), "{deployment_summary:?}");
    assert!(
        entity_text(preference_summary).contains("rust"),
        "{preference_summary:?}"
    );
    assert!(
        preference_summary
            .entities
            .iter()
            .any(|entity| entity.kind == "preference"),
        "{preference_summary:?}"
    );
    assert!(
        !entity_text(preference_summary).contains("friday"),
        "{preference_summary:?}"
    );
    println!("batch_summary_ms={}", started.elapsed().as_millis());

    let constraints = source(
        &provider,
        "constraints",
        "Use TemporalStore code only for memory ingestion and retrieval. Store repositories in Ubuntu local folders. Optimize CPU and memory usage for large corpora.",
    );
    let started = std::time::Instant::now();
    let summary = context_summaries_for_extract(&provider, &constraints).unwrap();
    let supported = entity_text(&summary);
    for fact in ["temporalstore", "ubuntu", "cpu", "memory"] {
        assert!(supported.contains(fact), "missing {fact}: {summary:?}");
    }
    for definition in [
        "routing abstract",
        "semantic synthesis",
        "field definitions",
    ] {
        assert!(
            !summary.l0.contains(definition) && !summary.l1.contains(definition),
            "{summary:?}"
        );
    }
    assert!(!summary.l0.trim_start().starts_with('{'), "{summary:?}");
    assert!(!summary.l1.trim_start().starts_with('{'), "{summary:?}");
    println!("constraint_summary_ms={}", started.elapsed().as_millis());
}
