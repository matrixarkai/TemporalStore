// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Context model-provider calls (summaries/embeddings, OpenAI-compatible), split from context_workflow.rs.
use super::*;

#[derive(Debug, Clone)]
pub(crate) struct ContextExtractSummaries {
    pub(crate) status: Status,
    pub(crate) provider: ContextModelProviderConfig,
    pub(crate) l0: String,
    pub(crate) l1: String,
    pub(crate) l2_ref: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct OpenAiChatCompletionRequest<'a> {
    model: &'a str,
    messages: Vec<OpenAiChatMessage<'a>>,
    temperature: f32,
}

#[derive(Debug, Serialize)]
pub(crate) struct OpenAiChatMessage<'a> {
    role: &'a str,
    content: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct OpenAiEmbeddingRequest<'a> {
    model: &'a str,
    input: Vec<&'a str>,
}

/// When set (`MATRIXARK_REQUIRE_MODEL_SUMMARIES=1`), context summaries must come from a
/// real model: mock/deterministic providers are rejected loudly and, if the model omits
/// l0/l1, the model's own output is used rather than the title/body string heuristic.
/// Off by default so CI/dev without credentials still runs deterministically.
pub(crate) fn context_require_model_summaries() -> bool {
    std::env::var("MATRIXARK_REQUIRE_MODEL_SUMMARIES")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// When set (`MATRIXARK_REQUIRE_MODEL_EMBEDDINGS=1`), embeddings must come from a real
/// model: a mock/deterministic provider is rejected loudly instead of emitting hash vectors.
pub(crate) fn context_require_model_embeddings() -> bool {
    std::env::var("MATRIXARK_REQUIRE_MODEL_EMBEDDINGS")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

pub(crate) fn context_summaries_for_extract(
    provider: &ContextModelProviderConfig,
    request: &ContextExtractRequest,
) -> Result<ContextExtractSummaries, Status> {
    let provider = normalize_provider(provider.clone());
    if provider.mock_mode || provider.provider_kind == ContextProviderKind::Mock {
        if context_require_model_summaries() {
            return Err(Status::error(
                "model_required_but_provider_is_mock",
                "MATRIXARK_REQUIRE_MODEL_SUMMARIES is set but the context provider is mock/deterministic; configure a real OpenAI-compatible (OSS) provider with mock_mode=false",
            ));
        }
        return Ok(mock_context_summaries(provider, request));
    }
    match provider.provider_kind {
        ContextProviderKind::OpenAiCompatible => {
            let content = call_openai_compatible_context_provider(&provider, request)?;
            let (l0, l1) = parse_provider_summary_content(&content, request);
            let l2_ref = format!(
                "tsctx://tenant/{}/model/{}/source/{}",
                request.tenant_hash, provider.provider_name, request.source_id
            );
            Ok(ContextExtractSummaries {
                status: Status::ok(),
                provider,
                l0,
                l1,
                l2_ref,
            })
        }
        ContextProviderKind::Mock => Ok(mock_context_summaries(provider, request)),
    }
}

/// Backfill helper: compute REAL model embeddings for a batch of already-stored
/// context node/event texts, reusing the exact provider path the live extract
/// path uses -- request batching, `MATRIXARK_REQUIRE_MODEL_EMBEDDINGS`
/// enforcement, and response shape/finiteness validation. Returns one vector per
/// input text, in the same order.
///
/// This exists so an out-of-band embed-backfill (the `context_embed_backfill`
/// bin) can attach semantic vectors to `raw_first` bulk stores WITHOUT rerunning
/// summary extraction or changing live-ingest behavior.
pub fn context_backfill_embeddings(
    provider: &ContextModelProviderConfig,
    texts: &[&str],
) -> Result<Vec<Vec<f32>>, Status> {
    if texts.is_empty() {
        return Ok(Vec::new());
    }
    let inputs: Vec<(&str, u64, u32, &str)> = texts
        .iter()
        .map(|text| ("node_l0", 0u64, 1u32, *text))
        .collect();
    let (vectors, _report) = context_embeddings_for_extract(provider, &inputs)?;
    Ok(vectors)
}

pub(crate) fn context_embeddings_for_extract(
    provider: &ContextModelProviderConfig,
    inputs: &[(&str, u64, u32, &str)],
) -> Result<(Vec<Vec<f32>>, ContextEmbeddingGenerationReport), Status> {
    let provider = normalize_provider(provider.clone());
    if provider.mock_mode || provider.provider_kind == ContextProviderKind::Mock {
        if context_require_model_embeddings() {
            return Err(Status::error(
                "model_required_but_provider_is_mock",
                "MATRIXARK_REQUIRE_MODEL_EMBEDDINGS is set but the context provider is mock/deterministic; configure a real OpenAI-compatible (OSS) embedding provider with mock_mode=false",
            ));
        }
        let vectors = inputs
            .iter()
            .map(|(_, _, _, text)| deterministic_context_embedding(&provider.embedding_model, text))
            .collect::<Vec<_>>();
        let vector_dimension = vectors.first().map(Vec::len).unwrap_or_default();
        return Ok((
            vectors,
            ContextEmbeddingGenerationReport {
                status: Status::ok(),
                provider_name: provider.provider_name,
                provider_kind: provider.provider_kind,
                embedding_model: provider.embedding_model,
                vector_dimension,
                requested_vector_count: inputs.len(),
                generated_vector_count: inputs.len(),
                batch_count: usize::from(!inputs.is_empty()),
                live_call_count: 0,
                fallback_used: false,
                mock_mode: true,
                production_evidence_ready: false,
            },
        ));
    }

    let vectors = match provider.provider_kind {
        ContextProviderKind::OpenAiCompatible => {
            call_openai_compatible_embedding_provider(&provider, inputs)?
        }
        ContextProviderKind::Mock => inputs
            .iter()
            .map(|(_, _, _, text)| deterministic_context_embedding(&provider.embedding_model, text))
            .collect(),
    };
    let vector_dimension = vectors.first().map(Vec::len).unwrap_or_default();
    if vectors.len() != inputs.len() || vector_dimension == 0 {
        return Err(Status::error(
            "embedding_provider_bad_response",
            format!(
                "embedding provider {} returned {} vectors for {} inputs",
                provider.provider_name,
                vectors.len(),
                inputs.len()
            ),
        ));
    }
    if !vectors.iter().all(|vector| {
        vector.len() == vector_dimension && vector.iter().all(|value| value.is_finite())
    }) {
        return Err(Status::error(
            "embedding_provider_bad_response",
            format!(
                "embedding provider {} returned inconsistent or non-finite vectors",
                provider.provider_name
            ),
        ));
    }
    Ok((
        vectors,
        ContextEmbeddingGenerationReport {
            status: Status::ok(),
            provider_name: provider.provider_name,
            provider_kind: provider.provider_kind,
            embedding_model: provider.embedding_model,
            vector_dimension,
            requested_vector_count: inputs.len(),
            generated_vector_count: inputs.len(),
            batch_count: usize::from(!inputs.is_empty()),
            live_call_count: usize::from(!inputs.is_empty()),
            fallback_used: false,
            mock_mode: false,
            production_evidence_ready: true,
        },
    ))
}

pub(crate) fn mock_context_summaries(
    provider: ContextModelProviderConfig,
    request: &ContextExtractRequest,
) -> ContextExtractSummaries {
    let node_hash = stable_hash64(&format!(
        "{}:{}:{}",
        request.tenant_hash, request.source_kind as u8, request.source_id
    ));
    ContextExtractSummaries {
        status: Status::ok(),
        provider,
        l0: summarize_l0(&request.title, &request.body),
        l1: summarize_l1(request.source_kind, &request.title, &request.body),
        l2_ref: format!(
            "tsctx://tenant/{}/node/{}/source/{}",
            request.tenant_hash, node_hash, request.source_id
        ),
    }
}

pub(crate) fn call_openai_compatible_embedding_provider(
    provider: &ContextModelProviderConfig,
    inputs: &[(&str, u64, u32, &str)],
) -> Result<Vec<Vec<f32>>, Status> {
    let (addr, path_prefix) = parse_openai_compatible_base_url(&provider.base_url)?;
    let api_key = if provider.api_key_env.trim().is_empty() {
        None
    } else {
        Some(std::env::var(&provider.api_key_env).map_err(|_| {
            Status::error(
                "embedding_provider_auth_missing",
                format!(
                    "embedding provider {} requires environment variable {}",
                    provider.provider_name, provider.api_key_env
                ),
            )
        })?)
    };
    let embedding_request = OpenAiEmbeddingRequest {
        model: &provider.embedding_model,
        input: inputs.iter().map(|(_, _, _, text)| *text).collect(),
    };
    let headers = api_key
        .as_ref()
        .map(|key| format!("Authorization: Bearer {key}\r\n"))
        .unwrap_or_default();
    let path = format!("{}/embeddings", path_prefix.trim_end_matches('/'));
    let response: Value = post_json_with_options_and_headers(
        &addr,
        &path,
        &embedding_request,
        &headers,
        HttpRequestOptions {
            connect_timeout_ms: provider.timeout_ms.min(5_000).max(1),
            io_timeout_ms: provider.timeout_ms.max(1),
            max_retries: provider.max_retries,
        },
    )
    .map_err(|err| {
        Status::error(
            "embedding_provider_request_failed",
            format!(
                "embedding provider {} request failed: {err}",
                provider.provider_name
            ),
        )
    })?;
    response["data"]
        .as_array()
        .ok_or_else(|| {
            Status::error(
                "embedding_provider_bad_response",
                format!(
                    "embedding provider {} response missing data",
                    provider.provider_name
                ),
            )
        })?
        .iter()
        .map(|item| {
            item["embedding"]
                .as_array()
                .ok_or_else(|| {
                    Status::error(
                        "embedding_provider_bad_response",
                        format!(
                            "embedding provider {} response missing data[].embedding",
                            provider.provider_name
                        ),
                    )
                })?
                .iter()
                .map(|value| {
                    value.as_f64().map(|value| value as f32).ok_or_else(|| {
                        Status::error(
                            "embedding_provider_bad_response",
                            format!(
                                "embedding provider {} returned a non-float embedding value",
                                provider.provider_name
                            ),
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()
}

pub(crate) fn call_openai_compatible_context_provider(
    provider: &ContextModelProviderConfig,
    request: &ContextExtractRequest,
) -> Result<String, Status> {
    let (addr, path_prefix) = parse_openai_compatible_base_url(&provider.base_url)?;
    let api_key = if provider.api_key_env.trim().is_empty() {
        None
    } else {
        Some(std::env::var(&provider.api_key_env).map_err(|_| {
            Status::error(
                "model_provider_auth_missing",
                format!(
                    "context provider {} requires environment variable {}",
                    provider.provider_name, provider.api_key_env
                ),
            )
        })?)
    };
    // Data-adaptive richness hint so the model scales L1 to how much the source
    // actually carries -- thin sources get a minimal L1 (no token padding), rich
    // sources get a full synthesis. This keeps generation frugal.
    let l1_guidance = if context_source_warrants_l1(&request.body) {
        "l1: a richer semantic synthesis for tree-first retrieval -- fold the source's key facts, named entities, and CURRENT state into <=1200 characters. Prefer current state and resolved contradictions over stale facts."
    } else {
        "l1: this source is thin; keep l1 minimal (a single distilled fact line, <=220 characters). Do NOT pad or repeat l0."
    };
    let prompt = format!(
        "Generate hierarchical ContextNode traversal summaries for source_kind={:?}, source_id={}, title={}.\n\
         Return ONLY JSON with string fields l0 and l1.\n\
         l0: a compact routing abstract (<=220 characters) used for embedding-scored tree traversal.\n\
         {l1_guidance}\n\
         Do not invent facts beyond the source.\n\
         Source body:\n{}",
        request.source_kind, request.source_id, request.title, request.body
    );
    let completion_request = OpenAiChatCompletionRequest {
        model: &provider.model,
        messages: vec![
            OpenAiChatMessage {
                role: "system",
                content: "You generate TemporalStore ContextNode traversal summaries. \
                          Be faithful to the supplied source; prefer current state and resolved \
                          contradictions; never invent facts. l0 is a compact routing abstract; \
                          l1 is a richer synthesis whose length scales to the source's information."
                    .to_string(),
            },
            OpenAiChatMessage {
                role: "user",
                content: prompt,
            },
        ],
        temperature: 0.0,
    };
    let headers = api_key
        .as_ref()
        .map(|key| format!("Authorization: Bearer {key}\r\n"))
        .unwrap_or_default();
    let path = format!("{}/chat/completions", path_prefix.trim_end_matches('/'));
    let response: Value = post_json_with_options_and_headers(
        &addr,
        &path,
        &completion_request,
        &headers,
        HttpRequestOptions {
            connect_timeout_ms: provider.timeout_ms.min(5_000).max(1),
            io_timeout_ms: provider.timeout_ms.max(1),
            max_retries: provider.max_retries,
        },
    )
    .map_err(|err| {
        Status::error(
            "model_provider_request_failed",
            format!(
                "context provider {} request failed: {err}",
                provider.provider_name
            ),
        )
    })?;
    response["choices"]
        .as_array()
        .and_then(|choices| choices.first())
        .and_then(|choice| choice["message"]["content"].as_str())
        .map(str::to_string)
        .ok_or_else(|| {
            Status::error(
                "model_provider_bad_response",
                format!(
                    "context provider {} response missing choices[0].message.content",
                    provider.provider_name
                ),
            )
        })
}

pub(crate) fn parse_openai_compatible_base_url(base_url: &str) -> Result<(String, String), Status> {
    let Some(rest) = base_url.strip_prefix("http://") else {
        return Err(Status::error(
            "model_provider_url_unsupported",
            "context provider currently supports http:// OpenAI-compatible endpoints in the Rust-native local runtime",
        ));
    };
    let (host, path) = rest.split_once('/').unwrap_or((rest, "v1"));
    if host.trim().is_empty() {
        return Err(Status::error(
            "model_provider_url_invalid",
            "context provider base_url is missing a host",
        ));
    }
    Ok((host.to_string(), format!("/{}", path.trim_matches('/'))))
}

pub(crate) fn parse_provider_summary_content(
    content: &str,
    request: &ContextExtractRequest,
) -> (String, String) {
    // When models are required, a missing l0/l1 falls back to the model's own raw
    // content (still model-derived) rather than the title/body string heuristic.
    let require_model = context_require_model_summaries();
    if let Ok(value) = serde_json::from_str::<Value>(content) {
        let l0 = value["l0"]
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .map(|value| truncate_words(value, 32))
            .unwrap_or_else(|| {
                if require_model {
                    truncate_words(content, 32)
                } else {
                    summarize_l0(&request.title, &request.body)
                }
            });
        let l1 = value["l1"]
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .map(|value| truncate_words(value, 96))
            .unwrap_or_else(|| {
                if require_model {
                    truncate_words(content, 96)
                } else {
                    summarize_l1(request.source_kind, &request.title, &request.body)
                }
            });
        return (l0, l1);
    }
    (
        truncate_words(content, 32),
        format!(
            "kind={:?}; title={}; provider_summary={}",
            request.source_kind,
            request.title,
            truncate_words(content, 96)
        ),
    )
}
