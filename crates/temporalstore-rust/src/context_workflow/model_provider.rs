// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Context model-provider calls for summaries and embeddings across supported provider protocols.
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::OnceLock;

use crate::llm_context::{BatchEmbedder, EmbeddingBatch, OllamaConfig, OllamaProvider};
use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};

use super::*;

const CONTEXT_SUMMARY_SYSTEM_PROMPT: &str = "You generate TemporalStore ContextNode traversal summaries. Be faithful to the supplied source; prefer current state and resolved contradictions; never invent facts. l0 is a compact routing abstract; l1 is a richer synthesis whose length scales to the source's information.";
const CONTEXT_SUMMARY_BATCH_PROMPT_PREFIX: &str = "Generate an independent hierarchical summary for every source below. Return one item for every source_index and preserve each ordinal exactly. l0 is a routing abstract of at most 220 characters. For rich_source=true, l1 is a semantic synthesis of at most 1200 characters; otherwise l1 is one distilled fact line of at most 220 characters. Do not combine facts between sources or invent facts. Sources: ";
const DEFAULT_EMBED_MAX_BATCH_ITEMS: usize = 128;
const DEFAULT_EMBED_MAX_INPUT_BYTES: usize = 64 * 1024;
const DEFAULT_EMBED_MAX_REQUEST_BYTES: usize = 512 * 1024;

#[derive(Debug, Clone)]
pub(crate) struct ContextExtractSummaries {
    pub(crate) status: Status,
    pub(crate) provider: ContextModelProviderConfig,
    pub(crate) l0: String,
    pub(crate) l1: String,
    pub(crate) l2_ref: String,
}

#[derive(Debug, Deserialize)]
struct ContextSummaryBatchOutput {
    items: Vec<ContextSummaryBatchItem>,
}

#[derive(Debug, Deserialize)]
struct ContextSummaryBatchItem {
    source_index: usize,
    l0: String,
    l1: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct ContextSummaryOutput {
    #[serde(default)]
    l0: Value,
    #[serde(default)]
    l1: Value,
}

#[derive(Serialize)]
struct ContextSummaryBatchSource<'a> {
    source_index: usize,
    source_kind: &'static str,
    source_id: &'a str,
    title: &'a str,
    rich_source: bool,
    body: &'a str,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ContextEmbeddingBatchLimits {
    pub(crate) max_items: usize,
    pub(crate) max_input_bytes: usize,
    pub(crate) max_request_bytes: usize,
}

pub(crate) fn context_embedding_batch_limits(
    provider: &ContextModelProviderConfig,
) -> ContextEmbeddingBatchLimits {
    if provider.provider_kind == ContextProviderKind::Ollama {
        let config = OllamaConfig::from_env();
        return ContextEmbeddingBatchLimits {
            max_items: config.max_batch_items.max(1),
            max_input_bytes: config.max_input_bytes.max(1),
            max_request_bytes: config.max_request_bytes.max(1),
        };
    }
    ContextEmbeddingBatchLimits {
        max_items: crate::env_flag::env_number(
            "MATRIXARK_EMBED_MAX_BATCH_ITEMS",
            DEFAULT_EMBED_MAX_BATCH_ITEMS,
        )
        .max(1),
        max_input_bytes: crate::env_flag::env_number(
            "MATRIXARK_EMBED_MAX_INPUT_BYTES",
            DEFAULT_EMBED_MAX_INPUT_BYTES,
        )
        .max(1),
        max_request_bytes: crate::env_flag::env_number(
            "MATRIXARK_EMBED_MAX_REQUEST_BYTES",
            DEFAULT_EMBED_MAX_REQUEST_BYTES,
        )
        .max(1),
    }
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

#[derive(Debug, Deserialize)]
struct OpenAiEmbeddingResponse {
    data: FlatOpenAiEmbeddingData,
}

#[derive(Debug, Default)]
struct FlatOpenAiEmbeddingData {
    values: Vec<f32>,
    row_ends: Vec<usize>,
}

impl<'de> Deserialize<'de> for FlatOpenAiEmbeddingData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct DataVisitor;

        impl<'de> Visitor<'de> for DataVisitor {
            type Value = FlatOpenAiEmbeddingData;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an array of embedding response items")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut data = FlatOpenAiEmbeddingData {
                    values: Vec::new(),
                    row_ends: Vec::with_capacity(sequence.size_hint().unwrap_or(0)),
                };
                while sequence
                    .next_element_seed(OpenAiEmbeddingItemSeed {
                        values: &mut data.values,
                    })?
                    .is_some()
                {
                    data.row_ends.push(data.values.len());
                }
                Ok(data)
            }
        }

        deserializer.deserialize_seq(DataVisitor)
    }
}

struct OpenAiEmbeddingItemSeed<'a> {
    values: &'a mut Vec<f32>,
}

impl<'de, 'a> DeserializeSeed<'de> for OpenAiEmbeddingItemSeed<'a> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(OpenAiEmbeddingItemVisitor {
            values: self.values,
        })
    }
}

struct OpenAiEmbeddingItemVisitor<'a> {
    values: &'a mut Vec<f32>,
}

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum OpenAiEmbeddingItemField {
    Embedding,
    #[serde(other)]
    Other,
}

impl<'de, 'a> Visitor<'de> for OpenAiEmbeddingItemVisitor<'a> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an embedding response item")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut found_embedding = false;
        while let Some(field) = map.next_key::<OpenAiEmbeddingItemField>()? {
            match field {
                OpenAiEmbeddingItemField::Embedding => {
                    if found_embedding {
                        return Err(serde::de::Error::duplicate_field("embedding"));
                    }
                    map.next_value_seed(OpenAiEmbeddingValuesSeed {
                        values: self.values,
                    })?;
                    found_embedding = true;
                }
                OpenAiEmbeddingItemField::Other => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        if !found_embedding {
            return Err(serde::de::Error::missing_field("embedding"));
        }
        Ok(())
    }
}

struct OpenAiEmbeddingValuesSeed<'a> {
    values: &'a mut Vec<f32>,
}

impl<'de, 'a> DeserializeSeed<'de> for OpenAiEmbeddingValuesSeed<'a> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ValuesVisitor<'a> {
            values: &'a mut Vec<f32>,
        }

        impl<'de, 'a> Visitor<'de> for ValuesVisitor<'a> {
            type Value = ();

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an array of floating-point embedding values")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                self.values.reserve(sequence.size_hint().unwrap_or(0));
                while let Some(value) = sequence.next_element::<f32>()? {
                    self.values.push(value);
                }
                Ok(())
            }
        }

        deserializer.deserialize_seq(ValuesVisitor {
            values: self.values,
        })
    }
}

/// When set (`MATRIXARK_REQUIRE_MODEL_SUMMARIES=1`), context summaries must come from a
/// real model: mock/deterministic providers are rejected loudly and, if the model omits
/// l0/l1, the model's own output is used rather than the title/body string heuristic.
/// Off by default so CI/dev without credentials still runs deterministically.
pub(crate) fn context_require_model_summaries() -> bool {
    // `1` and `true` were the only words that reached this; `yes` and `on` read as off, and so
    // did a value with a stray space. An operator who asks for a real model and silently gets
    // the heuristic has no way to tell from the outside.
    crate::env_flag::env_bool("MATRIXARK_REQUIRE_MODEL_SUMMARIES", false)
}

/// When set (`MATRIXARK_REQUIRE_MODEL_EMBEDDINGS=1`), embeddings must come from a real
/// model: a mock/deterministic provider is rejected loudly instead of emitting hash vectors.
pub(crate) fn context_require_model_embeddings() -> bool {
    // Same shape as its neighbour above, and the same silent answer: asking for a real model
    // with `on` left the mock provider emitting hash vectors.
    crate::env_flag::env_bool("MATRIXARK_REQUIRE_MODEL_EMBEDDINGS", false)
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
        ContextProviderKind::Ollama => {
            let output = call_ollama_context_provider(&provider, request)?;
            let (l0, l1) = normalize_ollama_summary(output, request);
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

pub(crate) fn context_summaries_for_extract_batch(
    provider: &ContextModelProviderConfig,
    requests: &[&ContextExtractRequest],
) -> Vec<Result<ContextExtractSummaries, Status>> {
    if requests.is_empty() {
        return Vec::new();
    }
    let provider = normalize_provider(provider.clone());
    if requests.len() == 1 {
        return vec![context_summaries_for_extract(&provider, requests[0])];
    }
    if provider.mock_mode || provider.provider_kind != ContextProviderKind::Ollama {
        return requests
            .iter()
            .map(|request| context_summaries_for_extract(&provider, request))
            .collect();
    }

    let client = match ollama_provider(&provider) {
        Ok(client) => client,
        Err(status) => return requests.iter().map(|_| Err(status.clone())).collect(),
    };
    let mut results: Vec<Option<Result<ContextExtractSummaries, Status>>> =
        std::iter::repeat_with(|| None).take(requests.len()).collect();
    let mut start = 0usize;
    while start < requests.len() {
        let (end, prompt) = ollama_summary_batch_prompt(&client, requests, start);
        let chunk = &requests[start..end];
        let output: Result<ContextSummaryBatchOutput, Status> = client
            .generate_structured_output(
                CONTEXT_SUMMARY_SYSTEM_PROMPT,
                &prompt,
                context_summary_batch_schema(),
            )
            .map_err(|err| {
                Status::error(
                    "model_provider_request_failed",
                    format!(
                        "context provider {} request failed: {err}",
                        provider.provider_name
                    ),
                )
            });

        if let Ok(output) = output {
            let mut seen = HashSet::with_capacity(output.items.len());
            for item in output.items {
                if item.source_index >= chunk.len() || !seen.insert(item.source_index) {
                    continue;
                }
                if item.l0.trim().is_empty() || item.l1.trim().is_empty() {
                    continue;
                }
                let request = chunk[item.source_index];
                results[start + item.source_index] = Some(Ok(ContextExtractSummaries {
                    status: Status::ok(),
                    provider: provider.clone(),
                    l0: truncate_words(&item.l0, 32),
                    l1: truncate_words(&item.l1, 96),
                    l2_ref: format!(
                        "tsctx://tenant/{}/model/{}/source/{}",
                        request.tenant_hash, provider.provider_name, request.source_id
                    ),
                }));
            }
        }

        // A malformed or partial batch response should not discard unrelated sources. Retry only
        // entries that did not receive a valid ordinal through the established single-source path.
        for (offset, request) in chunk.iter().enumerate() {
            let result_index = start + offset;
            if results[result_index].is_none() {
                results[result_index] = Some(context_summaries_for_extract(&provider, request));
            }
        }
        start = end;
    }

    results
        .into_iter()
        .map(|result| result.expect("every summary request is completed"))
        .collect()
}

/// The embedding provider a deployment has configured, read from the environment.
///
/// The engine could already call a real embedding endpoint, but only a caller holding a
/// `ContextModelProviderConfig` could reach it -- and the proxy had none, which is why a query
/// vector had to be computed elsewhere and sent in.
///
/// It reads the variables the rest of the deployment already sets rather than a second set of its
/// own: `MATRIXARK_EMBEDDING_API_BASE`, `MATRIXARK_EMBEDDING_MODEL`, and
/// `MATRIXARK_EMBEDDING_API_KEY_ENV` for the NAME of the variable holding a key, so a key is never
/// read from a value that might be logged.
///
/// With no base URL the provider stays deterministic, exactly as it was: a deployment that has not
/// configured an encoder does not silently acquire one, and one that requires a real model still
/// gets the error `MATRIXARK_REQUIRE_MODEL_EMBEDDINGS` exists to raise.
pub fn context_provider_from_env() -> ContextModelProviderConfig {
    if std::env::var("MATRIXARK_CONTEXT_PROVIDER")
        .ok()
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("ollama"))
    {
        let config = OllamaConfig::from_env();
        let embedding_model = std::env::var("MATRIXARK_EMBEDDING_MODEL")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| config.embedding_model.clone());
        return ContextModelProviderConfig {
            provider_name: "ollama".to_string(),
            provider_kind: ContextProviderKind::Ollama,
            base_url: config.base_url,
            model: config.extraction_model,
            embedding_model,
            timeout_ms: config.timeout_ms,
            max_retries: config.max_retries,
            mock_mode: false,
            ..ContextModelProviderConfig::default()
        };
    }

    // Two names for one endpoint, because two paths grew their own: the serving side and the
    // Python side read `MATRIXARK_EMBEDDING_API_BASE`, the embed drainer read
    // `MATRIXARK_EMBED_BASE_URL`. Both are honoured here so there is one constructor rather than
    // two spellings of the same deployment, and the newer name wins when both are set.
    let base_url = std::env::var("MATRIXARK_EMBEDDING_API_BASE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::env::var("MATRIXARK_EMBED_BASE_URL")
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
        .unwrap_or_default()
        .trim()
        .trim_end_matches('/')
        .to_string();
    let embedding_model = std::env::var("MATRIXARK_EMBEDDING_MODEL")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let api_key_env = std::env::var("MATRIXARK_EMBEDDING_API_KEY_ENV")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::env::var("MATRIXARK_EMBED_API_KEY_ENV")
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
        .unwrap_or_default()
        .trim()
        .to_string();

    let mut provider = ContextModelProviderConfig {
        base_url: base_url.clone(),
        api_key_env,
        // An endpoint that answers without a key is the local, self-hosted case, and it is the
        // configuration this is most often pointed at.
        mock_mode: base_url.is_empty(),
        ..ContextModelProviderConfig::default()
    };
    if !base_url.is_empty() {
        provider.provider_kind = ContextProviderKind::OpenAiCompatible;
    }
    if let Some(model) = embedding_model {
        provider.embedding_model = model;
    }
    provider
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
    let (batch, _report) = context_embeddings_for_extract(provider, &inputs)?;
    Ok(batch.into_rows())
}

pub(crate) fn context_embeddings_for_extract(
    provider: &ContextModelProviderConfig,
    inputs: &[(&str, u64, u32, &str)],
) -> Result<(EmbeddingBatch, ContextEmbeddingGenerationReport), Status> {
    let (unique_batch, input_rows, report) =
        context_embeddings_for_extract_compact(provider, inputs)?;
    if unique_batch.rows() == inputs.len() {
        return Ok((unique_batch, report));
    }

    let dimensions = unique_batch.dimensions;
    let mut values = Vec::with_capacity(inputs.len().saturating_mul(dimensions));
    for row in input_rows {
        let start = row.saturating_mul(dimensions);
        values.extend_from_slice(&unique_batch.values[start..start + dimensions]);
    }
    Ok((EmbeddingBatch { dimensions, values }, report))
}

/// Encodes each distinct text once and returns the logical input-to-vector row map separately.
/// Batch ingestion uses this form so duplicate source text never expands back into copied vectors.
pub(crate) fn context_embeddings_for_extract_compact(
    provider: &ContextModelProviderConfig,
    inputs: &[(&str, u64, u32, &str)],
) -> Result<(EmbeddingBatch, Vec<usize>, ContextEmbeddingGenerationReport), Status> {
    let provider = normalize_provider(provider.clone());
    if inputs.is_empty() {
        let (batch, report) = context_embeddings_for_extract_one_batch(&provider, inputs)?;
        return Ok((batch, Vec::new(), report));
    }

    // Encoder output depends on the configured model and text, not the caller's label or object
    // id. Preserve the first occurrence for the provider request and remember where every input
    // maps so repeated event bodies or summaries do not consume duplicate local-model work.
    let mut unique_by_text = HashMap::<&str, usize>::with_capacity(inputs.len());
    let mut unique_inputs = Vec::with_capacity(inputs.len());
    let mut input_rows = Vec::with_capacity(inputs.len());
    for input in inputs {
        let row = if let Some(row) = unique_by_text.get(input.3) {
            *row
        } else {
            let row = unique_inputs.len();
            unique_by_text.insert(input.3, row);
            unique_inputs.push(*input);
            row
        };
        input_rows.push(row);
    }

    let (unique_batch, mut report) =
        context_embeddings_for_unique_inputs(&provider, &unique_inputs)?;
    report.requested_vector_count = inputs.len();
    report.generated_vector_count = inputs.len();
    Ok((unique_batch, input_rows, report))
}

/// Encode an already deduplicated input plan while reporting its logical row count.
pub(crate) fn context_embeddings_for_prepared_unique(
    provider: &ContextModelProviderConfig,
    unique_inputs: &[(&str, u64, u32, &str)],
    logical_input_count: usize,
) -> Result<(EmbeddingBatch, ContextEmbeddingGenerationReport), Status> {
    let provider = normalize_provider(provider.clone());
    let (batch, mut report) = context_embeddings_for_unique_inputs(&provider, unique_inputs)?;
    report.requested_vector_count = logical_input_count;
    report.generated_vector_count = logical_input_count;
    Ok((batch, report))
}

fn context_embeddings_for_unique_inputs(
    provider: &ContextModelProviderConfig,
    inputs: &[(&str, u64, u32, &str)],
) -> Result<(EmbeddingBatch, ContextEmbeddingGenerationReport), Status> {
    if inputs.is_empty() {
        return context_embeddings_for_extract_one_batch(provider, inputs);
    }
    if context_embedding_batch_end(provider, inputs, 0) == inputs.len() {
        return context_embeddings_for_extract_one_batch(provider, inputs);
    }

    let mut start = 0usize;
    let mut combined = EmbeddingBatch::default();
    let mut combined_report: Option<ContextEmbeddingGenerationReport> = None;
    while start < inputs.len() {
        let end = context_embedding_batch_end(provider, inputs, start);
        let (batch, report) =
            context_embeddings_for_extract_one_batch(provider, &inputs[start..end])?;
        if combined.dimensions == 0 {
            combined.dimensions = batch.dimensions;
            combined
                .values
                .reserve(inputs.len().saturating_mul(batch.dimensions));
        } else if batch.dimensions != combined.dimensions {
            return Err(Status::error(
                "embedding_provider_bad_response",
                format!(
                    "embedding provider {} changed dimensions between request batches",
                    provider.provider_name
                ),
            ));
        }
        combined.values.extend(batch.values);
        if let Some(total) = combined_report.as_mut() {
            total.requested_vector_count = total
                .requested_vector_count
                .saturating_add(report.requested_vector_count);
            total.generated_vector_count = total
                .generated_vector_count
                .saturating_add(report.generated_vector_count);
            total.batch_count = total.batch_count.saturating_add(report.batch_count);
            total.live_call_count = total
                .live_call_count
                .saturating_add(report.live_call_count);
            total.production_evidence_ready &= report.production_evidence_ready;
        } else {
            combined_report = Some(report);
        }
        start = end;
    }
    Ok((
        combined,
        combined_report.expect("non-empty embedding input produces a report"),
    ))
}

fn context_embedding_batch_end(
    provider: &ContextModelProviderConfig,
    inputs: &[(&str, u64, u32, &str)],
    start: usize,
) -> usize {
    let limits = context_embedding_batch_limits(provider);
    let mut end = start;
    let mut bytes = 0usize;
    while end < inputs.len() && end - start < limits.max_items {
        let input_bytes = inputs[end].3.len();
        if end > start && bytes.saturating_add(input_bytes) > limits.max_request_bytes {
            break;
        }
        bytes = bytes.saturating_add(input_bytes);
        end += 1;
        if input_bytes > limits.max_input_bytes || bytes > limits.max_request_bytes {
            break;
        }
    }
    end.max(start + 1).min(inputs.len())
}

fn context_embeddings_for_extract_one_batch(
    provider: &ContextModelProviderConfig,
    inputs: &[(&str, u64, u32, &str)],
) -> Result<(EmbeddingBatch, ContextEmbeddingGenerationReport), Status> {
    let limits = context_embedding_batch_limits(provider);
    if let Some((index, input)) = inputs
        .iter()
        .enumerate()
        .find(|(_, input)| input.3.len() > limits.max_input_bytes)
    {
        return Err(Status::error(
            "embedding_input_too_large",
            format!(
                "embedding input {index} is {} bytes; limit is {}",
                input.3.len(), limits.max_input_bytes
            ),
        ));
    }
    let request_bytes = inputs.iter().fold(0usize, |bytes, input| {
        bytes.saturating_add(input.3.len())
    });
    if inputs.len() > limits.max_items || request_bytes > limits.max_request_bytes {
        return Err(Status::error(
            "embedding_request_too_large",
            format!(
                "embedding request has {} items and {request_bytes} input bytes; limits are {} items and {} bytes",
                inputs.len(), limits.max_items, limits.max_request_bytes
            ),
        ));
    }

    if provider.mock_mode || provider.provider_kind == ContextProviderKind::Mock {
        if context_require_model_embeddings() {
            return Err(Status::error(
                "model_required_but_provider_is_mock",
                "MATRIXARK_REQUIRE_MODEL_EMBEDDINGS is set but the context provider is mock/deterministic; configure a real OpenAI-compatible (OSS) embedding provider with mock_mode=false",
            ));
        }
        let batch = deterministic_embedding_batch(&provider.embedding_model, inputs);
        let vector_dimension = batch.dimensions;
        return Ok((
            batch,
            ContextEmbeddingGenerationReport {
                status: Status::ok(),
                provider_name: provider.provider_name.clone(),
                provider_kind: provider.provider_kind.clone(),
                embedding_model: provider.embedding_model.clone(),
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

    let batch = match provider.provider_kind {
        ContextProviderKind::OpenAiCompatible => {
            call_openai_compatible_embedding_provider(&provider, inputs)?
        }
        ContextProviderKind::Ollama => call_ollama_embedding_provider(&provider, inputs)?,
        ContextProviderKind::Mock => {
            deterministic_embedding_batch(&provider.embedding_model, inputs)
        }
    };
    let vector_dimension = batch.dimensions;
    if vector_dimension == 0
        || batch.values.len() % vector_dimension != 0
        || batch.rows() != inputs.len()
    {
        return Err(Status::error(
            "embedding_provider_bad_response",
            format!(
                "embedding provider {} returned {} vectors for {} inputs",
                provider.provider_name,
                batch.rows(),
                inputs.len()
            ),
        ));
    }
    if !batch.values.iter().all(|value| value.is_finite()) {
        return Err(Status::error(
            "embedding_provider_bad_response",
            format!(
                "embedding provider {} returned inconsistent or non-finite vectors",
                provider.provider_name
            ),
        ));
    }
    Ok((
        batch,
        ContextEmbeddingGenerationReport {
            status: Status::ok(),
            provider_name: provider.provider_name.clone(),
            provider_kind: provider.provider_kind.clone(),
            embedding_model: provider.embedding_model.clone(),
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

fn deterministic_embedding_batch(model: &str, inputs: &[(&str, u64, u32, &str)]) -> EmbeddingBatch {
    let mut dimensions = 0usize;
    let mut values = Vec::new();
    for (_, _, _, text) in inputs {
        let vector = deterministic_context_embedding(model, text);
        if dimensions == 0 {
            dimensions = vector.len();
            values.reserve(inputs.len().saturating_mul(dimensions));
        }
        values.extend(vector);
    }
    EmbeddingBatch { dimensions, values }
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

fn ollama_provider(provider: &ContextModelProviderConfig) -> Result<OllamaProvider, Status> {
    let mut config = OllamaConfig::from_env();
    if !provider.base_url.trim().is_empty() {
        config.base_url = provider.base_url.trim().trim_end_matches('/').to_string();
    }
    config.extraction_model = provider.model.clone();
    config.embedding_model = provider.embedding_model.clone();
    config.timeout_ms = provider.timeout_ms;
    config.max_retries = provider.max_retries;
    OllamaProvider::new(config).map_err(|err| {
        Status::error(
            "ollama_provider_invalid",
            format!(
                "context provider {} is invalid: {err}",
                provider.provider_name
            ),
        )
    })
}

fn call_ollama_embedding_provider(
    provider: &ContextModelProviderConfig,
    inputs: &[(&str, u64, u32, &str)],
) -> Result<EmbeddingBatch, Status> {
    let client = ollama_provider(provider)?;
    let texts = inputs
        .iter()
        .map(|(_, _, _, text)| *text)
        .collect::<Vec<_>>();
    client.embed_batch(&texts).map_err(|err| {
        Status::error(
            "embedding_provider_request_failed",
            format!(
                "embedding provider {} request failed: {err}",
                provider.provider_name
            ),
        )
    })
}

fn call_ollama_context_provider(
    provider: &ContextModelProviderConfig,
    request: &ContextExtractRequest,
) -> Result<ContextSummaryOutput, Status> {
    let client = ollama_provider(provider)?;
    let prompt = context_summary_prompt(request);
    client
        .generate_structured_output(
            CONTEXT_SUMMARY_SYSTEM_PROMPT,
            &prompt,
            context_summary_schema(),
        )
        .map_err(|err| {
            Status::error(
                "model_provider_request_failed",
                format!(
                    "context provider {} request failed: {err}",
                    provider.provider_name
                ),
            )
        })
}

fn normalize_ollama_summary(
    output: ContextSummaryOutput,
    request: &ContextExtractRequest,
) -> (String, String) {
    let l0 = output.l0.as_str().filter(|value| !value.trim().is_empty());
    let l1 = output.l1.as_str().filter(|value| !value.trim().is_empty());
    let require_model = context_require_model_summaries();
    let raw = if require_model && (l0.is_none() || l1.is_none()) {
        serde_json::to_string(&output).unwrap_or_default()
    } else {
        String::new()
    };
    let l0 = l0
        .map(|value| truncate_words(value, 32))
        .unwrap_or_else(|| {
            if require_model {
                truncate_words(&raw, 32)
            } else {
                summarize_l0(&request.title, &request.body)
            }
        });
    let l1 = l1
        .map(|value| truncate_words(value, 96))
        .unwrap_or_else(|| {
            if require_model {
                truncate_words(&raw, 96)
            } else {
                summarize_l1(request.source_kind, &request.title, &request.body)
            }
        });
    (l0, l1)
}

pub(crate) fn call_openai_compatible_embedding_provider(
    provider: &ContextModelProviderConfig,
    inputs: &[(&str, u64, u32, &str)],
) -> Result<EmbeddingBatch, Status> {
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
    let response: OpenAiEmbeddingResponse = post_json_with_options_and_headers(
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
    let data = response.data;
    let mut dimensions = 0usize;
    let mut row_start = 0usize;
    for row_end in data.row_ends.iter().copied() {
        let embedding = &data.values[row_start..row_end];
        if dimensions == 0 {
            dimensions = embedding.len();
        } else if embedding.len() != dimensions {
            return Err(Status::error(
                "embedding_provider_bad_response",
                format!(
                    "embedding provider {} returned inconsistent embedding dimensions",
                    provider.provider_name
                ),
            ));
        }
        if !embedding.iter().all(|value| value.is_finite()) {
            return Err(Status::error(
                "embedding_provider_bad_response",
                format!(
                    "embedding provider {} returned a non-finite embedding value",
                    provider.provider_name
                ),
            ));
        }
        row_start = row_end;
    }
    Ok(EmbeddingBatch {
        dimensions,
        values: data.values,
    })
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
    let prompt = context_summary_prompt_with_guidance(request, l1_guidance);
    let completion_request = OpenAiChatCompletionRequest {
        model: &provider.model,
        messages: vec![
            OpenAiChatMessage {
                role: "system",
                content: CONTEXT_SUMMARY_SYSTEM_PROMPT.to_string(),
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

fn context_summary_prompt(request: &ContextExtractRequest) -> String {
    let guidance = if context_source_warrants_l1(&request.body) {
        "l1: a richer semantic synthesis for tree-first retrieval -- fold the source's key facts, named entities, and CURRENT state into <=1200 characters. Prefer current state and resolved contradictions over stale facts."
    } else {
        "l1: this source is thin; keep l1 minimal (a single distilled fact line, <=220 characters). Do NOT pad or repeat l0."
    };
    context_summary_prompt_with_guidance(request, guidance)
}

fn ollama_summary_batch_prompt(
    client: &OllamaProvider,
    requests: &[&ContextExtractRequest],
    start: usize,
) -> (usize, String) {
    let config = client.config();
    let max_end = requests
        .len()
        .min(start.saturating_add(config.max_batch_items));
    let mut end = start;
    let estimated_prompt_bytes = requests[start..max_end].iter().fold(
        CONTEXT_SUMMARY_BATCH_PROMPT_PREFIX.len() + 2,
        |bytes, request| {
            bytes
                .saturating_add(request.source_id.len())
                .saturating_add(request.title.len())
                .saturating_add(request.body.len())
                .saturating_add(128)
        },
    );
    let mut prompt = Vec::with_capacity(client.bounded_prompt_capacity(
        CONTEXT_SUMMARY_SYSTEM_PROMPT.len(),
        estimated_prompt_bytes,
    ));
    prompt.extend_from_slice(CONTEXT_SUMMARY_BATCH_PROMPT_PREFIX.as_bytes());
    prompt.push(b'[');
    while end < max_end {
        let source_index = end - start;
        let item_start = prompt.len();
        if source_index > 0 {
            prompt.push(b',');
        }
        serde_json::to_writer(
            &mut prompt,
            &context_summary_batch_source(source_index, requests[end]),
        )
        .expect("borrowed context summary source serializes");
        let candidate_prompt_bytes = prompt
            .len()
            .saturating_add(1);
        let fits = candidate_prompt_bytes <= config.max_input_bytes
            && CONTEXT_SUMMARY_SYSTEM_PROMPT
                .len()
                .saturating_add(candidate_prompt_bytes)
                <= config.max_request_bytes;
        if !fits && end > start {
            prompt.truncate(item_start);
            break;
        }
        end += 1;
        if !fits {
            break;
        }
    }
    prompt.push(b']');
    (
        end.max(start + 1),
        String::from_utf8(prompt).expect("serialized context summary prompt is UTF-8"),
    )
}

fn context_summary_batch_source(
    source_index: usize,
    request: &ContextExtractRequest,
) -> ContextSummaryBatchSource<'_> {
    ContextSummaryBatchSource {
        source_index,
        source_kind: match request.source_kind {
            ContextSourceKind::Document => "Document",
            ContextSourceKind::Chat => "Chat",
            ContextSourceKind::Ticket => "Ticket",
            ContextSourceKind::Code => "Code",
            ContextSourceKind::Incident => "Incident",
            ContextSourceKind::UserEvent => "UserEvent",
        },
        source_id: &request.source_id,
        title: &request.title,
        rich_source: context_source_warrants_l1(&request.body),
        body: &request.body,
    }
}

fn context_summary_prompt_with_guidance(
    request: &ContextExtractRequest,
    l1_guidance: &str,
) -> String {
    format!(
        "Generate hierarchical ContextNode traversal summaries for source_kind={:?}, source_id={}, title={}.\n\
         Return ONLY JSON with string fields l0 and l1.\n\
         l0: a compact routing abstract (<=220 characters) used for embedding-scored tree traversal.\n\
         {l1_guidance}\n\
         Do not invent facts beyond the source.\n\
         Source body:\n{}",
        request.source_kind, request.source_id, request.title, request.body
    )
}

fn context_summary_schema() -> &'static Value {
    static SCHEMA: OnceLock<Value> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        serde_json::json!({
            "type": "object",
            "properties": {
                "l0": { "type": "string" },
                "l1": { "type": "string" }
            },
            "required": ["l0", "l1"]
        })
    })
}

fn context_summary_batch_schema() -> &'static Value {
    static SCHEMA: OnceLock<Value> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        serde_json::json!({
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "source_index": { "type": "integer" },
                            "l0": { "type": "string" },
                            "l1": { "type": "string" }
                        },
                        "required": ["source_index", "l0", "l1"]
                    }
                }
            },
            "required": ["items"]
        })
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
