// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::OnceLock;

use serde::de::{DeserializeOwned, DeserializeSeed, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::env_flag::{env_bool_first, env_number_first, env_value_any};
use crate::http::{
    post_encoded_json_streaming_with_options_and_headers,
    post_json_streaming_with_options_and_headers, HttpError, HttpRequestOptions,
};

use super::{
    BatchEmbedder, BatchExtractor, ContextMessage, ContextRole, EmbeddingBatch, ExtractedContext,
};

const DEFAULT_BASE_URL: &str = "http://127.0.0.1:11434";
const DEFAULT_EXTRACTION_MODEL: &str = "qwen2.5:7b";
const DEFAULT_EMBEDDING_MODEL: &str = "nomic-embed-text";
const EXTRACTION_SYSTEM_PROMPT: &str = "Extract durable, useful context from the supplied messages. Preserve concrete facts, decisions, preferences, constraints, and unresolved work. Exclude greetings, repeated text, command output without lasting value, and unsupported inference. Return only JSON matching the supplied schema.";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OllamaConfig {
    pub base_url: String,
    pub extraction_model: String,
    pub embedding_model: String,
    pub keep_alive: String,
    pub timeout_ms: u64,
    pub max_retries: usize,
    pub max_batch_items: usize,
    pub max_input_bytes: usize,
    pub max_request_bytes: usize,
    pub truncate_embeddings: bool,
}

impl Default for OllamaConfig {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            extraction_model: DEFAULT_EXTRACTION_MODEL.to_string(),
            embedding_model: DEFAULT_EMBEDDING_MODEL.to_string(),
            keep_alive: "5m".to_string(),
            timeout_ms: 120_000,
            max_retries: 1,
            max_batch_items: 32,
            max_input_bytes: 64 * 1024,
            max_request_bytes: 256 * 1024,
            truncate_embeddings: true,
        }
    }
}

impl OllamaConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();
        if let Some(base_url) = env_value_any(&[
            "TEMPORALSTORE_OLLAMA_BASE_URL",
            "MATRIXARK_OLLAMA_BASE_URL",
            "OLLAMA_HOST",
        ]) {
            config.base_url = if base_url.contains("://") {
                base_url
            } else {
                format!("http://{base_url}")
            };
        }
        if let Some(model) = env_value_any(&[
            "TEMPORALSTORE_OLLAMA_EXTRACTION_MODEL",
            "MATRIXARK_OLLAMA_EXTRACTION_MODEL",
        ]) {
            config.extraction_model = model;
        }
        if let Some(model) = env_value_any(&[
            "TEMPORALSTORE_OLLAMA_EMBEDDING_MODEL",
            "MATRIXARK_OLLAMA_EMBEDDING_MODEL",
        ]) {
            config.embedding_model = model;
        }
        if let Some(keep_alive) = env_value_any(&[
            "TEMPORALSTORE_OLLAMA_KEEP_ALIVE",
            "MATRIXARK_OLLAMA_KEEP_ALIVE",
            "OLLAMA_KEEP_ALIVE",
        ]) {
            config.keep_alive = keep_alive;
        }
        config.timeout_ms = env_number_first(
            &[
                "TEMPORALSTORE_OLLAMA_TIMEOUT_MS",
                "MATRIXARK_OLLAMA_TIMEOUT_MS",
            ],
            config.timeout_ms,
        );
        config.max_retries = env_number_first(
            &[
                "TEMPORALSTORE_OLLAMA_MAX_RETRIES",
                "MATRIXARK_OLLAMA_MAX_RETRIES",
            ],
            config.max_retries,
        );
        config.max_batch_items = env_number_first(
            &[
                "TEMPORALSTORE_OLLAMA_MAX_BATCH_ITEMS",
                "MATRIXARK_OLLAMA_MAX_BATCH_ITEMS",
            ],
            config.max_batch_items,
        );
        config.max_input_bytes = env_number_first(
            &[
                "TEMPORALSTORE_OLLAMA_MAX_INPUT_BYTES",
                "MATRIXARK_OLLAMA_MAX_INPUT_BYTES",
            ],
            config.max_input_bytes,
        );
        config.max_request_bytes = env_number_first(
            &[
                "TEMPORALSTORE_OLLAMA_MAX_REQUEST_BYTES",
                "MATRIXARK_OLLAMA_MAX_REQUEST_BYTES",
            ],
            config.max_request_bytes,
        );
        config.truncate_embeddings = env_bool_first(
            &[
                "TEMPORALSTORE_OLLAMA_TRUNCATE_EMBEDDINGS",
                "MATRIXARK_OLLAMA_TRUNCATE_EMBEDDINGS",
            ],
            config.truncate_embeddings,
        );
        config
    }
}

#[derive(Debug, Error)]
pub enum OllamaError {
    #[error("Ollama base URL must start with http://")]
    UnsupportedUrl,
    #[error("Ollama base URL is missing a host")]
    MissingHost,
    #[error("Ollama model name cannot be empty")]
    EmptyModel,
    #[error("Ollama timeout must be greater than zero")]
    InvalidTimeout,
    #[error("Ollama batch limits must be greater than zero")]
    InvalidBatchLimit,
    #[error("input at position {index} is {bytes} bytes; limit is {limit}")]
    InputTooLarge {
        index: usize,
        bytes: usize,
        limit: usize,
    },
    #[error("Ollama request failed")]
    Http(#[source] HttpError),
    #[error("Ollama returned invalid JSON content")]
    ContentJson(#[source] serde_json::Error),
    #[error("Ollama returned {actual} embeddings for {expected} inputs")]
    EmbeddingCount { expected: usize, actual: usize },
    #[error("Ollama returned an empty embedding")]
    EmptyEmbedding,
    #[error("Ollama embedding dimensions changed from {expected} to {actual}")]
    EmbeddingDimensions { expected: usize, actual: usize },
    #[error("Ollama returned a non-finite embedding value")]
    NonFiniteEmbedding,
    #[error("Ollama extraction response omitted message.content")]
    MissingMessageContent,
    #[error("Ollama extraction returned unknown source message {0}")]
    UnknownSourceMessage(u64),
    #[error("Ollama extraction returned an empty context item")]
    EmptyContext,
    #[error("input batch contains duplicate message id {0}")]
    DuplicateMessageId(u64),
}

#[derive(Debug, Clone)]
pub struct OllamaProvider {
    config: OllamaConfig,
    addr: String,
    path_prefix: String,
}

impl OllamaProvider {
    pub fn new(config: OllamaConfig) -> Result<Self, OllamaError> {
        if config.extraction_model.trim().is_empty() || config.embedding_model.trim().is_empty() {
            return Err(OllamaError::EmptyModel);
        }
        if config.timeout_ms == 0 {
            return Err(OllamaError::InvalidTimeout);
        }
        if config.max_batch_items == 0
            || config.max_input_bytes == 0
            || config.max_request_bytes == 0
        {
            return Err(OllamaError::InvalidBatchLimit);
        }
        let (addr, path_prefix) = parse_base_url(&config.base_url)?;
        Ok(Self {
            config,
            addr,
            path_prefix,
        })
    }

    pub fn from_env() -> Result<Self, OllamaError> {
        Self::new(OllamaConfig::from_env())
    }

    pub fn config(&self) -> &OllamaConfig {
        &self.config
    }

    fn request_options(&self) -> HttpRequestOptions {
        HttpRequestOptions {
            connect_timeout_ms: self.config.timeout_ms.min(5_000).max(1),
            io_timeout_ms: self.config.timeout_ms.max(1),
            max_retries: self.config.max_retries,
        }
    }

    pub(crate) fn bounded_prompt_capacity(
        &self,
        system_prompt_bytes: usize,
        estimated_prompt_bytes: usize,
    ) -> usize {
        estimated_prompt_bytes
            .min(self.config.max_input_bytes)
            .min(
                self.config
                    .max_request_bytes
                    .saturating_sub(system_prompt_bytes),
            )
    }

    fn endpoint(&self, suffix: &str) -> String {
        if self.path_prefix.is_empty() {
            suffix.to_string()
        } else {
            format!("{}{suffix}", self.path_prefix)
        }
    }

    fn embedding_batch_end(&self, inputs: &[&str], start: usize) -> Result<usize, OllamaError> {
        let mut end = start;
        let mut request_bytes = ollama_embed_request_wire_bytes(&self.config, &[]);
        while end < inputs.len() && end - start < self.config.max_batch_items {
            let item_bytes = inputs[end].len();
            if item_bytes > self.config.max_input_bytes {
                return Err(OllamaError::InputTooLarge {
                    index: end,
                    bytes: item_bytes,
                    limit: self.config.max_input_bytes,
                });
            }
            let candidate_bytes = request_bytes
                .saturating_add(usize::from(end > start))
                .saturating_add(json_string_wire_bytes(inputs[end]));
            if end > start && candidate_bytes > self.config.max_request_bytes {
                break;
            }
            if candidate_bytes > self.config.max_request_bytes {
                return Err(OllamaError::InputTooLarge {
                    index: end,
                    bytes: candidate_bytes,
                    limit: self.config.max_request_bytes,
                });
            }
            request_bytes = candidate_bytes;
            end += 1;
        }
        Ok(end)
    }

    fn extraction_batch_prompt(
        &self,
        messages: &[ContextMessage],
        start: usize,
    ) -> Result<(usize, String), OllamaError> {
        let max_end = messages
            .len()
            .min(start.saturating_add(self.config.max_batch_items));
        let mut end = start;
        let estimated_prompt_bytes = messages[start..max_end]
            .iter()
            .fold(2usize, |bytes, message| {
                bytes
                    .saturating_add(message.text.len())
                    .saturating_add(64)
            });
        let mut prompt = Vec::with_capacity(self.bounded_prompt_capacity(
            EXTRACTION_SYSTEM_PROMPT.len(),
            estimated_prompt_bytes,
        ));
        prompt.push(b'[');

        while end < max_end {
            let message = &messages[end];
            if message.text.len() > self.config.max_input_bytes {
                return Err(OllamaError::InputTooLarge {
                    index: end,
                    bytes: message.text.len(),
                    limit: self.config.max_input_bytes,
                });
            }

            let item_start = prompt.len();
            if end > start {
                prompt.push(b',');
            }
            serde_json::to_writer(
                &mut prompt,
                &OllamaExtractionPromptMessage {
                    source_message_id: message.message_id,
                    role: match message.role {
                        ContextRole::User => "user",
                        ContextRole::Assistant => "assistant",
                        ContextRole::Tool => "tool",
                        ContextRole::System => "system",
                    },
                    text: &message.text,
                },
            )
            .map_err(OllamaError::ContentJson)?;

            let prompt_bytes = prompt.len().saturating_add(1);
            let request_bytes = EXTRACTION_SYSTEM_PROMPT.len().saturating_add(prompt_bytes);
            if prompt_bytes > self.config.max_input_bytes
                || request_bytes > self.config.max_request_bytes
            {
                if end > start {
                    prompt.truncate(item_start);
                    break;
                }
                let (bytes, limit) = if prompt_bytes > self.config.max_input_bytes {
                    (prompt_bytes, self.config.max_input_bytes)
                } else {
                    (request_bytes, self.config.max_request_bytes)
                };
                return Err(OllamaError::InputTooLarge {
                    index: 1,
                    bytes,
                    limit,
                });
            }
            end += 1;
        }

        prompt.push(b']');
        Ok((
            end,
            String::from_utf8(prompt).expect("JSON serialization always produces UTF-8"),
        ))
    }

    fn embed(&self, texts: &[&str]) -> Result<EmbeddingBatch, OllamaError> {
        if texts.is_empty() {
            return Ok(EmbeddingBatch {
                dimensions: 0,
                values: Vec::new(),
            });
        }

        // Validate the complete input plan before spending inference on its prefix.
        let mut batch_ends = Vec::with_capacity(texts.len().div_ceil(self.config.max_batch_items));
        let mut start = 0usize;
        while start < texts.len() {
            let end = self.embedding_batch_end(texts, start)?;
            batch_ends.push(end);
            start = end;
        }

        let mut dimensions = 0usize;
        let mut values = Vec::new();
        let mut request_body = Vec::new();
        let mut start = 0usize;
        for end in batch_ends {
            let request = OllamaEmbedRequest {
                model: &self.config.embedding_model,
                input: &texts[start..end],
                truncate: self.config.truncate_embeddings,
                keep_alive: &self.config.keep_alive,
            };
            let request_bytes = ollama_embed_request_wire_bytes(&self.config, request.input);
            request_body.clear();
            request_body.reserve(request_bytes);
            serde_json::to_writer(&mut request_body, &request).map_err(OllamaError::ContentJson)?;
            debug_assert_eq!(request_body.len(), request_bytes);
            let mut response: OllamaEmbedResponse =
                post_encoded_json_streaming_with_options_and_headers(
                    &self.addr,
                    &self.endpoint("/api/embed"),
                    &request_body,
                    "",
                    self.request_options(),
                )
                .map_err(OllamaError::Http)?;
            let expected = end - start;
            if response.embeddings.len() != expected {
                return Err(OllamaError::EmbeddingCount {
                    expected,
                    actual: response.embeddings.len(),
                });
            }
            let mut row_start = 0usize;
            for row_end in response.embeddings.row_ends.iter().copied() {
                let vector = &response.embeddings.values[row_start..row_end];
                if vector.is_empty() {
                    return Err(OllamaError::EmptyEmbedding);
                }
                if dimensions == 0 {
                    dimensions = vector.len();
                } else if vector.len() != dimensions {
                    return Err(OllamaError::EmbeddingDimensions {
                        expected: dimensions,
                        actual: vector.len(),
                    });
                }
                if !vector.iter().all(|value| value.is_finite()) {
                    return Err(OllamaError::NonFiniteEmbedding);
                }
                row_start = row_end;
            }
            if values.is_empty() {
                values = response.embeddings.values;
                values.reserve(
                    texts
                        .len()
                        .saturating_mul(dimensions)
                        .saturating_sub(values.len()),
                );
            } else {
                values.append(&mut response.embeddings.values);
            }
            start = end;
        }
        Ok(EmbeddingBatch { dimensions, values })
    }

    pub fn generate_structured_json(
        &self,
        system_prompt: &str,
        user_prompt: &str,
        schema: &Value,
    ) -> Result<Value, OllamaError> {
        self.generate_structured_output(system_prompt, user_prompt, schema)
    }

    pub(crate) fn generate_structured_output<T: DeserializeOwned>(
        &self,
        system_prompt: &str,
        user_prompt: &str,
        schema: &Value,
    ) -> Result<T, OllamaError> {
        for (index, prompt) in [system_prompt, user_prompt].into_iter().enumerate() {
            if prompt.len() > self.config.max_input_bytes {
                return Err(OllamaError::InputTooLarge {
                    index,
                    bytes: prompt.len(),
                    limit: self.config.max_input_bytes,
                });
            }
        }
        let request_bytes = system_prompt.len().saturating_add(user_prompt.len());
        if request_bytes > self.config.max_request_bytes {
            return Err(OllamaError::InputTooLarge {
                index: 1,
                bytes: request_bytes,
                limit: self.config.max_request_bytes,
            });
        }
        let request_messages = [
            OllamaChatMessage {
                role: "system",
                content: system_prompt,
            },
            OllamaChatMessage {
                role: "user",
                content: user_prompt,
            },
        ];
        let request = OllamaChatRequest {
            model: &self.config.extraction_model,
            messages: &request_messages,
            format: schema,
            stream: false,
            keep_alive: &self.config.keep_alive,
            options: OllamaChatOptions { temperature: 0.0 },
        };
        let response: OllamaChatResponse = post_json_streaming_with_options_and_headers(
            &self.addr,
            &self.endpoint("/api/chat"),
            &request,
            "",
            self.request_options(),
        )
        .map_err(OllamaError::Http)?;
        let content = response
            .message
            .map(|message| message.content)
            .ok_or(OllamaError::MissingMessageContent)?;
        serde_json::from_str(&content).map_err(OllamaError::ContentJson)
    }

    fn extract(&self, messages: &[ContextMessage]) -> Result<Vec<ExtractedContext>, OllamaError> {
        let mut messages_by_id = (messages.len() <= self.config.max_batch_items)
            .then(|| HashMap::with_capacity(messages.len()));
        let mut seen_message_ids = messages_by_id
            .is_none()
            .then(|| HashSet::with_capacity(messages.len()));
        for (message_index, message) in messages.iter().enumerate() {
            let duplicate = if let Some(messages_by_id) = messages_by_id.as_mut() {
                messages_by_id
                    .insert(message.message_id, message_index)
                    .is_some()
            } else {
                !seen_message_ids
                    .as_mut()
                    .expect("large extraction has an id set")
                    .insert(message.message_id)
            };
            if duplicate {
                return Err(OllamaError::DuplicateMessageId(message.message_id));
            }
        }
        drop(seen_message_ids);

        let mut extracted = Vec::new();
        let mut messages_in_chunk = HashMap::new();
        let mut start = 0usize;
        while start < messages.len() {
            let (end, prompt) = self.extraction_batch_prompt(messages, start)?;
            if messages_by_id.is_none() {
                messages_in_chunk.clear();
                messages_in_chunk.reserve(end - start);
                messages_in_chunk.extend(
                    messages[start..end]
                        .iter()
                        .enumerate()
                        .map(|(offset, message)| (message.message_id, start + offset)),
                );
            }
            let output: OllamaExtractionOutput = self.generate_structured_output(
                EXTRACTION_SYSTEM_PROMPT,
                &prompt,
                extraction_schema(),
            )?;
            for item in output.items {
                let source_index = messages_by_id
                    .as_ref()
                    .unwrap_or(&messages_in_chunk)
                    .get(&item.source_message_id)
                    .copied()
                    .ok_or(OllamaError::UnknownSourceMessage(item.source_message_id))?;
                let source = &messages[source_index];
                let text = trim_owned(item.text);
                if text.is_empty() {
                    return Err(OllamaError::EmptyContext);
                }
                extracted.push(ExtractedContext {
                    tenant_id: source.tenant_id,
                    context_id: context_id(source.tenant_id, source.message_id, &text),
                    source_message_id: source.message_id,
                    event_time_ms: source.event_time_ms,
                    text,
                });
            }
            start = end;
        }
        Ok(extracted)
    }
}

impl BatchEmbedder for OllamaProvider {
    type Error = OllamaError;

    fn embed_batch(&self, texts: &[&str]) -> Result<EmbeddingBatch, Self::Error> {
        self.embed(texts)
    }
}

impl BatchExtractor for OllamaProvider {
    type Error = OllamaError;

    fn extract_batch(
        &self,
        messages: &[ContextMessage],
    ) -> Result<Vec<ExtractedContext>, Self::Error> {
        self.extract(messages)
    }
}

fn parse_base_url(base_url: &str) -> Result<(String, String), OllamaError> {
    let binding = base_url.trim();
    let Some(rest) = binding.trim_end_matches('/').strip_prefix("http://") else {
        return Err(OllamaError::UnsupportedUrl);
    };
    let (host, path) = rest.split_once('/').unwrap_or((rest, ""));
    if host.trim().is_empty() {
        return Err(OllamaError::MissingHost);
    }
    let path_prefix = if path.trim_matches('/').is_empty() {
        String::new()
    } else {
        format!("/{}", path.trim_matches('/'))
    };
    Ok((host.to_string(), path_prefix))
}

fn ollama_embed_request_wire_bytes(config: &OllamaConfig, inputs: &[&str]) -> usize {
    let input_bytes = inputs
        .iter()
        .enumerate()
        .fold(0usize, |bytes, (index, input)| {
            bytes
                .saturating_add(usize::from(index > 0))
                .saturating_add(json_string_wire_bytes(input))
        });
    b"{\"model\":"
        .len()
        .saturating_add(json_string_wire_bytes(&config.embedding_model))
        .saturating_add(b",\"input\":[".len())
        .saturating_add(input_bytes)
        .saturating_add(b"],\"truncate\":".len())
        .saturating_add(if config.truncate_embeddings { 4 } else { 5 })
        .saturating_add(b",\"keep_alive\":".len())
        .saturating_add(json_string_wire_bytes(&config.keep_alive))
        .saturating_add(1)
}

fn json_string_wire_bytes(value: &str) -> usize {
    value.bytes().fold(2usize, |bytes, byte| {
        bytes.saturating_add(match byte {
            b'"' | b'\\' | b'\x08' | b'\t' | b'\n' | b'\x0c' | b'\r' => 2,
            0x00..=0x1f => 6,
            _ => 1,
        })
    })
}

fn context_id(tenant_id: u64, source_message_id: u64, text: &str) -> u64 {
    let mut digest = Sha256::new();
    digest.update(tenant_id.to_le_bytes());
    digest.update(source_message_id.to_le_bytes());
    digest.update(text.as_bytes());
    let bytes: [u8; 8] = digest.finalize()[..8]
        .try_into()
        .expect("fixed digest prefix");
    u64::from_le_bytes(bytes)
}

fn trim_owned(mut text: String) -> String {
    let start = text.len().saturating_sub(text.trim_start().len());
    let end = start.saturating_add(text.trim().len());
    if end < text.len() {
        text.truncate(end);
    }
    if start > 0 {
        drop(text.drain(..start));
    }
    text
}

fn extraction_schema() -> &'static Value {
    static SCHEMA: OnceLock<Value> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        json!({
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "source_message_id": { "type": "integer" },
                            "text": { "type": "string" }
                        },
                        "required": ["source_message_id", "text"]
                    }
                }
            },
            "required": ["items"]
        })
    })
}

#[derive(Debug, Serialize)]
struct OllamaEmbedRequest<'a> {
    model: &'a str,
    input: &'a [&'a str],
    truncate: bool,
    keep_alive: &'a str,
}

#[derive(Debug, Deserialize)]
struct OllamaEmbedResponse {
    embeddings: FlatEmbeddingRows,
}

#[derive(Debug, Default)]
struct FlatEmbeddingRows {
    values: Vec<f32>,
    row_ends: Vec<usize>,
}

impl FlatEmbeddingRows {
    fn len(&self) -> usize {
        self.row_ends.len()
    }
}

impl<'de> Deserialize<'de> for FlatEmbeddingRows {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct RowsVisitor;

        impl<'de> Visitor<'de> for RowsVisitor {
            type Value = FlatEmbeddingRows;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an array of embedding rows")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut rows = FlatEmbeddingRows {
                    values: Vec::new(),
                    row_ends: Vec::with_capacity(sequence.size_hint().unwrap_or(0)),
                };
                while sequence
                    .next_element_seed(EmbeddingRowSeed {
                        values: &mut rows.values,
                    })?
                    .is_some()
                {
                    rows.row_ends.push(rows.values.len());
                }
                Ok(rows)
            }
        }

        deserializer.deserialize_seq(RowsVisitor)
    }
}

struct EmbeddingRowSeed<'a> {
    values: &'a mut Vec<f32>,
}

impl<'de, 'a> DeserializeSeed<'de> for EmbeddingRowSeed<'a> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(EmbeddingRowVisitor {
            values: self.values,
        })
    }
}

struct EmbeddingRowVisitor<'a> {
    values: &'a mut Vec<f32>,
}

impl<'de, 'a> Visitor<'de> for EmbeddingRowVisitor<'a> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an embedding row of floating-point values")
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

#[derive(Debug, Serialize)]
struct OllamaExtractionPromptMessage<'a> {
    source_message_id: u64,
    role: &'static str,
    text: &'a str,
}

#[derive(Debug, Serialize)]
struct OllamaChatRequest<'a> {
    model: &'a str,
    messages: &'a [OllamaChatMessage<'a>],
    format: &'a Value,
    stream: bool,
    keep_alive: &'a str,
    options: OllamaChatOptions,
}

#[derive(Debug, Serialize)]
struct OllamaChatMessage<'a> {
    role: &'static str,
    content: &'a str,
}

#[derive(Debug, Serialize)]
struct OllamaChatOptions {
    temperature: f32,
}

#[derive(Debug, Deserialize)]
struct OllamaChatResponse {
    message: Option<OllamaResponseMessage>,
}

#[derive(Debug, Deserialize)]
struct OllamaResponseMessage {
    content: String,
}

#[derive(Debug, Deserialize)]
struct OllamaExtractionOutput {
    items: Vec<OllamaExtractionItem>,
}

#[derive(Debug, Deserialize)]
struct OllamaExtractionItem {
    source_message_id: u64,
    text: String,
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::process::Command;
    use std::thread;

    use super::*;

    fn provider(addr: std::net::SocketAddr) -> OllamaProvider {
        OllamaProvider::new(OllamaConfig {
            base_url: format!("http://{addr}"),
            max_batch_items: 2,
            ..OllamaConfig::default()
        })
        .unwrap()
    }

    #[test]
    fn temporalstore_environment_configures_the_native_provider() {
        const CHILD: &str = "TEMPORALSTORE_OLLAMA_CONFIG_TEST_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let config = OllamaConfig::from_env();
            assert_eq!(config.base_url, "http://127.0.0.1:21434");
            assert_eq!(config.extraction_model, "qwen-test");
            assert_eq!(config.embedding_model, "embed-test");
            assert_eq!(config.keep_alive, "17m");
            assert_eq!(config.timeout_ms, 7_001);
            assert_eq!(config.max_retries, 3);
            assert_eq!(config.max_batch_items, 11);
            assert_eq!(config.max_input_bytes, 12_345);
            assert_eq!(config.max_request_bytes, 54_321);
            assert!(!config.truncate_embeddings);

            let provider = crate::context_workflow::context_provider_from_env();
            assert_eq!(
                provider.provider_kind,
                crate::context_workflow::ContextProviderKind::Ollama
            );
            assert_eq!(provider.base_url, config.base_url);
            assert_eq!(provider.model, config.extraction_model);
            assert_eq!(provider.embedding_model, config.embedding_model);
            assert!(!provider.mock_mode);
            return;
        }

        let output = Command::new(std::env::current_exe().expect("current test executable"))
            .args([
                "--exact",
                "llm_context::ollama::tests::temporalstore_environment_configures_the_native_provider",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("TEMPORALSTORE_CONTEXT_PROVIDER", "ollama")
            .env("TEMPORALSTORE_OLLAMA_BASE_URL", "127.0.0.1:21434")
            .env("TEMPORALSTORE_OLLAMA_EXTRACTION_MODEL", "qwen-test")
            .env("TEMPORALSTORE_OLLAMA_EMBEDDING_MODEL", "embed-test")
            .env("TEMPORALSTORE_OLLAMA_KEEP_ALIVE", "17m")
            .env("TEMPORALSTORE_OLLAMA_TIMEOUT_MS", "7001")
            .env("TEMPORALSTORE_OLLAMA_MAX_RETRIES", "3")
            .env("TEMPORALSTORE_OLLAMA_MAX_BATCH_ITEMS", "11")
            .env("TEMPORALSTORE_OLLAMA_MAX_INPUT_BYTES", "12345")
            .env("TEMPORALSTORE_OLLAMA_MAX_REQUEST_BYTES", "54321")
            .env("TEMPORALSTORE_OLLAMA_TRUNCATE_EMBEDDINGS", "off")
            .env("MATRIXARK_CONTEXT_PROVIDER", "mock")
            .env("MATRIXARK_OLLAMA_BASE_URL", "127.0.0.1:1")
            .output()
            .expect("child test process");
        assert!(
            output.status.success(),
            "child failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn serve_json(responses: Vec<&'static str>) -> (std::net::SocketAddr, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = Vec::new();
                let mut buf = [0u8; 4096];
                loop {
                    let count = stream.read(&mut buf).unwrap();
                    request.extend_from_slice(&buf[..count]);
                    let Some(header_end) = request
                        .windows(4)
                        .position(|window| window == b"\r\n\r\n")
                        .map(|offset| offset + 4)
                    else {
                        continue;
                    };
                    let headers = String::from_utf8_lossy(&request[..header_end]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            line.strip_prefix("Content-Length: ")
                                .and_then(|value| value.parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    if request.len() >= header_end + content_length {
                        break;
                    }
                }
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    response.len()
                );
                stream.write_all(head.as_bytes()).unwrap();
                stream.flush().unwrap();
                thread::sleep(std::time::Duration::from_millis(5));
                for chunk in response.as_bytes().chunks(7) {
                    stream.write_all(chunk).unwrap();
                }
            }
        });
        (addr, handle)
    }

    #[test]
    fn native_embedding_batches_and_flattens_rows() {
        let (addr, server) = serve_json(vec![
            r#"{"embeddings":[[1.0,0.0],[0.0,1.0]]}"#,
            r#"{"embeddings":[[0.5,0.5]]}"#,
        ]);
        let output = provider(addr)
            .embed_batch(&["one", "two", "three"])
            .unwrap();
        server.join().unwrap();

        assert_eq!(output.dimensions, 2);
        assert_eq!(output.values, vec![1.0, 0.0, 0.0, 1.0, 0.5, 0.5]);
    }

    #[test]
    fn embedding_wire_size_matches_the_encoded_request() {
        let config = OllamaConfig {
            embedding_model: "embed\"model".to_string(),
            keep_alive: "5m\\local".to_string(),
            truncate_embeddings: false,
            ..OllamaConfig::default()
        };
        let inputs = [
            "plain",
            "quote \" newline\n control \u{0001}",
            "unicode \u{4e0a}\u{4e0b}\u{6587}",
        ];
        let request = OllamaEmbedRequest {
            model: &config.embedding_model,
            input: &inputs,
            truncate: config.truncate_embeddings,
            keep_alive: &config.keep_alive,
        };

        let encoded = serde_json::to_vec(&request).unwrap();

        assert_eq!(
            ollama_embed_request_wire_bytes(&config, &inputs),
            encoded.len()
        );
    }

    #[test]
    fn embedding_batches_use_encoded_wire_bytes() {
        let first = "quote-heavy \"\"\" and newline\n";
        let second = "second";
        let mut config = OllamaConfig::default();
        config.max_request_bytes = ollama_embed_request_wire_bytes(&config, &[first]);
        let provider = OllamaProvider::new(config.clone()).unwrap();

        assert_eq!(
            provider.embedding_batch_end(&[first, second], 0).unwrap(),
            1
        );
        assert_eq!(
            provider.embedding_batch_end(&[first, second], 1).unwrap(),
            2
        );

        config.max_request_bytes -= 1;
        let provider = OllamaProvider::new(config).unwrap();
        assert!(matches!(
            provider.embedding_batch_end(&[first], 0),
            Err(OllamaError::InputTooLarge { index: 0, .. })
        ));
    }

    #[test]
    fn native_extraction_maps_model_output_to_source_metadata() {
        let (addr, server) = serve_json(vec![
            r#"{"message":{"content":"{\"items\":[{\"source_message_id\":7,\"text\":\"Prefers local models\"}]}"}}"#,
        ]);
        let message = ContextMessage {
            tenant_id: 4,
            message_id: 7,
            event_time_ms: 11,
            role: ContextRole::User,
            text: "I prefer local models".to_string(),
        };
        let output = provider(addr).extract_batch(&[message]).unwrap();
        server.join().unwrap();

        assert_eq!(output.len(), 1);
        assert_eq!(output[0].tenant_id, 4);
        assert_eq!(output[0].source_message_id, 7);
        assert_eq!(output[0].event_time_ms, 11);
        assert_eq!(output[0].text, "Prefers local models");
    }

    #[test]
    fn native_extraction_reuses_a_bounded_chunk_lookup() {
        let (addr, server) = serve_json(vec![
            r#"{"message":{"content":"{\"items\":[{\"source_message_id\":7,\"text\":\"first fact\"},{\"source_message_id\":8,\"text\":\"second fact\"}]}"}}"#,
            r#"{"message":{"content":"{\"items\":[{\"source_message_id\":9,\"text\":\"third fact\"}]}"}}"#,
        ]);
        let messages = [
            ContextMessage {
                tenant_id: 4,
                message_id: 7,
                event_time_ms: 11,
                role: ContextRole::User,
                text: "first".to_string(),
            },
            ContextMessage {
                tenant_id: 4,
                message_id: 8,
                event_time_ms: 12,
                role: ContextRole::Assistant,
                text: "second".to_string(),
            },
            ContextMessage {
                tenant_id: 4,
                message_id: 9,
                event_time_ms: 13,
                role: ContextRole::User,
                text: "third".to_string(),
            },
        ];
        let output = provider(addr).extract_batch(&messages).unwrap();
        server.join().unwrap();

        assert_eq!(
            output
                .iter()
                .map(|item| {
                    (
                        item.source_message_id,
                        item.event_time_ms,
                        item.text.as_str(),
                    )
                })
                .collect::<Vec<_>>(),
            vec![
                (7, 11, "first fact"),
                (8, 12, "second fact"),
                (9, 13, "third fact"),
            ]
        );
    }

    #[test]
    fn invalid_or_oversized_inputs_fail_before_network_io() {
        let provider = OllamaProvider::new(OllamaConfig {
            max_input_bytes: 3,
            ..OllamaConfig::default()
        })
        .unwrap();
        assert!(matches!(
            provider.embed_batch(&["four"]),
            Err(OllamaError::InputTooLarge { .. })
        ));
        assert!(matches!(
            OllamaProvider::new(OllamaConfig {
                base_url: "https://localhost:11434".to_string(),
                ..OllamaConfig::default()
            }),
            Err(OllamaError::UnsupportedUrl)
        ));

        let duplicate_messages = [
            ContextMessage {
                tenant_id: 1,
                message_id: 7,
                event_time_ms: 1,
                role: ContextRole::User,
                text: "first".to_string(),
            },
            ContextMessage {
                tenant_id: 1,
                message_id: 8,
                event_time_ms: 2,
                role: ContextRole::Assistant,
                text: "middle".to_string(),
            },
            ContextMessage {
                tenant_id: 1,
                message_id: 7,
                event_time_ms: 3,
                role: ContextRole::User,
                text: "duplicate in a later request chunk".to_string(),
            },
        ];
        let duplicate_provider = OllamaProvider::new(OllamaConfig {
            max_batch_items: 2,
            ..OllamaConfig::default()
        })
        .unwrap();
        assert!(matches!(
            duplicate_provider.extract_batch(&duplicate_messages),
            Err(OllamaError::DuplicateMessageId(7))
        ));
    }
}
