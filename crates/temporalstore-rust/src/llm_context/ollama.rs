// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::HashSet;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::http::{post_json_with_options, HttpError, HttpRequestOptions};

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
        if let Some(base_url) =
            string_from_env("MATRIXARK_OLLAMA_BASE_URL").or_else(|| string_from_env("OLLAMA_HOST"))
        {
            config.base_url = if base_url.contains("://") {
                base_url
            } else {
                format!("http://{base_url}")
            };
        }
        set_string_from_env(
            "MATRIXARK_OLLAMA_EXTRACTION_MODEL",
            &mut config.extraction_model,
        );
        set_string_from_env(
            "MATRIXARK_OLLAMA_EMBEDDING_MODEL",
            &mut config.embedding_model,
        );
        set_string_from_env("MATRIXARK_OLLAMA_KEEP_ALIVE", &mut config.keep_alive);
        config
    }
}

fn string_from_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn set_string_from_env(name: &str, target: &mut String) {
    if let Some(value) = string_from_env(name) {
        *target = value;
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

    fn endpoint(&self, suffix: &str) -> String {
        if self.path_prefix.is_empty() {
            suffix.to_string()
        } else {
            format!("{}{suffix}", self.path_prefix)
        }
    }

    fn batch_end<T>(
        &self,
        inputs: &[T],
        start: usize,
        input_size: impl Fn(&T) -> usize,
    ) -> Result<usize, OllamaError> {
        let mut end = start;
        let mut bytes = 0usize;
        while end < inputs.len() && end - start < self.config.max_batch_items {
            let item_bytes = input_size(&inputs[end]);
            if item_bytes > self.config.max_input_bytes {
                return Err(OllamaError::InputTooLarge {
                    index: end,
                    bytes: item_bytes,
                    limit: self.config.max_input_bytes,
                });
            }
            if end > start && bytes.saturating_add(item_bytes) > self.config.max_request_bytes {
                break;
            }
            if item_bytes > self.config.max_request_bytes {
                return Err(OllamaError::InputTooLarge {
                    index: end,
                    bytes: item_bytes,
                    limit: self.config.max_request_bytes,
                });
            }
            bytes += item_bytes;
            end += 1;
        }
        Ok(end)
    }

    fn embed(&self, texts: &[&str]) -> Result<EmbeddingBatch, OllamaError> {
        if texts.is_empty() {
            return Ok(EmbeddingBatch {
                dimensions: 0,
                values: Vec::new(),
            });
        }

        let mut dimensions = 0usize;
        let mut values = Vec::new();
        let mut start = 0usize;
        while start < texts.len() {
            let end = self.batch_end(texts, start, |text| text.len())?;
            let request = OllamaEmbedRequest {
                model: &self.config.embedding_model,
                input: &texts[start..end],
                truncate: self.config.truncate_embeddings,
                keep_alive: &self.config.keep_alive,
            };
            let response: OllamaEmbedResponse = post_json_with_options(
                &self.addr,
                &self.endpoint("/api/embed"),
                &request,
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
            for vector in response.embeddings {
                if vector.is_empty() {
                    return Err(OllamaError::EmptyEmbedding);
                }
                if dimensions == 0 {
                    dimensions = vector.len();
                    values.reserve(texts.len().saturating_mul(dimensions));
                } else if vector.len() != dimensions {
                    return Err(OllamaError::EmbeddingDimensions {
                        expected: dimensions,
                        actual: vector.len(),
                    });
                }
                if !vector.iter().all(|value| value.is_finite()) {
                    return Err(OllamaError::NonFiniteEmbedding);
                }
                values.extend_from_slice(&vector);
            }
            start = end;
        }
        Ok(EmbeddingBatch { dimensions, values })
    }

    fn extract(&self, messages: &[ContextMessage]) -> Result<Vec<ExtractedContext>, OllamaError> {
        let mut message_ids = HashSet::with_capacity(messages.len());
        for message in messages {
            if !message_ids.insert(message.message_id) {
                return Err(OllamaError::DuplicateMessageId(message.message_id));
            }
        }

        let mut extracted = Vec::new();
        let mut start = 0usize;
        while start < messages.len() {
            let end = self.batch_end(messages, start, |message| message.text.len())?;
            let prompt = extraction_prompt(&messages[start..end])?;
            let request_messages = [
                OllamaChatMessage {
                    role: "system",
                    content: EXTRACTION_SYSTEM_PROMPT,
                },
                OllamaChatMessage {
                    role: "user",
                    content: &prompt,
                },
            ];
            let request = OllamaChatRequest {
                model: &self.config.extraction_model,
                messages: &request_messages,
                format: extraction_schema(),
                stream: false,
                keep_alive: &self.config.keep_alive,
                options: OllamaChatOptions { temperature: 0.0 },
            };
            let response: OllamaChatResponse = post_json_with_options(
                &self.addr,
                &self.endpoint("/api/chat"),
                &request,
                self.request_options(),
            )
            .map_err(OllamaError::Http)?;
            let content = response
                .message
                .map(|message| message.content)
                .ok_or(OllamaError::MissingMessageContent)?;
            let output: OllamaExtractionOutput =
                serde_json::from_str(&content).map_err(OllamaError::ContentJson)?;
            for item in output.items {
                let source = messages[start..end]
                    .iter()
                    .find(|message| message.message_id == item.source_message_id)
                    .ok_or(OllamaError::UnknownSourceMessage(item.source_message_id))?;
                let text = item.text.trim();
                if text.is_empty() {
                    return Err(OllamaError::EmptyContext);
                }
                extracted.push(ExtractedContext {
                    tenant_id: source.tenant_id,
                    context_id: context_id(source.tenant_id, source.message_id, text),
                    source_message_id: source.message_id,
                    event_time_ms: source.event_time_ms,
                    text: text.to_string(),
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

fn extraction_prompt(messages: &[ContextMessage]) -> Result<String, OllamaError> {
    #[derive(Serialize)]
    struct PromptMessage<'a> {
        source_message_id: u64,
        role: &'static str,
        text: &'a str,
    }

    let rows = messages
        .iter()
        .map(|message| PromptMessage {
            source_message_id: message.message_id,
            role: match message.role {
                ContextRole::User => "user",
                ContextRole::Assistant => "assistant",
                ContextRole::Tool => "tool",
                ContextRole::System => "system",
            },
            text: &message.text,
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&rows).map_err(OllamaError::ContentJson)
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
    embeddings: Vec<Vec<f32>>,
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
                let reply = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    response.len(),
                    response
                );
                stream.write_all(reply.as_bytes()).unwrap();
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
    }
}
