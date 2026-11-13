// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Bounded protobuf transport for explicit memories; no summary-model invocation.

use super::*;
use crate::sdk::v1 as wire;
use prost::Message;

pub const EXPLICIT_PROTOCOL_VERSION: u32 = 1;
pub const EXPLICIT_MAX_BODY_BYTES: usize = 16 * 1024 * 1024;
pub const EXPLICIT_MAX_SOURCES: usize = 1_000;

fn invalid(message: impl Into<String>) -> Status {
    Status::error("context_explicit_wire_invalid", message)
}

fn provider_to_wire(provider: ContextModelProviderConfig) -> wire::LlmContextProvider {
    wire::LlmContextProvider {
        provider_name: provider.provider_name,
        provider_kind: match provider.provider_kind {
            ContextProviderKind::Mock => 1,
            ContextProviderKind::OpenAiCompatible => 2,
            ContextProviderKind::Ollama => 3,
        },
        base_url: provider.base_url,
        api_key_env: provider.api_key_env,
        model: provider.model,
        embedding_model: provider.embedding_model,
        vlm_model: provider.vlm_model,
        timeout_ms: provider.timeout_ms,
        max_retries: provider.max_retries as u64,
        fallback_provider: provider
            .fallback_provider
            .map(|provider| Box::new(provider_to_wire(*provider))),
        mock_mode: provider.mock_mode,
    }
}

fn provider_from_wire(
    provider: wire::LlmContextProvider,
) -> Result<ContextModelProviderConfig, Status> {
    Ok(ContextModelProviderConfig {
        provider_name: provider.provider_name,
        provider_kind: match provider.provider_kind {
            1 => ContextProviderKind::Mock,
            2 => ContextProviderKind::OpenAiCompatible,
            3 => ContextProviderKind::Ollama,
            _ => return Err(invalid("unknown context provider kind")),
        },
        base_url: provider.base_url,
        api_key_env: provider.api_key_env,
        model: provider.model,
        embedding_model: provider.embedding_model,
        vlm_model: provider.vlm_model,
        timeout_ms: provider.timeout_ms,
        max_retries: usize::try_from(provider.max_retries)
            .map_err(|_| invalid("retry count overflow"))?,
        fallback_provider: provider
            .fallback_provider
            .map(|provider| provider_from_wire(*provider).map(Box::new))
            .transpose()?,
        mock_mode: provider.mock_mode,
    })
}

pub fn encode_request(request: ContextIngestExtractRequest) -> Result<Vec<u8>, Status> {
    if request.sources.len() > EXPLICIT_MAX_SOURCES {
        return Err(invalid("explicit context source limit exceeded"));
    }
    let request = wire::ContextExplicitIngestRequest {
        protocol_version: EXPLICIT_PROTOCOL_VERSION,
        shard_id: request.shard_id,
        tenant_hash: request.tenant_hash,
        sources: request
            .sources
            .into_iter()
            .map(|source| wire::LlmContextSource {
                source_kind: match source.source_kind {
                    ContextSourceKind::Document => 1,
                    ContextSourceKind::Chat => 2,
                    ContextSourceKind::Ticket => 3,
                    ContextSourceKind::Code => 4,
                    ContextSourceKind::Incident => 5,
                    ContextSourceKind::UserEvent => 6,
                },
                source_id: source.source_id,
                title: source.title,
                body: source.body,
                timestamp_ms: source.timestamp_ms,
                provider: (!source.provider.provider_name.is_empty())
                    .then(|| provider_to_wire(source.provider)),
            })
            .collect(),
        provider: Some(provider_to_wire(request.provider)),
        start_time_ms: request.start_time_ms,
        end_time_ms: request.end_time_ms,
        max_events: request.max_events as u64,
        query: request.query,
    };
    if request.encoded_len() > EXPLICIT_MAX_BODY_BYTES {
        return Err(invalid("explicit context request exceeds body limit"));
    }
    Ok(request.encode_to_vec())
}

pub fn decode_request(bytes: &[u8]) -> Result<ContextIngestExtractRequest, Status> {
    if bytes.len() > EXPLICIT_MAX_BODY_BYTES {
        return Err(invalid("explicit context request exceeds body limit"));
    }
    // Count repeated sources before the generated decoder can allocate their message slots.
    let mut cursor = bytes;
    let mut source_count = 0;
    while !cursor.is_empty() {
        let (tag, kind) =
            prost::encoding::decode_key(&mut cursor).map_err(|error| invalid(error.to_string()))?;
        if tag == 4 {
            source_count += 1;
            if source_count > EXPLICIT_MAX_SOURCES {
                return Err(invalid("explicit context source limit exceeded"));
            }
        }
        prost::encoding::skip_field(
            kind,
            tag,
            &mut cursor,
            prost::encoding::DecodeContext::default(),
        )
        .map_err(|error| invalid(error.to_string()))?;
    }
    let request = wire::ContextExplicitIngestRequest::decode(bytes)
        .map_err(|error| invalid(error.to_string()))?;
    if request.protocol_version != EXPLICIT_PROTOCOL_VERSION {
        return Err(invalid("unsupported explicit context protocol version"));
    }
    let shard_id = request.shard_id;
    let tenant_hash = request.tenant_hash;
    let provider = request
        .provider
        .map(provider_from_wire)
        .transpose()?
        .unwrap_or_default();
    let sources = request
        .sources
        .into_iter()
        .map(|source| {
            Ok(ContextExtractRequest {
                shard_id,
                tenant_hash,
                source_kind: match source.source_kind {
                    1 => ContextSourceKind::Document,
                    2 => ContextSourceKind::Chat,
                    3 => ContextSourceKind::Ticket,
                    4 => ContextSourceKind::Code,
                    5 => ContextSourceKind::Incident,
                    6 => ContextSourceKind::UserEvent,
                    _ => return Err(invalid("unknown context source kind")),
                },
                source_id: source.source_id,
                title: source.title,
                body: source.body,
                timestamp_ms: source.timestamp_ms,
                provider: source
                    .provider
                    .map(provider_from_wire)
                    .transpose()?
                    .unwrap_or_else(inherited_context_source_provider),
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    Ok(ContextIngestExtractRequest {
        shard_id,
        tenant_hash,
        sources,
        query: request.query,
        start_time_ms: request.start_time_ms,
        end_time_ms: request.end_time_ms,
        max_events: usize::try_from(request.max_events)
            .map_err(|_| invalid("event count overflow"))?,
        provider,
    })
}

pub fn encode_response(status: Status, accepted: usize, failed: usize) -> Vec<u8> {
    wire::ContextExplicitIngestResponse {
        protocol_version: EXPLICIT_PROTOCOL_VERSION,
        status: Some(wire::Status {
            ok: status.ok,
            code: status.code,
            message: status.message,
        }),
        accepted: accepted as u64,
        failed: failed as u64,
    }
    .encode_to_vec()
}

/// Execute bounded source windows and retain only counts for the RPC acknowledgment.
pub fn execute_request(
    engine: &TemporalEngine,
    mut request: ContextIngestExtractRequest,
) -> Vec<u8> {
    let mut sources = std::mem::take(&mut request.sources).into_iter();
    let mut accepted = 0;
    let mut failed = 0;
    let mut failure = None;
    loop {
        let window = sources
            .by_ref()
            .take(super::ingest::CONTEXT_MODEL_SOURCE_WINDOW)
            .collect::<Vec<_>>();
        if window.is_empty() {
            break;
        }
        let report = ingest_context_without_inference(
            engine,
            ContextIngestExtractRequest {
                sources: window,
                provider: request.provider.clone(),
                query: String::new(),
                shard_id: request.shard_id,
                tenant_hash: request.tenant_hash,
                start_time_ms: request.start_time_ms,
                end_time_ms: request.end_time_ms,
                max_events: request.max_events,
            },
        );
        accepted += report.accepted;
        failed += report.failed;
        if !report.status.ok && failure.is_none() {
            failure = Some(report.status);
        }
    }
    let status = failure.unwrap_or_else(|| {
        if failed == 0 {
            Status::ok()
        } else {
            Status::error(
                "context_explicit_ingest_failed",
                "explicit context sources failed",
            )
        }
    });
    encode_response(status, accepted, failed)
}

pub fn validate_response(bytes: &[u8], expected: usize) -> Result<(), Status> {
    if bytes.len() > 64 * 1024 {
        return Err(invalid("explicit context response exceeds body limit"));
    }
    let response = wire::ContextExplicitIngestResponse::decode(bytes)
        .map_err(|error| invalid(error.to_string()))?;
    let status = response
        .status
        .ok_or_else(|| invalid("missing explicit context status"))?;
    if response.protocol_version != EXPLICIT_PROTOCOL_VERSION
        || !status.ok
        || response.failed != 0
        || response.accepted != expected as u64
    {
        return Err(Status::error(
            "context_explicit_ingest_failed",
            format!(
                "explicit ingest accepted {} of {expected}, failed {}: {} {}",
                response.accepted, response.failed, status.code, status.message
            ),
        ));
    }
    Ok(())
}
