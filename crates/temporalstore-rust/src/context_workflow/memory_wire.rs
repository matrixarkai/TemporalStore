// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::sdk::v1 as wire;
use prost::Message;

pub const MAX_FACTS_PER_SOURCE: usize = 8;

fn check_budgets(mut bytes: &[u8], expected: usize) -> Result<(), Status> {
    use prost::encoding::{decode_key, decode_varint, skip_field, DecodeContext, WireType};
    let invalid = || {
        Status::error(
            "invalid_memory_extraction_response",
            "invalid fact receipt encoding",
        )
    };
    let mut sources = 0;
    while !bytes.is_empty() {
        let (tag, kind) = decode_key(&mut bytes).map_err(|_| invalid())?;
        if tag != 2 {
            skip_field(kind, tag, &mut bytes, DecodeContext::default()).map_err(|_| invalid())?;
            continue;
        }
        sources += 1;
        if sources > expected {
            return Err(Status::error(
                "memory_receipt_budget_exceeded",
                "too many source receipts before decoding",
            ));
        }
        if kind != WireType::LengthDelimited {
            return Err(invalid());
        }
        let len = usize::try_from(decode_varint(&mut bytes).map_err(|_| invalid())?)
            .map_err(|_| invalid())?;
        let mut source = bytes.get(..len).ok_or_else(&invalid)?;
        bytes = &bytes[len..];
        let mut facts = 0;
        while !source.is_empty() {
            let (tag, kind) = decode_key(&mut source).map_err(|_| invalid())?;
            if tag == 5 {
                facts += 1;
                if facts > MAX_FACTS_PER_SOURCE {
                    return Err(Status::error(
                        "memory_receipt_budget_exceeded",
                        "too many source facts before decoding",
                    ));
                }
            }
            skip_field(kind, tag, &mut source, DecodeContext::default()).map_err(|_| invalid())?;
        }
    }
    Ok(())
}

fn wire_status(status: Status) -> wire::Status {
    wire::Status {
        ok: status.ok,
        code: status.code,
        message: status.message,
    }
}

pub fn encode_error(status: Status) -> Vec<u8> {
    wire::ContextMemoryIngestResponse {
        acknowledgment: Some(wire::ContextExplicitIngestResponse {
            protocol_version: ingest_wire::EXPLICIT_PROTOCOL_VERSION,
            status: Some(wire_status(status)),
            accepted: 0,
            failed: 0,
        }),
        sources: Vec::new(),
    }
    .encode_to_vec()
}

/// Retain full extraction reports for one model window, then encode fact receipts directly.
pub fn execute_request(
    engine: &TemporalEngine,
    mut request: ContextIngestExtractRequest,
) -> Vec<u8> {
    let mut sources = std::mem::take(&mut request.sources).into_iter();
    let mut bytes = Vec::new();
    let mut accepted = 0;
    let mut failed = 0;
    loop {
        let window = sources
            .by_ref()
            .take(super::ingest::CONTEXT_MODEL_SOURCE_WINDOW)
            .collect::<Vec<_>>();
        if window.is_empty() {
            break;
        }
        let report = ingest_extract_context(
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
        for extract in report.extracts {
            let receipt = wire::ContextMemorySourceReceipt {
                status: Some(wire_status(extract.status)),
                source_ref: extract.source_ref,
                node_hash: extract.node.node_hash,
                extracted_entity_count: extract.extracted_entity_count as u64,
                facts: extract
                    .facts
                    .into_iter()
                    .map(|fact| wire::ContextMemoryFact {
                        entity_hash: fact.entity_hash,
                        kind: fact.kind,
                        name: fact.name,
                        value: fact.value,
                    })
                    .collect(),
            };
            prost::encoding::message::encode(2, &receipt, &mut bytes);
        }
    }
    let status = if failed == 0 {
        Status::ok()
    } else if accepted > 0 {
        Status::error(
            "partial_context_ingest_extract_failure",
            format!("{failed} context sources failed"),
        )
    } else {
        Status::error(
            "context_ingest_extract_failed",
            "all context sources failed ingestion/extraction",
        )
    };
    // Protobuf fields may arrive in any order; emit the aggregate acknowledgment last.
    wire::ContextMemoryIngestResponse {
        acknowledgment: Some(wire::ContextExplicitIngestResponse {
            protocol_version: ingest_wire::EXPLICIT_PROTOCOL_VERSION,
            status: Some(wire_status(status)),
            accepted: accepted as u64,
            failed: failed as u64,
        }),
        sources: Vec::new(),
    }
    .encode(&mut bytes)
    .expect("a Vec can grow to encode the acknowledgment");
    bytes
}

pub(crate) fn decode_response(
    bytes: &[u8],
    expected: usize,
) -> Result<Vec<wire::ContextMemorySourceReceipt>, Status> {
    let invalid = || {
        Status::error(
            "invalid_memory_extraction_response",
            "incomplete or inconsistent extracted fact receipt",
        )
    };
    if bytes.len() > ingest_wire::EXPLICIT_MAX_BODY_BYTES
        || expected > ingest_wire::EXPLICIT_MAX_SOURCES
    {
        return Err(invalid());
    }
    check_budgets(bytes, expected)?;
    let response = wire::ContextMemoryIngestResponse::decode(bytes).map_err(|_| invalid())?;
    let ack = response.acknowledgment.ok_or_else(&invalid)?;
    if ack.protocol_version != ingest_wire::EXPLICIT_PROTOCOL_VERSION {
        return Err(invalid());
    }
    let status = ack.status.ok_or_else(&invalid)?;
    if !status.ok {
        return Err(if status.code.is_empty() {
            invalid()
        } else {
            Status::error(status.code, status.message)
        });
    }
    if ack.failed != 0 || ack.accepted != expected as u64 || response.sources.len() != expected {
        return Err(invalid());
    }
    Ok(response.sources)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request_with_sources(count: usize, tenant_hash: u64) -> ContextIngestExtractRequest {
        ContextIngestExtractRequest {
            shard_id: 1,
            tenant_hash,
            query: String::new(),
            start_time_ms: 0,
            end_time_ms: count as u64 + 2,
            max_events: 10,
            provider: Default::default(),
            sources: (0..count)
                .map(|index| ContextExtractRequest {
                    shard_id: 1,
                    tenant_hash,
                    source_kind: ContextSourceKind::Chat,
                    source_id: format!("window-source-{index}"),
                    title: "user".into(),
                    body: format!("Source {index} prefers Rust."),
                    timestamp_ms: index as u64 + 1,
                    provider: Default::default(),
                })
                .collect(),
        }
    }

    fn open_engine(root: &std::path::Path) -> TemporalEngine {
        let engine = TemporalEngine::with_local_dirs(
            16 << 20,
            root.join("cache"),
            root.join("blocks"),
            root.join("indexes"),
        );
        engine.load_shard(1);
        engine
    }

    #[test]
    fn inferred_memory_wire_windows_preserve_full_report_receipts() {
        let root = tempfile::tempdir().unwrap();
        let engine = open_engine(root.path());
        let count = super::super::ingest::CONTEXT_MODEL_SOURCE_WINDOW + 1;
        let request = request_with_sources(count, 42);
        let full = ingest_extract_context(&engine, request.clone());
        assert!(full.status.ok, "{:?}", full.status);
        let bytes = execute_request(&engine, request);
        let receipts = decode_response(&bytes, count).unwrap();
        assert_eq!(receipts.len(), full.extracts.len());
        for (receipt, extract) in receipts.iter().zip(full.extracts) {
            assert_eq!(receipt.status, Some(wire_status(extract.status)));
            assert_eq!(receipt.source_ref, extract.source_ref);
            assert_eq!(receipt.node_hash, extract.node.node_hash);
            assert_eq!(
                receipt.extracted_entity_count,
                extract.extracted_entity_count as u64
            );
            let expected_facts = extract
                .facts
                .into_iter()
                .map(|fact| wire::ContextMemoryFact {
                    entity_hash: fact.entity_hash,
                    kind: fact.kind,
                    name: fact.name,
                    value: fact.value,
                })
                .collect::<Vec<_>>();
            assert_eq!(receipt.facts, expected_facts);
        }
        assert_eq!(
            receipts.last().unwrap().source_ref,
            format!("window-source-{}", count - 1)
        );
    }

    #[test]
    fn inferred_memory_wire_windows_aggregate_partial_failure() {
        let root = tempfile::tempdir().unwrap();
        let engine = open_engine(root.path());
        let window = super::super::ingest::CONTEXT_MODEL_SOURCE_WINDOW;
        let mut request = request_with_sources(window + 1, 42);
        request.sources[window].body =
            "x".repeat(ContextWorkflowPolicy::default().max_extract_body_bytes + 1);
        let bytes = execute_request(&engine, request);
        let response = wire::ContextMemoryIngestResponse::decode(bytes.as_slice()).unwrap();
        let ack = response.acknowledgment.unwrap();
        assert_eq!((ack.accepted, ack.failed), (window as u64, 1));
        assert_eq!(response.sources.len(), window);
        assert_eq!(
            ack.status.unwrap().code,
            "partial_context_ingest_extract_failure"
        );
        assert_eq!(
            decode_response(&bytes, window + 1).unwrap_err().code,
            "partial_context_ingest_extract_failure"
        );
    }

    #[test]
    fn inferred_memory_wire_windows_preserve_all_failed_acknowledgment() {
        let root = tempfile::tempdir().unwrap();
        let engine = open_engine(root.path());
        let count = super::super::ingest::CONTEXT_MODEL_SOURCE_WINDOW + 1;
        let bytes = execute_request(&engine, request_with_sources(count, 0));
        let response = wire::ContextMemoryIngestResponse::decode(bytes.as_slice()).unwrap();
        let ack = response.acknowledgment.unwrap();
        assert_eq!((ack.accepted, ack.failed), (0, count as u64));
        assert!(response.sources.is_empty());
        assert_eq!(ack.status.unwrap().code, "context_ingest_extract_failed");
    }

    #[test]
    fn inferred_memory_wire_windows_preserve_empty_acknowledgment() {
        let root = tempfile::tempdir().unwrap();
        let engine = open_engine(root.path());
        let bytes = execute_request(&engine, request_with_sources(0, 42));
        assert!(decode_response(&bytes, 0).unwrap().is_empty());
    }

    #[test]
    fn inferred_memory_wire_preserves_extraction_errors_without_acknowledging_sources() {
        let bytes = wire::ContextMemoryIngestResponse {
            acknowledgment: Some(wire::ContextExplicitIngestResponse {
                protocol_version: ingest_wire::EXPLICIT_PROTOCOL_VERSION,
                status: Some(wire::Status {
                    ok: false,
                    code: "model_timeout".into(),
                    message: "model deadline".into(),
                }),
                accepted: 0,
                failed: 1,
            }),
            sources: Vec::new(),
        }
        .encode_to_vec();
        assert_eq!(
            decode_response(&bytes, 1).unwrap_err().code,
            "model_timeout"
        );
    }

    #[test]
    fn inferred_memory_wire_bounds_repeated_allocations_before_decoding() {
        let source = wire::ContextMemorySourceReceipt {
            facts: vec![wire::ContextMemoryFact::default(); MAX_FACTS_PER_SOURCE + 1],
            ..Default::default()
        };
        let response = wire::ContextMemoryIngestResponse {
            sources: vec![source],
            ..Default::default()
        }
        .encode_to_vec();
        assert_eq!(
            decode_response(&response, 1).unwrap_err().code,
            "memory_receipt_budget_exceeded"
        );
        let response = wire::ContextMemoryIngestResponse {
            sources: vec![wire::ContextMemorySourceReceipt::default(); 2],
            ..Default::default()
        }
        .encode_to_vec();
        assert_eq!(
            decode_response(&response, 1).unwrap_err().code,
            "memory_receipt_budget_exceeded"
        );
        assert!(decode_response(b"not protobuf", 1).is_err());
    }

    #[test]
    fn inferred_memory_wire_omits_raw_source_and_embedding_payloads() {
        let root = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            16 << 20,
            root.path().join("cache"),
            root.path().join("blocks"),
            root.path().join("indexes"),
        );
        engine.load_shard(1);
        let request = ContextIngestExtractRequest {
            shard_id: 1,
            tenant_hash: 42,
            query: String::new(),
            start_time_ms: 0,
            end_time_ms: 2,
            max_events: 1,
            provider: Default::default(),
            sources: vec![ContextExtractRequest {
                shard_id: 1,
                tenant_hash: 42,
                source_kind: ContextSourceKind::Chat,
                source_id: "source".into(),
                title: "user".into(),
                body: "RAW_PAYLOAD_NOT_A_RECEIPT ".repeat(1024),
                timestamp_ms: 1,
                provider: Default::default(),
            }],
        };
        let bytes = execute_request(&engine, request);
        assert!(
            bytes.len() < 1024,
            "receipt must not copy raw text or vectors: {}",
            bytes.len()
        );
        let receipts = decode_response(&bytes, 1).unwrap();
        assert_eq!(receipts[0].source_ref, "source");
        assert!(receipts[0].facts.is_empty());
        assert!(receipts[0].status.as_ref().unwrap().ok);
    }
}
