// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

pub(super) fn memory_normalize_text(mut text: String, infer: bool) -> String {
    if infer {
        let leading = text.len() - text.trim_start().len();
        if leading != 0 {
            drop(text.drain(..leading));
        }
        text.truncate(text.trim_end().len());
    }
    text
}

pub(super) fn memory_metadata_fits(metadata: &Value, sources: usize, infer: bool) -> bool {
    struct Budget(usize);
    impl std::io::Write for Budget {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.0 {
                return Err(std::io::Error::other("metadata budget exceeded"));
            }
            self.0 -= bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let copies = sources.saturating_mul(if infer {
        crate::context_workflow::memory_wire::MAX_FACTS_PER_SOURCE
    } else {
        1
    });
    if copies == 0 {
        return true;
    }
    // Count through serde without retaining another encoded metadata buffer.
    serde_json::to_writer(
        Budget(crate::context_workflow::ingest_wire::EXPLICIT_MAX_BODY_BYTES / copies),
        metadata,
    )
    .is_ok()
}

impl ProxyService {
    pub(super) fn memory_extract_records(
        &self,
        scope: &context::ProxyContextScope,
        tenant_hash: u64,
        records: &mut [MemoryRecord],
        roles: Vec<String>,
        provider: Option<Value>,
    ) -> Result<Vec<u8>, (u16, Vec<u8>)> {
        let provider = provider
            .map(serde_json::from_value::<crate::ContextModelProviderConfig>)
            .transpose()
            .map_err(|error| self.memory_bad_request(error.to_string()))?
            .unwrap_or_default();
        self.context_extract_facts(
            scope,
            memory_extract_request(
                self.context_shard_id(tenant_hash),
                tenant_hash,
                records,
                roles,
                provider,
            ),
        )
    }
}

fn memory_extract_request(
    shard_id: crate::types::ShardId,
    tenant_hash: u64,
    records: &mut [MemoryRecord],
    roles: Vec<String>,
    provider: crate::ContextModelProviderConfig,
) -> crate::ContextIngestExtractRequest {
    let sources = records
        .iter_mut()
        .zip(roles)
        .map(|(record, title)| crate::ContextExtractRequest {
            shard_id,
            tenant_hash,
            source_kind: crate::ContextSourceKind::Chat,
            source_id: record.id.clone(),
            title,
            body: std::mem::take(&mut record.memory),
            timestamp_ms: record.updated_at_ms,
            provider: crate::ContextModelProviderConfig {
                provider_name: String::new(),
                ..Default::default()
            },
        })
        .collect::<Vec<_>>();
    crate::ContextIngestExtractRequest {
        shard_id,
        tenant_hash,
        query: String::new(),
        start_time_ms: 0,
        end_time_ms: records
            .iter()
            .map(|row| row.updated_at_ms)
            .max()
            .unwrap_or(0)
            .saturating_add(1),
        max_events: sources.len(),
        sources,
        provider,
    }
}

#[cfg(test)]
#[path = "memory_input_ownership_tests.rs"]
mod input_ownership_tests;

pub(super) fn memory_fact_ref(reference: &str) -> Option<(&str, u64)> {
    let (source, entity) = reference.strip_prefix("entity:")?.rsplit_once(':')?;
    let (tenant, suffix) = source.split_once('-')?;
    if tenant.len() != 16 || suffix.len() != 16 || entity.len() != 16 {
        return None;
    }
    u64::from_str_radix(tenant, 16).ok()?;
    u64::from_str_radix(suffix, 16).ok()?;
    Some((source, u64::from_str_radix(entity, 16).ok()?))
}

pub(super) fn memory_entity_lookup_hash(node: u64, entity: u64) -> u64 {
    context::stable_scope_hash(&format!("memory-entity:{node:016x}:{entity:016x}"))
}

pub(super) fn memory_record_lookup_hash(tenant: u64, record: &MemoryRecord) -> u64 {
    let node = memory_record_node_hash(tenant, record);
    memory_fact_ref(&record.context_ref)
        .map(|(_, entity)| memory_entity_lookup_hash(node, entity))
        .unwrap_or(node)
}

pub(super) fn memory_block_lookup_hash(
    tenant: u64,
    block: &crate::context_workflow::ContextBlock,
) -> Option<u64> {
    if block.record_type != "entity" {
        return None;
    }
    let prefix = format!("context://tenant/{tenant:016x}/entity/");
    let entity = block.uri.strip_prefix(&prefix)?;
    if entity.len() != 16 {
        return None;
    }
    Some(memory_entity_lookup_hash(
        block.node_hash,
        u64::from_str_radix(entity, 16).ok()?,
    ))
}

pub(super) fn memory_inferred_records(
    tenant: u64,
    sources: Vec<MemoryRecord>,
    body: &[u8],
) -> Result<Vec<MemoryRecord>, Status> {
    let invalid = || {
        Status::error(
            "invalid_memory_extraction_response",
            "incomplete or inconsistent extracted fact receipt",
        )
    };
    let receipts = crate::context_workflow::memory_wire::decode_response(body, sources.len())?;
    let mut sources = sources
        .into_iter()
        .map(|record| (record.id.clone(), record))
        .collect::<HashMap<_, _>>();
    let mut records = Vec::new();
    for extract in receipts {
        let source = sources.remove(&extract.source_ref).ok_or_else(&invalid)?;
        if !extract.status.is_some_and(|status| status.ok)
            || extract.node_hash != memory_record_node_hash(tenant, &source)
            || extract.facts.len() as u64 != extract.extracted_entity_count
            || extract.facts.len() > crate::context_workflow::memory_wire::MAX_FACTS_PER_SOURCE
        {
            return Err(invalid());
        }
        let mut facts = BTreeMap::new();
        for fact in extract.facts {
            let entity = crate::context_workflow::ContextExtractedEntity {
                kind: fact.kind.clone(),
                name: fact.name.clone(),
                value: fact.value.clone(),
            };
            if fact.name.trim().is_empty()
                || fact.value.trim().is_empty()
                || fact.name.len() > 256
                || fact.value.len() > 1024
                || fact.kind.len() > 32
                || fact.entity_hash
                    != crate::context_workflow::context_extracted_entity_hash(&entity)
            {
                return Err(invalid());
            }
            // The entity store overwrites duplicate identities in command order.
            facts.insert(fact.entity_hash, fact);
        }
        for (entity, fact) in facts {
            records.push(MemoryRecord {
                id: memory_id(tenant, source.created_at_ms, entity, &source.id),
                context_ref: format!("entity:{}:{entity:016x}", source.id),
                memory: format!("{}: {}", fact.name, fact.value),
                metadata: source.metadata.clone(),
                tenant_id: source.tenant_id.clone(),
                account_id: source.account_id.clone(),
                user_id: source.user_id.clone(),
                agent_id: source.agent_id.clone(),
                run_id: source.run_id.clone(),
                created_at_ms: source.created_at_ms,
                updated_at_ms: source.updated_at_ms,
            });
        }
    }
    if !sources.is_empty() {
        return Err(invalid());
    }
    Ok(records)
}
