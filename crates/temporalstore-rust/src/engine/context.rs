use std::collections::{BTreeMap, HashMap, HashSet};

use crate::engine::constants::*;
use crate::page_store::LocalPageStore;
use crate::page_store::PageAddress;
use crate::types::{
    ContextAuditRef, ContextChildRef, ContextCompressionEvent, ContextEmbedding, ContextEntity,
    ContextEvent, ContextExtractedEventIndexes, ContextIndexLookup, ContextIndexRef, ContextNode,
    ContextPackAudit, ContextSummary, ContextSummaryDirtyMarker, ContextTraversedNode, ContextWire,
    FeaturePoint, InternalContextIndex, ShardId, Status,
};
use rustmtcache::MultiLayerCache;

use super::packed_pages::{
    read_feature_point_cached, read_feature_point_cold_with_cache_policy, ColdScanPackedPageCache,
};
use super::{read_page_bytes, stable_object_hash, ShardState};
pub(super) fn context_node_key(tenant_hash: u64, node_hash: u64) -> String {
    format!("ctx:node:{tenant_hash}:{node_hash}")
}

pub(super) fn context_event_key(tenant_hash: u64, node_hash: u64) -> String {
    format!("ctx:event:{tenant_hash}:{node_hash}")
}

pub(super) fn normalize_context_event_storage_keys(node_hash: u64, event: &mut ContextEvent) {
    if event.event_time_ms == 0 {
        event.event_time_ms = super::now_ms().max(1);
    }
    if event.ingestion_time_ms == 0 {
        event.ingestion_time_ms = event.event_time_ms;
    }
    if event.event_id_hash == 0 {
        event.event_id_hash = stable_object_hash(&format!(
            "context_event:{}:{}:{}",
            node_hash, event.event_time_ms, event.text
        ));
    }
}

pub(super) fn context_index_key(
    tenant_hash: u64,
    index_name: &str,
    index_value_hash: u64,
    scope_hash: u64,
) -> String {
    format!("ctxidx:{tenant_hash}:{index_name}:{index_value_hash}:{scope_hash}")
}

pub(super) fn context_index_disabled(
    indexes: &ContextExtractedEventIndexes,
    index: InternalContextIndex,
) -> bool {
    indexes.disabled_indexes.contains(&index)
}

pub(super) fn context_event_kind_hash(event: &ContextEvent) -> u64 {
    u64::from(event.event_type_code())
}

pub(super) fn context_audit_key(tenant_hash: u64, session_hash: u64) -> String {
    format!("ctx:audit:{tenant_hash}:{session_hash}")
}

pub(super) fn context_dirty_key(tenant_hash: u64, node_hash: u64) -> String {
    format!("ctx:dirty:{tenant_hash}:{node_hash}")
}

pub(super) fn context_entity_key(tenant_hash: u64, node_hash: u64, entity_hash: u64) -> String {
    format!("ctx:entity:{tenant_hash}:{node_hash}:{entity_hash}")
}

pub(super) fn context_entity_collection_key(tenant_hash: u64, node_hash: u64) -> String {
    format!("ctx:entity:{tenant_hash}:{node_hash}")
}

pub(super) fn context_child_key(tenant_hash: u64, parent_hash: u64) -> String {
    format!("ctx:child:{tenant_hash}:{parent_hash}")
}

pub(super) fn context_embedding_key(tenant_hash: u64, ref_hash: u64) -> String {
    format!("ctx:embedding:{tenant_hash}:{ref_hash}")
}

pub(super) fn context_summary_key(tenant_hash: u64, node_hash: u64, level: u32) -> String {
    format!("ctx:summary:{tenant_hash}:{node_hash}:{level}")
}

pub(super) fn context_compression_key(tenant_hash: u64, node_hash: u64) -> String {
    format!("ctx:compress:{tenant_hash}:{node_hash}")
}

pub(super) fn context_timeline_key(timestamp_ms: u64, disambiguator: u64) -> u64 {
    timestamp_ms
        .saturating_mul(CONTEXT_TIMELINE_FANOUT)
        .saturating_add(disambiguator % CONTEXT_TIMELINE_FANOUT)
}

pub(super) fn context_event_timeline_key_for_write(
    series: &BTreeMap<u64, PageAddress>,
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    event: &ContextEvent,
    first_write_only: bool,
) -> Option<u64> {
    let primary_time_ms = event.primary_time_ms();
    let start = context_timeline_key(primary_time_ms, event.event_id_hash);
    let end = context_timeline_end(primary_time_ms);
    let mut timeline_key = start;
    let mut page_cache = HashMap::new();
    while timeline_key < end {
        let Some(address) = series.get(&timeline_key) else {
            return Some(timeline_key);
        };
        if let Some(existing) = read_context_value_cached::<ContextEvent>(
            cache,
            page_store,
            shard_id,
            timeline_key,
            address,
            &mut page_cache,
        ) {
            if existing.event_id_hash == event.event_id_hash {
                return if first_write_only {
                    None
                } else {
                    Some(timeline_key)
                };
            }
        }
        timeline_key = timeline_key.saturating_add(1);
    }
    None
}

pub(super) fn context_timeline_start(timestamp_ms: u64) -> u64 {
    timestamp_ms.saturating_mul(CONTEXT_TIMELINE_FANOUT)
}

pub(super) fn context_timeline_end(timestamp_ms: u64) -> u64 {
    timestamp_ms
        .saturating_mul(CONTEXT_TIMELINE_FANOUT)
        .saturating_add(CONTEXT_TIMELINE_FANOUT)
}

pub(super) fn context_limit(limit: Option<usize>) -> usize {
    limit
        .unwrap_or(CONTEXT_DEFAULT_LIMIT)
        .max(1)
        .min(CONTEXT_MAX_LIMIT)
}

pub(super) fn context_bytes<T: ContextWire>(value: &T) -> Vec<u8> {
    value.encode_context_value()
}

pub(super) fn context_from_bytes<T: ContextWire>(bytes: &[u8]) -> Option<T> {
    T::decode_context_value(bytes)
}

pub(super) fn read_context_value_cold<T: ContextWire>(
    cache: Option<&MultiLayerCache>,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    timeline_key: u64,
    address: &PageAddress,
    packed_page_cache: &mut ColdScanPackedPageCache,
) -> Option<T> {
    let point = read_feature_point_cold_with_cache_policy(
        cache,
        page_store,
        shard_id,
        timeline_key,
        address,
        packed_page_cache,
    )?;
    context_from_bytes(&point.value)
}

pub(super) fn read_context_value_cached<T: ContextWire>(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    timeline_key: u64,
    address: &PageAddress,
    packed_page_cache: &mut HashMap<PageAddress, Option<Vec<FeaturePoint>>>,
) -> Option<T> {
    let point = read_feature_point_cached(
        cache,
        page_store,
        shard_id,
        timeline_key,
        address,
        packed_page_cache,
    )?;
    context_from_bytes(&point.value)
}

pub(super) fn context_event_matches_filter(
    event: &ContextEvent,
    current_valid_only: bool,
    as_of_ms: u64,
    end_time_ms: u64,
    kinds: &[u32],
    statuses: &[u32],
    min_confidence: f32,
    min_importance: f32,
) -> bool {
    if !kinds.is_empty() && !kinds.contains(&event.event_type_code()) {
        return false;
    }
    #[allow(deprecated)]
    if !statuses.is_empty() && !statuses.contains(&event.status) {
        return false;
    }
    if event.confidence < min_confidence || event.importance < min_importance {
        return false;
    }
    if current_valid_only {
        let as_of = if as_of_ms == 0 { end_time_ms } else { as_of_ms };
        if event.primary_time_ms() > as_of {
            return false;
        }
        #[allow(deprecated)]
        if event.valid_until_ms != 0 && event.valid_until_ms <= as_of {
            return false;
        }
    }
    true
}

pub(super) fn validate_context_required(ok: bool, message: &'static str) -> Result<(), Status> {
    if ok {
        Ok(())
    } else {
        Err(Status::error("invalid_argument", message))
    }
}

pub(super) fn validate_context_byte_len(
    name: &'static str,
    value_len: usize,
    max_len: usize,
) -> Result<(), Status> {
    if value_len <= max_len {
        Ok(())
    } else {
        Err(Status::error(
            "invalid_argument",
            format!("{name} is too large"),
        ))
    }
}

pub(super) fn validate_context_score(name: &'static str, value: f32) -> Result<(), Status> {
    if value.is_finite() && (0.0..=1.0).contains(&value) {
        Ok(())
    } else {
        Err(Status::error(
            "invalid_argument",
            format!("{name} must be in [0, 1]"),
        ))
    }
}

pub(super) fn validate_context_limit(limit: Option<usize>) -> Result<(), Status> {
    if limit.unwrap_or_default() <= CONTEXT_MAX_LIMIT {
        Ok(())
    } else {
        Err(Status::error("invalid_argument", "limit exceeds maximum"))
    }
}

pub(super) fn validate_context_range(start_time_ms: u64, end_time_ms: u64) -> Result<(), Status> {
    if end_time_ms > start_time_ms {
        validate_context_timestamp(start_time_ms)?;
        validate_context_timestamp(end_time_ms)
    } else {
        Err(Status::error(
            "invalid_argument",
            "end_time_ms must be greater than start_time_ms",
        ))
    }
}

pub(super) fn validate_context_timestamp(timestamp_ms: u64) -> Result<(), Status> {
    if timestamp_ms <= u64::MAX / CONTEXT_TIMELINE_FANOUT {
        Ok(())
    } else {
        Err(Status::error(
            "invalid_argument",
            "timestamp_ms is too large",
        ))
    }
}

pub(super) fn validate_context_index_name(index_name: &str) -> Result<(), Status> {
    validate_context_required(!index_name.is_empty(), "index_name is required")?;
    validate_context_byte_len("index_name", index_name.len(), CONTEXT_MAX_INDEX_NAME_BYTES)?;
    if index_name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        Ok(())
    } else {
        Err(Status::error(
            "invalid_argument",
            "index_name contains invalid characters",
        ))
    }
}

pub(super) fn validate_context_node(node: &ContextNode) -> Result<(), Status> {
    validate_context_required(node.node_hash != 0, "node_hash must be non-zero")?;
    validate_context_required(
        !node.canonical_name.is_empty(),
        "canonical_name is required",
    )?;
    validate_context_byte_len(
        "canonical_name",
        node.canonical_name.len(),
        CONTEXT_MAX_CANONICAL_NAME_BYTES,
    )?;
    validate_context_byte_len("l0", node.l0.len(), CONTEXT_MAX_L0_BYTES)?;
    validate_context_byte_len("l1_ref", node.l1_ref.len(), CONTEXT_MAX_REF_BYTES)?;
    validate_context_byte_len(
        "raw_metadata_ref",
        node.raw_metadata_ref.len(),
        CONTEXT_MAX_REF_BYTES,
    )?;
    if node.last_event_time_ms != 0 {
        validate_context_timestamp(node.last_event_time_ms)?;
    }
    Ok(())
}

pub(super) fn validate_context_event(event: &ContextEvent) -> Result<(), Status> {
    validate_context_required(
        event.primary_time_ms() != 0 && event.event_id_hash != 0,
        "ingestion_time_ms and event_id_hash must be non-zero",
    )?;
    #[allow(deprecated)]
    if event.valid_until_ms != 0 && event.valid_until_ms <= event.event_time_ms {
        return Err(Status::error(
            "invalid_argument",
            "valid_until_ms must be greater than event_time_ms",
        ));
    }
    validate_context_timestamp(event.primary_time_ms())?;
    if event.event_time_ms != 0 {
        validate_context_timestamp(event.event_time_ms)?;
    }
    #[allow(deprecated)]
    if event.valid_until_ms != 0 {
        validate_context_timestamp(event.valid_until_ms)?;
    }
    validate_context_score("confidence", event.confidence)?;
    validate_context_score("importance", event.importance)?;
    validate_context_byte_len("text", event.text.len(), CONTEXT_MAX_EVENT_TEXT_BYTES)?;
    #[allow(deprecated)]
    validate_context_byte_len("source_ref", event.source_ref.len(), CONTEXT_MAX_REF_BYTES)?;
    #[allow(deprecated)]
    if event.related_node_hashes.len() > CONTEXT_MAX_RELATED_NODE_HASHES {
        return Err(Status::error(
            "invalid_argument",
            "related_node_hashes exceeds maximum",
        ));
    }
    #[allow(deprecated)]
    validate_context_byte_len(
        "compact_attrs",
        event.compact_attrs.len(),
        CONTEXT_MAX_COMPACT_ATTRS_BYTES,
    )
}

pub(super) fn validate_context_filters(
    kinds: &[u32],
    statuses: &[u32],
    min_confidence: f32,
    min_importance: f32,
) -> Result<(), Status> {
    if kinds.len() > CONTEXT_MAX_FILTER_VALUES || statuses.len() > CONTEXT_MAX_FILTER_VALUES {
        return Err(Status::error("invalid_argument", "too many filter values"));
    }
    validate_context_score("min_confidence", min_confidence)?;
    validate_context_score("min_importance", min_importance)
}

pub(super) fn validate_context_index_ref(index_ref: &ContextIndexRef) -> Result<(), Status> {
    validate_context_required(
        index_ref.primary_node_hash != 0
            && index_ref.primary_event_time_ms != 0
            && index_ref.event_id_hash != 0,
        "invalid context index ref",
    )?;
    validate_context_timestamp(index_ref.primary_event_time_ms)
}

pub(super) fn context_index_ref_identity(index_ref: &ContextIndexRef) -> (u64, u64, u64) {
    (
        index_ref.primary_node_hash,
        index_ref.primary_event_time_ms,
        index_ref.event_id_hash,
    )
}

pub(super) fn validate_context_index_lookup(lookup: &ContextIndexLookup) -> Result<(), Status> {
    validate_context_index_name(&lookup.index_name)?;
    validate_context_required(lookup.index_value_hash != 0, "index_value_hash is required")?;
    validate_context_timestamp(lookup.start_time_ms)?;
    validate_context_timestamp(lookup.end_time_ms)?;
    validate_context_required(
        lookup.start_time_ms <= lookup.end_time_ms,
        "start_time_ms must be <= end_time_ms",
    )
}

pub(super) fn validate_context_extracted_indexes(
    event: &ContextEvent,
    indexes: &ContextExtractedEventIndexes,
) -> Result<(), Status> {
    if !context_index_disabled(indexes, InternalContextIndex::EventKind) {
        validate_context_required(
            context_event_kind_hash(event) != 0,
            "event kind is required",
        )?;
    }
    if !context_index_disabled(indexes, InternalContextIndex::Status) {
        validate_context_required(indexes.status_hash != 0, "status_hash is required")?;
    }
    if !context_index_disabled(indexes, InternalContextIndex::Source) {
        validate_context_required(indexes.source_hash != 0, "source_hash is required")?;
    }
    if !context_index_disabled(indexes, InternalContextIndex::EventTimeBucket) {
        validate_context_required(
            indexes.event_time_bucket_ms != 0,
            "event_time_bucket_ms is required",
        )?;
        validate_context_timestamp(indexes.event_time_bucket_ms)?;
    }
    if !context_index_disabled(indexes, InternalContextIndex::Entity) {
        for entity_hash in &indexes.entity_hashes {
            validate_context_required(*entity_hash != 0, "entity_hashes cannot contain zero")?;
        }
    }
    Ok(())
}

pub(super) fn validate_context_audit_ref(audit_ref: &ContextAuditRef) -> Result<(), Status> {
    validate_context_required(
        audit_ref.node_hash != 0 && audit_ref.event_time_ms != 0,
        "audit ref node_hash and event_time_ms are required",
    )?;
    validate_context_timestamp(audit_ref.event_time_ms)?;
    validate_context_byte_len(
        "audit ref reason",
        audit_ref.reason.len(),
        CONTEXT_MAX_REF_BYTES,
    )
}

pub(super) fn validate_context_pack_audit(audit: &ContextPackAudit) -> Result<(), Status> {
    validate_context_required(
        audit.session_hash != 0 && audit.request_time_ms != 0 && !audit.query_id.is_empty(),
        "session_hash, request_time_ms, and query_id are required",
    )?;
    validate_context_byte_len("query_id", audit.query_id.len(), CONTEXT_MAX_REF_BYTES)?;
    validate_context_timestamp(audit.request_time_ms)?;
    if audit.selected_refs.len() > CONTEXT_MAX_AUDIT_REFS
        || audit.blocked_refs.len() > CONTEXT_MAX_AUDIT_REFS
    {
        return Err(Status::error(
            "invalid_argument",
            "audit refs exceed maximum",
        ));
    }
    for audit_ref in &audit.selected_refs {
        validate_context_audit_ref(audit_ref)?;
    }
    for audit_ref in &audit.blocked_refs {
        validate_context_audit_ref(audit_ref)?;
    }
    Ok(())
}

pub(super) fn validate_context_dirty_marker(
    marker: &ContextSummaryDirtyMarker,
) -> Result<(), Status> {
    validate_context_required(
        marker.node_hash != 0 && marker.event_time_ms != 0,
        "node_hash and event_time_ms are required",
    )?;
    if marker.propagate_depth > CONTEXT_MAX_PROPAGATE_DEPTH {
        return Err(Status::error(
            "invalid_argument",
            "propagate_depth exceeds maximum",
        ));
    }
    validate_context_timestamp(marker.event_time_ms)
}

pub(super) fn validate_context_entity(entity: &ContextEntity) -> Result<(), Status> {
    validate_context_required(
        entity.entity_hash != 0 && entity.node_hash != 0 && entity.updated_at_ms != 0,
        "entity_hash, node_hash, and updated_at_ms are required",
    )?;
    validate_context_timestamp(entity.updated_at_ms)?;
    if entity.valid_from_ms != 0 {
        validate_context_timestamp(entity.valid_from_ms)?;
    }
    validate_context_score("confidence", entity.confidence)?;
    validate_context_byte_len(
        "entity name",
        entity.name.len(),
        CONTEXT_MAX_ENTITY_NAME_BYTES,
    )?;
    validate_context_byte_len(
        "entity value",
        entity.value.len(),
        CONTEXT_MAX_ENTITY_VALUE_BYTES,
    )?;
    if entity.source_event_hashes.len() > CONTEXT_MAX_AUDIT_REFS {
        return Err(Status::error(
            "invalid_argument",
            "source_event_hashes exceeds maximum",
        ));
    }
    Ok(())
}

pub(super) fn validate_context_child_ref(child_ref: &ContextChildRef) -> Result<(), Status> {
    validate_context_required(
        child_ref.parent_hash != 0 && child_ref.child_hash != 0 && child_ref.updated_at_ms != 0,
        "parent_hash, child_hash, and updated_at_ms are required",
    )?;
    validate_context_timestamp(child_ref.updated_at_ms)
}

pub(super) fn validate_context_embedding_vector(
    name: &'static str,
    vector: &[f32],
) -> Result<(), Status> {
    validate_context_required(!vector.is_empty(), "embedding vector is required")?;
    if vector.len() > CONTEXT_MAX_EMBEDDING_DIM {
        return Err(Status::error(
            "invalid_argument",
            format!("{name} dimension exceeds maximum"),
        ));
    }
    if vector.iter().all(|value| value.is_finite()) {
        Ok(())
    } else {
        Err(Status::error(
            "invalid_argument",
            format!("{name} contains non-finite value"),
        ))
    }
}

pub(super) fn validate_context_embedding(embedding: &ContextEmbedding) -> Result<(), Status> {
    validate_context_required(
        embedding.ref_hash != 0 && embedding.level != 0 && embedding.updated_at_ms != 0,
        "ref_hash, level, and updated_at_ms are required",
    )?;
    validate_context_timestamp(embedding.updated_at_ms)?;
    validate_context_embedding_vector("embedding vector", &embedding.vector)
}

pub(super) fn validate_context_summary(summary: &ContextSummary) -> Result<(), Status> {
    validate_context_required(
        summary.node_hash != 0 && summary.level != 0 && summary.valid_from_ms != 0,
        "node_hash, level, and valid_from_ms are required",
    )?;
    validate_context_timestamp(summary.valid_from_ms)?;
    validate_context_byte_len(
        "summary text",
        summary.text.len(),
        CONTEXT_MAX_SUMMARY_BYTES,
    )
}

pub(super) fn validate_context_compression_event(
    event: &ContextCompressionEvent,
) -> Result<(), Status> {
    validate_context_required(
        event.compression_id_hash != 0
            && event.node_hash != 0
            && event.source_start_ms != 0
            && event.source_end_ms != 0
            && event.compressed_time_ms != 0,
        "compression_id_hash, node_hash, source range, and compressed_time_ms are required",
    )?;
    validate_context_range(event.source_start_ms, event.source_end_ms)?;
    validate_context_timestamp(event.compressed_time_ms)?;
    validate_context_byte_len(
        "compression summary",
        event.summary.len(),
        CONTEXT_MAX_SUMMARY_BYTES,
    )
}

pub(super) fn load_context_children(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    object_key: &str,
) -> Vec<ContextChildRef> {
    let mut page_cache = HashMap::new();
    load_context_children_with_page_cache(
        cache,
        page_store,
        shard_id,
        shard,
        object_key,
        &mut page_cache,
    )
}

pub(super) fn load_context_children_with_page_cache(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    object_key: &str,
    page_cache: &mut HashMap<PageAddress, Option<Vec<FeaturePoint>>>,
) -> Vec<ContextChildRef> {
    shard
        .context_children
        .get(object_key)
        .map(|series| {
            let mut latest_by_child = HashMap::new();
            for child_ref in series.iter().filter_map(|(timeline_key, address)| {
                read_context_value_cached::<ContextChildRef>(
                    cache,
                    page_store,
                    shard_id,
                    *timeline_key,
                    address,
                    page_cache,
                )
            }) {
                latest_by_child
                    .entry(child_ref.child_hash)
                    .and_modify(|stored: &mut ContextChildRef| {
                        if child_ref.updated_at_ms > stored.updated_at_ms {
                            *stored = child_ref.clone();
                        }
                    })
                    .or_insert(child_ref);
            }
            latest_by_child.into_values().collect()
        })
        .unwrap_or_default()
}

pub(super) fn context_child_refs_may_exist(shard: &ShardState, object_key: &str) -> bool {
    shard
        .context_children
        .get(object_key)
        .map(|series| !series.is_empty())
        .unwrap_or(false)
}

pub(super) fn load_context_embedding(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    tenant_hash: u64,
    ref_hash: u64,
) -> Option<ContextEmbedding> {
    shard
        .context_embeddings
        .get(&context_embedding_key(tenant_hash, ref_hash))
        .and_then(|address| {
            read_page_bytes(cache, page_store, shard_id, address)
                .and_then(|bytes| context_from_bytes::<ContextEmbedding>(&bytes))
        })
}

pub(super) fn load_context_embedding_with_cache(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    tenant_hash: u64,
    ref_hash: u64,
    embedding_cache: &mut HashMap<u64, Option<ContextEmbedding>>,
) -> Option<ContextEmbedding> {
    if let Some(cached) = embedding_cache.get(&ref_hash) {
        return cached.clone();
    }
    let embedding =
        load_context_embedding(cache, page_store, shard_id, shard, tenant_hash, ref_hash);
    embedding_cache.insert(ref_hash, embedding.clone());
    embedding
}

pub(super) fn load_context_summaries(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    object_key: &str,
    as_of_ms: u64,
    limit: Option<usize>,
) -> Vec<ContextSummary> {
    shard
        .context_summaries
        .get(object_key)
        .map(|series| {
            let mut page_cache = HashMap::new();
            let mut summaries = series
                .range(0..context_timeline_end(as_of_ms))
                .rev()
                .filter_map(|(timeline_key, address)| {
                    read_context_value_cached::<ContextSummary>(
                        cache,
                        page_store,
                        shard_id,
                        *timeline_key,
                        address,
                        &mut page_cache,
                    )
                })
                .filter(|summary| summary.valid_from_ms <= as_of_ms)
                .take(context_limit(limit))
                .collect::<Vec<_>>();
            summaries.sort_by_key(|summary| summary.valid_from_ms);
            summaries
        })
        .unwrap_or_default()
}

pub(super) fn load_latest_context_summary(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    object_key: &str,
    as_of_ms: u64,
) -> Option<ContextSummary> {
    load_context_summaries(
        cache,
        page_store,
        shard_id,
        shard,
        object_key,
        as_of_ms,
        Some(1),
    )
    .into_iter()
    .next()
}

pub(super) fn load_context_compression_events(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    tenant_hash: u64,
    node_hashes: &[u64],
    start_time_ms: u64,
    end_time_ms: u64,
    limit: Option<usize>,
) -> Vec<ContextCompressionEvent> {
    let event_limit = context_limit(limit);
    let trim_threshold = event_limit.saturating_mul(2).max(event_limit);
    let mut events = Vec::with_capacity(event_limit);
    let mut seen_nodes = HashSet::new();
    let mut page_cache = HashMap::new();
    for node_hash in node_hashes
        .iter()
        .copied()
        .filter(|node_hash| *node_hash != 0 && seen_nodes.insert(*node_hash))
    {
        let object_key = context_compression_key(tenant_hash, node_hash);
        if let Some(series) = shard.context_compressions.get(&object_key) {
            for event in series.iter().filter_map(|(timeline_key, address)| {
                read_context_value_cached::<ContextCompressionEvent>(
                    cache,
                    page_store,
                    shard_id,
                    *timeline_key,
                    address,
                    &mut page_cache,
                )
                .filter(|event| {
                    event.source_end_ms >= start_time_ms && event.source_start_ms <= end_time_ms
                })
            }) {
                events.push(event);
                if events.len() > trim_threshold {
                    sort_truncate_context_compression_events(&mut events, event_limit);
                }
            }
        }
    }
    sort_truncate_context_compression_events(&mut events, event_limit);
    events
}

pub(super) fn load_context_compression_events_cold(
    cache: Option<&MultiLayerCache>,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    tenant_hash: u64,
    node_hashes: &[u64],
    start_time_ms: u64,
    end_time_ms: u64,
    limit: Option<usize>,
) -> Vec<ContextCompressionEvent> {
    let event_limit = context_limit(limit);
    let trim_threshold = event_limit.saturating_mul(2).max(event_limit);
    let mut events = Vec::with_capacity(event_limit);
    let mut seen_nodes = HashSet::new();
    let mut packed_page_cache = ColdScanPackedPageCache::default();
    for node_hash in node_hashes
        .iter()
        .copied()
        .filter(|node_hash| *node_hash != 0 && seen_nodes.insert(*node_hash))
    {
        let object_key = context_compression_key(tenant_hash, node_hash);
        if let Some(series) = shard.context_compressions.get(&object_key) {
            for event in series.iter().filter_map(|(timeline_key, address)| {
                read_context_value_cold::<ContextCompressionEvent>(
                    cache,
                    page_store,
                    shard_id,
                    *timeline_key,
                    address,
                    &mut packed_page_cache,
                )
                .filter(|event| {
                    event.source_end_ms >= start_time_ms && event.source_start_ms <= end_time_ms
                })
            }) {
                events.push(event);
                if events.len() > trim_threshold {
                    sort_truncate_context_compression_events(&mut events, event_limit);
                }
            }
        }
    }
    sort_truncate_context_compression_events(&mut events, event_limit);
    events
}

fn sort_truncate_context_compression_events(
    events: &mut Vec<ContextCompressionEvent>,
    limit: usize,
) {
    events.sort_by(|left, right| {
        right
            .source_end_ms
            .cmp(&left.source_end_ms)
            .then_with(|| right.compressed_time_ms.cmp(&left.compressed_time_ms))
            .then_with(|| left.compression_id_hash.cmp(&right.compression_id_hash))
    });
    events.truncate(limit);
}

pub(super) fn cosine_similarity(left: &[f32], right: &[f32]) -> f32 {
    if left.is_empty() || left.len() != right.len() {
        return 0.0;
    }
    let mut dot = 0.0_f32;
    let mut left_norm = 0.0_f32;
    let mut right_norm = 0.0_f32;
    for (l, r) in left.iter().zip(right.iter()) {
        dot += l * r;
        left_norm += l * l;
        right_norm += r * r;
    }
    if left_norm <= 0.0 || right_norm <= 0.0 {
        return 0.0;
    }
    dot / (left_norm.sqrt() * right_norm.sqrt())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn traverse_context_tree(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    tenant_hash: u64,
    start_node_hash: u64,
    query_vector: &[f32],
    max_depth: Option<u32>,
    top_k_per_depth: Option<usize>,
    max_children_scored_per_parent: Option<usize>,
    max_candidate_nodes: Option<usize>,
    leaf_only: bool,
) -> Vec<ContextTraversedNode> {
    let max_depth = max_depth.unwrap_or(6).min(CONTEXT_MAX_TRAVERSAL_DEPTH);
    let top_k = top_k_per_depth
        .unwrap_or(CONTEXT_DEFAULT_TRAVERSAL_TOP_K)
        .max(1)
        .min(CONTEXT_MAX_LIMIT);
    let child_limit = max_children_scored_per_parent
        .unwrap_or(CONTEXT_MAX_LIMIT)
        .max(1)
        .min(CONTEXT_MAX_LIMIT);
    let candidate_limit = max_candidate_nodes
        .unwrap_or(CONTEXT_DEFAULT_TRAVERSAL_CANDIDATES)
        .max(1)
        .min(CONTEXT_MAX_LIMIT);
    let mut frontier = vec![ContextTraversedNode {
        node_hash: start_node_hash,
        depth: 0,
        score: 1.0,
    }];
    let mut results = Vec::new();
    let mut embedding_cache = HashMap::new();
    for depth in 1..=max_depth {
        let mut scored_layer = Vec::new();
        let mut child_page_cache = HashMap::new();
        for parent in &frontier {
            let child_key = context_child_key(tenant_hash, parent.node_hash);
            let mut children = load_context_children_with_page_cache(
                cache,
                page_store,
                shard_id,
                shard,
                &child_key,
                &mut child_page_cache,
            );
            children.sort_by_key(|child_ref| (child_ref.updated_at_ms, child_ref.child_hash));
            children.truncate(child_limit);
            for child in children {
                let Some(embedding) = load_context_embedding_with_cache(
                    cache,
                    page_store,
                    shard_id,
                    shard,
                    tenant_hash,
                    child.child_hash,
                    &mut embedding_cache,
                ) else {
                    continue;
                };
                let score = cosine_similarity(query_vector, &embedding.vector);
                if score > 0.0 {
                    scored_layer.push(ContextTraversedNode {
                        node_hash: child.child_hash,
                        depth,
                        score,
                    });
                }
            }
        }
        scored_layer.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.node_hash.cmp(&right.node_hash))
        });
        scored_layer.truncate(top_k);
        let mut next_frontier = Vec::new();
        for node in scored_layer {
            let child_key = context_child_key(tenant_hash, node.node_hash);
            let is_leaf = !context_child_refs_may_exist(shard, &child_key);
            next_frontier.push(node.clone());
            if !leaf_only || is_leaf {
                results.push(node);
                if results.len() >= candidate_limit {
                    return results;
                }
            }
        }
        if next_frontier.is_empty() {
            break;
        }
        frontier = next_frontier;
    }
    results
}

pub(super) fn build_context_compression_event(
    tenant_hash: u64,
    node_hash: u64,
    source_start_ms: u64,
    source_end_ms: u64,
    compressed_time_ms: u64,
    selected: &[ContextEvent],
    truncated: bool,
) -> ContextCompressionEvent {
    let mut summary = format!("Temporal compression window {source_start_ms}-{source_end_ms}:");
    for event in selected {
        let mut text = event.text.clone();
        if text.len() > CONTEXT_MAX_COMPRESSION_SNIPPET_BYTES {
            text.truncate(CONTEXT_MAX_COMPRESSION_SNIPPET_BYTES);
        }
        summary.push(' ');
        summary.push_str(&text);
    }
    if truncated {
        summary.push_str(" additional source events truncated.");
    }
    if summary.len() > CONTEXT_MAX_SUMMARY_BYTES {
        summary.truncate(CONTEXT_MAX_SUMMARY_BYTES);
    }
    ContextCompressionEvent {
        compression_id_hash: stable_object_hash(&format!(
            "{tenant_hash}:{node_hash}:{source_start_ms}:{source_end_ms}:{}",
            selected
                .iter()
                .map(|event| event.event_id_hash.to_string())
                .collect::<Vec<_>>()
                .join(",")
        )),
        node_hash,
        source_start_ms,
        source_end_ms,
        compressed_time_ms: if compressed_time_ms == 0 {
            source_end_ms
        } else {
            compressed_time_ms
        },
        summary,
    }
}
