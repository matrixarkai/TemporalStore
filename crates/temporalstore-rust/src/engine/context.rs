// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::HashMap;

use crate::engine::constants::*;
use crate::block_store::LocalBlockStore;
use crate::block_store::BlockAddress;
use crate::types::{
    ContextAuditRef, ContextChildRef, ContextCompressionEvent, ContextEntity,
    ContextEvent, ContextExtractedEventIndexes, ContextIndexLookup, ContextIndexRef, ContextNode,
    ContextPackAudit, ContextSummary, ContextTraversedNode, ContextWire,
    FeaturePoint, InternalContextIndex, ShardId, Status,
};
use matrixcache::MultiLayerCache;

use super::packed_pages::{read_feature_point, read_feature_point_cached, read_feature_point_cold};
use super::{read_page_bytes, stable_object_hash, ShardState};
/// `prefix` then two decimal parts, joined by colons, in one allocation.
///
/// `format!` would take two: the String it returns, plus one inside the formatting machinery.
/// `write!` into a String that already has capacity takes none, so building the key directly halves
/// its cost. These run on the hot path -- `context_node_key` once per retrieval candidate -- and the
/// bytes are identical to the format strings they replace, which matters because these keys are
/// stored in the shard's maps and the bucket index and compared against keys already written.
fn context_key_2(prefix: &str, first: u64, second: u64) -> String {
    use std::fmt::Write as _;
    // Prefix, two u64 at 20 digits each, and the colon between them.
    let mut key = String::with_capacity(prefix.len() + 41);
    key.push_str(prefix);
    let _ = write!(key, "{first}:{second}");
    key
}

/// As [`context_key_2`], with a third part.
fn context_key_3(prefix: &str, first: u64, second: u64, third: u64) -> String {
    use std::fmt::Write as _;
    let mut key = String::with_capacity(prefix.len() + 62);
    key.push_str(prefix);
    let _ = write!(key, "{first}:{second}:{third}");
    key
}

pub(super) fn context_node_key(tenant_hash: u64, node_hash: u64) -> String {
    context_key_2("ctx:node:", tenant_hash, node_hash)
}

pub(super) fn context_event_key(tenant_hash: u64, node_hash: u64) -> String {
    context_key_2("ctx:event:", tenant_hash, node_hash)
}

pub(super) fn normalize_context_event_storage_keys(node_hash: u64, event: &mut ContextEvent) {
    if event.event_time_ms == 0 {
        event.event_time_ms = super::resolve_now_ms().max(1);
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
    context_key_2("ctx:audit:", tenant_hash, session_hash)
}

pub(super) fn context_dirty_key(tenant_hash: u64, node_hash: u64) -> String {
    context_key_2("ctx:dirty:", tenant_hash, node_hash)
}

pub(super) fn context_embedding_dirty_key(tenant_hash: u64, node_hash: u64) -> String {
    context_key_2("ctx:embdirty:", tenant_hash, node_hash)
}

pub(super) fn context_entity_key(tenant_hash: u64, node_hash: u64, entity_hash: u64) -> String {
    context_key_3("ctx:entity:", tenant_hash, node_hash, entity_hash)
}

/// Split a persisted per-entity key back into (collection key, entity hash).
///
/// The index stores entities grouped by node, but the PERSISTED page entry still carries the
/// per-entity key `ctx:entity:{tenant}:{node}:{entity_hash}` -- that is what keeps the on-disk
/// shape identical across this fold in both directions. Load calls this to rebuild the grouping.
/// Returns None for a key that does not carry a numeric entity hash, so a malformed or
/// foreign-shaped entry is skipped rather than folded under a wrong node.
pub(super) fn split_context_entity_key(object_key: &str) -> Option<(String, u64)> {
    let (collection_key, entity_hash) = object_key.rsplit_once(':')?;
    if !collection_key.starts_with("ctx:entity:") {
        return None;
    }
    // The collection key itself ends in the node hash, so it must still hold four segments
    // (ctx, entity, tenant, node) once the entity hash is removed -- otherwise this key WAS a
    // collection key and splitting it would silently reinterpret the node hash as an entity.
    if collection_key.split(':').count() != 4 {
        return None;
    }
    Some((collection_key.to_string(), entity_hash.parse::<u64>().ok()?))
}

pub(super) fn context_entity_collection_key(tenant_hash: u64, node_hash: u64) -> String {
    context_key_2("ctx:entity:", tenant_hash, node_hash)
}

pub(super) fn context_child_key(tenant_hash: u64, parent_hash: u64) -> String {
    context_key_2("ctx:child:", tenant_hash, parent_hash)
}

pub(super) fn context_summary_key(tenant_hash: u64, node_hash: u64, level: u32) -> String {
    context_key_3("ctx:summary:", tenant_hash, node_hash, u64::from(level))
}

pub(super) fn context_compression_key(tenant_hash: u64, node_hash: u64) -> String {
    context_key_2("ctx:compress:", tenant_hash, node_hash)
}

/// Configurable temporal-compression policy for a context node.
///
/// Hierarchical session archival: keep a recent
/// window of raw memory and progressively compress/fade older windows into summary
/// records once a threshold is crossed. Here the trigger is per-node and evidence-
/// oriented (count- or age-based) rather than per-session token budget, but the shape
/// is the same. Disabled by default; every field is overridable via environment.
#[derive(Debug, Clone)]
pub(super) struct ContextCompressionPolicy {
    pub(super) enabled: bool,
    pub(super) max_raw_events_per_node: usize,
    pub(super) keep_recent_events: usize,
    pub(super) window_size: usize,
    pub(super) max_event_age_ms: u64,
}

impl Default for ContextCompressionPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            max_raw_events_per_node: 64,
            keep_recent_events: 32,
            window_size: 32,
            max_event_age_ms: 30 * 24 * 60 * 60 * 1000, // 30 days
        }
    }
}

/// How many nodes each traversal depth keeps, when the request does not say.
///
/// Retrieval breadth is a deployment decision, not a build-time one: a corpus of short playbook
/// steps wants a wider frontier than one of long prose, and the right value is found by measuring
/// recall on the operator's own documents. The request-level `top_k_per_depth` still wins where a
/// caller sets it; this only supplies the default it falls back to.
///
/// Widening costs latency roughly linearly -- traversal work is bounded by depth x top_k x
/// children -- which is why it is capped at CONTEXT_MAX_LIMIT rather than left open.
pub(super) fn default_traversal_top_k() -> usize {
    std::env::var("MATRIXARK_RETRIEVAL_TRAVERSAL_TOP_K")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(CONTEXT_DEFAULT_TRAVERSAL_TOP_K)
}

/// Ceiling on nodes collected across the whole traversal, when the request does not say.
pub(super) fn default_traversal_candidates() -> usize {
    std::env::var("MATRIXARK_RETRIEVAL_MAX_CANDIDATES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(CONTEXT_DEFAULT_TRAVERSAL_CANDIDATES)
}

pub(super) fn context_compression_policy_from_env() -> ContextCompressionPolicy {
    fn env_usize(name: &str, default: usize) -> usize {
        std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
    }
    fn env_u64(name: &str, default: u64) -> u64 {
        std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
    }
    fn env_bool(name: &str, default: bool) -> bool {
        std::env::var(name)
            .ok()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(default)
    }
    let d = ContextCompressionPolicy::default();
    ContextCompressionPolicy {
        enabled: env_bool("MATRIXARK_CONTEXT_COMPRESSION_ENABLED", d.enabled),
        max_raw_events_per_node: env_usize(
            "MATRIXARK_CONTEXT_COMPRESSION_MAX_RAW_EVENTS",
            d.max_raw_events_per_node,
        )
        .max(1),
        keep_recent_events: env_usize(
            "MATRIXARK_CONTEXT_COMPRESSION_KEEP_RECENT",
            d.keep_recent_events,
        ),
        window_size: env_usize("MATRIXARK_CONTEXT_COMPRESSION_WINDOW", d.window_size).max(1),
        max_event_age_ms: env_u64("MATRIXARK_CONTEXT_COMPRESSION_MAX_AGE_MS", d.max_event_age_ms),
    }
}

/// One planned compression window: a contiguous run of old event times to fold
/// into a single `ContextCompressionEvent`.
pub(super) struct ContextCompressionWindow {
    pub(super) start_ms: u64,
    pub(super) end_ms: u64,
    pub(super) count: usize,
}

/// Decide the next window (if any) to compress for a node, given its event times
/// sorted ascending, the per-node high-water mark (latest already-compressed event
/// time), and the current time. Keeps the newest `keep_recent_events` raw; only
/// compresses events past the watermark; bounds a pass to `window_size` events.
/// Pure and deterministic so it can be unit/example tested.
pub(super) fn plan_next_compression_window(
    policy: &ContextCompressionPolicy,
    event_times_sorted: &[u64],
    watermark: u64,
    now_ms: u64,
) -> Option<ContextCompressionWindow> {
    if !policy.enabled {
        return None;
    }
    let len = event_times_sorted.len();
    if len == 0 {
        return None;
    }
    let keep = policy.keep_recent_events.min(len);
    let compressible = &event_times_sorted[..len - keep];
    let oldest = match compressible.first() {
        Some(t) => *t,
        None => return None,
    };
    let count_trigger = len > policy.max_raw_events_per_node;
    let age_cutoff = now_ms.saturating_sub(policy.max_event_age_ms);
    let age_trigger = oldest < age_cutoff;
    if !count_trigger && !age_trigger {
        return None;
    }
    // Only events strictly newer than the watermark are still pending.
    let start_idx = compressible.partition_point(|&t| t <= watermark);
    let pending = &compressible[start_idx..];
    if pending.is_empty() {
        return None;
    }
    // Under a count trigger take the oldest pending window; under an age-only
    // trigger restrict to events older than the cutoff.
    let eligible: &[u64] = if count_trigger {
        pending
    } else {
        let end = pending.partition_point(|&t| t < age_cutoff);
        &pending[..end]
    };
    if eligible.is_empty() {
        return None;
    }
    let window = &eligible[..eligible.len().min(policy.window_size)];
    Some(ContextCompressionWindow {
        start_ms: *window.first().unwrap(),
        end_ms: *window.last().unwrap(),
        count: window.len(),
    })
}

pub(super) fn context_timeline_key(timestamp_ms: u64, disambiguator: u64) -> u64 {
    timestamp_ms
        .saturating_mul(CONTEXT_TIMELINE_FANOUT)
        .saturating_add(disambiguator % CONTEXT_TIMELINE_FANOUT)
}

/// Events of one node within a time window, oldest first, as (timeline_key, address).
///
/// The primary event map is keyed by event id hash so update and delete can address a single
/// event in log n. Hash order is effectively random, so a time window is not a contiguous range
/// there; it is a contiguous range in `context_event_timeline`, which maps timeline_key ->
/// event id. This ranges over that index and dereferences each hit into the primary map, so a
/// time-windowed read stays log n + k instead of scanning the node's entire series.
///
/// Returns a DoubleEndedIterator because every caller reads newest-first (`.rev()`): retrieval
/// wants recent, serving-relevant context before cold history.
pub(super) fn context_event_time_range<'a>(
    shard: &'a super::state::ShardState,
    object_key: &str,
    start_time_ms: u64,
    end_time_ms: u64,
) -> impl DoubleEndedIterator<Item = (u64, &'a BlockAddress)> + 'a {
    let series = shard.context_events.get(object_key);
    let start = context_timeline_start(start_time_ms);
    let end = context_timeline_end(end_time_ms);
    shard
        .context_event_timeline
        .get(object_key)
        .into_iter()
        .flat_map(move |timeline| timeline.range(start..end))
        .filter_map(move |(timeline_key, event_id_hash)| {
            series
                .and_then(|series| series.get(event_id_hash))
                .map(|address| (*timeline_key, address))
        })
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

pub(super) fn read_context_value<T: ContextWire>(
    cache: &MultiLayerCache,
    page_store: &LocalBlockStore,
    shard_id: ShardId,
    timeline_key: u64,
    address: &BlockAddress,
) -> Option<T> {
    let point = read_feature_point(cache, page_store, shard_id, timeline_key, address)?;
    context_from_bytes(&point.value)
}

pub(super) fn read_context_value_cold<T: ContextWire>(
    page_store: &LocalBlockStore,
    timeline_key: u64,
    address: &BlockAddress,
) -> Option<T> {
    let point = read_feature_point_cold(page_store, timeline_key, address)?;
    context_from_bytes(&point.value)
}

pub(super) fn read_context_value_cached<T: ContextWire>(
    cache: &MultiLayerCache,
    page_store: &LocalBlockStore,
    shard_id: ShardId,
    timeline_key: u64,
    address: &BlockAddress,
    packed_page_cache: &mut HashMap<BlockAddress, Option<Vec<FeaturePoint>>>,
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

pub(super) fn validate_context_dirty_node(
    node_hash: u64,
    event_time_ms: u64,
    _reason: u32,
    propagate_depth: u32,
) -> Result<(), Status> {
    validate_context_required(
        node_hash != 0 && event_time_ms != 0,
        "node_hash and event_time_ms are required",
    )?;
    if propagate_depth > CONTEXT_MAX_PROPAGATE_DEPTH {
        return Err(Status::error(
            "invalid_argument",
            "propagate_depth exceeds maximum",
        ));
    }
    validate_context_timestamp(event_time_ms)
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
    page_store: &LocalBlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    object_key: &str,
) -> Vec<ContextChildRef> {
    shard
        .context_children
        .get(object_key)
        .map(|series| {
            series
                .iter()
                .filter_map(|(timeline_key, address)| {
                    read_context_value::<ContextChildRef>(
                        cache,
                        page_store,
                        shard_id,
                        *timeline_key,
                        address,
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

pub(super) fn load_context_node(
    cache: &MultiLayerCache,
    page_store: &LocalBlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    tenant_hash: u64,
    node_hash: u64,
) -> Option<ContextNode> {
    // Same two-step lookup as the ContextGetNode command: current nodes live in the hashes map
    // under the CONTEXT_NODE_FIELD slot; context_nodes is the pre-hashes location still read for
    // blocks written before that move.
    let object_key = context_node_key(tenant_hash, node_hash);
    shard
        .hashes
        .get(&object_key)
        .and_then(|fields| fields.get(CONTEXT_NODE_FIELD))
        .or_else(|| shard.context_nodes.get(&object_key))
        .and_then(|address| {
            // Shared, not copied: the bytes are parsed here and dropped, so owning them costs a
            // page-sized memcpy and an allocation for nothing.
            super::read_page_shared(cache, page_store, shard_id, address)
                .and_then(|bytes| context_from_bytes::<ContextNode>(&bytes))
        })
}

pub(super) fn load_context_summaries(
    cache: &MultiLayerCache,
    page_store: &LocalBlockStore,
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
            series
                .range(0..context_timeline_end(as_of_ms))
                .take(context_limit(limit))
                .filter_map(|(timeline_key, address)| {
                    read_context_value::<ContextSummary>(
                        cache,
                        page_store,
                        shard_id,
                        *timeline_key,
                        address,
                    )
                })
                .filter(|summary| summary.valid_from_ms <= as_of_ms)
                .collect()
        })
        .unwrap_or_default()
}

/// The NEWEST summary at or before `as_of_ms`, decoding one record in the ordinary case.
///
/// `load_context_summaries` with `Some(1)` returns the OLDEST match, not the newest: the series is
/// keyed by `context_timeline_key`, which is `timestamp_ms * FANOUT + disambiguator` and therefore
/// ascends with time, so `range(0..end).take(1)` takes the earliest entry in the window. Callers
/// that want the current summary and reach for `take(1)` get the node's first-ever version, and
/// because a stale vector is still the right width it still produces a plausible cosine -- there is
/// nothing to notice.
///
/// `load_latest_context_summary` gets the answer right but decodes the node's whole window to do
/// it. This walks the range backwards instead, so the common case decodes exactly one record; it
/// keeps walking only if a decode fails or an entry does not satisfy `valid_from_ms <= as_of_ms`.
pub(super) fn load_newest_context_summary(
    cache: &MultiLayerCache,
    page_store: &LocalBlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    object_key: &str,
    as_of_ms: u64,
) -> Option<ContextSummary> {
    shard
        .context_summaries
        .get(object_key)
        .and_then(|series| {
            series
                .range(0..context_timeline_end(as_of_ms))
                .rev()
                .filter_map(|(timeline_key, address)| {
                    read_context_value::<ContextSummary>(
                        cache,
                        page_store,
                        shard_id,
                        *timeline_key,
                        address,
                    )
                })
                .find(|summary| summary.valid_from_ms <= as_of_ms)
        })
}

pub(super) fn load_latest_context_summary(
    cache: &MultiLayerCache,
    page_store: &LocalBlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    object_key: &str,
    as_of_ms: u64,
) -> Option<ContextSummary> {
    load_context_summaries(
        cache, page_store, shard_id, shard, object_key, as_of_ms, None,
    )
    .into_iter()
    .max_by_key(|summary| summary.valid_from_ms)
}

pub(super) fn load_context_compression_events(
    cache: &MultiLayerCache,
    page_store: &LocalBlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    tenant_hash: u64,
    node_hashes: &[u64],
    start_time_ms: u64,
    end_time_ms: u64,
    limit: Option<usize>,
) -> Vec<ContextCompressionEvent> {
    let mut events = Vec::new();
    for node_hash in node_hashes
        .iter()
        .copied()
        .filter(|node_hash| *node_hash != 0)
    {
        let object_key = context_compression_key(tenant_hash, node_hash);
        if let Some(series) = shard.context_compressions.get(&object_key) {
            events.extend(series.iter().filter_map(|(timeline_key, address)| {
                read_context_value::<ContextCompressionEvent>(
                    cache,
                    page_store,
                    shard_id,
                    *timeline_key,
                    address,
                )
                .filter(|event| {
                    event.source_end_ms >= start_time_ms && event.source_start_ms <= end_time_ms
                })
            }));
        }
        if events.len() >= context_limit(limit) {
            break;
        }
    }
    events.sort_by(|left, right| {
        right
            .source_end_ms
            .cmp(&left.source_end_ms)
            .then_with(|| right.compressed_time_ms.cmp(&left.compressed_time_ms))
            .then_with(|| left.compression_id_hash.cmp(&right.compression_id_hash))
    });
    events.truncate(context_limit(limit));
    events
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
    page_store: &LocalBlockStore,
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
        .unwrap_or_else(default_traversal_top_k)
        .max(1)
        .min(CONTEXT_MAX_LIMIT);
    let child_limit = max_children_scored_per_parent
        .unwrap_or(CONTEXT_MAX_LIMIT)
        .max(1)
        .min(CONTEXT_MAX_LIMIT);
    let candidate_limit = max_candidate_nodes
        .unwrap_or_else(default_traversal_candidates)
        .max(1)
        .min(CONTEXT_MAX_LIMIT);
    let mut frontier = vec![ContextTraversedNode {
        node_hash: start_node_hash,
        depth: 0,
        score: 1.0,
    }];
    let mut results = Vec::new();
    for depth in 1..=max_depth {
        let mut scored_layer = Vec::new();
        for parent in &frontier {
            let child_key = context_child_key(tenant_hash, parent.node_hash);
            let mut children =
                load_context_children(cache, page_store, shard_id, shard, &child_key);
            children.sort_by_key(|child_ref| (child_ref.updated_at_ms, child_ref.child_hash));
            children.truncate(child_limit);
            for child in children {
                // Score from the vector the node itself carries. The node is addressable by
                // the child hash already in hand; the retired separate records were not -- they
                // were keyed by a one-way hash of (tenant, owner, level) that no reader could
                // reconstruct without already holding the owner.
                let vector = load_context_node(
                    cache,
                    page_store,
                    shard_id,
                    shard,
                    tenant_hash,
                    child.child_hash,
                )
                .filter(|node| !node.vector.is_empty())
                .map(|node| node.vector);
                let Some(vector) = vector else {
                    continue;
                };
                let score = cosine_similarity(query_vector, &vector);
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
            let is_leaf =
                load_context_children(cache, page_store, shard_id, shard, &child_key).is_empty();
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
