// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Command inspection, admission control, and precondition validation helpers, split from engine.rs.
use super::*;

/// Keys READ (not written) by a command, for LRU recency tracking. Covers the
/// key-addressed read commands; the hash-addressed context queries are omitted (their
/// buckets refresh recency on the next write -- acceptable, eviction is memory-only).
pub(super) fn command_read_keys(command: &Command) -> Vec<String> {
    match command {
        Command::CommonTtl { key, .. }
        | Command::CommonExists { key, .. }
        | Command::StringGet { key, .. }
        | Command::HashGet { key, .. }
        | Command::HashMultiGet { key, .. }
        | Command::HashGetAll { key, .. }
        | Command::HashLen { key, .. }
        | Command::SetMembers { key, .. }
        | Command::FeatureQuery { key, .. }
        | Command::SequenceQuery { key, .. } => vec![key.clone()],
        _ => Vec::new(),
    }
}

/// Keys a command touched, read or write -- used to stamp per-bucket LRU recency.
pub(super) fn command_touched_keys(command: &Command) -> Vec<String> {
    let keys = command_object_keys(command);
    if keys.is_empty() {
        command_read_keys(command)
    } else {
        keys
    }
}

pub(crate) fn command_object_keys(command: &Command) -> Vec<String> {
    match command {
        // Touches no object, so it holds no object keys.
        Command::LeaderEstablish => Vec::new(),
        Command::CommonDelete { key } => associated_record_keys(key),
        Command::CommonPersist { key } => associated_record_keys(key),
        Command::CommonExpire { key, .. }
        | Command::StringSet { key, .. }
        | Command::StringSetEx { key, .. }
        | Command::StringSetConditional { key, .. }
        | Command::StringDelete { key }
        | Command::HashSet { key, .. }
        | Command::HashMultiSet { key, .. }
        | Command::HashIncrBy { key, .. }
        | Command::HashDelete { key, .. }
        | Command::SetAdd { key, .. }
        | Command::SetRemove { key, .. }
        | Command::ListPush { key, .. }
        | Command::ListPop { key, .. }
        | Command::ListRange { key, .. }
        | Command::ListLen { key }
        | Command::ZSetAdd { key, .. }
        | Command::ZSetScore { key, .. }
        | Command::ZSetRemove { key, .. }
        | Command::ZSetCard { key }
        | Command::ZSetRange { key, .. }
        | Command::ZSetRangeByScore { key, .. }
        | Command::ZSetIncrBy { key, .. }
        | Command::ZSetPop { key, .. }
        | Command::ZSetRank { key, .. }
        | Command::BucketTake { key, .. }
        | Command::BucketPeek { key, .. }
        | Command::SeenCheck { key, .. }
        | Command::SeenCard { key }
        | Command::FeatureAppend { key, .. }
        | Command::FeatureAppendWithPolicy { key, .. }
        | Command::FeatureReplace { key, .. }
        | Command::FeatureDelete { key }
        | Command::SequenceAdd { key, .. }
        | Command::ControlStateIncrement { key, .. }
        | Command::ControlStateIncrementWithOptions { key, .. }
        | Command::ControlStateChangeAdd { key, .. }
        | Command::ControlStateSelectionSet { key, .. } => vec![key.clone()],
        Command::ControlStateSet { family, key, .. }
        | Command::ControlStateSetAndGet { family, key, .. }
        | Command::ControlStateSetAndGetWithOptions { family, key, .. } => {
            vec![control_state_family_key(*family, key)]
        }
        Command::ContextUpsertNode { tenant_hash, node } => {
            vec![context_node_key(*tenant_hash, node.node_hash)]
        }
        Command::ContextWriteEvent {
            tenant_hash,
            node_hash,
            ..
        } => vec![context_event_key(*tenant_hash, *node_hash)],
        Command::ContextWriteExtractedEvent {
            tenant_hash,
            node_hash,
            event,
            indexes,
            ..
        } => {
            let mut keys = vec![context_event_key(*tenant_hash, *node_hash)];
            if !context_index_disabled(indexes, InternalContextIndex::EventKind) {
                keys.push(context_index_key(
                    *tenant_hash,
                    "event_kind",
                    context_event_kind_hash(event),
                    indexes.scope_hash,
                ));
            }
            if !context_index_disabled(indexes, InternalContextIndex::Status)
                && indexes.status_hash != 0
            {
                keys.push(context_index_key(
                    *tenant_hash,
                    "status",
                    indexes.status_hash,
                    indexes.scope_hash,
                ));
            }
            if !context_index_disabled(indexes, InternalContextIndex::Source)
                && indexes.source_hash != 0
            {
                keys.push(context_index_key(
                    *tenant_hash,
                    "source",
                    indexes.source_hash,
                    indexes.scope_hash,
                ));
            }
            if !context_index_disabled(indexes, InternalContextIndex::EventTimeBucket)
                && indexes.event_time_bucket_ms != 0
            {
                keys.push(context_index_key(
                    *tenant_hash,
                    "event_time_bucket",
                    indexes.event_time_bucket_ms,
                    indexes.scope_hash,
                ));
            }
            if !context_index_disabled(indexes, InternalContextIndex::Entity) {
                keys.extend(
                    indexes
                        .entity_hashes
                        .iter()
                        .copied()
                        .filter(|hash| *hash != 0)
                        .map(|entity_hash| {
                            context_index_key(
                                *tenant_hash,
                                "entity",
                                entity_hash,
                                indexes.scope_hash,
                            )
                        }),
                );
            }
            keys
        }
        Command::ContextWriteIndexRef {
            tenant_hash,
            index_name,
            index_value_hash,
            scope_hash,
            ..
        } => vec![context_index_key(
            *tenant_hash,
            index_name,
            *index_value_hash,
            *scope_hash,
        )],
        Command::ContextWritePackAudit { tenant_hash, audit } => {
            vec![context_audit_key(*tenant_hash, audit.session_hash)]
        }
        Command::ContextMarkSummaryDirty {
            tenant_hash,
            node_hash,
            ..
        } => vec![context_dirty_key(*tenant_hash, *node_hash)],
        Command::ContextMarkEmbeddingDirty {
            tenant_hash,
            node_hash,
            ..
        } => vec![context_embedding_dirty_key(*tenant_hash, *node_hash)],
        Command::ContextUpsertEntity {
            tenant_hash,
            entity,
        } => vec![context_entity_key(
            *tenant_hash,
            entity.node_hash,
            entity.entity_hash,
        )],
        Command::ContextUpsertChildRef {
            tenant_hash,
            child_ref,
        } => vec![context_child_key(*tenant_hash, child_ref.parent_hash)],
        Command::ContextSetNodeEmbedding {
            tenant_hash,
            node_hash,
            ..
        } => vec![context_node_key(*tenant_hash, *node_hash)],
        Command::ContextQueryNodeEmbeddings {
            tenant_hash,
            node_hashes,
        } => node_hashes
            .iter()
            .map(|node_hash| context_node_key(*tenant_hash, *node_hash))
            .collect(),
        Command::ContextUpsertSummary {
            tenant_hash,
            summary,
        } => vec![context_summary_key(
            *tenant_hash,
            summary.node_hash,
            summary.level,
        )],
        Command::ContextWriteCompressionEvent { tenant_hash, event } => {
            vec![context_compression_key(*tenant_hash, event.node_hash)]
        }
        Command::ContextCompressEvents {
            tenant_hash,
            node_hash,
            ..
        } => vec![context_compression_key(*tenant_hash, *node_hash)],
        Command::SequenceBatchQuery { .. }
        | Command::CommonTtl { .. }
        | Command::CommonExists { .. }
        | Command::StringGet { .. }
        | Command::HashGet { .. }
        | Command::HashMultiGet { .. }
        | Command::HashGetAll { .. }
        | Command::HashLen { .. }
        | Command::SetMembers { .. }
        | Command::FeatureQuery { .. }
        | Command::FeatureQueryFiltered { .. }
        | Command::FeatureAggQuery { .. }
        | Command::SequenceQuery { .. }
        | Command::ControlStateCount { .. }
        | Command::ControlStateQuery { .. }
        | Command::ControlStateDetail { .. }
        | Command::ControlStateFamilyQuery { .. }
        | Command::ControlStateSelectionQuery { .. }
        | Command::ControlStateManager { .. }
        | Command::ControlStateDebug { .. }
        | Command::ContextGetNode { .. }
        | Command::ContextGetNodes { .. }
        | Command::ContextQueryEvents { .. }
        | Command::ContextQueryIndex { .. }
        | Command::ContextQueryIndexIntersection { .. }
        | Command::ContextQueryPackAudit { .. }
        | Command::ContextQuerySummaryDirty { .. }
        | Command::ContextQueryEmbeddingDirty { .. }
        | Command::ContextGetEntity { .. }
        | Command::ContextQueryEntities { .. }
        | Command::ContextQueryChildren { .. }
        | Command::ContextTraverseTree { .. }
        | Command::ContextQuerySummaries { .. }
        | Command::ContextQuerySummaryVectors { .. }
        | Command::ContextQueryCompressionEvents { .. }
        | Command::ContextResourceBlobBegin { .. }
        | Command::ContextResourceBlobAppend { .. }
        | Command::ContextResourceBlobCommit { .. }
        | Command::ContextResourceBlobPut { .. }
        | Command::ContextResourceBlobFetch { .. }
        | Command::ContextResourceBlobSweep { .. }
        | Command::ContextQueryNodeContext { .. } => Vec::new(),
    }
}

pub(super) fn command_updates_bucket_index_directly(command: &Command) -> bool {
    matches!(
        command,
        Command::CommonDelete { .. }
            | Command::StringDelete { .. }
            | Command::StringSet { .. }
            | Command::StringSetEx { .. }
            | Command::StringSetConditional { .. }
            | Command::HashSet { .. }
            | Command::HashMultiSet { .. }
            | Command::HashIncrBy { .. }
            | Command::HashDelete { .. }
            | Command::SetAdd { .. }
            | Command::SetRemove { .. }
            | Command::ListPush { .. }
            | Command::ListPop { .. }
            | Command::ZSetAdd { .. }
            | Command::ZSetRemove { .. }
            | Command::ZSetIncrBy { .. }
            | Command::ZSetPop { .. }
            | Command::ControlStateIncrement { .. }
            | Command::ControlStateIncrementWithOptions { .. }
            | Command::ControlStateSet { .. }
            | Command::ControlStateSetAndGet { .. }
            | Command::ControlStateSetAndGetWithOptions { .. }
            // These file their pages through `sync_bucket_index_object_pages`, which maintains
            // exactly what a rebuild would recompute. Without them here the post-command path took
            // the rebuild branch on EVERY call: measured at twice the shard's page count per call,
            // so a one-point feature append into a 1,024-key store cost 12,717 allocations, none of
            // it about the point being appended.
            | Command::FeatureAppend { .. }
            | Command::FeatureAppendWithPolicy { .. }
            | Command::FeatureReplace { .. }
            | Command::FeatureDelete { .. }
            | Command::SequenceAdd { .. }
    )
}

/// True when a command mutates only state that no page backs.
///
/// The seen sets, the control-state change and selection maps, and the token buckets live outside the
/// page-backed models, and their durability rides the index-log `key_states` blobs rather than a page
/// entry. So a rebuild of the page index after one of these can only recompute what it already held.
///
/// This is deliberately NOT folded into `command_updates_bucket_index_directly`: these commands do
/// not update that index, and saying they do to skip the rebuild would make the predicate assert
/// something false. They need their own reason.
pub(super) fn command_writes_no_page(command: &Command) -> bool {
    matches!(
        command,
        Command::SeenCheck { .. }
            | Command::ControlStateChangeAdd { .. }
            | Command::ControlStateSelectionSet { .. }
            | Command::BucketTake { .. }
    )
}

pub(crate) fn is_write_command(command: &Command) -> bool {
    matches!(
        command,
        Command::CommonDelete { .. }
            | Command::CommonExpire { .. }
            | Command::CommonPersist { .. }
            | Command::StringSet { .. }
            | Command::StringSetEx { .. }
            | Command::StringSetConditional { .. }
            | Command::StringDelete { .. }
            | Command::HashSet { .. }
            | Command::HashMultiSet { .. }
            | Command::HashIncrBy { .. }
            | Command::HashDelete { .. }
            | Command::SetAdd { .. }
            | Command::SetRemove { .. }
            | Command::ListPush { .. }
            | Command::ListPop { .. }
            | Command::ZSetAdd { .. }
            | Command::ZSetRemove { .. }
            | Command::ZSetIncrBy { .. }
            | Command::ZSetPop { .. }
            | Command::BucketTake { .. }
            | Command::SeenCheck { .. }
            | Command::FeatureAppend { .. }
            | Command::FeatureAppendWithPolicy { .. }
            | Command::FeatureReplace { .. }
            | Command::FeatureDelete { .. }
            | Command::SequenceAdd { .. }
            | Command::ControlStateIncrement { .. }
            | Command::ControlStateIncrementWithOptions { .. }
            | Command::ControlStateChangeAdd { .. }
            | Command::ControlStateSet { .. }
            | Command::ControlStateSetAndGet { .. }
            | Command::ControlStateSetAndGetWithOptions { .. }
            | Command::ControlStateSelectionSet { .. }
            | Command::ContextResourceBlobBegin { .. }
            | Command::ContextResourceBlobAppend { .. }
            | Command::ContextResourceBlobCommit { .. }
            | Command::ContextResourceBlobPut { .. }
            | Command::ContextResourceBlobSweep { .. }
            | Command::ContextUpsertNode { .. }
            | Command::ContextWriteEvent { .. }
            | Command::ContextWriteExtractedEvent { .. }
            | Command::ContextWriteIndexRef { .. }
            | Command::ContextWritePackAudit { .. }
            | Command::ContextMarkSummaryDirty { .. }
            | Command::ContextMarkEmbeddingDirty { .. }
            | Command::ContextUpsertEntity { .. }
            | Command::ContextUpsertChildRef { .. }
            | Command::ContextSetNodeEmbedding { .. }
            | Command::ContextUpsertSummary { .. }
            | Command::ContextWriteCompressionEvent { .. }
            | Command::ContextCompressEvents { .. }
    )
}

pub(super) fn admission_limits(
    shard_id: ShardId,
    write_command: bool,
    config: &Config,
    info: &Option<ShardInfo>,
) -> Vec<AdmissionLimit> {
    let mut limits = Vec::new();
    // A configured qps of 0 means UNLIMITED (QuotaManager: a 0 qps installs no limiter
    // and ConsumeQuota always succeeds), NOT deny-all. Filtering out 0 here keeps the
    // downstream `limit == 0` reject from ever firing on an intended "no limit" setting.
    if let Some(limit) = (if write_command {
        config.write_qps
    } else {
        config.read_qps
    })
    .filter(|&limit| limit > 0)
    {
        limits.push(AdmissionLimit {
            scope: AdmissionScope::Shard(shard_id),
            limit,
            label: if write_command {
                "write_qps"
            } else {
                "read_qps"
            },
        });
    }
    if let Some(table_name) = info
        .as_ref()
        .map(|info| info.table_name.trim())
        .filter(|table_name| !table_name.is_empty())
    {
        if let Some(limit) = if write_command {
            config.table_write_qps
        } else {
            config.table_read_qps
        } {
            limits.push(AdmissionLimit {
                scope: AdmissionScope::Table(table_name.to_string()),
                limit,
                label: if write_command {
                    "table_write_qps"
                } else {
                    "table_read_qps"
                },
            });
        }
    }
    if let Some(tenant_name) = config
        .tenant_name
        .as_deref()
        .map(str::trim)
        .filter(|tenant_name| !tenant_name.is_empty())
    {
        if let Some(limit) = if write_command {
            config.tenant_write_qps
        } else {
            config.tenant_read_qps
        } {
            limits.push(AdmissionLimit {
                scope: AdmissionScope::Tenant(tenant_name.to_string()),
                limit,
                label: if write_command {
                    "tenant_write_qps"
                } else {
                    "tenant_read_qps"
                },
            });
        }
    }
    limits
}

pub(super) fn reset_admission_window(admission: &mut AdmissionState, now_sec: u64) {
    if admission.window_epoch_sec != now_sec {
        admission.window_epoch_sec = now_sec;
        admission.read_count = 0;
        admission.write_count = 0;
    }
}

pub(super) fn admission_count(admission: &mut AdmissionState, write_command: bool) -> &mut u64 {
    if write_command {
        &mut admission.write_count
    } else {
        &mut admission.read_count
    }
}

pub(super) fn now_epoch_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

pub(super) fn validate_command_preconditions(
    cache: &MultiLayerCache,
    page_store: &LocalBlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    command: &Command,
) -> Result<(), Status> {
    match command {
        // Expiry checks here use the SAME replay-aware clock as the executor's
        // `remove_if_expired`. Validation runs only on the leader today, where the two
        // clocks coincide, so this changes no production behaviour. It removes a split
        // that would matter the moment anything validates during replay: deciding
        // "is this key expired" against the restart clock is what makes a replayed
        // command fail its precondition and abort a shard load.
        Command::CommonExpire { key, .. } => {
            if shard
                .expires_at_ms
                .get(key)
                .map(|expires_at| *expires_at <= resolve_now_ms())
                .unwrap_or(false)
                || !record_exists(shard, key)
            {
                return Err(Status::error("not_found", "key not found"));
            }
        }
        Command::FeatureAppend { key, points } => {
            let current = shard
                .features
                .get(key)
                .map(|series| series.len())
                .unwrap_or(0);
            if current.saturating_add(points.len()) > FEATURE_ADD_HARD_MAX_SIZE {
                return Err(Status::error(
                    "invalid_argument",
                    format!("{key} size bigger than {FEATURE_ADD_HARD_MAX_SIZE}"),
                ));
            }
        }
        Command::FeatureAppendWithPolicy {
            key,
            points,
            policy,
        } => {
            if *policy == FeatureWritePolicy::Block {
                return Err(Status::error("invalid_argument", "Invalid write policy"));
            }
            let current = shard
                .features
                .get(key)
                .map(|series| series.len())
                .unwrap_or(0);
            if current.saturating_add(points.len()) > FEATURE_ADD_HARD_MAX_SIZE {
                return Err(Status::error(
                    "invalid_argument",
                    format!("{key} size bigger than {FEATURE_ADD_HARD_MAX_SIZE}"),
                ));
            }
        }
        Command::FeatureReplace { key, points, .. } => {
            let current = shard
                .features
                .get(key)
                .map(|series| series.len())
                .unwrap_or(0);
            if current.saturating_add(points.len()) > FEATURE_ADD_HARD_MAX_SIZE {
                return Err(Status::error(
                    "invalid_argument",
                    format!("{key} size bigger than {FEATURE_ADD_HARD_MAX_SIZE}"),
                ));
            }
        }
        Command::ContextUpsertNode { tenant_hash, node } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_node(node)?;
        }
        Command::ContextGetNode {
            tenant_hash,
            node_hash,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
        }
        Command::ContextGetNodes {
            tenant_hash,
            node_hashes,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(!node_hashes.is_empty(), "node_hashes are required")?;
            for node_hash in node_hashes {
                validate_context_required(*node_hash != 0, "node_hash is required")?;
            }
        }
        Command::ContextQuerySummaryVectors {
            tenant_hash,
            node_hashes,
            level,
            as_of_ms,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(!node_hashes.is_empty(), "node_hashes are required")?;
            validate_context_required(*level != 0, "level is required")?;
            validate_context_required(*as_of_ms != 0, "as_of_ms is required")?;
            for node_hash in node_hashes {
                validate_context_required(*node_hash != 0, "node_hash is required")?;
            }
        }
        Command::ContextWriteEvent {
            tenant_hash,
            node_hash,
            event,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_event(event)?;
        }
        Command::ContextWriteExtractedEvent {
            tenant_hash,
            node_hash,
            event,
            indexes,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_event(event)?;
            validate_context_extracted_indexes(event, indexes)?;
        }
        Command::ContextQueryEvents {
            tenant_hash,
            node_hash,
            start_time_ms,
            end_time_ms,
            limit,
            max_scan,
            kinds,
            statuses,
            min_confidence,
            min_importance,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_limit(*limit)?;
            validate_context_limit(*max_scan)?;
            validate_context_range(*start_time_ms, *end_time_ms)?;
            validate_context_filters(kinds, statuses, *min_confidence, *min_importance)?;
        }
        Command::ContextWriteIndexRef {
            tenant_hash,
            index_name,
            index_value_hash,
            event_time_ms,
            index_ref,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_index_name(index_name)?;
            validate_context_required(*index_value_hash != 0, "index_value_hash is required")?;
            validate_context_required(*event_time_ms != 0, "event_time_ms is required")?;
            validate_context_timestamp(*event_time_ms)?;
            validate_context_index_ref(index_ref)?;
        }
        Command::ContextQueryIndex {
            tenant_hash,
            index_name,
            index_value_hash,
            start_time_ms,
            end_time_ms,
            limit,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_index_name(index_name)?;
            validate_context_required(*index_value_hash != 0, "index_value_hash is required")?;
            validate_context_limit(*limit)?;
            validate_context_range(*start_time_ms, *end_time_ms)?;
        }
        Command::ContextQueryIndexIntersection {
            tenant_hash,
            predicates,
            limit,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(!predicates.is_empty(), "predicates are required")?;
            validate_context_limit(*limit)?;
            for predicate in predicates {
                validate_context_index_lookup(predicate)?;
            }
        }
        Command::ContextWritePackAudit { tenant_hash, audit } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_pack_audit(audit)?;
        }
        Command::ContextQueryPackAudit {
            tenant_hash,
            session_hash,
            start_time_ms,
            end_time_ms,
            limit,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*session_hash != 0, "session_hash is required")?;
            validate_context_limit(*limit)?;
            validate_context_range(*start_time_ms, *end_time_ms)?;
        }
        Command::ContextMarkSummaryDirty {
            tenant_hash,
            node_hash,
            event_time_ms,
            reason,
            propagate_depth,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_dirty_node(*node_hash, *event_time_ms, *reason, *propagate_depth)?;
        }
        Command::ContextQuerySummaryDirty {
            tenant_hash,
            node_hash,
            start_time_ms,
            end_time_ms,
            limit,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_limit(*limit)?;
            validate_context_range(*start_time_ms, *end_time_ms)?;
        }
        Command::ContextMarkEmbeddingDirty {
            tenant_hash,
            node_hash,
            event_time_ms,
            reason,
            propagate_depth,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_dirty_node(*node_hash, *event_time_ms, *reason, *propagate_depth)?;
        }
        Command::ContextQueryEmbeddingDirty {
            start_time_ms,
            end_time_ms,
            limit,
            ..
        } => {
            // Both node_hash == 0 (all pending nodes) and tenant_hash == 0 (all
            // tenants on the shard) are permitted: together they are the drainer's
            // O(pending) all-pending scan.
            validate_context_limit(*limit)?;
            validate_context_range(*start_time_ms, *end_time_ms)?;
        }
        Command::ContextUpsertEntity {
            tenant_hash,
            entity,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_entity(entity)?;
        }
        Command::ContextGetEntity {
            tenant_hash,
            node_hash,
            entity_hash,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_required(*entity_hash != 0, "entity_hash is required")?;
        }
        Command::ContextQueryEntities {
            tenant_hash,
            node_hash,
            entity_hashes,
            limit,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_limit(*limit)?;
            if entity_hashes.len() > CONTEXT_MAX_LIMIT {
                return Err(Status::error(
                    "invalid_argument",
                    "entity_hashes exceeds maximum",
                ));
            }
            if entity_hashes.iter().any(|hash| *hash == 0) {
                return Err(Status::error(
                    "invalid_argument",
                    "entity_hashes must be non-zero",
                ));
            }
        }
        Command::ContextUpsertChildRef {
            tenant_hash,
            child_ref,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_child_ref(child_ref)?;
        }
        Command::ContextQueryChildren {
            tenant_hash,
            parent_hash,
            limit,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*parent_hash != 0, "parent_hash is required")?;
            validate_context_limit(*limit)?;
        }
        Command::ContextTraverseTree {
            tenant_hash,
            start_node_hash,
            query_vector,
            max_depth,
            top_k_per_depth,
            max_children_scored_per_parent,
            max_candidate_nodes,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*start_node_hash != 0, "start_node_hash is required")?;
            validate_context_required(!query_vector.is_empty(), "query_vector is required")?;
            validate_context_embedding_vector("query_vector", query_vector)?;
            if max_depth.unwrap_or_default() > CONTEXT_MAX_TRAVERSAL_DEPTH {
                return Err(Status::error(
                    "invalid_argument",
                    "max_depth exceeds maximum",
                ));
            }
            for limit in [
                *top_k_per_depth,
                *max_children_scored_per_parent,
                *max_candidate_nodes,
            ] {
                validate_context_limit(limit)?;
            }
        }
        Command::ContextUpsertSummary {
            tenant_hash,
            summary,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_summary(summary)?;
        }
        Command::ContextQuerySummaries {
            tenant_hash,
            node_hash,
            level,
            as_of_ms,
            limit,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_required(*level != 0, "level is required")?;
            validate_context_required(*as_of_ms != 0, "as_of_ms is required")?;
            validate_context_timestamp(*as_of_ms)?;
            validate_context_limit(*limit)?;
        }
        Command::ContextWriteCompressionEvent { tenant_hash, event } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_compression_event(event)?;
        }
        Command::ContextQueryCompressionEvents {
            tenant_hash,
            node_hashes,
            start_time_ms,
            end_time_ms,
            limit,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_limit(*limit)?;
            validate_context_range(*start_time_ms, *end_time_ms)?;
            if node_hashes.len() > CONTEXT_MAX_FILTER_VALUES {
                return Err(Status::error("invalid_argument", "too many node_hashes"));
            }
        }
        Command::ContextCompressEvents {
            tenant_hash,
            node_hash,
            source_start_ms,
            source_end_ms,
            compressed_time_ms,
            max_source_events,
            min_confidence,
            min_importance,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_range(*source_start_ms, *source_end_ms)?;
            validate_context_limit(*max_source_events)?;
            validate_context_score("min_confidence", *min_confidence)?;
            validate_context_score("min_importance", *min_importance)?;
            if *compressed_time_ms != 0 {
                validate_context_timestamp(*compressed_time_ms)?;
            }
        }
        Command::ContextQueryNodeContext {
            tenant_hash,
            node_hash,
            summary_level,
            as_of_ms,
            cold_start_time_ms,
            cold_end_time_ms,
            compression_limit,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_required(*as_of_ms != 0, "as_of_ms is required")?;
            validate_context_timestamp(*as_of_ms)?;
            if summary_level.unwrap_or(1) == 0 {
                return Err(Status::error(
                    "invalid_argument",
                    "summary_level is required",
                ));
            }
            validate_context_limit(*compression_limit)?;
            if *cold_start_time_ms != 0 || *cold_end_time_ms != 0 {
                validate_context_range(*cold_start_time_ms, *cold_end_time_ms)?;
            }
        }
        _ => {}
    }

    if let Command::HashIncrBy {
        key,
        field,
        increment,
    } = command
    {
        if shard
            .expires_at_ms
            .get(key)
            .map(|expires_at| *expires_at <= resolve_now_ms())
            .unwrap_or(false)
        {
            return Ok(());
        }
        let Some(bytes) = shard
            .hashes
            .get(key)
            .and_then(|entries| entries.get(field))
            .and_then(|address| read_page_bytes(cache, page_store, shard_id, address))
        else {
            return 0_i64
                .checked_add(*increment)
                .map(|_| ())
                .ok_or_else(|| Status::error("out_of_range", "hash increment overflows i64"));
        };
        let current = parse_i64(&bytes)
            .ok_or_else(|| Status::error("unmatched", "hash value is not an integer"))?;
        current
            .checked_add(*increment)
            .map(|_| ())
            .ok_or_else(|| Status::error("out_of_range", "hash increment overflows i64"))?;
    }
    Ok(())
}

/// Encode a cached response.
///
/// msgpack, not JSON. `CommandResponse::Bytes` carries a `Vec<u8>`, and JSON writes that as an
/// ARRAY OF DECIMAL NUMBERS -- about four characters a byte. Measured: a 4 KiB value cached as
/// 16,410 bytes of text against 4,117 packed, and every HIT paid to parse those characters back.
/// That was the bulk of what a warm read allocated: 15 allocations and 2,788 bytes to serve a
/// 64-byte value, a cost that tracked the ENCODING rather than the value.
///
/// Nothing on disk depends on this: these entries are `put_memory_only`, so the choice is
/// contained to the pair of functions here. A stored entry that does not decode is dropped and
/// recomputed, which is what the JSON path already did -- so an entry written by an older build
/// in the same process costs one recompute, not an error.
fn encode_cached_response(response: &CommandResponse) -> Option<Vec<u8>> {
    let mut packed = Vec::new();
    let mut serializer = rmp_serde::Serializer::new(&mut packed).with_struct_map();
    // struct-as-MAP: `CommandResponse` is internally tagged (`#[serde(tag = "kind")]`), which
    // needs the field names present to read back. The array form would drop them.
    serde::Serialize::serialize(response, &mut serializer).ok()?;
    Some(packed)
}

fn decode_cached_response(bytes: &[u8]) -> Option<CommandResponse> {
    rmp_serde::from_slice::<CommandResponse>(bytes).ok()
}

pub(super) fn cached_response(
    cache: &MultiLayerCache,
    key: CacheKey,
    source: impl FnOnce() -> CommandResponse,
) -> CommandResponse {
    if let Ok(Some(bytes)) = cache.get(&key) {
        if let Some(response) = decode_cached_response(&bytes) {
            return response;
        }
        let _ = cache.invalidate(&key);
    }
    let response = source();
    if let Some(bytes) = encode_cached_response(&response) {
        cache.put_memory_only(key, bytes);
    }
    response
}

pub(super) fn invalidate_if_cached(cache: &MultiLayerCache, key: CacheKey) {
    if cache.peek(&key) {
        let _ = cache.invalidate(&key);
    }
}
