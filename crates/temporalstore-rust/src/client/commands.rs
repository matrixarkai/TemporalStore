use crate::types::Command;

use super::routing::key_is_dropped_by_percent;

pub(super) fn is_write(command: &Command) -> bool {
    matches!(
        command,
        Command::CommonDelete { .. }
            | Command::CommonExpire { .. }
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
            | Command::FeatureAppend { .. }
            | Command::FeatureAppendWithPolicy { .. }
            | Command::FeatureReplace { .. }
            | Command::FeatureDelete { .. }
            | Command::SequenceAdd { .. }
            | Command::IpsAdd { .. }
            | Command::IpsAddWithOptions { .. }
            | Command::IpsLoad { .. }
            | Command::IpsRemove { .. }
            | Command::IpsDelete { .. }
            | Command::RiskIncrement { .. }
            | Command::RiskIncrementWithOptions { .. }
            | Command::RiskChangeAdd { .. }
            | Command::RiskSet { .. }
            | Command::RiskSetAndGet { .. }
            | Command::RiskFolSet { .. }
            | Command::ContextUpsertNode { .. }
            | Command::ContextWriteEvent { .. }
            | Command::ContextWriteIndexRef { .. }
            | Command::ContextWritePackAudit { .. }
            | Command::ContextMarkSummaryDirty { .. }
    )
}

pub(super) fn command_is_dropped(command: &Command, drop_percent: u8) -> bool {
    command_routing_key(command)
        .as_deref()
        .map(|key| key_is_dropped_by_percent(key, drop_percent))
        .unwrap_or(false)
}

pub(super) fn command_key(command: &Command) -> Option<&str> {
    match command {
        Command::CommonDelete { key }
        | Command::CommonExpire { key, .. }
        | Command::CommonTtl { key }
        | Command::CommonExists { key }
        | Command::StringSet { key, .. }
        | Command::StringSetEx { key, .. }
        | Command::StringSetConditional { key, .. }
        | Command::StringGet { key }
        | Command::StringDelete { key }
        | Command::HashSet { key, .. }
        | Command::HashGet { key, .. }
        | Command::HashMultiGet { key, .. }
        | Command::HashMultiSet { key, .. }
        | Command::HashIncrBy { key, .. }
        | Command::HashGetAll { key }
        | Command::HashLen { key }
        | Command::HashDelete { key, .. }
        | Command::SetAdd { key, .. }
        | Command::SetMembers { key }
        | Command::SetRemove { key, .. }
        | Command::FeatureAppend { key, .. }
        | Command::FeatureAppendWithPolicy { key, .. }
        | Command::FeatureQuery { key, .. }
        | Command::FeatureQueryFiltered { key, .. }
        | Command::FeatureReplace { key, .. }
        | Command::FeatureDelete { key }
        | Command::FeatureAggQuery { key, .. }
        | Command::SequenceAdd { key, .. }
        | Command::SequenceQuery { key, .. }
        | Command::IpsAdd { key, .. }
        | Command::IpsAddWithOptions { key, .. }
        | Command::IpsLoad { key, .. }
        | Command::IpsQueryLast { key, .. }
        | Command::IpsQueryRange { key, .. }
        | Command::IpsQueryRangeWithOptions { key, .. }
        | Command::IpsSnapshot { key, .. }
        | Command::IpsSnapshotReport { key, .. }
        | Command::IpsStat { key, .. }
        | Command::IpsFilter { key, .. }
        | Command::IpsRemove { key, .. }
        | Command::IpsDelete { key }
        | Command::IpsCount { key, .. }
        | Command::RiskIncrement { key, .. }
        | Command::RiskIncrementWithOptions { key, .. }
        | Command::RiskChangeAdd { key, .. }
        | Command::RiskCount { key, .. }
        | Command::RiskQuery { key, .. }
        | Command::RiskDetail { key, .. }
        | Command::RiskSet { key, .. }
        | Command::RiskSetAndGet { key, .. }
        | Command::RiskFamilyQuery { key, .. }
        | Command::RiskFolSet { key, .. }
        | Command::RiskFolQuery { key }
        | Command::RiskManager { key }
        | Command::RiskDebug { key, .. } => Some(key),
        Command::IpsBatchQueryLast { .. }
        | Command::SequenceBatchQuery { .. }
        | Command::ContextUpsertNode { .. }
        | Command::ContextGetNode { .. }
        | Command::ContextGetNodes { .. }
        | Command::ContextWriteEvent { .. }
        | Command::ContextWriteExtractedEvent { .. }
        | Command::ContextQueryEvents { .. }
        | Command::ContextWriteIndexRef { .. }
        | Command::ContextQueryIndex { .. }
        | Command::ContextQueryIndexIntersection { .. }
        | Command::ContextWritePackAudit { .. }
        | Command::ContextQueryPackAudit { .. }
        | Command::ContextMarkSummaryDirty { .. }
        | Command::ContextQuerySummaryDirty { .. }
        | Command::ContextUpsertEntity { .. }
        | Command::ContextGetEntity { .. }
        | Command::ContextQueryEntities { .. }
        | Command::ContextUpsertChildRef { .. }
        | Command::ContextQueryChildren { .. }
        | Command::ContextUpsertEmbedding { .. }
        | Command::ContextQueryEmbeddings { .. }
        | Command::ContextTraverseTree { .. }
        | Command::ContextUpsertSummary { .. }
        | Command::ContextQuerySummaries { .. }
        | Command::ContextWriteCompressionEvent { .. }
        | Command::ContextQueryCompressionEvents { .. }
        | Command::ContextCompressEvents { .. }
        | Command::ContextQueryNodeContext { .. } => None,
    }
}

pub(super) fn command_routing_key(command: &Command) -> Option<String> {
    command_key(command)
        .map(str::to_string)
        .or_else(|| context_command_key(command))
}

pub(super) fn context_command_key(command: &Command) -> Option<String> {
    match command {
        Command::ContextUpsertNode { tenant_hash, node } => {
            Some(context_node_key(*tenant_hash, node.node_hash))
        }
        Command::ContextGetNode {
            tenant_hash,
            node_hash,
        } => Some(context_node_key(*tenant_hash, *node_hash)),
        Command::ContextGetNodes {
            tenant_hash,
            node_hashes,
        } => node_hashes
            .first()
            .map(|node_hash| context_node_key(*tenant_hash, *node_hash)),
        Command::ContextWriteEvent {
            tenant_hash,
            node_hash,
            ..
        }
        | Command::ContextWriteExtractedEvent {
            tenant_hash,
            node_hash,
            ..
        }
        | Command::ContextQueryEvents {
            tenant_hash,
            node_hash,
            ..
        } => Some(context_event_key(*tenant_hash, *node_hash)),
        Command::ContextWriteIndexRef {
            tenant_hash,
            index_name,
            index_value_hash,
            scope_hash,
            ..
        }
        | Command::ContextQueryIndex {
            tenant_hash,
            index_name,
            index_value_hash,
            scope_hash,
            ..
        } => Some(context_index_key(
            *tenant_hash,
            index_name,
            *index_value_hash,
            *scope_hash,
        )),
        Command::ContextQueryIndexIntersection {
            tenant_hash,
            predicates,
            ..
        } => predicates.first().map(|predicate| {
            context_index_key(
                *tenant_hash,
                &predicate.index_name,
                predicate.index_value_hash,
                predicate.scope_hash,
            )
        }),
        Command::ContextWritePackAudit { tenant_hash, audit } => {
            Some(context_audit_key(*tenant_hash, audit.session_hash))
        }
        Command::ContextQueryPackAudit {
            tenant_hash,
            session_hash,
            ..
        } => Some(context_audit_key(*tenant_hash, *session_hash)),
        Command::ContextMarkSummaryDirty {
            tenant_hash,
            marker,
        } => Some(context_dirty_key(*tenant_hash, marker.node_hash)),
        Command::ContextQuerySummaryDirty {
            tenant_hash,
            node_hash,
            ..
        } => Some(context_dirty_key(*tenant_hash, *node_hash)),
        Command::ContextUpsertEntity {
            tenant_hash,
            entity,
        } => Some(context_entity_key(
            *tenant_hash,
            entity.node_hash,
            entity.entity_hash,
        )),
        Command::ContextGetEntity {
            tenant_hash,
            node_hash,
            entity_hash,
        } => Some(context_entity_key(*tenant_hash, *node_hash, *entity_hash)),
        Command::ContextQueryEntities {
            tenant_hash,
            node_hash,
            ..
        } => Some(context_entity_collection_key(*tenant_hash, *node_hash)),
        Command::ContextUpsertChildRef {
            tenant_hash,
            child_ref,
        } => Some(context_child_key(*tenant_hash, child_ref.parent_hash)),
        Command::ContextQueryChildren {
            tenant_hash,
            parent_hash,
            ..
        } => Some(context_child_key(*tenant_hash, *parent_hash)),
        Command::ContextUpsertEmbedding {
            tenant_hash,
            embedding,
        } => Some(context_embedding_key(*tenant_hash, embedding.ref_hash)),
        Command::ContextQueryEmbeddings {
            tenant_hash,
            ref_hashes,
            ..
        } => ref_hashes
            .first()
            .map(|ref_hash| context_embedding_key(*tenant_hash, *ref_hash)),
        Command::ContextTraverseTree {
            tenant_hash,
            start_node_hash,
            ..
        } => Some(context_child_key(*tenant_hash, *start_node_hash)),
        Command::ContextUpsertSummary {
            tenant_hash,
            summary,
        } => Some(context_summary_key(
            *tenant_hash,
            summary.node_hash,
            summary.level,
        )),
        Command::ContextQuerySummaries {
            tenant_hash,
            node_hash,
            level,
            ..
        } => Some(context_summary_key(*tenant_hash, *node_hash, *level)),
        Command::ContextWriteCompressionEvent { tenant_hash, event } => {
            Some(context_compression_key(*tenant_hash, event.node_hash))
        }
        Command::ContextQueryCompressionEvents {
            tenant_hash,
            node_hashes,
            ..
        } => node_hashes
            .first()
            .map(|node_hash| context_compression_key(*tenant_hash, *node_hash)),
        Command::ContextCompressEvents {
            tenant_hash,
            node_hash,
            ..
        }
        | Command::ContextQueryNodeContext {
            tenant_hash,
            node_hash,
            ..
        } => Some(context_compression_key(*tenant_hash, *node_hash)),
        _ => None,
    }
}

pub(super) fn context_node_key(tenant_hash: u64, node_hash: u64) -> String {
    format!("ctx:node:{tenant_hash}:{node_hash}")
}

pub(super) fn context_event_key(tenant_hash: u64, node_hash: u64) -> String {
    format!("ctx:event:{tenant_hash}:{node_hash}")
}

pub(super) fn context_index_key(
    tenant_hash: u64,
    index_name: &str,
    index_value_hash: u64,
    scope_hash: u64,
) -> String {
    format!("ctxidx:{tenant_hash}:{index_name}:{index_value_hash}:{scope_hash}")
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
