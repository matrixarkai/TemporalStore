use std::collections::HashSet;

use crate::types::{Command, InternalContextIndex};

use super::context::{
    context_audit_key, context_child_key, context_compression_key, context_dirty_key,
    context_embedding_key, context_entity_key, context_event_key, context_event_kind_hash,
    context_index_disabled, context_index_key, context_node_key, context_summary_key,
};
use super::product_model::control_state_family_key;
use super::records::associated_record_keys;

pub(super) fn command_object_keys(command: &Command) -> Vec<String> {
    match command {
        Command::CommonDelete { key } => associated_record_keys(key),
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
        | Command::FeatureAppend { key, .. }
        | Command::FeatureAppendWithPolicy { key, .. }
        | Command::FeatureReplace { key, .. }
        | Command::FeatureDelete { key }
        | Command::SequenceAdd { key, .. }
        | Command::SequenceAddWithPolicy { key, .. }
        | Command::IpsAdd { key, .. }
        | Command::IpsAddWithOptions { key, .. }
        | Command::IpsLoad { key, .. }
        | Command::IpsRemove { key, .. }
        | Command::IpsDelete { key }
        | Command::ControlStateIncrement { key, .. }
        | Command::ControlStateIncrementWithOptions { key, .. }
        | Command::ControlStateChangeAdd { key, .. }
        | Command::ControlStateFolSet { key, .. } => vec![key.clone()],
        Command::ControlStateSet { family, key, .. }
        | Command::ControlStateSetAndGet { family, key, .. } => {
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
                let mut seen_entity_hashes = HashSet::new();
                keys.extend(
                    indexes
                        .entity_hashes
                        .iter()
                        .copied()
                        .filter(|hash| *hash != 0)
                        .filter(|hash| seen_entity_hashes.insert(*hash))
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
            marker,
        } => vec![context_dirty_key(*tenant_hash, marker.node_hash)],
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
        Command::ContextUpsertEmbedding {
            tenant_hash,
            embedding,
        } => vec![context_embedding_key(*tenant_hash, embedding.ref_hash)],
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
        | Command::IpsQueryLast { .. }
        | Command::IpsQueryRange { .. }
        | Command::IpsBatchQueryLast { .. }
        | Command::IpsCount { .. }
        | Command::IpsQueryRangeWithOptions { .. }
        | Command::IpsSnapshot { .. }
        | Command::IpsSnapshotReport { .. }
        | Command::IpsStat { .. }
        | Command::IpsFilter { .. }
        | Command::ControlStateCount { .. }
        | Command::ControlStateQuery { .. }
        | Command::ControlStateDetail { .. }
        | Command::ControlStateFamilyQuery { .. }
        | Command::ControlStateFolQuery { .. }
        | Command::ControlStateManager { .. }
        | Command::ControlStateDebug { .. }
        | Command::ContextGetNode { .. }
        | Command::ContextGetNodes { .. }
        | Command::ContextQueryEvents { .. }
        | Command::ContextQueryIndex { .. }
        | Command::ContextQueryIndexIntersection { .. }
        | Command::ContextQueryPackAudit { .. }
        | Command::ContextQuerySummaryDirty { .. }
        | Command::ContextGetEntity { .. }
        | Command::ContextQueryEntities { .. }
        | Command::ContextQueryChildren { .. }
        | Command::ContextQueryEmbeddings { .. }
        | Command::ContextTraverseTree { .. }
        | Command::ContextQuerySummaries { .. }
        | Command::ContextQueryCompressionEvents { .. }
        | Command::ContextQueryNodeContext { .. } => Vec::new(),
    }
}

pub(super) fn command_updates_slot_index_directly(command: &Command) -> bool {
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
            | Command::ControlStateIncrement { .. }
            | Command::ControlStateIncrementWithOptions { .. }
            | Command::ControlStateSet { .. }
            | Command::ControlStateSetAndGet { .. }
    )
}

pub(super) fn is_write_command(command: &Command) -> bool {
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
            | Command::SequenceAddWithPolicy { .. }
            | Command::IpsAdd { .. }
            | Command::IpsAddWithOptions { .. }
            | Command::IpsLoad { .. }
            | Command::IpsRemove { .. }
            | Command::IpsDelete { .. }
            | Command::ControlStateIncrement { .. }
            | Command::ControlStateIncrementWithOptions { .. }
            | Command::ControlStateChangeAdd { .. }
            | Command::ControlStateSet { .. }
            | Command::ControlStateSetAndGet { .. }
            | Command::ControlStateFolSet { .. }
            | Command::ContextUpsertNode { .. }
            | Command::ContextWriteEvent { .. }
            | Command::ContextWriteExtractedEvent { .. }
            | Command::ContextWriteIndexRef { .. }
            | Command::ContextWritePackAudit { .. }
            | Command::ContextMarkSummaryDirty { .. }
            | Command::ContextUpsertEntity { .. }
            | Command::ContextUpsertChildRef { .. }
            | Command::ContextUpsertEmbedding { .. }
            | Command::ContextUpsertSummary { .. }
            | Command::ContextWriteCompressionEvent { .. }
            | Command::ContextCompressEvents { .. }
    )
}
