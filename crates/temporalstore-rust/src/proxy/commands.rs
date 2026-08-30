// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::types::Command;

pub(super) fn proxy_command_is_write(command: &Command) -> bool {
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
            | Command::ControlStateIncrement { .. }
            | Command::ControlStateIncrementWithOptions { .. }
            | Command::ControlStateChangeAdd { .. }
            | Command::ControlStateSet { .. }
            | Command::ControlStateSetAndGet { .. }
            | Command::ControlStateSetAndGetWithOptions { .. }
            | Command::ControlStateSelectionSet { .. }
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

fn proxy_command_key(command: &Command) -> Option<&str> {
    match command {
        // Names no key: it exists to be committed, not to touch a record.
        Command::LeaderEstablish => None,
        Command::CommonDelete { key }
        | Command::CommonExpire { key, .. }
        | Command::CommonTtl { key }
        | Command::CommonPersist { key }
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
        | Command::FeatureQuery { key, .. }
        | Command::FeatureQueryFiltered { key, .. }
        | Command::FeatureReplace { key, .. }
        | Command::FeatureDelete { key }
        | Command::FeatureAggQuery { key, .. }
        | Command::SequenceAdd { key, .. }
        | Command::SequenceQuery { key, .. }
        | Command::ControlStateIncrement { key, .. }
        | Command::ControlStateIncrementWithOptions { key, .. }
        | Command::ControlStateChangeAdd { key, .. }
        | Command::ControlStateCount { key, .. }
        | Command::ControlStateQuery { key, .. }
        | Command::ControlStateDetail { key, .. }
        | Command::ControlStateSet { key, .. }
        | Command::ControlStateSetAndGet { key, .. }
        | Command::ControlStateSetAndGetWithOptions { key, .. }
        | Command::ControlStateFamilyQuery { key, .. }
        | Command::ControlStateSelectionSet { key, .. }
        | Command::ControlStateSelectionQuery { key }
        | Command::ControlStateManager { key, .. }
        | Command::ControlStateDebug { key, .. } => Some(key),
        Command::SequenceBatchQuery { .. }
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
        | Command::ContextMarkEmbeddingDirty { .. }
        | Command::ContextQueryEmbeddingDirty { .. }
        | Command::ContextUpsertEntity { .. }
        | Command::ContextGetEntity { .. }
        | Command::ContextQueryEntities { .. }
        | Command::ContextUpsertChildRef { .. }
        | Command::ContextQueryChildren { .. }
        | Command::ContextSetNodeEmbedding { .. }
        | Command::ContextQueryNodeEmbeddings { .. }
        | Command::ContextTraverseTree { .. }
        | Command::ContextUpsertSummary { .. }
        | Command::ContextQuerySummaries { .. }
        | Command::ContextQuerySummaryVectors { .. }
        | Command::ContextWriteCompressionEvent { .. }
        | Command::ContextQueryCompressionEvents { .. }
        | Command::ContextCompressEvents { .. }
        | Command::ContextResourceBlobBegin { .. }
        | Command::ContextResourceBlobAppend { .. }
        | Command::ContextResourceBlobCommit { .. }
        | Command::ContextResourceBlobPut { .. }
        | Command::ContextResourceBlobFetch { .. }
        | Command::ContextResourceBlobSweep { .. }
        | Command::ContextQueryNodeContext { .. } => None,
    }
}

pub(super) fn proxy_command_routing_key(command: &Command) -> Option<String> {
    proxy_command_key(command)
        .map(str::to_string)
        .or_else(|| match command {
            Command::ContextUpsertNode { tenant_hash, node } => {
                Some(format!("ctx:node:{tenant_hash}:{}", node.node_hash))
            }
            // ContextSetNodeEmbedding names a node exactly like the two above and the
            // client keys it that way, but this copy of the logic did not key it at all --
            // so a shed node was refused when read and accepted when its embedding was
            // written. Same record, same drain, two answers.
            Command::ContextGetNode {
                tenant_hash,
                node_hash,
            }
            | Command::ContextSetNodeEmbedding {
                tenant_hash,
                node_hash,
                ..
            } => Some(format!("ctx:node:{tenant_hash}:{node_hash}")),
            Command::ContextGetNodes {
                tenant_hash,
                node_hashes,
            } => node_hashes
                .first()
                .map(|node_hash| format!("ctx:node:{tenant_hash}:{node_hash}")),
            Command::ContextQuerySummaryVectors {
                tenant_hash,
                node_hashes,
                level,
                ..
            } => node_hashes
                .first()
                .map(|node_hash| format!("ctx:summary:{tenant_hash}:{node_hash}:{level}")),
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
            } => Some(format!("ctx:event:{tenant_hash}:{node_hash}")),
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
            } => Some(format!(
                "ctxidx:{tenant_hash}:{index_name}:{index_value_hash}:{scope_hash}"
            )),
            Command::ContextQueryIndexIntersection {
                tenant_hash,
                predicates,
                ..
            } => predicates.first().map(|predicate| {
                format!(
                    "ctxidx:{tenant_hash}:{}:{}:{}",
                    predicate.index_name, predicate.index_value_hash, predicate.scope_hash
                )
            }),
            Command::ContextWritePackAudit { tenant_hash, audit } => {
                Some(format!("ctx:audit:{tenant_hash}:{}", audit.session_hash))
            }
            Command::ContextQueryPackAudit {
                tenant_hash,
                session_hash,
                ..
            } => Some(format!("ctx:audit:{tenant_hash}:{session_hash}")),
            Command::ContextMarkSummaryDirty {
                tenant_hash,
                node_hash,
                ..
            } => Some(format!("ctx:dirty:{tenant_hash}:{}", node_hash)),
            Command::ContextQuerySummaryDirty {
                tenant_hash,
                node_hash,
                ..
            } => Some(format!("ctx:dirty:{tenant_hash}:{node_hash}")),
            Command::ContextMarkEmbeddingDirty {
                tenant_hash,
                node_hash,
                ..
            } => Some(format!("ctx:embdirty:{tenant_hash}:{}", node_hash)),
            Command::ContextQueryEmbeddingDirty {
                tenant_hash,
                node_hash,
                ..
            } => Some(format!("ctx:embdirty:{tenant_hash}:{node_hash}")),
            Command::ContextUpsertEntity {
                tenant_hash,
                entity,
            } => Some(format!(
                "ctx:entity:{tenant_hash}:{}:{}",
                entity.node_hash, entity.entity_hash
            )),
            Command::ContextGetEntity {
                tenant_hash,
                node_hash,
                entity_hash,
            } => Some(format!(
                "ctx:entity:{tenant_hash}:{node_hash}:{entity_hash}"
            )),
            Command::ContextQueryEntities {
                tenant_hash,
                node_hash,
                ..
            } => Some(format!("ctx:entity:{tenant_hash}:{node_hash}")),
            Command::ContextUpsertChildRef {
                tenant_hash,
                child_ref,
            } => Some(format!("ctx:child:{tenant_hash}:{}", child_ref.parent_hash)),
            Command::ContextQueryChildren {
                tenant_hash,
                parent_hash,
                ..
            } => Some(format!("ctx:child:{tenant_hash}:{parent_hash}")),
            Command::ContextTraverseTree {
                tenant_hash,
                start_node_hash,
                ..
            } => Some(format!("ctx:child:{tenant_hash}:{start_node_hash}")),
            Command::ContextUpsertSummary {
                tenant_hash,
                summary,
            } => Some(format!(
                "ctx:summary:{tenant_hash}:{}:{}",
                summary.node_hash, summary.level
            )),
            Command::ContextQuerySummaries {
                tenant_hash,
                node_hash,
                level,
                ..
            } => Some(format!("ctx:summary:{tenant_hash}:{node_hash}:{level}")),
            Command::ContextWriteCompressionEvent { tenant_hash, event } => {
                Some(format!("ctx:compress:{tenant_hash}:{}", event.node_hash))
            }
            Command::ContextQueryCompressionEvents {
                tenant_hash,
                node_hashes,
                ..
            } => node_hashes
                .first()
                .map(|node_hash| format!("ctx:compress:{tenant_hash}:{node_hash}")),
            Command::ContextCompressEvents {
                tenant_hash,
                node_hash,
                ..
            }
            | Command::ContextQueryNodeContext {
                tenant_hash,
                node_hash,
                ..
            } => Some(format!("ctx:compress:{tenant_hash}:{node_hash}")),
            _ => None,
        })
}
